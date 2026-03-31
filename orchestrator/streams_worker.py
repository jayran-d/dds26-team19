"""
orchestrator/streams_worker.py

Redis Streams consumer/producer for the orchestrator service.

Connections:
    coord_db   : orchestrator-db  — saga records, 2pc state
    order_db   : order-db         — writes order:{id}:status (polled by order-service)
    stock_db   : stock-db         — stock.commands (write) + stock.events (read)
    payment_db : payment-db       — payment.commands (write) + payment.events (read)

Workers:
    CONSUMER_WORKERS threads → stock.events
    CONSUMER_WORKERS threads → payment.events
    1 orphan recovery thread per stream source
    1 timeout scanner thread (saga mode only)
"""

import os
import threading
import time

import redis as redis_module

from common.streams_client import StreamsClient
from common.worker_logging import log_worker_exception
from common.messages import (
    STOCK_EVENTS_TOPIC,
    PAYMENT_EVENTS_TOPIC,
    STOCK_COMMANDS_TOPIC,
    PAYMENT_COMMANDS_TOPIC,
)
from protocols.saga.saga import (
    saga_route_order,
    saga_start_checkout,
    recover as saga_recover,
    check_timeouts as saga_check_timeouts,
    init_saga,
)


TRANSACTION_MODE    = os.getenv("TRANSACTION_MODE", "saga")
CONSUMER_WORKERS    = int(os.getenv("CONSUMER_WORKERS", "4"))
STREAM_BATCH_SIZE   = int(os.getenv("STREAM_BATCH_SIZE", "32"))
TIMEOUT_SCAN_INTERVAL  = 5
ORPHAN_CLAIM_INTERVAL  = 30
ORPHAN_MIN_IDLE_MS     = 30_000

ORCHESTRATOR_GROUP = "orchestrator-service"

_coord_db:   redis_module.Redis | None = None
_order_db:   redis_module.Redis | None = None
_stock_db:   redis_module.Redis | None = None
_payment_db: redis_module.Redis | None = None
_stock_sc:   StreamsClient | None = None
_payment_sc: StreamsClient | None = None
_available = False
_logger = None


def _make_order_db() -> redis_module.Redis:
    return redis_module.Redis(
        host=os.environ['ORDER_REDIS_HOST'],
        port=int(os.environ.get('ORDER_REDIS_PORT', 6379)),
        password=os.environ['ORDER_REDIS_PASSWORD'],
        db=int(os.environ.get('ORDER_REDIS_DB', 0)),
        socket_connect_timeout=5,
        socket_timeout=5,
        retry_on_timeout=True,
        health_check_interval=30,
    )


def _make_stock_db() -> redis_module.Redis:
    return redis_module.Redis(
        host=os.environ['STOCK_REDIS_HOST'],
        port=int(os.environ.get('STOCK_REDIS_PORT', 6379)),
        password=os.environ['STOCK_REDIS_PASSWORD'],
        db=int(os.environ.get('STOCK_REDIS_DB', 0)),
        socket_connect_timeout=5,
        socket_timeout=5,
        retry_on_timeout=True,
        health_check_interval=30,
    )


def _make_payment_db() -> redis_module.Redis:
    return redis_module.Redis(
        host=os.environ['PAYMENT_REDIS_HOST'],
        port=int(os.environ.get('PAYMENT_REDIS_PORT', 6379)),
        password=os.environ['PAYMENT_REDIS_PASSWORD'],
        db=int(os.environ.get('PAYMENT_REDIS_DB', 0)),
        socket_connect_timeout=5,
        socket_timeout=5,
        retry_on_timeout=True,
        health_check_interval=30,
    )


def _route_event(msg: dict, publish_fn) -> None:
    if TRANSACTION_MODE == "saga":
        saga_route_order(msg, _coord_db, publish_fn, _logger)
    elif TRANSACTION_MODE == "2pc":
        from protocols.two_pc import _2pc_route_order
        _2pc_route_order(msg)


def _ack_processed(
    streams_client: StreamsClient,
    group: str,
    processed: list[tuple[str, bytes]],
) -> None:
    by_stream: dict[str, list[bytes]] = {}
    for stream, msg_id in processed:
        by_stream.setdefault(stream, []).append(msg_id)
    for stream, msg_ids in by_stream.items():
        streams_client.ack_many(stream, group, msg_ids)


def _process_batch(
    streams_client: StreamsClient,
    group: str,
    batch: list[tuple[str, bytes, dict]],
    publish_fn,
) -> None:
    processed: list[tuple[str, bytes]] = []
    try:
        for stream, msg_id, msg in batch:
            _route_event(msg, publish_fn)
            processed.append((stream, msg_id))
    finally:
        _ack_processed(streams_client, group, processed)


def _event_worker(
    worker_id: str,
    streams_client: StreamsClient,
    streams: list,
    group: str,
    publish_fn,
) -> None:
    consumer_name = f"orch-worker-{os.getpid()}-{worker_id}"

    # Drain PEL first
    while True:
        batch = streams_client.read_many(
            streams, group, consumer_name, pending=True, count=STREAM_BATCH_SIZE,
        )
        if not batch:
            break
        try:
            _process_batch(streams_client, group, batch, publish_fn)
        except Exception as exc:
            log_worker_exception(_logger, "OrchestratorStreams", f"{consumer_name} PEL", exc)
            break

    while _available:
        try:
            batch = streams_client.read_many(
                streams, group, consumer_name, count=STREAM_BATCH_SIZE,
            )
            if not batch:
                continue
            _process_batch(streams_client, group, batch, publish_fn)
        except Exception as exc:
            log_worker_exception(_logger, "OrchestratorStreams", consumer_name, exc)
            time.sleep(0.5)


def _orphan_recovery_worker(
    streams_client: StreamsClient,
    streams: list,
    group: str,
    publish_fn,
) -> None:
    consumer_name = f"orch-orphan-{os.getpid()}"
    while _available:
        time.sleep(ORPHAN_CLAIM_INTERVAL)
        try:
            orphans = streams_client.claim_orphans(streams, group, consumer_name, ORPHAN_MIN_IDLE_MS)
            if not orphans:
                continue
            try:
                _process_batch(streams_client, group, orphans, publish_fn)
            except Exception as exc:
                log_worker_exception(_logger, "OrchestratorStreams", "orphan recovery", exc)
        except Exception as exc:
            log_worker_exception(_logger, "OrchestratorStreams", "orphan recovery worker", exc)


def _recovery_loop(publish_fn) -> None:
    while _available:
        try:
            if TRANSACTION_MODE == "saga":
                saga_check_timeouts(_coord_db, publish_fn, _logger)
            elif TRANSACTION_MODE == "2pc":
                from protocols.two_pc import recover_incomplete_2pc
                recover_incomplete_2pc()
        except Exception as exc:
            log_worker_exception(_logger, "OrchestratorStreams", "recovery loop", exc)
        time.sleep(TIMEOUT_SCAN_INTERVAL)


def init_streams(logger, coord_db: redis_module.Redis) -> None:
    global _coord_db, _order_db, _stock_db, _payment_db, _stock_sc, _payment_sc, _available, _logger

    _logger   = logger
    _coord_db = coord_db
    _order_db = _make_order_db()
    _stock_db = _make_stock_db()
    _payment_db = _make_payment_db()
    _available = True

    stock_sc   = StreamsClient(_stock_db)
    payment_sc = StreamsClient(_payment_db)
    _stock_sc   = stock_sc
    _payment_sc = payment_sc

    stock_sc.ensure_group(STOCK_EVENTS_TOPIC,   ORCHESTRATOR_GROUP)
    payment_sc.ensure_group(PAYMENT_EVENTS_TOPIC, ORCHESTRATOR_GROUP)

    # Ensure command streams exist so stock/payment can find them
    stock_sc.ensure_group(STOCK_COMMANDS_TOPIC,   "stock-service")
    payment_sc.ensure_group(PAYMENT_COMMANDS_TOPIC, "payment-service")

    def publish_fn(stream: str, message: dict) -> None:
        if 'stock' in stream:
            stock_sc.publish(stream, message)
        else:
            payment_sc.publish(stream, message)

    if TRANSACTION_MODE == "saga":
        init_saga(_order_db)
        saga_recover(_coord_db, publish_fn, logger)
    elif TRANSACTION_MODE == "2pc":
        from protocols.two_pc import init_2pc, recover_incomplete_2pc
        init_2pc(_coord_db, _order_db, publish_fn, logger)
        recover_incomplete_2pc()

    threading.Thread(target=_recovery_loop, args=(publish_fn,), daemon=True).start()

    for i in range(CONSUMER_WORKERS):
        threading.Thread(
            target=_event_worker,
            args=(f"s{i}", stock_sc, [STOCK_EVENTS_TOPIC], ORCHESTRATOR_GROUP, publish_fn),
            daemon=True,
        ).start()

    for i in range(CONSUMER_WORKERS):
        threading.Thread(
            target=_event_worker,
            args=(f"p{i}", payment_sc, [PAYMENT_EVENTS_TOPIC], ORCHESTRATOR_GROUP, publish_fn),
            daemon=True,
        ).start()

    threading.Thread(
        target=_orphan_recovery_worker,
        args=(stock_sc, [STOCK_EVENTS_TOPIC], ORCHESTRATOR_GROUP, publish_fn),
        daemon=True,
    ).start()
    threading.Thread(
        target=_orphan_recovery_worker,
        args=(payment_sc, [PAYMENT_EVENTS_TOPIC], ORCHESTRATOR_GROUP, publish_fn),
        daemon=True,
    ).start()

    logger.info(
        f"[OrchestratorStreams] {CONSUMER_WORKERS} stock.events + "
        f"{CONSUMER_WORKERS} payment.events workers started (mode={TRANSACTION_MODE})"
    )


def close_streams() -> None:
    global _available
    _available = False


def is_available() -> bool:
    return _available


def start_checkout(order_id: str, user_id: str, total_cost: int, items: list) -> dict:
    if not _available:
        raise RuntimeError("Streams worker not available")

    def publish_fn(stream: str, message: dict) -> None:
        if 'stock' in stream:
            _stock_sc.publish(stream, message)
        else:
            _payment_sc.publish(stream, message)

    if TRANSACTION_MODE == "saga":
        return saga_start_checkout(publish_fn, _coord_db, _logger, order_id, user_id, total_cost, items)

    if TRANSACTION_MODE == "2pc":
        from protocols.two_pc import _2pc_start_checkout
        return _2pc_start_checkout(_coord_db, _logger, order_id, user_id, total_cost, items)

    raise RuntimeError(f"Unknown TRANSACTION_MODE: {TRANSACTION_MODE}")
