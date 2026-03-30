"""
order-service/streams_worker.py

Redis Streams transport for the order service.
Replaces kafka_worker.py with much lower latency.

Architecture:
    - Order service connects to THREE Redis instances:
        order_db   : own Redis for saga records and order state
        stock_db   : stock-db for stock.commands (write) and stock.events (read)
        payment_db : payment-db for payment.commands (write) and payment.events (read)

    - CONSUMER_WORKERS threads read stock.events from stock_db
    - CONSUMER_WORKERS threads read payment.events from payment_db
    - publish() routes to the right Redis based on stream name

    - checkout_notify.notify() is called by saga.py on terminal states,
      unblocking the HTTP handler via threading.Event instead of polling.
"""

import os
import threading
import time

import redis as redis_module

from common.streams_client import StreamsClient
from common.messages import (
    ALL_TOPICS,
    STOCK_EVENTS_TOPIC,
    PAYMENT_EVENTS_TOPIC,
    STOCK_COMMANDS_TOPIC,
    PAYMENT_COMMANDS_TOPIC,
)
from transactions_modes.saga.saga import (
    saga_route_order,
    saga_start_checkout,
    recover as saga_recover,
    check_timeouts as saga_check_timeouts,
)


TRANSACTION_MODE = os.getenv("TRANSACTION_MODE", "saga")
CONSUMER_WORKERS = int(os.getenv("CONSUMER_WORKERS", "4"))
STREAM_BATCH_SIZE = int(os.getenv("STREAM_BATCH_SIZE", "32"))
TIMEOUT_SCAN_INTERVAL = 5
ORPHAN_CLAIM_INTERVAL = 30
ORPHAN_MIN_IDLE_MS = 30_000

ORDER_GROUP = "order-service"

_order_db: redis_module.Redis | None = None
_stock_db: redis_module.Redis | None = None
_payment_db: redis_module.Redis | None = None
_stock_sc: StreamsClient | None = None
_payment_sc: StreamsClient | None = None
_available = False
_logger = None


class _StreamProducer:
    def __init__(self, publish_fn):
        self._publish_fn = publish_fn

    def publish(self, stream: str, message: dict) -> None:
        self._publish_fn(stream, message)


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
        saga_route_order(msg, _order_db, publish_fn, _logger)
    elif TRANSACTION_MODE == "2pc":
        from transactions_modes.two_pc import _2pc_route_order
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
    consumer_name = f"order-worker-{os.getpid()}-{worker_id}"

    # Drain our own PEL first (messages from our previous run that weren't acked)
    while True:
        batch = streams_client.read_many(
            streams,
            group,
            consumer_name,
            pending=True,
            count=STREAM_BATCH_SIZE,
        )
        if not batch:
            break
        try:
            _process_batch(streams_client, group, batch, publish_fn)
        except Exception as exc:
            _logger.error(f"[OrderStreams] {consumer_name} PEL recovery error: {exc}")
            break

    # Normal consume loop
    while _available:
        try:
            batch = streams_client.read_many(
                streams,
                group,
                consumer_name,
                count=STREAM_BATCH_SIZE,
            )
            if not batch:
                continue
            _process_batch(streams_client, group, batch, publish_fn)
        except Exception as exc:
            _logger.error(f"[OrderStreams] {consumer_name} crashed: {exc}")
            time.sleep(0.5)


def _orphan_recovery_worker(
    streams_client: StreamsClient,
    streams: list,
    group: str,
    publish_fn,
) -> None:
    """Periodically reclaim messages idle > ORPHAN_MIN_IDLE_MS from any consumer."""
    consumer_name = f"order-orphan-{os.getpid()}"
    while _available:
        time.sleep(ORPHAN_CLAIM_INTERVAL)
        try:
            orphans = streams_client.claim_orphans(streams, group, consumer_name, ORPHAN_MIN_IDLE_MS)
            if not orphans:
                continue
            try:
                _process_batch(streams_client, group, orphans, publish_fn)
            except Exception as exc:
                _logger.error(f"[OrderStreams] orphan recovery error: {exc}")
        except Exception as exc:
            _logger.error(f"[OrderStreams] orphan recovery worker crashed: {exc}")


def _timeout_loop(publish_fn) -> None:
    while _available and TRANSACTION_MODE == "saga":
        try:
            saga_check_timeouts(_order_db, publish_fn, _logger)
        except Exception as exc:
            _logger.error(f"[OrderStreams] Timeout loop crashed: {exc}")
        time.sleep(TIMEOUT_SCAN_INTERVAL)


def init_streams(logger, order_db: redis_module.Redis) -> None:
    """
    Initialise Redis Streams consumers and start background worker threads.
    Called once at gunicorn startup.
    """
    global _order_db, _stock_db, _payment_db, _stock_sc, _payment_sc, _available, _logger

    _logger = logger
    _order_db = order_db
    _stock_db = _make_stock_db()
    _payment_db = _make_payment_db()
    _available = True

    stock_sc = StreamsClient(_stock_db)
    payment_sc = StreamsClient(_payment_db)
    _stock_sc = stock_sc
    _payment_sc = payment_sc

    # Ensure consumer groups exist
    stock_sc.ensure_group(STOCK_EVENTS_TOPIC, ORDER_GROUP)
    payment_sc.ensure_group(PAYMENT_EVENTS_TOPIC, ORDER_GROUP)

    # Also ensure command streams exist so stock/payment services can find them
    stock_sc.ensure_group(STOCK_COMMANDS_TOPIC, "stock-service")
    payment_sc.ensure_group(PAYMENT_COMMANDS_TOPIC, "payment-service")

    def publish_fn(stream: str, message: dict) -> None:
        if 'stock' in stream:
            stock_sc.publish(stream, message)
        else:
            payment_sc.publish(stream, message)

    if TRANSACTION_MODE == "saga":
        saga_recover(_order_db, publish_fn, logger)
        threading.Thread(target=_timeout_loop, args=(publish_fn,), daemon=True).start()

    # Start stock.events consumer workers (share stock_sc; pool is thread-safe)
    for i in range(CONSUMER_WORKERS):
        threading.Thread(
            target=_event_worker,
            args=(f"s{i}", stock_sc, [STOCK_EVENTS_TOPIC], ORDER_GROUP, publish_fn),
            daemon=True,
        ).start()

    # Start payment.events consumer workers
    for i in range(CONSUMER_WORKERS):
        threading.Thread(
            target=_event_worker,
            args=(f"p{i}", payment_sc, [PAYMENT_EVENTS_TOPIC], ORDER_GROUP, publish_fn),
            daemon=True,
        ).start()

    # Orphan recovery threads (one per stream source)
    threading.Thread(
        target=_orphan_recovery_worker,
        args=(stock_sc, [STOCK_EVENTS_TOPIC], ORDER_GROUP, publish_fn),
        daemon=True,
    ).start()
    threading.Thread(
        target=_orphan_recovery_worker,
        args=(payment_sc, [PAYMENT_EVENTS_TOPIC], ORDER_GROUP, publish_fn),
        daemon=True,
    ).start()

    logger.info(
        f"[OrderStreams] {CONSUMER_WORKERS} stock.events workers + "
        f"{CONSUMER_WORKERS} payment.events workers started (mode={TRANSACTION_MODE})"
    )


def close_streams() -> None:
    global _available
    _available = False


def is_available() -> bool:
    return _available


def start_checkout(order_id: str, order_entry) -> dict:
    """Kick off a checkout transaction. Called by app.py."""
    if not _available:
        raise RuntimeError("Streams worker not available")

    def publish_fn(stream: str, message: dict) -> None:
        if 'stock' in stream:
            _stock_sc.publish(stream, message)
        else:
            _payment_sc.publish(stream, message)

    if TRANSACTION_MODE == "saga":
        return saga_start_checkout(publish_fn, _order_db, _logger, order_id, order_entry)

    if TRANSACTION_MODE == "2pc":
        from transactions_modes.two_pc import _2pc_start_checkout
        _2pc_start_checkout(None, _order_db, _logger, order_id, order_entry)
        return {"started": True, "reason": "started"}

    raise RuntimeError(f"Unknown TRANSACTION_MODE: {TRANSACTION_MODE}")
