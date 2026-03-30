"""
stock-service/streams_worker.py

Redis Streams transport for the stock service.
Replaces kafka_worker.py.

Consumes from : stock.commands (on stock-db)
Publishes to  : stock.events   (on stock-db)

Both streams live on the stock service's own Redis, so no cross-service
Redis connections are needed. The publish() call after processing is just
an XADD to the local Redis — very fast.
"""

import os
import threading
import time

import redis as redis_module

from common.streams_client import StreamsClient
from common.messages import STOCK_COMMANDS_TOPIC, STOCK_EVENTS_TOPIC

import ledger as stock_ledger
from transaction_modes.saga import saga_route_stock
from transaction_modes.simple import simple_route_stock
from transaction_modes.two_pc import _2pc_route_stock

TRANSACTION_MODE = os.getenv("TRANSACTION_MODE", "saga")
CONSUMER_WORKERS = int(os.getenv("CONSUMER_WORKERS", "4"))
STREAM_BATCH_SIZE = int(os.getenv("STREAM_BATCH_SIZE", "32"))
ORPHAN_CLAIM_INTERVAL = 30
ORPHAN_MIN_IDLE_MS = 30_000

STOCK_GROUP = "stock-service"

_db: redis_module.Redis | None = None
_sc: StreamsClient | None = None
_available = False
_logger = None


class _StreamProducer:
    def __init__(self, publish_fn):
        self._publish_fn = publish_fn

    def publish(self, stream: str, message: dict) -> None:
        self._publish_fn(stream, message)


def _replay_unreplied_entries(sc: StreamsClient) -> None:
    """Re-publish any replies that were applied but not sent before a crash."""
    if TRANSACTION_MODE not in {"saga", "2pc"} or _db is None:
        return
    entries = stock_ledger.get_unreplied_entries(_db)
    if not entries:
        return
    _logger.info(f"[StockStreams] Replaying {len(entries)} unreplied ledger entries")
    for entry in entries:
        reply = entry.get("reply_message")
        if reply:
            sc.publish(STOCK_EVENTS_TOPIC, reply)
            stock_ledger.mark_replied(_db, entry["tx_id"], entry["action_type"])


def _route_command(msg: dict, publish_fn) -> None:
    msg_type = msg.get("type")
    if TRANSACTION_MODE == "simple":
        simple_route_stock(_StreamProducer(publish_fn), _logger, msg)
    elif TRANSACTION_MODE == "saga":
        saga_route_stock(msg, _db, publish_fn, _logger)
    elif TRANSACTION_MODE == "2pc":
        _2pc_route_stock(msg, msg_type)


def _ack_processed(sc: StreamsClient, group: str, processed: list[tuple[str, bytes]]) -> None:
    by_stream: dict[str, list[bytes]] = {}
    for stream, msg_id in processed:
        by_stream.setdefault(stream, []).append(msg_id)
    for stream, msg_ids in by_stream.items():
        sc.ack_many(stream, group, msg_ids)


def _process_batch(sc: StreamsClient, batch: list[tuple[str, bytes, dict]], publish_fn) -> None:
    processed: list[tuple[str, bytes]] = []
    try:
        for stream, msg_id, msg in batch:
            _route_command(msg, publish_fn)
            processed.append((stream, msg_id))
    finally:
        _ack_processed(sc, STOCK_GROUP, processed)


def _consumer_worker(worker_id: str, sc: StreamsClient, publish_fn) -> None:
    consumer_name = f"stock-worker-{os.getpid()}-{worker_id}"

    # Drain own PEL first
    while True:
        batch = sc.read_many(
            [STOCK_COMMANDS_TOPIC],
            STOCK_GROUP,
            consumer_name,
            pending=True,
            count=STREAM_BATCH_SIZE,
        )
        if not batch:
            break
        try:
            _process_batch(sc, batch, publish_fn)
        except Exception as exc:
            _logger.error(f"[StockStreams] {consumer_name} PEL recovery error: {exc}")
            break

    while _available:
        try:
            batch = sc.read_many(
                [STOCK_COMMANDS_TOPIC],
                STOCK_GROUP,
                consumer_name,
                count=STREAM_BATCH_SIZE,
            )
            if not batch:
                continue
            _process_batch(sc, batch, publish_fn)
        except Exception as exc:
            _logger.error(f"[StockStreams] {consumer_name} crashed: {exc}")
            time.sleep(0.5)


def _orphan_recovery_worker(sc: StreamsClient, publish_fn) -> None:
    consumer_name = f"stock-orphan-{os.getpid()}"
    while _available:
        time.sleep(ORPHAN_CLAIM_INTERVAL)
        try:
            orphans = sc.claim_orphans([STOCK_COMMANDS_TOPIC], STOCK_GROUP, consumer_name, ORPHAN_MIN_IDLE_MS)
            if not orphans:
                continue
            try:
                _process_batch(sc, orphans, publish_fn)
            except Exception as exc:
                _logger.error(f"[StockStreams] orphan recovery error: {exc}")
        except Exception as exc:
            _logger.error(f"[StockStreams] orphan recovery worker crashed: {exc}")


def init_streams(logger, db: redis_module.Redis) -> None:
    global _db, _sc, _available, _logger
    _logger = logger
    _db = db
    _available = True

    main_sc = StreamsClient(db)
    main_sc.ensure_group(STOCK_COMMANDS_TOPIC, STOCK_GROUP)
    main_sc.ensure_group(STOCK_EVENTS_TOPIC, STOCK_GROUP)

    def publish_fn(stream: str, message: dict) -> None:
        main_sc.publish(stream, message)

    if TRANSACTION_MODE == "2pc":
        from transaction_modes.two_pc import init_2pc
        init_2pc(db, publish_fn, logger)

    _replay_unreplied_entries(main_sc)

    for i in range(CONSUMER_WORKERS):
        # Workers share the same StreamsClient; redis-py connection pool is thread-safe.
        # Each XREADGROUP BLOCK call acquires its own socket from the pool.
        threading.Thread(
            target=_consumer_worker,
            args=(str(i), main_sc, publish_fn),
            daemon=True,
        ).start()

    threading.Thread(
        target=_orphan_recovery_worker,
        args=(main_sc, publish_fn),
        daemon=True,
    ).start()

    logger.info(f"[StockStreams] {CONSUMER_WORKERS} consumer workers started (mode={TRANSACTION_MODE})")


def close_streams() -> None:
    global _available
    _available = False
