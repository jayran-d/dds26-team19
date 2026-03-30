"""
payment-service/streams_worker.py

Redis Streams transport for the payment service.
Replaces kafka_worker.py.

Consumes from : payment.commands (on payment-db)
Publishes to  : payment.events   (on payment-db)
"""

import os
import threading
import time

import redis as redis_module

from common.streams_client import StreamsClient
from common.messages import PAYMENT_COMMANDS_TOPIC, PAYMENT_EVENTS_TOPIC

import ledger as payment_ledger
from transactions_modes.saga import saga_route_payment
from transactions_modes.simple import simple_route_payment
from transactions_modes.two_pc import _2pc_route_payment

TRANSACTION_MODE = os.getenv("TRANSACTION_MODE", "saga")
CONSUMER_WORKERS = int(os.getenv("CONSUMER_WORKERS", "4"))
ORPHAN_CLAIM_INTERVAL = 30
ORPHAN_MIN_IDLE_MS = 30_000

PAYMENT_GROUP = "payment-service"

_db: redis_module.Redis | None = None
_available = False
_logger = None


def _replay_unreplied_entries(sc: StreamsClient) -> None:
    if TRANSACTION_MODE != "saga" or _db is None:
        return
    entries = payment_ledger.get_unreplied_entries(_db)
    if not entries:
        return
    _logger.info(f"[PaymentStreams] Replaying {len(entries)} unreplied ledger entries")
    for entry in entries:
        reply = entry.get("reply_message")
        if reply:
            sc.publish(PAYMENT_EVENTS_TOPIC, reply)
            payment_ledger.mark_replied(_db, entry["tx_id"], entry["action_type"])


def _route_command(msg: dict, publish_fn) -> None:
    msg_type = msg.get("type")
    if TRANSACTION_MODE == "simple":
        simple_route_payment(None, _logger, msg, msg_type)
    elif TRANSACTION_MODE == "saga":
        saga_route_payment(msg, _db, publish_fn, _logger)
    elif TRANSACTION_MODE == "2pc":
        _2pc_route_payment(msg)


def _consumer_worker(worker_id: str, sc: StreamsClient, publish_fn) -> None:
    consumer_name = f"payment-worker-{os.getpid()}-{worker_id}"

    # Drain own PEL first
    while True:
        result = sc.read_one([PAYMENT_COMMANDS_TOPIC], PAYMENT_GROUP, consumer_name, pending=True)
        if not result:
            break
        stream, msg_id, msg = result
        try:
            _route_command(msg, publish_fn)
            sc.ack(stream, PAYMENT_GROUP, msg_id)
        except Exception as exc:
            _logger.error(f"[PaymentStreams] {consumer_name} PEL recovery error: {exc}")
            break

    while _available:
        try:
            result = sc.read_one([PAYMENT_COMMANDS_TOPIC], PAYMENT_GROUP, consumer_name)
            if not result:
                continue
            stream, msg_id, msg = result
            _route_command(msg, publish_fn)
            sc.ack(stream, PAYMENT_GROUP, msg_id)
        except Exception as exc:
            _logger.error(f"[PaymentStreams] {consumer_name} crashed: {exc}")
            time.sleep(0.5)


def _orphan_recovery_worker(sc: StreamsClient, publish_fn) -> None:
    consumer_name = f"payment-orphan-{os.getpid()}"
    while _available:
        time.sleep(ORPHAN_CLAIM_INTERVAL)
        try:
            orphans = sc.claim_orphans([PAYMENT_COMMANDS_TOPIC], PAYMENT_GROUP, consumer_name, ORPHAN_MIN_IDLE_MS)
            for stream, msg_id, msg in orphans:
                try:
                    _route_command(msg, publish_fn)
                    sc.ack(stream, PAYMENT_GROUP, msg_id)
                except Exception as exc:
                    _logger.error(f"[PaymentStreams] orphan recovery error: {exc}")
        except Exception as exc:
            _logger.error(f"[PaymentStreams] orphan recovery worker crashed: {exc}")


def init_streams(logger, db: redis_module.Redis) -> None:
    global _db, _available, _logger
    _logger = logger
    _db = db
    _available = True

    main_sc = StreamsClient(db)
    main_sc.ensure_group(PAYMENT_COMMANDS_TOPIC, PAYMENT_GROUP)
    main_sc.ensure_group(PAYMENT_EVENTS_TOPIC, PAYMENT_GROUP)

    def publish_fn(stream: str, message: dict) -> None:
        main_sc.publish(stream, message)

    _replay_unreplied_entries(main_sc)

    for i in range(CONSUMER_WORKERS):
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

    logger.info(f"[PaymentStreams] {CONSUMER_WORKERS} consumer workers started (mode={TRANSACTION_MODE})")


def close_streams() -> None:
    global _available
    _available = False
