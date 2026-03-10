"""
order-service/kafka_worker.py

Kafka plumbing + transaction mode switching for the order service.

app.py calls:
    start_checkout(order_id, order_entry)  — kicks off the transaction

Internally routes to saga or 2PC based on TRANSACTION_MODE env var.
Event loop receives stock.events + payment.events and drives the
transaction forward using the same mode switch.

This file does NOT contain Flask or HTTP logic.
"""

import os
import uuid
import threading
import time

import redis as redis_module

from transactions_modes.simple import simple_route_order, simple_start_checkout
from transactions_modes.saga.saga import (
    saga_route_order,
    saga_start_checkout,
    recover as saga_recover,
    check_timeouts as saga_check_timeouts,
)

from transactions_modes.two_pc import _2pc_route_order, _2pc_start_checkout

from common.kafka_client import KafkaProducerClient, KafkaConsumerClient
from common.messages import ALL_TOPICS, STOCK_EVENTS_TOPIC, PAYMENT_EVENTS_TOPIC

USE_KAFKA = os.getenv("USE_KAFKA", "false").lower() == "true"
TRANSACTION_MODE = os.getenv("TRANSACTION_MODE", "simple")  # "simple" | "saga" | "2pc"
TIMEOUT_SCAN_INTERVAL_SECONDS = 5

# ── Module-level state ─────────────────────────────────────────────────────────
_producer: KafkaProducerClient | None = None
_consumer: KafkaConsumerClient | None = None
_db: redis_module.Redis | None = None
_available: bool = False
_logger = None



# ============================================================
# INIT / TEARDOWN
# ============================================================


def init_kafka(logger, db: redis_module.Redis) -> None:
    """
    Initialise producer, consumer, and start the event loop thread.
    Called once at gunicorn startup. Does nothing if USE_KAFKA=false.
    """
    global _producer, _consumer, _db, _available, _logger

    if not USE_KAFKA:
        return

    _logger = logger
    _db = db

    _producer = KafkaProducerClient(ensure_topics=ALL_TOPICS)
    _consumer = KafkaConsumerClient(
        topics=[STOCK_EVENTS_TOPIC, PAYMENT_EVENTS_TOPIC],
        group_id="order-service",
        auto_commit=True,
        auto_offset_reset="earliest",
        ensure_topics=ALL_TOPICS,
    )

    _available = True

    if TRANSACTION_MODE == "saga":
        saga_recover(_db, _producer.publish, _logger)

        timeout_thread = threading.Thread(target=_timeout_loop, daemon=True)
        timeout_thread.start()

    event_thread = threading.Thread(target=_event_loop, daemon=True)
    event_thread.start()
    logger.info(f"[OrderKafka] Event loop started (mode={TRANSACTION_MODE})")


def close_kafka() -> None:
    global _available
    _available = False
    if _consumer is not None:
        _consumer.close()
    if _producer is not None:
        _producer.close()


def is_available() -> bool:
    return _available


# ============================================================
# PUBLIC API  (called by app.py)
# ============================================================


def start_checkout(order_id: str, order_entry) -> None:
    """
    Kick off a checkout transaction.
    Delegates to simple, saga, or 2PC based on TRANSACTION_MODE.
    Raises RuntimeError if Kafka is unavailable.
    """
    if not _available or _producer is None:
        raise RuntimeError("Kafka is not available")

    if TRANSACTION_MODE == "simple":
        simple_start_checkout(_producer, _db, _logger, order_id, order_entry)
    elif TRANSACTION_MODE == "saga":
        saga_start_checkout(_producer.publish, _db, _logger, order_id, order_entry)
    elif TRANSACTION_MODE == "2pc":
        _2pc_start_checkout(_producer, _db, _logger, order_id, order_entry)
    else:
        raise RuntimeError(f"Unknown TRANSACTION_MODE: {TRANSACTION_MODE}")


# ============================================================
# EVENT LOOP
# ============================================================


def _event_loop() -> None:
    while True:
        try:
            result = _consumer.poll(timeout=1.0)

            if result.error:
                _logger.error(f"[OrderKafka] Poll error: {result.error}")
                time.sleep(1)
                continue

            if not result.ok:
                continue

            _route_event(result.msg)

        except Exception as exc:
            _logger.error(f"[OrderKafka] Event loop crashed: {exc}")
            time.sleep(1)

def _timeout_loop() -> None:
    while _available and TRANSACTION_MODE == "saga":
        try:
            saga_check_timeouts(_db, _producer.publish, _logger)
        except Exception as exc:
            _logger.error(f"[OrderKafka] Timeout loop crashed: {exc}")
        time.sleep(TIMEOUT_SCAN_INTERVAL_SECONDS)


def _route_event(msg: dict) -> None:
    msg_type = msg.get("type")

    _logger.info(
        f"[OrderKafka] event={msg_type} "
        f"order={msg.get('order_id')} tx={msg.get('tx_id')}"
    )

    if TRANSACTION_MODE == "simple":
        simple_route_order(_producer, _db, _logger, msg, msg_type)
    elif TRANSACTION_MODE == "saga":
        saga_route_order(msg, _db, _producer.publish, _logger)
    elif TRANSACTION_MODE == "2pc":
        _2pc_route_order(msg)
