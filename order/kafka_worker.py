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
from concurrent.futures import ThreadPoolExecutor

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
_event_executor: ThreadPoolExecutor | None = None



# ============================================================
# INIT / TEARDOWN
# ============================================================


def init_kafka(logger, db: redis_module.Redis) -> None:
    """
    Initialise producer, consumer, and start the event loop thread.
    Called once at gunicorn startup. Does nothing if USE_KAFKA=false.
    """
    global _producer, _consumer, _db, _available, _logger, _event_executor

    if not USE_KAFKA:
        return

    _logger = logger
    _db = db

    _producer = KafkaProducerClient(ensure_topics=ALL_TOPICS)
    _consumer = KafkaConsumerClient(
        topics=[STOCK_EVENTS_TOPIC, PAYMENT_EVENTS_TOPIC],
        group_id="order-service",
        # The order service also needs manual commits.
        # It must not ack an event before the Saga record is durably advanced.
        auto_commit=False,
        auto_offset_reset="earliest",
        ensure_topics=ALL_TOPICS,
    )

    _available = True
    
    # Create thread pool for parallel event processing (up to 8 concurrent events)
    _event_executor = ThreadPoolExecutor(max_workers=8, thread_name_prefix="event-worker")

    if TRANSACTION_MODE == "saga":
        # Recover before the event loop starts consuming new events. That keeps
        # startup deterministic: first restore in-flight intent, then process
        # whatever Kafka redelivers afterwards.
        saga_recover(_db, _producer.publish, _logger)

        timeout_thread = threading.Thread(target=_timeout_loop, daemon=True)
        timeout_thread.start()

    event_thread = threading.Thread(target=_event_loop, daemon=True)
    event_thread.start()
    logger.info(f"[OrderKafka] Event loop started (mode={TRANSACTION_MODE})")


def close_kafka() -> None:
    global _available, _event_executor
    _available = False
    if _event_executor is not None:
        _event_executor.shutdown(wait=True)
    if _consumer is not None:
        _consumer.close()
    if _producer is not None:
        _producer.close()


def is_available() -> bool:
    return _available


# ============================================================
# PUBLIC API  (called by app.py)
# ============================================================


def start_checkout(order_id: str, order_entry):
    """
    Kick off a checkout transaction.

    Returns a small result dict so the HTTP route can distinguish:
    - new checkout started
    - checkout already in progress
    - error
    """
    if not _available or _producer is None:
        raise RuntimeError("Kafka is not available")

    if TRANSACTION_MODE == "simple":
        simple_start_checkout(_producer, _db, _logger, order_id, order_entry)
        return {"started": True, "reason": "started"}

    if TRANSACTION_MODE == "saga":
        return saga_start_checkout(_producer.publish, _db, _logger, order_id, order_entry)

    if TRANSACTION_MODE == "2pc":
        _2pc_start_checkout(_producer, _db, _logger, order_id, order_entry)
        return {"started": True, "reason": "started"}

    raise RuntimeError(f"Unknown TRANSACTION_MODE: {TRANSACTION_MODE}")


# ============================================================
# EVENT LOOP
# ============================================================


def _event_loop() -> None:
    while True:
        try:
            result = _consumer.poll(timeout=0.1)

            if result.error:
                _logger.error(f"[OrderKafka] Poll error: {result.error}")
                time.sleep(1)
                continue

            if not result.ok:
                continue

            # Submit event processing to thread pool instead of blocking
            _event_executor.submit(_process_event_async, result.msg)

        except Exception as exc:
            # No commit here.
            # If we fail before the Saga state is durably advanced, Kafka should
            # redeliver the event and let the orchestrator try again.
            _logger.error(f"[OrderKafka] Event loop crashed: {exc}")
            time.sleep(1)


def _process_event_async(msg: dict) -> None:
    """Process event in thread pool and commit after successful handling."""
    try:
        _route_event(msg)
        # Commit only after event handling returned successfully.
        _consumer.commit()
    except Exception as exc:
        # No commit on error - let Kafka redeliver
        _logger.error(f"[OrderKafka] Event processing failed: {exc}")

def _timeout_loop() -> None:
    while _available and TRANSACTION_MODE == "saga":
        try:
            # Timeouts are handled centrally by the orchestrator. Participants
            # stay simpler: they only need durable local handling + replay.
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
