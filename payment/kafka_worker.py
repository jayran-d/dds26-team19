"""
payment-service/kafka_worker.py

Kafka plumbing for the payment service.

Consumes from : payment.commands
Publishes to  : payment.events

Handles:
    PROCESS_PAYMENT  → charge user  → PAYMENT_SUCCESS | PAYMENT_FAILED
    REFUND_PAYMENT   → refund user  → PAYMENT_REFUNDED
"""

import os
import time
import threading

import redis as redis_module

from common.kafka_client import KafkaProducerClient, KafkaConsumerClient
from common.messages import (
    ALL_TOPICS,
    PAYMENT_COMMANDS_TOPIC,
)

from transactions_modes.simple import simple_route_payment
from transactions_modes.saga import saga_route_payment
from transactions_modes.two_pc import _2pc_route_payment

USE_KAFKA = os.getenv("USE_KAFKA", "false").lower() == "true"
TRANSACTION_MODE = os.getenv("TRANSACTION_MODE", "simple")  # "simple" | "saga" | "2pc"


# ── Module-level state ─────────────────────────────────────────────────────────
_producer: KafkaProducerClient | None = None
_consumer: KafkaConsumerClient | None = None
_logger = None
_db: redis_module.Redis | None = None



# ============================================================
# INIT / TEARDOWN
# ============================================================


def init_kafka(logger, db) -> None:
    """
    Initialise Kafka clients and start the background consumer thread.
    Does nothing if USE_KAFKA=false.
    """
    global _producer, _consumer, _logger, _db

    if not USE_KAFKA:
        return

    _logger = logger
    _db = db

    _producer = KafkaProducerClient(ensure_topics=ALL_TOPICS)
    _consumer = KafkaConsumerClient(
        topics=[PAYMENT_COMMANDS_TOPIC],
        group_id="payment-service",
        auto_commit=True,
        auto_offset_reset="earliest",
        ensure_topics=ALL_TOPICS,
    )

    thread = threading.Thread(target=_consumer_loop, daemon=True)
    thread.start()
    logger.info("[PaymentKafka] Consumer thread started")


def close_kafka() -> None:
    if _consumer is not None:
        _consumer.close()
    if _producer is not None:
        _producer.close()


# ============================================================
# INTERNAL HELPERS
# ============================================================

# def _publish(message: dict) -> None:
#     _producer.send(PAYMENT_EVENTS_TOPIC, message)
#     _producer.flush()


# ============================================================
# CONSUMER LOOP
# ============================================================


def _consumer_loop() -> None:
    while True:
        try:
            result = _consumer.poll(timeout=1.0)

            if result.error:
                _logger.info(f"[PaymentKafka] Poll error: {result.error}")
                time.sleep(1)
                continue

            if not result.ok:
                continue

            _route_command(result.msg)

        except Exception as exc:
            _logger.info(f"[PaymentKafka] Consumer loop crashed: {exc}")
            time.sleep(1)


def _route_command(msg: dict) -> None:
    msg_type = msg.get("type")
    _logger.info(
        f"[PaymentKafka] command={msg_type} "
        f"order={msg.get('order_id')} tx={msg.get('tx_id')}"
    )

    if TRANSACTION_MODE == "simple":
        simple_route_payment(_producer, _logger, msg, msg_type)
    elif TRANSACTION_MODE == "saga":
        saga_route_payment(msg, _db, _producer.publish, _logger)
    elif TRANSACTION_MODE == "2pc":
        _2pc_route_payment(_producer, _db, _logger, msg, msg_type)
