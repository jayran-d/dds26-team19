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

from common.kafka_client import KafkaProducerClient, KafkaConsumerClient
from common.messages import (
    ALL_TOPICS,
    PAYMENT_COMMANDS_TOPIC,
    PAYMENT_EVENTS_TOPIC,
    PROCESS_PAYMENT,
    REFUND_PAYMENT,
    build_payment_success,
    build_payment_failed,
    build_payment_refunded,
)

USE_KAFKA = os.getenv("USE_KAFKA", "false").lower() == "true"

# ── Module-level state ─────────────────────────────────────────────────────────
_producer: KafkaProducerClient | None = None
_consumer: KafkaConsumerClient | None = None
_logger = None


# ============================================================
# INIT / TEARDOWN
# ============================================================

def init_kafka(logger) -> None:
    """
    Initialise Kafka clients and start the background consumer thread.
    Does nothing if USE_KAFKA=false.
    """
    global _producer, _consumer, _logger

    if not USE_KAFKA:
        return

    _logger = logger

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

def _publish(message: dict) -> None:
    _producer.send(PAYMENT_EVENTS_TOPIC, message)
    _producer.flush()


# ============================================================
# CONSUMER LOOP
# ============================================================

def _consumer_loop() -> None:
    while True:
        try:
            result = _consumer.poll(timeout=1.0)

            if result.error:
                _logger.error(f"[PaymentKafka] Poll error: {result.error}")
                time.sleep(1)
                continue

            if not result.ok:
                continue

            _route_command(result.msg)

        except Exception as exc:
            _logger.error(f"[PaymentKafka] Consumer loop crashed: {exc}")
            time.sleep(1)


def _route_command(msg: dict) -> None:
    msg_type = msg.get("type")
    _logger.debug(
        f"[PaymentKafka] command={msg_type} "
        f"order={msg.get('order_id')} tx={msg.get('tx_id')}"
    )

    if msg_type == PROCESS_PAYMENT:
        _handle_process_payment(msg)
    elif msg_type == REFUND_PAYMENT:
        _handle_refund_payment(msg)
    else:
        _logger.warning(f"[PaymentKafka] Unknown command type: {msg_type!r} — dropping")


# ============================================================
# COMMAND HANDLERS
# ============================================================

def _handle_process_payment(msg: dict) -> None:
    tx_id    = msg.get("tx_id")
    order_id = msg.get("order_id")
    payload  = msg.get("payload", {})
    user_id  = str(payload.get("user_id", ""))
    amount   = payload.get("amount")

    if not user_id or amount is None:
        _logger.error(f"[PaymentKafka] Invalid PROCESS_PAYMENT payload: {msg}")
        _publish(build_payment_failed(tx_id, order_id, "Invalid payment command payload"))
        return

    # Import here to avoid circular imports with app.py
    from app import remove_credit_internal

    success, error, _ = remove_credit_internal(user_id, int(amount))

    if success:
        _logger.debug(f"[PaymentKafka] order={order_id} payment success")
        _publish(build_payment_success(tx_id, order_id))
    else:
        _logger.debug(f"[PaymentKafka] order={order_id} payment failed: {error}")
        _publish(build_payment_failed(tx_id, order_id, error))


def _handle_refund_payment(msg: dict) -> None:
    tx_id    = msg.get("tx_id")
    order_id = msg.get("order_id")
    payload  = msg.get("payload", {})
    user_id  = str(payload.get("user_id", ""))
    amount   = payload.get("amount")

    if not user_id or amount is None:
        _logger.error(f"[PaymentKafka] Invalid REFUND_PAYMENT payload: {msg}")
        return

    from app import add_credit_internal

    success, error, _ = add_credit_internal(user_id, int(amount))

    if success:
        _logger.debug(f"[PaymentKafka] order={order_id} refund success")
        _publish(build_payment_refunded(tx_id, order_id))
    else:
        _logger.error(f"[PaymentKafka] order={order_id} refund failed: {error}")