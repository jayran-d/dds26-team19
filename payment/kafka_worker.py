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
    PAYMENT_EVENTS_TOPIC,
)

import ledger as payment_ledger


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
        # Manual commit is required for Saga durability:
        # Kafka must not acknowledge a command before we have durably handled it.
        auto_commit=False,
        auto_offset_reset="earliest",
        ensure_topics=ALL_TOPICS,
    )

    _replay_unreplied_entries()

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

            # Only acknowledge Kafka after the handler returned successfully.
            # In Saga mode this means the participant has already durably written
            # its ledger/business state, so a crash after this point is recoverable.
            _consumer.commit()

        except Exception as exc:
            # Intentionally do NOT commit here.
            # If we crashed before durable handling completed, we want Kafka
            # to redeliver the command after restart.
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
        _2pc_route_payment(msg)

def _replay_unreplied_entries() -> None:
    if TRANSACTION_MODE != "saga" or _db is None or _producer is None:
        return

    entries = payment_ledger.get_unreplied_entries(_db)
    if not entries:
        return

    _logger.info(f"[PaymentKafka] Replaying {len(entries)} unreplied payment ledger entries")

    for entry in entries:
        reply_message = entry.get("reply_message")
        if not reply_message:
            _logger.warning(
                f"[PaymentKafka] Missing reply_message in ledger tx={entry.get('tx_id')} action={entry.get('action_type')}"
            )
            continue

        _producer.publish(PAYMENT_EVENTS_TOPIC, reply_message)
        payment_ledger.mark_replied(_db, entry["tx_id"], entry["action_type"])
