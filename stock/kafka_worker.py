"""
stock-service/kafka_worker.py

Kafka plumbing for the stock service.

Consumes from : stock.commands
Publishes to  : stock.events

Handles:
    RESERVE_STOCK  → subtract stock for all items (all-or-nothing)
                   → STOCK_RESERVED | STOCK_RESERVATION_FAILED
    RELEASE_STOCK  → restore stock for all items (compensation)
                   → STOCK_RELEASED
"""

import os
import time
import threading

import redis as redis_module

from common.kafka_client import KafkaProducerClient, KafkaConsumerClient
from common.messages import (
    ALL_TOPICS,
    STOCK_COMMANDS_TOPIC,
    STOCK_EVENTS_TOPIC,
)

import ledger as stock_ledger

from transaction_modes.saga import saga_route_stock
from transaction_modes.simple import simple_route_stock
from transaction_modes.two_pc import _2pc_route_stock

USE_KAFKA = os.getenv("USE_KAFKA", "false").lower() == "true"
TRANSACTION_MODE = os.getenv("TRANSACTION_MODE", "simple")  # "simple" | "saga" | "2pc"
CONSUMER_THREADS = max(
    1,
    int(os.getenv("STOCK_KAFKA_CONSUMERS", os.getenv("KAFKA_NUM_PARTITIONS", "4"))),
)

# ── Module-level state ─────────────────────────────────────────────────────────
_producer: KafkaProducerClient | None = None
_consumers: list[KafkaConsumerClient] = []
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
    global _producer, _consumers, _logger, _db

    if not USE_KAFKA:
        return

    _logger = logger
    _db = db

    _producer = KafkaProducerClient(ensure_topics=ALL_TOPICS)
    _consumers = []
    for consumer_index in range(CONSUMER_THREADS):
        _consumers.append(
            KafkaConsumerClient(
                topics=[STOCK_COMMANDS_TOPIC],
                group_id="stock-service",
                # Disable Kafka auto-acknowledgement.
                # We only want to commit after local Saga handling is durable.
                auto_commit=False,
                auto_offset_reset="earliest",
                ensure_topics=ALL_TOPICS if consumer_index == 0 else [],
            )
        )

    _replay_unreplied_entries()

    for consumer in _consumers:
        thread = threading.Thread(target=_consumer_loop, args=(consumer,), daemon=True)
        thread.start()
    logger.info(f"[StockKafka] Consumer threads started (count={len(_consumers)})")


def close_kafka() -> None:
    for consumer in _consumers:
        consumer.close()
    if _producer is not None:
        _producer.close()

# ============================================================
# INTERNAL HELPERS
# ============================================================

# def _publish(message: dict) -> None:
#     _producer.send(STOCK_EVENTS_TOPIC, message)
#     _producer.flush()
# ============================================================
# CONSUMER LOOP
# ============================================================

def _consumer_loop(consumer: KafkaConsumerClient) -> None:
    while True:
        try:
            result = consumer.poll(timeout=1.0)

            if result.error:
                _logger.info(f"[StockKafka] Poll error: {result.error}")
                time.sleep(1)
                continue

            if not result.ok:
                continue

            _route_event(result.msg)

            # Commit only after the stock Saga handler finished.
            # At that point the ledger/business state must already be durable.
            consumer.commit()

        except Exception as exc:
            # No commit on failure: let Kafka redeliver.
            _logger.info(f"[StockKafka] Consumer loop crashed: {exc}")
            time.sleep(1)



def _route_event(msg: dict) -> None:
    msg_type = msg.get("type")

    _logger.info(
        f"[OrderKafka] event={msg_type} "
        f"order={msg.get('order_id')} tx={msg.get('tx_id')}"
    )

    if TRANSACTION_MODE == "simple":
        simple_route_stock(_producer, _logger, msg)
    elif TRANSACTION_MODE == "saga":
        saga_route_stock(msg, _db, _producer.publish, _logger)
    elif TRANSACTION_MODE == "2pc":
        _2pc_route_stock(msg, msg_type)

def _replay_unreplied_entries() -> None:
    if TRANSACTION_MODE != "saga" or _db is None or _producer is None:
        return

    entries = stock_ledger.get_unreplied_entries(_db)
    if not entries:
        return

    _logger.info(f"[StockKafka] Replaying {len(entries)} unreplied stock ledger entries")

    for entry in entries:
        reply_message = entry.get("reply_message")
        if not reply_message:
            _logger.warning(
                f"[StockKafka] Missing reply_message in ledger tx={entry.get('tx_id')} action={entry.get('action_type')}"
            )
            continue

        _producer.publish(STOCK_EVENTS_TOPIC, reply_message)
        stock_ledger.mark_replied(_db, entry["tx_id"], entry["action_type"])
