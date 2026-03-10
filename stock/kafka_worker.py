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
)

from transaction_modes.saga import saga_route_stock
from transaction_modes.simple import simple_route_stock
from transaction_modes.two_pc import _2pc_route_stock

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
    _db = _db

    _producer = KafkaProducerClient(ensure_topics=ALL_TOPICS)
    _consumer = KafkaConsumerClient(
        topics=[STOCK_COMMANDS_TOPIC],
        group_id="stock-service",
        auto_commit=True,
        auto_offset_reset="earliest",
        ensure_topics=ALL_TOPICS,
    )

    thread = threading.Thread(target=_consumer_loop, daemon=True)
    thread.start()
    logger.info("[StockKafka] Consumer thread started")


def close_kafka() -> None:
    if _consumer is not None:
        _consumer.close()
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

def _consumer_loop() -> None:
    while True:
        try:
            result = _consumer.poll(timeout=1.0)

            if result.error:
                _logger.info(f"[StockKafka] Poll error: {result.error}")
                time.sleep(1)
                continue

            if not result.ok:
                continue

            _route_event(result.msg)

        except Exception as exc:
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
        _2pc_route_stock(_producer, _logger, msg)