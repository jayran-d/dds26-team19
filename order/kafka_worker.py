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

from common.kafka_client import KafkaProducerClient, KafkaConsumerClient
from common.messages import (
    ALL_TOPICS,
    STOCK_COMMANDS_TOPIC,
    PAYMENT_COMMANDS_TOPIC,
    STOCK_EVENTS_TOPIC,
    PAYMENT_EVENTS_TOPIC,
    SagaOrderStatus,
    TwoPhaseOrderStatus,
    # Saga stock events
    STOCK_RESERVED,
    STOCK_RESERVATION_FAILED,
    STOCK_RELEASED,
    # Saga payment events
    PAYMENT_SUCCESS,
    PAYMENT_FAILED,
    PAYMENT_REFUNDED,
    # 2PC stock events
    STOCK_PREPARED,
    STOCK_PREPARE_FAILED,
    STOCK_COMMITTED,
    STOCK_ABORTED,
    # 2PC payment events
    PAYMENT_PREPARED,
    PAYMENT_PREPARE_FAILED,
    PAYMENT_COMMITTED,
    PAYMENT_ABORTED,
    # builders
    build_reserve_stock,
    build_process_payment,
    build_prepare_stock,
    build_prepare_payment,
)

USE_KAFKA        = os.getenv("USE_KAFKA", "false").lower() == "true"
TRANSACTION_MODE = os.getenv("TRANSACTION_MODE", "simple")  # "simple" | "saga" | "2pc"

# ── Module-level state ─────────────────────────────────────────────────────────
_producer:  KafkaProducerClient | None = None
_consumer:  KafkaConsumerClient | None = None
_db:        redis_module.Redis  | None = None
_available: bool = False
_logger          = None


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
    _db     = db

    _producer = KafkaProducerClient(ensure_topics=ALL_TOPICS)
    _consumer = KafkaConsumerClient(
        topics=[STOCK_EVENTS_TOPIC, PAYMENT_EVENTS_TOPIC],
        group_id="order-service",
        auto_commit=True,
        auto_offset_reset="earliest",
        ensure_topics=ALL_TOPICS,
    )

    _available = True

    thread = threading.Thread(target=_event_loop, daemon=True)
    thread.start()
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
        _simple_start_checkout(order_id, order_entry)
    elif TRANSACTION_MODE == "saga":
        _saga_start_checkout(order_id, order_entry)
    elif TRANSACTION_MODE == "2pc":
        _2pc_start_checkout(order_id, order_entry)
    else:
        raise RuntimeError(f"Unknown TRANSACTION_MODE: {TRANSACTION_MODE}")


# ============================================================
# INTERNAL PUBLISH HELPER
# ============================================================

def _publish(topic: str, message: dict) -> None:
    _producer.send(topic, message)
    _producer.flush()


# ============================================================
# REDIS HELPERS
# ============================================================

def _set_status(order_id: str, status: str) -> None:
    try:
        _db.set(f"order:{order_id}:status", status)
    except Exception as e:
        _logger.error(f"[OrderKafka] Failed to set status {status} for {order_id}: {e}")


def _get_order(order_id: str):
    """Return the raw msgpack bytes for an order from Redis."""
    try:
        return _db.get(order_id)
    except Exception as e:
        _logger.error(f"[OrderKafka] Failed to get order {order_id}: {e}")
        return None


def _get_tx_id(order_id: str) -> str | None:
    try:
        val = _db.get(f"order:{order_id}:tx_id")
        return val.decode() if val else None
    except Exception:
        return None


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


def _route_event(msg: dict) -> None:
    msg_type = msg.get("type")
    
    _logger.info(
        f"[OrderKafka] event={msg_type} "
        f"order={msg.get('order_id')} tx={msg.get('tx_id')}"
    )

    if TRANSACTION_MODE == "simple":
        _simple_route(msg, msg_type)
    elif TRANSACTION_MODE == "saga":
        _saga_route(msg, msg_type)
    elif TRANSACTION_MODE == "2pc":
        _2pc_route(msg, msg_type)


# ============================================================
# SIMPLE  (no orchestration — Kafka equivalent of the HTTP template)
# ============================================================

def _simple_start_checkout(order_id: str, order_entry) -> None:
    """
    Publishes RESERVE_STOCK for all items.
    On STOCK_RESERVED → publishes PROCESS_PAYMENT.
    On STOCK_RESERVATION_FAILED → publishes RELEASE_STOCK for already-reserved items.
    On PAYMENT_FAILED → publishes RELEASE_STOCK.
    On PAYMENT_SUCCESS → marks order paid.
    No saga state machine, no 2PC. Direct Kafka equivalent of the template checkout.
    """
    from collections import defaultdict

    items_quantities: dict = defaultdict(int)
    for item_id, quantity in order_entry.items:
        items_quantities[item_id] += quantity

    items = [
        {"item_id": iid, "quantity": qty}
        for iid, qty in items_quantities.items()
    ]

    tx_id = str(uuid.uuid4())
    _db.set(f"order:{order_id}:tx_id", tx_id)

    msg = build_reserve_stock(tx_id=tx_id, order_id=order_id, items=items)
    _publish(STOCK_COMMANDS_TOPIC, msg)
    _logger.debug(f"[Simple] order={order_id} tx={tx_id} RESERVE_STOCK published")


def _simple_route(msg: dict, msg_type: str) -> None:
    handlers = {
        STOCK_RESERVED:           _simple_on_stock_reserved,
        STOCK_RESERVATION_FAILED: _simple_on_stock_reservation_failed,
        STOCK_RELEASED:           _simple_on_stock_released,
        PAYMENT_SUCCESS:          _simple_on_payment_success,
        PAYMENT_FAILED:           _simple_on_payment_failed,
    }
    handler = handlers.get(msg_type)
    if handler:
        handler(msg)
    else:
        _logger.warning(f"[Simple] Unknown event type: {msg_type!r} — dropping")


def _simple_on_stock_reserved(msg: dict) -> None:
    """Stock reserved — now charge the user."""
    from msgspec import msgpack
    from app import OrderValue

    order_id = msg.get("order_id")
    tx_id    = msg.get("tx_id")

    raw = _get_order(order_id)
    if not raw:
        _logger.error(f"[Simple] order {order_id} not found in Redis")
        return

    order = msgpack.decode(raw, type=OrderValue)
    payment_msg = build_process_payment(
        tx_id=tx_id,
        order_id=order_id,
        user_id=order.user_id,
        amount=order.total_cost,
    )
    _publish(PAYMENT_COMMANDS_TOPIC, payment_msg)
    _logger.debug(f"[Simple] order={order_id} PROCESS_PAYMENT published")


def _simple_on_stock_reservation_failed(msg: dict) -> None:
    """Stock reservation failed — nothing was reserved, just mark failed."""
    order_id = msg.get("order_id")
    _logger.debug(f"[Simple] order={order_id} stock reservation failed — no rollback needed")
    _set_status(order_id, "failed")


def _simple_on_stock_released(msg: dict) -> None:
    """Stock successfully released after a payment failure — mark order failed."""
    order_id = msg.get("order_id")
    _logger.debug(f"[Simple] order={order_id} stock released — marking failed")
    _set_status(order_id, "failed")


def _simple_on_payment_success(msg: dict) -> None:
    """Payment succeeded — mark order as paid."""
    from msgspec import msgpack
    from app import OrderValue

    order_id = msg.get("order_id")

    raw = _get_order(order_id)
    if not raw:
        _logger.error(f"[Simple] order {order_id} not found in Redis")
        return

    order = msgpack.decode(raw, type=OrderValue)
    order = OrderValue(
        paid=True,
        items=order.items,
        user_id=order.user_id,
        total_cost=order.total_cost,
    )
    _db.set(order_id, msgpack.encode(order))
    _set_status(order_id, "completed")
    _logger.debug(f"[Simple] order={order_id} marked paid — COMPLETED")


def _simple_on_payment_failed(msg: dict) -> None:
    """Payment failed — release the reserved stock."""
    from msgspec import msgpack
    from app import OrderValue

    order_id = msg.get("order_id")
    tx_id    = msg.get("tx_id")

    raw = _get_order(order_id)
    if not raw:
        _logger.error(f"[Simple] order {order_id} not found in Redis")
        return

    order = msgpack.decode(raw, type=OrderValue)
    from collections import defaultdict
    items_quantities: dict = defaultdict(int)
    for item_id, quantity in order.items:
        items_quantities[item_id] += quantity

    items = [{"item_id": iid, "quantity": qty} for iid, qty in items_quantities.items()]

    from common.messages import build_release_stock
    release_msg = build_release_stock(tx_id=tx_id, order_id=order_id, items=items)
    _publish(STOCK_COMMANDS_TOPIC, release_msg)
    _logger.debug(f"[Simple] order={order_id} payment failed — RELEASE_STOCK published")


# ============================================================
# SAGA
# ============================================================

def _saga_start_checkout(order_id: str, order_entry) -> None:
    from collections import defaultdict
    from msgspec import msgpack
    from app import OrderValue

    items_quantities: dict = defaultdict(int)
    for item_id, quantity in order_entry.items:
        items_quantities[item_id] += quantity

    items = [
        {"item_id": iid, "quantity": qty}
        for iid, qty in items_quantities.items()
    ]

    tx_id = str(uuid.uuid4())
    _db.set(f"order:{order_id}:tx_id", tx_id)
    _set_status(order_id, SagaOrderStatus.RESERVING_STOCK)

    msg = build_reserve_stock(tx_id=tx_id, order_id=order_id, items=items)
    _publish(STOCK_COMMANDS_TOPIC, msg)
    _logger.debug(f"[Saga] order={order_id} tx={tx_id} RESERVE_STOCK published")


def _saga_route(msg: dict, msg_type: str) -> None:
    handlers = {
        STOCK_RESERVED:           _saga_on_stock_reserved,
        STOCK_RESERVATION_FAILED: _saga_on_stock_reservation_failed,
        STOCK_RELEASED:           _saga_on_stock_released,
        PAYMENT_SUCCESS:          _saga_on_payment_success,
        PAYMENT_FAILED:           _saga_on_payment_failed,
        PAYMENT_REFUNDED:         _saga_on_payment_refunded,
    }
    handler = handlers.get(msg_type)
    if handler:
        handler(msg)
    else:
        _logger.warning(f"[Saga] Unknown event type: {msg_type!r} — dropping")


def _saga_on_stock_reserved(msg: dict) -> None:
    # TODO: read order from Redis, publish PROCESS_PAYMENT, set status = PROCESSING_PAYMENT
    pass


def _saga_on_stock_reservation_failed(msg: dict) -> None:
    # TODO: set status = FAILED (stock untouched, no compensation needed)
    pass


def _saga_on_stock_released(msg: dict) -> None:
    # TODO: set status = FAILED (stock compensation complete)
    pass


def _saga_on_payment_success(msg: dict) -> None:
    # TODO: mark order paid = True in Redis, set status = COMPLETED
    pass


def _saga_on_payment_failed(msg: dict) -> None:
    # TODO: publish RELEASE_STOCK, set status = COMPENSATING
    pass


def _saga_on_payment_refunded(msg: dict) -> None:
    # TODO: set status = FAILED (payment compensation complete)
    pass


# ============================================================
# 2PC
# ============================================================

def _2pc_start_checkout(order_id: str, order_entry) -> None:
    # TODO: publish PREPARE_STOCK + PREPARE_PAYMENT, set status = PREPARING_STOCK
    pass


def _2pc_route(msg: dict, msg_type: str) -> None:
    handlers = {
        STOCK_PREPARED:        _2pc_on_stock_prepared,
        STOCK_PREPARE_FAILED:  _2pc_on_stock_prepare_failed,
        STOCK_COMMITTED:       _2pc_on_stock_committed,
        STOCK_ABORTED:         _2pc_on_stock_aborted,
        PAYMENT_PREPARED:      _2pc_on_payment_prepared,
        PAYMENT_PREPARE_FAILED:_2pc_on_payment_prepare_failed,
        PAYMENT_COMMITTED:     _2pc_on_payment_committed,
        PAYMENT_ABORTED:       _2pc_on_payment_aborted,
    }
    handler = handlers.get(msg_type)
    if handler:
        handler(msg)
    else:
        _logger.warning(f"[2PC] Unknown event type: {msg_type!r} — dropping")


def _2pc_on_stock_prepared(msg: dict) -> None:
    # TODO: if payment also prepared → send COMMIT to both; else wait
    pass


def _2pc_on_stock_prepare_failed(msg: dict) -> None:
    # TODO: send ABORT_STOCK, set status = FAILED
    pass


def _2pc_on_stock_committed(msg: dict) -> None:
    # TODO: if payment also committed → set status = COMPLETED
    pass


def _2pc_on_stock_aborted(msg: dict) -> None:
    # TODO: set status = FAILED
    pass


def _2pc_on_payment_prepared(msg: dict) -> None:
    # TODO: if stock also prepared → send COMMIT to both; else wait
    pass


def _2pc_on_payment_prepare_failed(msg: dict) -> None:
    # TODO: send ABORT_PAYMENT + ABORT_STOCK if needed, set status = FAILED
    pass


def _2pc_on_payment_committed(msg: dict) -> None:
    # TODO: if stock also committed → set status = COMPLETED
    pass


def _2pc_on_payment_aborted(msg: dict) -> None:
    # TODO: set status = FAILED
    pass