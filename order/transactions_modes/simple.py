import uuid
import redis as redis_module
from common.messages import (
    STOCK_COMMANDS_TOPIC,
    PAYMENT_COMMANDS_TOPIC,
    # Saga stock events
    STOCK_RESERVED,
    STOCK_RESERVATION_FAILED,
    STOCK_RELEASED,
    # Saga payment events
    PAYMENT_SUCCESS,
    PAYMENT_FAILED,
    # builders
    build_reserve_stock,
    build_process_payment,
)

from redis_helpers import get_order, set_status
from common.kafka_client import KafkaProducerClient

# ============================================================
# SIMPLE  (no orchestration — Kafka equivalent of the HTTP template)
# ============================================================


def simple_start_checkout(
    producer: KafkaProducerClient,
    db: redis_module.Redis,
    logger,
    order_id: str,
    order_entry,
) -> None:
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

    items = [{"item_id": iid, "quantity": qty} for iid, qty in items_quantities.items()]

    tx_id = str(uuid.uuid4())
    db.set(f"order:{order_id}:tx_id", tx_id)

    msg = build_reserve_stock(tx_id=tx_id, order_id=order_id, items=items)
    producer.publish(STOCK_COMMANDS_TOPIC, msg)
    logger.debug(f"[Simple] order={order_id} tx={tx_id} RESERVE_STOCK published")


def simple_route(
    producer: KafkaProducerClient,
    db: redis_module.Redis,
    logger,
    msg: dict,
    msg_type: str,
) -> None:
    handlers = {
        STOCK_RESERVED: simple_on_stock_reserved,
        STOCK_RESERVATION_FAILED: simple_on_stock_reservation_failed,
        STOCK_RELEASED: simple_on_stock_released,
        PAYMENT_SUCCESS: simple_on_payment_success,
        PAYMENT_FAILED: simple_on_payment_failed,
    }
    handler = handlers.get(msg_type)
    if handler:
        handler(producer, db, logger, msg)
    else:
        logger.warning(f"[Simple] Unknown event type: {msg_type!r} — dropping")


def simple_on_stock_reserved(
    producer: KafkaProducerClient, db: redis_module.Redis, logger, msg: dict
) -> None:
    """Stock reserved — now charge the user."""
    from msgspec import msgpack
    from app import OrderValue

    order_id = msg.get("order_id")
    tx_id = msg.get("tx_id")

    raw = get_order(logger, db, order_id)
    if not raw:
        logger.info(f"[Simple] order {order_id} not found in Redis")
        return

    order = msgpack.decode(raw, type=OrderValue)
    payment_msg = build_process_payment(
        tx_id=tx_id,
        order_id=order_id,
        user_id=order.user_id,
        amount=order.total_cost,
    )

    producer.publish(PAYMENT_COMMANDS_TOPIC, payment_msg)

    logger.info(f"[Simple] order={order_id} PROCESS_PAYMENT published")


def simple_on_stock_reservation_failed(
    producer: KafkaProducerClient, db: redis_module.Redis, logger, msg: dict
) -> None:
    """Stock reservation failed — nothing was reserved, just mark failed."""
    order_id = msg.get("order_id")
    logger.debug(
        f"[Simple] order={order_id} stock reservation failed — no rollback needed"
    )
    set_status(logger, db, order_id, "failed")


def simple_on_stock_released(
    producer: KafkaProducerClient, db: redis_module.Redis, logger, msg: dict
) -> None:
    """Stock successfully released after a payment failure — mark order failed."""
    order_id = msg.get("order_id")
    logger.debug(f"[Simple] order={order_id} stock released — marking failed")
    set_status(logger, db, order_id, "failed")


def simple_on_payment_success(
    producer: KafkaProducerClient, db: redis_module.Redis, logger, msg: dict
) -> None:
    """Payment succeeded — mark order as paid."""
    from msgspec import msgpack
    from app import OrderValue

    order_id = msg.get("order_id")

    raw = get_order(logger, db, order_id)
    if not raw:
        logger.error(f"[Simple] order {order_id} not found in Redis")
        return

    order = msgpack.decode(raw, type=OrderValue)
    order = OrderValue(
        paid=True,
        items=order.items,
        user_id=order.user_id,
        total_cost=order.total_cost,
    )
    db.set(order_id, msgpack.encode(order))
    set_status(logger, db, order_id, "completed")
    logger.debug(f"[Simple] order={order_id} marked paid — COMPLETED")


def simple_on_payment_failed(
    producer: KafkaProducerClient, db: redis_module.Redis, logger, msg: dict
) -> None:
    """Payment failed — release the reserved stock."""
    from msgspec import msgpack
    from app import OrderValue

    order_id = msg.get("order_id")
    tx_id = msg.get("tx_id")

    raw = get_order(logger, db, order_id)
    if not raw:
        logger.error(f"[Simple] order {order_id} not found in Redis")
        return

    order = msgpack.decode(raw, type=OrderValue)
    from collections import defaultdict

    items_quantities: dict = defaultdict(int)
    for item_id, quantity in order.items:
        items_quantities[item_id] += quantity

    items = [{"item_id": iid, "quantity": qty} for iid, qty in items_quantities.items()]

    from common.messages import build_release_stock

    release_msg = build_release_stock(tx_id=tx_id, order_id=order_id, items=items)

    producer.publish(STOCK_COMMANDS_TOPIC, release_msg)

    logger.debug(f"[Simple] order={order_id} payment failed — RELEASE_STOCK published")
