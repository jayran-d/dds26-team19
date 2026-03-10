import uuid
import redis as redis_module
from common.messages import (
    PAYMENT_ABORTED,
    PAYMENT_COMMITTED,
    PAYMENT_PREPARE_FAILED,
    PAYMENT_PREPARED,
    STOCK_ABORTED,
    STOCK_COMMANDS_TOPIC,
    PAYMENT_COMMANDS_TOPIC,
    STOCK_COMMITTED,
    STOCK_PREPARE_FAILED,
    STOCK_PREPARED,
    # Saga stock events
    STOCK_RESERVED,
    STOCK_RESERVATION_FAILED,
    STOCK_RELEASED,
    # Saga payment events
    PAYMENT_SUCCESS,
    PAYMENT_FAILED,
    # 2PC
    PREPARE_PAYMENT,
    PREPARE_STOCK,
    # builders
    build_reserve_stock,
    build_prepare_stock,
    build_prepare_payment,
    build_process_payment,
)

from redis_helpers import get_order, set_status
from common.kafka_client import KafkaProducerClient

# ============================================================
# 2PC
# ============================================================

def _2pc_start_checkout( 
    producer: KafkaProducerClient,
    db: redis_module.Redis,
    logger,
    order_id: str,
    order_entry) -> None:

    # TODO: publish PREPARE_STOCK + PREPARE_PAYMENT, set status = PREPARING_STOCK
    from collections import defaultdict
    from msgspec import msgpack
    from app import OrderValue

    items_quantities: dict = defaultdict(int)
    for item_id, quantity in order_entry.items:
        items_quantities[item_id] += quantity

    items = [{"item_id": iid, "quantity": qty} for iid, qty in items_quantities.items()]

    tx_id = str(uuid.uuid4())
    db.set(f"order:{order_id}:tx_id", tx_id)

    prepare_stock_msg = build_prepare_stock(tx_id=tx_id, order_id=order_id, items=items)
    producer.publish(STOCK_COMMANDS_TOPIC, prepare_stock_msg)
    logger.info(f"[Order2PC] order={order_id} tx={tx_id} PREPARE_STOCK published")

    raw = get_order(logger, db, order_id)
    if not raw:
        logger.info(f"[Order2PC] order {order_id} not found in Redis")
        return

    order = msgpack.decode(raw, type=OrderValue)
    payment_msg = build_prepare_payment(tx_id, order_id, user_id=order.user_id, amount=order.total_cost)
    producer.publish(PAYMENT_COMMANDS_TOPIC, payment_msg)

    logger.info(f"[Order2PC] order={order_id} tx={tx_id} PREPARE_PAYMENT published - user_id={order.user_id} amount={order.total_cost}")


def _2pc_route_order(producer: KafkaProducerClient,
    db: redis_module.Redis,
    logger,
    msg: dict,
    msg_type: str,) -> None:
    pass

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
        handler(producer, db, logger, msg)
    else:
        logger.info(f"[Route-Order2PC] Unknown event type: {msg_type!r} — dropping")


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