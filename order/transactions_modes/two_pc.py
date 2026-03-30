import uuid
from collections import defaultdict

import redis as redis_module
from msgspec import msgpack

import checkout_notify
from app import OrderValue
from common.messages import (
    STOCK_COMMANDS_TOPIC,
    PAYMENT_COMMANDS_TOPIC,
    TwoPhaseOrderStatus,
    STOCK_PREPARED,
    STOCK_PREPARE_FAILED,
    STOCK_COMMITTED,
    STOCK_ABORTED,
    PAYMENT_PREPARED,
    PAYMENT_PREPARE_FAILED,
    PAYMENT_COMMITTED,
    PAYMENT_ABORTED,
    build_prepare_stock,
    build_prepare_payment,
    build_commit_stock,
    build_commit_payment,
    build_abort_stock,
    build_abort_payment,
    STOCK_UNKNOWN,
    STOCK_READY,
    STOCK_FAILED,
    PAYMENT_UNKNOWN,
    PAYMENT_READY,
    PAYMENT_FAILED,
    STOCK_NOT_COMMITTED,
    STOCK_COMMIT_CONFIRMED,
    STOCK_ABORT_CONFIRMED,
    PAYMENT_NOT_COMMITTED,
    PAYMENT_COMMIT_CONFIRMED,
    PAYMENT_ABORT_CONFIRMED,
    DECISION_NONE,
    DECISION_COMMIT,
    DECISION_ABORT,
)
from redis_helpers import _2pc_key, set_status, get_order

_db: redis_module.Redis | None = None
_publish = None
_logger = None


def init_2pc(db: redis_module.Redis, publish_fn, logger) -> None:
    global _db, _publish, _logger
    _db = db
    _publish = publish_fn
    _logger = logger


def _decode(raw, default=""):
    if raw is None:
        return default
    if isinstance(raw, bytes):
        return raw.decode()
    return str(raw)


def _publish_stock(msg: dict) -> None:
    _publish(STOCK_COMMANDS_TOPIC, msg)


def _publish_payment(msg: dict) -> None:
    _publish(PAYMENT_COMMANDS_TOPIC, msg)


def _mark_paid(order_id: str) -> None:
    raw = get_order(_logger, _db, order_id)
    if not raw:
        return
    order_entry = msgpack.decode(raw, type=OrderValue)
    if not order_entry.paid:
        _db.set(order_id, msgpack.encode(OrderValue(
            paid=True,
            items=order_entry.items,
            user_id=order_entry.user_id,
            total_cost=order_entry.total_cost,
        )))


def _finish(order_id: str, success: bool) -> None:
    if success:
        _mark_paid(order_id)
        set_status(_logger, _db, order_id, TwoPhaseOrderStatus.COMPLETED)
    else:
        set_status(_logger, _db, order_id, TwoPhaseOrderStatus.FAILED)
    checkout_notify.notify(order_id)


def _evaluate_2pc(order_id: str) -> None:
    state = _db.hgetall(_2pc_key(order_id))
    if not state:
        return

    stock_state = _decode(state.get(b"stock_state"), STOCK_UNKNOWN)
    payment_state = _decode(state.get(b"payment_state"), PAYMENT_UNKNOWN)
    decision = _decode(state.get(b"decision"), DECISION_NONE)

    tx_id = _decode(_db.get(f"order:{order_id}:tx_id"))
    if not tx_id:
        _logger.warning(f"[Order2PC] order={order_id} missing tx_id")
        return

    if decision != DECISION_NONE:
        return

    if stock_state == STOCK_FAILED or payment_state == PAYMENT_FAILED:
        _logger.info(f"[Order2PC] order={order_id} ABORT")
        _db.hset(_2pc_key(order_id), mapping={"decision": DECISION_ABORT})
        set_status(_logger, _db, order_id, TwoPhaseOrderStatus.ABORTING)
        _publish_stock(build_abort_stock(tx_id, order_id))
        _publish_payment(build_abort_payment(tx_id, order_id))
        return

    if stock_state == STOCK_READY and payment_state == PAYMENT_READY:
        _logger.info(f"[Order2PC] order={order_id} COMMIT")
        _db.hset(_2pc_key(order_id), mapping={"decision": DECISION_COMMIT})
        set_status(_logger, _db, order_id, TwoPhaseOrderStatus.COMMITTING)
        _publish_stock(build_commit_stock(tx_id, order_id))
        _publish_payment(build_commit_payment(tx_id, order_id))


def recover_incomplete_2pc() -> None:
    for key in _db.scan_iter("order:*:2pcstate"):
        order_id = _decode(key).split(":")[1]
        state = _db.hgetall(key)
        decision = _decode(state.get(b"decision"), DECISION_NONE)
        tx_id = _decode(_db.get(f"order:{order_id}:tx_id"))
        if not tx_id:
            continue
        if decision == DECISION_NONE:
            _evaluate_2pc(order_id)
        elif decision == DECISION_COMMIT:
            if _decode(state.get(b"stock_commit_state"), STOCK_NOT_COMMITTED) != STOCK_COMMIT_CONFIRMED:
                _publish_stock(build_commit_stock(tx_id, order_id))
            if _decode(state.get(b"payment_commit_state"), PAYMENT_NOT_COMMITTED) != PAYMENT_COMMIT_CONFIRMED:
                _publish_payment(build_commit_payment(tx_id, order_id))
        elif decision == DECISION_ABORT:
            if _decode(state.get(b"stock_commit_state"), STOCK_NOT_COMMITTED) != STOCK_ABORT_CONFIRMED:
                _publish_stock(build_abort_stock(tx_id, order_id))
            if _decode(state.get(b"payment_commit_state"), PAYMENT_NOT_COMMITTED) != PAYMENT_ABORT_CONFIRMED:
                _publish_payment(build_abort_payment(tx_id, order_id))


def _2pc_start_checkout(_producer_unused, db: redis_module.Redis, logger, order_id: str, order_entry) -> None:
    # keep signature compatible with existing call site
    if _db is None:
        init_2pc(db, _publish, logger)

    items_quantities: dict[str, int] = defaultdict(int)
    for item_id, quantity in order_entry.items:
        items_quantities[str(item_id)] += int(quantity)
    items = [{"item_id": iid, "quantity": qty} for iid, qty in items_quantities.items()]

    tx_id = str(uuid.uuid4())
    _db.set(f"order:{order_id}:tx_id", tx_id)
    _db.hset(
        _2pc_key(order_id),
        mapping={
            "stock_state": STOCK_UNKNOWN,
            "payment_state": PAYMENT_UNKNOWN,
            "decision": DECISION_NONE,
            "stock_commit_state": STOCK_NOT_COMMITTED,
            "payment_commit_state": PAYMENT_NOT_COMMITTED,
        },
    )
    set_status(_logger, _db, order_id, TwoPhaseOrderStatus.PREPARING_STOCK)

    _publish_stock(build_prepare_stock(tx_id, order_id, items))
    _publish_payment(build_prepare_payment(tx_id, order_id, order_entry.user_id, order_entry.total_cost))


def _handle_prepare_result(msg: dict, key: str, ready_value: str, failed_value: str, preparing_status: str) -> None:
    order_id = msg.get("order_id")
    tx_id = msg.get("tx_id")
    if not order_id or not tx_id:
        return
    event_type = msg.get("type")
    new_state = ready_value if event_type in {STOCK_PREPARED, PAYMENT_PREPARED} else failed_value
    _db.hset(_2pc_key(order_id), mapping={key: new_state})
    if event_type in {PAYMENT_PREPARED}:
        set_status(_logger, _db, order_id, TwoPhaseOrderStatus.PREPARING_PAYMENT)
    elif event_type in {STOCK_PREPARED}:
        set_status(_logger, _db, order_id, preparing_status)
    _evaluate_2pc(order_id)


def _handle_commit_confirmation(msg: dict, key: str, confirmed_value: str, abort_value: str | None = None) -> None:
    order_id = msg.get("order_id")
    if not order_id:
        return
    event_type = msg.get("type")
    if event_type in {STOCK_COMMITTED, PAYMENT_COMMITTED}:
        _db.hset(_2pc_key(order_id), mapping={key: confirmed_value})
    else:
        _db.hset(_2pc_key(order_id), mapping={key: abort_value})

    state = _db.hgetall(_2pc_key(order_id))
    decision = _decode(state.get(b"decision"), DECISION_NONE)
    stock_commit = _decode(state.get(b"stock_commit_state"), STOCK_NOT_COMMITTED)
    payment_commit = _decode(state.get(b"payment_commit_state"), PAYMENT_NOT_COMMITTED)

    if decision == DECISION_COMMIT and stock_commit == STOCK_COMMIT_CONFIRMED and payment_commit == PAYMENT_COMMIT_CONFIRMED:
        _finish(order_id, True)
    elif decision == DECISION_ABORT and stock_commit == STOCK_ABORT_CONFIRMED and payment_commit == PAYMENT_ABORT_CONFIRMED:
        _finish(order_id, False)


def _2pc_route_order(msg: dict) -> None:
    msg_type = msg.get("type")
    if msg_type == STOCK_PREPARED:
        _db.hset(_2pc_key(msg["order_id"]), mapping={"stock_state": STOCK_READY})
        _evaluate_2pc(msg["order_id"])
    elif msg_type == STOCK_PREPARE_FAILED:
        _db.hset(_2pc_key(msg["order_id"]), mapping={"stock_state": STOCK_FAILED})
        _evaluate_2pc(msg["order_id"])
    elif msg_type == PAYMENT_PREPARED:
        _db.hset(_2pc_key(msg["order_id"]), mapping={"payment_state": PAYMENT_READY})
        _evaluate_2pc(msg["order_id"])
    elif msg_type == PAYMENT_PREPARE_FAILED:
        _db.hset(_2pc_key(msg["order_id"]), mapping={"payment_state": PAYMENT_FAILED})
        _evaluate_2pc(msg["order_id"])
    elif msg_type == STOCK_COMMITTED:
        _handle_commit_confirmation(msg, "stock_commit_state", STOCK_COMMIT_CONFIRMED, STOCK_ABORT_CONFIRMED)
    elif msg_type == PAYMENT_COMMITTED:
        _handle_commit_confirmation(msg, "payment_commit_state", PAYMENT_COMMIT_CONFIRMED, PAYMENT_ABORT_CONFIRMED)
    elif msg_type == STOCK_ABORTED:
        _handle_commit_confirmation(msg, "stock_commit_state", STOCK_COMMIT_CONFIRMED, STOCK_ABORT_CONFIRMED)
    elif msg_type == PAYMENT_ABORTED:
        _handle_commit_confirmation(msg, "payment_commit_state", PAYMENT_COMMIT_CONFIRMED, PAYMENT_ABORT_CONFIRMED)
    else:
        _logger.info(f"[Order2PC] Unknown event type: {msg_type!r}")
