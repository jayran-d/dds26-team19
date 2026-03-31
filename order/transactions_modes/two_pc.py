import uuid
from collections import defaultdict

import redis as redis_module

import checkout_notify
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
from redis_helpers import _2pc_key, set_status

_db: redis_module.Redis | None = None
_publish = None
_logger = None
_participant_update_script = None
_commit_state_update_script = None


_PARTICIPANT_UPDATE_LUA = """
local state_key = KEYS[1]
local status_key = KEYS[2]
local tx_key = KEYS[3]

local participant_field = ARGV[1]
local participant_value = ARGV[2]
local committing_status = ARGV[3]
local aborting_status = ARGV[4]
local decision_none = ARGV[5]
local stock_ready = ARGV[6]
local payment_ready = ARGV[7]
local stock_failed = ARGV[8]
local payment_failed = ARGV[9]
local decision_commit = ARGV[10]
local decision_abort = ARGV[11]

redis.call('HSET', state_key, participant_field, participant_value)

local tx_id = redis.call('GET', tx_key)
if not tx_id then
    return {'missing_tx_id', '', ''}
end

local decision = redis.call('HGET', state_key, 'decision') or decision_none
if decision ~= decision_none then
    return {'noop', tx_id, decision}
end

local stock_state = redis.call('HGET', state_key, 'stock_state') or ''
local payment_state = redis.call('HGET', state_key, 'payment_state') or ''

if stock_state == stock_failed or payment_state == payment_failed then
    redis.call('HSET', state_key, 'decision', decision_abort)
    redis.call('SET', status_key, aborting_status)
    return {'publish_abort', tx_id, ''}
end

if stock_state == stock_ready and payment_state == payment_ready then
    redis.call('HSET', state_key, 'decision', decision_commit)
    redis.call('SET', status_key, committing_status)
    return {'publish_commit', tx_id, ''}
end

return {'wait', tx_id, ''}
"""


_COMMIT_STATE_UPDATE_LUA = """
local state_key = KEYS[1]

local commit_field = ARGV[1]
local commit_value = ARGV[2]
local decision_commit = ARGV[3]
local decision_abort = ARGV[4]
local stock_commit_confirmed = ARGV[5]
local payment_commit_confirmed = ARGV[6]
local stock_abort_confirmed = ARGV[7]
local payment_abort_confirmed = ARGV[8]

redis.call('HSET', state_key, commit_field, commit_value)

local decision = redis.call('HGET', state_key, 'decision') or ''
local stock_commit_state = redis.call('HGET', state_key, 'stock_commit_state') or ''
local payment_commit_state = redis.call('HGET', state_key, 'payment_commit_state') or ''

if decision == decision_commit and stock_commit_state == stock_commit_confirmed and payment_commit_state == payment_commit_confirmed then
    return 'finish_success'
end

if decision == decision_abort and stock_commit_state == stock_abort_confirmed and payment_commit_state == payment_abort_confirmed then
    return 'finish_failed'
end

return 'wait'
"""


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


def _get_participant_update_script():
    global _participant_update_script
    if _participant_update_script is None:
        _participant_update_script = _db.register_script(_PARTICIPANT_UPDATE_LUA)
    return _participant_update_script


def _get_commit_state_update_script():
    global _commit_state_update_script
    if _commit_state_update_script is None:
        _commit_state_update_script = _db.register_script(_COMMIT_STATE_UPDATE_LUA)
    return _commit_state_update_script


def _finish(order_id: str, success: bool) -> None:
    if success:
        set_status(_logger, _db, order_id, TwoPhaseOrderStatus.COMPLETED)
    else:
        set_status(_logger, _db, order_id, TwoPhaseOrderStatus.FAILED)
    checkout_notify.notify(order_id)


def _evaluate_2pc(order_id: str) -> None:
    state_key = _2pc_key(order_id)
    state = _db.hmget(
        state_key,
        "stock_state",
        "payment_state",
        "decision",
    )
    if not state:
        return

    stock_state = _decode(state[0], STOCK_UNKNOWN)
    payment_state = _decode(state[1], PAYMENT_UNKNOWN)
    decision = _decode(state[2], DECISION_NONE)

    tx_id = _decode(_db.get(f"order:{order_id}:tx_id"))
    if not tx_id:
        _logger.warning(f"[Order2PC] order={order_id} missing tx_id")
        return

    if decision != DECISION_NONE:
        return

    if stock_state == STOCK_FAILED or payment_state == PAYMENT_FAILED:
        _logger.debug(f"[Order2PC] order={order_id} ABORT")
        pipe = _db.pipeline(transaction=False)
        pipe.hset(state_key, mapping={"decision": DECISION_ABORT})
        pipe.set(f"order:{order_id}:status", TwoPhaseOrderStatus.ABORTING)
        pipe.execute()
        _publish_stock(build_abort_stock(tx_id, order_id))
        _publish_payment(build_abort_payment(tx_id, order_id))
        return

    if stock_state == STOCK_READY and payment_state == PAYMENT_READY:
        _logger.debug(f"[Order2PC] order={order_id} COMMIT")
        pipe = _db.pipeline(transaction=False)
        pipe.hset(state_key, mapping={"decision": DECISION_COMMIT})
        pipe.set(f"order:{order_id}:status", TwoPhaseOrderStatus.COMMITTING)
        pipe.execute()
        _publish_stock(build_commit_stock(tx_id, order_id))
        _publish_payment(build_commit_payment(tx_id, order_id))


def _update_participant_and_maybe_decide(
    order_id: str,
    participant_field: str,
    participant_state: str,
) -> tuple[str, str]:
    result = _get_participant_update_script()(
        keys=[
            _2pc_key(order_id),
            f"order:{order_id}:status",
            f"order:{order_id}:tx_id",
        ],
        args=[
            participant_field,
            participant_state,
            TwoPhaseOrderStatus.COMMITTING,
            TwoPhaseOrderStatus.ABORTING,
            DECISION_NONE,
            STOCK_READY,
            PAYMENT_READY,
            STOCK_FAILED,
            PAYMENT_FAILED,
            DECISION_COMMIT,
            DECISION_ABORT,
        ],
        client=_db,
    )
    action = _decode(result[0])
    tx_id = _decode(result[1])
    return action, tx_id


def recover_incomplete_2pc() -> None:
    for key in _db.scan_iter("order:*:2pcstate"):
        order_id = _decode(key).split(":")[1]
        decision = _decode(_db.hget(key, "decision"), DECISION_NONE)
        tx_id = _decode(_db.get(f"order:{order_id}:tx_id"))
        if not tx_id:
            continue
        if decision == DECISION_NONE:
            _evaluate_2pc(order_id)
        elif decision == DECISION_COMMIT:
            if _decode(_db.hget(key, "stock_commit_state"), STOCK_NOT_COMMITTED) != STOCK_COMMIT_CONFIRMED:
                _publish_stock(build_commit_stock(tx_id, order_id))
            if _decode(_db.hget(key, "payment_commit_state"), PAYMENT_NOT_COMMITTED) != PAYMENT_COMMIT_CONFIRMED:
                _publish_payment(build_commit_payment(tx_id, order_id))
        elif decision == DECISION_ABORT:
            if _decode(_db.hget(key, "stock_commit_state"), STOCK_NOT_COMMITTED) != STOCK_ABORT_CONFIRMED:
                _publish_stock(build_abort_stock(tx_id, order_id))
            if _decode(_db.hget(key, "payment_commit_state"), PAYMENT_NOT_COMMITTED) != PAYMENT_ABORT_CONFIRMED:
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
    pipe = _db.pipeline(transaction=False)
    pipe.set(f"order:{order_id}:tx_id", tx_id)
    pipe.hset(
        _2pc_key(order_id),
        mapping={
            "stock_state": STOCK_UNKNOWN,
            "payment_state": PAYMENT_UNKNOWN,
            "decision": DECISION_NONE,
            "stock_commit_state": STOCK_NOT_COMMITTED,
            "payment_commit_state": PAYMENT_NOT_COMMITTED,
        },
    )
    pipe.set(f"order:{order_id}:status", TwoPhaseOrderStatus.PREPARING_STOCK)
    pipe.execute()

    _publish_stock(build_prepare_stock(tx_id, order_id, items))
    _publish_payment(build_prepare_payment(tx_id, order_id, order_entry.user_id, order_entry.total_cost))


def _handle_commit_confirmation(msg: dict, key: str, confirmed_value: str, abort_value: str | None = None) -> None:
    order_id = msg.get("order_id")
    if not order_id:
        return
    event_type = msg.get("type")
    state_key = _2pc_key(order_id)
    new_value = confirmed_value if event_type in {STOCK_COMMITTED, PAYMENT_COMMITTED} else abort_value
    outcome = _decode(
        _get_commit_state_update_script()(
            keys=[state_key],
            args=[
                key,
                new_value,
                DECISION_COMMIT,
                DECISION_ABORT,
                STOCK_COMMIT_CONFIRMED,
                PAYMENT_COMMIT_CONFIRMED,
                STOCK_ABORT_CONFIRMED,
                PAYMENT_ABORT_CONFIRMED,
            ],
            client=_db,
        )
    )

    if outcome == "finish_success":
        _finish(order_id, True)
    elif outcome == "finish_failed":
        _finish(order_id, False)


def _2pc_route_order(msg: dict) -> None:
    msg_type = msg.get("type")
    if msg_type == STOCK_PREPARED:
        action, tx_id = _update_participant_and_maybe_decide(msg["order_id"], "stock_state", STOCK_READY)
        if action == "publish_commit":
            _logger.debug(f"[Order2PC] order={msg['order_id']} COMMIT")
            _publish_stock(build_commit_stock(tx_id, msg["order_id"]))
            _publish_payment(build_commit_payment(tx_id, msg["order_id"]))
        elif action == "publish_abort":
            _logger.debug(f"[Order2PC] order={msg['order_id']} ABORT")
            _publish_stock(build_abort_stock(tx_id, msg["order_id"]))
            _publish_payment(build_abort_payment(tx_id, msg["order_id"]))
        elif action == "missing_tx_id":
            _logger.warning(f"[Order2PC] order={msg['order_id']} missing tx_id")
    elif msg_type == STOCK_PREPARE_FAILED:
        action, tx_id = _update_participant_and_maybe_decide(msg["order_id"], "stock_state", STOCK_FAILED)
        if action == "publish_abort":
            _logger.debug(f"[Order2PC] order={msg['order_id']} ABORT")
            _publish_stock(build_abort_stock(tx_id, msg["order_id"]))
            _publish_payment(build_abort_payment(tx_id, msg["order_id"]))
        elif action == "missing_tx_id":
            _logger.warning(f"[Order2PC] order={msg['order_id']} missing tx_id")
    elif msg_type == PAYMENT_PREPARED:
        action, tx_id = _update_participant_and_maybe_decide(msg["order_id"], "payment_state", PAYMENT_READY)
        if action == "publish_commit":
            _logger.debug(f"[Order2PC] order={msg['order_id']} COMMIT")
            _publish_stock(build_commit_stock(tx_id, msg["order_id"]))
            _publish_payment(build_commit_payment(tx_id, msg["order_id"]))
        elif action == "publish_abort":
            _logger.debug(f"[Order2PC] order={msg['order_id']} ABORT")
            _publish_stock(build_abort_stock(tx_id, msg["order_id"]))
            _publish_payment(build_abort_payment(tx_id, msg["order_id"]))
        elif action == "missing_tx_id":
            _logger.warning(f"[Order2PC] order={msg['order_id']} missing tx_id")
    elif msg_type == PAYMENT_PREPARE_FAILED:
        action, tx_id = _update_participant_and_maybe_decide(msg["order_id"], "payment_state", PAYMENT_FAILED)
        if action == "publish_abort":
            _logger.debug(f"[Order2PC] order={msg['order_id']} ABORT")
            _publish_stock(build_abort_stock(tx_id, msg["order_id"]))
            _publish_payment(build_abort_payment(tx_id, msg["order_id"]))
        elif action == "missing_tx_id":
            _logger.warning(f"[Order2PC] order={msg['order_id']} missing tx_id")
    elif msg_type == STOCK_COMMITTED:
        _handle_commit_confirmation(msg, "stock_commit_state", STOCK_COMMIT_CONFIRMED, STOCK_ABORT_CONFIRMED)
    elif msg_type == PAYMENT_COMMITTED:
        _handle_commit_confirmation(msg, "payment_commit_state", PAYMENT_COMMIT_CONFIRMED, PAYMENT_ABORT_CONFIRMED)
    elif msg_type == STOCK_ABORTED:
        _handle_commit_confirmation(msg, "stock_commit_state", STOCK_COMMIT_CONFIRMED, STOCK_ABORT_CONFIRMED)
    elif msg_type == PAYMENT_ABORTED:
        _handle_commit_confirmation(msg, "payment_commit_state", PAYMENT_COMMIT_CONFIRMED, PAYMENT_ABORT_CONFIRMED)
    else:
        _logger.debug(f"[Order2PC] Unknown event type: {msg_type!r}")
