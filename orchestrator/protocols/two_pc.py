"""
orchestrator/protocols/two_pc.py

Two-Phase Commit coordinator — runs inside the orchestrator service.

Coordination state lives in coord_db (orchestrator-db).
Order status is written to order_db (order-db) so order-service can poll.
"""

import json
import uuid
from collections import defaultdict

import redis as redis_module

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


def _2pc_key(order_id: str) -> str:
    return f"order:{order_id}:2pcstate"


def _active_key(order_id: str) -> str:
    return f"2pc:active:{order_id}"


def _status_key(order_id: str) -> str:
    return f"order:{order_id}:status"


def _tx_key(order_id: str) -> str:
    return f"order:{order_id}:tx_id"


def _incomplete_key() -> str:
    # Recovery iterates this set instead of scanning every historical 2PC state key.
    return "2pc:incomplete"


_coord_db: redis_module.Redis | None = None
_order_db: redis_module.Redis | None = None
_publish = None
_logger = None
_participant_update_script = None
_commit_state_update_script = None

TERMINAL_STATUSES = {
    TwoPhaseOrderStatus.COMPLETED,
    TwoPhaseOrderStatus.FAILED,
}

IN_PROGRESS_STATUSES = {
    TwoPhaseOrderStatus.PREPARING_STOCK,
    TwoPhaseOrderStatus.PREPARING_PAYMENT,
    TwoPhaseOrderStatus.COMMITTING,
    TwoPhaseOrderStatus.ABORTING,
}


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


def init_2pc(coord_db: redis_module.Redis, order_db: redis_module.Redis, publish_fn, logger) -> None:
    global _coord_db, _order_db, _publish, _logger
    _coord_db = coord_db
    _order_db = order_db
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
        _participant_update_script = _coord_db.register_script(_PARTICIPANT_UPDATE_LUA)
    return _participant_update_script


def _get_commit_state_update_script():
    global _commit_state_update_script
    if _commit_state_update_script is None:
        _commit_state_update_script = _coord_db.register_script(_COMMIT_STATE_UPDATE_LUA)
    return _commit_state_update_script


def _write_order_status(order_id: str, status: str) -> None:
    if _coord_db is not None:
        try:
            _coord_db.set(_status_key(order_id), status)
        except Exception:
            pass
    if _order_db is not None:
        try:
            _order_db.set(_status_key(order_id), status)
        except Exception:
            pass


def _get_active_tx_id(order_id: str) -> str | None:
    if _coord_db is None:
        return None
    try:
        raw = _coord_db.get(_active_key(order_id))
        return _decode(raw) if raw else None
    except Exception:
        return None


def _clear_active_tx_id(order_id: str, tx_id: str) -> None:
    if _coord_db is None:
        return
    try:
        _coord_db.eval(
            """
            if redis.call('GET', KEYS[1]) == ARGV[1] then
                return redis.call('DEL', KEYS[1])
            end
            return 0
            """,
            1,
            _active_key(order_id),
            tx_id,
        )
    except Exception:
        pass


def _clear_incomplete(order_id: str) -> None:
    if _coord_db is None:
        return
    try:
        _coord_db.srem(_incomplete_key(), order_id)
    except Exception:
        pass


def _decode_items(raw: bytes | str | None) -> list[dict]:
    if raw is None:
        return []
    try:
        if isinstance(raw, bytes):
            raw = raw.decode()
        return json.loads(raw)
    except Exception:
        return []


def _create_if_no_active(
    order_id: str,
    tx_id: str,
    initial_status: str,
    state_mapping: dict[str, str],
) -> tuple[bool, str | None]:
    active_key = _active_key(order_id)
    status_key = _status_key(order_id)
    tx_key = _tx_key(order_id)

    while True:
        pipe = _coord_db.pipeline()
        try:
            pipe.watch(active_key, status_key, tx_key)

            raw_active = pipe.get(active_key)
            raw_status = pipe.get(status_key)
            raw_current_tx = pipe.get(tx_key)

            current_status = _decode(raw_status)
            current_tx_id = _decode(raw_current_tx)

            if raw_active:
                active_tx_id = _decode(raw_active)
                if current_status not in TERMINAL_STATUSES:
                    pipe.unwatch()
                    return False, active_tx_id
            elif current_status in IN_PROGRESS_STATUSES and current_tx_id:
                # If the active marker was lost but durable order/2PC state says
                # checkout is still running, rebuild the marker instead of
                # starting a second coordinator instance for the same order.
                pipe.multi()
                pipe.set(active_key, current_tx_id)
                # Restore the recovery index if a previous crash left only the durable state behind.
                pipe.sadd(_incomplete_key(), order_id)
                pipe.execute()
                return False, current_tx_id

            pipe.multi()
            pipe.set(active_key, tx_id)
            pipe.set(tx_key, tx_id)
            pipe.hset(_2pc_key(order_id), mapping=state_mapping)
            pipe.set(status_key, initial_status)
            # New transactions become recoverable as part of the same atomic write.
            pipe.sadd(_incomplete_key(), order_id)
            pipe.execute()
            return True, None

        except redis_module.exceptions.WatchError:
            continue
        except Exception:
            return False, None
        finally:
            pipe.reset()


def _finish(order_id: str, success: bool) -> None:
    status = TwoPhaseOrderStatus.COMPLETED if success else TwoPhaseOrderStatus.FAILED
    _write_order_status(order_id, status)
    tx_id = _decode(_coord_db.get(_tx_key(order_id)))
    if tx_id:
        _clear_active_tx_id(order_id, tx_id)
    _clear_incomplete(order_id)


def _update_participant_and_maybe_decide(
    order_id: str,
    participant_field: str,
    participant_state: str,
) -> tuple[str, str]:
    # The Lua script reads/writes 2pc state from coord_db.
    # Status key (KEYS[2]) writes to coord_db too — we then mirror it to order_db.
    result = _get_participant_update_script()(
        keys=[
            _2pc_key(order_id),
            _status_key(order_id),
            _tx_key(order_id),
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
        client=_coord_db,
    )
    action = _decode(result[0])
    tx_id  = _decode(result[1])
    # Mirror status to order_db when a decision is made
    if action == "publish_commit":
        _write_order_status(order_id, TwoPhaseOrderStatus.COMMITTING)
    elif action == "publish_abort":
        _write_order_status(order_id, TwoPhaseOrderStatus.ABORTING)
    return action, tx_id


def recover_incomplete_2pc() -> None:
    # Only unfinished orders stay in 2pc:incomplete, so recovery work stays bounded.
    try:
        raw_order_ids = _coord_db.smembers(_incomplete_key())
    except Exception:
        return

    for raw_order_id in raw_order_ids:
        order_id = _decode(raw_order_id)
        key = _2pc_key(order_id)
        decision = _decode(_coord_db.hget(key, "decision"), DECISION_NONE)
        tx_id = _decode(_coord_db.get(_tx_key(order_id)))
        status = _decode(_coord_db.get(_status_key(order_id)))
        if not _coord_db.exists(key):
            _clear_incomplete(order_id)
            continue
        if not tx_id:
            if status in TERMINAL_STATUSES:
                _clear_incomplete(order_id)
            continue
        if status in TERMINAL_STATUSES:
            _clear_active_tx_id(order_id, tx_id)
            _clear_incomplete(order_id)
            continue
        else:
            try:
                _coord_db.set(_active_key(order_id), tx_id)
                _coord_db.sadd(_incomplete_key(), order_id)
            except Exception:
                pass
        if decision == DECISION_NONE:
            # A coordinator crash can happen before either PREPARE command was
            # actually delivered. Rebuild those messages from durable inputs.
            stock_state = _decode(_coord_db.hget(key, "stock_state"), STOCK_UNKNOWN)
            payment_state = _decode(_coord_db.hget(key, "payment_state"), PAYMENT_UNKNOWN)
            if stock_state == STOCK_UNKNOWN:
                items = _decode_items(_coord_db.hget(key, "items_json"))
                if items:
                    _publish_stock(build_prepare_stock(tx_id, order_id, items))
            if payment_state == PAYMENT_UNKNOWN:
                user_id = _decode(_coord_db.hget(key, "user_id"))
                amount = _decode(_coord_db.hget(key, "amount"))
                if user_id and amount:
                    _publish_payment(build_prepare_payment(tx_id, order_id, user_id, int(amount)))
            _evaluate_2pc(order_id)
        elif decision == DECISION_COMMIT:
            if _decode(_coord_db.hget(key, "stock_commit_state"), STOCK_NOT_COMMITTED) != STOCK_COMMIT_CONFIRMED:
                _publish_stock(build_commit_stock(tx_id, order_id))
            if _decode(_coord_db.hget(key, "payment_commit_state"), PAYMENT_NOT_COMMITTED) != PAYMENT_COMMIT_CONFIRMED:
                _publish_payment(build_commit_payment(tx_id, order_id))
        elif decision == DECISION_ABORT:
            if _decode(_coord_db.hget(key, "stock_commit_state"), STOCK_NOT_COMMITTED) != STOCK_ABORT_CONFIRMED:
                _publish_stock(build_abort_stock(tx_id, order_id))
            if _decode(_coord_db.hget(key, "payment_commit_state"), PAYMENT_NOT_COMMITTED) != PAYMENT_ABORT_CONFIRMED:
                _publish_payment(build_abort_payment(tx_id, order_id))


def _evaluate_2pc(order_id: str) -> None:
    state_key = _2pc_key(order_id)
    state = _coord_db.hmget(state_key, "stock_state", "payment_state", "decision")
    if not state:
        return

    stock_state   = _decode(state[0], STOCK_UNKNOWN)
    payment_state = _decode(state[1], PAYMENT_UNKNOWN)
    decision      = _decode(state[2], DECISION_NONE)

    tx_id = _decode(_coord_db.get(_tx_key(order_id)))
    if not tx_id:
        _logger.warning(f"[Order2PC] order={order_id} missing tx_id")
        return

    if decision != DECISION_NONE:
        return

    if stock_state == STOCK_FAILED or payment_state == PAYMENT_FAILED:
        pipe = _coord_db.pipeline(transaction=False)
        pipe.hset(state_key, mapping={"decision": DECISION_ABORT})
        pipe.set(_status_key(order_id), TwoPhaseOrderStatus.ABORTING)
        pipe.execute()
        _write_order_status(order_id, TwoPhaseOrderStatus.ABORTING)
        _publish_stock(build_abort_stock(tx_id, order_id))
        _publish_payment(build_abort_payment(tx_id, order_id))
        return

    if stock_state == STOCK_READY and payment_state == PAYMENT_READY:
        pipe = _coord_db.pipeline(transaction=False)
        pipe.hset(state_key, mapping={"decision": DECISION_COMMIT})
        pipe.set(_status_key(order_id), TwoPhaseOrderStatus.COMMITTING)
        pipe.execute()
        _write_order_status(order_id, TwoPhaseOrderStatus.COMMITTING)
        _publish_stock(build_commit_stock(tx_id, order_id))
        _publish_payment(build_commit_payment(tx_id, order_id))


def _handle_commit_confirmation(msg: dict, key: str, confirmed_value: str, abort_value: str | None = None) -> None:
    order_id = msg.get("order_id")
    if not order_id:
        return
    event_type = msg.get("type")
    state_key  = _2pc_key(order_id)
    new_value  = confirmed_value if event_type in {STOCK_COMMITTED, PAYMENT_COMMITTED} else abort_value
    outcome    = _decode(
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
            client=_coord_db,
        )
    )

    if outcome == "finish_success":
        _finish(order_id, True)
    elif outcome == "finish_failed":
        _finish(order_id, False)


def _2pc_start_checkout(coord_db: redis_module.Redis, logger, order_id: str, user_id: str, total_cost: int, items) -> dict:
    items_quantities: dict[str, int] = defaultdict(int)
    for item in items:
        if isinstance(item, (list, tuple)) and len(item) == 2:
            iid, qty = str(item[0]), int(item[1])
        elif isinstance(item, dict):
            iid, qty = str(item["item_id"]), int(item["quantity"])
        else:
            continue
        items_quantities[iid] += qty
    canonical_items = [{"item_id": iid, "quantity": qty} for iid, qty in items_quantities.items()]

    tx_id = str(uuid.uuid4())
    ok, active_tx_id = _create_if_no_active(
        order_id=order_id,
        tx_id=tx_id,
        initial_status=TwoPhaseOrderStatus.PREPARING_STOCK,
        state_mapping={
            "stock_state": STOCK_UNKNOWN,
            "payment_state": PAYMENT_UNKNOWN,
            "decision": DECISION_NONE,
            "stock_commit_state": STOCK_NOT_COMMITTED,
            "payment_commit_state": PAYMENT_NOT_COMMITTED,
            "user_id": str(user_id),
            "amount": str(int(total_cost)),
            "items_json": json.dumps(canonical_items),
        },
    )

    if not ok:
        if active_tx_id:
            logger.debug(f"[Order2PC] already in progress order={order_id} active_tx={active_tx_id}")
            return {"started": False, "reason": "already_in_progress", "tx_id": active_tx_id}
        logger.error(f"[Order2PC] failed to create state for order={order_id}")
        return {"started": False, "reason": "error"}

    # Mirror initial status to order-db for polling
    _write_order_status(order_id, TwoPhaseOrderStatus.PREPARING_STOCK)
    if _order_db is not None:
        try:
            _order_db.set(_tx_key(order_id), tx_id)
        except Exception:
            pass

    _publish_stock(build_prepare_stock(tx_id, order_id, canonical_items))
    _publish_payment(build_prepare_payment(tx_id, order_id, str(user_id), int(total_cost)))
    return {"started": True, "reason": "started", "tx_id": tx_id}


def _2pc_route_order(msg: dict) -> None:
    order_id = msg.get("order_id")
    tx_id = msg.get("tx_id")
    if not order_id or not tx_id:
        _logger.warning(f"[Order2PC] malformed event: {msg}")
        return

    active_tx_id = _get_active_tx_id(order_id)
    if active_tx_id != tx_id:
        _logger.debug(
            f"[Order2PC] stale tx_id={tx_id} for order={order_id} active_tx={active_tx_id!r}"
        )
        return

    msg_type = msg.get("type")
    if msg_type == STOCK_PREPARED:
        action, tx_id = _update_participant_and_maybe_decide(order_id, "stock_state", STOCK_READY)
        if action == "publish_commit":
            _logger.debug(f"[Order2PC] order={order_id} COMMIT")
            _publish_stock(build_commit_stock(tx_id, order_id))
            _publish_payment(build_commit_payment(tx_id, order_id))
        elif action == "publish_abort":
            _logger.debug(f"[Order2PC] order={order_id} ABORT")
            _publish_stock(build_abort_stock(tx_id, order_id))
            _publish_payment(build_abort_payment(tx_id, order_id))
        elif action == "missing_tx_id":
            _logger.warning(f"[Order2PC] order={order_id} missing tx_id")
    elif msg_type == STOCK_PREPARE_FAILED:
        action, tx_id = _update_participant_and_maybe_decide(order_id, "stock_state", STOCK_FAILED)
        if action == "publish_abort":
            _logger.debug(f"[Order2PC] order={order_id} ABORT")
            _publish_stock(build_abort_stock(tx_id, order_id))
            _publish_payment(build_abort_payment(tx_id, order_id))
        elif action == "missing_tx_id":
            _logger.warning(f"[Order2PC] order={order_id} missing tx_id")
    elif msg_type == PAYMENT_PREPARED:
        action, tx_id = _update_participant_and_maybe_decide(order_id, "payment_state", PAYMENT_READY)
        if action == "publish_commit":
            _logger.debug(f"[Order2PC] order={order_id} COMMIT")
            _publish_stock(build_commit_stock(tx_id, order_id))
            _publish_payment(build_commit_payment(tx_id, order_id))
        elif action == "publish_abort":
            _logger.debug(f"[Order2PC] order={order_id} ABORT")
            _publish_stock(build_abort_stock(tx_id, order_id))
            _publish_payment(build_abort_payment(tx_id, order_id))
        elif action == "missing_tx_id":
            _logger.warning(f"[Order2PC] order={order_id} missing tx_id")
    elif msg_type == PAYMENT_PREPARE_FAILED:
        action, tx_id = _update_participant_and_maybe_decide(order_id, "payment_state", PAYMENT_FAILED)
        if action == "publish_abort":
            _logger.debug(f"[Order2PC] order={order_id} ABORT")
            _publish_stock(build_abort_stock(tx_id, order_id))
            _publish_payment(build_abort_payment(tx_id, order_id))
        elif action == "missing_tx_id":
            _logger.warning(f"[Order2PC] order={order_id} missing tx_id")
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
