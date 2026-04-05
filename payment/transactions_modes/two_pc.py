"""
payment/transactions_modes/two_pc.py

2PC command handlers for the payment service.

The local prepare/commit/abort path runs inside Redis Lua so reserved-credit
bookkeeping and durable ledger writes happen atomically without Python
WATCH/MULTI retry loops on every command.
"""

import json

import redis as redis_module

from common.messages import (
    PAYMENT_EVENTS_TOPIC,
    PREPARE_PAYMENT,
    COMMIT_PAYMENT,
    ABORT_PAYMENT,
    PAYMENT_PREPARED,
    PAYMENT_PREPARE_FAILED,
    build_payment_prepared,
    build_payment_prepare_failed,
    build_payment_committed,
    build_payment_aborted,
)
import ledger as payment_ledger
from ledger import LedgerState

_db: redis_module.Redis | None = None
_publish = None
_logger = None


_PREPARE_PAYMENT_LUA = """
local prepare_key = KEYS[1]
local abort_key = KEYS[2]
local commit_key = KEYS[3]

local tx_id = ARGV[1]
local order_id = ARGV[2]
local user_id = ARGV[3]
local amount = tonumber(ARGV[4])
local success_reply = cjson.decode(ARGV[5])
local failure_reply = cjson.decode(ARGV[6])
local ttl = tonumber(ARGV[7])

local now = redis.call('TIME')
local now_ms = tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000)

local function load_entry(key)
    local raw = redis.call('GET', key)
    if not raw then
        return nil
    end
    return cjson.decode(raw)
end

local function is_terminal(entry)
    if not entry then
        return false
    end
    local state = entry['local_state']
    return state == 'applied' or state == 'replied'
end

local prepare_entry = load_entry(prepare_key)
if is_terminal(prepare_entry) then
    return {
        prepare_entry['local_state'] or '',
        prepare_entry['result'] or '',
        cjson.encode(prepare_entry['reply_message'])
    }
end

local created_at_ms = now_ms
if prepare_entry and prepare_entry['created_at_ms'] then
    created_at_ms = prepare_entry['created_at_ms']
end

local abort_entry = load_entry(abort_key)
if is_terminal(abort_entry) then
    return {
        abort_entry['local_state'] or '',
        abort_entry['result'] or '',
        cjson.encode(abort_entry['reply_message'])
    }
end

local commit_entry = load_entry(commit_key)
if is_terminal(commit_entry) then
    return {
        commit_entry['local_state'] or '',
        commit_entry['result'] or '',
        cjson.encode(commit_entry['reply_message'])
    }
end

local function persist(result, reply)
    local entry = {
        tx_id = tx_id,
        action_type = 'PREPARE_PAYMENT',
        local_state = 'applied',
        result = result,
        reply_message = reply,
        business_snapshot = { order_id = order_id, user_id = user_id, amount = amount },
        created_at_ms = created_at_ms,
        updated_at_ms = now_ms,
    }
    redis.call('SET', prepare_key, cjson.encode(entry), 'EX', ttl)
    return {'applied', result, cjson.encode(reply)}
end

if user_id == '' or not amount then
    failure_reply['payload']['reason'] = 'Invalid payment payload'
    return persist('failure', failure_reply)
end

local reservation_key = 'payment:reservation:' .. tx_id
local reserved_total_key = 'payment:reserved_total:' .. user_id
local raw_existing_reservation = redis.call('GET', reservation_key)
if raw_existing_reservation then
    return persist('success', success_reply)
end

local raw_user = redis.call('GET', user_id)
if not raw_user then
    failure_reply['payload']['reason'] = 'User ' .. user_id .. ' not found'
    return persist('failure', failure_reply)
end

local credit = tonumber(raw_user)
local reserved_total = tonumber(redis.call('GET', reserved_total_key) or '0')
local available = (credit or 0) - reserved_total
if not credit or available < amount then
    failure_reply['payload']['reason'] =
        'Insufficient credit for user ' .. user_id ..
        ' (have ' .. tostring(available) ..
        ', need ' .. tostring(amount) .. ')'
    return persist('failure', failure_reply)
end

redis.call('SET', reservation_key, tostring(amount), 'EX', ttl)
redis.call('INCRBY', reserved_total_key, amount)
return persist('success', success_reply)
"""

_COMMIT_PAYMENT_LUA = """
local commit_key = KEYS[1]
local prepare_key = KEYS[2]
local abort_key = KEYS[3]

local tx_id = ARGV[1]
local order_id = ARGV[2]
local success_reply = cjson.decode(ARGV[3])
local ttl = tonumber(ARGV[4])

local now = redis.call('TIME')
local now_ms = tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000)

local function load_entry(key)
    local raw = redis.call('GET', key)
    if not raw then
        return nil
    end
    return cjson.decode(raw)
end

local function is_terminal(entry)
    if not entry then
        return false
    end
    local state = entry['local_state']
    return state == 'applied' or state == 'replied'
end

local commit_entry = load_entry(commit_key)
if is_terminal(commit_entry) then
    return {
        commit_entry['local_state'] or '',
        commit_entry['result'] or '',
        cjson.encode(commit_entry['reply_message'])
    }
end

local created_at_ms = now_ms
if commit_entry and commit_entry['created_at_ms'] then
    created_at_ms = commit_entry['created_at_ms']
end

local abort_entry = load_entry(abort_key)
if is_terminal(abort_entry) then
    return {'error', 'abort_already_applied', ''}
end

local prepare_entry = load_entry(prepare_key)
if not prepare_entry or prepare_entry['result'] ~= 'success' then
    return {'error', 'missing_successful_prepare', ''}
end

local snapshot = prepare_entry['business_snapshot'] or {}
local user_id = tostring(snapshot['user_id'] or '')
local amount = tonumber(snapshot['amount'])
if user_id == '' or not amount then
    return {'error', 'missing_prepare_snapshot', ''}
end

local reservation_key = 'payment:reservation:' .. tx_id
local reserved_total_key = 'payment:reserved_total:' .. user_id
local raw_reservation = redis.call('GET', reservation_key)
if not raw_reservation then
    return {'error', 'missing_reservation', user_id}
end

local raw_user = redis.call('GET', user_id)
if not raw_user then
    return {'error', 'missing_user', user_id}
end

local credit = tonumber(raw_user)
local reserved_total = tonumber(redis.call('GET', reserved_total_key) or '0')
if not credit then
    return {'error', 'missing_user', user_id}
end
if reserved_total < amount then
    return {'error', 'reserved_total_below_reservation', user_id}
end
if credit < amount then
    return {'error', 'credit_below_reservation', user_id}
end

redis.call('SET', user_id, tostring(credit - amount))
redis.call('DECRBY', reserved_total_key, amount)
redis.call('DEL', reservation_key)

local entry = {
    tx_id = tx_id,
    action_type = 'COMMIT_PAYMENT',
    local_state = 'applied',
    result = 'success',
    reply_message = success_reply,
    business_snapshot = { order_id = order_id },
    created_at_ms = created_at_ms,
    updated_at_ms = now_ms,
}
redis.call('SET', commit_key, cjson.encode(entry), 'EX', ttl)
return {'applied', 'success', cjson.encode(success_reply)}
"""

_ABORT_PAYMENT_LUA = """
local abort_key = KEYS[1]
local prepare_key = KEYS[2]
local commit_key = KEYS[3]

local tx_id = ARGV[1]
local order_id = ARGV[2]
local success_reply = cjson.decode(ARGV[3])
local ttl = tonumber(ARGV[4])

local now = redis.call('TIME')
local now_ms = tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000)

local function load_entry(key)
    local raw = redis.call('GET', key)
    if not raw then
        return nil
    end
    return cjson.decode(raw)
end

local function is_terminal(entry)
    if not entry then
        return false
    end
    local state = entry['local_state']
    return state == 'applied' or state == 'replied'
end

local abort_entry = load_entry(abort_key)
if is_terminal(abort_entry) then
    return {
        abort_entry['local_state'] or '',
        abort_entry['result'] or '',
        cjson.encode(abort_entry['reply_message'])
    }
end

local created_at_ms = now_ms
if abort_entry and abort_entry['created_at_ms'] then
    created_at_ms = abort_entry['created_at_ms']
end

local commit_entry = load_entry(commit_key)
if is_terminal(commit_entry) then
    return {'error', 'commit_already_applied', ''}
end

local prepare_entry = load_entry(prepare_key)
local prepare_succeeded = prepare_entry and prepare_entry['result'] == 'success'
local user_id = ''
local amount = 0

if prepare_succeeded then
    local snapshot = prepare_entry['business_snapshot'] or {}
    user_id = tostring(snapshot['user_id'] or '')
    amount = tonumber(snapshot['amount']) or 0
end

if user_id ~= '' and amount > 0 then
    local reservation_key = 'payment:reservation:' .. tx_id
    local raw_reservation = redis.call('GET', reservation_key)
    if raw_reservation then
        local reserved_total_key = 'payment:reserved_total:' .. user_id
        local reserved_total = tonumber(redis.call('GET', reserved_total_key) or '0')
        if reserved_total < amount then
            return {'error', 'reserved_total_below_reservation', user_id}
        end
        redis.call('DECRBY', reserved_total_key, amount)
        redis.call('DEL', reservation_key)
    end
end

local entry = {
    tx_id = tx_id,
    action_type = 'ABORT_PAYMENT',
    local_state = 'applied',
    result = 'success',
    reply_message = success_reply,
    business_snapshot = { order_id = order_id },
    created_at_ms = created_at_ms,
    updated_at_ms = now_ms,
}
redis.call('SET', abort_key, cjson.encode(entry), 'EX', ttl)
return {'applied', 'success', cjson.encode(success_reply)}
"""

_prepare_payment_script = None
_commit_payment_script = None
_abort_payment_script = None


def init_2pc(db: redis_module.Redis, publish_fn, logger) -> None:
    global _db, _publish, _logger
    _db = db
    _publish = publish_fn
    _logger = logger


def _ledger_key(tx_id: str, action_type: str) -> str:
    return f"payment:ledger:{tx_id}:{action_type}"


def _decode_scalar(value) -> str:
    return value.decode() if isinstance(value, bytes) else value


def _get_prepare_payment_script(db: redis_module.Redis):
    global _prepare_payment_script
    if _prepare_payment_script is None:
        _prepare_payment_script = db.register_script(_PREPARE_PAYMENT_LUA)
    return _prepare_payment_script


def _get_commit_payment_script(db: redis_module.Redis):
    global _commit_payment_script
    if _commit_payment_script is None:
        _commit_payment_script = db.register_script(_COMMIT_PAYMENT_LUA)
    return _commit_payment_script


def _get_abort_payment_script(db: redis_module.Redis):
    global _abort_payment_script
    if _abort_payment_script is None:
        _abort_payment_script = db.register_script(_ABORT_PAYMENT_LUA)
    return _abort_payment_script


def _apply_prepare_payment_atomically(
    db: redis_module.Redis,
    tx_id: str,
    order_id: str,
    user_id: str,
    amount: int,
) -> tuple[str, dict, str]:
    script = _get_prepare_payment_script(db)
    success_reply = build_payment_prepared(tx_id, order_id)
    failure_reply = build_payment_prepare_failed(tx_id, order_id, "")
    result = script(
        keys=[
            _ledger_key(tx_id, PREPARE_PAYMENT),
            _ledger_key(tx_id, ABORT_PAYMENT),
            _ledger_key(tx_id, COMMIT_PAYMENT),
        ],
        args=[
            tx_id,
            order_id,
            user_id,
            amount,
            json.dumps(success_reply),
            json.dumps(failure_reply),
            payment_ledger.LEDGER_TTL_SECONDS,
        ],
        client=db,
    )

    state = _decode_scalar(result[0])
    if state == "error":
        raise RuntimeError(f"{_decode_scalar(result[1])}: {_decode_scalar(result[2])}")
    return state, json.loads(result[2]), _decode_scalar(result[1])


def _apply_commit_payment_atomically(
    db: redis_module.Redis,
    tx_id: str,
    order_id: str,
) -> tuple[str, dict]:
    script = _get_commit_payment_script(db)
    success_reply = build_payment_committed(tx_id, order_id)
    result = script(
        keys=[
            _ledger_key(tx_id, COMMIT_PAYMENT),
            _ledger_key(tx_id, PREPARE_PAYMENT),
            _ledger_key(tx_id, ABORT_PAYMENT),
        ],
        args=[
            tx_id,
            order_id,
            json.dumps(success_reply),
            payment_ledger.LEDGER_TTL_SECONDS,
        ],
        client=db,
    )

    state = _decode_scalar(result[0])
    if state == "error":
        raise RuntimeError(f"{_decode_scalar(result[1])}: {_decode_scalar(result[2])}")
    return state, json.loads(result[2])


def _apply_abort_payment_atomically(
    db: redis_module.Redis,
    tx_id: str,
    order_id: str,
) -> tuple[str, dict]:
    script = _get_abort_payment_script(db)
    success_reply = build_payment_aborted(tx_id, order_id)
    result = script(
        keys=[
            _ledger_key(tx_id, ABORT_PAYMENT),
            _ledger_key(tx_id, PREPARE_PAYMENT),
            _ledger_key(tx_id, COMMIT_PAYMENT),
        ],
        args=[
            tx_id,
            order_id,
            json.dumps(success_reply),
            payment_ledger.LEDGER_TTL_SECONDS,
        ],
        client=db,
    )

    state = _decode_scalar(result[0])
    if state == "error":
        raise RuntimeError(f"{_decode_scalar(result[1])}: {_decode_scalar(result[2])}")
    return state, json.loads(result[2])


def _handle_prepare_payment(msg, db, producer, logger):
    tx_id = msg.get("tx_id")
    order_id = msg.get("order_id")
    payload = msg.get("payload", {})
    user_id = str(payload.get("user_id", ""))
    amount = payload.get("amount")

    if not tx_id or not order_id or not user_id or amount is None:
        logger.warning(f"[Payment2PC] Missing fields in PREPARE_PAYMENT: {msg}")
        return

    try:
        state, reply, result = _apply_prepare_payment_atomically(
            db,
            tx_id,
            order_id,
            user_id,
            int(amount),
        )
    except RuntimeError as exc:
        logger.error(f"[Payment2PC] PREPARE_PAYMENT tx={tx_id} failed: {exc}")
        return

    producer(PAYMENT_EVENTS_TOPIC, reply)
    if state != LedgerState.REPLIED and reply.get("type") in {PAYMENT_PREPARED, PAYMENT_PREPARE_FAILED}:
        payment_ledger.mark_replied(db, tx_id, PREPARE_PAYMENT)
    logger.debug(f"[Payment2PC] order={order_id} PREPARE_PAYMENT {result}")


def _handle_commit_payment(msg, db, producer, logger):
    tx_id = msg.get("tx_id")
    order_id = msg.get("order_id")

    try:
        state, reply = _apply_commit_payment_atomically(db, tx_id, order_id)
    except RuntimeError as exc:
        logger.error(f"[Payment2PC] COMMIT_PAYMENT tx={tx_id} failed: {exc}")
        return

    producer(PAYMENT_EVENTS_TOPIC, reply)
    if state != LedgerState.REPLIED:
        payment_ledger.mark_replied(db, tx_id, COMMIT_PAYMENT)
    logger.debug(f"[Payment2PC] order={order_id} PAYMENT_COMMITTED")


def _handle_abort_payment(msg, db, producer, logger):
    tx_id = msg.get("tx_id")
    order_id = msg.get("order_id")

    try:
        state, reply = _apply_abort_payment_atomically(db, tx_id, order_id)
    except RuntimeError as exc:
        logger.error(f"[Payment2PC] ABORT_PAYMENT tx={tx_id} failed: {exc}")
        return

    producer(PAYMENT_EVENTS_TOPIC, reply)
    if state != LedgerState.REPLIED:
        payment_ledger.mark_replied(db, tx_id, ABORT_PAYMENT)
    logger.debug(f"[Payment2PC] order={order_id} PAYMENT_ABORTED")


def _2pc_route_payment(msg: dict) -> None:
    msg_type = msg.get("type")
    if msg_type == PREPARE_PAYMENT:
        _handle_prepare_payment(msg, _db, _publish, _logger)
    elif msg_type == COMMIT_PAYMENT:
        _handle_commit_payment(msg, _db, _publish, _logger)
    elif msg_type == ABORT_PAYMENT:
        _handle_abort_payment(msg, _db, _publish, _logger)
    else:
        _logger.debug(f"[Payment2PC] Unknown command type: {msg_type!r} — dropping")
