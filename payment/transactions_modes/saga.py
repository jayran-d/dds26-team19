"""
payment/transactions_modes/saga.py

Saga command handlers for the payment service.

The local charge/refund and durable ledger transition are executed inside
Redis Lua so the participant hot path avoids Python WATCH/MULTI retry loops
while preserving crash recovery semantics.
"""

import json
import redis as redis_module

from common.messages import (
    PROCESS_PAYMENT,
    REFUND_PAYMENT,
    PAYMENT_EVENTS_TOPIC,
    build_payment_success,
    build_payment_failed,
    build_payment_refunded,
)
import ledger as payment_ledger
from ledger import LedgerState


_PROCESS_PAYMENT_LUA = """
local ledger_key = KEYS[1]
local tx_id = ARGV[1]
local user_id = ARGV[2]
local amount = tonumber(ARGV[3])
local business_snapshot = cjson.decode(ARGV[4])
local success_reply = cjson.decode(ARGV[5])
local failure_reply = cjson.decode(ARGV[6])
local ttl = tonumber(ARGV[7])

local now = redis.call('TIME')
local now_ms = tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000)

local created_at_ms = now_ms
local raw_entry = redis.call('GET', ledger_key)
if raw_entry then
    local entry = cjson.decode(raw_entry)
    local state = entry['local_state']
    if state == 'applied' or state == 'replied' then
        return {state or '', entry['result'] or '', cjson.encode(entry['reply_message'])}
    end
    if entry['created_at_ms'] then
        created_at_ms = entry['created_at_ms']
    end
end

local function persist(result, reply)
    local entry = {
        tx_id = tx_id,
        action_type = 'PROCESS_PAYMENT',
        local_state = 'applied',
        result = result,
        reply_message = reply,
        business_snapshot = business_snapshot,
        created_at_ms = created_at_ms,
        updated_at_ms = now_ms,
    }
    redis.call('SET', ledger_key, cjson.encode(entry), 'EX', ttl)
    return {'applied', result, cjson.encode(reply)}
end

local raw_user = redis.call('GET', user_id)
local credit = raw_user and tonumber(raw_user) or nil

if not credit then
    failure_reply['payload']['reason'] = 'User: ' .. user_id .. ' not found!'
    return persist('failure', failure_reply)
end

if credit - amount < 0 then
    failure_reply['payload']['reason'] = 'User: ' .. user_id .. ' credit cannot get reduced below zero!'
    return persist('failure', failure_reply)
end

redis.call('SET', user_id, tostring(credit - amount))
return persist('success', success_reply)
"""

_REFUND_PAYMENT_LUA = """
local refund_key = KEYS[1]
local payment_key = KEYS[2]
local tx_id = ARGV[1]
local user_id = ARGV[2]
local amount = tonumber(ARGV[3])
local business_snapshot = cjson.decode(ARGV[4])
local success_reply = cjson.decode(ARGV[5])
local ttl = tonumber(ARGV[6])

local now = redis.call('TIME')
local now_ms = tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000)

local created_at_ms = now_ms
local raw_refund = redis.call('GET', refund_key)
if raw_refund then
    local refund_entry = cjson.decode(raw_refund)
    local state = refund_entry['local_state']
    if state == 'applied' or state == 'replied' then
        return {state or '', refund_entry['result'] or '', cjson.encode(refund_entry['reply_message'])}
    end
    if refund_entry['created_at_ms'] then
        created_at_ms = refund_entry['created_at_ms']
    end
end

local function persist(reply)
    local entry = {
        tx_id = tx_id,
        action_type = 'REFUND_PAYMENT',
        local_state = 'applied',
        result = 'success',
        reply_message = reply,
        business_snapshot = business_snapshot,
        created_at_ms = created_at_ms,
        updated_at_ms = now_ms,
    }
    redis.call('SET', refund_key, cjson.encode(entry), 'EX', ttl)
    return {'applied', 'success', cjson.encode(reply)}
end

local raw_payment = redis.call('GET', payment_key)
if raw_payment then
    local payment_entry = cjson.decode(raw_payment)
    if payment_entry['result'] ~= 'success' then
        return persist(success_reply)
    end
else
    return persist(success_reply)
end

local raw_user = redis.call('GET', user_id)
local credit = raw_user and tonumber(raw_user) or nil
if not credit then
    return {'error', 'missing_user', user_id}
end

redis.call('SET', user_id, tostring(credit + amount))
return persist(success_reply)
"""

_process_payment_script = None
_refund_payment_script = None


def saga_route_payment(msg: dict, db: redis_module.Redis, publish, logger) -> None:
    msg_type = msg.get("type")
    if msg_type == PROCESS_PAYMENT:
        _handle_process_payment(msg, db, publish, logger)
    elif msg_type == REFUND_PAYMENT:
        _handle_refund_payment(msg, db, publish, logger)
    else:
        logger.info(f"[PaymentSaga] Unknown command type: {msg_type!r} — dropping")


def _handle_process_payment(
    msg: dict,
    db: redis_module.Redis,
    publish,
    logger,
) -> None:
    tx_id = msg.get("tx_id")
    order_id = msg.get("order_id")
    payload = msg.get("payload", {})
    user_id = str(payload.get("user_id", ""))
    amount = payload.get("amount")

    if not user_id or amount is None:
        logger.error(f"[PaymentSaga] Invalid PROCESS_PAYMENT payload: {msg}")
        return

    try:
        state, reply, result = _apply_process_payment_atomically(
            db=db,
            tx_id=tx_id,
            order_id=order_id,
            user_id=user_id,
            amount=int(amount),
        )
    except RuntimeError as exc:
        logger.error(f"[PaymentSaga] PROCESS_PAYMENT atomic apply failed tx={tx_id}: {exc}")
        return

    publish(PAYMENT_EVENTS_TOPIC, reply)
    if state != LedgerState.REPLIED:
        payment_ledger.mark_replied(db, tx_id, PROCESS_PAYMENT)
    logger.debug(f"[PaymentSaga] PROCESS_PAYMENT {result} tx={tx_id} order={order_id}")


def _handle_refund_payment(
    msg: dict,
    db: redis_module.Redis,
    publish,
    logger,
) -> None:
    tx_id = msg.get("tx_id")
    order_id = msg.get("order_id")
    payload = msg.get("payload", {})
    user_id = str(payload.get("user_id", ""))
    amount = payload.get("amount")

    if not user_id or amount is None:
        logger.error(f"[PaymentSaga] Invalid REFUND_PAYMENT payload: {msg}")
        return

    try:
        state, reply, _ = _apply_refund_payment_atomically(
            db=db,
            tx_id=tx_id,
            order_id=order_id,
            user_id=user_id,
            amount=int(amount),
        )
    except RuntimeError as exc:
        logger.error(f"[PaymentSaga] REFUND_PAYMENT atomic apply failed tx={tx_id}: {exc}")
        return

    publish(PAYMENT_EVENTS_TOPIC, reply)
    if state != LedgerState.REPLIED:
        payment_ledger.mark_replied(db, tx_id, REFUND_PAYMENT)
    logger.debug(f"[PaymentSaga] REFUND_PAYMENT done tx={tx_id} order={order_id}")


def _get_process_payment_script(db: redis_module.Redis):
    global _process_payment_script
    if _process_payment_script is None:
        _process_payment_script = db.register_script(_PROCESS_PAYMENT_LUA)
    return _process_payment_script


def _get_refund_payment_script(db: redis_module.Redis):
    global _refund_payment_script
    if _refund_payment_script is None:
        _refund_payment_script = db.register_script(_REFUND_PAYMENT_LUA)
    return _refund_payment_script


def _decode_scalar(value) -> str:
    return value.decode() if isinstance(value, bytes) else value


def _apply_process_payment_atomically(
    db: redis_module.Redis,
    tx_id: str,
    order_id: str,
    user_id: str,
    amount: int,
) -> tuple[str, dict, str]:
    success_reply = build_payment_success(tx_id, order_id)
    failure_reply = build_payment_failed(tx_id, order_id, "")
    script = _get_process_payment_script(db)

    result = script(
        keys=[f"payment:ledger:{tx_id}:{PROCESS_PAYMENT}"],
        args=[
            tx_id,
            user_id,
            amount,
            json.dumps({"user_id": user_id, "amount": int(amount)}),
            json.dumps(success_reply),
            json.dumps(failure_reply),
            payment_ledger.LEDGER_TTL_SECONDS,
        ],
        client=db,
    )

    state = _decode_scalar(result[0])
    if state == "error":
        raise RuntimeError(_decode_scalar(result[1]))
    return state, json.loads(result[2]), _decode_scalar(result[1])


def _apply_refund_payment_atomically(
    db: redis_module.Redis,
    tx_id: str,
    order_id: str,
    user_id: str,
    amount: int,
) -> tuple[str, dict, str]:
    success_reply = build_payment_refunded(tx_id, order_id)
    script = _get_refund_payment_script(db)

    result = script(
        keys=[
            f"payment:ledger:{tx_id}:{REFUND_PAYMENT}",
            f"payment:ledger:{tx_id}:{PROCESS_PAYMENT}",
        ],
        args=[
            tx_id,
            user_id,
            amount,
            json.dumps({"user_id": user_id, "amount": int(amount)}),
            json.dumps(success_reply),
            payment_ledger.LEDGER_TTL_SECONDS,
        ],
        client=db,
    )

    state = _decode_scalar(result[0])
    if state == "error":
        raise RuntimeError(f"Missing user during REFUND_PAYMENT: {_decode_scalar(result[2])}")
    return state, json.loads(result[2]), _decode_scalar(result[1])
