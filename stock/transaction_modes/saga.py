"""
stock/transaction_modes/saga.py

Saga command handlers for the stock service.

The hot path is executed inside Redis Lua so that:
    - stock validation + mutation + ledger write happen in one atomic step
    - we avoid Python WATCH/MULTI retry loops on every command
    - the ledger still stores an APPLIED reply payload for crash recovery
"""

import json
import redis as redis_module

from common.messages import (
    RESERVE_STOCK,
    RELEASE_STOCK,
    STOCK_EVENTS_TOPIC,
    build_stock_reserved,
    build_stock_reservation_failed,
    build_stock_released,
)

import ledger as stock_ledger
from ledger import LedgerState


_RESERVE_STOCK_LUA = """
local ledger_key = KEYS[1]
local tx_id = ARGV[1]
local items = cjson.decode(ARGV[2])
local success_reply = cjson.decode(ARGV[3])
local failure_reply = cjson.decode(ARGV[4])
local ttl = tonumber(ARGV[5])

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
        action_type = 'RESERVE_STOCK',
        local_state = 'applied',
        result = result,
        reply_message = reply,
        business_snapshot = { items = items },
        created_at_ms = created_at_ms,
        updated_at_ms = now_ms,
    }
    redis.call('SET', ledger_key, cjson.encode(entry), 'EX', ttl)
    return {'applied', result, cjson.encode(reply)}
end

if next(items) == nil then
    failure_reply['payload']['reason'] = 'No items in payload'
    return persist('failure', failure_reply)
end

for _, item in ipairs(items) do
    local item_id = tostring(item['item_id'] or '')
    local quantity = tonumber(item['quantity'])

    if item_id == '' or not quantity then
        failure_reply['payload']['reason'] = 'Invalid item payload'
        return persist('failure', failure_reply)
    end

    local stock_raw = redis.call('HGET', item_id, 'stock')
    if not stock_raw then
        failure_reply['payload']['reason'] = 'Item ' .. item_id .. ' not found'
        return persist('failure', failure_reply)
    end

    local stock = tonumber(stock_raw)
    if not stock or stock < quantity then
        failure_reply['payload']['reason'] =
            'Insufficient stock for item ' .. item_id ..
            ' (have ' .. tostring(stock or 0) ..
            ', need ' .. tostring(quantity) .. ')'
        return persist('failure', failure_reply)
    end
end

for _, item in ipairs(items) do
    redis.call('HINCRBY', tostring(item['item_id']), 'stock', -tonumber(item['quantity']))
end

return persist('success', success_reply)
"""

_RELEASE_STOCK_LUA = """
local release_key = KEYS[1]
local reserve_key = KEYS[2]
local tx_id = ARGV[1]
local items = cjson.decode(ARGV[2])
local success_reply = cjson.decode(ARGV[3])
local ttl = tonumber(ARGV[4])

local now = redis.call('TIME')
local now_ms = tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000)

local created_at_ms = now_ms
local raw_release = redis.call('GET', release_key)
if raw_release then
    local release_entry = cjson.decode(raw_release)
    local state = release_entry['local_state']
    if state == 'applied' or state == 'replied' then
        return {state or '', release_entry['result'] or '', cjson.encode(release_entry['reply_message'])}
    end
    if release_entry['created_at_ms'] then
        created_at_ms = release_entry['created_at_ms']
    end
end

local function persist(reply)
    local entry = {
        tx_id = tx_id,
        action_type = 'RELEASE_STOCK',
        local_state = 'applied',
        result = 'success',
        reply_message = reply,
        business_snapshot = { items = items },
        created_at_ms = created_at_ms,
        updated_at_ms = now_ms,
    }
    redis.call('SET', release_key, cjson.encode(entry), 'EX', ttl)
    return {'applied', 'success', cjson.encode(reply)}
end

local raw_reserve = redis.call('GET', reserve_key)
if raw_reserve then
    local reserve_entry = cjson.decode(raw_reserve)
    if reserve_entry['result'] ~= 'success' then
        return persist(success_reply)
    end
else
    return persist(success_reply)
end

for _, item in ipairs(items) do
    local item_id = tostring(item['item_id'] or '')
    local quantity = tonumber(item['quantity']) or 0
    local stock_raw = redis.call('HGET', item_id, 'stock')

    if item_id == '' or not stock_raw then
        return {'error', 'missing_item', item_id}
    end

    redis.call('HINCRBY', item_id, 'stock', quantity)
end

return persist(success_reply)
"""

_reserve_stock_script = None
_release_stock_script = None


def saga_route_stock(msg: dict, db: redis_module.Redis, publish, logger) -> None:
    msg_type = msg.get("type")
    if msg_type == RESERVE_STOCK:
        _handle_reserve_stock(msg, db, publish, logger)
    elif msg_type == RELEASE_STOCK:
        _handle_release_stock(msg, db, publish, logger)
    else:
        logger.info(f"[StockSaga] Unknown command type: {msg_type!r} — dropping")


def _handle_reserve_stock(
    msg: dict,
    db: redis_module.Redis,
    publish,
    logger,
) -> None:
    tx_id = msg.get("tx_id")
    order_id = msg.get("order_id")
    items = msg.get("payload", {}).get("items", [])

    try:
        state, reply, result = _reserve_stock_atomically(
            db=db,
            tx_id=tx_id,
            order_id=order_id,
            items=items,
        )
    except RuntimeError as exc:
        logger.error(f"[StockSaga] RESERVE_STOCK atomic apply failed tx={tx_id}: {exc}")
        return

    publish(STOCK_EVENTS_TOPIC, reply)
    if state != LedgerState.REPLIED:
        stock_ledger.mark_replied(db, tx_id, RESERVE_STOCK)
    logger.debug(f"[StockSaga] RESERVE_STOCK {result} tx={tx_id} order={order_id}")


def _handle_release_stock(
    msg: dict,
    db: redis_module.Redis,
    publish,
    logger,
) -> None:
    tx_id = msg.get("tx_id")
    order_id = msg.get("order_id")
    items = msg.get("payload", {}).get("items", [])

    try:
        state, reply, _ = _release_stock_atomically(
            db=db,
            tx_id=tx_id,
            order_id=order_id,
            items=items,
        )
    except RuntimeError as exc:
        logger.error(f"[StockSaga] RELEASE_STOCK atomic apply failed tx={tx_id}: {exc}")
        return

    publish(STOCK_EVENTS_TOPIC, reply)
    if state != LedgerState.REPLIED:
        stock_ledger.mark_replied(db, tx_id, RELEASE_STOCK)
    logger.debug(f"[StockSaga] RELEASE_STOCK done tx={tx_id} order={order_id}")


def _get_reserve_stock_script(db: redis_module.Redis):
    global _reserve_stock_script
    if _reserve_stock_script is None:
        _reserve_stock_script = db.register_script(_RESERVE_STOCK_LUA)
    return _reserve_stock_script


def _get_release_stock_script(db: redis_module.Redis):
    global _release_stock_script
    if _release_stock_script is None:
        _release_stock_script = db.register_script(_RELEASE_STOCK_LUA)
    return _release_stock_script


def _decode_scalar(value) -> str:
    return value.decode() if isinstance(value, bytes) else value


def _reserve_stock_atomically(
    db: redis_module.Redis,
    tx_id: str,
    order_id: str,
    items: list[dict],
) -> tuple[str, dict, str]:
    ledger_key = f"stock:ledger:{tx_id}:{RESERVE_STOCK}"
    success_reply = build_stock_reserved(tx_id, order_id)
    failure_reply = build_stock_reservation_failed(tx_id, order_id, "")
    script = _get_reserve_stock_script(db)

    result = script(
        keys=[ledger_key],
        args=[
            tx_id,
            json.dumps(items),
            json.dumps(success_reply),
            json.dumps(failure_reply),
            stock_ledger.LEDGER_TTL_SECONDS,
        ],
        client=db,
    )

    state = _decode_scalar(result[0])
    if state == "error":
        raise RuntimeError(_decode_scalar(result[1]))
    return state, json.loads(result[2]), _decode_scalar(result[1])


def _release_stock_atomically(
    db: redis_module.Redis,
    tx_id: str,
    order_id: str,
    items: list[dict],
) -> tuple[str, dict, str]:
    release_key = f"stock:ledger:{tx_id}:{RELEASE_STOCK}"
    reserve_key = f"stock:ledger:{tx_id}:{RESERVE_STOCK}"
    success_reply = build_stock_released(tx_id, order_id)
    script = _get_release_stock_script(db)

    result = script(
        keys=[release_key, reserve_key],
        args=[
            tx_id,
            json.dumps(items),
            json.dumps(success_reply),
            stock_ledger.LEDGER_TTL_SECONDS,
        ],
        client=db,
    )

    state = _decode_scalar(result[0])
    if state == "error":
        raise RuntimeError(f"Missing item during RELEASE_STOCK: {_decode_scalar(result[2])}")
    return state, json.loads(result[2]), _decode_scalar(result[1])
