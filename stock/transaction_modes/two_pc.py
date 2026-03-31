"""
stock/transaction_modes/two_pc.py

2PC command handlers for the stock service.

Like the saga participant hot path, prepare/commit/abort now run inside
Redis Lua so reservation bookkeeping and durable ledger writes happen in a
single Redis-side atomic step instead of Python WATCH/MULTI retry loops.
"""

import json

import redis as redis_module

from common.messages import (
    STOCK_EVENTS_TOPIC,
    PREPARE_STOCK,
    COMMIT_STOCK,
    ABORT_STOCK,
    STOCK_PREPARED,
    STOCK_PREPARE_FAILED,
    build_stock_prepared,
    build_stock_prepare_failed,
    build_stock_committed,
    build_stock_aborted,
)
import ledger as stock_ledger
from ledger import LedgerState

_db: redis_module.Redis | None = None
_publish = None
_logger = None


_PREPARE_STOCK_LUA = """
local prepare_key = KEYS[1]
local abort_key = KEYS[2]
local commit_key = KEYS[3]

local tx_id = ARGV[1]
local order_id = ARGV[2]
local items = cjson.decode(ARGV[3])
local success_reply = cjson.decode(ARGV[4])
local failure_reply = cjson.decode(ARGV[5])
local ttl = tonumber(ARGV[6])

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
        action_type = 'PREPARE_STOCK',
        local_state = 'applied',
        result = result,
        reply_message = reply,
        business_snapshot = { order_id = order_id, items = items },
        created_at_ms = created_at_ms,
        updated_at_ms = now_ms,
    }
    redis.call('SET', prepare_key, cjson.encode(entry), 'EX', ttl)
    return {'applied', result, cjson.encode(reply)}
end

if next(items) == nil then
    failure_reply['payload']['reason'] = 'No items in payload'
    return persist('failure', failure_reply)
end

local saw_reservation = false
for _, item in ipairs(items) do
    local item_id = tostring(item['item_id'] or '')
    local quantity = tonumber(item['quantity'])
    if item_id == '' or not quantity or quantity <= 0 then
        failure_reply['payload']['reason'] = 'Invalid item payload'
        return persist('failure', failure_reply)
    end

    local reservation_key = 'stock:reservation:' .. tx_id .. ':' .. item_id
    if redis.call('GET', reservation_key) then
        saw_reservation = true
    end
end

if saw_reservation then
    return persist('success', success_reply)
end

for _, item in ipairs(items) do
    local item_id = tostring(item['item_id'])
    local quantity = tonumber(item['quantity'])
    local stock_raw = redis.call('HGET', item_id, 'stock')
    if not stock_raw then
        failure_reply['payload']['reason'] = 'Item ' .. item_id .. ' not found'
        return persist('failure', failure_reply)
    end

    local stock = tonumber(stock_raw)
    local reserved_total = tonumber(redis.call('GET', 'stock:reserved_total:' .. item_id) or '0')
    local available = (stock or 0) - reserved_total
    if not stock or available < quantity then
        failure_reply['payload']['reason'] =
            'Insufficient stock for item ' .. item_id ..
            ' (have ' .. tostring(available) ..
            ', need ' .. tostring(quantity) .. ')'
        return persist('failure', failure_reply)
    end
end

for _, item in ipairs(items) do
    local item_id = tostring(item['item_id'])
    local quantity = tonumber(item['quantity'])
    redis.call('SET', 'stock:reservation:' .. tx_id .. ':' .. item_id, tostring(quantity), 'EX', ttl)
    redis.call('INCRBY', 'stock:reserved_total:' .. item_id, quantity)
end

return persist('success', success_reply)
"""

_COMMIT_STOCK_LUA = """
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
local items = snapshot['items'] or {}
if next(items) == nil then
    return {'error', 'missing_prepare_items', ''}
end

for _, item in ipairs(items) do
    local item_id = tostring(item['item_id'] or '')
    local reservation_key = 'stock:reservation:' .. tx_id .. ':' .. item_id
    local raw_reserved = redis.call('GET', reservation_key)
    local reserved_qty = tonumber(raw_reserved)
    local stock_raw = redis.call('HGET', item_id, 'stock')
    local stock = tonumber(stock_raw)
    local reserved_total = tonumber(redis.call('GET', 'stock:reserved_total:' .. item_id) or '0')

    if item_id == '' or not reserved_qty or reserved_qty <= 0 then
        return {'error', 'missing_reservation', item_id}
    end
    if not stock then
        return {'error', 'missing_item', item_id}
    end
    if stock < reserved_qty then
        return {'error', 'stock_below_reservation', item_id}
    end
    if reserved_total < reserved_qty then
        return {'error', 'reserved_total_below_reservation', item_id}
    end
end

for _, item in ipairs(items) do
    local item_id = tostring(item['item_id'])
    local reservation_key = 'stock:reservation:' .. tx_id .. ':' .. item_id
    local reserved_qty = tonumber(redis.call('GET', reservation_key))
    redis.call('HINCRBY', item_id, 'stock', -reserved_qty)
    redis.call('DECRBY', 'stock:reserved_total:' .. item_id, reserved_qty)
    redis.call('DEL', reservation_key)
end

local entry = {
    tx_id = tx_id,
    action_type = 'COMMIT_STOCK',
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

_ABORT_STOCK_LUA = """
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
local items = {}
if prepare_succeeded then
    local snapshot = prepare_entry['business_snapshot'] or {}
    items = snapshot['items'] or {}
end

for _, item in ipairs(items) do
    local item_id = tostring(item['item_id'] or '')
    local reservation_key = 'stock:reservation:' .. tx_id .. ':' .. item_id
    local raw_reserved = redis.call('GET', reservation_key)
    local reserved_qty = tonumber(raw_reserved)
    if reserved_qty and reserved_qty > 0 then
        local reserved_total = tonumber(redis.call('GET', 'stock:reserved_total:' .. item_id) or '0')
        if reserved_total < reserved_qty then
            return {'error', 'reserved_total_below_reservation', item_id}
        end
    end
end

for _, item in ipairs(items) do
    local item_id = tostring(item['item_id'])
    local reservation_key = 'stock:reservation:' .. tx_id .. ':' .. item_id
    local raw_reserved = redis.call('GET', reservation_key)
    local reserved_qty = tonumber(raw_reserved)
    if reserved_qty and reserved_qty > 0 then
        redis.call('DECRBY', 'stock:reserved_total:' .. item_id, reserved_qty)
        redis.call('DEL', reservation_key)
    end
end

local entry = {
    tx_id = tx_id,
    action_type = 'ABORT_STOCK',
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

_prepare_stock_script = None
_commit_stock_script = None
_abort_stock_script = None


def init_2pc(db: redis_module.Redis, publish_fn, logger) -> None:
    global _db, _publish, _logger
    _db = db
    _publish = publish_fn
    _logger = logger


def _ledger_key(tx_id: str, action_type: str) -> str:
    return f"stock:ledger:{tx_id}:{action_type}"


def _merge_items(items: list[dict]) -> list[dict]:
    qty_by_item: dict[str, int] = {}
    for item in items or []:
        item_id = str(item["item_id"])
        qty_by_item[item_id] = qty_by_item.get(item_id, 0) + int(item["quantity"])
    return [
        {"item_id": item_id, "quantity": qty_by_item[item_id]}
        for item_id in sorted(qty_by_item.keys())
    ]


def _decode_scalar(value) -> str:
    return value.decode() if isinstance(value, bytes) else value


def _get_prepare_stock_script(db: redis_module.Redis):
    global _prepare_stock_script
    if _prepare_stock_script is None:
        _prepare_stock_script = db.register_script(_PREPARE_STOCK_LUA)
    return _prepare_stock_script


def _get_commit_stock_script(db: redis_module.Redis):
    global _commit_stock_script
    if _commit_stock_script is None:
        _commit_stock_script = db.register_script(_COMMIT_STOCK_LUA)
    return _commit_stock_script


def _get_abort_stock_script(db: redis_module.Redis):
    global _abort_stock_script
    if _abort_stock_script is None:
        _abort_stock_script = db.register_script(_ABORT_STOCK_LUA)
    return _abort_stock_script


def _apply_prepare_stock_atomically(
    db: redis_module.Redis,
    tx_id: str,
    order_id: str,
    items: list[dict],
) -> tuple[str, dict, str]:
    script = _get_prepare_stock_script(db)
    canonical_items = _merge_items(items)
    success_reply = build_stock_prepared(tx_id, order_id)
    failure_reply = build_stock_prepare_failed(tx_id, order_id, "")
    result = script(
        keys=[
            _ledger_key(tx_id, PREPARE_STOCK),
            _ledger_key(tx_id, ABORT_STOCK),
            _ledger_key(tx_id, COMMIT_STOCK),
        ],
        args=[
            tx_id,
            order_id,
            json.dumps(canonical_items),
            json.dumps(success_reply),
            json.dumps(failure_reply),
            stock_ledger.LEDGER_TTL_SECONDS,
        ],
        client=db,
    )

    state = _decode_scalar(result[0])
    if state == "error":
        raise RuntimeError(f"{_decode_scalar(result[1])}: {_decode_scalar(result[2])}")
    return state, json.loads(result[2]), _decode_scalar(result[1])


def _apply_commit_stock_atomically(
    db: redis_module.Redis,
    tx_id: str,
    order_id: str,
) -> tuple[str, dict]:
    script = _get_commit_stock_script(db)
    success_reply = build_stock_committed(tx_id, order_id)
    result = script(
        keys=[
            _ledger_key(tx_id, COMMIT_STOCK),
            _ledger_key(tx_id, PREPARE_STOCK),
            _ledger_key(tx_id, ABORT_STOCK),
        ],
        args=[
            tx_id,
            order_id,
            json.dumps(success_reply),
            stock_ledger.LEDGER_TTL_SECONDS,
        ],
        client=db,
    )

    state = _decode_scalar(result[0])
    if state == "error":
        raise RuntimeError(f"{_decode_scalar(result[1])}: {_decode_scalar(result[2])}")
    return state, json.loads(result[2])


def _apply_abort_stock_atomically(
    db: redis_module.Redis,
    tx_id: str,
    order_id: str,
) -> tuple[str, dict]:
    script = _get_abort_stock_script(db)
    success_reply = build_stock_aborted(tx_id, order_id)
    result = script(
        keys=[
            _ledger_key(tx_id, ABORT_STOCK),
            _ledger_key(tx_id, PREPARE_STOCK),
            _ledger_key(tx_id, COMMIT_STOCK),
        ],
        args=[
            tx_id,
            order_id,
            json.dumps(success_reply),
            stock_ledger.LEDGER_TTL_SECONDS,
        ],
        client=db,
    )

    state = _decode_scalar(result[0])
    if state == "error":
        raise RuntimeError(f"{_decode_scalar(result[1])}: {_decode_scalar(result[2])}")
    return state, json.loads(result[2])


def _handle_prepare_stock(msg, db, producer, logger):
    order_id = msg.get("order_id")
    tx_id = msg.get("tx_id")
    items = msg.get("payload", {}).get("items", [])

    try:
        state, reply, result = _apply_prepare_stock_atomically(db, tx_id, order_id, items)
    except RuntimeError as exc:
        logger.error(f"[Stock2PC] PREPARE_STOCK tx={tx_id} failed: {exc}")
        return

    producer(STOCK_EVENTS_TOPIC, reply)
    if state != LedgerState.REPLIED and reply.get("type") in {STOCK_PREPARED, STOCK_PREPARE_FAILED}:
        stock_ledger.mark_replied(db, tx_id, PREPARE_STOCK)
    logger.debug(f"[Stock2PC] order={order_id} PREPARE_STOCK {result}")


def _handle_commit_stock(msg, db, producer, logger):
    order_id = msg.get("order_id")
    tx_id = msg.get("tx_id")

    try:
        state, reply = _apply_commit_stock_atomically(db, tx_id, order_id)
    except RuntimeError as exc:
        logger.error(f"[Stock2PC] COMMIT_STOCK tx={tx_id} failed: {exc}")
        return

    producer(STOCK_EVENTS_TOPIC, reply)
    if state != LedgerState.REPLIED:
        stock_ledger.mark_replied(db, tx_id, COMMIT_STOCK)
    logger.debug(f"[Stock2PC] order={order_id} STOCK_COMMITTED")


def _handle_abort_stock(msg, db, producer, logger):
    order_id = msg.get("order_id")
    tx_id = msg.get("tx_id")

    try:
        state, reply = _apply_abort_stock_atomically(db, tx_id, order_id)
    except RuntimeError as exc:
        logger.error(f"[Stock2PC] ABORT_STOCK tx={tx_id} failed: {exc}")
        return

    producer(STOCK_EVENTS_TOPIC, reply)
    if state != LedgerState.REPLIED:
        stock_ledger.mark_replied(db, tx_id, ABORT_STOCK)
    logger.debug(f"[Stock2PC] order={order_id} STOCK_ABORTED")


def _2pc_route_stock(msg: dict, msg_type: str | None = None) -> None:
    msg_type = msg_type or msg.get("type")
    if msg_type == PREPARE_STOCK:
        _handle_prepare_stock(msg, _db, _publish, _logger)
    elif msg_type == COMMIT_STOCK:
        _handle_commit_stock(msg, _db, _publish, _logger)
    elif msg_type == ABORT_STOCK:
        _handle_abort_stock(msg, _db, _publish, _logger)
    else:
        _logger.debug(f"[Stock2PC] Unknown command type: {msg_type!r} — dropping")
