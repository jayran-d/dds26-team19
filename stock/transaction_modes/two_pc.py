import json
import time

import redis as redis_module

from common.messages import (
    STOCK_EVENTS_TOPIC,
    PREPARE_STOCK,
    COMMIT_STOCK,
    ABORT_STOCK,
    STOCK_PREPARED,
    STOCK_PREPARE_FAILED,
    STOCK_COMMITTED,
    STOCK_ABORTED,
    build_message,
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


def init_2pc(db: redis_module.Redis, publish_fn, logger) -> None:
    global _db, _publish, _logger
    _db = db
    _publish = publish_fn
    _logger = logger


def _ledger_key(tx_id: str, action_type: str) -> str:
    return f"stock:ledger:{tx_id}:{action_type}"


def _reservation_key(tx_id: str, item_id: str) -> str:
    return f"stock:reservation:{tx_id}:{item_id}"


def _reserved_total_key(item_id: str) -> str:
    return f"stock:reserved_total:{item_id}"


def _decode_int(raw, default: int = 0) -> int:
    if raw is None:
        return default
    if isinstance(raw, bytes):
        raw = raw.decode()
    return int(raw)


def _merge_items(items: list[dict]) -> dict[str, int]:
    qty_by_item: dict[str, int] = {}
    for item in items or []:
        item_id = str(item["item_id"])
        qty_by_item[item_id] = qty_by_item.get(item_id, 0) + int(item["quantity"])
    return qty_by_item


def _build_applied_entry(entry: dict, result: str, response_event_type: str, response_payload: dict) -> dict:
    updated = dict(entry)
    updated["local_state"] = LedgerState.APPLIED
    updated["result"] = result
    updated["response_event_type"] = response_event_type
    updated["response_payload"] = response_payload
    updated["reply_message"] = build_message(
        entry["tx_id"],
        entry["business_snapshot"].get("order_id", ""),
        response_event_type,
        response_payload,
    )
    updated["updated_at_ms"] = int(time.time() * 1000)
    return updated


def _item_stock(pipe, item_id: str) -> int | None:
    raw = pipe.hget(item_id, 'stock')
    return int(raw) if raw is not None else None


def _prepare_stock_atomically(db: redis_module.Redis, tx_id: str, order_id: str, items: list[dict]) -> tuple[dict, str]:
    ledger_key = _ledger_key(tx_id, PREPARE_STOCK)
    abort_key = _ledger_key(tx_id, ABORT_STOCK)
    commit_key = _ledger_key(tx_id, COMMIT_STOCK)

    qty_by_item = _merge_items(items)
    item_ids = sorted(qty_by_item.keys())
    reserved_total_keys = [_reserved_total_key(item_id) for item_id in item_ids]
    reservation_keys = [_reservation_key(tx_id, item_id) for item_id in item_ids]

    while True:
        pipe = db.pipeline()
        try:
            pipe.watch(ledger_key, abort_key, commit_key, *item_ids, *reserved_total_keys, *reservation_keys)

            raw_abort = pipe.get(abort_key)
            if raw_abort:
                abort_entry = json.loads(raw_abort)
                if abort_entry.get("local_state") in (LedgerState.APPLIED, LedgerState.REPLIED):
                    pipe.unwatch()
                    return build_message(tx_id, order_id, abort_entry["response_event_type"], abort_entry.get("response_payload", {})), "aborted"

            raw_commit = pipe.get(commit_key)
            if raw_commit:
                commit_entry = json.loads(raw_commit)
                if commit_entry.get("local_state") in (LedgerState.APPLIED, LedgerState.REPLIED):
                    pipe.unwatch()
                    return build_message(tx_id, order_id, commit_entry["response_event_type"], commit_entry.get("response_payload", {})), "committed"

            raw_entry = pipe.get(ledger_key)
            if not raw_entry:
                pipe.unwatch()
                raise RuntimeError(f"Missing PREPARE_STOCK ledger entry for tx={tx_id}")
            entry = json.loads(raw_entry)
            if entry.get("local_state") in (LedgerState.APPLIED, LedgerState.REPLIED):
                pipe.unwatch()
                return build_message(tx_id, order_id, entry["response_event_type"], entry.get("response_payload", {})), entry.get("result", "success")

            existing_reservations = pipe.mget(reservation_keys)
            if any(raw is not None for raw in existing_reservations):
                reply = build_stock_prepared(tx_id, order_id)
                updated_entry = _build_applied_entry(entry, "success", STOCK_PREPARED, {})
                pipe.multi(); pipe.set(ledger_key, json.dumps(updated_entry), ex=stock_ledger.LEDGER_TTL_SECONDS); pipe.execute()
                return reply, "success"

            for item_id, raw_reserved in zip(item_ids, pipe.mget(reserved_total_keys)):
                stock = _item_stock(pipe, item_id)
                if stock is None:
                    reason = f"Item {item_id} not found"
                    reply = build_stock_prepare_failed(tx_id, order_id, reason)
                    updated = _build_applied_entry(entry, "failure", STOCK_PREPARE_FAILED, {"reason": reason})
                    pipe.multi(); pipe.set(ledger_key, json.dumps(updated), ex=stock_ledger.LEDGER_TTL_SECONDS); pipe.execute()
                    return reply, "failure"
                reserved_total = _decode_int(raw_reserved)
                needed = qty_by_item[item_id]
                available = stock - reserved_total
                if available < needed:
                    reason = f"Insufficient stock for item {item_id} (have {available}, need {needed})"
                    reply = build_stock_prepare_failed(tx_id, order_id, reason)
                    updated = _build_applied_entry(entry, "failure", STOCK_PREPARE_FAILED, {"reason": reason})
                    pipe.multi(); pipe.set(ledger_key, json.dumps(updated), ex=stock_ledger.LEDGER_TTL_SECONDS); pipe.execute()
                    return reply, "failure"

            reply = build_stock_prepared(tx_id, order_id)
            updated = _build_applied_entry(entry, "success", STOCK_PREPARED, {})
            pipe.multi()
            for item_id in item_ids:
                qty = qty_by_item[item_id]
                pipe.set(_reservation_key(tx_id, item_id), qty, ex=stock_ledger.LEDGER_TTL_SECONDS)
                pipe.incrby(_reserved_total_key(item_id), qty)
            pipe.set(ledger_key, json.dumps(updated), ex=stock_ledger.LEDGER_TTL_SECONDS)
            pipe.execute()
            return reply, "success"
        except redis_module.WatchError:
            continue
        finally:
            pipe.reset()


def _commit_stock_atomically(db: redis_module.Redis, tx_id: str, order_id: str) -> dict:
    commit_key = _ledger_key(tx_id, COMMIT_STOCK)
    prepare_key = _ledger_key(tx_id, PREPARE_STOCK)
    abort_key = _ledger_key(tx_id, ABORT_STOCK)
    prepare_entry = stock_ledger.get_entry(db, tx_id, PREPARE_STOCK)
    qty_by_item = _merge_items((prepare_entry or {}).get("business_snapshot", {}).get("items", []))
    item_ids = sorted(qty_by_item.keys())
    reserved_total_keys = [_reserved_total_key(item_id) for item_id in item_ids]
    reservation_keys = [_reservation_key(tx_id, item_id) for item_id in item_ids]
    while True:
        pipe = db.pipeline()
        try:
            pipe.watch(commit_key, prepare_key, abort_key, *item_ids, *reserved_total_keys, *reservation_keys)
            raw_commit = pipe.get(commit_key)
            if not raw_commit:
                pipe.unwatch(); raise RuntimeError(f"Missing COMMIT_STOCK ledger entry for tx={tx_id}")
            commit_entry = json.loads(raw_commit)
            if commit_entry.get("local_state") in (LedgerState.APPLIED, LedgerState.REPLIED):
                pipe.unwatch(); return build_message(tx_id, order_id, commit_entry["response_event_type"], commit_entry.get("response_payload", {}))
            raw_abort = pipe.get(abort_key)
            if raw_abort:
                abort_entry = json.loads(raw_abort)
                if abort_entry.get("local_state") in (LedgerState.APPLIED, LedgerState.REPLIED):
                    pipe.unwatch(); raise RuntimeError(f"ABORT_STOCK already applied for tx={tx_id}")
            raw_prepare = pipe.get(prepare_key)
            prepare_entry = json.loads(raw_prepare) if raw_prepare else None
            if not prepare_entry or prepare_entry.get("result") != "success":
                pipe.unwatch(); raise RuntimeError(f"Missing successful PREPARE_STOCK for tx={tx_id}")

            reserved_totals = pipe.mget(reserved_total_keys)
            per_tx_reserved = pipe.mget(reservation_keys)
            for item_id, raw_total, raw_tx_reserved in zip(item_ids, reserved_totals, per_tx_reserved):
                reserved_qty = _decode_int(raw_tx_reserved)
                if reserved_qty <= 0:
                    pipe.unwatch(); raise RuntimeError(f"Missing reservation for item {item_id} during COMMIT_STOCK tx={tx_id}")
                stock = _item_stock(pipe, item_id)
                if stock is None or stock < reserved_qty:
                    pipe.unwatch(); raise RuntimeError(f"Item {item_id} stock below reserved quantity during COMMIT_STOCK tx={tx_id}")
                if _decode_int(raw_total) < reserved_qty:
                    pipe.unwatch(); raise RuntimeError(f"Reserved total for item {item_id} below tx reservation during COMMIT_STOCK tx={tx_id}")

            updated = _build_applied_entry(commit_entry, "success", STOCK_COMMITTED, {})
            reply = build_stock_committed(tx_id, order_id)
            pipe.multi()
            for item_id, raw_tx_reserved in zip(item_ids, per_tx_reserved):
                reserved_qty = _decode_int(raw_tx_reserved)
                pipe.hincrby(item_id, 'stock', -reserved_qty)
                pipe.decrby(_reserved_total_key(item_id), reserved_qty)
                pipe.delete(_reservation_key(tx_id, item_id))
            pipe.set(commit_key, json.dumps(updated), ex=stock_ledger.LEDGER_TTL_SECONDS)
            pipe.execute()
            return reply
        except redis_module.WatchError:
            continue
        finally:
            pipe.reset()


def _abort_stock_atomically(db: redis_module.Redis, tx_id: str, order_id: str) -> dict:
    abort_key = _ledger_key(tx_id, ABORT_STOCK)
    prepare_key = _ledger_key(tx_id, PREPARE_STOCK)
    commit_key = _ledger_key(tx_id, COMMIT_STOCK)
    prepare_entry = stock_ledger.get_entry(db, tx_id, PREPARE_STOCK)
    qty_by_item = _merge_items((prepare_entry or {}).get("business_snapshot", {}).get("items", []))
    item_ids = sorted(qty_by_item.keys())
    reserved_total_keys = [_reserved_total_key(item_id) for item_id in item_ids]
    reservation_keys = [_reservation_key(tx_id, item_id) for item_id in item_ids]
    while True:
        pipe = db.pipeline()
        try:
            pipe.watch(abort_key, prepare_key, commit_key, *reserved_total_keys, *reservation_keys)
            raw_abort = pipe.get(abort_key)
            if not raw_abort:
                pipe.unwatch(); raise RuntimeError(f"Missing ABORT_STOCK ledger entry for tx={tx_id}")
            abort_entry = json.loads(raw_abort)
            if abort_entry.get("local_state") in (LedgerState.APPLIED, LedgerState.REPLIED):
                pipe.unwatch(); return build_message(tx_id, order_id, abort_entry["response_event_type"], abort_entry.get("response_payload", {}))
            raw_commit = pipe.get(commit_key)
            if raw_commit:
                commit_entry = json.loads(raw_commit)
                if commit_entry.get("local_state") in (LedgerState.APPLIED, LedgerState.REPLIED):
                    pipe.unwatch(); raise RuntimeError(f"COMMIT_STOCK already applied for tx={tx_id}")
            raw_prepare = pipe.get(prepare_key)
            prepare_entry = json.loads(raw_prepare) if raw_prepare else None
            prepare_succeeded = prepare_entry is not None and prepare_entry.get("result") == "success"
            reserved_totals = pipe.mget(reserved_total_keys) if prepare_succeeded else []
            per_tx_reserved = pipe.mget(reservation_keys) if prepare_succeeded else []
            updated = _build_applied_entry(abort_entry, "success", STOCK_ABORTED, {})
            reply = build_stock_aborted(tx_id, order_id)
            pipe.multi()
            if prepare_succeeded:
                for item_id, raw_total, raw_tx_reserved in zip(item_ids, reserved_totals, per_tx_reserved):
                    reserved_qty = _decode_int(raw_tx_reserved)
                    if reserved_qty > 0:
                        if _decode_int(raw_total) < reserved_qty:
                            pipe.reset(); raise RuntimeError(f"Reserved total for item {item_id} below tx reservation during ABORT_STOCK tx={tx_id}")
                        pipe.decrby(_reserved_total_key(item_id), reserved_qty)
                        pipe.delete(_reservation_key(tx_id, item_id))
            pipe.set(abort_key, json.dumps(updated), ex=stock_ledger.LEDGER_TTL_SECONDS)
            pipe.execute(); return reply
        except redis_module.WatchError:
            continue
        finally:
            pipe.reset()


def _handle_prepare_stock(msg, db, producer, logger):
    order_id = msg.get("order_id")
    tx_id = msg.get("tx_id")
    items = msg.get("payload", {}).get("items", [])
    logger.info(f"[Stock2PC] order={order_id} PREPARING_STOCK")
    abort_entry = stock_ledger.get_entry(db, tx_id, ABORT_STOCK)
    if abort_entry and abort_entry.get("local_state") in (LedgerState.APPLIED, LedgerState.REPLIED):
        producer(STOCK_EVENTS_TOPIC, build_message(tx_id, order_id, abort_entry["response_event_type"], abort_entry.get("response_payload", {}))); return
    entry = stock_ledger.get_entry(db, tx_id, PREPARE_STOCK)
    if entry and entry.get("local_state") in (LedgerState.APPLIED, LedgerState.REPLIED):
        producer(STOCK_EVENTS_TOPIC, build_message(tx_id, order_id, entry["response_event_type"], entry.get("response_payload", {})))
        stock_ledger.mark_replied(db, tx_id, PREPARE_STOCK); return
    if not entry:
        created = stock_ledger.create_entry(db, tx_id, PREPARE_STOCK, {"items": items, "order_id": order_id})
        if not created and not stock_ledger.get_entry(db, tx_id, PREPARE_STOCK):
            logger.error(f"[Stock2PC] order={order_id} failed to create/read PREPARE ledger"); return
    try:
        reply, result = _prepare_stock_atomically(db, tx_id, order_id, items)
    except RuntimeError as exc:
        logger.error(f"[Stock2PC] PREPARE_STOCK tx={tx_id} failed: {exc}"); return
    producer(STOCK_EVENTS_TOPIC, reply)
    if reply.get("type") in (STOCK_PREPARED, STOCK_PREPARE_FAILED):
        stock_ledger.mark_replied(db, tx_id, PREPARE_STOCK)
    logger.info(f"[Stock2PC] order={order_id} PREPARE_STOCK {result}")


def _handle_commit_stock(msg, db, producer, logger):
    order_id = msg.get("order_id"); tx_id = msg.get("tx_id")
    entry = stock_ledger.get_entry(db, tx_id, COMMIT_STOCK)
    if entry and entry.get("local_state") in (LedgerState.APPLIED, LedgerState.REPLIED):
        producer(STOCK_EVENTS_TOPIC, build_message(tx_id, order_id, entry["response_event_type"], entry.get("response_payload", {})))
        stock_ledger.mark_replied(db, tx_id, COMMIT_STOCK); return
    prepare_entry = stock_ledger.get_entry(db, tx_id, PREPARE_STOCK)
    if not prepare_entry or prepare_entry.get("result") != "success":
        logger.info(f"[Stock2PC] COMMIT_STOCK tx={tx_id} -- no successful PREPARE found, ignoring"); return
    if not entry:
        created = stock_ledger.create_entry(db, tx_id, COMMIT_STOCK, {"order_id": order_id})
        if not created and not stock_ledger.get_entry(db, tx_id, COMMIT_STOCK):
            logger.error(f"[Stock2PC] order={order_id} failed to create/read COMMIT ledger"); return
    try:
        reply = _commit_stock_atomically(db, tx_id, order_id)
    except RuntimeError as exc:
        logger.error(f"[Stock2PC] COMMIT_STOCK tx={tx_id} failed: {exc}"); return
    producer(STOCK_EVENTS_TOPIC, reply); stock_ledger.mark_replied(db, tx_id, COMMIT_STOCK)
    logger.info(f"[Stock2PC] order={order_id} STOCK_COMMITTED")


def _handle_abort_stock(msg, db, producer, logger):
    order_id = msg.get("order_id"); tx_id = msg.get("tx_id")
    entry = stock_ledger.get_entry(db, tx_id, ABORT_STOCK)
    if entry and entry.get("local_state") in (LedgerState.APPLIED, LedgerState.REPLIED):
        producer(STOCK_EVENTS_TOPIC, build_message(tx_id, order_id, entry["response_event_type"], entry.get("response_payload", {})))
        stock_ledger.mark_replied(db, tx_id, ABORT_STOCK); return
    if not entry:
        created = stock_ledger.create_entry(db, tx_id, ABORT_STOCK, {"order_id": order_id})
        if not created and not stock_ledger.get_entry(db, tx_id, ABORT_STOCK):
            logger.error(f"[Stock2PC] order={order_id} failed to create/read ABORT ledger"); return
    try:
        reply = _abort_stock_atomically(db, tx_id, order_id)
    except RuntimeError as exc:
        logger.error(f"[Stock2PC] ABORT_STOCK tx={tx_id} failed: {exc}"); return
    producer(STOCK_EVENTS_TOPIC, reply); stock_ledger.mark_replied(db, tx_id, ABORT_STOCK)
    logger.info(f"[Stock2PC] order={order_id} STOCK_ABORTED")


def _2pc_route_stock(msg: dict, msg_type: str | None = None) -> None:
    msg_type = msg_type or msg.get("type")
    if msg_type == PREPARE_STOCK:
        _handle_prepare_stock(msg, _db, _publish, _logger)
    elif msg_type == COMMIT_STOCK:
        _handle_commit_stock(msg, _db, _publish, _logger)
    elif msg_type == ABORT_STOCK:
        _handle_abort_stock(msg, _db, _publish, _logger)
    else:
        _logger.info(f"[Stock2PC] Unknown command type: {msg_type!r} — dropping")
