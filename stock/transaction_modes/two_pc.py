import json
import time

import redis as redis_module
from msgspec import msgpack

from common.messages import (
    STOCK_EVENTS_TOPIC,
    PREPARE_STOCK,
    STOCK_PREPARE_FAILED,
    STOCK_PREPARED,
    COMMIT_STOCK,
    STOCK_COMMITTED,
    STOCK_COMMIT_CONFIRMED,
    ABORT_STOCK,
    STOCK_ABORTED,
    build_message,
    build_stock_aborted,
    build_stock_committed,
    build_stock_prepare_failed,
    build_stock_prepared,
)

import ledger as stock_ledger
from ledger import LedgerState
from common.kafka_client import KafkaProducerClient


LOCK_TTL = 10000  # milliseconds

def _2pc_route_stock(msg: dict, db: redis_module.Redis, producer: KafkaProducerClient, logger) -> None:
    msg_type = msg.get("type")
    if msg_type == PREPARE_STOCK:
        _handle_prepare_stock(msg, db, producer, logger)
    elif msg_type == COMMIT_STOCK:
        _handle_commit_stock(msg, db, producer, logger)
    elif msg_type == ABORT_STOCK:
        _handle_abort_stock(msg, db, producer, logger)
    else:
        logger.info(f"[Stock2PC] Unknown command type: {msg_type!r} — dropping")
    

def _ledger_key(tx_id: str, action_type: str) -> str:
    return f"stock:ledger:{tx_id}:{action_type}"


def _build_applied_entry(
    entry: dict,
    result: str,
    response_event_type: str,
    response_payload: dict,
) -> dict:
    updated = dict(entry)
    updated["local_state"] = LedgerState.APPLIED
    updated["result"] = result
    updated["response_event_type"] = response_event_type
    updated["response_payload"] = response_payload
    updated["updated_at_ms"] = int(time.time() * 1000)
    return updated


def _merge_items(items: list[dict]) -> dict[str, int]:
    qty_by_item: dict[str, int] = {}
    for item in items:
        item_id = str(item.get("item_id", ""))
        quantity = item.get("quantity")
        if not item_id or quantity is None:
            raise RuntimeError("Invalid item payload")
        qty_by_item[item_id] = qty_by_item.get(item_id, 0) + int(quantity)
    return qty_by_item


def _prepare_stock_atomically(
    db: redis_module.Redis,
    tx_id: str,
    order_id: str,
    items: list[dict],
) -> tuple[dict, str]:
    from app import StockValue

    if not items:
        raise RuntimeError("No items in payload")

    qty_by_item = _merge_items(items)
    item_ids = sorted(qty_by_item.keys())
    ledger_key = _ledger_key(tx_id, PREPARE_STOCK)
    abort_key = _ledger_key(tx_id, ABORT_STOCK)

    while True:
        pipe = db.pipeline()
        try:
            pipe.watch(ledger_key, abort_key, *item_ids)

            raw_abort = pipe.get(abort_key)
            if raw_abort:
                abort_entry = json.loads(raw_abort)
                if abort_entry.get("local_state") in (LedgerState.APPLIED, LedgerState.REPLIED):
                    pipe.unwatch()
                    reply = build_message(
                        tx_id,
                        order_id,
                        abort_entry["response_event_type"],
                        abort_entry.get("response_payload", {}),
                    )
                    return reply, "aborted"

            raw_entry = pipe.get(ledger_key)
            if not raw_entry:
                pipe.unwatch()
                raise RuntimeError(f"Missing PREPARE_STOCK ledger entry for tx={tx_id}")

            entry = json.loads(raw_entry)
            state = entry.get("local_state")
            if state in (LedgerState.APPLIED, LedgerState.REPLIED):
                pipe.unwatch()
                reply = build_message(
                    tx_id,
                    order_id,
                    entry["response_event_type"],
                    entry.get("response_payload", {}),
                )
                return reply, entry.get("result", "success")

            current_rows = pipe.mget(item_ids)
            updated_rows: dict[str, bytes] = {}

            for item_id, raw in zip(item_ids, current_rows):
                if not raw:
                    reason = f"Item {item_id} not found"
                    reply = build_stock_prepare_failed(tx_id, order_id, reason)
                    updated_entry = _build_applied_entry(
                        entry,
                        "failure",
                        STOCK_PREPARE_FAILED,
                        {"reason": reason},
                    )

                    pipe.multi()
                    pipe.set(ledger_key, json.dumps(updated_entry), ex=stock_ledger.LEDGER_TTL_SECONDS)
                    pipe.execute()
                    return reply, "failure"

                item_entry = msgpack.decode(raw, type=StockValue)
                needed = qty_by_item[item_id]
                if item_entry.stock < needed:
                    reason = f"Insufficient stock for item {item_id} (have {item_entry.stock}, need {needed})"
                    reply = build_stock_prepare_failed(tx_id, order_id, reason)
                    updated_entry = _build_applied_entry(
                        entry,
                        "failure",
                        STOCK_PREPARE_FAILED,
                        {"reason": reason},
                    )

                    pipe.multi()
                    pipe.set(ledger_key, json.dumps(updated_entry), ex=stock_ledger.LEDGER_TTL_SECONDS)
                    pipe.execute()
                    return reply, "failure"

                updated_item = StockValue(
                    stock=item_entry.stock - needed,
                    price=item_entry.price,
                )
                updated_rows[item_id] = msgpack.encode(updated_item)

            reply = build_stock_prepared(tx_id, order_id)
            updated_entry = _build_applied_entry(entry, "success", STOCK_PREPARED, {})

            pipe.multi()
            for item_id, encoded in updated_rows.items():
                pipe.set(item_id, encoded)
            pipe.set(ledger_key, json.dumps(updated_entry), ex=stock_ledger.LEDGER_TTL_SECONDS)
            pipe.execute()
            return reply, "success"

        except redis_module.WatchError:
            continue
        finally:
            pipe.reset()


def _abort_stock_atomically(
    db: redis_module.Redis,
    tx_id: str,
    order_id: str,
) -> dict:
    from app import StockValue

    abort_key = _ledger_key(tx_id, ABORT_STOCK)
    prepare_key = _ledger_key(tx_id, PREPARE_STOCK)
    prepare_entry = stock_ledger.get_entry(db, tx_id, PREPARE_STOCK)

    qty_by_item: dict[str, int] = {}
    item_ids: list[str] = []
    if prepare_entry:
        snapshot = prepare_entry.get("business_snapshot", {})
        items = snapshot.get("items", [])
        if items:
            qty_by_item = _merge_items(items)
            item_ids = sorted(qty_by_item.keys())

    while True:
        pipe = db.pipeline()
        try:
            pipe.watch(abort_key, prepare_key, *item_ids)

            raw_abort = pipe.get(abort_key)
            if not raw_abort:
                pipe.unwatch()
                raise RuntimeError(f"Missing ABORT_STOCK ledger entry for tx={tx_id}")

            abort_entry = json.loads(raw_abort)
            state = abort_entry.get("local_state")
            if state in (LedgerState.APPLIED, LedgerState.REPLIED):
                pipe.unwatch()
                return build_message(
                    tx_id,
                    order_id,
                    abort_entry["response_event_type"],
                    abort_entry.get("response_payload", {}),
                )

            raw_prepare = pipe.get(prepare_key)
            prepare_entry = json.loads(raw_prepare) if raw_prepare else None
            reserve_succeeded = (
                prepare_entry is not None and prepare_entry.get("result") == "success"
            )
            if prepare_entry and not item_ids:
                snapshot = prepare_entry.get("business_snapshot", {})
                discovered_items = snapshot.get("items", [])
                if discovered_items:
                    qty_by_item = _merge_items(discovered_items)
                    item_ids = sorted(qty_by_item.keys())
                    pipe.unwatch()
                    continue

            updated_abort = _build_applied_entry(abort_entry, "success", STOCK_ABORTED, {})
            reply = build_stock_aborted(tx_id, order_id)

            if not reserve_succeeded or not item_ids:
                pipe.multi()
                pipe.set(abort_key, json.dumps(updated_abort), ex=stock_ledger.LEDGER_TTL_SECONDS)
                pipe.execute()
                return reply

            current_rows = pipe.mget(item_ids)
            updated_rows: dict[str, bytes] = {}

            for item_id, raw in zip(item_ids, current_rows):
                if not raw:
                    pipe.unwatch()
                    raise RuntimeError(f"Missing item row {item_id} during ABORT_STOCK tx={tx_id}")

                item_entry = msgpack.decode(raw, type=StockValue)
                updated_item = StockValue(
                    stock=item_entry.stock + qty_by_item[item_id],
                    price=item_entry.price,
                )
                updated_rows[item_id] = msgpack.encode(updated_item)

            pipe.multi()
            for item_id, encoded in updated_rows.items():
                pipe.set(item_id, encoded)
            pipe.set(abort_key, json.dumps(updated_abort), ex=stock_ledger.LEDGER_TTL_SECONDS)
            pipe.execute()
            return reply

        except redis_module.WatchError:
            continue
        finally:
            pipe.reset()


# def _handle_prepare_stock(
#     msg: dict,
#     db: redis_module.Redis,
#     producer,
#     logger,
# ) -> None:
#     logger.info(f"[Stock2PC] - Handle-Reserve-Stock XX Not yet implemented\n")
#     return

# def _handle_commit_stock(
#     msg: dict,
#     db: redis_module.Redis,
#     publish,
#     logger,
# ) -> None:
#     logger.info(f"[Stock2PC] - Handle-Commit-Stock XX Not yet implemented\n")
#     return 

# def _handle_abort_stock(
#     msg: dict,
#     db: redis_module.Redis,
#     publish,
#     logger,
# ) -> None:
#     logger.info(f"[Stock2PC] - Handle-Abort-Stock XX Not yet implemented\n")
#     return


# ---------------- PREPARE ----------------
def _handle_prepare_stock(msg, db, producer, logger):
    order_id = msg.get("order_id")
    tx_id = msg.get("tx_id")
    items = msg.get("payload", {}).get("items", [])
    action_type = PREPARE_STOCK

    logger.info(f"[Stock2PC] order={order_id} PREPARING_STOCK")

    abort_entry = stock_ledger.get_entry(db, tx_id, ABORT_STOCK)
    if abort_entry and abort_entry.get("local_state") in (LedgerState.APPLIED, LedgerState.REPLIED):
        producer.publish(
            STOCK_EVENTS_TOPIC,
            build_message(
                tx_id,
                order_id,
                abort_entry["response_event_type"],
                abort_entry.get("response_payload", {}),
            ),
        )
        return

    entry = stock_ledger.get_entry(db, tx_id, action_type)
    if entry:
        if entry["local_state"] in (LedgerState.APPLIED, LedgerState.REPLIED):
            producer.publish(
                STOCK_EVENTS_TOPIC,
                build_message(
                    tx_id,
                    order_id,
                    entry["response_event_type"],
                    entry.get("response_payload", {}),
                ),
            )
            stock_ledger.mark_replied(db, tx_id, action_type)
            return

    if not entry:
        created = stock_ledger.create_entry(
            db,
            tx_id,
            action_type,
            {"items": items},
        )
        if not created:
            entry = stock_ledger.get_entry(db, tx_id, action_type)
            if not entry:
                logger.error(f"[Stock2PC] order={order_id} failed to create/read PREPARE ledger")
                return

    try:
        reply, result = _prepare_stock_atomically(db, tx_id, order_id, items)
    except RuntimeError as exc:
        logger.error(f"[Stock2PC] PREPARE_STOCK tx={tx_id} failed: {exc}")
        return

    producer.publish(STOCK_EVENTS_TOPIC, reply)
    if reply.get("type") in (STOCK_PREPARED, STOCK_PREPARE_FAILED):
        stock_ledger.mark_replied(db, tx_id, action_type)
    logger.info(f"[Stock2PC] order={order_id} PREPARE_STOCK {result}")


# ---------------- COMMIT ----------------
def _handle_commit_stock(msg, db, producer, logger):
    order_id = msg.get("order_id")
    tx_id = msg.get("tx_id")

    entry = stock_ledger.get_entry(db, tx_id, COMMIT_STOCK)
    if entry and entry.get("local_state") in (LedgerState.APPLIED, LedgerState.REPLIED):
        producer.publish(
            STOCK_EVENTS_TOPIC,
            build_message(
                tx_id,
                order_id,
                entry["response_event_type"],
                entry.get("response_payload", {}),
            ),
        )
        stock_ledger.mark_replied(db, tx_id, COMMIT_STOCK)
        return

    prepare_entry = stock_ledger.get_entry(db, tx_id, PREPARE_STOCK)
    if not prepare_entry or prepare_entry.get("result") != "success":
        logger.info(f"[Stock2PC] COMMIT_STOCK tx={tx_id} -- no successful PREPARE found, ignoring")
        return

    if not entry:
        created = stock_ledger.create_entry(db, tx_id, COMMIT_STOCK, {})
        if not created:
            entry = stock_ledger.get_entry(db, tx_id, COMMIT_STOCK)
            if not entry:
                logger.error(f"[Stock2PC] order={order_id} failed to create/read COMMIT ledger")
                return

    ok = stock_ledger.mark_applied(db, tx_id, COMMIT_STOCK, "success", STOCK_COMMITTED, {})
    if not ok:
        logger.error(f"[Stock2PC] order={order_id} failed to mark COMMIT_STOCK applied")
        return

    producer.publish(STOCK_EVENTS_TOPIC, build_stock_committed(tx_id, order_id))
    stock_ledger.mark_replied(db, tx_id, COMMIT_STOCK)
    logger.info(f"[Stock2PC] order={order_id} STOCK_COMMITTED")



# ---------------- ABORT ----------------
def _handle_abort_stock(msg, db, producer, logger):
    order_id = msg.get("order_id")
    tx_id = msg.get("tx_id")

    entry = stock_ledger.get_entry(db, tx_id, ABORT_STOCK)
    if entry and entry.get("local_state") in (LedgerState.APPLIED, LedgerState.REPLIED):
        producer.publish(
            STOCK_EVENTS_TOPIC,
            build_message(
                tx_id,
                order_id,
                entry["response_event_type"],
                entry.get("response_payload", {}),
            ),
        )
        stock_ledger.mark_replied(db, tx_id, ABORT_STOCK)
        return

    if not entry:
        created = stock_ledger.create_entry(db, tx_id, ABORT_STOCK, {})
        if not created:
            entry = stock_ledger.get_entry(db, tx_id, ABORT_STOCK)
            if not entry:
                logger.error(f"[Stock2PC] order={order_id} failed to create/read ABORT ledger")
                return

    try:
        reply = _abort_stock_atomically(db, tx_id, order_id)
    except RuntimeError as exc:
        logger.error(f"[Stock2PC] ABORT_STOCK tx={tx_id} failed: {exc}")
        return

    producer.publish(STOCK_EVENTS_TOPIC, reply)
    stock_ledger.mark_replied(db, tx_id, ABORT_STOCK)
    logger.info(f"[Stock2PC] order={order_id} STOCK_ABORTED")

    
