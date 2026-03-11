"""
stock/transaction_modes/saga.py

Saga command handlers for the stock service.

Handles:
    RESERVE_STOCK  → all-or-nothing stock reservation
    RELEASE_STOCK  → stock compensation (release previously reserved stock)

Each handler follows the ledger pattern:
    1. check ledger for (tx_id, action_type)
    2. if already replied  → re-emit stored reply, return
    3. if applied but not replied → publish stored reply, mark replied, return
    4. if no entry → process normally, write ledger at each stage
"""

from collections import defaultdict
import json
import time
import redis as redis_module
from msgspec import msgpack

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


# ============================================================
# PUBLIC ROUTE
# ============================================================


def saga_route_stock(msg: dict, db: redis_module.Redis, publish, logger) -> None:
    msg_type = msg.get("type")
    if msg_type == RESERVE_STOCK:
        _handle_reserve_stock(msg, db, publish, logger)
    elif msg_type == RELEASE_STOCK:
        _handle_release_stock(msg, db, publish, logger)
    else:
        logger.info(f"[StockSaga] Unknown command type: {msg_type!r} — dropping")


# ============================================================
# RESERVE_STOCK
# ============================================================


def _handle_reserve_stock(
    msg: dict,
    db: redis_module.Redis,
    publish,
    logger,
) -> None:
    tx_id = msg.get("tx_id")
    order_id = msg.get("order_id")
    items = msg.get("payload", {}).get("items", [])

    entry = stock_ledger.get_entry(db, tx_id, RESERVE_STOCK)

    if entry:
        state = entry.get("local_state")
        if state == LedgerState.REPLIED:
            logger.debug(f"[StockSaga] RESERVE_STOCK duplicate (replied) tx={tx_id}")
            _republish(entry, publish)
            return
        if state == LedgerState.APPLIED:
            logger.debug(f"[StockSaga] RESERVE_STOCK duplicate (applied) tx={tx_id}")
            _republish(entry, publish)
            stock_ledger.mark_replied(db, tx_id, RESERVE_STOCK)
            return

    if not entry:
        created = stock_ledger.create_entry(
            db=db,
            tx_id=tx_id,
            action_type=RESERVE_STOCK,
            business_snapshot={"items": items},
        )
        if not created:
            entry = stock_ledger.get_entry(db, tx_id, RESERVE_STOCK)
            if not entry:
                logger.error(f"[StockSaga] Failed to create/read RESERVE_STOCK ledger tx={tx_id}")
                return

    try:
        reply, result = _reserve_stock_atomically(
            db=db,
            tx_id=tx_id,
            order_id=order_id,
            items=items,
        )
    except RuntimeError as exc:
        logger.error(f"[StockSaga] RESERVE_STOCK atomic apply failed tx={tx_id}: {exc}")
        return

    publish(STOCK_EVENTS_TOPIC, reply)
    # Mark replied only after publish succeeds so restart replay can recover
    # the "applied locally, reply not sent" crash window.
    stock_ledger.mark_replied(db, tx_id, RESERVE_STOCK)
    logger.info(f"[StockSaga] RESERVE_STOCK {result} tx={tx_id} order={order_id}")



# ============================================================
# RELEASE_STOCK  (compensation)
# ============================================================


def _handle_release_stock(
    msg: dict,
    db: redis_module.Redis,
    publish,
    logger,
) -> None:
    tx_id = msg.get("tx_id")
    order_id = msg.get("order_id")
    items = msg.get("payload", {}).get("items", [])

    entry = stock_ledger.get_entry(db, tx_id, RELEASE_STOCK)

    if entry:
        state = entry.get("local_state")
        if state == LedgerState.REPLIED:
            logger.debug(f"[StockSaga] RELEASE_STOCK duplicate (replied) tx={tx_id}")
            _republish(entry, publish)
            return
        if state == LedgerState.APPLIED:
            logger.debug(f"[StockSaga] RELEASE_STOCK duplicate (applied) tx={tx_id}")
            _republish(entry, publish)
            stock_ledger.mark_replied(db, tx_id, RELEASE_STOCK)
            return

    if not entry:
        created = stock_ledger.create_entry(
            db=db,
            tx_id=tx_id,
            action_type=RELEASE_STOCK,
            business_snapshot={"items": items},
        )
        if not created:
            entry = stock_ledger.get_entry(db, tx_id, RELEASE_STOCK)
            if not entry:
                logger.error(f"[StockSaga] Failed to create/read RELEASE_STOCK ledger tx={tx_id}")
                return

    try:
        reply, _ = _release_stock_atomically(
            db=db,
            tx_id=tx_id,
            order_id=order_id,
            items=items,
        )
    except RuntimeError as exc:
        logger.error(f"[StockSaga] RELEASE_STOCK atomic apply failed tx={tx_id}: {exc}")
        return

    publish(STOCK_EVENTS_TOPIC, reply)
    # Compensation uses the same applied/replied split as reserve so duplicate
    # delivery and restart replay stay deterministic.
    stock_ledger.mark_replied(db, tx_id, RELEASE_STOCK)
    logger.info(f"[StockSaga] RELEASE_STOCK done tx={tx_id} order={order_id}")


# ============================================================
# INTERNAL HELPERS
# ============================================================

def _republish(entry: dict, publish) -> None:
    """Re-publish the stored reply event saved in the ledger entry."""
    reply_message = entry.get("reply_message")
    if reply_message:
        publish(STOCK_EVENTS_TOPIC, reply_message)

def _build_applied_entry(entry: dict, result: str, reply_message: dict) -> dict:
    updated = dict(entry)
    updated["local_state"] = LedgerState.APPLIED
    updated["result"] = result
    updated["reply_message"] = reply_message
    updated["updated_at_ms"] = int(time.time() * 1000)
    return updated


def _reserve_stock_atomically(
    db: redis_module.Redis,
    tx_id: str,
    order_id: str,
    items: list[dict],
) -> tuple[dict, str]:
    from app import StockValue

    ledger_key = f"stock:ledger:{tx_id}:{RESERVE_STOCK}"
    item_ids = sorted({str(item.get('item_id', '')) for item in items if item.get("item_id") is not None})

    while True:
        pipe = db.pipeline()
        try:
            # Every item touched by the reservation participates in the watch.
            # That lets Redis reject the transaction if concurrent activity
            # changed stock after we validated but before we commit.
            pipe.watch(ledger_key, *item_ids)

            raw_entry = pipe.get(ledger_key)
            if not raw_entry:
                pipe.unwatch()
                raise RuntimeError(f"Missing RESERVE_STOCK ledger entry for tx={tx_id}")

            entry = json.loads(raw_entry)
            state = entry.get("local_state")
            if state in (LedgerState.APPLIED, LedgerState.REPLIED):
                pipe.unwatch()
                return entry["reply_message"], entry.get("result", "failure")

            if not items:
                reply = build_stock_reservation_failed(tx_id, order_id, "No items in payload")
                updated_entry = _build_applied_entry(entry, "failure", reply)

                pipe.multi()
                pipe.set(ledger_key, json.dumps(updated_entry), ex=stock_ledger.LEDGER_TTL_SECONDS)
                pipe.execute()
                return reply, "failure"

            updated_items = {}

            for item_entry in items:
                item_id = str(item_entry.get("item_id", ""))
                quantity = item_entry.get("quantity")

                if not item_id or quantity is None:
                    reply = build_stock_reservation_failed(tx_id, order_id, "Invalid item payload")
                    updated_entry = _build_applied_entry(entry, "failure", reply)

                    pipe.multi()
                    pipe.set(ledger_key, json.dumps(updated_entry), ex=stock_ledger.LEDGER_TTL_SECONDS)
                    pipe.execute()
                    return reply, "failure"

                raw_item = pipe.get(item_id)
                if not raw_item:
                    reply = build_stock_reservation_failed(tx_id, order_id, f"Item {item_id} not found")
                    updated_entry = _build_applied_entry(entry, "failure", reply)

                    pipe.multi()
                    pipe.set(ledger_key, json.dumps(updated_entry), ex=stock_ledger.LEDGER_TTL_SECONDS)
                    pipe.execute()
                    return reply, "failure"

                stock_entry = msgpack.decode(raw_item, type=StockValue)
                quantity = int(quantity)

                if stock_entry.stock < quantity:
                    reply = build_stock_reservation_failed(
                        tx_id,
                        order_id,
                        f"Insufficient stock for item {item_id} (have {stock_entry.stock}, need {quantity})",
                    )
                    updated_entry = _build_applied_entry(entry, "failure", reply)

                    pipe.multi()
                    pipe.set(ledger_key, json.dumps(updated_entry), ex=stock_ledger.LEDGER_TTL_SECONDS)
                    pipe.execute()
                    return reply, "failure"

                updated_items[item_id] = StockValue(
                    stock=stock_entry.stock - quantity,
                    price=stock_entry.price,
                )

            reply = build_stock_reserved(tx_id, order_id)
            updated_entry = _build_applied_entry(entry, "success", reply)

            # Commit the entire reservation and the ledger transition in one
            # EXEC so stock is never partially deducted without a matching
            # durable participant state.
            pipe.multi()
            for item_id, updated_item in updated_items.items():
                pipe.set(item_id, msgpack.encode(updated_item))
            pipe.set(ledger_key, json.dumps(updated_entry), ex=stock_ledger.LEDGER_TTL_SECONDS)
            pipe.execute()
            return reply, "success"

        except redis_module.exceptions.WatchError:
            continue
        finally:
            pipe.reset()


def _release_stock_atomically(
    db: redis_module.Redis,
    tx_id: str,
    order_id: str,
    items: list[dict],
) -> tuple[dict, str]:
    from app import StockValue

    release_key = f"stock:ledger:{tx_id}:{RELEASE_STOCK}"
    reserve_key = f"stock:ledger:{tx_id}:{RESERVE_STOCK}"
    item_ids = sorted({str(item.get('item_id', '')) for item in items if item.get("item_id") is not None})

    while True:
        pipe = db.pipeline()
        try:
            # Compensation depends on both the original reserve result and the
            # current item rows, so they all need to be observed together.
            pipe.watch(release_key, reserve_key, *item_ids)

            raw_release = pipe.get(release_key)
            if not raw_release:
                pipe.unwatch()
                raise RuntimeError(f"Missing RELEASE_STOCK ledger entry for tx={tx_id}")

            release_entry = json.loads(raw_release)
            release_state = release_entry.get("local_state")
            if release_state in (LedgerState.APPLIED, LedgerState.REPLIED):
                pipe.unwatch()
                return release_entry["reply_message"], release_entry.get("result", "success")

            raw_reserve = pipe.get(reserve_key)
            reserve_entry = json.loads(raw_reserve) if raw_reserve else None
            reserve_succeeded = reserve_entry is not None and reserve_entry.get("result") == "success"

            reply = build_stock_released(tx_id, order_id)
            updated_release = _build_applied_entry(release_entry, "success", reply)

            if not reserve_succeeded:
                # If RESERVE_STOCK never succeeded, RELEASE_STOCK is a no-op
                # compensation. We still persist the terminal participant state
                # so retries remain idempotent.
                pipe.multi()
                pipe.set(release_key, json.dumps(updated_release), ex=stock_ledger.LEDGER_TTL_SECONDS)
                pipe.execute()
                return reply, "success"

            updated_items = {}

            for item_entry in items:
                item_id = str(item_entry.get("item_id", ""))
                quantity = int(item_entry.get("quantity", 0))

                raw_item = pipe.get(item_id)
                if not raw_item:
                    pipe.unwatch()
                    raise RuntimeError(f"Missing item {item_id} during RELEASE_STOCK for tx={tx_id}")

                stock_entry = msgpack.decode(raw_item, type=StockValue)
                updated_items[item_id] = StockValue(
                    stock=stock_entry.stock + quantity,
                    price=stock_entry.price,
                )

            pipe.multi()
            for item_id, updated_item in updated_items.items():
                pipe.set(item_id, msgpack.encode(updated_item))
            pipe.set(release_key, json.dumps(updated_release), ex=stock_ledger.LEDGER_TTL_SECONDS)
            pipe.execute()
            return reply, "success"

        except redis_module.exceptions.WatchError:
            continue
        finally:
            pipe.reset()
