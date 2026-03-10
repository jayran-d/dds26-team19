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
    
    from app import apply_stock_delta, get_item_from_db

    tx_id = msg.get("tx_id")
    order_id = msg.get("order_id")
    items = msg.get("payload", {}).get("items", [])

    # ── Step 1: check ledger ───────────────────────────────────────────────────
    entry = stock_ledger.get_entry(db, tx_id, RESERVE_STOCK)

    if entry:
        state = entry.get("local_state")

        if state == LedgerState.REPLIED:
            # Already fully handled — re-emit stored reply and return.
            logger.info(
                f"[StockSaga] RESERVE_STOCK duplicate (replied) tx={tx_id} — re-emitting"
            )
            _republish(entry, tx_id, order_id, publish)
            return

        if state == LedgerState.APPLIED:
            # Applied but reply was lost — publish stored reply and mark replied.
            logger.info(
                f"[StockSaga] RESERVE_STOCK duplicate (applied) tx={tx_id} — republishing reply"
            )
            _republish(entry, tx_id, order_id, publish)
            stock_ledger.mark_replied(db, tx_id, RESERVE_STOCK)
            return

        # state == received: a previous attempt started but didn't finish.
        # Fall through and reprocess — the NX guard means we won't double-create.

    # ── Step 2: create ledger entry (received) ─────────────────────────────────
    if not entry:
        stock_ledger.create_entry(
            db=db,
            tx_id=tx_id,
            action_type=RESERVE_STOCK,
            business_snapshot={"items": items},
        )

    if not items:
        _fail(
            db, tx_id, order_id, RESERVE_STOCK, "No items in payload", publish, logger
        )
        return

    # ── Step 3: validate all items (all-or-nothing) ────────────────────────────
    for item_entry in items:
        item_id = str(item_entry.get("item_id", ""))
        quantity = item_entry.get("quantity")

        if not item_id or quantity is None:
            _fail(
                db,
                tx_id,
                order_id,
                RESERVE_STOCK,
                "Invalid item payload",
                publish,
                logger,
            )
            return

        item = get_item_from_db(item_id)
        if item is None:
            _fail(
                db,
                tx_id,
                order_id,
                RESERVE_STOCK,
                f"Item {item_id} not found",
                publish,
                logger,
            )
            return

        if item.stock < int(quantity):
            _fail(
                db,
                tx_id,
                order_id,
                RESERVE_STOCK,
                f"Insufficient stock for item {item_id} (have {item.stock}, need {quantity})",
                publish,
                logger,
            )
            return

    # ── Step 4: subtract all items ─────────────────────────────────────────────
    for item_entry in items:
        item_id = str(item_entry.get("item_id"))
        quantity = int(item_entry.get("quantity"))
        success, error, _ = apply_stock_delta(item_id, -quantity)
        if not success:
            # Extremely unlikely — we just validated. Treat as failure.
            _fail(db, tx_id, order_id, RESERVE_STOCK, error, publish, logger)
            return

    # ── Step 5: mark applied ───────────────────────────────────────────────────
    stock_ledger.mark_applied(
        db=db,
        tx_id=tx_id,
        action_type=RESERVE_STOCK,
        result="success",
        response_event_type="STOCK_RESERVED",
        response_payload={},
    )

    # ── Step 6: publish reply ──────────────────────────────────────────────────
    reply = build_stock_reserved(tx_id, order_id)
    publish(STOCK_EVENTS_TOPIC, reply)
    logger.info(f"[StockSaga] RESERVE_STOCK success tx={tx_id} order={order_id}")

    # ── Step 7: mark replied ───────────────────────────────────────────────────
    stock_ledger.mark_replied(db, tx_id, RESERVE_STOCK)


# ============================================================
# RELEASE_STOCK  (compensation)
# ============================================================


def _handle_release_stock(
    msg: dict,
    db: redis_module.Redis,
    publish,
    logger,
) -> None:
    from app import apply_stock_delta

    tx_id = msg.get("tx_id")
    order_id = msg.get("order_id")
    items = msg.get("payload", {}).get("items", [])

    # ── Step 1: check ledger ───────────────────────────────────────────────────
    entry = stock_ledger.get_entry(db, tx_id, RELEASE_STOCK)

    if entry:
        state = entry.get("local_state")
        if state == LedgerState.REPLIED:
            logger.info(
                f"[StockSaga] RELEASE_STOCK duplicate (replied) tx={tx_id} — re-emitting"
            )
            _republish(entry, tx_id, order_id, publish)
            return
        if state == LedgerState.APPLIED:
            logger.info(
                f"[StockSaga] RELEASE_STOCK duplicate (applied) tx={tx_id} — republishing"
            )
            _republish(entry, tx_id, order_id, publish)
            stock_ledger.mark_replied(db, tx_id, RELEASE_STOCK)
            return

    # ── Step 2: check whether RESERVE_STOCK ever succeeded ────────────────────
    # If stock was never reserved, compensation is a no-op — but we still reply.
    reserve_entry = stock_ledger.get_entry(db, tx_id, RESERVE_STOCK)
    reserve_succeeded = (
        reserve_entry is not None and reserve_entry.get("result") == "success"
    )

    # ── Step 3: create ledger entry ───────────────────────────────────────────
    if not entry:
        stock_ledger.create_entry(
            db=db,
            tx_id=tx_id,
            action_type=RELEASE_STOCK,
            business_snapshot={"items": items},
        )

    # ── Step 4: restore stock only if it was actually reserved ────────────────
    if reserve_succeeded:
        for item_entry in items:
            item_id = str(item_entry.get("item_id", ""))
            quantity = int(item_entry.get("quantity", 0))
            success, error, _ = apply_stock_delta(item_id, quantity)
            if not success:
                logger.error(
                    f"[StockSaga] Failed to release stock for item {item_id}: {error}"
                )
    else:
        logger.info(
            f"[StockSaga] RELEASE_STOCK tx={tx_id} — stock was never reserved, no-op"
        )

    # ── Step 5: mark applied ───────────────────────────────────────────────────
    stock_ledger.mark_applied(
        db=db,
        tx_id=tx_id,
        action_type=RELEASE_STOCK,
        result="success",
        response_event_type="STOCK_RELEASED",
        response_payload={},
    )

    # ── Step 6: publish reply ──────────────────────────────────────────────────
    reply = build_stock_released(tx_id, order_id)
    publish(STOCK_EVENTS_TOPIC,reply)
    logger.info(f"[StockSaga] RELEASE_STOCK done tx={tx_id} order={order_id}")

    # ── Step 7: mark replied ───────────────────────────────────────────────────
    stock_ledger.mark_replied(db, tx_id, RELEASE_STOCK)


# ============================================================
# INTERNAL HELPERS
# ============================================================


def _fail(
    db: redis_module.Redis,
    tx_id: str,
    order_id: str,
    action_type: str,
    reason: str,
    publish,
    logger,
) -> None:
    """Mark ledger applied (failure) and publish STOCK_RESERVATION_FAILED."""
    stock_ledger.mark_applied(
        db=db,
        tx_id=tx_id,
        action_type=action_type,
        result="failure",
        response_event_type="STOCK_RESERVATION_FAILED",
        response_payload={"reason": reason},
    )
    reply = build_stock_reservation_failed(tx_id, order_id, reason)
    publish(STOCK_EVENTS_TOPIC, reply)
    logger.info(f"[StockSaga] RESERVE_STOCK failed tx={tx_id}: {reason}")
    stock_ledger.mark_replied(db, tx_id, action_type)


def _republish(
    entry: dict,
    tx_id: str,
    order_id: str,
    publish,
) -> None:
    """Rebuild and re-publish the stored reply event from the ledger entry."""
    from common.messages import build_message

    event_type = entry.get("response_event_type")
    payload = entry.get("response_payload", {})
    msg = build_message(tx_id, order_id, event_type, payload)
    publish(STOCK_EVENTS_TOPIC, msg)
