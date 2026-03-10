"""
payment/transaction_modes/saga.py

Saga command handlers for the payment service.

Handles:
    PROCESS_PAYMENT  → charge user credit
    REFUND_PAYMENT   → refund user credit (compensation)

Each handler follows the same ledger pattern as stock/saga.py.
"""

import redis as redis_module

from common.messages import (
    PROCESS_PAYMENT,
    REFUND_PAYMENT,
    build_payment_success,
    build_payment_failed,
    build_payment_refunded,
    build_message,
)
import ledger as payment_ledger
from ledger import LedgerState


# ============================================================
# PUBLIC ROUTE
# ============================================================

def route(msg: dict, db: redis_module.Redis, publish, logger) -> None:
    msg_type = msg.get("type")
    if msg_type == PROCESS_PAYMENT:
        _handle_process_payment(msg, db, publish, logger)
    elif msg_type == REFUND_PAYMENT:
        _handle_refund_payment(msg, db, publish, logger)
    else:
        logger.warning(f"[PaymentSaga] Unknown command type: {msg_type!r} — dropping")


# ============================================================
# PROCESS_PAYMENT
# ============================================================

def _handle_process_payment(
    msg:     dict,
    db:      redis_module.Redis,
    publish,
    logger,
) -> None:
    from app import remove_credit_internal

    tx_id    = msg.get("tx_id")
    order_id = msg.get("order_id")
    payload  = msg.get("payload", {})
    user_id  = str(payload.get("user_id", ""))
    amount   = payload.get("amount")

    # ── Step 1: check ledger ───────────────────────────────────────────────────
    entry = payment_ledger.get_entry(db, tx_id, PROCESS_PAYMENT)

    if entry:
        state = entry.get("local_state")
        if state == LedgerState.REPLIED:
            logger.debug(f"[PaymentSaga] PROCESS_PAYMENT duplicate (replied) tx={tx_id} — re-emitting")
            _republish(entry, tx_id, order_id, publish)
            return
        if state == LedgerState.APPLIED:
            logger.debug(f"[PaymentSaga] PROCESS_PAYMENT duplicate (applied) tx={tx_id} — republishing")
            _republish(entry, tx_id, order_id, publish)
            payment_ledger.mark_replied(db, tx_id, PROCESS_PAYMENT)
            return

    # ── Step 2: validate payload ───────────────────────────────────────────────
    if not user_id or amount is None:
        logger.error(f"[PaymentSaga] Invalid PROCESS_PAYMENT payload: {msg}")
        return

    # ── Step 3: create ledger entry (received) ─────────────────────────────────
    if not entry:
        payment_ledger.create_entry(
            db          = db,
            tx_id       = tx_id,
            action_type = PROCESS_PAYMENT,
            business_snapshot = {"user_id": user_id, "amount": int(amount)},
        )

    # ── Step 4: apply business effect ─────────────────────────────────────────
    success, error, _ = remove_credit_internal(user_id, int(amount))

    if success:
        response_event_type = "PAYMENT_SUCCESS"
        response_payload    = {}
        reply               = build_payment_success(tx_id, order_id)
        result              = "success"
    else:
        response_event_type = "PAYMENT_FAILED"
        response_payload    = {"reason": error}
        reply               = build_payment_failed(tx_id, order_id, error)
        result              = "failure"

    # ── Step 5: mark applied ───────────────────────────────────────────────────
    payment_ledger.mark_applied(
        db                  = db,
        tx_id               = tx_id,
        action_type         = PROCESS_PAYMENT,
        result              = result,
        response_event_type = response_event_type,
        response_payload    = response_payload,
    )

    # ── Step 6: publish reply ──────────────────────────────────────────────────
    publish(reply)
    logger.debug(f"[PaymentSaga] PROCESS_PAYMENT {result} tx={tx_id} order={order_id}")

    # ── Step 7: mark replied ───────────────────────────────────────────────────
    payment_ledger.mark_replied(db, tx_id, PROCESS_PAYMENT)


# ============================================================
# REFUND_PAYMENT  (compensation)
# ============================================================

def _handle_refund_payment(
    msg:     dict,
    db:      redis_module.Redis,
    publish,
    logger,
) -> None:
    from app import add_credit_internal

    tx_id    = msg.get("tx_id")
    order_id = msg.get("order_id")
    payload  = msg.get("payload", {})
    user_id  = str(payload.get("user_id", ""))
    amount   = payload.get("amount")

    # ── Step 1: check ledger ───────────────────────────────────────────────────
    entry = payment_ledger.get_entry(db, tx_id, REFUND_PAYMENT)

    if entry:
        state = entry.get("local_state")
        if state == LedgerState.REPLIED:
            logger.debug(f"[PaymentSaga] REFUND_PAYMENT duplicate (replied) tx={tx_id} — re-emitting")
            _republish(entry, tx_id, order_id, publish)
            return
        if state == LedgerState.APPLIED:
            logger.debug(f"[PaymentSaga] REFUND_PAYMENT duplicate (applied) tx={tx_id} — republishing")
            _republish(entry, tx_id, order_id, publish)
            payment_ledger.mark_replied(db, tx_id, REFUND_PAYMENT)
            return

    if not user_id or amount is None:
        logger.error(f"[PaymentSaga] Invalid REFUND_PAYMENT payload: {msg}")
        return

    # ── Step 2: check whether PROCESS_PAYMENT ever succeeded ──────────────────
    # If payment never succeeded, refund is a no-op — but we still reply.
    pay_entry       = payment_ledger.get_entry(db, tx_id, PROCESS_PAYMENT)
    pay_succeeded   = (
        pay_entry is not None
        and pay_entry.get("result") == "success"
    )

    # ── Step 3: create ledger entry ───────────────────────────────────────────
    if not entry:
        payment_ledger.create_entry(
            db          = db,
            tx_id       = tx_id,
            action_type = REFUND_PAYMENT,
            business_snapshot = {"user_id": user_id, "amount": int(amount)},
        )

    # ── Step 4: apply refund only if payment actually happened ────────────────
    if pay_succeeded:
        success, error, _ = add_credit_internal(user_id, int(amount))
        if not success:
            logger.error(f"[PaymentSaga] Refund failed for user {user_id}: {error}")
    else:
        logger.debug(f"[PaymentSaga] REFUND_PAYMENT tx={tx_id} — payment never succeeded, no-op")

    # ── Step 5: mark applied ───────────────────────────────────────────────────
    payment_ledger.mark_applied(
        db                  = db,
        tx_id               = tx_id,
        action_type         = REFUND_PAYMENT,
        result              = "success",
        response_event_type = "PAYMENT_REFUNDED",
        response_payload    = {},
    )

    # ── Step 6: publish reply ──────────────────────────────────────────────────
    reply = build_payment_refunded(tx_id, order_id)
    publish(reply)
    logger.debug(f"[PaymentSaga] REFUND_PAYMENT done tx={tx_id} order={order_id}")

    # ── Step 7: mark replied ───────────────────────────────────────────────────
    payment_ledger.mark_replied(db, tx_id, REFUND_PAYMENT)


# ============================================================
# INTERNAL HELPERS
# ============================================================

def _republish(entry: dict, tx_id: str, order_id: str, publish) -> None:
    """Rebuild and re-publish the stored reply event from the ledger entry."""
    event_type = entry.get("response_event_type")
    payload    = entry.get("response_payload", {})
    msg = build_message(tx_id, order_id, event_type, payload)
    publish(msg)