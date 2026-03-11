"""
payment/transaction_modes/saga.py

Saga command handlers for the payment service.

Handles:
    PROCESS_PAYMENT  → charge user credit
    REFUND_PAYMENT   → refund user credit (compensation)

Each handler follows the same ledger pattern as stock/saga.py.
"""

import redis as redis_module
from msgspec import msgpack
import json
import time

from common.messages import (
    PROCESS_PAYMENT,
    REFUND_PAYMENT,
    PAYMENT_EVENTS_TOPIC,
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

def saga_route_payment(msg: dict, db: redis_module.Redis, publish, logger) -> None:
    msg_type = msg.get("type")
    if msg_type == PROCESS_PAYMENT:
        _handle_process_payment(msg, db, publish, logger)
    elif msg_type == REFUND_PAYMENT:
        _handle_refund_payment(msg, db, publish, logger)
    else:
        logger.info(f"[PaymentSaga] Unknown command type: {msg_type!r} — dropping")


# ============================================================
# PROCESS_PAYMENT
# ============================================================

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

    entry = payment_ledger.get_entry(db, tx_id, PROCESS_PAYMENT)

    if entry:
        state = entry.get("local_state")
        if state == LedgerState.REPLIED:
            logger.debug(f"[PaymentSaga] PROCESS_PAYMENT duplicate (replied) tx={tx_id}")
            _republish(entry, publish)
            return
        if state == LedgerState.APPLIED:
            logger.debug(f"[PaymentSaga] PROCESS_PAYMENT duplicate (applied) tx={tx_id}")
            _republish(entry, publish)
            payment_ledger.mark_replied(db, tx_id, PROCESS_PAYMENT)
            return

    if not user_id or amount is None:
        logger.error(f"[PaymentSaga] Invalid PROCESS_PAYMENT payload: {msg}")
        return

    if not entry:
        created = payment_ledger.create_entry(
            db=db,
            tx_id=tx_id,
            action_type=PROCESS_PAYMENT,
            business_snapshot={"user_id": user_id, "amount": int(amount)},
        )
        if not created:
            entry = payment_ledger.get_entry(db, tx_id, PROCESS_PAYMENT)
            if not entry:
                logger.error(f"[PaymentSaga] Failed to create/read PROCESS_PAYMENT ledger tx={tx_id}")
                return

    try:
        reply, result = _apply_process_payment_atomically(
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
    payment_ledger.mark_replied(db, tx_id, PROCESS_PAYMENT)
    logger.info(f"[PaymentSaga] PROCESS_PAYMENT {result} tx={tx_id} order={order_id}")


# ============================================================
# REFUND_PAYMENT  (compensation)
# ============================================================

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

    entry = payment_ledger.get_entry(db, tx_id, REFUND_PAYMENT)

    if entry:
        state = entry.get("local_state")
        if state == LedgerState.REPLIED:
            logger.debug(f"[PaymentSaga] REFUND_PAYMENT duplicate (replied) tx={tx_id}")
            _republish(entry, publish)
            return
        if state == LedgerState.APPLIED:
            logger.debug(f"[PaymentSaga] REFUND_PAYMENT duplicate (applied) tx={tx_id}")
            _republish(entry, publish)
            payment_ledger.mark_replied(db, tx_id, REFUND_PAYMENT)
            return

    if not user_id or amount is None:
        logger.error(f"[PaymentSaga] Invalid REFUND_PAYMENT payload: {msg}")
        return

    if not entry:
        created = payment_ledger.create_entry(
            db=db,
            tx_id=tx_id,
            action_type=REFUND_PAYMENT,
            business_snapshot={"user_id": user_id, "amount": int(amount)},
        )
        if not created:
            entry = payment_ledger.get_entry(db, tx_id, REFUND_PAYMENT)
            if not entry:
                logger.error(f"[PaymentSaga] Failed to create/read REFUND_PAYMENT ledger tx={tx_id}")
                return

    try:
        reply, _ = _apply_refund_payment_atomically(
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
    payment_ledger.mark_replied(db, tx_id, REFUND_PAYMENT)
    logger.info(f"[PaymentSaga] REFUND_PAYMENT done tx={tx_id} order={order_id}")



# ============================================================
# INTERNAL HELPERS
# ============================================================

def _republish(entry: dict, publish) -> None:
    """Re-publish the stored reply event saved in the ledger entry."""
    reply_message = entry.get("reply_message")
    if reply_message:
        publish(PAYMENT_EVENTS_TOPIC, reply_message)
        
def _build_applied_entry(entry: dict, result: str, reply_message: dict) -> dict:
    updated = dict(entry)
    updated["local_state"] = LedgerState.APPLIED
    updated["result"] = result
    updated["reply_message"] = reply_message
    updated["updated_at_ms"] = int(time.time() * 1000)
    return updated


def _apply_process_payment_atomically(
    db: redis_module.Redis,
    tx_id: str,
    order_id: str,
    user_id: str,
    amount: int,
) -> tuple[dict, str]:
    from app import UserValue

    ledger_key = f"payment:ledger:{tx_id}:{PROCESS_PAYMENT}"

    while True:
        pipe = db.pipeline()
        try:
            pipe.watch(ledger_key, user_id)

            raw_entry = pipe.get(ledger_key)
            if not raw_entry:
                pipe.unwatch()
                raise RuntimeError(f"Missing PROCESS_PAYMENT ledger entry for tx={tx_id}")

            entry = json.loads(raw_entry)
            state = entry.get("local_state")
            if state in (LedgerState.APPLIED, LedgerState.REPLIED):
                pipe.unwatch()
                return entry["reply_message"], entry.get("result", "failure")

            raw_user = pipe.get(user_id)
            if not raw_user:
                reply = build_payment_failed(tx_id, order_id, f"User: {user_id} not found!")
                updated_entry = _build_applied_entry(entry, "failure", reply)

                pipe.multi()
                pipe.set(ledger_key, json.dumps(updated_entry), ex=payment_ledger.LEDGER_TTL_SECONDS)
                pipe.execute()
                return reply, "failure"

            user_entry = msgpack.decode(raw_user, type=UserValue)
            new_credit = user_entry.credit - amount

            if new_credit < 0:
                reply = build_payment_failed(
                    tx_id,
                    order_id,
                    f"User: {user_id} credit cannot get reduced below zero!",
                )
                updated_entry = _build_applied_entry(entry, "failure", reply)

                pipe.multi()
                pipe.set(ledger_key, json.dumps(updated_entry), ex=payment_ledger.LEDGER_TTL_SECONDS)
                pipe.execute()
                return reply, "failure"

            updated_user = UserValue(credit=new_credit)
            reply = build_payment_success(tx_id, order_id)
            updated_entry = _build_applied_entry(entry, "success", reply)

            pipe.multi()
            pipe.set(user_id, msgpack.encode(updated_user))
            pipe.set(ledger_key, json.dumps(updated_entry), ex=payment_ledger.LEDGER_TTL_SECONDS)
            pipe.execute()
            return reply, "success"

        except redis_module.exceptions.WatchError:
            continue
        finally:
            pipe.reset()


def _apply_refund_payment_atomically(
    db: redis_module.Redis,
    tx_id: str,
    order_id: str,
    user_id: str,
    amount: int,
) -> tuple[dict, str]:
    from app import UserValue

    refund_key = f"payment:ledger:{tx_id}:{REFUND_PAYMENT}"
    payment_key = f"payment:ledger:{tx_id}:{PROCESS_PAYMENT}"

    while True:
        pipe = db.pipeline()
        try:
            pipe.watch(refund_key, payment_key, user_id)

            raw_refund = pipe.get(refund_key)
            if not raw_refund:
                pipe.unwatch()
                raise RuntimeError(f"Missing REFUND_PAYMENT ledger entry for tx={tx_id}")

            refund_entry = json.loads(raw_refund)
            refund_state = refund_entry.get("local_state")
            if refund_state in (LedgerState.APPLIED, LedgerState.REPLIED):
                pipe.unwatch()
                return refund_entry["reply_message"], refund_entry.get("result", "success")

            raw_payment = pipe.get(payment_key)
            payment_entry = json.loads(raw_payment) if raw_payment else None
            payment_succeeded = payment_entry is not None and payment_entry.get("result") == "success"

            reply = build_payment_refunded(tx_id, order_id)
            updated_refund = _build_applied_entry(refund_entry, "success", reply)

            if not payment_succeeded:
                pipe.multi()
                pipe.set(refund_key, json.dumps(updated_refund), ex=payment_ledger.LEDGER_TTL_SECONDS)
                pipe.execute()
                return reply, "success"

            raw_user = pipe.get(user_id)
            if not raw_user:
                pipe.unwatch()
                raise RuntimeError(f"Missing user {user_id} during refund for tx={tx_id}")

            user_entry = msgpack.decode(raw_user, type=UserValue)
            updated_user = UserValue(credit=user_entry.credit + amount)

            pipe.multi()
            pipe.set(user_id, msgpack.encode(updated_user))
            pipe.set(refund_key, json.dumps(updated_refund), ex=payment_ledger.LEDGER_TTL_SECONDS)
            pipe.execute()
            return reply, "success"

        except redis_module.exceptions.WatchError:
            continue
        finally:
            pipe.reset()
