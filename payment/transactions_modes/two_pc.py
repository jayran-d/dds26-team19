
import uuid

import redis as redis_module
from msgspec import msgpack

from common.messages import (
    PAYMENT_EVENTS_TOPIC,
    PREPARE_PAYMENT,
    COMMIT_PAYMENT,
    ABORT_PAYMENT,
    PAYMENT_PREPARED,
    PAYMENT_PREPARE_FAILED,
    PAYMENT_COMMITTED,
    PAYMENT_ABORTED,
    build_message,
    build_payment_prepared,
    build_payment_prepare_failed,
    build_payment_committed,
    build_payment_aborted,
)

import ledger as payment_ledger
from ledger import LedgerState
from common.kafka_client import KafkaProducerClient

LOCK_TTL = 10000  # milliseconds


def _2pc_route_payment(producer, db: redis_module.Redis, logger, msg: dict, msg_type) -> None:
    if msg_type == PREPARE_PAYMENT:
        _handle_prepare_payment(msg, db, producer, logger)
    elif msg_type == COMMIT_PAYMENT:
        _handle_commit_payment(msg, db, producer, logger)
    elif msg_type == ABORT_PAYMENT:
        _handle_abort_payment(msg, db, producer, logger)
    else:
        logger.info(f"[Payment2PC] Unknown command type: {msg_type!r} — dropping")


# ---------------- PREPARE ----------------
def _handle_prepare_payment(msg, db, producer, logger):
    from app import get_user_from_db

    tx_id    = msg.get("tx_id")
    order_id = msg.get("order_id")
    user_id  = msg.get("payload", {}).get("user_id")
    amount   = msg.get("payload", {}).get("amount")

    if not tx_id or not order_id or user_id is None or amount is None:
        logger.warning(f"[Payment2PC] Missing fields in PREPARE_PAYMENT: {msg}")
        return

    logger.info(f"[Payment2PC] order={order_id} PREPARING_PAYMENT")

    # Duplicate check via ledger
    entry = payment_ledger.get_entry(db, tx_id, PREPARE_PAYMENT)
    if entry:
        if entry["local_state"] in (LedgerState.APPLIED, LedgerState.REPLIED):
            producer.publish(PAYMENT_EVENTS_TOPIC,
                             build_message(tx_id, order_id, entry["response_event_type"], entry["response_payload"]))
            payment_ledger.mark_replied(db, tx_id, PREPARE_PAYMENT)
        return

    # Acquire a lock for this user
    lock_key = f"lock:payment:user:{user_id}"
    lock_val = str(uuid.uuid4())
    acquired = db.set(lock_key, lock_val, nx=True, px=LOCK_TTL)
    if not acquired:
        logger.info(f"[Payment2PC] order={order_id} failed to acquire lock for user {user_id}")
        producer.publish(PAYMENT_EVENTS_TOPIC,
                         build_payment_prepare_failed(tx_id, order_id, f"User {user_id} locked by another transaction"))
        return

    logger.info(f"[Payment2PC] order={order_id} LOCK_SET for user={user_id}")

    # Check credit (without deducting)
    user = get_user_from_db(user_id)
    if user is None:
        db.delete(lock_key)
        producer.publish(PAYMENT_EVENTS_TOPIC,
                         build_payment_prepare_failed(tx_id, order_id, f"User {user_id} not found"))
        return

    if user.credit < int(amount):
        if db.get(lock_key) == lock_val.encode():
            db.delete(lock_key)
        logger.info(f"[Payment2PC] order={order_id} insufficient credit (have {user.credit}, need {amount})")
        producer.publish(PAYMENT_EVENTS_TOPIC,
                         build_payment_prepare_failed(tx_id, order_id,
                                                      f"Insufficient credit for user {user_id} (have {user.credit}, need {amount})"))
        return

    # Store lock info in ledger snapshot so COMMIT/ABORT can release it
    snapshot = {"user_id": user_id, "amount": int(amount), "lock_key": lock_key, "lock_val": lock_val}
    payment_ledger.create_entry(db, tx_id, PREPARE_PAYMENT, snapshot)
    payment_ledger.mark_applied(db, tx_id, PREPARE_PAYMENT, "success", PAYMENT_PREPARED, {})

    producer.publish(PAYMENT_EVENTS_TOPIC, build_payment_prepared(tx_id, order_id))
    logger.info(f"[Payment2PC] order={order_id} PAYMENT_PREPARED")


# ---------------- COMMIT ----------------
def _handle_commit_payment(msg, db, producer, logger):
    from app import UserValue

    tx_id    = msg.get("tx_id")
    order_id = msg.get("order_id")

    # Dedup
    entry = payment_ledger.get_entry(db, tx_id, COMMIT_PAYMENT)
    if entry and entry.get("local_state") in (LedgerState.APPLIED, LedgerState.REPLIED):
        producer.publish(PAYMENT_EVENTS_TOPIC, build_payment_committed(tx_id, order_id))
        return

    # Read the PREPARE snapshot for user_id, amount, and lock info
    prepare_entry = payment_ledger.get_entry(db, tx_id, PREPARE_PAYMENT)
    if not prepare_entry or prepare_entry.get("result") != "success":
        logger.info(f"[Payment2PC] COMMIT_PAYMENT tx={tx_id} -- no successful PREPARE found, ignoring")
        return

    snapshot = prepare_entry["business_snapshot"]
    user_id  = snapshot["user_id"]
    amount   = snapshot["amount"]
    lock_key = snapshot["lock_key"]
    lock_val = snapshot["lock_val"]

    max_retries = 5
    committed = False
    for _ in range(max_retries):
        try:
            with db.pipeline() as pipe:
                pipe.watch(user_id, lock_key)

                if pipe.get(lock_key) != lock_val.encode():
                    logger.warning(
                        f"[Payment2PC] COMMIT_PAYMENT tx={tx_id} lock lost for user {user_id}"
                    )
                    pipe.unwatch()
                    return

                raw_user = pipe.get(user_id)
                if not raw_user:
                    logger.error(
                        f"[Payment2PC] COMMIT_PAYMENT tx={tx_id} missing user row {user_id}"
                    )
                    pipe.unwatch()
                    return

                user_entry = msgpack.decode(raw_user, type=UserValue)
                user_entry.credit -= amount
                if user_entry.credit < 0:
                    logger.error(
                        f"[Payment2PC] COMMIT_PAYMENT tx={tx_id} would make user {user_id} negative"
                    )
                    pipe.unwatch()
                    return

                pipe.multi()
                pipe.set(user_id, msgpack.encode(user_entry))
                pipe.delete(lock_key)
                pipe.execute()
                committed = True
                break
        except redis_module.WatchError:
            continue

    if not committed:
        logger.warning(f"[Payment2PC] COMMIT_PAYMENT tx={tx_id} failed after retries, will rely on resend")
        return

    payment_ledger.create_entry(db, tx_id, COMMIT_PAYMENT, {})
    payment_ledger.mark_applied(db, tx_id, COMMIT_PAYMENT, "success", PAYMENT_COMMITTED, {})
    producer.publish(PAYMENT_EVENTS_TOPIC, build_payment_committed(tx_id, order_id))
    payment_ledger.mark_replied(db, tx_id, COMMIT_PAYMENT)
    logger.info(f"[Payment2PC] order={order_id} PAYMENT_COMMITTED atomically, lock released")


# ---------------- ABORT ----------------
def _handle_abort_payment(msg, db, producer, logger):
    tx_id    = msg.get("tx_id")
    order_id = msg.get("order_id")

    # Dedup
    entry = payment_ledger.get_entry(db, tx_id, ABORT_PAYMENT)
    if entry and entry.get("local_state") in (LedgerState.APPLIED, LedgerState.REPLIED):
        producer.publish(PAYMENT_EVENTS_TOPIC, build_payment_aborted(tx_id, order_id))
        return

    # Credit was never deducted — just release the lock
    prepare_entry = payment_ledger.get_entry(db, tx_id, PREPARE_PAYMENT)
    if prepare_entry and prepare_entry.get("result") == "success":
        snapshot = prepare_entry["business_snapshot"]
        lock_key = snapshot["lock_key"]
        lock_val = snapshot["lock_val"]
        if db.get(lock_key) == lock_val.encode():
            db.delete(lock_key)
    else:
        logger.info(f"[Payment2PC] ABORT_PAYMENT tx={tx_id} -- PREPARE did not succeed, nothing to release")

    payment_ledger.create_entry(db, tx_id, ABORT_PAYMENT, {})
    payment_ledger.mark_applied(db, tx_id, ABORT_PAYMENT, "success", PAYMENT_ABORTED, {})
    producer.publish(PAYMENT_EVENTS_TOPIC, build_payment_aborted(tx_id, order_id))
    payment_ledger.mark_replied(db, tx_id, ABORT_PAYMENT)
    logger.info(f"[Payment2PC] order={order_id} PAYMENT_ABORTED, lock released")
