import json
import time

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

def _2pc_route_payment(producer, db: redis_module.Redis, logger, msg: dict, msg_type) -> None:
    if msg_type == PREPARE_PAYMENT:
        _handle_prepare_payment(msg, db, producer, logger)
    elif msg_type == COMMIT_PAYMENT:
        _handle_commit_payment(msg, db, producer, logger)
    elif msg_type == ABORT_PAYMENT:
        _handle_abort_payment(msg, db, producer, logger)
    else:
        logger.info(f"[Payment2PC] Unknown command type: {msg_type!r} — dropping")

def _ledger_key(tx_id: str, action_type: str) -> str:
    return f"payment:ledger:{tx_id}:{action_type}"


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


def _prepare_payment_atomically(
    db: redis_module.Redis,
    tx_id: str,
    order_id: str,
    user_id: str,
    amount: int,
) -> tuple[dict, str]:
    from app import UserValue

    ledger_key = _ledger_key(tx_id, PREPARE_PAYMENT)
    abort_key = _ledger_key(tx_id, ABORT_PAYMENT)

    while True:
        pipe = db.pipeline()
        try:
            pipe.watch(ledger_key, abort_key, user_id)

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
                raise RuntimeError(f"Missing PREPARE_PAYMENT ledger entry for tx={tx_id}")

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

            raw_user = pipe.get(user_id)
            if not raw_user:
                reason = f"User {user_id} not found"
                reply = build_payment_prepare_failed(tx_id, order_id, reason)
                updated_entry = _build_applied_entry(
                    entry,
                    "failure",
                    PAYMENT_PREPARE_FAILED,
                    {"reason": reason},
                )

                pipe.multi()
                pipe.set(ledger_key, json.dumps(updated_entry), ex=payment_ledger.LEDGER_TTL_SECONDS)
                pipe.execute()
                return reply, "failure"

            user_entry = msgpack.decode(raw_user, type=UserValue)
            if user_entry.credit < amount:
                reason = f"Insufficient credit for user {user_id} (have {user_entry.credit}, need {amount})"
                reply = build_payment_prepare_failed(tx_id, order_id, reason)
                updated_entry = _build_applied_entry(
                    entry,
                    "failure",
                    PAYMENT_PREPARE_FAILED,
                    {"reason": reason},
                )

                pipe.multi()
                pipe.set(ledger_key, json.dumps(updated_entry), ex=payment_ledger.LEDGER_TTL_SECONDS)
                pipe.execute()
                return reply, "failure"

            user_entry = UserValue(credit=user_entry.credit - amount)
            reply = build_payment_prepared(tx_id, order_id)
            updated_entry = _build_applied_entry(entry, "success", PAYMENT_PREPARED, {})

            pipe.multi()
            pipe.set(user_id, msgpack.encode(user_entry))
            pipe.set(ledger_key, json.dumps(updated_entry), ex=payment_ledger.LEDGER_TTL_SECONDS)
            pipe.execute()
            return reply, "success"

        except redis_module.WatchError:
            continue
        finally:
            pipe.reset()


def _abort_payment_atomically(
    db: redis_module.Redis,
    tx_id: str,
    order_id: str,
) -> dict:
    from app import UserValue

    abort_key = _ledger_key(tx_id, ABORT_PAYMENT)
    prepare_key = _ledger_key(tx_id, PREPARE_PAYMENT)
    prepare_entry = payment_ledger.get_entry(db, tx_id, PREPARE_PAYMENT)

    user_id = None
    amount = 0
    if prepare_entry:
        snapshot = prepare_entry.get("business_snapshot", {})
        user_id = snapshot.get("user_id")
        amount = int(snapshot.get("amount", 0))

    while True:
        pipe = db.pipeline()
        try:
            watch_keys = [abort_key, prepare_key]
            if user_id:
                watch_keys.append(user_id)
            pipe.watch(*watch_keys)

            raw_abort = pipe.get(abort_key)
            if not raw_abort:
                pipe.unwatch()
                raise RuntimeError(f"Missing ABORT_PAYMENT ledger entry for tx={tx_id}")

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
            prepare_succeeded = (
                prepare_entry is not None and prepare_entry.get("result") == "success"
            )
            if prepare_entry and not user_id:
                snapshot = prepare_entry.get("business_snapshot", {})
                discovered_user = snapshot.get("user_id")
                if discovered_user:
                    user_id = discovered_user
                    amount = int(snapshot.get("amount", 0))
                    pipe.unwatch()
                    continue

            updated_abort = _build_applied_entry(abort_entry, "success", PAYMENT_ABORTED, {})
            reply = build_payment_aborted(tx_id, order_id)

            if not prepare_succeeded or not user_id:
                pipe.multi()
                pipe.set(abort_key, json.dumps(updated_abort), ex=payment_ledger.LEDGER_TTL_SECONDS)
                pipe.execute()
                return reply

            raw_user = pipe.get(user_id)
            if not raw_user:
                pipe.unwatch()
                raise RuntimeError(f"Missing user row {user_id} during ABORT_PAYMENT tx={tx_id}")

            user_entry = msgpack.decode(raw_user, type=UserValue)
            updated_user = UserValue(credit=user_entry.credit + amount)

            pipe.multi()
            pipe.set(user_id, msgpack.encode(updated_user))
            pipe.set(abort_key, json.dumps(updated_abort), ex=payment_ledger.LEDGER_TTL_SECONDS)
            pipe.execute()
            return reply

        except redis_module.WatchError:
            continue
        finally:
            pipe.reset()


# ---------------- PREPARE ----------------
def _handle_prepare_payment(msg, db, producer, logger):
    tx_id    = msg.get("tx_id")
    order_id = msg.get("order_id")
    user_id  = msg.get("payload", {}).get("user_id")
    amount   = msg.get("payload", {}).get("amount")

    if not tx_id or not order_id or user_id is None or amount is None:
        logger.warning(f"[Payment2PC] Missing fields in PREPARE_PAYMENT: {msg}")
        return

    logger.info(f"[Payment2PC] order={order_id} PREPARING_PAYMENT")

    abort_entry = payment_ledger.get_entry(db, tx_id, ABORT_PAYMENT)
    if abort_entry and abort_entry.get("local_state") in (LedgerState.APPLIED, LedgerState.REPLIED):
        producer.publish(
            PAYMENT_EVENTS_TOPIC,
            build_message(
                tx_id,
                order_id,
                abort_entry["response_event_type"],
                abort_entry.get("response_payload", {}),
            ),
        )
        return

    # Duplicate check via ledger
    entry = payment_ledger.get_entry(db, tx_id, PREPARE_PAYMENT)
    if entry:
        if entry["local_state"] in (LedgerState.APPLIED, LedgerState.REPLIED):
            producer.publish(PAYMENT_EVENTS_TOPIC,
                             build_message(tx_id, order_id, entry["response_event_type"], entry["response_payload"]))
            payment_ledger.mark_replied(db, tx_id, PREPARE_PAYMENT)
        return

    amount = int(amount)
    created = payment_ledger.create_entry(
        db,
        tx_id,
        PREPARE_PAYMENT,
        {"user_id": user_id, "amount": amount},
    )
    if not created:
        entry = payment_ledger.get_entry(db, tx_id, PREPARE_PAYMENT)
        if not entry:
            logger.error(f"[Payment2PC] order={order_id} failed to create/read PREPARE ledger")
            return

    try:
        reply, result = _prepare_payment_atomically(db, tx_id, order_id, user_id, amount)
    except RuntimeError as exc:
        logger.error(f"[Payment2PC] PREPARE_PAYMENT tx={tx_id} failed: {exc}")
        return

    producer.publish(PAYMENT_EVENTS_TOPIC, reply)
    if reply.get("type") in (PAYMENT_PREPARED, PAYMENT_PREPARE_FAILED):
        payment_ledger.mark_replied(db, tx_id, PREPARE_PAYMENT)
    logger.info(f"[Payment2PC] order={order_id} PREPARE_PAYMENT {result}")


# ---------------- COMMIT ----------------
def _handle_commit_payment(msg, db, producer, logger):
    from app import UserValue

    tx_id = msg.get("tx_id")
    order_id = msg.get("order_id")

    # Dedup
    entry = payment_ledger.get_entry(db, tx_id, COMMIT_PAYMENT)
    if entry and entry.get("local_state") in (LedgerState.APPLIED, LedgerState.REPLIED):
        producer.publish(
            PAYMENT_EVENTS_TOPIC,
            build_message(
                tx_id,
                order_id,
                entry["response_event_type"],
                entry.get("response_payload", {}),
            ),
        )
        payment_ledger.mark_replied(db, tx_id, COMMIT_PAYMENT)
        return

    # The credit reservation already happened durably in PREPARE_PAYMENT.
    # COMMIT_PAYMENT only confirms that prepared reservation.
    prepare_entry = payment_ledger.get_entry(db, tx_id, PREPARE_PAYMENT)
    if not prepare_entry or prepare_entry.get("result") != "success":
        logger.info(f"[Payment2PC] COMMIT_PAYMENT tx={tx_id} -- no successful PREPARE found, ignoring")
        return

    if not entry:
        created = payment_ledger.create_entry(db, tx_id, COMMIT_PAYMENT, {})
        if not created:
            entry = payment_ledger.get_entry(db, tx_id, COMMIT_PAYMENT)
            if not entry:
                logger.error(f"[Payment2PC] COMMIT_PAYMENT tx={tx_id} failed to create/read commit ledger")
                return

    ok = payment_ledger.mark_applied(db, tx_id, COMMIT_PAYMENT, "success", PAYMENT_COMMITTED, {})
    if not ok:
        logger.error(f"[Payment2PC] COMMIT_PAYMENT tx={tx_id} failed to mark commit ledger applied")
        return

    producer.publish(PAYMENT_EVENTS_TOPIC, build_payment_committed(tx_id, order_id))
    payment_ledger.mark_replied(db, tx_id, COMMIT_PAYMENT)
    logger.info(f"[Payment2PC] order={order_id} PAYMENT_COMMITTED")



# ---------------- ABORT ----------------
def _handle_abort_payment(msg, db, producer, logger):
    tx_id    = msg.get("tx_id")
    order_id = msg.get("order_id")

    # Dedup
    entry = payment_ledger.get_entry(db, tx_id, ABORT_PAYMENT)
    if entry and entry.get("local_state") in (LedgerState.APPLIED, LedgerState.REPLIED):
        producer.publish(
            PAYMENT_EVENTS_TOPIC,
            build_message(
                tx_id,
                order_id,
                entry["response_event_type"],
                entry.get("response_payload", {}),
            ),
        )
        payment_ledger.mark_replied(db, tx_id, ABORT_PAYMENT)
        return

    if not entry:
        created = payment_ledger.create_entry(db, tx_id, ABORT_PAYMENT, {})
        if not created:
            entry = payment_ledger.get_entry(db, tx_id, ABORT_PAYMENT)
            if not entry:
                logger.error(f"[Payment2PC] order={order_id} failed to create/read ABORT ledger")
                return

    try:
        reply = _abort_payment_atomically(db, tx_id, order_id)
    except RuntimeError as exc:
        logger.error(f"[Payment2PC] ABORT_PAYMENT tx={tx_id} failed: {exc}")
        return

    producer.publish(PAYMENT_EVENTS_TOPIC, reply)
    payment_ledger.mark_replied(db, tx_id, ABORT_PAYMENT)
    logger.info(f"[Payment2PC] order={order_id} PAYMENT_ABORTED")
