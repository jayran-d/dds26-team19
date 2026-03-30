import json
import time

import redis as redis_module

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

_db: redis_module.Redis | None = None
_publish = None
_logger = None


def init_2pc(db: redis_module.Redis, publish_fn, logger) -> None:
    global _db, _publish, _logger
    _db = db
    _publish = publish_fn
    _logger = logger


def _ledger_key(tx_id: str, action_type: str) -> str:
    return f"payment:ledger:{tx_id}:{action_type}"


def _reservation_key(tx_id: str) -> str:
    return f"payment:reservation:{tx_id}"


def _reserved_total_key(user_id: str) -> str:
    return f"payment:reserved_total:{user_id}"


def _decode_int(raw, default: int = 0) -> int:
    if raw is None:
        return default
    if isinstance(raw, bytes):
        raw = raw.decode()
    return int(raw)


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


def _prepare_payment_atomically(db: redis_module.Redis, tx_id: str, order_id: str, user_id: str, amount: int) -> tuple[dict, str]:
    ledger_key = _ledger_key(tx_id, PREPARE_PAYMENT)
    abort_key = _ledger_key(tx_id, ABORT_PAYMENT)
    commit_key = _ledger_key(tx_id, COMMIT_PAYMENT)
    reserved_total_key = _reserved_total_key(user_id)
    reservation_key = _reservation_key(tx_id)
    while True:
        pipe = db.pipeline()
        try:
            pipe.watch(ledger_key, abort_key, commit_key, user_id, reserved_total_key, reservation_key)
            raw_abort = pipe.get(abort_key)
            if raw_abort:
                abort_entry = json.loads(raw_abort)
                if abort_entry.get("local_state") in (LedgerState.APPLIED, LedgerState.REPLIED):
                    pipe.unwatch(); return build_message(tx_id, order_id, abort_entry["response_event_type"], abort_entry.get("response_payload", {})), "aborted"
            raw_commit = pipe.get(commit_key)
            if raw_commit:
                commit_entry = json.loads(raw_commit)
                if commit_entry.get("local_state") in (LedgerState.APPLIED, LedgerState.REPLIED):
                    pipe.unwatch(); return build_message(tx_id, order_id, commit_entry["response_event_type"], commit_entry.get("response_payload", {})), "committed"
            raw_entry = pipe.get(ledger_key)
            if not raw_entry:
                pipe.unwatch(); raise RuntimeError(f"Missing PREPARE_PAYMENT ledger entry for tx={tx_id}")
            entry = json.loads(raw_entry)
            if entry.get("local_state") in (LedgerState.APPLIED, LedgerState.REPLIED):
                pipe.unwatch(); return build_message(tx_id, order_id, entry["response_event_type"], entry.get("response_payload", {})), entry.get("result", "success")
            raw_user = pipe.get(user_id)
            raw_reserved_total = pipe.get(reserved_total_key)
            raw_existing_reservation = pipe.get(reservation_key)
            if raw_existing_reservation is not None:
                reply = build_payment_prepared(tx_id, order_id)
                updated = _build_applied_entry(entry, "success", PAYMENT_PREPARED, {})
                pipe.multi(); pipe.set(ledger_key, json.dumps(updated), ex=payment_ledger.LEDGER_TTL_SECONDS); pipe.execute(); return reply, "success"
            if not raw_user:
                reason = f"User {user_id} not found"
                reply = build_payment_prepare_failed(tx_id, order_id, reason)
                updated = _build_applied_entry(entry, "failure", PAYMENT_PREPARE_FAILED, {"reason": reason})
                pipe.multi(); pipe.set(ledger_key, json.dumps(updated), ex=payment_ledger.LEDGER_TTL_SECONDS); pipe.execute(); return reply, "failure"
            available = _decode_int(raw_user) - _decode_int(raw_reserved_total)
            if available < amount:
                reason = f"Insufficient credit for user {user_id} (have {available}, need {amount})"
                reply = build_payment_prepare_failed(tx_id, order_id, reason)
                updated = _build_applied_entry(entry, "failure", PAYMENT_PREPARE_FAILED, {"reason": reason})
                pipe.multi(); pipe.set(ledger_key, json.dumps(updated), ex=payment_ledger.LEDGER_TTL_SECONDS); pipe.execute(); return reply, "failure"
            reply = build_payment_prepared(tx_id, order_id)
            updated = _build_applied_entry(entry, "success", PAYMENT_PREPARED, {})
            pipe.multi(); pipe.set(reservation_key, json.dumps({"user_id": user_id, "amount": amount}), ex=payment_ledger.LEDGER_TTL_SECONDS); pipe.incrby(reserved_total_key, amount); pipe.set(ledger_key, json.dumps(updated), ex=payment_ledger.LEDGER_TTL_SECONDS); pipe.execute(); return reply, "success"
        except redis_module.WatchError:
            continue
        finally:
            pipe.reset()


def _commit_payment_atomically(db: redis_module.Redis, tx_id: str, order_id: str) -> dict:
    commit_key = _ledger_key(tx_id, COMMIT_PAYMENT)
    prepare_key = _ledger_key(tx_id, PREPARE_PAYMENT)
    abort_key = _ledger_key(tx_id, ABORT_PAYMENT)
    reservation_key = _reservation_key(tx_id)
    prepare_entry = payment_ledger.get_entry(db, tx_id, PREPARE_PAYMENT)
    snapshot = (prepare_entry or {}).get("business_snapshot", {})
    user_id = snapshot.get("user_id")
    amount = int(snapshot.get("amount", 0))
    reserved_total_key = _reserved_total_key(user_id) if user_id else None
    while True:
        pipe = db.pipeline()
        try:
            watch_keys = [commit_key, prepare_key, abort_key, reservation_key]
            if user_id:
                watch_keys.extend([user_id, reserved_total_key])
            pipe.watch(*watch_keys)
            raw_commit = pipe.get(commit_key)
            if not raw_commit:
                pipe.unwatch(); raise RuntimeError(f"Missing COMMIT_PAYMENT ledger entry for tx={tx_id}")
            commit_entry = json.loads(raw_commit)
            if commit_entry.get("local_state") in (LedgerState.APPLIED, LedgerState.REPLIED):
                pipe.unwatch(); return build_message(tx_id, order_id, commit_entry["response_event_type"], commit_entry.get("response_payload", {}))
            raw_abort = pipe.get(abort_key)
            if raw_abort:
                abort_entry = json.loads(raw_abort)
                if abort_entry.get("local_state") in (LedgerState.APPLIED, LedgerState.REPLIED):
                    pipe.unwatch(); raise RuntimeError(f"ABORT_PAYMENT already applied for tx={tx_id}")
            raw_prepare = pipe.get(prepare_key)
            prepare_entry = json.loads(raw_prepare) if raw_prepare else None
            if not prepare_entry or prepare_entry.get("result") != "success":
                pipe.unwatch(); raise RuntimeError(f"Missing successful PREPARE_PAYMENT for tx={tx_id}")
            if not user_id:
                snapshot = prepare_entry.get("business_snapshot", {})
                user_id = snapshot.get("user_id"); amount = int(snapshot.get("amount", 0)); reserved_total_key = _reserved_total_key(user_id) if user_id else None
                pipe.unwatch(); continue
            raw_reservation = pipe.get(reservation_key)
            if raw_reservation is None:
                pipe.unwatch(); raise RuntimeError(f"Missing reservation during COMMIT_PAYMENT tx={tx_id}")
            raw_user = pipe.get(user_id)
            if not raw_user:
                pipe.unwatch(); raise RuntimeError(f"Missing user row {user_id} during COMMIT_PAYMENT tx={tx_id}")
            reserved_total = _decode_int(pipe.get(reserved_total_key))
            user_credit = _decode_int(raw_user)
            if reserved_total < amount:
                pipe.unwatch(); raise RuntimeError(f"Reserved total for user {user_id} below tx reservation during COMMIT_PAYMENT tx={tx_id}")
            if user_credit < amount:
                pipe.unwatch(); raise RuntimeError(f"User {user_id} credit below reserved amount during COMMIT_PAYMENT tx={tx_id}")
            updated = _build_applied_entry(commit_entry, "success", PAYMENT_COMMITTED, {})
            reply = build_payment_committed(tx_id, order_id)
            pipe.multi(); pipe.set(user_id, str(user_credit - amount)); pipe.decrby(reserved_total_key, amount); pipe.delete(reservation_key); pipe.set(commit_key, json.dumps(updated), ex=payment_ledger.LEDGER_TTL_SECONDS); pipe.execute(); return reply
        except redis_module.WatchError:
            continue
        finally:
            pipe.reset()


def _abort_payment_atomically(db: redis_module.Redis, tx_id: str, order_id: str) -> dict:
    abort_key = _ledger_key(tx_id, ABORT_PAYMENT)
    prepare_key = _ledger_key(tx_id, PREPARE_PAYMENT)
    commit_key = _ledger_key(tx_id, COMMIT_PAYMENT)
    reservation_key = _reservation_key(tx_id)
    prepare_entry = payment_ledger.get_entry(db, tx_id, PREPARE_PAYMENT)
    snapshot = (prepare_entry or {}).get("business_snapshot", {})
    user_id = snapshot.get("user_id")
    amount = int(snapshot.get("amount", 0))
    reserved_total_key = _reserved_total_key(user_id) if user_id else None
    while True:
        pipe = db.pipeline()
        try:
            watch_keys = [abort_key, prepare_key, commit_key, reservation_key]
            if user_id:
                watch_keys.append(reserved_total_key)
            pipe.watch(*watch_keys)
            raw_abort = pipe.get(abort_key)
            if not raw_abort:
                pipe.unwatch(); raise RuntimeError(f"Missing ABORT_PAYMENT ledger entry for tx={tx_id}")
            abort_entry = json.loads(raw_abort)
            if abort_entry.get("local_state") in (LedgerState.APPLIED, LedgerState.REPLIED):
                pipe.unwatch(); return build_message(tx_id, order_id, abort_entry["response_event_type"], abort_entry.get("response_payload", {}))
            raw_commit = pipe.get(commit_key)
            if raw_commit:
                commit_entry = json.loads(raw_commit)
                if commit_entry.get("local_state") in (LedgerState.APPLIED, LedgerState.REPLIED):
                    pipe.unwatch(); raise RuntimeError(f"COMMIT_PAYMENT already applied for tx={tx_id}")
            raw_prepare = pipe.get(prepare_key)
            prepare_entry = json.loads(raw_prepare) if raw_prepare else None
            prepare_succeeded = prepare_entry is not None and prepare_entry.get("result") == "success"
            updated = _build_applied_entry(abort_entry, "success", PAYMENT_ABORTED, {})
            reply = build_payment_aborted(tx_id, order_id)
            raw_reservation = pipe.get(reservation_key)
            reserved_total = _decode_int(pipe.get(reserved_total_key)) if reserved_total_key else 0
            pipe.multi()
            if prepare_succeeded and raw_reservation is not None and amount > 0:
                if reserved_total < amount:
                    pipe.reset(); raise RuntimeError(f"Reserved total for user {user_id} below tx reservation during ABORT_PAYMENT tx={tx_id}")
                pipe.decrby(reserved_total_key, amount)
                pipe.delete(reservation_key)
            pipe.set(abort_key, json.dumps(updated), ex=payment_ledger.LEDGER_TTL_SECONDS)
            pipe.execute(); return reply
        except redis_module.WatchError:
            continue
        finally:
            pipe.reset()


def _handle_prepare_payment(msg, db, producer, logger):
    tx_id = msg.get("tx_id"); order_id = msg.get("order_id"); user_id = msg.get("payload", {}).get("user_id"); amount = msg.get("payload", {}).get("amount")
    if not tx_id or not order_id or user_id is None or amount is None:
        logger.warning(f"[Payment2PC] Missing fields in PREPARE_PAYMENT: {msg}"); return
    logger.info(f"[Payment2PC] order={order_id} PREPARING_PAYMENT")
    abort_entry = payment_ledger.get_entry(db, tx_id, ABORT_PAYMENT)
    if abort_entry and abort_entry.get("local_state") in (LedgerState.APPLIED, LedgerState.REPLIED):
        producer(PAYMENT_EVENTS_TOPIC, build_message(tx_id, order_id, abort_entry["response_event_type"], abort_entry.get("response_payload", {}))); return
    entry = payment_ledger.get_entry(db, tx_id, PREPARE_PAYMENT)
    if entry and entry.get("local_state") in (LedgerState.APPLIED, LedgerState.REPLIED):
        producer(PAYMENT_EVENTS_TOPIC, build_message(tx_id, order_id, entry["response_event_type"], entry.get("response_payload", {})))
        payment_ledger.mark_replied(db, tx_id, PREPARE_PAYMENT); return
    amount = int(amount)
    if not entry:
        created = payment_ledger.create_entry(db, tx_id, PREPARE_PAYMENT, {"user_id": user_id, "amount": amount, "order_id": order_id})
        if not created and not payment_ledger.get_entry(db, tx_id, PREPARE_PAYMENT):
            logger.error(f"[Payment2PC] order={order_id} failed to create/read PREPARE ledger"); return
    try:
        reply, result = _prepare_payment_atomically(db, tx_id, order_id, user_id, amount)
    except RuntimeError as exc:
        logger.error(f"[Payment2PC] PREPARE_PAYMENT tx={tx_id} failed: {exc}"); return
    producer(PAYMENT_EVENTS_TOPIC, reply)
    if reply.get("type") in (PAYMENT_PREPARED, PAYMENT_PREPARE_FAILED):
        payment_ledger.mark_replied(db, tx_id, PREPARE_PAYMENT)
    logger.info(f"[Payment2PC] order={order_id} PREPARE_PAYMENT {result}")


def _handle_commit_payment(msg, db, producer, logger):
    tx_id = msg.get("tx_id"); order_id = msg.get("order_id")
    entry = payment_ledger.get_entry(db, tx_id, COMMIT_PAYMENT)
    if entry and entry.get("local_state") in (LedgerState.APPLIED, LedgerState.REPLIED):
        producer(PAYMENT_EVENTS_TOPIC, build_message(tx_id, order_id, entry["response_event_type"], entry.get("response_payload", {})))
        payment_ledger.mark_replied(db, tx_id, COMMIT_PAYMENT); return
    prepare_entry = payment_ledger.get_entry(db, tx_id, PREPARE_PAYMENT)
    if not prepare_entry or prepare_entry.get("result") != "success":
        logger.info(f"[Payment2PC] COMMIT_PAYMENT tx={tx_id} -- no successful PREPARE found, ignoring"); return
    if not entry:
        created = payment_ledger.create_entry(db, tx_id, COMMIT_PAYMENT, {"order_id": order_id})
        if not created and not payment_ledger.get_entry(db, tx_id, COMMIT_PAYMENT):
            logger.error(f"[Payment2PC] order={order_id} failed to create/read COMMIT ledger"); return
    try:
        reply = _commit_payment_atomically(db, tx_id, order_id)
    except RuntimeError as exc:
        logger.error(f"[Payment2PC] COMMIT_PAYMENT tx={tx_id} failed: {exc}"); return
    producer(PAYMENT_EVENTS_TOPIC, reply); payment_ledger.mark_replied(db, tx_id, COMMIT_PAYMENT)
    logger.info(f"[Payment2PC] order={order_id} PAYMENT_COMMITTED")


def _handle_abort_payment(msg, db, producer, logger):
    tx_id = msg.get("tx_id"); order_id = msg.get("order_id")
    entry = payment_ledger.get_entry(db, tx_id, ABORT_PAYMENT)
    if entry and entry.get("local_state") in (LedgerState.APPLIED, LedgerState.REPLIED):
        producer(PAYMENT_EVENTS_TOPIC, build_message(tx_id, order_id, entry["response_event_type"], entry.get("response_payload", {})))
        payment_ledger.mark_replied(db, tx_id, ABORT_PAYMENT); return
    if not entry:
        created = payment_ledger.create_entry(db, tx_id, ABORT_PAYMENT, {"order_id": order_id})
        if not created and not payment_ledger.get_entry(db, tx_id, ABORT_PAYMENT):
            logger.error(f"[Payment2PC] order={order_id} failed to create/read ABORT ledger"); return
    try:
        reply = _abort_payment_atomically(db, tx_id, order_id)
    except RuntimeError as exc:
        logger.error(f"[Payment2PC] ABORT_PAYMENT tx={tx_id} failed: {exc}"); return
    producer(PAYMENT_EVENTS_TOPIC, reply); payment_ledger.mark_replied(db, tx_id, ABORT_PAYMENT)
    logger.info(f"[Payment2PC] order={order_id} PAYMENT_ABORTED")


def _2pc_route_payment(msg: dict) -> None:
    msg_type = msg.get("type")
    if msg_type == PREPARE_PAYMENT:
        _handle_prepare_payment(msg, _db, _publish, _logger)
    elif msg_type == COMMIT_PAYMENT:
        _handle_commit_payment(msg, _db, _publish, _logger)
    elif msg_type == ABORT_PAYMENT:
        _handle_abort_payment(msg, _db, _publish, _logger)
    else:
        _logger.info(f"[Payment2PC] Unknown command type: {msg_type!r} — dropping")
