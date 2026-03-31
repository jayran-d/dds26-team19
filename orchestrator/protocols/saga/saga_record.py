"""
orchestrator/protocols/saga/saga_record.py

Durable Saga record stored in the orchestrator's own Redis (coord_db).

Key space (all in orchestrator-db, not order-db):
    saga:record:<tx_id>   → msgpack-encoded record dict
    saga:active:<order_id> → active tx_id string
    saga:seen:<message_id> → "1"  (TTL 24h, dedup)
"""

import time
import redis as redis_module
from msgspec import msgpack

from common.messages import SagaOrderStatus


SAGA_RECORD_TTL = 86400   # 24 hours
SEEN_EVENT_TTL  = 86400   # 24 hours
TIMEOUT_SECONDS = 30


def _record_key(tx_id: str) -> str:
    return f"saga:record:{tx_id}"


def _seen_key(message_id: str) -> str:
    return f"saga:seen:{message_id}"


def _active_key(order_id: str) -> str:
    return f"saga:active:{order_id}"


def _decode_record(raw: bytes | None) -> dict | None:
    if not raw:
        return None
    try:
        return msgpack.decode(raw)
    except Exception:
        try:
            import json
            return json.loads(raw)
        except Exception:
            return None


def _encode_record(record: dict) -> bytes:
    return msgpack.encode(record)


# ── Read ──────────────────────────────────────────────────────────────────────

def get(db: redis_module.Redis, tx_id: str) -> dict | None:
    try:
        return _decode_record(db.get(_record_key(tx_id)))
    except Exception:
        return None


def get_active_tx_id(db: redis_module.Redis, order_id: str) -> str | None:
    try:
        raw = db.get(_active_key(order_id))
        return raw.decode() if raw else None
    except Exception:
        return None


def load_event_context(
    db: redis_module.Redis,
    message_id: str,
    order_id: str,
    tx_id: str,
) -> tuple[bool, str | None, dict | None]:
    """Single round-trip read: (seen, active_tx_id, record)."""
    try:
        raw_seen, raw_active, raw_record = db.mget(
            _seen_key(message_id),
            _active_key(order_id),
            _record_key(tx_id),
        )
        active_tx_id = raw_active.decode() if raw_active else None
        return raw_seen is not None, active_tx_id, _decode_record(raw_record)
    except Exception:
        return False, None, None


def get_all_active(db: redis_module.Redis) -> list[dict]:
    terminal = {SagaOrderStatus.COMPLETED, SagaOrderStatus.FAILED}
    try:
        keys = db.keys("saga:record:*")
        if not keys:
            return []
        raws = db.mget(keys)
        records = []
        for raw in raws:
            if not raw:
                continue
            record = _decode_record(raw)
            if not record:
                continue
            if record.get("state") not in terminal:
                records.append(record)
        return records
    except Exception:
        return []


# ── Writes ────────────────────────────────────────────────────────────────────

def transition(
    db: redis_module.Redis,
    tx_id: str,
    new_state: str,
    last_command_type: str | None = None,
    awaiting_event_type: str | None = None,
    needs_stock_comp: bool | None = None,
    needs_payment_comp: bool | None = None,
    failure_reason: str | None = None,
    reset_timeout: bool = True,
    record: dict | None = None,
) -> bool:
    try:
        record_obj = dict(record) if record is not None else get(db, tx_id)
        if not record_obj:
            return False

        now_ms = int(time.time() * 1000)
        record_obj["state"] = new_state
        record_obj["updated_at_ms"] = now_ms

        if last_command_type is not None:
            record_obj["last_command_type"] = last_command_type
        if awaiting_event_type is not None:
            record_obj["awaiting_event_type"] = awaiting_event_type
        if needs_stock_comp is not None:
            record_obj["needs_stock_compensation"] = needs_stock_comp
        if needs_payment_comp is not None:
            record_obj["needs_payment_compensation"] = needs_payment_comp
        if failure_reason is not None:
            record_obj["failure_reason"] = failure_reason
        if reset_timeout:
            record_obj["timeout_at_ms"] = now_ms + TIMEOUT_SECONDS * 1000

        db.set(_record_key(tx_id), _encode_record(record_obj), ex=SAGA_RECORD_TTL)
        if record is not None:
            record.clear()
            record.update(record_obj)
        return True
    except Exception:
        return False


def is_seen(db: redis_module.Redis, message_id: str) -> bool:
    try:
        return db.exists(_seen_key(message_id)) == 1
    except Exception:
        return False


def mark_seen(db: redis_module.Redis, message_id: str) -> None:
    try:
        db.set(_seen_key(message_id), "1", ex=SEEN_EVENT_TTL)
    except Exception:
        pass


def get_timed_out(db: redis_module.Redis) -> list[dict]:
    now = int(time.time() * 1000)
    terminal = {SagaOrderStatus.COMPLETED, SagaOrderStatus.FAILED}
    try:
        keys = db.keys("saga:record:*")
        if not keys:
            return []
        raws = db.mget(keys)
        timed_out = []
        for raw in raws:
            if not raw:
                continue
            record = _decode_record(raw)
            if not record:
                continue
            if record.get("state") in terminal:
                continue
            if record.get("timeout_at_ms", 0) < now:
                timed_out.append(record)
        return timed_out
    except Exception:
        return []


def create_if_no_active(
    db: redis_module.Redis,
    tx_id: str,
    order_id: str,
    user_id: str,
    amount: int,
    items: list,
    initial_state: str,
    last_command_type: str,
    awaiting_event_type: str,
) -> tuple[bool, str | None]:
    now = int(time.time() * 1000)
    new_record = {
        "tx_id": tx_id,
        "order_id": order_id,
        "state": initial_state,
        "user_id": user_id,
        "amount": amount,
        "items": items,
        "last_command_type": last_command_type,
        "awaiting_event_type": awaiting_event_type,
        "needs_stock_compensation": False,
        "needs_payment_compensation": False,
        "failure_reason": None,
        "started_at_ms": now,
        "updated_at_ms": now,
        "timeout_at_ms": now + TIMEOUT_SECONDS * 1000,
    }

    active_key = _active_key(order_id)
    terminal = {SagaOrderStatus.COMPLETED, SagaOrderStatus.FAILED}

    while True:
        pipe = db.pipeline()
        try:
            pipe.watch(active_key)

            raw_active = pipe.get(active_key)
            if raw_active:
                active_tx_id = raw_active.decode()
                raw_active_record = pipe.get(_record_key(active_tx_id))
                if raw_active_record:
                    active_record = _decode_record(raw_active_record)
                    if not active_record:
                        pipe.unwatch()
                        return False, None
                    if active_record.get("state") not in terminal:
                        pipe.unwatch()
                        return False, active_tx_id

            pipe.multi()
            pipe.set(_record_key(tx_id), _encode_record(new_record), ex=SAGA_RECORD_TTL)
            pipe.set(active_key, tx_id, ex=SAGA_RECORD_TTL)
            pipe.execute()
            return True, None

        except redis_module.exceptions.WatchError:
            continue
        except Exception:
            return False, None
        finally:
            pipe.reset()


def clear_active_tx_id(db: redis_module.Redis, order_id: str, tx_id: str) -> None:
    try:
        db.eval(
            """
            if redis.call('GET', KEYS[1]) == ARGV[1] then
                return redis.call('DEL', KEYS[1])
            end
            return 0
            """,
            1,
            _active_key(order_id),
            tx_id,
        )
    except Exception:
        pass
