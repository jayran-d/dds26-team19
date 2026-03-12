"""
order-service/saga_record.py

Durable Saga record for the order service (orchestrator).

The Saga record is the order service's source of truth about a transaction.
It stores enough information to:
    - know which phase the Saga is in
    - know what command was last sent
    - know what event is being waited for
    - know whether stock/payment need compensation if we abort
    - recover after a crash by replaying the last intended command

Redis key format:
    saga:<tx_id>  →  JSON blob

One record per transaction (tx_id), not per order_id.
If the same order is ever re-checked-out, it gets a new tx_id and a new record.

Processed event deduplication:
    saga:seen:<message_id>  →  "1"  (TTL 24h)
    Used to ignore duplicate events from Kafka.
"""

import json
import time
import redis as redis_module

from common.messages import SagaOrderStatus


# ── TTLs ───────────────────────────────────────────────────────────────────────

SAGA_RECORD_TTL = 86400  # 24 hours
SEEN_EVENT_TTL = 86400  # 24 hours
TIMEOUT_SECONDS = 30  # how long to wait for a reply before recovery acts


# ── Key helpers ────────────────────────────────────────────────────────────────


def _record_key(tx_id: str) -> str:
    # Stores the full Saga record (state, flags, timing, etc.) for one transaction.
    # Keyed by tx_id because one order can have multiple transaction attempts.
    # Example: saga:tx-aaa
    return f"saga:record:{tx_id}"


def _seen_key(message_id: str) -> str:
    # Tracks which Kafka message_ids have already been processed.
    # Used to drop exact duplicate messages re-delivered by Kafka.
    # Example: saga:seen:msg-uuid-123
    return f"saga:seen:{message_id}"


def _active_key(order_id: str) -> str:
    # Maps an order_id to its currently active tx_id.
    # Used by is_stale() to detect events from old/abandoned transaction attempts.
    # Example: saga:active:order-456  →  "tx-aaa"
    return f"saga:active:{order_id}"


# ============================================================
# CREATE
# ============================================================


def create(
    db: redis_module.Redis,
    tx_id: str,
    order_id: str,
    user_id: str,
    amount: int,
    items: list,
) -> bool:
    """
    Create a new Saga record in state=pending.

    items: collapsed list of {item_id, quantity} dicts.
    Returns True on success.
    """
    now = int(time.time() * 1000)
    record = {
        "tx_id": tx_id,
        "order_id": order_id,
        "state": SagaOrderStatus.PENDING,
        # Transaction data — needed to build commands on recovery
        "user_id": user_id,
        "amount": amount,
        "items": items,
        # What the orchestrator last did / is waiting for
        "last_command_type": None,
        "awaiting_event_type": None,
        # Compensation flags — set to True once a step succeeds
        # so recovery knows whether to release/refund
        "needs_stock_compensation": False,
        "needs_payment_compensation": False,
        # Failure info for debugging
        "failure_reason": None,
        # Timing
        "started_at_ms": now,
        "updated_at_ms": now,
        "timeout_at_ms": now + TIMEOUT_SECONDS * 1000,
    }
    try:
        db.set(_record_key(tx_id), json.dumps(record), ex=SAGA_RECORD_TTL)
        # also store tx_id → order_id mapping for recovery scan
        db.set(_active_key(order_id), tx_id, ex=SAGA_RECORD_TTL)
        return True
    except Exception:
        return False


# ============================================================
# READ
# ============================================================


def get(db: redis_module.Redis, tx_id: str) -> dict | None:
    """Return the Saga record for tx_id, or None if not found."""
    try:
        raw = db.get(_record_key(tx_id))
        return json.loads(raw) if raw else None
    except Exception:
        return None


def get_active_tx_id(db: redis_module.Redis, order_id: str) -> str | None:
    """Return the active tx_id for an order, or None."""
    try:
        raw = db.get(_active_key(order_id))
        return raw.decode() if raw else None
    except Exception:
        return None


def get_all_active(db: redis_module.Redis) -> list[dict]:
    """
    Return all Saga records that are not in a terminal state.
    Used by the recovery scanner on startup.
    """
    terminal = {SagaOrderStatus.COMPLETED, SagaOrderStatus.FAILED}
    try:
        keys = db.keys("saga:record:*")
        records = []
        for key in keys:
            raw = db.get(key)
            if not raw:
                continue
            record = json.loads(raw)
            if record.get("state") not in terminal:
                records.append(record)
        return records
    except Exception:
        return []



# ============================================================
# UPDATE STATE
# ============================================================


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
) -> bool:
    """
    Update the Saga record state and optional fields atomically.

    Call this BEFORE publishing the next command so that if we crash
    after writing but before publishing, recovery can replay the command.

    Returns True on success.
    """
    try:
        raw = db.get(_record_key(tx_id))
        if not raw:
            return False
        record = json.loads(raw)

        record["state"] = new_state
        record["updated_at_ms"] = int(time.time() * 1000)

        if last_command_type is not None:
            record["last_command_type"] = last_command_type
        if awaiting_event_type is not None:
            record["awaiting_event_type"] = awaiting_event_type
        if needs_stock_comp is not None:
            record["needs_stock_compensation"] = needs_stock_comp
        if needs_payment_comp is not None:
            record["needs_payment_compensation"] = needs_payment_comp
        if failure_reason is not None:
            record["failure_reason"] = failure_reason
        if reset_timeout:
            record["timeout_at_ms"] = int(time.time() * 1000) + TIMEOUT_SECONDS * 1000

        db.set(_record_key(tx_id), json.dumps(record), ex=SAGA_RECORD_TTL)
        return True
    except Exception:
        return False


# ============================================================
# EVENT DEDUPLICATION
# ============================================================


def is_seen(db: redis_module.Redis, message_id: str) -> bool:
    """Return True if this message_id has already been processed."""
    try:
        return db.exists(_seen_key(message_id)) == 1
    except Exception:
        return False


def mark_seen(db: redis_module.Redis, message_id: str) -> None:
    """Mark a message_id as processed. TTL = 24h."""
    try:
        db.set(_seen_key(message_id), "1", ex=SEEN_EVENT_TTL)
    except Exception:
        pass


# ============================================================
# STALE TX CHECK
# Example:
# Scenario 1 — Checkout called twice on the same order
# POST /orders/checkout/order-123   ← first attempt
#   → generates tx_id = "tx-aaa"
#   → publishes RESERVE_STOCK with tx_id="tx-aaa"
#   → order stuck in reserving_stock (maybe stock service was slow)

# POST /orders/checkout/order-123   ← second attempt (client retried)
#   → generates tx_id = "tx-bbb"
#   → publishes RESERVE_STOCK with tx_id="tx-bbb"
#   → active tx_id for order-123 is now "tx-bbb"

# stock service finally replies with STOCK_RESERVED, tx_id="tx-aaa"
#   → order service receives it
#   → is_stale() checks: active tx_id for order-123 = "tx-bbb" ≠ "tx-aaa"
#   → drop it
# ============================================================


def is_stale(db: redis_module.Redis, order_id: str, tx_id: str) -> bool:
    """
    Return True if the event's tx_id does not match the active Saga for this order.
    Stale events from old transaction attempts should be ignored.
    """
    active_tx_id = get_active_tx_id(db, order_id)
    return active_tx_id != tx_id


# ============================================================
# TIMEOUT CHECK
# ============================================================


def get_timed_out(db: redis_module.Redis) -> list[dict]:
    """
    Return all non-terminal Saga records whose timeout_at_ms has passed.
    Used by the recovery scanner to find stuck transactions.
    """
    now = int(time.time() * 1000)
    terminal = {SagaOrderStatus.COMPLETED, SagaOrderStatus.FAILED}
    try:
        keys = db.keys("saga:record:*")
        timed_out = []
        for key in keys:
            raw = db.get(key)
            if not raw:
                continue
            record = json.loads(raw)
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
    """
    Atomically create a new Saga record only if this order does not already
    have an active non-terminal Saga.

    Returns:
        (True, None)             -> new Saga was created and claimed the order
        (False, active_tx_id)    -> another Saga is already in progress
        (False, None)            -> Redis/other failure
    """
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
            # WATCH turns the active order slot into an optimistic lock.
            # If two checkout requests race, only one of them will win EXEC.
            pipe.watch(active_key)

            raw_active = pipe.get(active_key)
            if raw_active:
                active_tx_id = raw_active.decode()
                raw_active_record = pipe.get(_record_key(active_tx_id))

                if raw_active_record:
                    active_record = json.loads(raw_active_record)
                    if active_record.get("state") not in terminal:
                        pipe.unwatch()
                        return False, active_tx_id

            # MULTI/EXEC is the atomic "claim this order + create Saga record" step.
            pipe.multi()
            pipe.set(_record_key(tx_id), json.dumps(new_record), ex=SAGA_RECORD_TTL)
            pipe.set(active_key, tx_id, ex=SAGA_RECORD_TTL)
            pipe.execute()
            return True, None

        except redis_module.exceptions.WatchError:
            # Another request changed the active slot between WATCH and EXEC.
            # Retry and re-check who owns the order now.
            continue
        except Exception:
            return False, None
        finally:
            pipe.reset()

def clear_active_tx_id(db: redis_module.Redis, order_id: str, tx_id: str) -> None:
    """
    Clear the active tx pointer only if it still points to this tx_id.
    This prevents a finished Saga from accidentally deleting a newer claim.
    """
    try:
        active_tx_id = get_active_tx_id(db, order_id)
        if active_tx_id == tx_id:
            db.delete(_active_key(order_id))
    except Exception:
        pass
