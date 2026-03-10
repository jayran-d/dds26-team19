"""
payment-service/ledger.py

Participant ledger for the payment service.

Identical structure to stock/ledger.py — see that file for full explanation.

Redis key format:
    payment:ledger:<tx_id>:<action_type>

Local states:
    received  → command arrived, ledger entry created, business effect not yet applied
    applied   → business effect committed to Redis, reply not yet published to Kafka
    replied   → reply event published — fully done
"""

import json
import time
import redis as redis_module


# ── Local state constants ──────────────────────────────────────────────────────

class LedgerState:
    RECEIVED = "received"
    APPLIED  = "applied"
    REPLIED  = "replied"


LEDGER_TTL_SECONDS = 86400


# ── Key helper ─────────────────────────────────────────────────────────────────

def _key(tx_id: str, action_type: str) -> str:
    return f"payment:ledger:{tx_id}:{action_type}"


# ── Read ───────────────────────────────────────────────────────────────────────

def get_entry(db: redis_module.Redis, tx_id: str, action_type: str) -> dict | None:
    """
    Return the ledger entry for (tx_id, action_type), or None if it doesn't exist.
    """
    try:
        raw = db.get(_key(tx_id, action_type))
        return json.loads(raw) if raw else None
    except Exception:
        return None


# ── Write ──────────────────────────────────────────────────────────────────────

def create_entry(
    db:                redis_module.Redis,
    tx_id:             str,
    action_type:       str,
    business_snapshot: dict,
) -> bool:
    """
    Create a new ledger entry in state=received.
    business_snapshot should contain enough info to compensate later
    (e.g. user_id and amount that was charged).

    Uses SET NX so duplicate commands don't overwrite a progressed entry.
    Returns True on success, False on Redis error or if entry already exists.
    """
    entry = {
        "tx_id":               tx_id,
        "action_type":         action_type,
        "local_state":         LedgerState.RECEIVED,
        "result":              None,
        "response_event_type": None,
        "response_payload":    None,
        "business_snapshot":   business_snapshot,
        "created_at_ms":       int(time.time() * 1000),
        "updated_at_ms":       int(time.time() * 1000),
    }
    try:
        key = _key(tx_id, action_type)
        set_result = db.set(key, json.dumps(entry), nx=True, ex=LEDGER_TTL_SECONDS)
        return set_result is not None
    except Exception:
        return False


def mark_applied(
    db:                  redis_module.Redis,
    tx_id:               str,
    action_type:         str,
    result:              str,
    response_event_type: str,
    response_payload:    dict,
) -> bool:
    """
    Transition ledger entry to state=applied.
    Stores the result event so it can be re-published on crash recovery.

    result: "success" | "failure"
    response_event_type: e.g. "PAYMENT_SUCCESS" or "PAYMENT_FAILED"
    response_payload: the payload dict to include in the reply event
    """
    try:
        key = _key(tx_id, action_type)
        raw = db.get(key)
        if not raw:
            return False
        entry = json.loads(raw)
        entry["local_state"]         = LedgerState.APPLIED
        entry["result"]              = result
        entry["response_event_type"] = response_event_type
        entry["response_payload"]    = response_payload
        entry["updated_at_ms"]       = int(time.time() * 1000)
        db.set(key, json.dumps(entry), ex=LEDGER_TTL_SECONDS)
        return True
    except Exception:
        return False


def mark_replied(
    db:          redis_module.Redis,
    tx_id:       str,
    action_type: str,
) -> bool:
    """
    Transition ledger entry to state=replied.
    Called after the reply event has been successfully published to Kafka.
    """
    try:
        key = _key(tx_id, action_type)
        raw = db.get(key)
        if not raw:
            return False
        entry = json.loads(raw)
        entry["local_state"]   = LedgerState.REPLIED
        entry["updated_at_ms"] = int(time.time() * 1000)
        db.set(key, json.dumps(entry), ex=LEDGER_TTL_SECONDS)
        return True
    except Exception:
        return False


# ── Startup recovery scan ──────────────────────────────────────────────────────

def get_unreplied_entries(db: redis_module.Redis) -> list[dict]:
    """
    Return all ledger entries in state=applied but not yet replied.
    Called on service startup to re-publish any replies lost in a crash.
    """
    try:
        keys = db.keys("payment:ledger:*")
        entries = []
        for key in keys:
            raw = db.get(key)
            if not raw:
                continue
            entry = json.loads(raw)
            if entry.get("local_state") == LedgerState.APPLIED:
                entries.append(entry)
        return entries
    except Exception:
        return []