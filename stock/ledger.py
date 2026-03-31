"""
stock-service/ledger.py

Participant ledger for the stock service.

Each ledger entry is keyed by (tx_id, action_type) and stored in Redis.
This gives the stock service durable memory of what it already did for
each transaction — making it safe to replay commands and handle duplicates.

Redis key format:
    stock:ledger:<tx_id>:<action_type>

Local states:
    received  → command arrived, ledger entry created, business effect not yet applied
    applied   → business effect committed to Redis, reply not yet published to the event stream
    replied   → reply event published — fully done

The transition  applied → replied  is the crash-safe gap.
If the service crashes after applying but before publishing,
on restart it sees "applied" and re-publishes the stored reply.
"""

import json
import time
import redis as redis_module


# ── Local state constants ──────────────────────────────────────────────────────

class LedgerState:
    RECEIVED = "received"
    APPLIED  = "applied"
    REPLIED  = "replied"


# TTL for ledger entries — 24 hours is plenty for a transaction to complete.
LEDGER_TTL_SECONDS = 86400

_MARK_REPLIED_LUA = """
local raw = redis.call('GET', KEYS[1])
if not raw then
    return 0
end

local entry = cjson.decode(raw)
local now = redis.call('TIME')
entry['local_state'] = 'replied'
entry['updated_at_ms'] = tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000)

redis.call('SET', KEYS[1], cjson.encode(entry), 'EX', tonumber(ARGV[1]))
return 1
"""

_mark_replied_script = None


# ── Key helper ─────────────────────────────────────────────────────────────────

def _key(tx_id: str, action_type: str) -> str:
    return f"stock:ledger:{tx_id}:{action_type}"


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
    db:                  redis_module.Redis,
    tx_id:               str,
    action_type:         str,
    business_snapshot:   dict,
) -> bool:
    """
    Create a new ledger entry in state=received.
    business_snapshot should contain enough info to compensate later
    (e.g. the items list and quantities that were reserved).

    Returns True on success, False on Redis error.
    Uses SET NX (only set if not exists) so duplicate commands
    don't overwrite an already-progressed ledger entry.
    """
    now = int(time.time() * 1000)
    entry = {
        "tx_id": tx_id,
        "action_type": action_type,
        "local_state": LedgerState.RECEIVED,
        "result": None,
        "reply_message": None,
        "business_snapshot": business_snapshot,
        "created_at_ms": now,
        "updated_at_ms": now,
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
    reply_message:       dict,
) -> bool:
    """
    Transition ledger entry to state=applied.
    Stores the result event so it can be re-published on crash recovery.

    result: "success" | "failure"
    response_event_type: e.g. "STOCK_RESERVED" or "STOCK_RESERVATION_FAILED"
    response_payload: the payload dict to include in the reply event
    """
    try:
        key = _key(tx_id, action_type)
        raw = db.get(key)
        if not raw:
            return False
        entry = json.loads(raw)
        entry["local_state"] = LedgerState.APPLIED
        entry["result"] = result
        entry["reply_message"] = reply_message
        entry["updated_at_ms"] = int(time.time() * 1000)
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
    Called after the reply event has been successfully published to the event stream.
    """
    global _mark_replied_script
    try:
        if _mark_replied_script is None:
            _mark_replied_script = db.register_script(_MARK_REPLIED_LUA)
        result = _mark_replied_script(
            keys=[_key(tx_id, action_type)],
            args=[LEDGER_TTL_SECONDS],
            client=db,
        )
        return bool(result)
    except Exception:
        return False


# ── Startup recovery scan ──────────────────────────────────────────────────────

def get_unreplied_entries(db: redis_module.Redis) -> list[dict]:
    """
    Return all ledger entries in state=applied but not yet replied.
    Called on service startup to re-publish any replies that were lost in a crash.
    """
    try:
        keys = db.keys("stock:ledger:*")
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
