"""
orchestrator/leader_lease.py

Redis-backed leader lease for orchestrator replicas.

Only the current lease-holder may run:
  - startup recovery (saga_recover / recover_incomplete_2pc)
  - periodic timeout scanning / 2PC recovery loops

All replicas still:
  - accept /transactions/checkout HTTP traffic
  - consume stock.events and payment.events via consumer groups

Lease mechanics:
  SET orchestrator:leader <instance_id> NX EX <LEASE_TTL>

The holder renews every RENEW_INTERVAL seconds.  If the holder dies its
key expires within LEASE_TTL seconds and another replica wins on its next
renewal attempt.
"""

import logging
import os
import socket
import threading
import time
import uuid

_LEASE_KEY = "orchestrator:leader"
_LEASE_TTL = 15        # seconds — lease expires if holder crashes
_RENEW_INTERVAL = 5    # seconds — how often holders re-extend the TTL

# Lua: DEL only if the value still matches our instance_id.
# Atomically guards against deleting another replica's lease on shutdown.
_RELEASE_SCRIPT = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('DEL', KEYS[1])
end
return 0
"""

_redis = None
_logger = logging.getLogger(__name__)
_instance_id: str = f"{socket.gethostname()}-{os.getpid()}-{uuid.uuid4().hex[:8]}"
_is_leader: bool = False
_lock = threading.Lock()
_running = False


def init_lease(redis_client, logger=None) -> bool:
    """
    Initialise the lease against *redis_client*.

    Immediately tries to acquire leadership and starts a daemon thread that
    renews (or re-acquires after expiry) the lease every RENEW_INTERVAL
    seconds.

    Returns True if this instance won the initial election, False otherwise.
    """
    global _redis, _logger, _running
    _redis = redis_client
    if logger is not None:
        _logger = logger
    _running = True
    won = _try_acquire()
    threading.Thread(target=_renew_loop, daemon=True, name="leader-lease-renew").start()
    return won


def is_leader() -> bool:
    """Return True if this replica currently holds the leader lease."""
    return _is_leader


def release_lease() -> None:
    """
    Voluntarily release the lease on clean shutdown.

    Uses a Lua compare-and-delete so we never delete another replica's lease
    even if the key expired and was re-acquired between our check and the DEL.
    """
    global _is_leader, _running
    _running = False
    if _redis is not None:
        try:
            _redis.eval(_RELEASE_SCRIPT, 1, _LEASE_KEY, _instance_id)
        except Exception:
            pass
    with _lock:
        _is_leader = False


# ── Internal ──────────────────────────────────────────────────────────────────

def _try_acquire() -> bool:
    global _is_leader
    if _redis is None:
        return False
    try:
        # Try to set the key only if it does not exist.
        result = _redis.set(_LEASE_KEY, _instance_id, nx=True, ex=_LEASE_TTL)
        if result:
            with _lock:
                _is_leader = True
            _logger.info(
                f"[LeaderLease] acquired leader lease (instance={_instance_id})"
            )
            return True

        # Check whether we already own the lease and just need to renew.
        current = _redis.get(_LEASE_KEY)
        if current and current.decode() == _instance_id:
            _redis.expire(_LEASE_KEY, _LEASE_TTL)
            with _lock:
                _is_leader = True
            return True

        # Another instance holds the lease.
        with _lock:
            if _is_leader:
                _logger.warning("[LeaderLease] lost leader lease to another instance")
            _is_leader = False
        return False

    except Exception as exc:
        # Any Redis error means we cannot verify we still hold the lease.
        # Conservatively step down: the lease TTL will expire and the other
        # replicas will elect a new leader.  Transactions simply wait for the
        # next timeout scan once leadership is re-established.
        _logger.debug(f"[LeaderLease] lease check failed, stepping down: {exc}")
        with _lock:
            _is_leader = False
        return False


def _renew_loop() -> None:
    while _running:
        time.sleep(_RENEW_INTERVAL)
        _try_acquire()
