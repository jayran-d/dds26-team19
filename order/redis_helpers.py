import threading
import time

from common.messages import SagaOrderStatus


TERMINAL_STATUSES = {
    SagaOrderStatus.COMPLETED,
    SagaOrderStatus.FAILED,
}

_waiters_lock = threading.Lock()
_waiters: dict[str, "_CheckoutWaiter"] = {}


class _CheckoutWaiter:
    __slots__ = ("event", "status", "waiter_count")

    def __init__(self) -> None:
        self.event = threading.Event()
        self.status: str | None = None
        self.waiter_count = 0


def _register_waiter(order_id: str) -> _CheckoutWaiter:
    with _waiters_lock:
        waiter = _waiters.get(order_id)
        if waiter is None:
            waiter = _CheckoutWaiter()
            _waiters[order_id] = waiter
        waiter.waiter_count += 1
        return waiter


def _unregister_waiter(order_id: str, waiter: _CheckoutWaiter) -> None:
    with _waiters_lock:
        current = _waiters.get(order_id)
        if current is not waiter:
            return
        waiter.waiter_count -= 1
        if waiter.waiter_count <= 0:
            _waiters.pop(order_id, None)


def notify_status(order_id: str, status: str) -> None:
    with _waiters_lock:
        waiter = _waiters.get(order_id)
        if waiter is None:
            return
        waiter.status = status
        if status in TERMINAL_STATUSES:
            waiter.event.set()
        else:
            waiter.event.clear()


def wait_for_terminal_status(
    order_id: str,
    get_status,
    timeout_seconds: float,
) -> str | None:
    deadline = time.monotonic() + timeout_seconds

    while True:
        status = get_status(order_id) or SagaOrderStatus.PENDING
        if status in TERMINAL_STATUSES:
            return status

        waiter = _register_waiter(order_id)
        try:
            # Re-check after registration so a terminal transition cannot be
            # missed between the initial read and event subscription.
            status = get_status(order_id) or SagaOrderStatus.PENDING
            if status in TERMINAL_STATUSES:
                return status

            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return None

            waiter.event.wait(remaining)
            if waiter.status in TERMINAL_STATUSES:
                return waiter.status
        finally:
            _unregister_waiter(order_id, waiter)

# ============================================================
# REDIS HELPERS
# ============================================================

def set_status(logger, db, order_id: str, status: str) -> None:
    try:
        db.set(f"order:{order_id}:status", status)
        notify_status(order_id, status)
    except Exception as e:
        if logger is not None:
            logger.error(f"[OrderKafka] Failed to set status {status} for {order_id}: {e}")


def get_order(logger, db, order_id: str):
    """Return the raw msgpack bytes for an order from Redis."""
    try:
        return db.get(order_id)
    except Exception as e:
        logger.error(f"[OrderKafka] Failed to get order {order_id}: {e}")
        return None


# def get_tx_id(db, order_id: str) -> str | None:
#     try:
#         val = db.get(f"order:{order_id}:tx_id")
#         return val.decode() if val else None
#     except Exception:
#         return None
