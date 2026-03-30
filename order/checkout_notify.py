"""
order/checkout_notify.py

In-process notification for checkout completion.

Replaces the 50ms polling loop in app.py with threading.Event waits.
The event loop thread sets the event as soon as the saga reaches a terminal
state, so the HTTP handler unblocks within microseconds instead of up to 50ms.

Thread safety: all state is protected by a single lock.
Multiple concurrent requesters for the same order_id each get their own event.
"""

import threading

_lock = threading.Lock()
_waiting: dict[str, list[threading.Event]] = {}


def register(order_id: str) -> threading.Event:
    """
    Register a waiter for order_id. Call this BEFORE starting checkout
    to avoid a race where the saga completes before ev.wait() is called.
    threading.Event.wait() on an already-set event returns immediately.
    """
    ev = threading.Event()
    with _lock:
        _waiting.setdefault(order_id, []).append(ev)
    return ev


def notify(order_id: str) -> None:
    """
    Signal all waiters for order_id that the saga has reached a terminal state.
    Called by the saga event handlers in saga.py.
    """
    with _lock:
        events = _waiting.pop(order_id, [])
    for ev in events:
        ev.set()


def unregister(order_id: str, ev: threading.Event) -> None:
    """Remove a specific waiter, e.g. on timeout cleanup."""
    with _lock:
        events = _waiting.get(order_id)
        if events and ev in events:
            events.remove(ev)
            if not events:
                _waiting.pop(order_id, None)
