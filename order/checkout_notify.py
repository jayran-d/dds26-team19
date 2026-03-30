"""
order/checkout_notify.py

In-process checkout completion notification.

Supports both:
    - threading.Event waiters for synchronous callers
    - asyncio.Future waiters for async order-service checkout handlers

The saga event loop runs in background threads, so notify() resolves async
waiters via loop.call_soon_threadsafe().
"""

import asyncio
import threading

_lock = threading.Lock()
_thread_waiting: dict[str, list[threading.Event]] = {}
_async_waiting: dict[str, list[tuple[asyncio.AbstractEventLoop, asyncio.Future]]] = {}


def register(order_id: str) -> threading.Event:
    ev = threading.Event()
    with _lock:
        _thread_waiting.setdefault(order_id, []).append(ev)
    return ev


def unregister(order_id: str, ev: threading.Event) -> None:
    with _lock:
        events = _thread_waiting.get(order_id)
        if events and ev in events:
            events.remove(ev)
            if not events:
                _thread_waiting.pop(order_id, None)


def register_async(order_id: str) -> asyncio.Future:
    loop = asyncio.get_running_loop()
    fut = loop.create_future()
    with _lock:
        _async_waiting.setdefault(order_id, []).append((loop, fut))
    return fut


def unregister_async(order_id: str, fut: asyncio.Future) -> None:
    with _lock:
        waiters = _async_waiting.get(order_id)
        if not waiters:
            return
        waiters[:] = [(loop, entry) for loop, entry in waiters if entry is not fut]
        if not waiters:
            _async_waiting.pop(order_id, None)


def _resolve_future(fut: asyncio.Future) -> None:
    if not fut.done():
        fut.set_result(True)


def notify(order_id: str) -> None:
    with _lock:
        events = _thread_waiting.pop(order_id, [])
        futures = _async_waiting.pop(order_id, [])

    for ev in events:
        ev.set()

    for loop, fut in futures:
        loop.call_soon_threadsafe(_resolve_future, fut)
