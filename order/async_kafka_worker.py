"""
order-service/async_kafka_worker.py

Async worker for event-driven Saga processing.

Replaces the synchronous kafka_worker.py with:
- Async/await Python (asyncio)
- Redis Streams instead of Kafka
- Multiple concurrent worker tasks
- Non-blocking event processing

API remains the same (async versions of init_kafka, start_checkout, etc).
"""

import os
import uuid
import asyncio
import logging

import redis.asyncio as aioredis

from transactions_modes.saga.saga import (
    saga_route_order,
    saga_start_checkout,
    recover as saga_recover,
    check_timeouts as saga_check_timeouts,
)
from transactions_modes.simple import simple_route_order, simple_start_checkout
from transactions_modes.two_pc import _2pc_route_order, _2pc_start_checkout
from common.redis_streams_client import RedisStreamsClient
from common.messages import (
    ALL_TOPICS, STOCK_EVENTS_TOPIC, PAYMENT_EVENTS_TOPIC,
    SagaOrderStatus
)

USE_KAFKA = os.getenv("USE_KAFKA", "false").lower() == "true"
USE_REDIS_STREAMS = os.getenv("USE_REDIS_STREAMS", "true").lower() == "true"
TRANSACTION_MODE = os.getenv("TRANSACTION_MODE", "saga")
TIMEOUT_SCAN_INTERVAL_SECONDS = 5
WORKER_COUNT = int(os.getenv("WORKER_COUNT", "4"))

# ── Module-level state ─────────────────────────────────────────────────────────
_messaging: RedisStreamsClient | None = None
_db: aioredis.Redis | None = None
_available: bool = False
_logger: logging.Logger | None = None
_event_tasks: list[asyncio.Task] = []
_timeout_task: asyncio.Task | None = None
_shutdown_event: asyncio.Event | None = None


# ============================================================
# INIT / TEARDOWN
# ============================================================


async def init_kafka_async(logger: logging.Logger, db: aioredis.Redis) -> None:
    """
    Async initialization. Called once at startup.
    """
    global _messaging, _db, _available, _logger, _shutdown_event

    if not USE_REDIS_STREAMS and not USE_KAFKA:
        logger.info("[OrderWorker] Event loop disabled (USE_REDIS_STREAMS=false, USE_KAFKA=false)")
        return

    _logger = logger
    _db = db
    _shutdown_event = asyncio.Event()

    if USE_REDIS_STREAMS:
        _messaging = RedisStreamsClient()
        await _messaging.connect()
        _available = True
        
        if TRANSACTION_MODE == "saga":
            await saga_recover(_db, _messaging.publish, _logger)
        
        # Start timeout scanner for Saga
        if TRANSACTION_MODE == "saga":
            _timeout_task = asyncio.create_task(_timeout_loop())
        
        # Start worker tasks
        for i in range(WORKER_COUNT):
            task = asyncio.create_task(_worker(f"order-worker-{i}"))
            _event_tasks.append(task)
        
        logger.info(
            f"[OrderWorker] Event loop started with {WORKER_COUNT} workers "
            f"(mode={TRANSACTION_MODE})"
        )


def init_kafka(logger: logging.Logger, db):
    """
    Synchronous wrapper for startup from Flask app.
    Must be called from an async context or scheduled appropriately.
    """
    if not USE_REDIS_STREAMS and not USE_KAFKA:
        return
    
    # Schedule async init to run in the background
    # This assumes an event loop is already running (from hypercorn)
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            asyncio.create_task(init_kafka_async(logger, db))
        else:
            loop.run_until_complete(init_kafka_async(logger, db))
    except RuntimeError:
        # No event loop — create one
        asyncio.create_task(init_kafka_async(logger, db))


async def close_kafka_async() -> None:
    """Async cleanup."""
    global _available, _messaging, _event_tasks, _timeout_task, _shutdown_event

    _available = False
    
    if _shutdown_event:
        _shutdown_event.set()
    
    # Wait for tasks to finish cleanly
    if _event_tasks:
        await asyncio.gather(*_event_tasks, return_exceptions=True)
    
    if _timeout_task:
        _timeout_task.cancel()
        try:
            await _timeout_task
        except asyncio.CancelledError:
            pass
    
    if _messaging:
        await _messaging.close()
        _messaging = None


def close_kafka() -> None:
    """Synchronous wrapper for shutdown."""
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            asyncio.create_task(close_kafka_async())
        else:
            loop.run_until_complete(close_kafka_async())
    except RuntimeError:
        pass


def is_available() -> bool:
    return _available


# ============================================================
# PUBLIC API (called by app.py)
# ============================================================


async def start_checkout_async(order_id: str, order_entry) -> dict:
    """
    Async version of start_checkout.
    """
    if not _available or _messaging is None:
        raise RuntimeError("Event system is not available")

    if TRANSACTION_MODE == "saga":
        return await saga_start_checkout_async(
            _messaging.publish, _db, _logger, order_id, order_entry
        )
    
    raise RuntimeError(f"Unknown TRANSACTION_MODE: {TRANSACTION_MODE}")


def start_checkout(order_id: str, order_entry) -> dict:
    """
    Synchronous wrapper that schedules async start_checkout.
    Must be called from async context.
    """
    if not _available or _messaging is None:
        raise RuntimeError("Event system is not available")
    
    if TRANSACTION_MODE == "saga":
        # Use saga_start_checkout (sync version that uses async publish internally)
        # For now, we'll keep using the sync version and make it use async publish
        return saga_start_checkout_sync(
            _messaging.publish, _db, _logger, order_id, order_entry
        )
    
    raise RuntimeError(f"Unknown TRANSACTION_MODE: {TRANSACTION_MODE}")


# ============================================================
# ASYNC WRAPPERS FOR SAGA
# ============================================================


async def saga_start_checkout_async(
    publish,
    db: aioredis.Redis,
    logger,
    order_id: str,
    order_entry,
) -> dict:
    """Async version of saga_start_checkout."""
    from transactions_modes.saga import saga_record
    from common.messages import (
        SagaOrderStatus, RESERVE_STOCK, STOCK_RESERVED,
        build_reserve_stock, STOCK_COMMANDS_TOPIC
    )
    
    tx_id = str(uuid.uuid4())

    # Collapse duplicate item_ids
    item_counts = {}
    for item_id, quantity in order_entry.items:
        key = str(item_id)
        item_counts[key] = item_counts.get(key, 0) + quantity
    items = [{"item_id": k, "quantity": v} for k, v in item_counts.items()]

    # Create saga record
    ok, active_tx_id = saga_record.create_if_no_active(
        db=db,
        tx_id=tx_id,
        order_id=order_id,
        user_id=str(order_entry.user_id),
        amount=int(order_entry.total_cost),
        items=items,
        initial_state=SagaOrderStatus.RESERVING_STOCK,
        last_command_type=RESERVE_STOCK,
        awaiting_event_type=STOCK_RESERVED,
    )

    if not ok:
        if active_tx_id:
            logger.info(
                f"[Saga] checkout already in progress for order={order_id} active_tx={active_tx_id}"
            )
            return {
                "started": False,
                "reason": "already_in_progress",
                "tx_id": active_tx_id,
            }
        logger.error(f"[Saga] failed to create Saga record for order={order_id}")
        return {"started": False, "reason": "error"}

    # Set status and tx_id
    await db.set(f"order:{order_id}:tx_id", tx_id)
    await db.set(f"order:{order_id}:status", SagaOrderStatus.RESERVING_STOCK)

    # Publish command
    cmd = build_reserve_stock(tx_id, order_id, items)
    await publish("stock", cmd)

    logger.info(f"[Saga] started tx={tx_id} order={order_id}")
    return {"started": True, "reason": "started", "tx_id": tx_id}


def saga_start_checkout_sync(
    publish_async,
    db: aioredis.Redis,
    logger,
    order_id: str,
    order_entry,
) -> dict:
    """
    Sync version that uses async publish by creating a task.
    Used as bridge until we convert everything to async.
    """
    # For now, delegate to the async version scheduled as a task
    # This is a temporary bridge — in production, the checkout route
    # itself should be async
    task = asyncio.create_task(
        saga_start_checkout_async(publish_async, db, logger, order_id, order_entry)
    )
    # Block until done (not ideal, but temporary bridge)
    import time
    timeout = time.time() + 5
    while not task.done() and time.time() < timeout:
        time.sleep(0.01)
    
    if task.done():
        return task.result()
    else:
        raise RuntimeError("start_checkout_async timeout")


# ============================================================
# EVENT LOOP
# ============================================================


async def _worker(worker_id: str) -> None:
    """Worker task that consumes events from Redis Streams."""
    try:
        async for msg_id, data in _messaging.consume("order", worker_id):
            try:
                # Parse message
                msg = {}
                for k, v in data.items():
                    key = k.decode() if isinstance(k, bytes) else k
                    val = v.decode() if isinstance(v, bytes) else v
                    msg[key] = val
                
                msg_type = msg.get("type")
                _logger.info(
                    f"[OrderWorker] event={msg_type} "
                    f"order={msg.get('order_id')} tx={msg.get('tx_id')}"
                )

                if TRANSACTION_MODE == "saga":
                    await saga_route_order_async(
                        msg, _db, _messaging.publish, _logger
                    )
                
                await _messaging.ack("order", msg_id)
            
            except Exception as exc:
                _logger.error(f"[OrderWorker] Error processing message: {exc}")
    
    except asyncio.CancelledError:
        _logger.info(f"[OrderWorker] {worker_id} shutting down")
        raise


async def saga_route_order_async(
    msg: dict,
    db: aioredis.Redis,
    publish,
    logger,
) -> None:
    """Async version of saga_route_order."""
    from transactions_modes.saga import saga_record
    from common.messages import (
        SagaOrderStatus,
        STOCK_RESERVED,
        STOCK_RESERVATION_FAILED,
        STOCK_RELEASED,
        PAYMENT_SUCCESS,
        PAYMENT_FAILED,
    )
    
    msg_type = msg.get("type")
    message_id = msg.get("message_id")
    tx_id = msg.get("tx_id")
    order_id = msg.get("order_id")

    if not message_id or not tx_id or not order_id:
        logger.warning(f"[Saga] malformed event: {msg}")
        return

    if saga_record.is_seen(db, message_id):
        logger.debug(f"[Saga] duplicate message_id={message_id} — dropping")
        return

    if saga_record.is_stale(db, order_id, tx_id):
        logger.debug(f"[Saga] stale tx_id={tx_id} for order={order_id} — dropping")
        saga_record.mark_seen(db, message_id)
        return

    record = saga_record.get(db, tx_id)
    if not record:
        logger.warning(f"[Saga] no record found for tx_id={tx_id} — dropping")
        return

    state = record.get("state")

    if state in (SagaOrderStatus.COMPLETED, SagaOrderStatus.FAILED):
        logger.debug(
            f"[Saga] event {msg_type} arrived in terminal state={state} — dropping"
        )
        saga_record.mark_seen(db, message_id)
        return

    # Route to handler
    # (Keep using sync saga handlers for now, just wrap the publish call)
    saga_route_order(msg, db, publish, logger)
    
    saga_record.mark_seen(db, message_id)


async def _timeout_loop() -> None:
    """Periodic timeout check for Saga recovery."""
    while not _shutdown_event.is_set():
        try:
            await asyncio.sleep(TIMEOUT_SCAN_INTERVAL_SECONDS)
            if TRANSACTION_MODE == "saga":
                saga_check_timeouts(_db, _messaging.publish, _logger)
        except asyncio.CancelledError:
            break
        except Exception as exc:
            _logger.error(f"[OrderWorker] Timeout loop error: {exc}")
