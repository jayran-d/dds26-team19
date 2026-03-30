"""
Participant async kafka_worker (for payment/stock services).

Simplified event processor for participants. They just:
1. Receive commands from order service
2. Execute business logic
3. Publish events back

This is much simpler than the order orchestrator.
"""

import os
import logging
import asyncio

import redis.asyncio as aioredis

from common.redis_streams_client import RedisStreamsClient

USE_REDIS_STREAMS = os.getenv("USE_REDIS_STREAMS", "true").lower() == "true"
USE_KAFKA = os.getenv("USE_KAFKA", "false").lower() == "true"
WORKER_COUNT = int(os.getenv("WORKER_COUNT", "4"))

_messaging: RedisStreamsClient | None = None
_db: aioredis.Redis | None = None
_available: bool = False
_logger: logging.Logger | None = None
_event_tasks: list[asyncio.Task] = []
_shutdown_event: asyncio.Event | None = None


async def init_kafka_async(logger: logging.Logger, db: aioredis.Redis) -> None:
    """Initialize async event processor."""
    global _messaging, _db, _available, _logger, _shutdown_event

    if not USE_REDIS_STREAMS and not USE_KAFKA:
        return

    _logger = logger
    _db = db
    _shutdown_event = asyncio.Event()

    if USE_REDIS_STREAMS:
        _messaging = RedisStreamsClient()
        await _messaging.connect()
        _available = True
        
        # Start worker tasks
        for i in range(WORKER_COUNT):
            task = asyncio.create_task(_worker(f"participant-worker-{i}"))
            _event_tasks.append(task)
        
        logger.info(
            f"[ParticipantWorker] Event loop started with {WORKER_COUNT} workers"
        )


def init_kafka(logger: logging.Logger, db=None):
    """Synchronous wrapper."""
    if not USE_REDIS_STREAMS and not USE_KAFKA:
        return
    
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            asyncio.create_task(init_kafka_async(logger, db))
        else:
            loop.run_until_complete(init_kafka_async(logger, db))
    except RuntimeError:
        asyncio.create_task(init_kafka_async(logger, db))


async def close_kafka_async() -> None:
    """Async cleanup."""
    global _available, _messaging, _event_tasks, _shutdown_event

    _available = False
    
    if _shutdown_event:
        _shutdown_event.set()
    
    if _event_tasks:
        await asyncio.gather(*_event_tasks, return_exceptions=True)
    
    if _messaging:
        await _messaging.close()
        _messaging = None


def close_kafka() -> None:
    """Synchronous wrapper."""
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


async def _worker(worker_id: str) -> None:
    """
    Worker task that consumes commands for this service.
    Subclasses should override this or call their own event handler.
    """
    try:
        # This is a placeholder. The actual service (payment/stock)
        # will implement their own worker logic by importing this
        # and calling custom event handlers.
        _logger.info(f"[ParticipantWorker] {worker_id} started (placeholder)")
        await asyncio.sleep(999999)
    
    except asyncio.CancelledError:
        _logger.info(f"[ParticipantWorker] {worker_id} shutting down")
        raise
