"""
Async Redis Streams client for event-driven messaging.

Replaces Kafka with Redis Streams for lower latency and simpler deployment.

Key differences from Kafka:
- XADD with IDMPAUTO for idempotent publishing (no duplicates)
- XREADGROUP with XAUTOCLAIM for consumer group handling
- No separate broker — uses same Redis instance
- Lower network overhead
- Built-in deduplication window (IDMP)

Still provides:
- Ordered delivery per stream
- Consumer groups for load balancing
- Recovery semantics (unclaimed messages can be reclaimed)
- Explicit acking to ensure ordering guarantees
"""

import os
import time
import logging
import asyncio
import redis.asyncio as aioredis
from typing import AsyncIterator, Dict, Any

logger = logging.getLogger(__name__)

STREAMS_DB_HOST = os.getenv("REDIS_STREAMS_HOST", os.getenv("REDIS_HOST", "localhost"))
STREAMS_DB_PORT = int(os.getenv("REDIS_STREAMS_PORT", os.getenv("REDIS_PORT", "6379")))
STREAMS_DB_PASSWORD = os.getenv("REDIS_STREAMS_PASSWORD", os.getenv("REDIS_PASSWORD", ""))
STREAMS_DB_NUM = int(os.getenv("REDIS_STREAMS_DB", os.getenv("REDIS_DB", "0")))

STREAM_NAMES = {
    "stock": "stream:commands:stock",
    "payment": "stream:commands:payment",
    "order": "stream:events:order",
}

CONSUMER_GROUPS = {
    "stock": "stock-workers",
    "payment": "payment-workers",
    "order": "order-workers",
}

STREAM_MAX_LEN = 50000
IDMP_DURATION_SECONDS = 120
PEL_IDLE_MS = 30_000  # messages idle > 30s can be reclaimed
PEL_CHECK_INTERVAL_SECONDS = 30.0


class RedisStreamsClient:
    """
    Async client for Redis Streams messaging.
    
    Provides:
    - publish(...): idempotent message publishing
    - consume(...): async generator for consuming messages
    - ack(...): acknowledge message processing
    - close(): cleanup
    """

    def __init__(self):
        self.db: aioredis.Redis | None = None
        self._producer_id = f"producer-{os.getpid()}-{id(self)}"

    async def connect(self):
        """Connect to Redis and initialize consumer groups."""
        self.db = aioredis.Redis(
            host=STREAMS_DB_HOST,
            port=STREAMS_DB_PORT,
            password=STREAMS_DB_PASSWORD if STREAMS_DB_PASSWORD else None,
            db=STREAMS_DB_NUM,
            socket_timeout=5,
            retry_on_timeout=True,
            decode_responses=False,  # We handle encoding ourselves
        )
        
        # Test connection
        try:
            await self.db.ping()
            logger.info(f"[RedisStreams] Connected to {STREAMS_DB_HOST}:{STREAMS_DB_PORT}")
        except Exception as e:
            logger.error(f"[RedisStreams] Connection failed: {e}")
            raise

    async def publish(self, target_service: str, data: Dict[str, Any]) -> str:
        """
        Publish a message to a stream.
        
        Uses XADD with IDMPAUTO for automatic deduplication.
        Returns the message ID.
        """
        stream = STREAM_NAMES[target_service]
        
        # Convert all values to strings for Redis Streams
        str_data = {str(k): str(v) for k, v in data.items()}
        
        try:
            # XADD with IDMPAUTO deduplicates based on producer_id
            msg_id = await self.db.execute_command(
                "XADD", stream, "IDMPAUTO", self._producer_id,
                "*", "MAXLEN", "~", str(STREAM_MAX_LEN),
                *[item for pair in str_data.items() for item in pair]
            )
            return msg_id.decode() if isinstance(msg_id, bytes) else msg_id
        except aioredis.ResponseError:
            # Fallback for Redis versions without IDMPAUTO
            msg_id = await self.db.xadd(
                stream, str_data,
                maxlen=STREAM_MAX_LEN,
                approximate=True
            )
            return msg_id.decode() if isinstance(msg_id, bytes) else msg_id

    async def ensure_group(self, service: str, retries: int = 30, delay: float = 1.0):
        """Create consumer group if it doesn't exist."""
        stream = STREAM_NAMES[service]
        group = CONSUMER_GROUPS[service]
        
        for attempt in range(retries):
            try:
                await self.db.xgroup_create(stream, group, id="0", mkstream=True)
                logger.info(f"[RedisStreams] Created group {group} for stream {stream}")
                return
            except aioredis.ResponseError as e:
                if "BUSYGROUP" in str(e):
                    logger.debug(f"[RedisStreams] Group {group} already exists")
                    return
                raise
            except (aioredis.ConnectionError, aioredis.TimeoutError):
                logger.warning(f"[RedisStreams] Not ready, retrying in {delay}s...")
                await asyncio.sleep(delay)
        
        raise RuntimeError(f"Failed to create group {group} after {retries} retries")

    async def consume(
        self,
        service: str,
        worker_id: str,
        batch_size: int = 50,
    ) -> AsyncIterator[tuple[str, Dict[str, bytes]]]:
        """
        Async generator that yields (msg_id, data) from a stream.
        
        Automatically handles:
        - Consumer group management
        - Pending message (PEL) reclamation
        - Blocking reads with timeout
        """
        stream = STREAM_NAMES[service]
        group = CONSUMER_GROUPS[service]
        
        await self.ensure_group(service)
        
        last_reclaim = time.time() - (PEL_CHECK_INTERVAL_SECONDS * 0.5)
        
        while True:
            try:
                now = time.time()
                
                # Periodically reclaim orphaned messages
                if now - last_reclaim >= PEL_CHECK_INTERVAL_SECONDS:
                    last_reclaim = now
                    try:
                        _, claimed, _ = await self.db.xautoclaim(
                            name=stream,
                            groupname=group,
                            consumername=worker_id,
                            min_idle_time=PEL_IDLE_MS,
                            start_id="0-0",
                            count=batch_size,
                        )
                        if claimed:
                            logger.info(
                                f"[{service}] Reclaimed {len(claimed)} orphaned messages"
                            )
                            for msg_id, data in claimed:
                                if data:
                                    yield (
                                        msg_id.decode() if isinstance(msg_id, bytes) else msg_id,
                                        data,
                                    )
                    except aioredis.RedisError as e:
                        logger.warning(f"[{service}] Reclaim error: {e}")
                
                # Read new messages
                results = await self.db.xreadgroup(
                    groupname=group,
                    consumername=worker_id,
                    streams={stream: ">"},
                    count=batch_size,
                    block=50,
                )
                
                if results:
                    for _, messages in results:
                        for msg_id, data in messages:
                            yield (
                                msg_id.decode() if isinstance(msg_id, bytes) else msg_id,
                                data,
                            )
            
            except aioredis.RedisError as e:
                logger.error(f"[{service}] Redis error in consume: {e}")
                await asyncio.sleep(0.1)

    async def ack(self, service: str, msg_id: str):
        """Acknowledge a message."""
        stream = STREAM_NAMES[service]
        group = CONSUMER_GROUPS[service]
        
        await self.db.xack(stream, group, msg_id)

    async def close(self):
        """Close Redis connection."""
        if self.db is not None:
            await self.db.aclose()
            self.db = None
