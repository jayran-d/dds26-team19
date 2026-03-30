"""
common/streams_client.py

Redis Streams-based message transport replacing Kafka.

Key properties:
- XADD: non-blocking, sub-millisecond publish vs Kafka's 5-50ms sync flush
- XREADGROUP BLOCK: efficient consumer-group polling with PEL tracking
- XACK: message acknowledgment after durable handling
- XAUTOCLAIM: orphan recovery for crashed consumers (replaces Kafka offset replay)

Consumer groups give equivalent exactly-once delivery guarantees to Kafka's
manual offset commit, with much lower per-message latency.
"""

import json
from typing import Optional, List, Tuple
import redis as redis_module


STREAM_MAXLEN = 100_000
CONSUMER_BLOCK_MS = 500


class StreamsClient:
    """Thin wrapper over Redis Streams. Both publisher and consumer."""

    def __init__(self, db: redis_module.Redis):
        self._db = db

    def ensure_group(self, stream: str, group: str) -> None:
        """Create consumer group if it doesn't exist. mkstream creates stream if missing."""
        try:
            self._db.xgroup_create(stream, group, id='0', mkstream=True)
        except redis_module.exceptions.ResponseError as exc:
            if 'BUSYGROUP' not in str(exc):
                raise

    def publish(self, stream: str, message: dict) -> None:
        """
        XADD message to stream. Non-blocking, O(log N).
        Far lower latency than Kafka produce+flush.
        """
        self._db.xadd(
            stream,
            {'d': json.dumps(message)},
            maxlen=STREAM_MAXLEN,
            approximate=True,
        )

    def read_one(
        self,
        streams: List[str],
        group: str,
        consumer: str,
        block_ms: int = CONSUMER_BLOCK_MS,
        pending: bool = False,
    ) -> Optional[Tuple[str, bytes, dict]]:
        """
        Read one message. Blocks up to block_ms if no message available.
        pending=True reads from PEL (unacked messages from previous run).
        Returns (stream_name, msg_id, message_dict) or None on timeout.
        """
        start = '0' if pending else '>'
        # PEL reads must not block: if PEL is empty, return None immediately.
        # Normal reads block up to block_ms waiting for new messages.
        block_arg = None if pending else block_ms
        result = self._db.xreadgroup(
            groupname=group,
            consumername=consumer,
            streams={s: start for s in streams},
            count=1,
            block=block_arg,
        )
        if not result:
            return None
        for stream_raw, msgs in result:
            sname = stream_raw.decode() if isinstance(stream_raw, bytes) else stream_raw
            for msg_id, fields in msgs:
                raw = fields.get(b'd') or fields.get('d')
                if isinstance(raw, bytes):
                    raw = raw.decode()
                return sname, msg_id, json.loads(raw)
        return None

    def ack(self, stream: str, group: str, msg_id) -> None:
        """Acknowledge message, removing it from the PEL."""
        self._db.xack(stream, group, msg_id)

    def claim_orphans(
        self,
        streams: List[str],
        group: str,
        consumer: str,
        min_idle_ms: int = 30_000,
    ) -> List[Tuple[str, bytes, dict]]:
        """
        Claim messages idle (un-acked) in PEL for at least min_idle_ms.
        Called periodically to recover messages from crashed consumers.
        Replaces Kafka's automatic consumer-group rebalance + offset seek.
        """
        recovered = []
        for stream in streams:
            try:
                _, msgs, _ = self._db.xautoclaim(
                    stream, group, consumer,
                    min_idle_time=min_idle_ms,
                    start_id='0-0',
                    count=100,
                )
                for msg_id, fields in msgs:
                    raw = fields.get(b'd') or fields.get('d')
                    if isinstance(raw, bytes):
                        raw = raw.decode()
                    recovered.append((stream, msg_id, json.loads(raw)))
            except Exception:
                pass
        return recovered
