import os
import time
import json
from confluent_kafka import Producer, Consumer, KafkaError
from confluent_kafka.admin import AdminClient, NewTopic


BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
NUM_PARTITIONS = int(os.getenv("KAFKA_NUM_PARTITIONS", "4"))
REPLICATION_FACTOR = int(os.getenv("KAFKA_REPLICATION_FACTOR", "1"))


def _delivery_report(err, msg):
    if err is not None:
        print(f"[Kafka] Delivery failed for {msg.topic()}: {err}", flush=True)


def _ensure_topics_exist(topics: list[str]):
    """Create topics if they don't already exist. Shared by producer and consumer init."""
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})
    new_topics = [
        NewTopic(
            t, num_partitions=NUM_PARTITIONS, replication_factor=REPLICATION_FACTOR
        )
        for t in topics
    ]
    for f in admin.create_topics(new_topics).values():
        try:
            f.result()
        except Exception:
            pass  # topic already exists — fine


class KafkaProducerClient:
    """
    Thin producer wrapper. Blocks at init until the broker is reachable.
    Optionally pre-creates topics so consumers never hit UNKNOWN_TOPIC_OR_PART.
    """

    def __init__(self, ensure_topics: list[str] | None = None):
        self._producer = None
        self._ensure_topics = ensure_topics or []
        self._connect()

    def _connect(self):
        while True:
            try:
                p = Producer(
                    {
                        "bootstrap.servers": BOOTSTRAP_SERVERS,
                        "acks": "all",
                        "metadata.request.timeout.ms": "5000",
                    }
                )
                p.list_topics(timeout=3)  # actually probe the broker
                if self._ensure_topics:
                    _ensure_topics_exist(self._ensure_topics)
                self._producer = p
                print(f"[KafkaProducer] Connected to {BOOTSTRAP_SERVERS}", flush=True)
                break
            except Exception as e:
                print(
                    f"[KafkaProducer] Connection failed: {e} — retrying in 2s",
                    flush=True,
                )
                time.sleep(2)

    def send(self, topic: str, message: dict, key: str | None = None):
        """Produce a JSON message. Kafka decides the partition automatically."""
        self._producer.produce(
            topic=topic,
            value=json.dumps(message).encode("utf-8"),
            key=key.encode("utf-8") if key else None,
            on_delivery=_delivery_report,
        )
        self._producer.poll(0)

    def flush(self, timeout: float = 5.0):
        self._producer.flush(timeout)

    def close(self):
        self._producer.flush()
        self._producer = None


class PollResult:
    """
    Wraps a single poll() outcome. Three possible states:
      - No message yet  → ok=False, error=None
      - Good message    → ok=True,  msg=dict
      - Error           → ok=False, error=str
    """

    __slots__ = ("msg", "error")

    def __init__(self, msg: dict | None = None, error: str | None = None):
        self.msg = msg
        self.error = error

    @property
    def ok(self) -> bool:
        return self.msg is not None

    def __repr__(self):
        if self.error:
            return f"PollResult(error={self.error!r})"
        return f"PollResult(msg={self.msg!r})"


class KafkaConsumerClient:
    """
    Thin consumer wrapper. Blocks at init until the broker is reachable.
    Pre-creates topics before subscribing to avoid UNKNOWN_TOPIC_OR_PART.

    Supports subscribing to multiple topics via the topics parameter.
    Kafka handles partition assignment automatically via the consumer group.
    """

    def __init__(
        self,
        topics: list[str],
        group_id: str,
        auto_commit: bool = True,
        ensure_topics: list[str] | None = None,
        auto_offset_reset: str = "earliest",
    ):
        self.topics = topics
        self.group_id = group_id
        self._auto_commit = auto_commit
        self._ensure_topics = ensure_topics if ensure_topics is not None else topics
        self._auto_offset_reset = auto_offset_reset
        self._consumer = None
        self._connect()

    def _connect(self):
        while True:
            try:
                # Consumer() never throws — probe with a temp Producer instead.
                probe = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})
                probe.list_topics(timeout=3)
                if self._ensure_topics:
                    _ensure_topics_exist(self._ensure_topics)
                consumer = Consumer(
                    {
                        "bootstrap.servers": BOOTSTRAP_SERVERS,
                        "group.id": self.group_id,
                        "auto.offset.reset": self._auto_offset_reset,
                        "enable.auto.commit": "true" if self._auto_commit else "false",
                        "session.timeout.ms": "30000",
                        "max.poll.interval.ms": "300000",
                    }
                )
                consumer.subscribe(self.topics)
                print(
                    f"[KafkaConsumer] Subscribed to {self.topics} (group={self.group_id})",
                    flush=True,
                )
                self._consumer = consumer
                break
            except Exception as e:
                print(
                    f"[KafkaConsumer] Connection failed: {e} — retrying in 2s",
                    flush=True,
                )
                time.sleep(2)

    def poll(self, timeout: float = 1.0) -> PollResult:
        msg = self._consumer.poll(timeout)
        if msg is None:
            return PollResult()
        if msg.error():
            code = msg.error().code()
            # EOF and retriable errors (e.g. NOT_COORDINATOR) are non-fatal — ignore silently.
            if code == KafkaError._PARTITION_EOF or msg.error().retriable():
                return PollResult()
            err_str = str(msg.error())
            print(f"[KafkaConsumer] Error: {err_str}", flush=True)
            return PollResult(error=err_str)
        try:
            return PollResult(msg=json.loads(msg.value().decode("utf-8")))
        except Exception as e:
            err_str = f"JSON decode error: {e}"
            print(f"[KafkaConsumer] {err_str}", flush=True)
            return PollResult(error=err_str)

    def commit(self):
        """Manually commit offset. Only meaningful when auto_commit=False."""
        self._consumer.commit(asynchronous=False)

    def close(self):
        self._consumer.close()
        self._consumer = None
