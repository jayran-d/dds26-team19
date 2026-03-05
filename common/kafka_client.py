import os
import time
import json
from confluent_kafka import Producer, Consumer
from confluent_kafka.admin import AdminClient, NewTopic


BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")


def _delivery_report(err, msg):
    if err is not None:
        print(f"[Kafka] Delivery failed for {msg.topic()}: {err}", flush=True)


class KafkaProducerClient:
    """
    Kafka producer wrapper.

    Blocks at __init__ until the broker is reachable (infinite retry with 2s sleep).
    Optionally pre-creates topics via AdminClient so consumers never see
    UNKNOWN_TOPIC_OR_PART on first subscribe.

    NOTE: Producer({'bootstrap.servers': ...}) never throws — the actual TCP
    connection happens lazily in librdkafka background threads.  list_topics()
    is the first call that actually probes the broker and throws on failure,
    which is what makes the retry loop work correctly.
    """

    def __init__(self, ensure_topics: list[str] | None = None):
        self._producer = None
        self._ensure_topics = ensure_topics or []
        self._connect()

    def _connect(self):
        while True:
            try:
                p = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS, "acks": "all"})
                # Actually probe the broker — list_topics() blocks until connected or timeout.
                p.list_topics(timeout=3)
                if self._ensure_topics:
                    admin = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})
                    new_topics = [
                        NewTopic(t, num_partitions=1, replication_factor=1)
                        for t in self._ensure_topics
                    ]
                    for f in admin.create_topics(new_topics).values():
                        try:
                            f.result()
                        except Exception:
                            pass  # topic already exists
                self._producer = p
                print(f"[KafkaProducer] Connected to {BOOTSTRAP_SERVERS}", flush=True)
                break
            except Exception as e:
                print(f"[KafkaProducer] Connection failed: {e} — retrying in 2s", flush=True)
                time.sleep(2)

    def send(self, topic: str, message: dict, key: str | None = None):
        """Produce a JSON message. Non-blocking — call flush() for delivery guarantee."""
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


class KafkaConsumerClient:
    """
    Kafka consumer wrapper.

    Blocks at __init__ until the broker is reachable (infinite retry with 2s sleep).
    Pre-creates topic(s) via AdminClient before subscribing to avoid
    UNKNOWN_TOPIC_OR_PART errors on the first poll.

    NOTE: Consumer({'bootstrap.servers': ...}) and subscribe() never throw even
    if the broker is unreachable — they succeed silently and only fail later at
    poll() time.  We therefore probe with a temporary Producer.list_topics()
    call which does throw, making the retry loop actually trigger.
    """

    def __init__(
        self,
        topic: str,
        group_id: str,
        auto_commit: bool = True,
        ensure_topics: list[str] | None = None,
    ):
        self.topic = topic
        self.group_id = group_id
        self._auto_commit = auto_commit
        # Default: ensure the subscribed topic itself exists.
        self._ensure_topics = ensure_topics if ensure_topics is not None else [topic]
        self._consumer = None
        self._connect()

    def _connect(self):
        while True:
            try:
                # Consumer() never throws, so probe with a temp Producer instead.
                probe = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})
                probe.list_topics(timeout=3)
                # Ensure topics exist before subscribing.
                if self._ensure_topics:
                    admin = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})
                    new_topics = [
                        NewTopic(t, num_partitions=1, replication_factor=1)
                        for t in self._ensure_topics
                    ]
                    for f in admin.create_topics(new_topics).values():
                        try:
                            f.result()
                        except Exception:
                            pass  # topic already exists
                consumer = Consumer({
                    "bootstrap.servers": BOOTSTRAP_SERVERS,
                    "group.id": self.group_id,
                    "auto.offset.reset": "earliest",
                    "enable.auto.commit": "true" if self._auto_commit else "false",
                })
                consumer.subscribe([self.topic])
                self._consumer = consumer
                print(
                    f"[KafkaConsumer] Subscribed to {self.topic} (group={self.group_id})",
                    flush=True,
                )
                break
            except Exception as e:
                print(f"[KafkaConsumer] Connection failed: {e} — retrying in 2s", flush=True)
                time.sleep(2)

    def poll(self, timeout: float = 1.0) -> dict | None:
        """Poll for the next message. Returns decoded JSON dict or None."""
        msg = self._consumer.poll(timeout)
        if msg is None:
            return None
        if msg.error():
            print(f"[KafkaConsumer] Error on {self.topic}: {msg.error()}", flush=True)
            return None
        try:
            return json.loads(msg.value().decode("utf-8"))
        except Exception as e:
            print(f"[KafkaConsumer] JSON decode error: {e}", flush=True)
            return None

    def close(self):
        self._consumer.close()
