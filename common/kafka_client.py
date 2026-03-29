import os
import time
import json
import threading
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
        
class KafkaPublishError(RuntimeError):
    """Raised when a Kafka publish did not complete successfully."""

class KafkaProducerClient:
    """
    Thin producer wrapper. Blocks at init until the broker is reachable.
    Optionally pre-creates topics so consumers never hit UNKNOWN_TOPIC_OR_PART.

    Important distinction:
    - send(...)    = low-level async enqueue
    - publish(...) = synchronous "wait until delivery success/failure is known"

    Saga code should use publish(...), because correctness depends on knowing
    whether the command/event really left the service or not.
    """

    def __init__(self, ensure_topics: list[str] | None = None):
        self._producer = None
        self._ensure_topics = ensure_topics or []
        self._poll_thread = None
        self._running = False
        self._connect()

    def _connect(self):
        while True:
            try:
                p = Producer(
                    {
                        "bootstrap.servers": BOOTSTRAP_SERVERS,
                        "acks": "all",
                        "metadata.request.timeout.ms": "5000",
                        "linger.ms": "5",
                        "batch.num.messages": "10000",
                        "compression.type": "lz4",
                        "queue.buffering.max.messages": "200000",
                    }
                )
                p.list_topics(timeout=3)  # actually probe the broker
                if self._ensure_topics:
                    _ensure_topics_exist(self._ensure_topics)
                self._producer = p
                self._running = True
                self._poll_thread = threading.Thread(target=self._poll_loop, daemon=True)
                self._poll_thread.start()
                print(f"[KafkaProducer] Connected to {BOOTSTRAP_SERVERS}", flush=True)
                break
            except Exception as e:
                print(
                    f"[KafkaProducer] Connection failed: {e} — retrying in 2s",
                    flush=True,
                )
                time.sleep(2)

    def _poll_loop(self):
        while self._running and self._producer is not None:
            try:
                self._producer.poll(0.1)
            except Exception as exc:
                print(f"[KafkaProducer] Poll loop error: {exc}", flush=True)
                time.sleep(0.1)

    def _default_key(self, message: dict, key: str | None = None) -> str | None:
        if key:
            return key
        return message.get("tx_id") or message.get("order_id")

    def send(self, topic: str, message: dict, key: str | None = None):
        """
        Low-level async produce.
        This only queues the message locally in librdkafka and does NOT guarantee
        broker delivery yet. Keep this as a utility, but Saga paths should prefer publish().
        """
        resolved_key = self._default_key(message, key)
        self._producer.produce(
            topic=topic,
            value=json.dumps(message).encode("utf-8"),
            key=resolved_key.encode("utf-8") if resolved_key else None,
            on_delivery=_delivery_report,
        )
        self._producer.poll(0)

    def flush(self, timeout: float = 5.0) -> int:
        """
        Flush pending producer work.

        Returns:
            number of messages still waiting after the timeout.
            0 means everything currently queued got processed.
        """
        return self._producer.flush(timeout)

    def publish(
        self,
        topic: str,
        message: dict,
        key: str | None = None,
        timeout: float = 5.0,
    ) -> None:
        """
        Synchronous publish used by the Saga code.

        Why the callback + flush pattern exists:
        - confluent-kafka is asynchronous internally
        - produce(...) only queues the message locally
        - the delivery callback runs later when poll()/flush() drives the client
        - callback exceptions do not naturally bubble into this function call

        So we store the callback outcome in a small mutable holder, then flush,
        then inspect that holder and raise if delivery failed or timed out.
        """
        delivery = {
            "done": False,
            "error": None,
        }

        delivery_event = threading.Event()

        def on_delivery(err, msg):
            delivery["done"] = True
            delivery["error"] = err
            delivery_event.set()

        resolved_key = self._default_key(message, key)

        try:
            self._producer.produce(
                topic=topic,
                value=json.dumps(message).encode("utf-8"),
                key=resolved_key.encode("utf-8") if resolved_key else None,
                on_delivery=on_delivery,
            )
        except Exception as exc:
            raise KafkaPublishError(
                f"Failed to enqueue Kafka message for topic={topic}: {exc}"
            ) from exc

        self._producer.poll(0)

        if not delivery_event.wait(timeout):
            raise KafkaPublishError(
                f"Timed out waiting for Kafka delivery on topic={topic}"
            )

        if delivery["error"] is not None:
            raise KafkaPublishError(
                f"Kafka delivery failed for topic={topic}: {delivery['error']}"
            )

        if not delivery["done"]:
            raise KafkaPublishError(
                f"Kafka publish finished without a delivery callback for topic={topic}"
            )

    def close(self):
        self._running = False
        if self._poll_thread is not None:
            self._poll_thread.join(timeout=1.0)
        if self._producer is not None:
            self._producer.flush(5.0)
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
