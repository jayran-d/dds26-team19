"""
order-service/kafka_worker.py

Kafka plumbing + transaction mode switching for the order service.

app.py calls:
    start_checkout(order_id, order_entry)  — kicks off the transaction

Internally routes to saga or 2PC based on TRANSACTION_MODE env var.
Event loop receives stock.events + payment.events and drives the
transaction forward using the same mode switch.

This file does NOT contain Flask or HTTP logic.
"""

import os
import uuid
import threading
import time

import redis as redis_module

from transactions_modes.simple import simple_route_order, simple_start_checkout
from transactions_modes.saga.saga import (
    saga_route_order,
    saga_start_checkout,
    recover as saga_recover,
    check_timeouts as saga_check_timeouts,
)

from transactions_modes.two_pc import _2pc_route_order, _2pc_start_checkout

from common.kafka_client import KafkaProducerClient, KafkaConsumerClient
from common.messages import ALL_TOPICS, STOCK_EVENTS_TOPIC, PAYMENT_EVENTS_TOPIC

USE_KAFKA = os.getenv("USE_KAFKA", "false").lower() == "true"
TRANSACTION_MODE = os.getenv("TRANSACTION_MODE", "simple")  # "simple" | "saga" | "2pc"
TIMEOUT_SCAN_INTERVAL_SECONDS = 5
EVENT_LOOP_CONSUMERS = max(
    1,
    int(os.getenv("ORDER_KAFKA_EVENT_CONSUMERS", os.getenv("KAFKA_NUM_PARTITIONS", "4"))),
)
COORDINATOR_KEY = os.getenv(
    "ORDER_KAFKA_COORDINATOR_KEY",
    "order:kafka:coordinator:leader",
)
COORDINATOR_LEASE_SECONDS = max(
    TIMEOUT_SCAN_INTERVAL_SECONDS * 2,
    int(os.getenv("ORDER_KAFKA_COORDINATOR_LEASE_SECONDS", "15")),
)
COORDINATOR_RETRY_INTERVAL_SECONDS = float(
    os.getenv("ORDER_KAFKA_COORDINATOR_RETRY_INTERVAL_SECONDS", "1.0")
)

# ── Module-level state ─────────────────────────────────────────────────────────
_producer: KafkaProducerClient | None = None
_consumers: list[KafkaConsumerClient] = []
_db: redis_module.Redis | None = None
_available: bool = False
_logger = None
_instance_id = f"{os.getpid()}:{uuid.uuid4()}"



# ============================================================
# INIT / TEARDOWN
# ============================================================


def init_kafka(logger, db: redis_module.Redis) -> None:
    """
    Initialise producer, consumer, and start the event loop thread.
    Called once at gunicorn startup. Does nothing if USE_KAFKA=false.
    """
    global _producer, _consumers, _db, _available, _logger

    if not USE_KAFKA:
        return

    _logger = logger
    _db = db

    _producer = KafkaProducerClient(ensure_topics=ALL_TOPICS)
    _consumers = []
    for consumer_index in range(EVENT_LOOP_CONSUMERS):
        _consumers.append(
            KafkaConsumerClient(
                topics=[STOCK_EVENTS_TOPIC, PAYMENT_EVENTS_TOPIC],
                group_id="order-service",
                # The order service also needs manual commits.
                # It must not ack an event before the Saga record is durably advanced.
                auto_commit=False,
                auto_offset_reset="earliest",
                ensure_topics=ALL_TOPICS if consumer_index == 0 else [],
            )
        )

    _available = True

    if TRANSACTION_MODE == "saga":
        coordinator_thread = threading.Thread(target=_coordinator_loop, daemon=True)
        coordinator_thread.start()

    for consumer in _consumers:
        event_thread = threading.Thread(target=_event_loop, args=(consumer,), daemon=True)
        event_thread.start()

    logger.info(
        f"[OrderKafka] Event loops started (mode={TRANSACTION_MODE}, consumers={len(_consumers)})"
    )


def close_kafka() -> None:
    global _available
    _available = False
    _release_coordinator_role()
    for consumer in _consumers:
        consumer.close()
    if _producer is not None:
        _producer.close()


def is_available() -> bool:
    return _available


# ============================================================
# PUBLIC API  (called by app.py)
# ============================================================


def start_checkout(order_id: str, order_entry):
    """
    Kick off a checkout transaction.

    Returns a small result dict so the HTTP route can distinguish:
    - new checkout started
    - checkout already in progress
    - error
    """
    if not _available or _producer is None:
        raise RuntimeError("Kafka is not available")

    if TRANSACTION_MODE == "simple":
        simple_start_checkout(_producer, _db, _logger, order_id, order_entry)
        return {"started": True, "reason": "started"}

    if TRANSACTION_MODE == "saga":
        return saga_start_checkout(_producer.publish, _db, _logger, order_id, order_entry)

    if TRANSACTION_MODE == "2pc":
        _2pc_start_checkout(_producer, _db, _logger, order_id, order_entry)
        return {"started": True, "reason": "started"}

    raise RuntimeError(f"Unknown TRANSACTION_MODE: {TRANSACTION_MODE}")


# ============================================================
# EVENT LOOP
# ============================================================


def _event_loop(consumer: KafkaConsumerClient) -> None:
    while True:
        try:
            result = consumer.poll(timeout=1.0)

            if result.error:
                _logger.error(f"[OrderKafka] Poll error: {result.error}")
                time.sleep(1)
                continue

            if not result.ok:
                continue

            _route_event(result.msg)

            # Commit only after Saga event handling returned successfully.
            # By this point the order service should already have:
            # 1. durably updated the Saga record,
            # 2. published the next command if needed,
            # 3. marked the incoming event as seen.
            consumer.commit()

        except Exception as exc:
            # No commit here.
            # If we fail before the Saga state is durably advanced, Kafka should
            # redeliver the event and let the orchestrator try again.
            _logger.error(f"[OrderKafka] Event loop crashed: {exc}")
            time.sleep(1)

def _coordinator_loop() -> None:
    was_leader = False

    while _available and TRANSACTION_MODE == "saga":
        try:
            is_leader = _claim_or_renew_coordinator_role()

            if not is_leader:
                if was_leader:
                    _logger.info("[OrderKafka] Lost coordinator role")
                was_leader = False
                time.sleep(COORDINATOR_RETRY_INTERVAL_SECONDS)
                continue

            if not was_leader:
                _logger.info("[OrderKafka] Acquired coordinator role")
                # Recover only on coordinator ownership so multi-worker startup
                # does not replay the same in-flight Sagas from every process.
                saga_recover(_db, _producer.publish, _logger)

            was_leader = True

            # Timeouts are handled centrally by the coordinator. Participants
            # stay simpler: they only need durable local handling + replay.
            saga_check_timeouts(_db, _producer.publish, _logger)
        except Exception as exc:
            _logger.error(f"[OrderKafka] Coordinator loop crashed: {exc}")
        time.sleep(TIMEOUT_SCAN_INTERVAL_SECONDS)


def _claim_or_renew_coordinator_role() -> bool:
    if _db is None:
        return False

    try:
        acquired = _db.set(
            COORDINATOR_KEY,
            _instance_id,
            nx=True,
            ex=COORDINATOR_LEASE_SECONDS,
        )
        if acquired:
            return True
    except redis_module.exceptions.RedisError:
        return False

    while True:
        pipe = _db.pipeline()
        try:
            pipe.watch(COORDINATOR_KEY)
            raw_leader = pipe.get(COORDINATOR_KEY)
            if not raw_leader:
                pipe.unwatch()
                return False

            leader_id = raw_leader.decode()
            if leader_id != _instance_id:
                pipe.unwatch()
                return False

            pipe.multi()
            pipe.set(
                COORDINATOR_KEY,
                _instance_id,
                ex=COORDINATOR_LEASE_SECONDS,
            )
            pipe.execute()
            return True
        except redis_module.exceptions.WatchError:
            continue
        except redis_module.exceptions.RedisError:
            return False
        finally:
            pipe.reset()


def _release_coordinator_role() -> None:
    if _db is None:
        return

    while True:
        pipe = _db.pipeline()
        try:
            pipe.watch(COORDINATOR_KEY)
            raw_leader = pipe.get(COORDINATOR_KEY)
            if not raw_leader:
                pipe.unwatch()
                return

            if raw_leader.decode() != _instance_id:
                pipe.unwatch()
                return

            pipe.multi()
            pipe.delete(COORDINATOR_KEY)
            pipe.execute()
            return
        except redis_module.exceptions.WatchError:
            continue
        except redis_module.exceptions.RedisError:
            return
        finally:
            pipe.reset()


def _route_event(msg: dict) -> None:
    msg_type = msg.get("type")

    _logger.info(
        f"[OrderKafka] event={msg_type} "
        f"order={msg.get('order_id')} tx={msg.get('tx_id')}"
    )

    if TRANSACTION_MODE == "simple":
        simple_route_order(_producer, _db, _logger, msg, msg_type)
    elif TRANSACTION_MODE == "saga":
        saga_route_order(msg, _db, _producer.publish, _logger)
    elif TRANSACTION_MODE == "2pc":
        _2pc_route_order(msg)
