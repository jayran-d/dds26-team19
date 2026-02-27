import logging
import os
import atexit
import uuid
import json
import threading
import time

import redis
from kafka import KafkaConsumer, KafkaProducer

from msgspec import msgpack, Struct
from flask import Flask, jsonify, abort, Response

DB_ERROR_STR = "DB error"
USE_KAFKA = os.getenv("USE_KAFKA", "false").lower() == "true"
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
PAYMENT_COMMAND_TOPIC = "payment.commands"


app = Flask("payment-service")

db: redis.Redis = redis.Redis(host=os.environ['REDIS_HOST'],
                              port=int(os.environ['REDIS_PORT']),
                              password=os.environ['REDIS_PASSWORD'],
                              db=int(os.environ['REDIS_DB']))
kafka_producer: KafkaProducer | None = None
kafka_consumer: KafkaConsumer | None = None


def close_connections():
    db.close()
    if kafka_consumer is not None:
        kafka_consumer.close()
    if kafka_producer is not None:
        kafka_producer.close()


atexit.register(close_connections)


class UserValue(Struct):
    credit: int


def get_user_from_db(user_id: str) -> UserValue | None:
    try:
        # get serialized data
        entry: bytes = db.get(user_id)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    # deserialize data if it exists else return null
    entry: UserValue | None = msgpack.decode(entry, type=UserValue) if entry else None
    if entry is None:
        # if user does not exist in the database; abort
        abort(400, f"User: {user_id} not found!")
    return entry


def create_kafka_clients():
    global kafka_producer
    global kafka_consumer
    if not USE_KAFKA:
        return
    for _ in range(20):
        try:
            kafka_producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda value: json.dumps(value).encode("utf-8"),
                key_serializer=lambda key: key.encode("utf-8") if key else None
            )
            kafka_consumer = KafkaConsumer(
                PAYMENT_COMMAND_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                group_id="payment-service",
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                value_deserializer=lambda value: json.loads(value.decode("utf-8"))
            )
            app.logger.info("Kafka payment consumer connected")
            return
        except Exception:
            time.sleep(1)
    app.logger.error("Kafka payment consumer could not connect after retries")


def remove_credit_internal(user_id: str, amount: int) -> tuple[bool, str | None, int | None]:
    try:
        user_entry: UserValue = get_user_from_db(user_id)
    except Exception as exc:
        return False, getattr(exc, "description", str(exc)), None
    user_entry.credit -= amount
    if user_entry.credit < 0:
        return False, f"User: {user_id} credit cannot get reduced below zero!", None
    try:
        db.set(user_id, msgpack.encode(user_entry))
    except redis.exceptions.RedisError:
        return False, DB_ERROR_STR, None
    return True, None, user_entry.credit


def publish_reply(reply_topic: str, payload: dict):
    if kafka_producer is None:
        return
    kafka_producer.send(reply_topic, payload, key=str(payload.get("correlation_id", ""))).get(timeout=5)


def process_payment_command(command: dict):
    action = command.get("action")
    correlation_id = command.get("correlation_id", "")
    reply_topic = command.get("reply_to", "order.replies")
    user_id = str(command.get("user_id", ""))
    amount_raw = command.get("amount")
    try:
        amount = int(amount_raw)
    except (TypeError, ValueError):
        amount = None
    response = {
        "correlation_id": correlation_id,
        "service": "payment",
        "action": action
    }
    if amount is None or not user_id:
        response["status"] = "error"
        response["error"] = "Invalid payment command payload"
        publish_reply(reply_topic, response)
        return
    if action == "pay":
        success, error, credit = remove_credit_internal(user_id, amount)
    else:
        success, error, credit = False, f"Unknown payment action: {action}", None
    response["status"] = "ok" if success else "error"
    response["user_id"] = user_id
    response["amount"] = amount
    if success:
        response["credit"] = credit
    else:
        response["error"] = error
    publish_reply(reply_topic, response)


def kafka_consumer_loop():
    if kafka_consumer is None:
        return
    while True:
        try:
            records = kafka_consumer.poll(timeout_ms=1000)
            for topic_records in records.values():
                for record in topic_records:
                    process_payment_command(record.value)
        except Exception as exc:
            app.logger.error(f"Kafka payment command loop error: {exc}")
            time.sleep(1)


def start_kafka_consumer_thread():
    create_kafka_clients()
    if kafka_consumer is None:
        return
    thread = threading.Thread(target=kafka_consumer_loop, daemon=True)
    thread.start()


@app.post('/create_user')
def create_user():
    key = str(uuid.uuid4())
    value = msgpack.encode(UserValue(credit=0))
    try:
        db.set(key, value)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({'user_id': key})


@app.post('/batch_init/<n>/<starting_money>')
def batch_init_users(n: int, starting_money: int):
    n = int(n)
    starting_money = int(starting_money)
    kv_pairs: dict[str, bytes] = {f"{i}": msgpack.encode(UserValue(credit=starting_money))
                                  for i in range(n)}
    try:
        db.mset(kv_pairs)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({"msg": "Batch init for users successful"})


@app.get('/find_user/<user_id>')
def find_user(user_id: str):
    user_entry: UserValue = get_user_from_db(user_id)
    return jsonify(
        {
            "user_id": user_id,
            "credit": user_entry.credit
        }
    )


@app.post('/add_funds/<user_id>/<amount>')
def add_credit(user_id: str, amount: int):
    user_entry: UserValue = get_user_from_db(user_id)
    # update credit, serialize and update database
    user_entry.credit += int(amount)
    try:
        db.set(user_id, msgpack.encode(user_entry))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(f"User: {user_id} credit updated to: {user_entry.credit}", status=200)


@app.post('/pay/<user_id>/<amount>')
def remove_credit(user_id: str, amount: int):
    app.logger.debug(f"Removing {amount} credit from user: {user_id}")
    success, error, updated_credit = remove_credit_internal(user_id, int(amount))
    if not success:
        abort(400, error)
    return Response(f"User: {user_id} credit updated to: {updated_credit}", status=200)


start_kafka_consumer_thread()


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
