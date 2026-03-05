import logging
import os
import atexit
import uuid
import json
import threading
import time

import redis
from confluent_kafka import Producer, Consumer

from msgspec import msgpack, Struct
from flask import Flask, jsonify, abort, Response


DB_ERROR_STR = "DB error"
USE_KAFKA = os.getenv("USE_KAFKA", "false").lower() == "true"
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
STOCK_COMMAND_TOPIC = "stock.commands"

app = Flask("stock-service")

db: redis.Redis = redis.Redis(host=os.environ['REDIS_HOST'],
                              port=int(os.environ['REDIS_PORT']),
                              password=os.environ['REDIS_PASSWORD'],
                              db=int(os.environ['REDIS_DB']))
kafka_producer: Producer | None = None
kafka_consumer: Consumer | None = None


def close_connections():
    db.close()
    if kafka_consumer is not None:
        kafka_consumer.close()
    if kafka_producer is not None:
        kafka_producer.flush()


atexit.register(close_connections)


class StockValue(Struct):
    stock: int
    price: int


def get_item_from_db(item_id: str) -> StockValue | None:
    # get serialized data
    try:
        entry: bytes = db.get(item_id)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    # deserialize data if it exists else return null
    entry: StockValue | None = msgpack.decode(entry, type=StockValue) if entry else None
    if entry is None:
        # if item does not exist in the database; abort
        abort(400, f"Item: {item_id} not found!")
    return entry


def create_kafka_clients():
    global kafka_producer
    global kafka_consumer
    if not USE_KAFKA:
        return
    for _ in range(20):
        try:
            kafka_producer = Producer({
                'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
            })
            kafka_consumer = Consumer({
                'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
                'group.id': 'stock-service',
                'auto.offset.reset': 'earliest',
                'enable.auto.commit': 'true',
            })
            kafka_consumer.subscribe([STOCK_COMMAND_TOPIC])
            app.logger.info("Kafka stock consumer connected")
            return
        except Exception:
            time.sleep(1)
    app.logger.error("Kafka stock consumer could not connect after retries")


def apply_stock_delta(item_id: str, delta: int) -> tuple[bool, str | None, int | None]:
    try:
        item_entry: StockValue = get_item_from_db(item_id)
    except Exception as exc:
        return False, getattr(exc, "description", str(exc)), None
    item_entry.stock += delta
    if item_entry.stock < 0:
        return False, f"Item: {item_id} stock cannot get reduced below zero!", None
    try:
        db.set(item_id, msgpack.encode(item_entry))
    except redis.exceptions.RedisError:
        return False, DB_ERROR_STR, None
    return True, None, item_entry.stock


def publish_reply(reply_topic: str, payload: dict):
    if kafka_producer is None:
        return
    kafka_producer.produce(
        reply_topic,
        value=json.dumps(payload).encode("utf-8"),
        key=str(payload.get("correlation_id", "")).encode("utf-8")
    )
    kafka_producer.flush(timeout=5)


def process_stock_command(command: dict):
    action = command.get("action")
    correlation_id = command.get("correlation_id", "")
    reply_topic = command.get("reply_to", "order.replies")
    item_id = str(command.get("item_id", ""))
    amount_raw = command.get("amount")
    try:
        amount = int(amount_raw)
    except (TypeError, ValueError):
        amount = None
    response = {
        "correlation_id": correlation_id,
        "service": "stock",
        "action": action
    }
    if amount is None or not item_id:
        response["status"] = "error"
        response["error"] = "Invalid stock command payload"
        publish_reply(reply_topic, response)
        return
    if action == "subtract_stock":
        success, error, stock = apply_stock_delta(item_id, -amount)
    elif action == "add_stock":
        success, error, stock = apply_stock_delta(item_id, amount)
    else:
        success, error, stock = False, f"Unknown stock action: {action}", None
    response["status"] = "ok" if success else "error"
    response["item_id"] = item_id
    response["amount"] = amount
    if success:
        response["stock"] = stock
    else:
        response["error"] = error
    publish_reply(reply_topic, response)


def kafka_consumer_loop():
    if kafka_consumer is None:
        return
    while True:
        try:
            msg = kafka_consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                app.logger.error(f"Kafka consumer error: {msg.error()}")
                continue
            process_stock_command(json.loads(msg.value().decode("utf-8")))
        except Exception as exc:
            app.logger.error(f"Kafka stock command loop error: {exc}")
            time.sleep(1)


def start_kafka_consumer_thread():
    create_kafka_clients()
    if kafka_consumer is None:
        return
    thread = threading.Thread(target=kafka_consumer_loop, daemon=True)
    thread.start()


@app.post('/item/create/<price>')
def create_item(price: int):
    key = str(uuid.uuid4())
    app.logger.debug(f"Item: {key} created")
    value = msgpack.encode(StockValue(stock=0, price=int(price)))
    try:
        db.set(key, value)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({'item_id': key})


@app.post('/batch_init/<n>/<starting_stock>/<item_price>')
def batch_init_users(n: int, starting_stock: int, item_price: int):
    n = int(n)
    starting_stock = int(starting_stock)
    item_price = int(item_price)
    kv_pairs: dict[str, bytes] = {f"{i}": msgpack.encode(StockValue(stock=starting_stock, price=item_price))
                                  for i in range(n)}
    try:
        db.mset(kv_pairs)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({"msg": "Batch init for stock successful"})


@app.get('/find/<item_id>')
def find_item(item_id: str):
    item_entry: StockValue = get_item_from_db(item_id)
    return jsonify(
        {
            "stock": item_entry.stock,
            "price": item_entry.price
        }
    )


@app.post('/add/<item_id>/<amount>')
def add_stock(item_id: str, amount: int):
    success, error, updated_stock = apply_stock_delta(item_id, int(amount))
    if not success:
        abort(400, error)
    return Response(f"Item: {item_id} stock updated to: {updated_stock}", status=200)


@app.post('/subtract/<item_id>/<amount>')
def remove_stock(item_id: str, amount: int):
    success, error, updated_stock = apply_stock_delta(item_id, -int(amount))
    if not success:
        abort(400, error)
    app.logger.debug(f"Item: {item_id} stock updated to: {updated_stock}")
    return Response(f"Item: {item_id} stock updated to: {updated_stock}", status=200)


start_kafka_consumer_thread()


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
