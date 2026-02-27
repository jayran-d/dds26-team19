import logging
import os
import atexit
import random
import uuid
import json
import time
from collections import defaultdict

import redis
import requests
from kafka import KafkaConsumer, KafkaProducer

from msgspec import msgpack, Struct
from flask import Flask, jsonify, abort, Response


DB_ERROR_STR = "DB error"
REQ_ERROR_STR = "Requests error"

GATEWAY_URL = os.environ['GATEWAY_URL']
USE_KAFKA = os.getenv("USE_KAFKA", "false").lower() == "true"
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
ORDER_REPLY_TOPIC = f"order.replies.{os.getpid()}"
STOCK_COMMAND_TOPIC = "stock.commands"
PAYMENT_COMMAND_TOPIC = "payment.commands"

app = Flask("order-service")

db: redis.Redis = redis.Redis(host=os.environ['REDIS_HOST'],
                              port=int(os.environ['REDIS_PORT']),
                              password=os.environ['REDIS_PASSWORD'],
                              db=int(os.environ['REDIS_DB']))
kafka_producer: KafkaProducer | None = None
kafka_consumer: KafkaConsumer | None = None
kafka_available = False


def close_connections():
    db.close()
    if kafka_consumer is not None:
        kafka_consumer.close()
    if kafka_producer is not None:
        kafka_producer.close()


atexit.register(close_connections)


class OrderValue(Struct):
    paid: bool
    items: list[tuple[str, int]]
    user_id: str
    total_cost: int


def get_order_from_db(order_id: str) -> OrderValue | None:
    try:
        # get serialized data
        entry: bytes = db.get(order_id)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    # deserialize data if it exists else return null
    entry: OrderValue | None = msgpack.decode(entry, type=OrderValue) if entry else None
    if entry is None:
        # if order does not exist in the database; abort
        abort(400, f"Order: {order_id} not found!")
    return entry


def create_kafka_clients():
    global kafka_producer
    global kafka_consumer
    global kafka_available
    if not USE_KAFKA:
        return
    for _ in range(20):
        try:
            kafka_producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda value: json.dumps(value).encode("utf-8"),
                key_serializer=lambda key: key.encode("utf-8") if key else None
            )
            # group_id=None makes each worker receive all replies and filter by correlation id.
            kafka_consumer = KafkaConsumer(
                ORDER_REPLY_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                group_id=None,
                auto_offset_reset="earliest",
                enable_auto_commit=False,
                value_deserializer=lambda value: json.loads(value.decode("utf-8"))
            )
            # Force initial metadata/assignment fetch so first reply is not missed.
            kafka_consumer.poll(timeout_ms=0)
            kafka_available = True
            app.logger.info("Kafka order producer/consumer connected")
            return
        except Exception:
            time.sleep(1)
    kafka_available = False
    app.logger.error("Kafka order clients could not connect after retries")


def send_kafka_command(topic: str, payload: dict, timeout_seconds: int = 12) -> dict:
    if not kafka_available or kafka_producer is None or kafka_consumer is None:
        return {"status": "error", "error": "Kafka is not available"}
    correlation_id = str(uuid.uuid4())
    command = payload | {
        "correlation_id": correlation_id,
        "reply_to": ORDER_REPLY_TOPIC
    }
    try:
        kafka_producer.send(topic, command, key=correlation_id).get(timeout=5)
    except Exception as exc:
        return {"status": "error", "error": f"Kafka send failed: {exc}"}
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        try:
            records = kafka_consumer.poll(timeout_ms=200)
        except Exception as exc:
            return {"status": "error", "error": f"Kafka receive failed: {exc}"}
        for topic_records in records.values():
            for record in topic_records:
                response: dict = record.value
                if response.get("correlation_id") == correlation_id:
                    return response
    return {"status": "error", "error": f"Kafka response timeout for topic {topic}"}


@app.post('/create/<user_id>')
def create_order(user_id: str):
    key = str(uuid.uuid4())
    value = msgpack.encode(OrderValue(paid=False, items=[], user_id=user_id, total_cost=0))
    try:
        db.set(key, value)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({'order_id': key})


@app.post('/batch_init/<n>/<n_items>/<n_users>/<item_price>')
def batch_init_users(n: int, n_items: int, n_users: int, item_price: int):

    n = int(n)
    n_items = int(n_items)
    n_users = int(n_users)
    item_price = int(item_price)

    def generate_entry() -> OrderValue:
        user_id = random.randint(0, n_users - 1)
        item1_id = random.randint(0, n_items - 1)
        item2_id = random.randint(0, n_items - 1)
        value = OrderValue(paid=False,
                           items=[(f"{item1_id}", 1), (f"{item2_id}", 1)],
                           user_id=f"{user_id}",
                           total_cost=2*item_price)
        return value

    kv_pairs: dict[str, bytes] = {f"{i}": msgpack.encode(generate_entry())
                                  for i in range(n)}
    try:
        db.mset(kv_pairs)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({"msg": "Batch init for orders successful"})


@app.get('/find/<order_id>')
def find_order(order_id: str):
    order_entry: OrderValue = get_order_from_db(order_id)
    return jsonify(
        {
            "order_id": order_id,
            "paid": order_entry.paid,
            "items": order_entry.items,
            "user_id": order_entry.user_id,
            "total_cost": order_entry.total_cost
        }
    )


def send_post_request(url: str):
    try:
        response = requests.post(url)
    except requests.exceptions.RequestException:
        abort(400, REQ_ERROR_STR)
    else:
        return response


def send_get_request(url: str):
    try:
        response = requests.get(url)
    except requests.exceptions.RequestException:
        abort(400, REQ_ERROR_STR)
    else:
        return response


@app.post('/addItem/<order_id>/<item_id>/<quantity>')
def add_item(order_id: str, item_id: str, quantity: int):
    order_entry: OrderValue = get_order_from_db(order_id)
    item_reply = send_get_request(f"{GATEWAY_URL}/stock/find/{item_id}")
    if item_reply.status_code != 200:
        # Request failed because item does not exist
        abort(400, f"Item: {item_id} does not exist!")
    item_json: dict = item_reply.json()
    order_entry.items.append((item_id, int(quantity)))
    order_entry.total_cost += int(quantity) * item_json["price"]
    try:
        db.set(order_id, msgpack.encode(order_entry))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(f"Item: {item_id} added to: {order_id} price updated to: {order_entry.total_cost}",
                    status=200)


def rollback_stock(removed_items: list[tuple[str, int]]):
    for item_id, quantity in removed_items:
        if USE_KAFKA and kafka_available:
            rollback_reply = send_kafka_command(
                STOCK_COMMAND_TOPIC,
                {
                    "action": "add_stock",
                    "item_id": item_id,
                    "amount": quantity
                },
                timeout_seconds=5
            )
            if rollback_reply.get("status") != "ok":
                send_post_request(f"{GATEWAY_URL}/stock/add/{item_id}/{quantity}")
        else:
            send_post_request(f"{GATEWAY_URL}/stock/add/{item_id}/{quantity}")


@app.post('/checkout/<order_id>')
def checkout(order_id: str):
    app.logger.debug(f"Checking out {order_id}")
    order_entry: OrderValue = get_order_from_db(order_id)
    # get the quantity per item
    items_quantities: dict[str, int] = defaultdict(int)
    for item_id, quantity in order_entry.items:
        items_quantities[item_id] += quantity
    # The removed items will contain the items that we already have successfully subtracted stock from
    # for rollback purposes.
    removed_items: list[tuple[str, int]] = []
    for item_id, quantity in items_quantities.items():
        if USE_KAFKA and kafka_available:
            stock_reply = send_kafka_command(
                STOCK_COMMAND_TOPIC,
                {
                    "action": "subtract_stock",
                    "item_id": item_id,
                    "amount": quantity
                }
            )
            if stock_reply.get("status") != "ok":
                rollback_stock(removed_items)
                abort(400, f"Out of stock on item_id: {item_id}")
        else:
            stock_reply = send_post_request(f"{GATEWAY_URL}/stock/subtract/{item_id}/{quantity}")
            if stock_reply.status_code != 200:
                rollback_stock(removed_items)
                abort(400, f'Out of stock on item_id: {item_id}')
        removed_items.append((item_id, quantity))
    if USE_KAFKA and kafka_available:
        user_reply = send_kafka_command(
            PAYMENT_COMMAND_TOPIC,
            {
                "action": "pay",
                "user_id": order_entry.user_id,
                "amount": order_entry.total_cost
            }
        )
        if user_reply.get("status") != "ok":
            rollback_stock(removed_items)
            abort(400, "User out of credit")
    else:
        user_reply = send_post_request(f"{GATEWAY_URL}/payment/pay/{order_entry.user_id}/{order_entry.total_cost}")
        if user_reply.status_code != 200:
            rollback_stock(removed_items)
            abort(400, "User out of credit")
    order_entry.paid = True
    try:
        db.set(order_id, msgpack.encode(order_entry))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    app.logger.debug("Checkout successful")
    return Response("Checkout successful", status=200)


create_kafka_clients()


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
