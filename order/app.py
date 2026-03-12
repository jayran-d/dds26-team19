import logging
import os
import atexit
import random
import uuid
from collections import defaultdict

import redis
import requests
from msgspec import msgpack, Struct
from flask import Flask, jsonify, abort, Response

from kafka_worker import (
    init_kafka,
    close_kafka,
    is_available,
    start_checkout,
)
from common.messages import SagaOrderStatus

DB_ERROR_STR  = "DB error"
REQ_ERROR_STR = "Requests error"

GATEWAY_URL = os.environ['GATEWAY_URL']
USE_KAFKA   = os.getenv("USE_KAFKA", "false").lower() == "true"

app = Flask("order-service")

db: redis.Redis = redis.Redis(
    host=os.environ['REDIS_HOST'],
    port=int(os.environ['REDIS_PORT']),
    password=os.environ['REDIS_PASSWORD'],
    db=int(os.environ['REDIS_DB']),
)


def close_connections():
    db.close()
    close_kafka()


atexit.register(close_connections)


class OrderValue(Struct):
    paid: bool
    items: list[tuple[str, int]]
    user_id: str
    total_cost: int


def get_order_from_db(order_id: str) -> OrderValue | None:
    try:
        entry: bytes = db.get(order_id)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    entry: OrderValue | None = msgpack.decode(entry, type=OrderValue) if entry else None
    if entry is None:
        abort(400, f"Order: {order_id} not found!")
    return entry


def get_order_status(order_id: str) -> str | None:
    try:
        val = db.get(f"order:{order_id}:status")
        return val.decode() if val else None
    except redis.exceptions.RedisError:
        return None


# ── Routes ─────────────────────────────────────────────────────────────────────

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
    n          = int(n)
    n_items    = int(n_items)
    n_users    = int(n_users)
    item_price = int(item_price)

    def generate_entry() -> OrderValue:
        user_id  = random.randint(0, n_users - 1)
        item1_id = random.randint(0, n_items - 1)
        item2_id = random.randint(0, n_items - 1)
        return OrderValue(
            paid=False,
            items=[(f"{item1_id}", 1), (f"{item2_id}", 1)],
            user_id=f"{user_id}",
            total_cost=2 * item_price,
        )

    kv_pairs: dict[str, bytes] = {f"{i}": msgpack.encode(generate_entry()) for i in range(n)}
    try:
        db.mset(kv_pairs)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({"msg": "Batch init for orders successful"})


@app.get('/find/<order_id>')
def find_order(order_id: str):
    order_entry: OrderValue = get_order_from_db(order_id)
    return jsonify({
        "order_id":   order_id,
        "paid":       order_entry.paid,
        "items":      order_entry.items,
        "user_id":    order_entry.user_id,
        "total_cost": order_entry.total_cost,
    })


@app.get('/status/<order_id>')
def order_status(order_id: str):
    """Poll this to check the result of an async checkout."""
    get_order_from_db(order_id)  # 400 if order doesn't exist
    status = get_order_status(order_id)
    return jsonify({
        "order_id": order_id,
        "status":   status or SagaOrderStatus.PENDING,
    })


def _send_post_request(url: str):
    try:
        return requests.post(url)
    except requests.exceptions.RequestException:
        abort(400, REQ_ERROR_STR)


def _send_get_request(url: str):
    try:
        return requests.get(url)
    except requests.exceptions.RequestException:
        abort(400, REQ_ERROR_STR)


@app.post('/addItem/<order_id>/<item_id>/<quantity>')
def add_item(order_id: str, item_id: str, quantity: int):
    order_entry: OrderValue = get_order_from_db(order_id)
    item_reply = _send_get_request(f"{GATEWAY_URL}/stock/find/{item_id}")
    if item_reply.status_code != 200:
        abort(400, f"Item: {item_id} does not exist!")
    item_json: dict = item_reply.json()
    order_entry.items.append((item_id, int(quantity)))
    order_entry.total_cost += int(quantity) * item_json["price"]
    try:
        db.set(order_id, msgpack.encode(order_entry))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(
        f"Item: {item_id} added to: {order_id} price updated to: {order_entry.total_cost}",
        status=200,
    )


@app.post('/checkout/<order_id>')
def checkout(order_id: str):
    """
    Kafka path:
        - if already completed, do not start again
        - if already in progress, return 202 again without creating a second tx
        - otherwise start a new checkout
    HTTP path:
          — synchronous fallback when USE_KAFKA=false.
    """
    order_entry: OrderValue = get_order_from_db(order_id)

    # Fast idempotency check: if the order is already paid/completed,
    # never try to charge/reserve again.
    status = get_order_status(order_id)
    if order_entry.paid or status == SagaOrderStatus.COMPLETED:
        return Response(
            "Order already completed",
            status=200,
            headers={"Location": f"/orders/status/{order_id}"},
        )

    if USE_KAFKA and is_available():
        # Friendly fast-path for obvious duplicates.
        if status in {
            SagaOrderStatus.RESERVING_STOCK,
            SagaOrderStatus.PROCESSING_PAYMENT,
            SagaOrderStatus.COMPENSATING,
        }:
            return Response(
                "Checkout already in progress",
                status=202,
                headers={"Location": f"/orders/status/{order_id}"},
            )

        try:
            result = start_checkout(order_id, order_entry)
        except Exception as exc:
            app.logger.error(f"[checkout] failed to start: {exc}")
            abort(400, str(exc))

        # This handles the real race-safe answer coming back from saga_start_checkout().
        if isinstance(result, dict) and result.get("reason") == "already_in_progress":
            return Response(
                "Checkout already in progress",
                status=202,
                headers={"Location": f"/orders/status/{order_id}"},
            )

        if isinstance(result, dict) and result.get("reason") == "error":
            abort(400, "Failed to start checkout")

        return Response(
            "Checkout initiated",
            status=202,
            headers={"Location": f"/orders/status/{order_id}"},
        )

    # ── HTTP fallback ──────────────────────────────────────────────────────────
    items_quantities: dict[str, int] = defaultdict(int)
    for item_id, quantity in order_entry.items:
        items_quantities[item_id] += quantity

    removed_items: list[tuple[str, int]] = []

    for item_id, quantity in items_quantities.items():
        reply = _send_post_request(f"{GATEWAY_URL}/stock/subtract/{item_id}/{quantity}")
        if reply.status_code != 200:
            for rid, rqty in removed_items:
                _send_post_request(f"{GATEWAY_URL}/stock/add/{rid}/{rqty}")
            abort(400, f"Out of stock on item_id: {item_id}")
        removed_items.append((item_id, quantity))

    reply = _send_post_request(
        f"{GATEWAY_URL}/payment/pay/{order_entry.user_id}/{order_entry.total_cost}"
    )
    if reply.status_code != 200:
        for rid, rqty in removed_items:
            _send_post_request(f"{GATEWAY_URL}/stock/add/{rid}/{rqty}")
        abort(400, "User out of credit")

    order_entry.paid = True
    try:
        db.set(order_id, msgpack.encode(order_entry))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)

    return Response("Checkout successful", status=200)


# ── Startup ────────────────────────────────────────────────────────────────────

if __name__ == '__main__':
    init_kafka(app.logger, db)
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
    init_kafka(app.logger, db)