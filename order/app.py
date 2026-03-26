import logging
import os
import atexit
import random
import uuid
import time
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
CHECKOUT_WAIT_TIMEOUT_SECONDS = float(os.getenv("CHECKOUT_WAIT_TIMEOUT_SECONDS", "45"))
CHECKOUT_POLL_INTERVAL_SECONDS = float(os.getenv("CHECKOUT_POLL_INTERVAL_SECONDS", "0.05"))

IN_PROGRESS_STATUSES = {
    SagaOrderStatus.RESERVING_STOCK,
    SagaOrderStatus.PROCESSING_PAYMENT,
    SagaOrderStatus.COMPENSATING,
}

TERMINAL_STATUSES = {
    SagaOrderStatus.COMPLETED,
    SagaOrderStatus.FAILED,
}

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

def _wait_for_terminal_checkout_status(order_id: str) -> str | None:
    """
    Block until the Saga reaches a terminal state or the timeout expires.

    This keeps the distributed transaction asynchronous internally, but gives
    the external HTTP API a final success/failure result.
    """
    deadline = time.time() + CHECKOUT_WAIT_TIMEOUT_SECONDS

    while time.time() < deadline:
        status = get_order_status(order_id) or SagaOrderStatus.PENDING
        if status in TERMINAL_STATUSES:
            return status
        time.sleep(CHECKOUT_POLL_INTERVAL_SECONDS)

    return None


def _build_terminal_checkout_response(order_id: str) -> Response:
    final_status = _wait_for_terminal_checkout_status(order_id)

    if final_status == SagaOrderStatus.COMPLETED:
        return Response(
            "Checkout successful",
            status=200,
            headers={"Location": f"/orders/status/{order_id}"},
        )

    if final_status == SagaOrderStatus.FAILED:
        return Response(
            "Checkout failed",
            status=400,
            headers={"Location": f"/orders/status/{order_id}"},
        )

    return Response(
        "Checkout timed out before reaching a terminal state",
        status=400,
        headers={"Location": f"/orders/status/{order_id}"},
    )

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
        - return a terminal HTTP result after the Saga finishes
        - duplicate concurrent requests wait for the same active Saga instead of
          starting a second checkout

    HTTP path:
        - synchronous fallback when USE_KAFKA=false
    """
    order_entry: OrderValue = get_order_from_db(order_id)
    status = get_order_status(order_id)

    if order_entry.paid or status == SagaOrderStatus.COMPLETED:
        return Response(
            "Order already completed",
            status=200,
            headers={"Location": f"/orders/status/{order_id}"},
        )

    if USE_KAFKA and is_available():
        # If another request already started the Saga for this order,
        # wait for its final result instead of starting a second one.
        if status in IN_PROGRESS_STATUSES:
            return _build_terminal_checkout_response(order_id)

        try:
            result = start_checkout(order_id, order_entry)
        except Exception as exc:
            app.logger.error(f"[checkout] failed to start: {exc}")
            abort(400, str(exc))

        if isinstance(result, dict) and result.get("reason") == "already_in_progress":
            return _build_terminal_checkout_response(order_id)

        if isinstance(result, dict) and result.get("reason") == "error":
            abort(400, "Failed to start checkout")

        return _build_terminal_checkout_response(order_id)

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