"""
order-service/async_app.py

Async version using Quart instead of Flask.

This version uses the same logic as the original app.py but with:
- Quart (async web framework)
- httpx.AsyncClient (async HTTP client - but used synchronously for consistency)
- redis.asyncio (async Redis)
- Hypercorn (async server)

Can use either Kafka or Redis Streams for message transport.
"""

import logging
import os
import atexit
import random
import uuid
from collections import defaultdict
import time
import asyncio

import redis.asyncio as aioredis
import httpx
from msgspec import msgpack, Struct
from quart import Quart, jsonify, abort, Response

from common.messages import SagaOrderStatus

DB_ERROR_STR = "DB error"
REQ_ERROR_STR = "Requests error"

GATEWAY_URL = os.environ['GATEWAY_URL']
USE_KAFKA = os.getenv("USE_KAFKA", "false").lower() == "true"
USE_REDIS_STREAMS = os.getenv("USE_REDIS_STREAMS", "false").lower() == "true"
CHECKOUT_WAIT_TIMEOUT_SECONDS = float(os.getenv("CHECKOUT_WAIT_TIMEOUT_SECONDS", "15"))
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

app = Quart("order-service")

db: aioredis.Redis = aioredis.Redis(
    host=os.environ['REDIS_HOST'],
    port=int(os.environ['REDIS_PORT']),
    password=os.environ['REDIS_PASSWORD'],
    db=int(os.environ['REDIS_DB']),
    socket_connect_timeout=2,
    socket_timeout=2,
    retry_on_timeout=True,
    health_check_interval=30,
)

_http_client: httpx.Client | None = None


async def close_connections_async():
    """Clean up on shutdown."""
    await db.aclose()
    if USE_KAFKA:
        from async_kafka_worker import close_kafka_async
        await close_kafka_async()


atexit.register(lambda: asyncio.run(close_connections_async()))


class OrderValue(Struct):
    paid: bool
    items: list[tuple[str, int]]
    user_id: str
    total_cost: int


async def get_order_from_db(order_id: str) -> OrderValue | None:
    try:
        entry: bytes = await db.get(order_id)
    except aioredis.RedisError:
        return abort(400, DB_ERROR_STR)
    entry: OrderValue | None = msgpack.decode(entry, type=OrderValue) if entry else None
    if entry is None:
        abort(400, f"Order: {order_id} not found!")
    return entry


async def get_order_status(order_id: str) -> str | None:
    try:
        val = await db.get(f"order:{order_id}:status")
        return val.decode() if val else None
    except aioredis.RedisError:
        return None


# ── Routes ─────────────────────────────────────────────────────────────────────

async def _wait_for_terminal_checkout_status(order_id: str) -> str | None:
    """
    Block until the Saga reaches a terminal state or the timeout expires.

    This keeps the distributed transaction asynchronous internally, but gives
    the external HTTP API a final success/failure result.
    """
    deadline = time.time() + CHECKOUT_WAIT_TIMEOUT_SECONDS

    while time.time() < deadline:
        status = await get_order_status(order_id) or SagaOrderStatus.PENDING
        if status in TERMINAL_STATUSES:
            return status
        await asyncio.sleep(CHECKOUT_POLL_INTERVAL_SECONDS)

    return None


async def _build_terminal_checkout_response(order_id: str) -> Response:
    final_status = await _wait_for_terminal_checkout_status(order_id)

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
async def create_order(user_id: str):
    key = str(uuid.uuid4())
    value = msgpack.encode(OrderValue(paid=False, items=[], user_id=user_id, total_cost=0))
    try:
        await db.set(key, value)
    except aioredis.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({'order_id': key})


@app.post('/batch_init/<n>/<n_items>/<n_users>/<item_price>')
async def batch_init_users(n: int, n_items: int, n_users: int, item_price: int):
    n = int(n)
    n_items = int(n_items)
    n_users = int(n_users)
    item_price = int(item_price)

    def generate_entry() -> OrderValue:
        user_id = random.randint(0, n_users - 1)
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
        await db.mset(kv_pairs)
    except aioredis.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({"msg": "Batch init for orders successful"})


@app.get('/find/<order_id>')
async def find_order(order_id: str):
    order_entry: OrderValue = await get_order_from_db(order_id)
    return jsonify({
        "order_id": order_id,
        "paid": order_entry.paid,
        "items": order_entry.items,
        "user_id": order_entry.user_id,
        "total_cost": order_entry.total_cost,
    })


@app.get('/status/<order_id>')
async def order_status(order_id: str):
    """Poll this to check the result of an async checkout."""
    await get_order_from_db(order_id)  # 400 if order doesn't exist
    status = await get_order_status(order_id)
    return jsonify({
        "order_id": order_id,
        "status": status or SagaOrderStatus.PENDING,
    })


async def _send_post_request(url: str):
    try:
        # Using blocking httpx.Client for consistency (prevents race conditions)
        # Blocking inside async function just blocks that coroutine, not the entire server
        return _http_client.post(url)
    except httpx.RequestError:
        abort(400, REQ_ERROR_STR)


async def _send_get_request(url: str):
    try:
        # Using blocking httpx.Client for consistency
        return _http_client.get(url)
    except httpx.RequestError:
        abort(400, REQ_ERROR_STR)


@app.post('/addItem/<order_id>/<item_id>/<quantity>')
async def add_item(order_id: str, item_id: str, quantity: int):
    order_entry: OrderValue = await get_order_from_db(order_id)
    item_reply = await _send_get_request(f"{GATEWAY_URL}/stock/find/{item_id}")
    if item_reply.status_code != 200:
        abort(400, f"Item: {item_id} does not exist!")
    item_json: dict = item_reply.json()
    order_entry.items.append((item_id, int(quantity)))
    order_entry.total_cost += int(quantity) * item_json["price"]
    try:
        await db.set(order_id, msgpack.encode(order_entry))
    except aioredis.RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(
        f"Item: {item_id} added to: {order_id} price updated to: {order_entry.total_cost}",
        status=200,
    )


@app.post('/checkout/<order_id>')
async def checkout(order_id: str):
    """
    Kafka path:
        - return a terminal HTTP result after the Saga finishes
        - duplicate concurrent requests wait for the same active Saga instead of
          starting a second checkout

    HTTP path:
        - synchronous fallback when USE_KAFKA=false
    """
    order_entry: OrderValue = await get_order_from_db(order_id)

    status = await get_order_status(order_id)

    if order_entry.paid or status == SagaOrderStatus.COMPLETED:
        return Response(
            "Order already completed",
            status=200,
            headers={"Location": f"/orders/status/{order_id}"},
        )

    if USE_KAFKA:
        # Check if Kafka worker is available
        try:
            from async_kafka_worker import is_available
            kafka_available = is_available()
        except ImportError:
            kafka_available = False

        if kafka_available:
            # If another request already started the Saga for this order,
            # wait for its final result instead of starting a second one.
            if status in IN_PROGRESS_STATUSES:
                return await _build_terminal_checkout_response(order_id)

            try:
                from async_kafka_worker import start_checkout
                result = start_checkout(order_id, order_entry)
            except Exception as exc:
                app.logger.error(f"[checkout] failed to start: {exc}")
                abort(400, str(exc))

            if isinstance(result, dict) and result.get("reason") == "already_in_progress":
                return await _build_terminal_checkout_response(order_id)

            if isinstance(result, dict) and result.get("reason") == "error":
                abort(400, "Failed to start checkout")

            return await _build_terminal_checkout_response(order_id)

    # ── HTTP fallback ──────────────────────────────────────────────────────────
    items_quantities: dict[str, int] = defaultdict(int)
    for item_id, quantity in order_entry.items:
        items_quantities[item_id] += quantity

    removed_items: list[tuple[str, int]] = []

    for item_id, quantity in items_quantities.items():
        reply = await _send_post_request(f"{GATEWAY_URL}/stock/subtract/{item_id}/{quantity}")
        if reply.status_code != 200:
            for rid, rqty in removed_items:
                await _send_post_request(f"{GATEWAY_URL}/stock/add/{rid}/{rqty}")
            abort(400, f"Out of stock on item_id: {item_id}")
        removed_items.append((item_id, quantity))

    reply = await _send_post_request(
        f"{GATEWAY_URL}/payment/pay/{order_entry.user_id}/{order_entry.total_cost}"
    )
    if reply.status_code != 200:
        for rid, rqty in removed_items:
            await _send_post_request(f"{GATEWAY_URL}/stock/add/{rid}/{rqty}")
        abort(400, "User out of credit")

    order_entry.paid = True
    try:
        await db.set(order_id, msgpack.encode(order_entry))
    except aioredis.RedisError:
        return abort(400, DB_ERROR_STR)

    return Response("Checkout successful", status=200)


@app.get('/health')
async def health():
    """Simple health check endpoint."""
    return jsonify({"status": "healthy"})


# ── Startup/Shutdown ───────────────────────────────────────────────────────────

@app.before_serving
async def startup():
    """Initialize HTTP client and Kafka worker on startup."""
    global _http_client
    _http_client = httpx.Client(timeout=10.0)
    if USE_KAFKA:
        from async_kafka_worker import init_kafka_async
        await init_kafka_async(app.logger, db)


@app.after_serving
async def shutdown():
    """Clean up on shutdown."""
    global _http_client
    if _http_client:
        _http_client.close()
    await db.aclose()
    if USE_KAFKA:
        from async_kafka_worker import close_kafka_async
        await close_kafka_async()
