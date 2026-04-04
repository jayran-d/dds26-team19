import asyncio
import atexit
import logging
import os
import random
import uuid

import redis
import requests
from msgspec import Struct, msgpack
from quart import Quart, Response, abort, jsonify

from common.messages import SagaOrderStatus, TwoPhaseOrderStatus

DB_ERROR_STR = "DB error"
REQ_ERROR_STR = "Requests error"

GATEWAY_URL = os.environ["GATEWAY_URL"]
ORCHESTRATOR_URL = os.environ.get("ORCHESTRATOR_URL", "http://orchestrator-service:5000")
CHECKOUT_WAIT_TIMEOUT_SECONDS = float(os.getenv("CHECKOUT_WAIT_TIMEOUT_SECONDS", "45"))
VERBOSE_LOGS = os.getenv("VERBOSE_LOGS", "false").lower() == "true"

CHECKOUT_POLL_INTERVAL = float(os.getenv("CHECKOUT_POLL_INTERVAL", "0.02"))

IN_PROGRESS_STATUSES = {
    SagaOrderStatus.RESERVING_STOCK,
    SagaOrderStatus.PROCESSING_PAYMENT,
    SagaOrderStatus.COMPENSATING,
    TwoPhaseOrderStatus.PREPARING_STOCK,
    TwoPhaseOrderStatus.PREPARING_PAYMENT,
    TwoPhaseOrderStatus.COMMITTING,
    TwoPhaseOrderStatus.ABORTING,
}

TERMINAL_SUCCESS_STATUSES = {
    SagaOrderStatus.COMPLETED,
    TwoPhaseOrderStatus.COMPLETED,
}

TERMINAL_FAILURE_STATUSES = {
    SagaOrderStatus.FAILED,
    TwoPhaseOrderStatus.FAILED,
}

TERMINAL_STATUSES = TERMINAL_SUCCESS_STATUSES | TERMINAL_FAILURE_STATUSES

app = Quart("order-service")

db: redis.Redis = redis.Redis(
    host=os.environ["REDIS_HOST"],
    port=int(os.environ["REDIS_PORT"]),
    password=os.environ["REDIS_PASSWORD"],
    db=int(os.environ["REDIS_DB"]),
    socket_connect_timeout=2,
    socket_timeout=2,
    retry_on_timeout=True,
    health_check_interval=30,
)


def close_connections() -> None:
    db.close()


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
    decoded: OrderValue | None = msgpack.decode(entry, type=OrderValue) if entry else None
    if decoded is None:
        abort(400, f"Order: {order_id} not found!")
    return decoded


def get_order_status(order_id: str) -> str | None:
    try:
        val = db.get(f"order:{order_id}:status")
        return val.decode() if val else None
    except redis.exceptions.RedisError:
        return None


def get_order_and_status(order_id: str) -> tuple[OrderValue | None, str | None]:
    try:
        raw_order, raw_status = db.mget(order_id, f"order:{order_id}:status")
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    order_entry: OrderValue | None = msgpack.decode(raw_order, type=OrderValue) if raw_order else None
    if order_entry is None:
        abort(400, f"Order: {order_id} not found!")
    status = raw_status.decode() if raw_status else None
    return order_entry, status


async def get_order_status_async(order_id: str) -> str | None:
    try:
        val = await asyncio.to_thread(db.get, f"order:{order_id}:status")
        return val.decode() if val else None
    except redis.exceptions.RedisError:
        return None


async def get_order_and_status_async(order_id: str) -> tuple[OrderValue | None, str | None]:
    try:
        raw_order, raw_status = await asyncio.to_thread(
            db.mget,
            order_id,
            f"order:{order_id}:status",
        )
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    order_entry: OrderValue | None = msgpack.decode(raw_order, type=OrderValue) if raw_order else None
    if order_entry is None:
        abort(400, f"Order: {order_id} not found!")
    status = raw_status.decode() if raw_status else None
    return order_entry, status


async def _poll_for_terminal_status(order_id: str) -> Response:
    """Poll shared order-db status until the orchestrator reaches a terminal state."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + CHECKOUT_WAIT_TIMEOUT_SECONDS

    while True:
        status = await get_order_status_async(order_id)
        if status in TERMINAL_STATUSES:
            break
        if loop.time() >= deadline:
            return Response(
                "Checkout timed out before reaching a terminal state",
                status=400,
                headers={"Location": f"/orders/status/{order_id}"},
            )
        await asyncio.sleep(CHECKOUT_POLL_INTERVAL)

    if status in TERMINAL_SUCCESS_STATUSES:
        return Response(
            "Checkout successful",
            status=200,
            headers={"Location": f"/orders/status/{order_id}"},
        )

    return Response(
        "Checkout failed",
        status=400,
        headers={"Location": f"/orders/status/{order_id}"},
    )


def _build_checkout_response_from_status(order_id: str, status: str) -> Response:
    if status in TERMINAL_SUCCESS_STATUSES:
        return Response(
            "Checkout successful",
            status=200,
            headers={"Location": f"/orders/status/{order_id}"},
        )
    return Response(
        "Checkout failed",
        status=400,
        headers={"Location": f"/orders/status/{order_id}"},
    )


async def _resolve_uncertain_checkout_start(order_id: str) -> Response | None:
    status = await get_order_status_async(order_id)
    if status in IN_PROGRESS_STATUSES:
        return await _poll_for_terminal_status(order_id)
    if status in TERMINAL_STATUSES:
        return _build_checkout_response_from_status(order_id, status)
    return None


@app.post("/create/<user_id>")
def create_order(user_id: str):
    key = str(uuid.uuid4())
    value = msgpack.encode(OrderValue(paid=False, items=[], user_id=user_id, total_cost=0))
    try:
        db.set(key, value)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({"order_id": key})


@app.post("/batch_init/<n>/<n_items>/<n_users>/<item_price>")
def batch_init_users(n: int, n_items: int, n_users: int, item_price: int):
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
        db.mset(kv_pairs)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({"msg": "Batch init for orders successful"})


@app.get("/find/<order_id>")
def find_order(order_id: str):
    order_entry, status = get_order_and_status(order_id)
    return jsonify(
        {
            "order_id": order_id,
            "paid": order_entry.paid or status in TERMINAL_SUCCESS_STATUSES,
            "items": order_entry.items,
            "user_id": order_entry.user_id,
            "total_cost": order_entry.total_cost,
        }
    )


@app.get("/status/<order_id>")
def order_status(order_id: str):
    get_order_from_db(order_id)
    status = get_order_status(order_id)
    default_status = TwoPhaseOrderStatus.PENDING if os.getenv("TRANSACTION_MODE", "saga") == "2pc" else SagaOrderStatus.PENDING
    return jsonify({"order_id": order_id, "status": status or default_status})


def _send_get_request(url: str):
    try:
        return requests.get(url)
    except requests.exceptions.RequestException:
        abort(400, REQ_ERROR_STR)


@app.post("/addItem/<order_id>/<item_id>/<quantity>")
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


@app.post("/checkout/<order_id>")
async def checkout(order_id: str):
    order_entry, status = await get_order_and_status_async(order_id)

    if order_entry.paid or status in TERMINAL_SUCCESS_STATUSES:
        return Response(
            "Order already completed",
            status=200,
            headers={"Location": f"/orders/status/{order_id}"},
        )

    if status in IN_PROGRESS_STATUSES:
        return await _poll_for_terminal_status(order_id)

    payload = {
        "order_id": order_id,
        "user_id": order_entry.user_id,
        "total_cost": order_entry.total_cost,
        "items": order_entry.items,
    }

    for attempt in range(2):
        try:
            resp = await asyncio.to_thread(
                requests.post,
                f"{ORCHESTRATOR_URL}/transactions/checkout",
                json=payload,
                timeout=5,
            )
        except requests.exceptions.RequestException as exc:
            app.logger.warning(
                f"[checkout] orchestrator call attempt={attempt + 1} failed: {exc}"
            )
            resolved = await _resolve_uncertain_checkout_start(order_id)
            if resolved is not None:
                return resolved
            if attempt == 0:
                await asyncio.sleep(CHECKOUT_POLL_INTERVAL)
                continue
            return Response(
                "Checkout start could not be confirmed",
                status=400,
                headers={"Location": f"/orders/status/{order_id}"},
            )

        if resp.status_code in (200, 202):
            return await _poll_for_terminal_status(order_id)

        if resp.status_code >= 500:
            app.logger.warning(
                f"[checkout] orchestrator returned {resp.status_code} on attempt={attempt + 1}"
            )
            resolved = await _resolve_uncertain_checkout_start(order_id)
            if resolved is not None:
                return resolved
            if attempt == 0:
                await asyncio.sleep(CHECKOUT_POLL_INTERVAL)
                continue
            return Response(
                "Checkout start could not be confirmed",
                status=400,
                headers={"Location": f"/orders/status/{order_id}"},
            )

        resolved = await _resolve_uncertain_checkout_start(order_id)
        if resolved is not None:
            return resolved
        abort(400, f"Orchestrator rejected checkout: {resp.status_code}")

    return Response(
        "Checkout start could not be confirmed",
        status=400,
        headers={"Location": f"/orders/status/{order_id}"},
    )


if __name__ == "__main__":
    app.logger.setLevel(logging.DEBUG if VERBOSE_LOGS else logging.INFO)
    app.run(host="0.0.0.0", port=8000, debug=VERBOSE_LOGS)
else:
    server_logger = logging.getLogger("hypercorn.error")
    if server_logger.handlers:
        app.logger.handlers = server_logger.handlers
        app.logger.setLevel(logging.DEBUG if VERBOSE_LOGS else server_logger.level)
