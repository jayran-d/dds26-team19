"""
stock-service/async_app.py

Async version using Quart instead of Flask.

Participants don't have complex orchestration, just handle commands and reply.
"""

import logging
import os
import atexit
import uuid
import asyncio

import redis.asyncio as aioredis
from msgspec import msgpack, Struct
from quart import Quart, jsonify, abort, Response

from async_kafka_worker import init_kafka_async, close_kafka_async

DB_ERROR_STR = "DB error"

app = Quart(__name__)

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


@app.before_serving
async def startup():
    """Initialize Kafka worker on startup."""
    await init_kafka_async(app.logger, db)


@app.after_serving
async def shutdown():
    """Clean up on shutdown."""
    await db.aclose()
    await close_kafka_async()


def close_connections():
    """atexit handler."""
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            asyncio.create_task(close_connections_async())
        else:
            loop.run_until_complete(close_connections_async())
    except:
        pass


async def close_connections_async():
    await db.aclose()
    await close_kafka_async()


atexit.register(close_connections)


class StockValue(Struct):
    stock: int
    price: int


async def get_item_from_db(item_id: str) -> StockValue | None:
    try:
        entry: bytes = await db.get(item_id)
    except aioredis.ResponseError:
        return await abort(400, DB_ERROR_STR)
    entry: StockValue | None = msgpack.decode(entry, type=StockValue) if entry else None
    if entry is None:
        await abort(400, f"Item: {item_id} not found!")
    return entry


# ── Business logic (also called directly by kafka_worker) ──────────────────────

async def apply_stock_delta(item_id: str, delta: int) -> tuple[bool, str | None, int | None]:
    """
    Add *delta* to the item's stock (negative delta = subtract).
    Returns (success, error_message, updated_stock).
    """
    try:
        item_entry = await get_item_from_db(item_id)
    except Exception as exc:
        return False, getattr(exc, "description", str(exc)), None
    item_entry.stock += delta
    if item_entry.stock < 0:
        return False, f"Item: {item_id} stock cannot get reduced below zero!", None
    try:
        await db.set(item_id, msgpack.encode(item_entry))
    except aioredis.ResponseError:
        return False, DB_ERROR_STR, None
    return True, None, item_entry.stock


# ── Routes ─────────────────────────────────────────────────────────────────────

@app.post('/item/create/<price>')
async def create_item(price: int):
    key = str(uuid.uuid4())
    app.logger.info(f"Item: {key} created")
    value = msgpack.encode(StockValue(stock=0, price=int(price)))
    try:
        await db.set(key, value)
    except aioredis.ResponseError:
        return await abort(400, DB_ERROR_STR)
    return jsonify({'item_id': key})


@app.post('/batch_init/<n>/<starting_stock>/<item_price>')
async def batch_init_users(n: int, starting_stock: int, item_price: int):
    n = int(n)
    starting_stock = int(starting_stock)
    item_price = int(item_price)
    kv_pairs: dict[str, bytes] = {
        f"{i}": msgpack.encode(StockValue(stock=starting_stock, price=item_price))
        for i in range(n)
    }
    try:
        await db.mset(kv_pairs)
    except aioredis.ResponseError:
        return await abort(400, DB_ERROR_STR)
    return jsonify({"msg": "Batch init for stock successful"})


@app.get('/find/<item_id>')
async def find_item(item_id: str):
    item_entry: StockValue = await get_item_from_db(item_id)
    return jsonify({"item_id": item_id, "stock": item_entry.stock, "price": item_entry.price})


@app.post('/subtract/<item_id>/<quantity>')
async def subtract_stock(item_id: str, quantity: int):
    app.logger.info(f"Subtracting {quantity} stock from item: {item_id}")
    success, error, updated_stock = await apply_stock_delta(item_id, -int(quantity))
    if not success:
        await abort(400, error)
    return Response(f"Item: {item_id} stock updated to: {updated_stock}", status=200)


@app.post('/add/<item_id>/<quantity>')
async def add_stock(item_id: str, quantity: int):
    app.logger.info(f"Adding {quantity} stock to item: {item_id}")
    success, error, updated_stock = await apply_stock_delta(item_id, int(quantity))
    if not success:
        await abort(400, error)
    return Response(f"Item: {item_id} stock updated to: {updated_stock}", status=200)


# ── Startup ────────────────────────────────────────────────────────────────────

if __name__ == '__main__':
    import hypercorn.asyncio
    
    config = hypercorn.Config()
    config.bind = ["0.0.0.0:8000"]
    config.workers = 1
    asyncio.run(hypercorn.asyncio.serve(app, config))
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
