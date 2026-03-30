"""
payment-service/async_app.py

Async version using Quart instead of Flask.

Participants don't have the complex orchestration logic, so they're simpler.
They just handle commands and publish events back.
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


class UserValue(Struct):
    credit: int


async def get_user_from_db(user_id: str) -> UserValue | None:
    try:
        entry: bytes = await db.get(user_id)
    except aioredis.ResponseError:
        return await abort(400, DB_ERROR_STR)
    entry: UserValue | None = msgpack.decode(entry, type=UserValue) if entry else None
    if entry is None:
        await abort(400, f"User: {user_id} not found!")
    return entry


# ── Business logic (also called directly by kafka_worker) ─────────────────────

async def remove_credit_internal(user_id: str, amount: int) -> tuple[bool, str | None, int | None]:
    try:
        user_entry = await get_user_from_db(user_id)
    except Exception as exc:
        return False, getattr(exc, "description", str(exc)), None
    user_entry.credit -= amount
    if user_entry.credit < 0:
        return False, f"User: {user_id} credit cannot get reduced below zero!", None
    try:
        await db.set(user_id, msgpack.encode(user_entry))
    except aioredis.ResponseError:
        return False, DB_ERROR_STR, None
    return True, None, user_entry.credit


async def add_credit_internal(user_id: str, amount: int) -> tuple[bool, str | None, int | None]:
    try:
        user_entry = await get_user_from_db(user_id)
    except Exception as exc:
        return False, getattr(exc, "description", str(exc)), None
    user_entry.credit += amount
    try:
        await db.set(user_id, msgpack.encode(user_entry))
    except aioredis.ResponseError:
        return False, DB_ERROR_STR, None
    return True, None, user_entry.credit


# ── Routes ─────────────────────────────────────────────────────────────────────

@app.post('/create_user')
async def create_user():
    key = str(uuid.uuid4())
    value = msgpack.encode(UserValue(credit=0))
    try:
        await db.set(key, value)
    except aioredis.ResponseError:
        return await abort(400, DB_ERROR_STR)
    return jsonify({'user_id': key})


@app.post('/batch_init/<n>/<starting_money>')
async def batch_init_users(n: int, starting_money: int):
    n = int(n)
    starting_money = int(starting_money)
    kv_pairs: dict[str, bytes] = {
        f"{i}": msgpack.encode(UserValue(credit=starting_money)) for i in range(n)
    }
    try:
        await db.mset(kv_pairs)
    except aioredis.ResponseError:
        return await abort(400, DB_ERROR_STR)
    return jsonify({"msg": "Batch init for users successful"})


@app.get('/find_user/<user_id>')
async def find_user(user_id: str):
    user_entry: UserValue = await get_user_from_db(user_id)
    return jsonify({"user_id": user_id, "credit": user_entry.credit})


@app.post('/add_funds/<user_id>/<amount>')
async def add_credit(user_id: str, amount: int):
    success, error, updated_credit = await add_credit_internal(user_id, int(amount))
    if not success:
        await abort(400, error)
    return Response(f"User: {user_id} credit updated to: {updated_credit}", status=200)


@app.post('/pay/<user_id>/<amount>')
async def remove_credit(user_id: str, amount: int):
    app.logger.info(f"Removing {amount} credit from user: {user_id}")
    success, error, updated_credit = await remove_credit_internal(user_id, int(amount))
    if not success:
        await abort(400, error)
    return Response(f"User: {user_id} credit updated to: {updated_credit}", status=200)


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
