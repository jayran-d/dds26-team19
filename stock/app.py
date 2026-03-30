import logging
import os
import atexit
import uuid

import redis
from streams_worker import init_streams, close_streams

from msgspec import msgpack, Struct
from flask import Flask, jsonify, abort, Response

DB_ERROR_STR = "DB error"
VERBOSE_LOGS = os.getenv("VERBOSE_LOGS", "false").lower() == "true"

app = Flask("stock-service")

db: redis.Redis = redis.Redis(
    host=os.environ['REDIS_HOST'],
    port=int(os.environ['REDIS_PORT']),
    password=os.environ['REDIS_PASSWORD'],
    db=int(os.environ['REDIS_DB']),
    socket_connect_timeout=2,
    socket_timeout=2,
    retry_on_timeout=True,
    health_check_interval=30,
)


def close_connections():
    db.close()
    close_streams()


atexit.register(close_connections)


class StockValue(Struct):
    stock: int
    price: int


def _decode_legacy_item(raw: bytes | None) -> StockValue | None:
    return msgpack.decode(raw, type=StockValue) if raw else None


def _write_item_to_db(item_id: str, item_entry: StockValue) -> None:
    mapping = {
        "stock": int(item_entry.stock),
        "price": int(item_entry.price),
    }
    try:
        db.hset(item_id, mapping=mapping)
    except redis.exceptions.ResponseError:
        # Local dev may still have old string/msgpack keys from before the storage
        # change. Replace them lazily the first time we write.
        db.delete(item_id)
        db.hset(item_id, mapping=mapping)


def get_item_from_db(item_id: str) -> StockValue | None:
    try:
        stock_raw, price_raw = db.hmget(item_id, "stock", "price")
        if stock_raw is not None and price_raw is not None:
            return StockValue(stock=int(stock_raw), price=int(price_raw))
    except redis.exceptions.ResponseError:
        pass
    try:
        entry: bytes = db.get(item_id)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    entry: StockValue | None = _decode_legacy_item(entry)
    if entry is None:
        abort(400, f"Item: {item_id} not found!")
    return entry


# ── Business logic (also called directly by streams_worker) ────────────────────

def apply_stock_delta(item_id: str, delta: int) -> tuple[bool, str | None, int | None]:
    """
    Add *delta* to the item's stock (negative delta = subtract).
    Returns (success, error_message, updated_stock).
    """
    try:
        item_entry = get_item_from_db(item_id)
    except Exception as exc:
        return False, getattr(exc, "description", str(exc)), None
    item_entry.stock += delta
    if item_entry.stock < 0:
        return False, f"Item: {item_id} stock cannot get reduced below zero!", None
    try:
        _write_item_to_db(item_id, item_entry)
    except redis.exceptions.RedisError:
        return False, DB_ERROR_STR, None
    return True, None, item_entry.stock


# ── Routes ─────────────────────────────────────────────────────────────────────

@app.post('/item/create/<price>')
def create_item(price: int):
    key = str(uuid.uuid4())
    app.logger.info(f"Item: {key} created")
    try:
        _write_item_to_db(key, StockValue(stock=0, price=int(price)))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({'item_id': key})


@app.post('/batch_init/<n>/<starting_stock>/<item_price>')
def batch_init_users(n: int, starting_stock: int, item_price: int):
    n = int(n)
    starting_stock = int(starting_stock)
    item_price = int(item_price)
    try:
        pipe = db.pipeline(transaction=False)
        for i in range(n):
            key = f"{i}"
            pipe.delete(key)
            pipe.hset(
                key,
                mapping={
                    "stock": starting_stock,
                    "price": item_price,
                },
            )
        pipe.execute()
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({"msg": "Batch init for stock successful"})


@app.get('/find/<item_id>')
def find_item(item_id: str):
    item_entry: StockValue = get_item_from_db(item_id)
    return jsonify({"stock": item_entry.stock, "price": item_entry.price})


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
    app.logger.info(f"Item: {item_id} stock updated to: {updated_stock}")
    return Response(f"Item: {item_id} stock updated to: {updated_stock}", status=200)


# ── Startup ────────────────────────────────────────────────────────────────────

if __name__ == '__main__':
    app.logger.setLevel(logging.DEBUG if VERBOSE_LOGS else logging.INFO)
    init_streams(app.logger, db)
    app.run(host="0.0.0.0", port=8000, debug=VERBOSE_LOGS)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(logging.DEBUG if VERBOSE_LOGS else gunicorn_logger.level)
    init_streams(app.logger, db)
