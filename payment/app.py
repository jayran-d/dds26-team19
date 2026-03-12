import logging
import os
import atexit
import uuid

import redis
from kafka_worker import init_kafka, close_kafka

from msgspec import msgpack, Struct
from flask import Flask, jsonify, abort, Response

DB_ERROR_STR = "DB error"

app = Flask("payment-service")

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


class UserValue(Struct):
    credit: int


def get_user_from_db(user_id: str) -> UserValue | None:
    try:
        entry: bytes = db.get(user_id)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    entry: UserValue | None = msgpack.decode(entry, type=UserValue) if entry else None
    if entry is None:
        abort(400, f"User: {user_id} not found!")
    return entry


# ── Business logic (also called directly by kafka_worker) ─────────────────────

def remove_credit_internal(user_id: str, amount: int) -> tuple[bool, str | None, int | None]:
    try:
        user_entry = get_user_from_db(user_id)
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


def add_credit_internal(user_id: str, amount: int) -> tuple[bool, str | None, int | None]:
    try:
        user_entry = get_user_from_db(user_id)
    except Exception as exc:
        return False, getattr(exc, "description", str(exc)), None
    user_entry.credit += amount
    try:
        db.set(user_id, msgpack.encode(user_entry))
    except redis.exceptions.RedisError:
        return False, DB_ERROR_STR, None
    return True, None, user_entry.credit


# ── Routes ─────────────────────────────────────────────────────────────────────

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
    kv_pairs: dict[str, bytes] = {
        f"{i}": msgpack.encode(UserValue(credit=starting_money)) for i in range(n)
    }
    try:
        db.mset(kv_pairs)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({"msg": "Batch init for users successful"})


@app.get('/find_user/<user_id>')
def find_user(user_id: str):
    user_entry: UserValue = get_user_from_db(user_id)
    return jsonify({"user_id": user_id, "credit": user_entry.credit})


@app.post('/add_funds/<user_id>/<amount>')
def add_credit(user_id: str, amount: int):
    success, error, updated_credit = add_credit_internal(user_id, int(amount))
    if not success:
        abort(400, error)
    return Response(f"User: {user_id} credit updated to: {updated_credit}", status=200)


@app.post('/pay/<user_id>/<amount>')
def remove_credit(user_id: str, amount: int):
    app.logger.info(f"Removing {amount} credit from user: {user_id}")
    success, error, updated_credit = remove_credit_internal(user_id, int(amount))
    if not success:
        abort(400, error)
    return Response(f"User: {user_id} credit updated to: {updated_credit}", status=200)


# ── Startup ────────────────────────────────────────────────────────────────────

if __name__ == '__main__':
    init_kafka(app.logger)
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
    init_kafka(app.logger, db)