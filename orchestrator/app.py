"""
orchestrator/app.py

Thin HTTP facade for the transaction orchestrator.

POST /transactions/checkout
    Body: {"order_id": "...", "user_id": "...", "total_cost": 1000, "items": [...]}
    Kicks off a Saga or 2PC transaction and returns immediately.
    The order-service polls order:{id}:status in order-db for the result.

GET /health
    Readiness check.
"""

import atexit
import logging
import os

import redis
from flask import Flask, request, jsonify, abort

from streams_worker import init_streams, close_streams, is_available, start_checkout

VERBOSE_LOGS = os.getenv("VERBOSE_LOGS", "false").lower() == "true"

app = Flask("orchestrator-service")

# Orchestrator's own Redis (coord_db): saga records, 2pc state, etc.
coord_db: redis.Redis = redis.Redis(
    host=os.environ['REDIS_HOST'],
    port=int(os.environ.get('REDIS_PORT', 6379)),
    password=os.environ['REDIS_PASSWORD'],
    db=int(os.environ.get('REDIS_DB', 0)),
    socket_connect_timeout=2,
    socket_timeout=2,
    retry_on_timeout=True,
    health_check_interval=30,
)


def close_connections():
    coord_db.close()
    close_streams()


atexit.register(close_connections)


@app.post('/transactions/checkout')
def checkout():
    data = request.get_json(force=True, silent=True) or {}

    order_id   = data.get('order_id')
    user_id    = data.get('user_id')
    total_cost = data.get('total_cost')
    items      = data.get('items', [])

    if not order_id or user_id is None or total_cost is None:
        abort(400, 'order_id, user_id, and total_cost are required')

    if not is_available():
        abort(503, 'Orchestrator not ready')

    try:
        result = start_checkout(order_id, str(user_id), int(total_cost), items)
    except Exception as exc:
        app.logger.error(f"[checkout] start_checkout raised: {exc}")
        abort(500, 'Internal error starting checkout')

    if isinstance(result, dict) and result.get('reason') == 'error':
        abort(400, 'Failed to start checkout')

    return jsonify(result), 200


@app.get('/health')
def health():
    return jsonify({'status': 'ok', 'available': is_available()})


# ── Startup ────────────────────────────────────────────────────────────────────

if __name__ == '__main__':
    app.logger.setLevel(logging.DEBUG if VERBOSE_LOGS else logging.INFO)
    init_streams(app.logger, coord_db)
    app.run(host='0.0.0.0', port=5000, debug=VERBOSE_LOGS)
else:
    server_logger = logging.getLogger('gunicorn.error')
    if server_logger.handlers:
        app.logger.handlers = server_logger.handlers
        app.logger.setLevel(logging.DEBUG if VERBOSE_LOGS else server_logger.level)
    init_streams(app.logger, coord_db)
