from collections import defaultdict

import redis as redis_module
from msgspec import msgpack

from common.messages import (
    STOCK_EVENTS_TOPIC,
    PREPARE_STOCK,
    COMMIT_STOCK,
    ABORT_STOCK,
    build_stock_reserved,
    build_stock_reservation_failed,
    build_stock_released,
)

import ledger as stock_ledger
from ledger import LedgerState


def _2pc_route_stock(msg: dict, db: redis_module.Redis, publish, logger) -> None:
    msg_type = msg.get("type")
    if msg_type == PREPARE_STOCK:
        _handle_prepare_stock(msg, db, publish, logger)
    elif msg_type == COMMIT_STOCK:
        _handle_commit_stock(msg, db, publish, logger)
    elif msg_type == ABORT_STOCK:
        _handle_abort_stock(msg, db, publish, logger)
    else:
        logger.info(f"[Stock2PC] Unknown command type: {msg_type!r} — dropping")


def _handle_prepare_stock(
    msg: dict,
    db: redis_module.Redis,
    publish,
    logger,
) -> None:
    logger.info(f"[Stock2PC] - Handle-Reserve-Stock XX Not yet implemented\n")
    return
    

def _handle_commit_stock(
    msg: dict,
    db: redis_module.Redis,
    publish,
    logger,
) -> None:
    logger.info(f"[Stock2PC] - Handle-Commit-Stock XX Not yet implemented\n")
    return 


def _handle_abort_stock(
    msg: dict,
    db: redis_module.Redis,
    publish,
    logger,
) -> None:
    logger.info(f"[Stock2PC] - Handle-Abort-Stock XX Not yet implemented\n")
    return
    