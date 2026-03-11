
from collections import defaultdict

import redis as redis_module
from msgspec import msgpack

from common.messages import (
    STOCK_EVENTS_TOPIC,
    PREPARE_PAYMENT,
    COMMIT_PAYMENT,
    ABORT_PAYMENT,
    build_stock_reserved,
    build_stock_reservation_failed,
    build_stock_released,
)

import ledger as stock_ledger
from ledger import LedgerState

def _2pc_route_payment(publish, db: redis_module.Redis, logger, msg: dict, msg_type) -> None:
    # msg_type = msg.get("type")
    if msg_type == PREPARE_PAYMENT:
        _handle_prepare_payment(msg, db, publish, logger)
    elif msg_type == COMMIT_PAYMENT:
        _handle_commit_payment(msg, db, publish, logger)
    elif msg_type == ABORT_PAYMENT:
        _handle_abort_payment(msg, db, publish, logger)
    else:
        logger.info(f"[Payment2PC] Unknown command type: {msg_type!r} — dropping")


def _handle_prepare_payment(
    msg: dict,
    db: redis_module.Redis,
    publish,
    logger,
) -> None:
    logger.info(f"[Payment2PC] - Handle-Prepare-Payment XX Not yet implemented\n")
    return
    

def _handle_commit_payment(
    msg: dict,
    db: redis_module.Redis,
    publish,
    logger,
) -> None:
    logger.info(f"[Payment2PC] - Handle-Commit-Payment XX Not yet implemented\n")
    return 


def _handle_abort_payment(
    msg: dict,
    db: redis_module.Redis,
    publish,
    logger,
) -> None:
    logger.info(f"[Payment2PC] - Handle-Abort-Payment XX Not yet implemented\n")
    return
    