from collections import defaultdict

import redis as redis_module
import time
import uuid
from msgspec import msgpack

from common.messages import (
    STOCK_EVENTS_TOPIC,
    PREPARE_STOCK,
    COMMIT_STOCK,
    ABORT_STOCK,
    build_message
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

# def _handle_prepare_stock(msg: dict, db, publish, logger) -> None:
#     """
#     2PC PREPARE_STOCK handler with ledger and crash-safe replay.
#     """
#     logger.info(f"[Stock2PC] - Handle-Prepare-Stock\n")
#     order_id = msg.get("order_id")
#     tx_id = msg.get("tx_id")
#     items = msg.get("payload", {}).get("items", [])
#     action_type = PREPARE_STOCK

#     if not order_id or not tx_id or not items:
#         logger.warning(f"[Stock2PC] Missing fields in PREPARE_STOCK msg: {msg}")
#         return

#     # Check ledger for duplicates
#     entry = stock_ledger.get_entry(db, tx_id, action_type)
#     if entry:
#         logger.info(f"[Stock2PC] tx={tx_id} PREPARE_STOCK already ledger state={entry['local_state']}")
#         if entry["local_state"] == stock_ledger.LedgerState.APPLIED:
#             publish(STOCK_EVENTS_TOPIC, build_message(tx_id, order_id, entry["response_event_type"], entry["response_payload"]))
#             stock_ledger.mark_replied(db, tx_id, action_type)
#         return

#     # Reserve stock
#     success = True
#     failed_items = []
#     for item in items:
#         if not stock_ledger.can_reserve(item["item_id"], item["quantity"]):
#             success = False
#             failed_items.append(item["item_id"])

#     snapshot = {"items": items}
#     if not stock_ledger.create_entry(db, tx_id, action_type, snapshot):
#         logger.error(f"[Stock2PC] tx={tx_id} could not create ledger entry")
#         return

#     # Apply effect and publish event
#     if success:
#         for item in items:
#             stock_ledger.reserve(item["item_id"], item["quantity"])
#         stock_ledger.mark_applied(db, tx_id, action_type, "success", "STOCK_PREPARED", {})
#         publish(STOCK_EVENTS_TOPIC, build_message(tx_id, order_id, "STOCK_PREPARED", {}))
#         logger.info(f"[Stock2PC] order={order_id} STOCK_PREPARED")
#     else:
#         reason = f"Out of stock items: {failed_items}"
#         stock_ledger.mark_applied(db, tx_id, action_type, "failure", "STOCK_PREPARE_FAILED", {"reason": reason})
#         publish(STOCK_EVENTS_TOPIC, build_message(tx_id, order_id, "STOCK_PREPARE_FAILED", {"reason": reason}))
#         logger.info(f"[Stock2PC] order={order_id} STOCK_PREPARE_FAILED: {reason}")

#     stock_ledger.mark_replied(db, tx_id, action_type)
    

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
    