from collections import defaultdict

import redis as redis_module
import time
import uuid
from msgspec import msgpack

from common.messages import (
    STOCK_EVENTS_TOPIC,
    PREPARE_STOCK,
    STOCK_PREPARE_FAILED,
    STOCK_PREPARED,
    COMMIT_STOCK,
    STOCK_COMMITTED,
    STOCK_COMMIT_CONFIRMED,
    ABORT_STOCK,
    STOCK_ABORTED,
    build_message,
    build_stock_aborted,
    build_stock_committed,
    build_stock_prepare_failed,
    build_stock_prepared
)

import ledger as stock_ledger
from ledger import LedgerState
from common.kafka_client import KafkaProducerClient

LOCK_TTL = 10000  # milliseconds

def _2pc_route_stock(msg: dict, db: redis_module.Redis, producer: KafkaProducerClient, logger) -> None:
    msg_type = msg.get("type")
    if msg_type == PREPARE_STOCK:
        _handle_prepare_stock(msg, db, producer, logger)
    elif msg_type == COMMIT_STOCK:
        _handle_commit_stock(msg, db, producer, logger)
    elif msg_type == ABORT_STOCK:
        _handle_abort_stock(msg, db, producer, logger)
    else:
        logger.info(f"[Stock2PC] Unknown command type: {msg_type!r} — dropping")

# def _handle_prepare_stock(
#     msg: dict,
#     db: redis_module.Redis,
#     producer,
#     logger,
# ) -> None:
#     logger.info(f"[Stock2PC] - Handle-Reserve-Stock XX Not yet implemented\n")
#     return

# def _handle_commit_stock(
#     msg: dict,
#     db: redis_module.Redis,
#     publish,
#     logger,
# ) -> None:
#     logger.info(f"[Stock2PC] - Handle-Commit-Stock XX Not yet implemented\n")
#     return 

# def _handle_abort_stock(
#     msg: dict,
#     db: redis_module.Redis,
#     publish,
#     logger,
# ) -> None:
#     logger.info(f"[Stock2PC] - Handle-Abort-Stock XX Not yet implemented\n")
#     return


# ---------------- PREPARE ----------------
def _handle_prepare_stock(msg, db, producer, logger):
    from app import get_item_from_db
    order_id = msg.get("order_id")
    tx_id = msg.get("tx_id")
    items = msg.get("payload", {}).get("items", [])
    action_type = PREPARE_STOCK

    logger.info(f"[Stock2PC] order={order_id} PREPARING_STOCK\n")

    # Duplicate check via ledger
    entry = stock_ledger.get_entry(db, tx_id, action_type)
    if entry:
        if entry["local_state"] in [LedgerState.APPLIED, LedgerState.REPLIED]:
            producer.publish(STOCK_EVENTS_TOPIC,
                             build_message(tx_id, order_id, entry["response_event_type"], entry["response_payload"]))
            stock_ledger.mark_replied(db, tx_id, action_type)
        return

    logger.info(f"[Stock2PC] order={order_id} DUPLICATE_CHECK_PASSED\n")
    # Attempt to acquire per-item locks
    item_locks = {}
    for item in items:
        lock_key = f"lock:stock:item:{item['item_id']}"
        lock_val = str(uuid.uuid4())
        logger.info(f"[Stock2PC] order={order_id} before db.set in locks\n")
        acquired = db.set(lock_key, lock_val, nx=True, px=LOCK_TTL)
        if not acquired:
            for k, v in item_locks.items():
                if db.get(k) == v.encode():
                    db.delete(k)
            logger.info(f"[Stock2PC] order={order_id} fails during lock acquisition\n")
            producer.publish(STOCK_EVENTS_TOPIC,
                             build_stock_prepare_failed(tx_id, order_id, f"Item {item['item_id']} locked"))
            return
        item_locks[lock_key] = lock_val

    logger.info(f"[Stock2PC] order={order_id} LOCKS_SET\n")

    # Check stock availability (without deducting)
    failed_items = []
    for item in items:
        item_obj = get_item_from_db(item['item_id'])
        qty = item_obj.stock if item_obj else 0
        logger.info(f"[Stock2PC] order={order_id} item {item['item_id']} has {qty} in stock which should be >= {item['quantity']}\n")
        if qty < item['quantity']:
            failed_items.append(item['item_id'])

    if failed_items:
        for k, v in item_locks.items():
            if db.get(k) == v.encode():
                db.delete(k)
        logger.info(f"[Stock2PC] order={order_id} fails after item quantity check\n")
        producer.publish(STOCK_EVENTS_TOPIC,
                         build_stock_prepare_failed(tx_id, order_id, f"Out of stock {failed_items}"))
        return

    # Create ledger entry marking as ready
    snapshot = {"items": items, "item_locks": item_locks}
    stock_ledger.create_entry(db, tx_id, action_type, snapshot)
    stock_ledger.mark_applied(db, tx_id, action_type, "success", STOCK_PREPARED, {})

    # Publish ready event
    producer.publish(STOCK_EVENTS_TOPIC, build_stock_prepared(tx_id, order_id))
    logger.info(f"[Stock2PC] order={order_id} STOCK_PREPARED")


# ---------------- COMMIT ----------------
def _handle_commit_stock(msg, db, producer, logger):
    from app import apply_stock_delta
    order_id = msg.get("order_id")
    tx_id = msg.get("tx_id")

    # Dedup
    entry = stock_ledger.get_entry(db, tx_id, COMMIT_STOCK)
    if entry and entry.get("local_state") in (LedgerState.APPLIED, LedgerState.REPLIED):
        producer.publish(STOCK_EVENTS_TOPIC, build_stock_committed(tx_id, order_id))
        return

    # Read items and locks from the PREPARE entry
    prepare_entry = stock_ledger.get_entry(db, tx_id, PREPARE_STOCK)
    if not prepare_entry or prepare_entry.get("result") != "success":
        logger.info(f"[Stock2PC] COMMIT_STOCK tx={tx_id} -- no successful PREPARE found, ignoring")
        return

    items      = prepare_entry["business_snapshot"]["items"]
    item_locks = prepare_entry["business_snapshot"].get("item_locks", {})

    # Permanently deduct stock
    for item in items:
        success, error, _ = apply_stock_delta(item['item_id'], -item['quantity'])
        if not success:
            logger.error(f"[Stock2PC] COMMIT_STOCK: failed to deduct {item['item_id']}: {error}")

    # Release locks
    for k, v in item_locks.items():
        if db.get(k) == v.encode():
            db.delete(k)

    stock_ledger.create_entry(db, tx_id, COMMIT_STOCK, {})
    stock_ledger.mark_applied(db, tx_id, COMMIT_STOCK, "success", STOCK_COMMITTED, {})
    producer.publish(STOCK_EVENTS_TOPIC, build_stock_committed(tx_id, order_id))
    stock_ledger.mark_replied(db, tx_id, COMMIT_STOCK)
    logger.info(f"[Stock2PC] order={order_id} STOCK_COMMITTED, locks released")


# ---------------- ABORT ----------------
def _handle_abort_stock(msg, db, producer, logger):
    order_id = msg.get("order_id")
    tx_id = msg.get("tx_id")

    # Dedup
    entry = stock_ledger.get_entry(db, tx_id, ABORT_STOCK)
    if entry and entry.get("local_state") in (LedgerState.APPLIED, LedgerState.REPLIED):
        producer.publish(STOCK_EVENTS_TOPIC, build_stock_aborted(tx_id, order_id))
        return

    # Read locks from the PREPARE entry — stock was never touched, just release locks
    prepare_entry = stock_ledger.get_entry(db, tx_id, PREPARE_STOCK)
    if prepare_entry and prepare_entry.get("result") == "success":
        item_locks = prepare_entry["business_snapshot"].get("item_locks", {})
        for k, v in item_locks.items():
                if db.get(k) == v.encode():
                    db.delete(k)
    else:
        logger.info(f"[Stock2PC] ABORT_STOCK tx={tx_id} -- PREPARE did not succeed, nothing to release")

    stock_ledger.create_entry(db, tx_id, ABORT_STOCK, {})
    stock_ledger.mark_applied(db, tx_id, ABORT_STOCK, "success", STOCK_ABORTED, {})
    producer.publish(STOCK_EVENTS_TOPIC, build_stock_aborted(tx_id, order_id))
    stock_ledger.mark_replied(db, tx_id, ABORT_STOCK)
    logger.info(f"[Stock2PC] order={order_id} STOCK_ABORTED, locks released")
    