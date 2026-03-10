# ============================================================
# SAGA COMMAND HANDLERS
# ============================================================

from collections import defaultdict

import redis as redis_module
from msgspec import msgpack

import ledger as stock_ledger
from ledger import LedgerState

from common.messages import (
    STOCK_EVENTS_TOPIC,
    RESERVE_STOCK,
    RELEASE_STOCK,
    build_stock_reserved,
    build_stock_reservation_failed,
    build_stock_released,
)

def simple_route_stock(_producer, _logger, msg: dict) -> None:
    msg_type = msg.get("type")
    _logger.info(
        f"[StockKafka] command={msg_type} "
        f"order={msg.get('order_id')} tx={msg.get('tx_id')}"
    )

    #these are saga type messages but for now we're using it as the default case.
    if msg_type == RESERVE_STOCK:
        _handle_reserve_stock(_producer, _logger, msg)
    elif msg_type == RELEASE_STOCK:
        _handle_release_stock(_producer, _logger, msg)
    else:
        _logger.info(f"[StockKafka] Unknown command type: {msg_type!r} — dropping")



def _handle_reserve_stock(_producer, _logger, msg: dict) -> None:
    """
    Subtract stock for all items in the order.
    All-or-nothing: validate every item has enough stock before
    subtracting any. If any item fails, nothing is changed.
    """
    from app import apply_stock_delta, get_item_from_db

    tx_id    = msg.get("tx_id")
    order_id = msg.get("order_id")
    items    = msg.get("payload", {}).get("items", [])

    if not items:
        _logger.info(f"[StockKafka] RESERVE_STOCK missing items: {msg}")
        _producer.publish(STOCK_EVENTS_TOPIC, build_stock_reservation_failed(tx_id, order_id, "No items in payload"))
        return

    # ── Validate all items first ───────────────────────────────────────────────
    for entry in items:
        item_id  = str(entry.get("item_id", ""))
        quantity = entry.get("quantity")

        if not item_id or quantity is None:
            _producer.publish(STOCK_EVENTS_TOPIC, build_stock_reservation_failed(tx_id, order_id, "Invalid item payload"))
            return

        item = get_item_from_db(item_id)
        if item is None:
            _producer.publish(STOCK_EVENTS_TOPIC, build_stock_reservation_failed(
                tx_id, order_id, f"Item {item_id} not found"
            ))
            return

        if item.stock < int(quantity):
            _producer.publish(STOCK_EVENTS_TOPIC, build_stock_reservation_failed(
                tx_id, order_id, f"Insufficient stock for item {item_id}"
            ))
            return

    # ── All items validated — now subtract ────────────────────────────────────
    for entry in items:
        item_id  = str(entry.get("item_id"))
        quantity = int(entry.get("quantity"))
        success, error, _ = apply_stock_delta(item_id, -quantity)
        if not success:
            # Extremely unlikely here since we just validated, but handle it anyway.
            _producer.publish(STOCK_EVENTS_TOPIC, build_stock_reservation_failed(tx_id, order_id, error))
            return

    _logger.info(f"[StockKafka] order={order_id} stock reserved for {len(items)} item(s)")
    _producer.publish(STOCK_EVENTS_TOPIC, build_stock_reserved(tx_id, order_id))


def _handle_release_stock(_producer, _logger, msg: dict) -> None:
    """Restore stock for all items — used as compensation on payment failure."""
    from app import apply_stock_delta

    tx_id    = msg.get("tx_id")
    order_id = msg.get("order_id")
    items    = msg.get("payload", {}).get("items", [])

    if not items:
        _logger.info(f"[StockKafka] RELEASE_STOCK missing items: {msg}")
        return

    for entry in items:
        item_id  = str(entry.get("item_id", ""))
        quantity = int(entry.get("quantity", 0))
        success, error, _ = apply_stock_delta(item_id, quantity)
        if not success:
            _logger.info(
                f"[StockKafka] Failed to release stock for item {item_id}: {error}"
            )

    _logger.info(f"[StockKafka] order={order_id} stock released for {len(items)} item(s)")
    _producer.publish(STOCK_EVENTS_TOPIC, build_stock_released(tx_id, order_id))