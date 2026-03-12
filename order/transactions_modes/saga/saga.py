"""
order-service/transaction_modes/saga.py

Saga orchestrator — runs inside the order service.

Responsibilities:
    - start_checkout(): create Saga record, publish first command
    - route_event(): receive stock/payment events, advance state machine
    - recover(): called on startup to replay in-flight Sagas

State machine transitions:
    pending
        └─ start_checkout()          → reserving_stock   + publish RESERVE_STOCK

    reserving_stock
        └─ STOCK_RESERVED            → processing_payment + publish PROCESS_PAYMENT
        └─ STOCK_RESERVATION_FAILED  → failed

    processing_payment
        └─ PAYMENT_SUCCESS           → completed
        └─ PAYMENT_FAILED            → compensating      + publish RELEASE_STOCK

    compensating
        └─ STOCK_RELEASED            → failed

Design rules:
    - transition() is always called BEFORE publish() so crash-before-publish is recoverable
    - every incoming event is checked for dedup (is_seen) and staleness (is_stale)
    - compensation flags are set the moment a step succeeds so recovery knows what to undo
"""

import uuid
import redis as redis_module
from msgspec import msgpack

from . import saga_record
from common.messages import (
    SagaOrderStatus,
    STOCK_RESERVED,
    STOCK_RESERVATION_FAILED,
    STOCK_RELEASED,
    PAYMENT_SUCCESS,
    PAYMENT_FAILED,
    RESERVE_STOCK,
    RELEASE_STOCK,
    PROCESS_PAYMENT,
    STOCK_COMMANDS_TOPIC,
    PAYMENT_COMMANDS_TOPIC,
    build_reserve_stock,
    build_process_payment,
    build_release_stock,
)


# ============================================================
# START CHECKOUT
# ============================================================


# (_producer, _db, _logger, order_id, order_entry)
def saga_start_checkout(
    publish,
    db: redis_module.Redis,
    logger,
    order_id: str,
    order_entry,
) -> dict:
    """
    Start a new Saga only if this order does not already have an active one.

    Return shape:
        {"started": True,  "reason": "started",             "tx_id": "..."}
        {"started": False, "reason": "already_in_progress", "tx_id": "..."}
        {"started": False, "reason": "error"}
    """
    tx_id = str(uuid.uuid4())

    # Collapse duplicate item_ids first so the Saga stores a canonical item list.
    item_counts = {}
    for item_id, quantity in order_entry.items:
        key = str(item_id)
        item_counts[key] = item_counts.get(key, 0) + quantity
    items = [{"item_id": k, "quantity": v} for k, v in item_counts.items()]

    ok, active_tx_id = saga_record.create_if_no_active(
        db=db,
        tx_id=tx_id,
        order_id=order_id,
        user_id=str(order_entry.user_id),
        amount=int(order_entry.total_cost),
        items=items,
        initial_state=SagaOrderStatus.RESERVING_STOCK,
        last_command_type=RESERVE_STOCK,
        awaiting_event_type=STOCK_RESERVED,
    )

    if not ok:
        if active_tx_id:
            logger.info(
                f"[Saga] checkout already in progress for order={order_id} active_tx={active_tx_id}"
            )
            return {
                "started": False,
                "reason": "already_in_progress",
                "tx_id": active_tx_id,
            }

        logger.error(f"[Saga] failed to create Saga record for order={order_id}")
        return {"started": False, "reason": "error"}

    # This is just a convenience pointer for observability / debugging.
    db.set(f"order:{order_id}:tx_id", tx_id)
    _set_status(db, order_id, SagaOrderStatus.RESERVING_STOCK)

    # Important: the Saga record was already durably written before this publish.
    # If publish fails or the service crashes now, recovery can replay RESERVE_STOCK.
    cmd = build_reserve_stock(tx_id, order_id, items)
    publish(STOCK_COMMANDS_TOPIC, cmd)

    logger.info(f"[Saga] started tx={tx_id} order={order_id}")
    return {"started": True, "reason": "started", "tx_id": tx_id}



# ============================================================
# EVENT ROUTER
# ============================================================

def saga_route_order(
    msg: dict,
    db: redis_module.Redis,
    publish,
    logger,
) -> None:
    """
    Called by the order service event loop for every message on
    stock.events and payment.events.
    """
    msg_type = msg.get("type")
    message_id = msg.get("message_id")
    tx_id = msg.get("tx_id")
    order_id = msg.get("order_id")

    if not message_id or not tx_id or not order_id:
        logger.warning(f"[Saga] malformed event: {msg}")
        return

    if saga_record.is_seen(db, message_id):
        logger.debug(f"[Saga] duplicate message_id={message_id} — dropping")
        return

    if saga_record.is_stale(db, order_id, tx_id):
        logger.debug(f"[Saga] stale tx_id={tx_id} for order={order_id} — dropping")
        saga_record.mark_seen(db, message_id)
        return

    record = saga_record.get(db, tx_id)
    if not record:
        logger.warning(f"[Saga] no record found for tx_id={tx_id} — dropping")
        return

    state = record.get("state")

    if state in (SagaOrderStatus.COMPLETED, SagaOrderStatus.FAILED):
        logger.debug(
            f"[Saga] event {msg_type} arrived in terminal state={state} — dropping"
        )
        saga_record.mark_seen(db, message_id)
        return

    if msg_type == STOCK_RESERVED:
        saga_on_stock_reserved(record, msg, db, publish, logger)
    elif msg_type == STOCK_RESERVATION_FAILED:
        saga_on_stock_reservation_failed(record, msg, db, logger)
    elif msg_type == STOCK_RELEASED:
        saga_on_stock_released(record, msg, db, logger)
    elif msg_type == PAYMENT_SUCCESS:
        saga_on_payment_success(record, msg, db, logger)
    elif msg_type == PAYMENT_FAILED:
        saga_on_payment_failed(record, msg, db, publish, logger)
    else:
        logger.warning(f"[Saga] unknown event type={msg_type} — dropping")
        return

    saga_record.mark_seen(db, message_id)


# ============================================================
# EVENT HANDLERS
# ============================================================


def saga_on_stock_reserved(record, msg, db, publish, logger):
    tx_id = record["tx_id"]
    order_id = record["order_id"]
    user_id = record["user_id"]
    amount = record["amount"]

    if record["state"] != SagaOrderStatus.RESERVING_STOCK:
        logger.info(
            f"[Saga] STOCK_RESERVED in unexpected state={record['state']} tx={tx_id}"
        )
        return

    # Stock is now reserved — set compensation flag before proceeding
    # If we crash after this point, recovery knows stock must be released
    saga_record.transition(
        db=db,
        tx_id=tx_id,
        new_state=SagaOrderStatus.PROCESSING_PAYMENT,
        last_command_type=PROCESS_PAYMENT,
        awaiting_event_type=PAYMENT_SUCCESS,
        needs_stock_comp=True,  # stock is reserved — must release if we abort
    )
    _set_status(db, order_id, SagaOrderStatus.PROCESSING_PAYMENT)

    cmd = build_process_payment(tx_id, order_id, user_id, amount)
    publish(PAYMENT_COMMANDS_TOPIC, cmd)
    logger.info(f"[Saga] stock reserved → processing payment tx={tx_id}")


def saga_on_stock_reservation_failed(record, msg, db, logger):
    tx_id = record["tx_id"]
    order_id = record["order_id"]
    reason = msg.get("payload", {}).get("reason", "unknown")

    if record["state"] != SagaOrderStatus.RESERVING_STOCK:
        logger.warning(
            f"[Saga] STOCK_RESERVATION_FAILED in unexpected state={record['state']} tx={tx_id}"
        )
        return

    saga_record.transition(
        db=db,
        tx_id=tx_id,
        new_state=SagaOrderStatus.FAILED,
        failure_reason=reason,
        reset_timeout=False,
    )
    _set_status(db, order_id, SagaOrderStatus.FAILED)

    # Terminal state reached: release the active-order claim so a later retry
    # can start a fresh transaction if the user wants to try again.
    saga_record.clear_active_tx_id(db, order_id, tx_id)

    logger.info(f"[Saga] stock reservation failed tx={tx_id}: {reason}")


def saga_on_payment_success(record, msg, db, logger):
    tx_id = record["tx_id"]
    order_id = record["order_id"]

    if record["state"] != SagaOrderStatus.PROCESSING_PAYMENT:
        logger.info(
            f"[Saga] PAYMENT_SUCCESS in unexpected state={record['state']} tx={tx_id}"
        )
        return

    from app import OrderValue

    raw = db.get(order_id)
    if raw:
        order_entry = msgpack.decode(raw, type=OrderValue)
        updated = order_entry.__class__(
            user_id=order_entry.user_id,
            items=order_entry.items,
            total_cost=order_entry.total_cost,
            paid=True,
        )
        db.set(order_id, msgpack.encode(updated))

    saga_record.transition(
        db=db,
        tx_id=tx_id,
        new_state=SagaOrderStatus.COMPLETED,
        reset_timeout=False,
    )
    _set_status(db, order_id, SagaOrderStatus.COMPLETED)

    # Terminal success: free the active-order slot.
    saga_record.clear_active_tx_id(db, order_id, tx_id)

    logger.info(f"[Saga] completed tx={tx_id} order={order_id}")



def saga_on_payment_failed(record, msg, db, publish, logger):
    tx_id = record["tx_id"]
    order_id = record["order_id"]
    reason = msg.get("payload", {}).get("reason", "unknown")

    if record["state"] != SagaOrderStatus.PROCESSING_PAYMENT:
        logger.info(
            f"[Saga] PAYMENT_FAILED in unexpected state={record['state']} tx={tx_id}"
        )
        return

    items = record.get("items", [])

    # Transition to compensating BEFORE publishing RELEASE_STOCK
    saga_record.transition(
        db=db,
        tx_id=tx_id,
        new_state=SagaOrderStatus.COMPENSATING,
        last_command_type=RELEASE_STOCK,
        awaiting_event_type=STOCK_RELEASED,
        failure_reason=reason,
    )
    _set_status(db, order_id, SagaOrderStatus.COMPENSATING)

    cmd = build_release_stock(tx_id, order_id, items)
    publish(STOCK_COMMANDS_TOPIC, cmd)
    logger.info(f"[Saga] payment failed → compensating tx={tx_id}: {reason}")


def saga_on_stock_released(record, msg, db, logger):
    tx_id = record["tx_id"]
    order_id = record["order_id"]

    if record["state"] != SagaOrderStatus.COMPENSATING:
        logger.warning(
            f"[Saga] STOCK_RELEASED in unexpected state={record['state']} tx={tx_id}"
        )
        return

    saga_record.transition(
        db=db,
        tx_id=tx_id,
        new_state=SagaOrderStatus.FAILED,
        reset_timeout=False,
    )
    _set_status(db, order_id, SagaOrderStatus.FAILED)

    # Compensation finished: the order is no longer active.
    saga_record.clear_active_tx_id(db, order_id, tx_id)

    logger.info(f"[Saga] compensation done → failed tx={tx_id}")



# ============================================================
# RECOVERY
# ============================================================


def recover(db: redis_module.Redis, publish, logger) -> None:
    """
    Called once on order service startup.

    Scans for all in-flight Sagas and replays the last intended command.
    Safe to call because participants are idempotent — duplicate commands
    are handled by their ledger.
    """
    records = saga_record.get_all_active(db)
    if not records:
        return

    logger.info(f"[Saga] recovery: found {len(records)} in-flight Saga(s)")

    for record in records:
        tx_id = record["tx_id"]
        order_id = record["order_id"]
        state = record["state"]
        items = record.get("items", [])
        user_id = record.get("user_id")
        amount = record.get("amount")

        logger.info(f"[Saga] recovering tx={tx_id} order={order_id} state={state}")

        if state == SagaOrderStatus.RESERVING_STOCK:
            # Crashed before or after publishing RESERVE_STOCK — replay it
            cmd = build_reserve_stock(tx_id, order_id, items)
            publish(STOCK_COMMANDS_TOPIC, cmd)

        elif state == SagaOrderStatus.PROCESSING_PAYMENT:
            # Stock was reserved, crashed before or after publishing PROCESS_PAYMENT
            cmd = build_process_payment(tx_id, order_id, user_id, amount)
            publish(PAYMENT_COMMANDS_TOPIC, cmd)

        elif state == SagaOrderStatus.COMPENSATING:
            # Payment failed, crashed before or after publishing RELEASE_STOCK
            cmd = build_release_stock(tx_id, order_id, items)
            publish(STOCK_COMMANDS_TOPIC, cmd)

        # COMPLETED and FAILED are terminal — get_all_active() excludes them


# ============================================================
# TIMEOUT SCANNER
# ============================================================


def check_timeouts(db: redis_module.Redis, publish, logger) -> None:
    """
    Called periodically by a background thread in kafka_worker.py.
    Replays commands for any Saga that has been waiting too long.
    Same replay logic as recover() — participants handle duplicates.
    """
    timed_out = saga_record.get_timed_out(db)
    for record in timed_out:
        tx_id = record["tx_id"]
        order_id = record["order_id"]
        state = record["state"]
        logger.info(
            f"[Saga] timeout detected tx={tx_id} order={order_id} state={state}"
        )

        # Reset timeout before replaying so we don't spam every check cycle
        saga_record.transition(
            db=db,
            tx_id=tx_id,
            new_state=state,  # keep same state
            reset_timeout=True,
        )

        items = record.get("items", [])
        user_id = record.get("user_id")
        amount = record.get("amount")

        if state == SagaOrderStatus.RESERVING_STOCK:
            publish(STOCK_COMMANDS_TOPIC, build_reserve_stock(tx_id, order_id, items))
        elif state == SagaOrderStatus.PROCESSING_PAYMENT:
            publish(PAYMENT_COMMANDS_TOPIC, build_process_payment(tx_id, order_id, user_id, amount))
        elif state == SagaOrderStatus.COMPENSATING:
            publish(STOCK_COMMANDS_TOPIC, build_release_stock(tx_id, order_id, items))


# ============================================================
# INTERNAL HELPERS
# ============================================================


def _set_status(db: redis_module.Redis, order_id: str, status: str) -> None:
    """Write the human-facing order status that GET /orders/status/<id> reads."""
    db.set(f"order:{order_id}:status", status)
