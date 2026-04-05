"""
orchestrator/protocols/saga/saga.py

Saga orchestrator — runs inside the orchestrator service.

State machine:
    pending
        └─ start_checkout()          → reserving_stock   + RESERVE_STOCK
    reserving_stock
        └─ STOCK_RESERVED            → processing_payment + PROCESS_PAYMENT
        └─ STOCK_RESERVATION_FAILED  → failed
    processing_payment
        └─ PAYMENT_SUCCESS           → completed
        └─ PAYMENT_FAILED            → compensating      + RELEASE_STOCK
    compensating
        └─ STOCK_RELEASED            → failed

Persistence:
    coord_db  — saga records (orchestrator-db)
    order_db  — order:{id}:status writes (order-db, polled by order-service)
"""

import uuid
import redis as redis_module
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

# Module-level order-db reference — set by init_saga()
_order_db: redis_module.Redis | None = None


def init_saga(order_db: redis_module.Redis) -> None:
    global _order_db
    _order_db = order_db


def _write_order_status(order_id: str, status: str) -> None:
    """Write the human-facing status to order-db so order-service can poll it."""
    if _order_db is not None:
        try:
            _order_db.set(f"order:{order_id}:status", status)
        except Exception:
            pass


# ── Start Checkout ─────────────────────────────────────────────────────────────

def saga_start_checkout(
    publish,
    coord_db: redis_module.Redis,
    logger,
    order_id: str,
    user_id: str,
    total_cost: int,
    items: list,
) -> dict:
    tx_id = str(uuid.uuid4())

    # Collapse duplicate item_ids
    item_counts: dict[str, int] = {}
    for item in items:
        if isinstance(item, (list, tuple)) and len(item) == 2:
            iid, qty = str(item[0]), int(item[1])
        elif isinstance(item, dict):
            iid, qty = str(item["item_id"]), int(item["quantity"])
        else:
            continue
        item_counts[iid] = item_counts.get(iid, 0) + qty
    canonical_items = [{"item_id": k, "quantity": v} for k, v in item_counts.items()]

    ok, active_tx_id = saga_record.create_if_no_active(
        db=coord_db,
        tx_id=tx_id,
        order_id=order_id,
        user_id=str(user_id),
        amount=int(total_cost),
        items=canonical_items,
        initial_state=SagaOrderStatus.RESERVING_STOCK,
        last_command_type=RESERVE_STOCK,
        awaiting_event_type=STOCK_RESERVED,
    )

    if not ok:
        if active_tx_id:
            logger.debug(f"[Saga] already in progress order={order_id} active_tx={active_tx_id}")
            return {"started": False, "reason": "already_in_progress", "tx_id": active_tx_id}
        logger.error(f"[Saga] failed to create record for order={order_id}")
        return {"started": False, "reason": "error"}

    # Persist tx pointer and initial status in order-db (for polling)
    if _order_db is not None:
        try:
            pipe = _order_db.pipeline(transaction=False)
            pipe.set(f"order:{order_id}:tx_id", tx_id)
            pipe.set(f"order:{order_id}:status", SagaOrderStatus.RESERVING_STOCK)
            pipe.execute()
        except Exception as exc:
            logger.warning(f"[Saga] order-db status write failed tx={tx_id}: {exc}")

    cmd = build_reserve_stock(tx_id, order_id, canonical_items)
    try:
        publish(STOCK_COMMANDS_TOPIC, cmd)
    except Exception as exc:
        logger.warning(f"[Saga] RESERVE_STOCK publish failed tx={tx_id}: {exc}")

    logger.debug(f"[Saga] started tx={tx_id} order={order_id}")
    return {"started": True, "reason": "started", "tx_id": tx_id}


# ── Event Router ──────────────────────────────────────────────────────────────

def saga_route_order(
    msg: dict,
    coord_db: redis_module.Redis,
    publish,
    logger,
) -> None:
    msg_type   = msg.get("type")
    message_id = msg.get("message_id")
    tx_id      = msg.get("tx_id")
    order_id   = msg.get("order_id")

    if not message_id or not tx_id or not order_id:
        logger.warning(f"[Saga] malformed event: {msg}")
        return

    seen, active_tx_id, record = saga_record.load_event_context(coord_db, message_id, order_id, tx_id)

    if seen:
        logger.debug(f"[Saga] duplicate message_id={message_id} — dropping")
        return

    if active_tx_id != tx_id:
        logger.debug(f"[Saga] stale tx_id={tx_id} order={order_id} — dropping")
        saga_record.mark_seen(coord_db, message_id)
        return

    if not record:
        logger.warning(f"[Saga] no record for tx_id={tx_id} — dropping")
        return

    state = record.get("state")
    if state in (SagaOrderStatus.COMPLETED, SagaOrderStatus.FAILED):
        logger.debug(f"[Saga] event {msg_type} in terminal state={state} — dropping")
        saga_record.mark_seen(coord_db, message_id)
        return

    if msg_type == STOCK_RESERVED:
        _on_stock_reserved(record, coord_db, publish, logger, message_id)
    elif msg_type == STOCK_RESERVATION_FAILED:
        _on_stock_reservation_failed(record, msg, coord_db, logger, message_id)
    elif msg_type == STOCK_RELEASED:
        _on_stock_released(record, coord_db, logger, message_id)
    elif msg_type == PAYMENT_SUCCESS:
        _on_payment_success(record, coord_db, logger, message_id)
    elif msg_type == PAYMENT_FAILED:
        _on_payment_failed(record, msg, coord_db, publish, logger, message_id)
    else:
        logger.warning(f"[Saga] unknown event type={msg_type} — dropping")
        # Unknown type: mark seen so it is not re-delivered forever.
        saga_record.mark_seen(coord_db, message_id)


# ── Handlers ─────────────────────────────────────────────────────────────────

def _on_stock_reserved(record, coord_db, publish, logger, message_id):
    tx_id    = record["tx_id"]
    order_id = record["order_id"]
    user_id  = record["user_id"]
    amount   = record["amount"]

    if record["state"] != SagaOrderStatus.RESERVING_STOCK:
        logger.debug(f"[Saga] STOCK_RESERVED in unexpected state={record['state']} tx={tx_id}")
        return

    saga_record.transition(
        db=coord_db, tx_id=tx_id,
        new_state=SagaOrderStatus.PROCESSING_PAYMENT,
        last_command_type=PROCESS_PAYMENT,
        awaiting_event_type=PAYMENT_SUCCESS,
        needs_stock_comp=True,
        record=record,
        message_id=message_id,
    )
    _write_order_status(order_id, SagaOrderStatus.PROCESSING_PAYMENT)

    publish(PAYMENT_COMMANDS_TOPIC, build_process_payment(tx_id, order_id, user_id, amount))
    logger.debug(f"[Saga] stock reserved → processing payment tx={tx_id}")


def _on_stock_reservation_failed(record, msg, coord_db, logger, message_id):
    tx_id    = record["tx_id"]
    order_id = record["order_id"]
    reason   = msg.get("payload", {}).get("reason", "unknown")

    if record["state"] != SagaOrderStatus.RESERVING_STOCK:
        logger.warning(f"[Saga] STOCK_RESERVATION_FAILED unexpected state={record['state']} tx={tx_id}")
        return

    saga_record.transition(
        db=coord_db, tx_id=tx_id,
        new_state=SagaOrderStatus.FAILED,
        failure_reason=reason,
        reset_timeout=False,
        record=record,
        message_id=message_id,
    )
    _write_order_status(order_id, SagaOrderStatus.FAILED)
    saga_record.clear_active_tx_id(coord_db, order_id, tx_id)
    logger.debug(f"[Saga] stock reservation failed tx={tx_id}: {reason}")


def _on_payment_success(record, coord_db, logger, message_id):
    tx_id    = record["tx_id"]
    order_id = record["order_id"]

    if record["state"] != SagaOrderStatus.PROCESSING_PAYMENT:
        logger.debug(f"[Saga] PAYMENT_SUCCESS unexpected state={record['state']} tx={tx_id}")
        return

    saga_record.transition(
        db=coord_db, tx_id=tx_id,
        new_state=SagaOrderStatus.COMPLETED,
        reset_timeout=False,
        record=record,
        message_id=message_id,
    )
    _write_order_status(order_id, SagaOrderStatus.COMPLETED)
    saga_record.clear_active_tx_id(coord_db, order_id, tx_id)
    logger.debug(f"[Saga] completed tx={tx_id} order={order_id}")


def _on_payment_failed(record, msg, coord_db, publish, logger, message_id):
    tx_id    = record["tx_id"]
    order_id = record["order_id"]
    reason   = msg.get("payload", {}).get("reason", "unknown")
    items    = record.get("items", [])

    if record["state"] != SagaOrderStatus.PROCESSING_PAYMENT:
        logger.debug(f"[Saga] PAYMENT_FAILED unexpected state={record['state']} tx={tx_id}")
        return

    saga_record.transition(
        db=coord_db, tx_id=tx_id,
        new_state=SagaOrderStatus.COMPENSATING,
        last_command_type=RELEASE_STOCK,
        awaiting_event_type=STOCK_RELEASED,
        failure_reason=reason,
        record=record,
        message_id=message_id,
    )
    _write_order_status(order_id, SagaOrderStatus.COMPENSATING)

    publish(STOCK_COMMANDS_TOPIC, build_release_stock(tx_id, order_id, items))
    logger.debug(f"[Saga] payment failed → compensating tx={tx_id}: {reason}")


def _on_stock_released(record, coord_db, logger, message_id):
    tx_id    = record["tx_id"]
    order_id = record["order_id"]

    if record["state"] != SagaOrderStatus.COMPENSATING:
        logger.warning(f"[Saga] STOCK_RELEASED unexpected state={record['state']} tx={tx_id}")
        return

    saga_record.transition(
        db=coord_db, tx_id=tx_id,
        new_state=SagaOrderStatus.FAILED,
        reset_timeout=False,
        record=record,
        message_id=message_id,
    )
    _write_order_status(order_id, SagaOrderStatus.FAILED)
    saga_record.clear_active_tx_id(coord_db, order_id, tx_id)
    logger.debug(f"[Saga] compensation done → failed tx={tx_id}")


# ── Recovery ──────────────────────────────────────────────────────────────────

def recover(coord_db: redis_module.Redis, publish, logger) -> None:
    records = saga_record.get_all_active(coord_db)
    if not records:
        return

    logger.info(f"[Saga] recovery: {len(records)} in-flight Saga(s)")

    for record in records:
        tx_id    = record["tx_id"]
        order_id = record["order_id"]
        state    = record["state"]
        items    = record.get("items", [])
        user_id  = record.get("user_id")
        amount   = record.get("amount")

        logger.info(f"[Saga] recovering tx={tx_id} order={order_id} state={state}")

        if state == SagaOrderStatus.RESERVING_STOCK:
            publish(STOCK_COMMANDS_TOPIC, build_reserve_stock(tx_id, order_id, items))
        elif state == SagaOrderStatus.PROCESSING_PAYMENT:
            publish(PAYMENT_COMMANDS_TOPIC, build_process_payment(tx_id, order_id, user_id, amount))
        elif state == SagaOrderStatus.COMPENSATING:
            publish(STOCK_COMMANDS_TOPIC, build_release_stock(tx_id, order_id, items))


# ── Timeout Scanner ───────────────────────────────────────────────────────────

def check_timeouts(coord_db: redis_module.Redis, publish, logger) -> None:
    timed_out = saga_record.get_timed_out(coord_db)
    for record in timed_out:
        tx_id    = record["tx_id"]
        order_id = record["order_id"]
        state    = record["state"]
        logger.info(f"[Saga] timeout tx={tx_id} order={order_id} state={state}")

        saga_record.transition(db=coord_db, tx_id=tx_id, new_state=state, reset_timeout=True)

        items   = record.get("items", [])
        user_id = record.get("user_id")
        amount  = record.get("amount")

        if state == SagaOrderStatus.RESERVING_STOCK:
            publish(STOCK_COMMANDS_TOPIC, build_reserve_stock(tx_id, order_id, items))
        elif state == SagaOrderStatus.PROCESSING_PAYMENT:
            publish(PAYMENT_COMMANDS_TOPIC, build_process_payment(tx_id, order_id, user_id, amount))
        elif state == SagaOrderStatus.COMPENSATING:
            publish(STOCK_COMMANDS_TOPIC, build_release_stock(tx_id, order_id, items))
