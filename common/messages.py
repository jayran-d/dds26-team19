"""
common/messages.py

System-wide message protocol for all services.

This file defines ONLY:
    1. Topic name constants
    2. Message type constants
    3. Order saga state constants
    4. Base message builder
    5. Domain-specific message builders (commands + events)

This file does NOT contain:
    - Saga orchestration logic
    - 2PC logic
    - Any service business logic

Topic ownership:
    stock.commands    <- order service writes, stock service reads
    stock.events      <- stock service writes, order service reads
    payment.commands  <- order service writes, payment service reads
    payment.events    <- payment service writes, order service reads
"""

import os
import uuid
import time

TRANSACTION_MODE = os.getenv("TRANSACTION_MODE", "simple")  # "simple" | "saga" | "2pc"


# ============================================================
# TOPIC NAMES
# ============================================================

STOCK_COMMANDS_TOPIC = "stock.commands"
STOCK_EVENTS_TOPIC = "stock.events"
PAYMENT_COMMANDS_TOPIC = "payment.commands"
PAYMENT_EVENTS_TOPIC = "payment.events"

ALL_TOPICS = [
    STOCK_COMMANDS_TOPIC,
    STOCK_EVENTS_TOPIC,
    PAYMENT_COMMANDS_TOPIC,
    PAYMENT_EVENTS_TOPIC,
]


# ============================================================
# ORDER STATUS STATE MACHINES
# ============================================================
# Stored in Redis as  order:<order_id>:status
# Active protocol controlled by TRANSACTION_MODE env var.
#
# Both classes share PENDING, COMPLETED, and FAILED as
# terminal/entry states — only the in-flight states differ.


class SagaOrderStatus:
    PENDING = "pending"  # checkout not yet called
    RESERVING_STOCK = "reserving_stock"  # RESERVE_STOCK sent, awaiting stock.events
    PROCESSING_PAYMENT = "processing_payment"  # PROCESS_PAYMENT sent, awaiting payment.events
    COMPENSATING = "compensating"  # compensation(s) in flight
    COMPLETED = "completed"  # stock deducted, payment taken — terminal success
    FAILED = "failed"  # compensations done — terminal failure


class TwoPhaseOrderStatus:
    PENDING = "pending"  # checkout not yet called
    PREPARING_STOCK = "preparing_stock"  # PREPARE_STOCK sent, awaiting stock.events
    PREPARING_PAYMENT = "preparing_payment"  # PREPARE_PAYMENT sent, awaiting payment.events
    COMMITTING = "committing"  # COMMIT sent to both services
    ABORTING = "aborting"  # ABORT sent to both services
    COMPLETED = "completed"  # both committed — terminal success
    FAILED = "failed"  # aborted — terminal failure


# ============================================================
# MESSAGE TYPE CONSTANTS
# ============================================================

# -------- Stock Commands (Order → Stock) --------
RESERVE_STOCK = "RESERVE_STOCK"  # deduct stock for all items in an order
RELEASE_STOCK = "RELEASE_STOCK"  # compensation: restore stock for all items

# -------- Stock Events (Stock → Order) --------
STOCK_RESERVED = "STOCK_RESERVED"
STOCK_RESERVATION_FAILED = "STOCK_RESERVATION_FAILED"
STOCK_RELEASED = "STOCK_RELEASED"  # confirmation of compensation

# -------- Payment Commands (Order → Payment) --------
PROCESS_PAYMENT = "PROCESS_PAYMENT"  # charge the user
REFUND_PAYMENT = "REFUND_PAYMENT"  # compensation: refund the user

# -------- Payment Events (Payment → Order) --------
PAYMENT_SUCCESS = "PAYMENT_SUCCESS"
PAYMENT_FAILED = "PAYMENT_FAILED"
PAYMENT_REFUNDED = "PAYMENT_REFUNDED"  # confirmation of compensation

# -------- Stock 2PC Commands (Order → Stock) --------
PREPARE_STOCK = "PREPARE_STOCK"
COMMIT_STOCK = "COMMIT_STOCK"
ABORT_STOCK = "ABORT_STOCK"

# -------- Stock 2PC Events (Stock → Order) --------
STOCK_PREPARED = "STOCK_PREPARED"
STOCK_PREPARE_FAILED = "STOCK_PREPARE_FAILED"
STOCK_COMMITTED = "STOCK_COMMITTED"
STOCK_ABORTED = "STOCK_ABORTED"

# -------- Payment 2PC Commands (Order → Payment) --------
PREPARE_PAYMENT = "PREPARE_PAYMENT"
COMMIT_PAYMENT = "COMMIT_PAYMENT"
ABORT_PAYMENT = "ABORT_PAYMENT"

# -------- Payment 2PC Events (Payment → Order) --------
PAYMENT_PREPARED = "PAYMENT_PREPARED"
PAYMENT_PREPARE_FAILED = "PAYMENT_PREPARE_FAILED"
PAYMENT_COMMITTED = "PAYMENT_COMMITTED"
PAYMENT_ABORTED = "PAYMENT_ABORTED"


# ============================================================
# 2PC Internal State (Coordinator)
# ============================================================

STOCK_UNKNOWN = "STOCK_UNKNOWN"
STOCK_READY = "STOCK_READY"
STOCK_FAILED = "STOCK_FAILED"

PAYMENT_UNKNOWN = "PAYMENT_UNKNOWN"
PAYMENT_READY = "PAYMENT_READY"
PAYMENT_FAILED = "PAYMENT_FAILED"

STOCK_NOT_COMMITTED = "STOCK_NOT_COMMITTED"
STOCK_COMMIT_CONFIRMED = "STOCK_COMMIT_CONFIRMED"
STOCK_ABORT_CONFIRMED = "STOCK_ABORT_CONFIRMED"

PAYMENT_NOT_COMMITTED = "PAYMENT_NOT_COMMITTED"
PAYMENT_COMMIT_CONFIRMED = "PAYMENT_COMMIT_CONFIRMED"
PAYMENT_ABORT_CONFIRMED = "PAYMENT_ABORT_CONFIRMED"

DECISION_NONE = "DECISION_NONE"
DECISION_COMMIT = "DECISION_COMMIT"
DECISION_ABORT = "DECISION_ABORT"


# ============================================================
# BASE MESSAGE BUILDER
# ============================================================


def build_message(tx_id: str, order_id: str, msg_type: str, payload: dict) -> dict:
    """
    Base message structure used by all services.

    Fields:
        message_id  -> Unique ID per message (used for idempotency checks)
        tx_id       -> Transaction ID, shared across all messages in one checkout flow
        order_id    -> The order this transaction belongs to
        type        -> Message type constant (see above)
        timestamp   -> Unix timestamp (ms) for tracing
        payload     -> Domain-specific data
    """
    return {
        "message_id": str(uuid.uuid4()),
        "tx_id": tx_id,
        "order_id": order_id,
        "type": msg_type,
        "timestamp": int(time.time() * 1000),
        "payload": payload,
    }


# ============================================================
# COMMAND BUILDERS
# ============================================================

# -------- Stock Commands --------


def build_reserve_stock(tx_id: str, order_id: str, items: list[dict]) -> dict:
    """
    items format: [{"item_id": "...", "quantity": 2}, ...]

    Stock service will validate ALL items are available before
    deducting any — all or nothing.
    """
    return build_message(tx_id, order_id, RESERVE_STOCK, {"items": items})


def build_release_stock(tx_id: str, order_id: str, items: list[dict]) -> dict:
    """Compensation: restore stock for all items in the order."""
    return build_message(tx_id, order_id, RELEASE_STOCK, {"items": items})


# -------- Payment Commands --------


def build_process_payment(tx_id: str, order_id: str, user_id: str, amount: int) -> dict:
    return build_message(
        tx_id,
        order_id,
        PROCESS_PAYMENT,
        {
            "user_id": user_id,
            "amount": amount,
        },
    )


def build_refund_payment(tx_id: str, order_id: str, user_id: str, amount: int) -> dict:
    """Compensation: refund the user."""
    return build_message(
        tx_id,
        order_id,
        REFUND_PAYMENT,
        {
            "user_id": user_id,
            "amount": amount,
        },
    )


# -------- Stock 2PC Commands --------


def build_prepare_stock(tx_id: str, order_id: str, items: list[dict]) -> dict:
    return build_message(tx_id, order_id, PREPARE_STOCK, {"items": items})


def build_commit_stock(tx_id: str, order_id: str) -> dict:
    return build_message(tx_id, order_id, COMMIT_STOCK, {})


def build_abort_stock(tx_id: str, order_id: str) -> dict:
    return build_message(tx_id, order_id, ABORT_STOCK, {})


# -------- Payment 2PC Commands --------


def build_prepare_payment(tx_id: str, order_id: str, user_id: str, amount: int) -> dict:
    return build_message(
        tx_id,
        order_id,
        PREPARE_PAYMENT,
        {
            "user_id": user_id,
            "amount": amount,
        },
    )


def build_commit_payment(tx_id: str, order_id: str) -> dict:
    return build_message(tx_id, order_id, COMMIT_PAYMENT, {})


def build_abort_payment(tx_id: str, order_id: str) -> dict:
    return build_message(tx_id, order_id, ABORT_PAYMENT, {})


# ============================================================
# EVENT BUILDERS
# ============================================================

# -------- Stock Events --------


def build_stock_reserved(tx_id: str, order_id: str) -> dict:
    return build_message(tx_id, order_id, STOCK_RESERVED, {})


def build_stock_reservation_failed(tx_id: str, order_id: str, reason: str) -> dict:
    return build_message(tx_id, order_id, STOCK_RESERVATION_FAILED, {"reason": reason})


def build_stock_released(tx_id: str, order_id: str) -> dict:
    return build_message(tx_id, order_id, STOCK_RELEASED, {})


def build_stock_prepared(tx_id: str, order_id: str) -> dict:
    return build_message(tx_id, order_id, STOCK_PREPARED, {})


def build_stock_prepare_failed(tx_id: str, order_id: str, reason: str) -> dict:
    return build_message(tx_id, order_id, STOCK_PREPARE_FAILED, {"reason": reason})


def build_stock_committed(tx_id: str, order_id: str) -> dict:
    return build_message(tx_id, order_id, STOCK_COMMITTED, {})


def build_stock_aborted(tx_id: str, order_id: str) -> dict:
    return build_message(tx_id, order_id, STOCK_ABORTED, {})


# -------- Payment Events --------


def build_payment_success(tx_id: str, order_id: str) -> dict:
    return build_message(tx_id, order_id, PAYMENT_SUCCESS, {})


def build_payment_failed(tx_id: str, order_id: str, reason: str) -> dict:
    return build_message(tx_id, order_id, PAYMENT_FAILED, {"reason": reason})


def build_payment_refunded(tx_id: str, order_id: str) -> dict:
    return build_message(tx_id, order_id, PAYMENT_REFUNDED, {})


def build_payment_prepared(tx_id: str, order_id: str) -> dict:
    return build_message(tx_id, order_id, PAYMENT_PREPARED, {})


def build_payment_prepare_failed(tx_id: str, order_id: str, reason: str) -> dict:
    return build_message(tx_id, order_id, PAYMENT_PREPARE_FAILED, {"reason": reason})


def build_payment_committed(tx_id: str, order_id: str) -> dict:
    return build_message(tx_id, order_id, PAYMENT_COMMITTED, {})


def build_payment_aborted(tx_id: str, order_id: str) -> dict:
    return build_message(tx_id, order_id, PAYMENT_ABORTED, {})
