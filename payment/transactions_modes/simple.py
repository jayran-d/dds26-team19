# ============================================================
# COMMAND HANDLERS
# ============================================================
from common.messages import (
    PAYMENT_EVENTS_TOPIC,
    PROCESS_PAYMENT,
    REFUND_PAYMENT,
    build_payment_success,
    build_payment_failed,
    build_payment_refunded,
)

def simple_route_payment(
    producer, logger, msg: dict, msg_type: str
) -> None:
    if msg_type == PROCESS_PAYMENT:
        _handle_process_payment(producer, logger, msg)
    elif msg_type == REFUND_PAYMENT:
        _handle_refund_payment(producer, logger, msg)
    else:
        logger.info(f"[PaymentKafka] Unknown command type: {msg_type!r} — dropping")


def _handle_process_payment(_producer, _logger, msg: dict) -> None:
    tx_id = msg.get("tx_id")
    order_id = msg.get("order_id")
    payload = msg.get("payload", {})
    user_id = str(payload.get("user_id", ""))
    amount = payload.get("amount")

    if not user_id or amount is None:
        _logger.info(f"[PaymentKafka] Invalid PROCESS_PAYMENT payload: {msg}")
        _producer.publish(
            PAYMENT_EVENTS_TOPIC,
            build_payment_failed(tx_id, order_id, "Invalid payment command payload"),
        )
        return

    # Import here to avoid circular imports with app.py
    from app import remove_credit_internal

    success, error, _ = remove_credit_internal(user_id, int(amount))

    if success:
        _logger.info(f"[PaymentKafka] order={order_id} payment success")
        _producer.publish(PAYMENT_EVENTS_TOPIC, build_payment_success(tx_id, order_id))
    else:
        _logger.info(f"[PaymentKafka] order={order_id} payment failed: {error}")
        _producer.publish(
            PAYMENT_EVENTS_TOPIC, build_payment_failed(tx_id, order_id, error)
        )


def _handle_refund_payment(_producer, _logger, msg: dict) -> None:
    tx_id = msg.get("tx_id")
    order_id = msg.get("order_id")
    payload = msg.get("payload", {})
    user_id = str(payload.get("user_id", ""))
    amount = payload.get("amount")

    if not user_id or amount is None:
        _logger.info(f"[PaymentKafka] Invalid REFUND_PAYMENT payload: {msg}")
        return

    from app import add_credit_internal

    success, error, _ = add_credit_internal(user_id, int(amount))

    if success:
        _logger.info(f"[PaymentKafka] order={order_id} refund success")
        _producer.publish(PAYMENT_EVENTS_TOPIC, build_payment_refunded(tx_id, order_id))
    else:
        _logger.info(f"[PaymentKafka] order={order_id} refund failed: {error}")
