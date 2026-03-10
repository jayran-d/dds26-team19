# def _handle_process_payment(msg: dict) -> None:
#     tx_id    = msg.get("tx_id")
#     order_id = msg.get("order_id")
#     payload  = msg.get("payload", {})
#     user_id  = str(payload.get("user_id", ""))
#     amount   = payload.get("amount")

#     if not user_id or amount is None:
#         _logger.error(f"[PaymentKafka] Invalid PROCESS_PAYMENT payload: {msg}")
#         _publish(build_payment_failed(tx_id, order_id, "Invalid payment command payload"))
#         return

#     # Import here to avoid circular imports with app.py
#     from app import remove_credit_internal

#     success, error, _ = remove_credit_internal(user_id, int(amount))

#     if success:
#         _logger.debug(f"[PaymentKafka] order={order_id} payment success")
#         _publish(build_payment_success(tx_id, order_id))
#     else:
#         _logger.debug(f"[PaymentKafka] order={order_id} payment failed: {error}")
#         _publish(build_payment_failed(tx_id, order_id, error))


# def _handle_refund_payment(msg: dict) -> None:
#     tx_id    = msg.get("tx_id")
#     order_id = msg.get("order_id")
#     payload  = msg.get("payload", {})
#     user_id  = str(payload.get("user_id", ""))
#     amount   = payload.get("amount")

#     if not user_id or amount is None:
#         _logger.error(f"[PaymentKafka] Invalid REFUND_PAYMENT payload: {msg}")
#         return

#     from app import add_credit_internal

#     success, error, _ = add_credit_internal(user_id, int(amount))

#     if success:
#         _logger.debug(f"[PaymentKafka] order={order_id} refund success")
#         _publish(build_payment_refunded(tx_id, order_id))
#     else:
#         _logger.error(f"[PaymentKafka] order={order_id} refund failed: {error}")