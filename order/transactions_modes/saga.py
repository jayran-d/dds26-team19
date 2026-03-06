# # ============================================================
# # SAGA
# # ============================================================

def saga_start_checkout(order_id: str, order_entry) -> None:
    pass
    # from collections import defaultdict
    # from msgspec import msgpack
    # from app import OrderValue

    # items_quantities: dict = defaultdict(int)
    # for item_id, quantity in order_entry.items:
    #     items_quantities[item_id] += quantity

    # items = [
    #     {"item_id": iid, "quantity": qty}
    #     for iid, qty in items_quantities.items()
    # ]

    # tx_id = str(uuid.uuid4())
    # _db.set(f"order:{order_id}:tx_id", tx_id)
    # _set_status(order_id, SagaOrderStatus.RESERVING_STOCK)

    # msg = build_reserve_stock(tx_id=tx_id, order_id=order_id, items=items)
    # _publish(STOCK_COMMANDS_TOPIC, msg)
    # _logger.debug(f"[Saga] order={order_id} tx={tx_id} RESERVE_STOCK published")


def saga_route(msg: dict, msg_type: str) -> None:
    pass

    # handlers = {
    #     STOCK_RESERVED:           _saga_on_stock_reserved,
    #     STOCK_RESERVATION_FAILED: _saga_on_stock_reservation_failed,
    #     STOCK_RELEASED:           _saga_on_stock_released,
    #     PAYMENT_SUCCESS:          _saga_on_payment_success,
    #     PAYMENT_FAILED:           _saga_on_payment_failed,
    #     PAYMENT_REFUNDED:         _saga_on_payment_refunded,
    # }
    # handler = handlers.get(msg_type)
    # if handler:
    #     handler(msg)
    # else:
    #     _logger.warning(f"[Saga] Unknown event type: {msg_type!r} — dropping")


# def saga_on_stock_reserved(msg: dict) -> None:
#     # TODO: read order from Redis, publish PROCESS_PAYMENT, set status = PROCESSING_PAYMENT
#     pass


# def saga_on_stock_reservation_failed(msg: dict) -> None:
#     # TODO: set status = FAILED (stock untouched, no compensation needed)
#     pass


# def saga_on_stock_released(msg: dict) -> None:
#     # TODO: set status = FAILED (stock compensation complete)
#     pass


# def saga_on_payment_success(msg: dict) -> None:
#     # TODO: mark order paid = True in Redis, set status = COMPLETED
#     pass


# def saga_on_payment_failed(msg: dict) -> None:
#     # TODO: publish RELEASE_STOCK, set status = COMPENSATING
#     pass


# def saga_on_payment_refunded(msg: dict) -> None:
#     # TODO: set status = FAILED (payment compensation complete)
#     pass
