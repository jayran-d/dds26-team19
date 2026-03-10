# ============================================================
# 2PC
# ============================================================

def _2pc_start_checkout(order_id: str, order_entry) -> None:
    # TODO: publish PREPARE_STOCK + PREPARE_PAYMENT, set status = PREPARING_STOCK
    pass


def _2pc_route_order(msg: dict, msg_type: str) -> None:
    pass

    # handlers = {
    #     STOCK_PREPARED:        _2pc_on_stock_prepared,
    #     STOCK_PREPARE_FAILED:  _2pc_on_stock_prepare_failed,
    #     STOCK_COMMITTED:       _2pc_on_stock_committed,
    #     STOCK_ABORTED:         _2pc_on_stock_aborted,
    #     PAYMENT_PREPARED:      _2pc_on_payment_prepared,
    #     PAYMENT_PREPARE_FAILED:_2pc_on_payment_prepare_failed,
    #     PAYMENT_COMMITTED:     _2pc_on_payment_committed,
    #     PAYMENT_ABORTED:       _2pc_on_payment_aborted,
    # }
    # handler = handlers.get(msg_type)
    # if handler:
    #     handler(msg)
    # else:
    #     _logger.warning(f"[2PC] Unknown event type: {msg_type!r} — dropping")


def _2pc_on_stock_prepared(msg: dict) -> None:
    # TODO: if payment also prepared → send COMMIT to both; else wait
    pass


def _2pc_on_stock_prepare_failed(msg: dict) -> None:
    # TODO: send ABORT_STOCK, set status = FAILED
    pass


def _2pc_on_stock_committed(msg: dict) -> None:
    # TODO: if payment also committed → set status = COMPLETED
    pass


def _2pc_on_stock_aborted(msg: dict) -> None:
    # TODO: set status = FAILED
    pass


def _2pc_on_payment_prepared(msg: dict) -> None:
    # TODO: if stock also prepared → send COMMIT to both; else wait
    pass


def _2pc_on_payment_prepare_failed(msg: dict) -> None:
    # TODO: send ABORT_PAYMENT + ABORT_STOCK if needed, set status = FAILED
    pass


def _2pc_on_payment_committed(msg: dict) -> None:
    # TODO: if stock also committed → set status = COMPLETED
    pass


def _2pc_on_payment_aborted(msg: dict) -> None:
    # TODO: set status = FAILED
    pass