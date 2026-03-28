import os
import time

from common.messages import SagaOrderStatus


TERMINAL_STATUSES = {
    SagaOrderStatus.COMPLETED,
    SagaOrderStatus.FAILED,
}

STATUS_CHANNEL_PREFIX = "order:status:channel:"
STATUS_RECHECK_INTERVAL_SECONDS = float(
    os.getenv("CHECKOUT_STATUS_RECHECK_INTERVAL_SECONDS", "0.5")
)


def _status_channel(order_id: str) -> str:
    return f"{STATUS_CHANNEL_PREFIX}{order_id}"


def notify_status(db, order_id: str, status: str) -> None:
    try:
        db.publish(_status_channel(order_id), status)
    except Exception:
        # Publishing is only a wake-up optimization. wait_for_terminal_status()
        # still re-checks Redis directly so correctness does not depend on this.
        pass


def wait_for_terminal_status(
    db,
    order_id: str,
    get_status,
    timeout_seconds: float,
) -> str | None:
    deadline = time.monotonic() + timeout_seconds
    pubsub = db.pubsub(ignore_subscribe_messages=True)

    try:
        pubsub.subscribe(_status_channel(order_id))

        while True:
            # Re-check Redis on every loop so correctness does not depend on
            # pub/sub delivery or which order-service worker handles the request.
            status = get_status(order_id) or SagaOrderStatus.PENDING
            if status in TERMINAL_STATUSES:
                return status

            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return None

            message = pubsub.get_message(
                timeout=min(remaining, STATUS_RECHECK_INTERVAL_SECONDS)
            )
            if not message:
                continue

            status = message.get("data")
            if isinstance(status, bytes):
                status = status.decode()
            if status in TERMINAL_STATUSES:
                confirmed_status = get_status(order_id) or SagaOrderStatus.PENDING
                if confirmed_status in TERMINAL_STATUSES:
                    return confirmed_status
    finally:
        pubsub.close()

# ============================================================
# REDIS HELPERS
# ============================================================

def set_status(logger, db, order_id: str, status: str) -> None:
    try:
        db.set(f"order:{order_id}:status", status)
        notify_status(db, order_id, status)
    except Exception as e:
        if logger is not None:
            logger.error(f"[OrderKafka] Failed to set status {status} for {order_id}: {e}")


def get_order(logger, db, order_id: str):
    """Return the raw msgpack bytes for an order from Redis."""
    try:
        return db.get(order_id)
    except Exception as e:
        logger.error(f"[OrderKafka] Failed to get order {order_id}: {e}")
        return None


# def get_tx_id(db, order_id: str) -> str | None:
#     try:
#         val = db.get(f"order:{order_id}:tx_id")
#         return val.decode() if val else None
#     except Exception:
#         return None
