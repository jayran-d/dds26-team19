# ============================================================
# REDIS HELPERS
# ============================================================

def set_status(logger, db, order_id: str, status: str) -> None:
    try:
        db.set(f"order:{order_id}:status", status)
    except Exception as e:
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
