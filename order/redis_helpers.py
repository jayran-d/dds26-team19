# ============================================================
# REDIS HELPERS
# ============================================================
from common.messages import (STOCK_COMMANDS_TOPIC, PAYMENT_COMMANDS_TOPIC)


def order_status_key(order_id: str) -> str:
    return f"order:{order_id}:status"


def order_status_channel(order_id: str) -> str:
    return f"order:{order_id}:status:events"


def set_status(logger, db, order_id: str, status: str) -> None:
    try:
        pipe = db.pipeline(transaction=False)
        pipe.set(order_status_key(order_id), status)
        pipe.publish(order_status_channel(order_id), status)
        pipe.execute()
    except Exception as e:
        logger.error(f"[OrderKafka] Failed to set status {status} for {order_id}: {e}")


def get_order(logger, db, order_id: str):
    """Return the raw msgpack bytes for an order from Redis."""
    try:
        return db.get(order_id)
    except Exception as e:
        logger.error(f"[OrderKafka] Failed to get order {order_id}: {e}")
        return None

def _2pc_key(order_id: str) -> str:
    return f"order:{order_id}:2pcstate"

# def _evaluate_2pc(producer, db, logger, order_id):

#     state = db.hgetall(_2pc_key(order_id))

#     stock_state = state[b"stock_state"].decode()
#     payment_state = state[b"payment_state"].decode()
#     decision = state[b"decision"].decode()

#     if decision != DECISION_NONE:
#         return

#     # ------------------------------------------------
#     # Abort rule
#     # ------------------------------------------------

#     if stock_state == STOCK_FAILED or payment_state == PAYMENT_FAILED:

#         logger.info(f"[Order2PC] order={order_id} ABORT")

#         db.hset(_2pc_key(order_id), "decision", DECISION_ABORT)

#         producer.publish(STOCK_COMMANDS_TOPIC, {
#             "type": ABORT_STOCK,
#             "order_id": order_id,
#         })

#         producer.publish(PAYMENT_COMMANDS_TOPIC, {
#             "type": ABORT_PAYMENT,
#             "order_id": order_id,
#         })

#         set_status(logger, db, order_id, "FAILED")
#         return

#     # ------------------------------------------------
#     # Commit rule
#     # ------------------------------------------------

#     if stock_state == STOCK_READY and payment_state == PAYMENT_READY:

#         logger.info(f"[Order2PC] order={order_id} COMMIT")

#         db.hset(_2pc_key(order_id), "decision", DECISION_COMMIT)

#         producer.publish(STOCK_COMMANDS_TOPIC, {
#             "type": COMMIT_STOCK,
#             "order_id": order_id,
#         })

#         producer.publish(PAYMENT_COMMANDS_TOPIC, {
#             "type": COMMIT_PAYMENT,
#             "order_id": order_id,
#         })

# def get_tx_id(db, order_id: str) -> str | None:
#     try:
#         val = db.get(f"order:{order_id}:tx_id")
#         return val.decode() if val else None
#     except Exception:
#         return None
