import uuid
import redis as redis_module
from common.messages import *

from redis_helpers import get_order, set_status, _2pc_key
from common.kafka_client import KafkaProducerClient

# Internal Redis Order Hash looks like:
#     db.hset(_2pc_key(order_id), mapping={
#         "stock_state": STOCK_UNKNOWN,
#         "payment_state": PAYMENT_UNKNOWN,
#         "decision": DECISION_NONE,
#         "stock_commit_state": STOCK_NOT_COMMITTED,
#         "payment_commit_state": PAYMENT_NOT_COMMITTED,
#     })

# This checks whether a COMMIT or ABORT should be sent. In case one of the participant's hasn't answered, it does nothing.
def _evaluate_2pc(producer, db, logger, order_id):

    state = db.hgetall(_2pc_key(order_id))

    stock_state = state[b"stock_state"].decode()
    payment_state = state[b"payment_state"].decode()
    decision = state[b"decision"].decode()

    if decision != DECISION_NONE:
        return

    # Abort rule

    if stock_state == STOCK_FAILED or payment_state == PAYMENT_FAILED:

        logger.info(f"[Order2PC] order={order_id} ABORT")

        tx_id = db.get(f"order:{order_id}:tx_id")
        if not tx_id:
            logger.warning(f"[Order2PC] order={order_id} missing tx_id, cannot abort")
            return
        tx_id = tx_id.decode()
        db.hset(_2pc_key(order_id), "decision", DECISION_ABORT)
        set_status(logger, db, order_id, TwoPhaseOrderStatus.ABORTING)
        producer.publish(STOCK_COMMANDS_TOPIC, build_abort_stock(tx_id, order_id))
        producer.publish(PAYMENT_COMMANDS_TOPIC, build_abort_payment(tx_id, order_id))
        set_status(logger, db, order_id, TwoPhaseOrderStatus.FAILED)
        return

    # COMMIT RULE
    if stock_state == STOCK_READY and payment_state == PAYMENT_READY:
        logger.info(f"[Order2PC] order={order_id} COMMIT")
        db.hset(_2pc_key(order_id), "decision", DECISION_COMMIT)
        set_status(logger, db, order_id, TwoPhaseOrderStatus.COMMITTING)
        tx_id = db.get(f"order:{order_id}:tx_id").decode()
        producer.publish(STOCK_COMMANDS_TOPIC, build_commit_stock(tx_id, order_id))
        producer.publish(PAYMENT_COMMANDS_TOPIC, build_commit_payment(tx_id, order_id))

def handle_2pc_event(producer, db, logger, msg, participant_key: str, participant_ready_value: str, failure_value: str):
    """
    Generic handler for a 2PC participant event (prepared or failed).

    Args:
        producer: Kafka producer
        db: Redis client
        logger: logger
        msg: incoming Kafka message
        participant_key: Redis hash key for this participant (e.g., 'stock_state', 'payment_state')
        participant_ready_value: value to set if participant is ready (e.g., STOCK_READY)
        failure_value: value to set if participant failed (e.g., STOCK_FAILED)
    """
    order_id = msg.get("order_id")
    tx_id = msg.get("tx_id")
    if not order_id or not tx_id:
        logger.info(f"[Order2PC] Missing order_id or tx_id in message: {msg}")
        return

    event_type = msg.get("type")
    # Determine state based on event type
    if event_type in (STOCK_PREPARED, PAYMENT_PREPARED):
        db.hset(_2pc_key(order_id), participant_key, participant_ready_value)
        logger.info(f"[Order2PC] order={order_id} {event_type}. READY")
    elif event_type in (STOCK_PREPARE_FAILED, PAYMENT_PREPARE_FAILED):
        db.hset(_2pc_key(order_id), participant_key, failure_value)
        logger.info(f"[Order2PC] order={order_id} {event_type}. FAILED")
    else:
        logger.info(f"[Order2PC] Unknown event type: {event_type} for order={order_id}")
        return

    _evaluate_2pc(producer, db, logger, order_id)

def handle_2pc_commit_confirmation(db, logger, msg, participant_commit_key, commit_value):
    order_id = msg.get("order_id")
    if not order_id:
        logger.warning(f"[Order2PC] Missing order_id in commit message: {msg}")
        return

    db.hset(_2pc_key(order_id), participant_commit_key, commit_value)
    logger.info(f"[Order2PC] order={order_id} {msg.get('type')}")
    # Check if both participants committed to mark COMPLETED
    state = db.hgetall(_2pc_key(order_id))
    stock_commit_state = state.get(b"stock_commit_state", b"").decode()
    payment_commit_state = state.get(b"payment_commit_state", b"").decode()
    logger.info(f"[Order2PC] order={order_id} commit states: stock={stock_commit_state} payment={payment_commit_state}")
    if (stock_commit_state == STOCK_COMMIT_CONFIRMED and
        payment_commit_state == PAYMENT_COMMIT_CONFIRMED):
        from msgspec import msgpack
        from app import OrderValue
        raw = db.get(order_id)
        if raw:
            order_entry = msgpack.decode(raw, type=OrderValue)
            order = order_entry.__class__(
            user_id=order_entry.user_id,
            items=order_entry.items,
            total_cost=order_entry.total_cost,
            paid=True,
        )
            db.set(order_id, msgpack.encode(order))
        set_status(logger, db, order_id, "completed")

# def recover_incomplete_2pc(db, producer, logger):
#     """ OLD!!!!!!!
#     Scan Redis for in-progress 2PC transactions and resume them. 
#     """
#     # Can change this to maybe only check for orders with DECISION_NONE, and only reevaluate these
#     # instead of all orders. We would have to change the PUBLISH msg to come first, and after set the decision in redis
#     for key in db.scan_iter("order:*:2pcstate"):
#         state = db.hgetall(key)
#         order_id = key.decode().split(":")[1]  # order:{order_id}:2pcstate
#         decision = state.get(b"decision", b"DECISION_NONE").decode()
#         tx_id = db.get(f"order:{order_id}:tx_id")
#         if not tx_id:
#             logger.warning(f"[Order2PC-Recover] order={order_id} missing tx_id, skipping")
#             continue
#         tx_id = tx_id.decode()
#         stock_state = state.get(b"stock_state", b"STOCK_UNKNOWN").decode()
#         payment_state = state.get(b"payment_state", b"PAYMENT_UNKNOWN").decode()

#         if decision == DECISION_NONE:
#             logger.info(f"[Order2PC-Recover] order={order_id} in-doubt, evaluating...")
#             _evaluate_2pc(producer, db, logger, order_id)

#         elif decision == DECISION_COMMIT:
#             logger.info(f"[Order2PC-Recover] order={order_id} DECISION_COMMIT, resending commits")
#             # resend commit messages using builders
#             producer.publish(STOCK_COMMANDS_TOPIC, build_commit_stock(tx_id, order_id))
#             producer.publish(PAYMENT_COMMANDS_TOPIC, build_commit_payment(tx_id, order_id))

#         elif decision == DECISION_ABORT:
#             logger.info(f"[Order2PC-Recover] order={order_id} DECISION_ABORT, resending aborts")
#             # resend abort messages using builders
#             producer.publish(STOCK_COMMANDS_TOPIC, build_abort_stock(tx_id, order_id))
#             producer.publish(PAYMENT_COMMANDS_TOPIC, build_abort_payment(tx_id, order_id))
#     return

def recover_incomplete_2pc(db, producer, logger):
    """
    On coordinator restart, scan all in-flight 2PC state and resume:
    - DECISION_NONE  : coordinator crashed before deciding — re-evaluate
    - DECISION_COMMIT: coordinator decided but may not have delivered — resend COMMITs
    - DECISION_ABORT : coordinator decided but may not have delivered — resend ABORTs
    Participants are idempotent via the ledger, so resending is safe.
    """
    for key in db.scan_iter("order:*:2pcstate"):
        state = db.hgetall(key)
        decision = state.get(b"decision", b"").decode()
        order_id = key.decode().split(":")[1]  # order:{order_id}:2pcstate

        tx_id_bytes = db.get(f"order:{order_id}:tx_id")
        if not tx_id_bytes:
            logger.info(f"[Order2PC-Recover] order={order_id} missing tx_id, skipping")
            continue
        tx_id = tx_id_bytes.decode()

        if decision == DECISION_NONE:
            logger.info(f"[Order2PC-Recover] order={order_id} in-doubt, re-evaluating")
            _evaluate_2pc(producer, db, logger, order_id)

        elif decision == DECISION_COMMIT:
            # Resend only to participants that haven't confirmed yet
            stock_cs = state.get(b"stock_commit_state", b"").decode()
            payment_cs = state.get(b"payment_commit_state", b"").decode()
            if stock_cs != STOCK_COMMIT_CONFIRMED:
                logger.info(f"[Order2PC-Recover] order={order_id} resending COMMIT_STOCK")
                producer.publish(STOCK_COMMANDS_TOPIC, build_commit_stock(tx_id, order_id))
            if payment_cs != PAYMENT_COMMIT_CONFIRMED:
                logger.info(f"[Order2PC-Recover] order={order_id} resending COMMIT_PAYMENT")
                producer.publish(PAYMENT_COMMANDS_TOPIC, build_commit_payment(tx_id, order_id))

        elif decision == DECISION_ABORT:
            logger.info(f"[Order2PC-Recover] order={order_id} resending ABORTs")
            producer.publish(STOCK_COMMANDS_TOPIC, build_abort_stock(tx_id, order_id))
            producer.publish(PAYMENT_COMMANDS_TOPIC, build_abort_payment(tx_id, order_id))

def _2pc_start_checkout( 
    producer: KafkaProducerClient,
    db: redis_module.Redis,
    logger,
    order_id: str,
    order_entry) -> None:

    # TODO: publish PREPARE_STOCK + PREPARE_PAYMENT, set status = PREPARING_STOCK
    from collections import defaultdict
    from msgspec import msgpack
    from app import OrderValue

    items_quantities: dict = defaultdict(int)
    for item_id, quantity in order_entry.items:
        items_quantities[item_id] += quantity

    items = [{"item_id": iid, "quantity": qty} for iid, qty in items_quantities.items()]

    tx_id = str(uuid.uuid4())
    db.set(f"order:{order_id}:tx_id", tx_id)

    db.hset(_2pc_key(order_id), mapping={
        "stock_state": STOCK_UNKNOWN,
        "payment_state": PAYMENT_UNKNOWN,
        "decision": DECISION_NONE,
        "stock_commit_state": STOCK_NOT_COMMITTED,
        "payment_commit_state": PAYMENT_NOT_COMMITTED,
    })
    set_status(logger, db, order_id, TwoPhaseOrderStatus.PREPARING_STOCK)

    prepare_stock_msg = build_prepare_stock(tx_id=tx_id, order_id=order_id, items=items)
    producer.publish(STOCK_COMMANDS_TOPIC, prepare_stock_msg)
    logger.info(f"[Order2PC] order={order_id} tx={tx_id} PREPARE_STOCK published")

    raw = get_order(logger, db, order_id)
    if not raw:
        logger.info(f"[Order2PC] order {order_id} not found in Redis")
        return

    order = msgpack.decode(raw, type=OrderValue)
    payment_msg = build_prepare_payment(tx_id, order_id, user_id=order.user_id, amount=order.total_cost)
    producer.publish(PAYMENT_COMMANDS_TOPIC, payment_msg)

    logger.info(f"[Order2PC] order={order_id} tx={tx_id} PREPARE_PAYMENT published - user_id={order.user_id} amount={order.total_cost}")


def _2pc_route_order(producer: KafkaProducerClient,
    db: redis_module.Redis,
    logger,
    msg: dict,
    msg_type: str,) -> None:

    handlers = {
        STOCK_PREPARED:        _2pc_on_stock_prepared,
        STOCK_PREPARE_FAILED:  _2pc_on_stock_prepare_failed,
        STOCK_COMMITTED:       _2pc_on_stock_committed,
        STOCK_ABORTED:         _2pc_on_stock_aborted,
        PAYMENT_PREPARED:      _2pc_on_payment_prepared,
        PAYMENT_PREPARE_FAILED:_2pc_on_payment_prepare_failed,
        PAYMENT_COMMITTED:     _2pc_on_payment_committed,
        PAYMENT_ABORTED:       _2pc_on_payment_aborted,
    }
    handler = handlers.get(msg_type)
    if handler:
        handler(producer, db, logger, msg)
    else:
        logger.info(f"[Route-Order2PC] Unknown event type: {msg_type!r} — dropping")


# ── 2PC Event Handlers ────────────────

# -------- Stock --------
def _2pc_on_stock_prepared(producer, db, logger, msg):
    handle_2pc_event(producer, db, logger, msg, participant_key="stock_state",
                     participant_ready_value=STOCK_READY, failure_value=STOCK_FAILED)

def _2pc_on_stock_prepare_failed(producer, db, logger, msg):
    handle_2pc_event(producer, db, logger, msg, participant_key="stock_state",
                     participant_ready_value=STOCK_READY, failure_value=STOCK_FAILED)

def _2pc_on_stock_committed(producer, db, logger, msg):
    handle_2pc_commit_confirmation(db, logger, msg, participant_commit_key="stock_commit_state",
                                   commit_value=STOCK_COMMIT_CONFIRMED)

def _2pc_on_stock_aborted(producer, db, logger, msg):
    handle_2pc_commit_confirmation(db, logger, msg, participant_commit_key="stock_commit_state",
                                   commit_value=STOCK_ABORTED)

# -------- Payment --------
def _2pc_on_payment_prepared(producer, db, logger, msg):
    handle_2pc_event(producer, db, logger, msg, participant_key="payment_state",
                     participant_ready_value=PAYMENT_READY, failure_value=PAYMENT_FAILED)

def _2pc_on_payment_prepare_failed(producer, db, logger, msg):
    handle_2pc_event(producer, db, logger, msg, participant_key="payment_state",
                     participant_ready_value=PAYMENT_READY, failure_value=PAYMENT_FAILED)

def _2pc_on_payment_committed(producer, db, logger, msg):
    handle_2pc_commit_confirmation(db, logger, msg, participant_commit_key="payment_commit_state",
                                   commit_value=PAYMENT_COMMIT_CONFIRMED)

def _2pc_on_payment_aborted(producer, db, logger, msg):
    handle_2pc_commit_confirmation(db, logger, msg, participant_commit_key="payment_commit_state",
                                   commit_value=PAYMENT_ABORTED)

# def _2pc_on_stock_prepared(producer, db, logger, msg):
#     order_id = msg.get("order_id")
#     tx_id = msg.get("tx_id")
#     if not order_id or not tx_id:
#         logger.warning(f"[Order2PC] Missing order_id or tx_id in message: {msg}")
#         return

#     db.hset(_2pc_key(order_id), "stock_state", STOCK_READY)
#     logger.info(f"[Order2PC] order={order_id} {STOCK_PREPARED}")
#     _evaluate_2pc(producer, db, logger, order_id)

# def _2pc_on_stock_prepare_failed(msg: dict) -> None:
#     # TODO: send ABORT_STOCK, set status = FAILED
#     pass


# def _2pc_on_stock_committed(msg: dict) -> None:
#     # TODO: if payment also committed → set status = COMPLETED
#     pass


# def _2pc_on_stock_aborted(msg: dict) -> None:
#     # TODO: set status = FAILED
#     pass


# def _2pc_on_payment_prepared(msg: dict) -> None:
#     # TODO: if stock also prepared → send COMMIT to both; else wait
#     pass


# def _2pc_on_payment_prepare_failed(msg: dict) -> None:
#     # TODO: send ABORT_PAYMENT + ABORT_STOCK if needed, set status = FAILED
#     pass


# def _2pc_on_payment_committed(msg: dict) -> None:
#     # TODO: if stock also committed → set status = COMPLETED
#     pass


# def _2pc_on_payment_aborted(msg: dict) -> None:
#     # TODO: set status = FAILED
#     pass
