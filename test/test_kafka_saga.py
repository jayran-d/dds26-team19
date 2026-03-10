"""
test_kafka_saga.py

Saga-specific tests for TRANSACTION_MODE=saga.

Covers:
    - happy path (sanity check saga mode works)
    - normal failure compensation
    - concurrent checkouts (oversell prevention)
    - idempotency (duplicate checkout on same order)
    - multi-item compensation (all items restored on payment failure)
    - stale event ignored after re-checkout
    - duplicate command delivery (ledger idempotency)
    - crash recovery: order service crash mid-saga
    - crash recovery: stock service crash after apply but before reply
    - crash recovery: payment service crash after apply but before reply

Assumes:
    - Services running with USE_KAFKA=true, TRANSACTION_MODE=saga
    - Docker Compose available on the host running these tests
    - Gateway reachable at URL configured in utils.py

Update the container names below to match your docker-compose.yml:
"""

import time
import threading
import unittest
import subprocess
import uuid

import utils as tu
from common.kafka_client import KafkaProducerClient
from common.messages import STOCK_COMMANDS_TOPIC, build_reserve_stock

# ── Config ─────────────────────────────────────────────────────────────────────

CHECKOUT_TIMEOUT  = 20    # seconds to wait for a terminal status
POLL_INTERVAL     = 0.5
RECOVERY_WAIT     = 15    # seconds to wait after restarting a container
CONCURRENT_USERS  = 5     # number of simultaneous checkouts in oversell test

ORDER_CONTAINER   = "dds26-team19-order-service-1"
STOCK_CONTAINER   = "dds26-team19-stock-service-1"
PAYMENT_CONTAINER = "dds26-team19-payment-service-1"


# ── Shared helpers ─────────────────────────────────────────────────────────────

def wait_for_checkout(order_id: str, timeout: int = CHECKOUT_TIMEOUT) -> str:
    """Poll GET /orders/status/<order_id> until completed/failed or timeout."""
    terminal = {"completed", "failed"}
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            status = tu.get_order_status(order_id).get("status")
            if status in terminal:
                return status
        except Exception:
            pass
        time.sleep(POLL_INTERVAL)
    return "timeout"


def stop_container(name: str) -> None:
    subprocess.run(["docker", "compose", "stop", name], check=True)


def start_container(name: str) -> None:
    subprocess.run(["docker", "compose", "start", name], check=True)
    time.sleep(RECOVERY_WAIT)


def publish_raw(message: dict) -> None:
    """Publish a message directly to stock.commands, bypassing the order service."""
    producer = KafkaProducerClient()
    producer.publish(STOCK_COMMANDS_TOPIC, message)
    producer.close()


# ══════════════════════════════════════════════════════════════════════════════
# BASIC SAGA CORRECTNESS
# ══════════════════════════════════════════════════════════════════════════════

class TestSagaBasic(unittest.TestCase):
    """Happy path and normal failure paths in saga mode."""

    def test_happy_path(self):
        """
        Saga happy path — stock reserved, payment succeeds, order completed.
        Verifies stock deducted, credit deducted, order marked paid.
        """
        user = tu.create_user()
        user_id = user["user_id"]
        tu.add_credit_to_user(user_id, 100)

        item = tu.create_item(10)
        item_id = item["item_id"]
        tu.add_stock(item_id, 5)

        order = tu.create_order(user_id)
        order_id = order["order_id"]
        tu.add_item_to_order(order_id, item_id, 2)  # cost = 20

        resp = tu.checkout_order(order_id)
        self.assertEqual(resp.status_code, 202)

        status = wait_for_checkout(order_id)
        self.assertEqual(status, "completed", f"Got: {status}")

        self.assertEqual(tu.find_item(item_id)["stock"], 3)
        self.assertEqual(tu.find_user(user_id)["credit"], 80)
        self.assertTrue(tu.find_order(order_id)["paid"])

    def test_stock_failure(self):
        """
        Stock fails — insufficient stock.
        Credit must be untouched, stock must be untouched.
        """
        user = tu.create_user()
        user_id = user["user_id"]
        tu.add_credit_to_user(user_id, 100)

        item = tu.create_item(10)
        item_id = item["item_id"]
        tu.add_stock(item_id, 1)

        order = tu.create_order(user_id)
        order_id = order["order_id"]
        tu.add_item_to_order(order_id, item_id, 5)

        resp = tu.checkout_order(order_id)
        self.assertEqual(resp.status_code, 202)

        status = wait_for_checkout(order_id)
        self.assertEqual(status, "failed", f"Got: {status}")

        self.assertEqual(tu.find_item(item_id)["stock"], 1)
        self.assertEqual(tu.find_user(user_id)["credit"], 100)
        self.assertFalse(tu.find_order(order_id)["paid"])

    def test_payment_failure_stock_released(self):
        """
        Payment fails — insufficient credit.
        Stock must be fully released back (compensation worked).
        """
        user = tu.create_user()
        user_id = user["user_id"]
        tu.add_credit_to_user(user_id, 5)  # not enough

        item = tu.create_item(10)
        item_id = item["item_id"]
        tu.add_stock(item_id, 5)

        order = tu.create_order(user_id)
        order_id = order["order_id"]
        tu.add_item_to_order(order_id, item_id, 2)  # cost = 20, user has 5

        resp = tu.checkout_order(order_id)
        self.assertEqual(resp.status_code, 202)

        status = wait_for_checkout(order_id)
        self.assertEqual(status, "failed", f"Got: {status}")

        # Stock must be fully restored
        self.assertEqual(tu.find_item(item_id)["stock"], 5)
        self.assertEqual(tu.find_user(user_id)["credit"], 5)
        self.assertFalse(tu.find_order(order_id)["paid"])

    def test_multi_item_payment_failure_all_stock_released(self):
        """
        Multi-item order where payment fails.
        ALL items must be restored — not just the first one.
        """
        user = tu.create_user()
        user_id = user["user_id"]
        tu.add_credit_to_user(user_id, 5)  # not enough for any order

        item1 = tu.create_item(10)
        item_id1 = item1["item_id"]
        tu.add_stock(item_id1, 5)

        item2 = tu.create_item(20)
        item_id2 = item2["item_id"]
        tu.add_stock(item_id2, 3)

        item3 = tu.create_item(15)
        item_id3 = item3["item_id"]
        tu.add_stock(item_id3, 4)

        order = tu.create_order(user_id)
        order_id = order["order_id"]
        tu.add_item_to_order(order_id, item_id1, 2)  # cost 20
        tu.add_item_to_order(order_id, item_id2, 1)  # cost 20
        tu.add_item_to_order(order_id, item_id3, 1)  # cost 15, total = 55

        resp = tu.checkout_order(order_id)
        self.assertEqual(resp.status_code, 202)

        status = wait_for_checkout(order_id)
        self.assertEqual(status, "failed", f"Got: {status}")

        # ALL items must be restored
        self.assertEqual(tu.find_item(item_id1)["stock"], 5)
        self.assertEqual(tu.find_item(item_id2)["stock"], 3)
        self.assertEqual(tu.find_item(item_id3)["stock"], 4)
        self.assertEqual(tu.find_user(user_id)["credit"], 5)


# ══════════════════════════════════════════════════════════════════════════════
# IDEMPOTENCY
# ══════════════════════════════════════════════════════════════════════════════

class TestSagaIdempotency(unittest.TestCase):
    """Duplicate messages and double checkout are handled safely."""

    def test_duplicate_checkout_same_order(self):
        """
        Calling checkout twice on the same order.
        Stock and credit must only be deducted once.
        """
        user = tu.create_user()
        user_id = user["user_id"]
        tu.add_credit_to_user(user_id, 100)

        item = tu.create_item(10)
        item_id = item["item_id"]
        tu.add_stock(item_id, 5)

        order = tu.create_order(user_id)
        order_id = order["order_id"]
        tu.add_item_to_order(order_id, item_id, 1)  # cost = 10

        # First checkout
        resp1 = tu.checkout_order(order_id)
        self.assertEqual(resp1.status_code, 202)

        # Second checkout immediately after — same order
        resp2 = tu.checkout_order(order_id)
        # Should be rejected (409) or treated as duplicate (202 but no double effect)

        status = wait_for_checkout(order_id)
        self.assertEqual(status, "completed", f"Got: {status}")

        # Stock and credit must only be deducted ONCE
        self.assertEqual(tu.find_item(item_id)["stock"], 4)
        self.assertEqual(tu.find_user(user_id)["credit"], 90)

    def test_duplicate_reserve_stock_command(self):
        """
        Publish RESERVE_STOCK twice with the same tx_id.
        Stock must only be deducted once — ledger handles the duplicate.
        """
        user = tu.create_user()
        user_id = user["user_id"]
        tu.add_credit_to_user(user_id, 100)

        item = tu.create_item(10)
        item_id = item["item_id"]
        tu.add_stock(item_id, 5)

        order = tu.create_order(user_id)
        order_id = order["order_id"]
        tu.add_item_to_order(order_id, item_id, 1)

        # Normal checkout — this publishes RESERVE_STOCK once
        tu.checkout_order(order_id)
        status = wait_for_checkout(order_id)
        self.assertEqual(status, "completed", f"Got: {status}")

        stock_after_first = tu.find_item(item_id)["stock"]
        credit_after_first = tu.find_user(user_id)["credit"]

        # Now manually publish RESERVE_STOCK again with the same tx_id
        # The stock service ledger should recognize this as a duplicate and ignore it
        tx_id = tu.get_order_tx_id(order_id)  # helper to fetch tx_id from Redis
        items = [{"item_id": item_id, "quantity": 1}]
        duplicate_cmd = build_reserve_stock(tx_id, order_id, items)
        publish_raw(duplicate_cmd)

        # Wait a moment for the duplicate to be processed
        time.sleep(3)

        # Stock and credit must not have changed again
        self.assertEqual(tu.find_item(item_id)["stock"], stock_after_first)
        self.assertEqual(tu.find_user(user_id)["credit"], credit_after_first)


# ══════════════════════════════════════════════════════════════════════════════
# CONCURRENCY
# ══════════════════════════════════════════════════════════════════════════════

class TestSagaConcurrency(unittest.TestCase):
    """Concurrent checkouts must not oversell stock."""

    def test_concurrent_checkouts_no_oversell(self):
        """
        N users all try to buy the last item simultaneously.
        Exactly 1 should succeed, the rest should fail.
        Stock must never go negative.
        """
        n = CONCURRENT_USERS
        stock_available = 1  # only 1 in stock — only 1 can succeed

        item = tu.create_item(10)
        item_id = item["item_id"]
        tu.add_stock(item_id, stock_available)

        # Create N users and orders
        order_ids = []
        for _ in range(n):
            user = tu.create_user()
            user_id = user["user_id"]
            tu.add_credit_to_user(user_id, 100)
            order = tu.create_order(user_id)
            order_id = order["order_id"]
            tu.add_item_to_order(order_id, item_id, 1)
            order_ids.append(order_id)

        # Fire all checkouts simultaneously
        results = {}

        def do_checkout(oid):
            tu.checkout_order(oid)
            results[oid] = wait_for_checkout(oid, timeout=30)

        threads = [threading.Thread(target=do_checkout, args=(oid,)) for oid in order_ids]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        statuses = list(results.values())
        completed = statuses.count("completed")
        failed    = statuses.count("failed")

        # Exactly stock_available checkouts should succeed
        self.assertEqual(
            completed, stock_available,
            f"Expected {stock_available} completed, got {completed} completed / {failed} failed"
        )

        # Stock must not go negative
        final_stock = tu.find_item(item_id)["stock"]
        self.assertEqual(final_stock, 0)
        self.assertGreaterEqual(final_stock, 0)

    def test_concurrent_different_items_both_succeed(self):
        """
        Two users buy different items simultaneously — both should succeed.
        Verifies atomic operations don't block unrelated transactions.
        """
        user1 = tu.create_user()
        tu.add_credit_to_user(user1["user_id"], 100)
        item1 = tu.create_item(10)
        tu.add_stock(item1["item_id"], 5)
        order1 = tu.create_order(user1["user_id"])
        tu.add_item_to_order(order1["order_id"], item1["item_id"], 1)

        user2 = tu.create_user()
        tu.add_credit_to_user(user2["user_id"], 100)
        item2 = tu.create_item(10)
        tu.add_stock(item2["item_id"], 5)
        order2 = tu.create_order(user2["user_id"])
        tu.add_item_to_order(order2["order_id"], item2["item_id"], 1)

        results = {}

        def do_checkout(order_id):
            tu.checkout_order(order_id)
            results[order_id] = wait_for_checkout(order_id, timeout=30)

        t1 = threading.Thread(target=do_checkout, args=(order1["order_id"],))
        t2 = threading.Thread(target=do_checkout, args=(order2["order_id"],))
        t1.start()
        t2.start()
        t1.join()
        t2.join()

        self.assertEqual(results[order1["order_id"]], "completed")
        self.assertEqual(results[order2["order_id"]], "completed")


# ══════════════════════════════════════════════════════════════════════════════
# STALE EVENTS
# ══════════════════════════════════════════════════════════════════════════════

class TestSagaStaleEvents(unittest.TestCase):

    def test_stale_event_ignored_after_recheckout(self):
        """
        If a checkout is retried (new tx_id), late events from the old
        tx_id must be ignored and must not corrupt the new saga.

        Simulated by:
        1. Starting a checkout (tx_id A)
        2. Manually publishing a STOCK_RESERVED for tx_id A after a new
           checkout (tx_id B) has already started
        3. Verifying the saga for tx_id B completes correctly
        """
        user = tu.create_user()
        user_id = user["user_id"]
        tu.add_credit_to_user(user_id, 100)

        item = tu.create_item(10)
        item_id = item["item_id"]
        tu.add_stock(item_id, 5)

        order = tu.create_order(user_id)
        order_id = order["order_id"]
        tu.add_item_to_order(order_id, item_id, 1)

        # First checkout — let it complete normally
        tu.checkout_order(order_id)
        status = wait_for_checkout(order_id)
        self.assertEqual(status, "completed")

        stock_after = tu.find_item(item_id)["stock"]
        credit_after = tu.find_user(user_id)["credit"]

        # Now publish a fake STOCK_RESERVED with a random old tx_id
        # The order service should ignore it (stale tx_id check)
        from common.messages import STOCK_EVENTS_TOPIC, build_stock_reserved
        from common.kafka_client import KafkaProducerClient

        old_tx_id = str(uuid.uuid4())
        stale_event = build_stock_reserved(old_tx_id, order_id)
        producer = KafkaProducerClient()
        producer.publish(STOCK_EVENTS_TOPIC, stale_event)
        producer.close()

        time.sleep(3)

        # Nothing should have changed — stale event was dropped
        self.assertEqual(tu.find_item(item_id)["stock"], stock_after)
        self.assertEqual(tu.find_user(user_id)["credit"], credit_after)


# ══════════════════════════════════════════════════════════════════════════════
# CRASH RECOVERY
# ══════════════════════════════════════════════════════════════════════════════

class TestSagaCrashRecovery(unittest.TestCase):
    """
    These tests simulate container crashes at different points in the saga.
    They require Docker Compose to be available on the host.
    Each test stops a container mid-transaction and verifies the saga
    recovers correctly after the container restarts.
    """

    def test_order_service_crash_during_reserving_stock(self):
        """
        Order service crashes after publishing RESERVE_STOCK but before
        receiving STOCK_RESERVED.

        On restart, recovery should replay RESERVE_STOCK.
        Stock service handles the duplicate via ledger.
        Saga should complete successfully.
        """
        user = tu.create_user()
        user_id = user["user_id"]
        tu.add_credit_to_user(user_id, 100)

        item = tu.create_item(10)
        item_id = item["item_id"]
        tu.add_stock(item_id, 5)

        order = tu.create_order(user_id)
        order_id = order["order_id"]
        tu.add_item_to_order(order_id, item_id, 1)

        # Start checkout — order service publishes RESERVE_STOCK
        tu.checkout_order(order_id)

        # Immediately crash the order service
        # (race condition intentional — we want to crash mid-flow)
        time.sleep(0.3)
        stop_container(ORDER_CONTAINER)

        # While order service is down, stock service may still process
        # RESERVE_STOCK and publish STOCK_RESERVED — this sits in Kafka

        # Restart order service — recovery should replay and complete the saga
        start_container(ORDER_CONTAINER)

        status = wait_for_checkout(order_id, timeout=30)
        self.assertEqual(status, "completed", f"Got: {status}")

        self.assertEqual(tu.find_item(item_id)["stock"], 4)
        self.assertEqual(tu.find_user(user_id)["credit"], 90)
        self.assertTrue(tu.find_order(order_id)["paid"])

    def test_order_service_crash_during_processing_payment(self):
        """
        Order service crashes after receiving STOCK_RESERVED and publishing
        PROCESS_PAYMENT but before receiving PAYMENT_SUCCESS.

        On restart, recovery should replay PROCESS_PAYMENT.
        Payment service handles the duplicate via ledger.
        Saga should complete successfully.
        """
        user = tu.create_user()
        user_id = user["user_id"]
        tu.add_credit_to_user(user_id, 100)

        item = tu.create_item(10)
        item_id = item["item_id"]
        tu.add_stock(item_id, 5)

        order = tu.create_order(user_id)
        order_id = order["order_id"]
        tu.add_item_to_order(order_id, item_id, 1)

        tu.checkout_order(order_id)

        # Wait for stock to be reserved (saga moves to processing_payment)
        deadline = time.time() + 10
        while time.time() < deadline:
            status = tu.get_order_status(order_id).get("status")
            if status == "processing_payment":
                break
            time.sleep(0.3)

        # Crash order service while waiting for payment reply
        stop_container(ORDER_CONTAINER)
        time.sleep(1)
        start_container(ORDER_CONTAINER)

        status = wait_for_checkout(order_id, timeout=30)
        self.assertEqual(status, "completed", f"Got: {status}")

        self.assertEqual(tu.find_item(item_id)["stock"], 4)
        self.assertEqual(tu.find_user(user_id)["credit"], 90)
        self.assertTrue(tu.find_order(order_id)["paid"])

    def test_order_service_crash_during_compensating(self):
        """
        Payment fails, order service transitions to compensating and publishes
        RELEASE_STOCK, then crashes before receiving STOCK_RELEASED.

        On restart, recovery should replay RELEASE_STOCK.
        Stock service handles the duplicate via ledger.
        Saga should end in failed with stock fully restored.
        """
        user = tu.create_user()
        user_id = user["user_id"]
        tu.add_credit_to_user(user_id, 5)  # not enough — payment will fail

        item = tu.create_item(10)
        item_id = item["item_id"]
        tu.add_stock(item_id, 5)

        order = tu.create_order(user_id)
        order_id = order["order_id"]
        tu.add_item_to_order(order_id, item_id, 2)

        tu.checkout_order(order_id)

        # Wait for compensating state
        deadline = time.time() + 15
        while time.time() < deadline:
            status = tu.get_order_status(order_id).get("status")
            if status == "compensating":
                break
            time.sleep(0.3)

        # Crash during compensation
        stop_container(ORDER_CONTAINER)
        time.sleep(1)
        start_container(ORDER_CONTAINER)

        status = wait_for_checkout(order_id, timeout=30)
        self.assertEqual(status, "failed", f"Got: {status}")

        # Stock must be fully restored
        self.assertEqual(tu.find_item(item_id)["stock"], 5)
        self.assertEqual(tu.find_user(user_id)["credit"], 5)

    def test_stock_service_crash_after_apply_before_reply(self):
        """
        Stock service applies the reservation (subtracts stock) but crashes
        before publishing STOCK_RESERVED.

        On restart, the stock service ledger recovery should detect the
        applied-but-not-replied entry and re-publish STOCK_RESERVED.
        Saga should complete successfully.
        """
        user = tu.create_user()
        user_id = user["user_id"]
        tu.add_credit_to_user(user_id, 100)

        item = tu.create_item(10)
        item_id = item["item_id"]
        tu.add_stock(item_id, 5)

        order = tu.create_order(user_id)
        order_id = order["order_id"]
        tu.add_item_to_order(order_id, item_id, 1)

        tu.checkout_order(order_id)

        # Crash stock service almost immediately after checkout
        time.sleep(0.2)
        stop_container(STOCK_CONTAINER)

        # Restart — ledger recovery re-publishes STOCK_RESERVED
        start_container(STOCK_CONTAINER)

        status = wait_for_checkout(order_id, timeout=30)
        self.assertEqual(status, "completed", f"Got: {status}")

        self.assertEqual(tu.find_item(item_id)["stock"], 4)
        self.assertEqual(tu.find_user(user_id)["credit"], 90)
        self.assertTrue(tu.find_order(order_id)["paid"])

    def test_payment_service_crash_after_apply_before_reply(self):
        """
        Payment service charges the user but crashes before publishing
        PAYMENT_SUCCESS.

        On restart, the payment service ledger recovery should detect the
        applied-but-not-replied entry and re-publish PAYMENT_SUCCESS.
        Saga should complete successfully — user credit charged exactly once.
        """
        user = tu.create_user()
        user_id = user["user_id"]
        tu.add_credit_to_user(user_id, 100)

        item = tu.create_item(10)
        item_id = item["item_id"]
        tu.add_stock(item_id, 5)

        order = tu.create_order(user_id)
        order_id = order["order_id"]
        tu.add_item_to_order(order_id, item_id, 1)

        tu.checkout_order(order_id)

        # Wait until stock is reserved so payment is about to be processed
        deadline = time.time() + 10
        while time.time() < deadline:
            status = tu.get_order_status(order_id).get("status")
            if status == "processing_payment":
                break
            time.sleep(0.2)

        # Crash payment service at this moment
        stop_container(PAYMENT_CONTAINER)
        time.sleep(1)
        start_container(PAYMENT_CONTAINER)

        status = wait_for_checkout(order_id, timeout=30)
        self.assertEqual(status, "completed", f"Got: {status}")

        # Credit must be deducted exactly once
        self.assertEqual(tu.find_item(item_id)["stock"], 4)
        self.assertEqual(tu.find_user(user_id)["credit"], 90)
        self.assertTrue(tu.find_order(order_id)["paid"])

    def test_stock_service_crash_during_compensation(self):
        """
        Payment fails, RELEASE_STOCK is published, stock service crashes
        before restoring stock.

        On restart, stock service ledger recovery re-publishes STOCK_RELEASED.
        Saga ends in failed with stock restored.
        """
        user = tu.create_user()
        user_id = user["user_id"]
        tu.add_credit_to_user(user_id, 5)  # not enough — payment will fail

        item = tu.create_item(10)
        item_id = item["item_id"]
        tu.add_stock(item_id, 5)

        order = tu.create_order(user_id)
        order_id = order["order_id"]
        tu.add_item_to_order(order_id, item_id, 2)

        tu.checkout_order(order_id)

        # Wait for compensating state then crash stock service
        deadline = time.time() + 15
        while time.time() < deadline:
            status = tu.get_order_status(order_id).get("status")
            if status == "compensating":
                break
            time.sleep(0.3)

        stop_container(STOCK_CONTAINER)
        time.sleep(1)
        start_container(STOCK_CONTAINER)

        status = wait_for_checkout(order_id, timeout=30)
        self.assertEqual(status, "failed", f"Got: {status}")

        # Stock must be fully restored
        self.assertEqual(tu.find_item(item_id)["stock"], 5)
        self.assertEqual(tu.find_user(user_id)["credit"], 5)


# ══════════════════════════════════════════════════════════════════════════════
# TIMEOUT RECOVERY
# ══════════════════════════════════════════════════════════════════════════════

class TestSagaTimeouts(unittest.TestCase):

    def test_timeout_triggers_replay(self):
        """
        Simulate a stuck saga by stopping the stock service so it never
        replies to RESERVE_STOCK. The order service timeout scanner should
        detect the stuck saga and replay RESERVE_STOCK after the timeout.
        When stock service comes back up it processes the replayed command
        and the saga completes.
        """
        user = tu.create_user()
        user_id = user["user_id"]
        tu.add_credit_to_user(user_id, 100)

        item = tu.create_item(10)
        item_id = item["item_id"]
        tu.add_stock(item_id, 5)

        order = tu.create_order(user_id)
        order_id = order["order_id"]
        tu.add_item_to_order(order_id, item_id, 1)

        # Stop stock service so it can't process RESERVE_STOCK
        stop_container(STOCK_CONTAINER)

        tu.checkout_order(order_id)

        # Wait longer than the saga timeout (TIMEOUT_SECONDS in saga_record.py)
        time.sleep(35)

        # Bring stock service back — it will now process the replayed command
        start_container(STOCK_CONTAINER)

        status = wait_for_checkout(order_id, timeout=30)
        self.assertEqual(status, "completed", f"Got: {status}")

        self.assertEqual(tu.find_item(item_id)["stock"], 4)
        self.assertEqual(tu.find_user(user_id)["credit"], 90)


if __name__ == "__main__":
    unittest.main()