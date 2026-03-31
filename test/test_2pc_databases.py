"""
Redis database stop/kill recovery tests for TRANSACTION_MODE=2pc.

Run from the repo root with:

    python3 test/test_2pc_databases.py
"""

from __future__ import annotations

import time
import unittest
import sys
from pathlib import Path

TEST_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = TEST_DIR.parent

if str(TEST_DIR) not in sys.path:
    sys.path.insert(0, str(TEST_DIR))
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import utils as tu
from test_2pc import (
    ORDER_DB_SERVICE,
    ORCHESTRATOR_DB_SERVICE,
    PAYMENT_DB_SERVICE,
    PAYMENT_SERVICE,
    RECOVERY_WAIT,
    STOCK_DB_SERVICE,
    STOCK_SERVICE,
    TwoPC_DockerTestCase,
    _ensure_services_started,
    _get_active_2pc_tx_id,
    _kill_service,
    _start_checkout_in_background,
    _start_service,
    _stop_service,
    _wait_for_2pc_field,
    _wait_for_background_checkout,
    _wait_for_checkout_direct,
)


def _retry_call(fn, *args, timeout: int = 30):
    deadline = time.time() + timeout
    last_exc = None
    while time.time() < deadline:
        try:
            return fn(*args)
        except Exception as exc:  # noqa: BLE001 - retry while containers recover
            last_exc = exc
            time.sleep(0.5)
    if last_exc is not None:
        raise last_exc
    raise AssertionError(f"{fn.__name__} did not succeed within {timeout}s")


class Test2pcDatabaseRecovery(TwoPC_DockerTestCase):
    def test_order_db_restart_recovers_from_preparing_stock(self):
        user = tu.create_user()
        user_id = user["user_id"]
        tu.add_credit_to_user(user_id, 100)

        item = tu.create_item(10)
        item_id = item["item_id"]
        tu.add_stock(item_id, 5)

        order = tu.create_order(user_id)
        order_id = order["order_id"]
        tu.add_item_to_order(order_id, item_id, 1)

        _stop_service(STOCK_SERVICE)

        thread, outcome = _start_checkout_in_background(order_id)
        self.assertTrue(
            _wait_for_2pc_field(order_id, "payment_state", "PAYMENT_READY"),
            "payment_state did not become PAYMENT_READY before order-db restart",
        )

        _stop_service(ORDER_DB_SERVICE)
        _start_service(ORDER_DB_SERVICE)
        _start_service(STOCK_SERVICE)

        self.assertEqual(_wait_for_checkout_direct(order_id), "completed")
        _wait_for_background_checkout(thread, outcome, allow_request_error=True)
        self.assertEqual(_retry_call(tu.find_item, item_id)["stock"], 4)
        self.assertEqual(_retry_call(tu.find_user, user_id)["credit"], 90)
        self.assertTrue(_retry_call(tu.find_order, order_id)["paid"])

    def test_orchestrator_db_restart_recovers_from_preparing_payment(self):
        user = tu.create_user()
        user_id = user["user_id"]
        tu.add_credit_to_user(user_id, 100)

        item = tu.create_item(10)
        item_id = item["item_id"]
        tu.add_stock(item_id, 5)

        order = tu.create_order(user_id)
        order_id = order["order_id"]
        tu.add_item_to_order(order_id, item_id, 1)

        _stop_service(PAYMENT_SERVICE)

        thread, outcome = _start_checkout_in_background(order_id)
        self.assertTrue(
            _wait_for_2pc_field(order_id, "stock_state", "STOCK_READY"),
            "stock_state did not become STOCK_READY before orchestrator-db restart",
        )

        _stop_service(ORCHESTRATOR_DB_SERVICE)
        _start_service(ORCHESTRATOR_DB_SERVICE)
        _start_service(PAYMENT_SERVICE)

        self.assertEqual(_wait_for_checkout_direct(order_id), "completed")
        _wait_for_background_checkout(thread, outcome, allow_request_error=True)
        self.assertEqual(_retry_call(tu.find_item, item_id)["stock"], 4)
        self.assertEqual(_retry_call(tu.find_user, user_id)["credit"], 90)
        self.assertTrue(_retry_call(tu.find_order, order_id)["paid"])

    def test_stock_db_restart_recovers_from_preparing_stock(self):
        user = tu.create_user()
        user_id = user["user_id"]
        tu.add_credit_to_user(user_id, 100)

        item = tu.create_item(10)
        item_id = item["item_id"]
        tu.add_stock(item_id, 5)

        order = tu.create_order(user_id)
        order_id = order["order_id"]
        tu.add_item_to_order(order_id, item_id, 1)

        _stop_service(STOCK_DB_SERVICE)

        thread, outcome = _start_checkout_in_background(order_id)
        time.sleep(2)
        self.assertTrue(thread.is_alive(), "checkout should still be waiting while stock-db is down")

        _start_service(STOCK_DB_SERVICE)

        self.assertEqual(_wait_for_checkout_direct(order_id), "completed")
        _wait_for_background_checkout(thread, outcome, allow_request_error=True)
        self.assertEqual(_retry_call(tu.find_item, item_id)["stock"], 4)
        self.assertEqual(_retry_call(tu.find_user, user_id)["credit"], 90)
        self.assertTrue(_retry_call(tu.find_order, order_id)["paid"])

    def test_payment_db_restart_recovers_from_preparing_payment(self):
        user = tu.create_user()
        user_id = user["user_id"]
        tu.add_credit_to_user(user_id, 100)

        item = tu.create_item(10)
        item_id = item["item_id"]
        tu.add_stock(item_id, 5)

        order = tu.create_order(user_id)
        order_id = order["order_id"]
        tu.add_item_to_order(order_id, item_id, 1)

        _stop_service(PAYMENT_DB_SERVICE)

        thread, outcome = _start_checkout_in_background(order_id)
        time.sleep(2)
        self.assertTrue(thread.is_alive(), "checkout should still be waiting while payment-db is down")

        _start_service(PAYMENT_DB_SERVICE)

        self.assertEqual(_wait_for_checkout_direct(order_id), "completed")
        _wait_for_background_checkout(thread, outcome, allow_request_error=True)
        self.assertEqual(_retry_call(tu.find_item, item_id)["stock"], 4)
        self.assertEqual(_retry_call(tu.find_user, user_id)["credit"], 90)
        self.assertTrue(_retry_call(tu.find_order, order_id)["paid"])


class Test2pcDatabaseKillRecovery(TwoPC_DockerTestCase):
    def test_orchestrator_db_kill_recovers_from_preparing_stock(self):
        user = tu.create_user()
        user_id = user["user_id"]
        tu.add_credit_to_user(user_id, 100)

        item = tu.create_item(10)
        item_id = item["item_id"]
        tu.add_stock(item_id, 5)

        order = tu.create_order(user_id)
        order_id = order["order_id"]
        tu.add_item_to_order(order_id, item_id, 1)

        _stop_service(STOCK_SERVICE)

        thread, outcome = _start_checkout_in_background(order_id)
        self.assertTrue(
            _wait_for_2pc_field(order_id, "payment_state", "PAYMENT_READY"),
            "payment_state did not become PAYMENT_READY before orchestrator-db kill",
        )

        active_tx_id = _get_active_2pc_tx_id(order_id)
        self.assertTrue(active_tx_id, "missing active 2PC transaction before orchestrator-db kill")

        _kill_service(ORCHESTRATOR_DB_SERVICE)
        _start_service(ORCHESTRATOR_DB_SERVICE)
        _start_service(STOCK_SERVICE)

        self.assertEqual(_wait_for_checkout_direct(order_id), "completed")
        _wait_for_background_checkout(thread, outcome, allow_request_error=True)
        self.assertEqual(_retry_call(tu.find_item, item_id)["stock"], 4)
        self.assertEqual(_retry_call(tu.find_user, user_id)["credit"], 90)
        self.assertEqual(_get_active_2pc_tx_id(order_id), "")

    def test_stock_db_kill_recovers_from_preparing_stock(self):
        user = tu.create_user()
        user_id = user["user_id"]
        tu.add_credit_to_user(user_id, 100)

        item = tu.create_item(10)
        item_id = item["item_id"]
        tu.add_stock(item_id, 5)

        order = tu.create_order(user_id)
        order_id = order["order_id"]
        tu.add_item_to_order(order_id, item_id, 1)

        _stop_service(STOCK_SERVICE)

        thread, outcome = _start_checkout_in_background(order_id)
        self.assertTrue(
            _wait_for_2pc_field(order_id, "payment_state", "PAYMENT_READY"),
            "payment_state did not become PAYMENT_READY before stock-db kill",
        )

        _kill_service(STOCK_DB_SERVICE)
        _start_service(STOCK_DB_SERVICE)
        _start_service(STOCK_SERVICE)

        self.assertEqual(_wait_for_checkout_direct(order_id), "completed")
        _wait_for_background_checkout(thread, outcome, allow_request_error=True)
        self.assertEqual(_retry_call(tu.find_item, item_id)["stock"], 4)
        self.assertEqual(_retry_call(tu.find_user, user_id)["credit"], 90)
        self.assertTrue(_retry_call(tu.find_order, order_id)["paid"])

    def test_payment_db_kill_recovers_from_preparing_payment(self):
        user = tu.create_user()
        user_id = user["user_id"]
        tu.add_credit_to_user(user_id, 100)

        item = tu.create_item(10)
        item_id = item["item_id"]
        tu.add_stock(item_id, 5)

        order = tu.create_order(user_id)
        order_id = order["order_id"]
        tu.add_item_to_order(order_id, item_id, 1)

        _stop_service(PAYMENT_SERVICE)

        thread, outcome = _start_checkout_in_background(order_id)
        self.assertTrue(
            _wait_for_2pc_field(order_id, "stock_state", "STOCK_READY"),
            "stock_state did not become STOCK_READY before payment-db kill",
        )

        _kill_service(PAYMENT_DB_SERVICE)
        _start_service(PAYMENT_DB_SERVICE)
        _start_service(PAYMENT_SERVICE)

        self.assertEqual(_wait_for_checkout_direct(order_id), "completed")
        _wait_for_background_checkout(thread, outcome, allow_request_error=True)
        self.assertEqual(_retry_call(tu.find_item, item_id)["stock"], 4)
        self.assertEqual(_retry_call(tu.find_user, user_id)["credit"], 90)
        self.assertTrue(_retry_call(tu.find_order, order_id)["paid"])


if __name__ == "__main__":
    unittest.main()
