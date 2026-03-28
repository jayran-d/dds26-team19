"""
Redis stop/kill recovery tests for TRANSACTION_MODE=saga.

Run from the repo root with:

    python3 test/test_kafka_saga_databases.py
"""

from __future__ import annotations

import time
import unittest

import utils as tu
from test_kafka_saga import (
    CHECKOUT_TIMEOUT,
    ORDER_DB_SERVICE,
    PAYMENT_DB_SERVICE,
    PAYMENT_EVENTS_TOPIC,
    PAYMENT_SERVICE,
    RECOVERY_WAIT,
    STOCK_DB_SERVICE,
    STOCK_SERVICE,
    SagaDockerTestCase,
    _create_item,
    _create_order,
    _create_user,
    _ensure_services_started,
    _get_order_tx_id,
    _kill_service,
    _publish_to_kafka,
    _retry_setup_call,
    _start_checkout_request,
    _start_service,
    _stop_service,
    build_payment_failed,
    wait_for_checkout,
    wait_for_status,
)


class TestSagaDatabaseRecovery(SagaDockerTestCase):
    def test_order_db_restart_recovers_from_reserving_stock(self):
        user = _create_user()
        user_id = user["user_id"]
        tu.add_credit_to_user(user_id, 100)

        item = _create_item(10)
        item_id = item["item_id"]
        tu.add_stock(item_id, 5)

        order = _create_order(user_id)
        order_id = order["order_id"]
        tu.add_item_to_order(order_id, item_id, 1)

        _stop_service(STOCK_SERVICE)

        thread, _ = _start_checkout_request(order_id)
        self.assertTrue(wait_for_status(order_id, "reserving_stock", timeout=CHECKOUT_TIMEOUT))

        _stop_service(ORDER_DB_SERVICE)
        _start_service(ORDER_DB_SERVICE)
        _start_service(STOCK_SERVICE)

        self.assertEqual(wait_for_checkout(order_id, timeout=CHECKOUT_TIMEOUT + RECOVERY_WAIT), "completed")
        thread.join(5)
        self.assertEqual(tu.find_item(item_id)["stock"], 4)
        self.assertEqual(tu.find_user(user_id)["credit"], 90)
        self.assertTrue(tu.find_order(order_id)["paid"])

    def test_order_db_restart_recovers_from_processing_payment(self):
        user = _create_user()
        user_id = user["user_id"]
        tu.add_credit_to_user(user_id, 100)

        item = _create_item(10)
        item_id = item["item_id"]
        tu.add_stock(item_id, 5)

        order = _create_order(user_id)
        order_id = order["order_id"]
        tu.add_item_to_order(order_id, item_id, 1)

        _stop_service(PAYMENT_SERVICE)

        thread, _ = _start_checkout_request(order_id)
        self.assertTrue(wait_for_status(order_id, "processing_payment", timeout=CHECKOUT_TIMEOUT))

        _stop_service(ORDER_DB_SERVICE)
        _start_service(ORDER_DB_SERVICE)
        _start_service(PAYMENT_SERVICE)

        self.assertEqual(wait_for_checkout(order_id, timeout=CHECKOUT_TIMEOUT + RECOVERY_WAIT), "completed")
        thread.join(5)
        self.assertEqual(tu.find_item(item_id)["stock"], 4)
        self.assertEqual(tu.find_user(user_id)["credit"], 90)
        self.assertTrue(tu.find_order(order_id)["paid"])

    def test_order_db_restart_recovers_from_compensating(self):
        user = _create_user()
        user_id = user["user_id"]
        tu.add_credit_to_user(user_id, 5)

        item = _create_item(10)
        item_id = item["item_id"]
        tu.add_stock(item_id, 5)

        order = _create_order(user_id)
        order_id = order["order_id"]
        tu.add_item_to_order(order_id, item_id, 2)

        try:
            _stop_service(PAYMENT_SERVICE)

            thread, _ = _start_checkout_request(order_id)
            self.assertTrue(wait_for_status(order_id, "processing_payment", timeout=CHECKOUT_TIMEOUT))

            _stop_service(STOCK_SERVICE)

            tx_id = _get_order_tx_id(order_id)
            forced_failure = build_payment_failed(tx_id, order_id, "forced test failure")
            _publish_to_kafka(PAYMENT_EVENTS_TOPIC, forced_failure)

            self.assertTrue(wait_for_status(order_id, "compensating", timeout=CHECKOUT_TIMEOUT))

            _stop_service(ORDER_DB_SERVICE)
            _start_service(ORDER_DB_SERVICE, timeout=RECOVERY_WAIT)
            _start_service(STOCK_SERVICE, timeout=RECOVERY_WAIT)
            _start_service(PAYMENT_SERVICE, timeout=RECOVERY_WAIT)

            self.assertEqual(wait_for_checkout(order_id, timeout=CHECKOUT_TIMEOUT + RECOVERY_WAIT), "failed")
            thread.join(5)
            self.assertEqual(tu.find_item(item_id)["stock"], 5)
            self.assertEqual(tu.find_user(user_id)["credit"], 5)
            self.assertFalse(tu.find_order(order_id)["paid"])
        finally:
            _ensure_services_started()

    def test_stock_db_restart_recovers_from_reserving_stock(self):
        user = _create_user()
        user_id = user["user_id"]
        tu.add_credit_to_user(user_id, 100)

        item = _create_item(10)
        item_id = item["item_id"]
        tu.add_stock(item_id, 5)

        order = _create_order(user_id)
        order_id = order["order_id"]
        tu.add_item_to_order(order_id, item_id, 1)

        _stop_service(STOCK_DB_SERVICE)

        thread, result = _start_checkout_request(order_id)
        self.assertTrue(wait_for_status(order_id, "reserving_stock", timeout=CHECKOUT_TIMEOUT))

        time.sleep(2)
        self.assertTrue(thread.is_alive(), "checkout should still be waiting while stock-db is down")

        _start_service(STOCK_DB_SERVICE)

        self.assertEqual(wait_for_checkout(order_id, timeout=CHECKOUT_TIMEOUT + RECOVERY_WAIT), "completed")
        thread.join(5)
        self.assertEqual(_retry_setup_call(tu.find_item, item_id)["stock"], 4)
        self.assertEqual(_retry_setup_call(tu.find_user, user_id)["credit"], 90)
        self.assertTrue(_retry_setup_call(tu.find_order, order_id)["paid"])

    def test_payment_db_restart_recovers_from_processing_payment(self):
        user = _create_user()
        user_id = user["user_id"]
        tu.add_credit_to_user(user_id, 100)

        item = _create_item(10)
        item_id = item["item_id"]
        tu.add_stock(item_id, 5)

        order = _create_order(user_id)
        order_id = order["order_id"]
        tu.add_item_to_order(order_id, item_id, 1)

        _stop_service(PAYMENT_DB_SERVICE)

        thread, result = _start_checkout_request(order_id)
        self.assertTrue(wait_for_status(order_id, "processing_payment", timeout=CHECKOUT_TIMEOUT))

        time.sleep(2)
        self.assertTrue(thread.is_alive(), "checkout should still be waiting while payment-db is down")

        _start_service(PAYMENT_DB_SERVICE)

        self.assertEqual(wait_for_checkout(order_id, timeout=CHECKOUT_TIMEOUT + RECOVERY_WAIT), "completed")
        thread.join(5)
        self.assertEqual(_retry_setup_call(tu.find_item, item_id)["stock"], 4)
        self.assertEqual(_retry_setup_call(tu.find_user, user_id)["credit"], 90)
        self.assertTrue(_retry_setup_call(tu.find_order, order_id)["paid"])


class TestSagaDatabaseKillRecovery(SagaDockerTestCase):
    def test_order_db_kill_recovers_from_processing_payment(self):
        user = _create_user()
        user_id = user["user_id"]
        tu.add_credit_to_user(user_id, 100)

        item = _create_item(10)
        item_id = item["item_id"]
        tu.add_stock(item_id, 5)

        order = _create_order(user_id)
        order_id = order["order_id"]
        tu.add_item_to_order(order_id, item_id, 1)

        _stop_service(PAYMENT_SERVICE)

        thread, _ = _start_checkout_request(order_id)
        self.assertTrue(wait_for_status(order_id, "processing_payment", timeout=CHECKOUT_TIMEOUT))

        _kill_service(ORDER_DB_SERVICE)
        time.sleep(2)
        _start_service(ORDER_DB_SERVICE)
        _start_service(PAYMENT_SERVICE)

        self.assertEqual(wait_for_checkout(order_id, timeout=CHECKOUT_TIMEOUT + RECOVERY_WAIT), "completed")
        thread.join(5)
        self.assertEqual(tu.find_item(item_id)["stock"], 4)
        self.assertEqual(tu.find_user(user_id)["credit"], 90)
        self.assertTrue(tu.find_order(order_id)["paid"])

    def test_stock_db_kill_recovers_from_reserving_stock(self):
        user = _create_user()
        user_id = user["user_id"]
        tu.add_credit_to_user(user_id, 100)

        item = _create_item(10)
        item_id = item["item_id"]
        tu.add_stock(item_id, 5)

        order = _create_order(user_id)
        order_id = order["order_id"]
        tu.add_item_to_order(order_id, item_id, 1)

        _stop_service(STOCK_SERVICE)

        thread, _ = _start_checkout_request(order_id)
        self.assertTrue(wait_for_status(order_id, "reserving_stock", timeout=CHECKOUT_TIMEOUT))

        _kill_service(STOCK_DB_SERVICE)
        _start_service(STOCK_SERVICE)
        _start_service(STOCK_DB_SERVICE)

        self.assertEqual(wait_for_checkout(order_id, timeout=CHECKOUT_TIMEOUT + RECOVERY_WAIT), "completed")
        thread.join(5)
        self.assertEqual(_retry_setup_call(tu.find_item, item_id)["stock"], 4)
        self.assertEqual(_retry_setup_call(tu.find_user, user_id)["credit"], 90)
        self.assertTrue(_retry_setup_call(tu.find_order, order_id)["paid"])

    def test_payment_db_kill_recovers_from_processing_payment(self):
        user = _create_user()
        user_id = user["user_id"]
        tu.add_credit_to_user(user_id, 100)

        item = _create_item(10)
        item_id = item["item_id"]
        tu.add_stock(item_id, 5)

        order = _create_order(user_id)
        order_id = order["order_id"]
        tu.add_item_to_order(order_id, item_id, 1)

        _stop_service(PAYMENT_SERVICE)

        thread, _ = _start_checkout_request(order_id)
        self.assertTrue(wait_for_status(order_id, "processing_payment", timeout=CHECKOUT_TIMEOUT))

        _kill_service(PAYMENT_DB_SERVICE)
        _start_service(PAYMENT_SERVICE)
        _start_service(PAYMENT_DB_SERVICE)

        self.assertEqual(wait_for_checkout(order_id, timeout=CHECKOUT_TIMEOUT + RECOVERY_WAIT), "completed")
        thread.join(5)
        self.assertEqual(_retry_setup_call(tu.find_item, item_id)["stock"], 4)
        self.assertEqual(_retry_setup_call(tu.find_user, user_id)["credit"], 90)
        self.assertTrue(_retry_setup_call(tu.find_order, order_id)["paid"])


if __name__ == "__main__":
    unittest.main()
