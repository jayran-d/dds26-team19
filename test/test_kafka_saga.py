"""
Saga integration tests for TRANSACTION_MODE=saga.

Run from the repo root with:

    python3 test/test_kafka_saga.py

Assumptions:
    - Docker Compose stack is already up
    - USE_KAFKA=true
    - TRANSACTION_MODE=saga
    - /orders/checkout/<order_id> is the async 202-based variant on this branch

Coverage:
    - happy path and normal compensation
    - duplicate checkout and duplicate command idempotency
    - stale event safety
    - oversell prevention
    - order-service restart recovery
    - timeout recovery while stock/payment are unavailable

What this does not deterministically cover:
    - the exact "participant applied locally but crashed before reply" line-level
      window. That needs explicit service failpoints or instrumentation.
"""

from __future__ import annotations

import json
import subprocess
import sys
import threading
import time
import unittest
import uuid
from pathlib import Path

import requests


TEST_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = TEST_DIR.parent

if str(TEST_DIR) not in sys.path:
    sys.path.insert(0, str(TEST_DIR))
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import utils as tu
from common.messages import (
    PAYMENT_EVENTS_TOPIC,
    STOCK_COMMANDS_TOPIC,
    STOCK_EVENTS_TOPIC,
    build_payment_failed,
    build_reserve_stock,
    build_stock_reserved,
)


# ── Config ────────────────────────────────────────────────────────────────────

CHECKOUT_TIMEOUT = 45
STATE_WAIT_TIMEOUT = 20
POLL_INTERVAL = 0.2
RECOVERY_WAIT = 60
SETUP_RETRY_TIMEOUT = 30
CONCURRENT_USERS = 5
SAGA_TIMEOUT_SECONDS = 30

ORDER_SERVICE = "order-service"
STOCK_SERVICE = "stock-service"
PAYMENT_SERVICE = "payment-service"
ORDER_DB_SERVICE = "order-db"
KAFKA_SERVICE = "kafka"
GATEWAY_SERVICE = "gateway"
REDIS_PASSWORD = "redis"


# ── Compose helpers ───────────────────────────────────────────────────────────

def _compose(
    *args: str,
    input_text: str | None = None,
    check: bool = True,
) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["docker", "compose", *args],
        cwd=PROJECT_ROOT,
        check=check,
        capture_output=True,
        text=True,
        input=input_text,
    )


def _kill_service(service: str) -> None:
    _compose("kill", service)
    time.sleep(1)


def _stop_service(service: str) -> None:
    _compose("stop", service)
    time.sleep(1)


def _start_service(service: str, timeout: int = RECOVERY_WAIT) -> None:
    # Keep the same container identity during recovery tests.
    _compose("start", service)
    time.sleep(1)
    _wait_for_service_http(service, timeout=timeout)
    _refresh_gateway()


def _ensure_services_started() -> None:
    _compose("start", GATEWAY_SERVICE, ORDER_SERVICE, STOCK_SERVICE, PAYMENT_SERVICE)
    _wait_for_service_http(ORDER_SERVICE)
    _wait_for_service_http(STOCK_SERVICE)
    _wait_for_service_http(PAYMENT_SERVICE)
    _refresh_gateway()
    _wait_for_gateway_routes()


def _wait_for_service_http(service: str, timeout: int = RECOVERY_WAIT) -> None:
    if service == ORDER_SERVICE:
        path = "/status/non-existent-order"
    elif service == STOCK_SERVICE:
        path = "/find/non-existent-item"
    elif service == PAYMENT_SERVICE:
        path = "/find_user/non-existent-user"
    else:
        raise ValueError(f"Unknown service: {service}")

    deadline = time.time() + timeout
    while time.time() < deadline:
        probe = _compose(
            "exec",
            "-T",
            service,
            "python",
            "-c",
            (
                "import sys\n"
                "import urllib.request\n"
                "import urllib.error\n"
                f"url = 'http://127.0.0.1:5000{path}'\n"
                "req = urllib.request.Request(url, method='GET')\n"
                "try:\n"
                "    with urllib.request.urlopen(req, timeout=1) as resp:\n"
                "        code = resp.getcode()\n"
                "except urllib.error.HTTPError as e:\n"
                "    code = e.code\n"
                "except Exception:\n"
                "    sys.exit(1)\n"
                "sys.exit(0 if code < 500 else 1)\n"
            ),
            check=False,
        )
        if probe.returncode == 0:
            return
        time.sleep(0.5)

    raise AssertionError(f"{service} did not become ready within {timeout}s")


def _wait_for_gateway_routes(timeout: int = RECOVERY_WAIT) -> None:
    checks = (
        f"{tu.ORDER_URL}/orders/status/non-existent-order",
        f"{tu.STOCK_URL}/stock/find/non-existent-item",
        f"{tu.PAYMENT_URL}/payment/find_user/non-existent-user",
    )

    deadline = time.time() + timeout
    while time.time() < deadline:
        all_ready = True
        for url in checks:
            try:
                response = requests.get(url, timeout=1)
                if response.status_code >= 500:
                    all_ready = False
                    break
            except requests.RequestException:
                all_ready = False
                break

        if all_ready:
            return

        time.sleep(0.5)

    raise AssertionError(f"gateway routes did not become ready within {timeout}s")


def _refresh_gateway() -> None:
    """
    The gateway uses static nginx upstream resolution. After service container
    restarts, refreshing the gateway makes it reconnect to the current backend
    addresses without changing gateway configuration.
    """
    _compose("restart", GATEWAY_SERVICE)
    time.sleep(1)


def _wait_for_gateway_service(service: str, timeout: int = RECOVERY_WAIT) -> None:
    if service == ORDER_SERVICE:
        url = f"{tu.ORDER_URL}/orders/status/non-existent-order"
    elif service == STOCK_SERVICE:
        url = f"{tu.STOCK_URL}/stock/find/non-existent-item"
    elif service == PAYMENT_SERVICE:
        url = f"{tu.PAYMENT_URL}/payment/find_user/non-existent-user"
    else:
        raise ValueError(f"Unknown service: {service}")

    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            response = requests.get(url, timeout=1)
            if response.status_code < 500:
                return
        except requests.RequestException:
            pass
        time.sleep(0.5)

    raise AssertionError(f"gateway route for {service} did not become ready within {timeout}s")


# ── Saga helpers ──────────────────────────────────────────────────────────────

def wait_for_checkout(order_id: str, timeout: int = CHECKOUT_TIMEOUT) -> str:
    terminal = {"completed", "failed"}
    deadline = time.time() + timeout

    while time.time() < deadline:
        try:
            status = _get_order_status_direct(order_id)
            if status in terminal:
                return status
        except Exception:
            pass
        time.sleep(POLL_INTERVAL)

    return "timeout"


def wait_for_status(order_id: str, expected_status: str, timeout: int = STATE_WAIT_TIMEOUT) -> bool:
    deadline = time.time() + timeout

    while time.time() < deadline:
        try:
            status = _get_order_status_direct(order_id)
            if status == expected_status:
                return True
        except Exception:
            pass
        time.sleep(POLL_INTERVAL)

    return False


def _retry_setup_call(fn, *args, timeout: int = SETUP_RETRY_TIMEOUT):
    deadline = time.time() + timeout
    last_exc: Exception | None = None

    while time.time() < deadline:
        try:
            return fn(*args)
        except Exception as exc:  # noqa: BLE001 - retry helper for transient 502/HTML
            last_exc = exc
            time.sleep(0.5)

    raise AssertionError(
        f"setup call {fn.__name__} did not succeed within {timeout}s: {last_exc}"
    )


def _create_user() -> dict:
    return _retry_setup_call(tu.create_user)


def _create_item(price: int) -> dict:
    return _retry_setup_call(tu.create_item, price)


def _create_order(user_id: str) -> dict:
    return _retry_setup_call(tu.create_order, user_id)


def _get_order_tx_id(order_id: str) -> str:
    result = _compose(
        "exec",
        "-T",
        ORDER_DB_SERVICE,
        "redis-cli",
        "-a",
        REDIS_PASSWORD,
        "GET",
        f"order:{order_id}:tx_id",
    )
    tx_id = result.stdout.strip()
    if not tx_id:
        raise AssertionError(f"No tx_id stored for order {order_id}")
    return tx_id


def _get_order_status_direct(order_id: str) -> str:
    result = _compose(
        "exec",
        "-T",
        ORDER_DB_SERVICE,
        "redis-cli",
        "-a",
        REDIS_PASSWORD,
        "GET",
        f"order:{order_id}:status",
    )
    status = result.stdout.strip()
    return status or "pending"


def _publish_to_kafka(topic: str, message: dict) -> None:
    _compose(
        "exec",
        "-T",
        KAFKA_SERVICE,
        "/opt/kafka/bin/kafka-console-producer.sh",
        "--bootstrap-server",
        "localhost:9092",
        "--topic",
        topic,
        input_text=json.dumps(message) + "\n",
    )


class SagaDockerTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        _ensure_services_started()

    def setUp(self) -> None:
        _ensure_services_started()

    def tearDown(self) -> None:
        _ensure_services_started()


class TestSagaBasic(SagaDockerTestCase):
    def test_happy_path(self):
        user = _create_user()
        user_id = user["user_id"]
        tu.add_credit_to_user(user_id, 100)

        item = _create_item(10)
        item_id = item["item_id"]
        tu.add_stock(item_id, 5)

        order = _create_order(user_id)
        order_id = order["order_id"]
        tu.add_item_to_order(order_id, item_id, 2)

        resp = tu.checkout_order(order_id)
        self.assertEqual(resp.status_code, 202)

        status = wait_for_checkout(order_id)
        self.assertEqual(status, "completed")
        self.assertEqual(tu.find_item(item_id)["stock"], 3)
        self.assertEqual(tu.find_user(user_id)["credit"], 80)
        self.assertTrue(tu.find_order(order_id)["paid"])

    def test_stock_failure(self):
        user = _create_user()
        user_id = user["user_id"]
        tu.add_credit_to_user(user_id, 100)

        item = _create_item(10)
        item_id = item["item_id"]
        tu.add_stock(item_id, 1)

        order = _create_order(user_id)
        order_id = order["order_id"]
        tu.add_item_to_order(order_id, item_id, 5)

        resp = tu.checkout_order(order_id)
        self.assertEqual(resp.status_code, 202)

        status = wait_for_checkout(order_id)
        self.assertEqual(status, "failed")
        self.assertEqual(tu.find_item(item_id)["stock"], 1)
        self.assertEqual(tu.find_user(user_id)["credit"], 100)
        self.assertFalse(tu.find_order(order_id)["paid"])

    def test_payment_failure_releases_stock(self):
        user = _create_user()
        user_id = user["user_id"]
        tu.add_credit_to_user(user_id, 5)

        item = _create_item(10)
        item_id = item["item_id"]
        tu.add_stock(item_id, 5)

        order = _create_order(user_id)
        order_id = order["order_id"]
        tu.add_item_to_order(order_id, item_id, 2)

        resp = tu.checkout_order(order_id)
        self.assertEqual(resp.status_code, 202)

        status = wait_for_checkout(order_id)
        self.assertEqual(status, "failed")
        self.assertEqual(tu.find_item(item_id)["stock"], 5)
        self.assertEqual(tu.find_user(user_id)["credit"], 5)
        self.assertFalse(tu.find_order(order_id)["paid"])


class TestSagaIdempotency(SagaDockerTestCase):
    def test_duplicate_checkout_same_order(self):
        user = _create_user()
        user_id = user["user_id"]
        tu.add_credit_to_user(user_id, 100)

        item = _create_item(10)
        item_id = item["item_id"]
        tu.add_stock(item_id, 5)

        order = _create_order(user_id)
        order_id = order["order_id"]
        tu.add_item_to_order(order_id, item_id, 1)

        resp1 = tu.checkout_order(order_id)
        resp2 = tu.checkout_order(order_id)
        self.assertEqual(resp1.status_code, 202)
        self.assertIn(resp2.status_code, (202, 200))

        status = wait_for_checkout(order_id)
        self.assertEqual(status, "completed")
        self.assertEqual(tu.find_item(item_id)["stock"], 4)
        self.assertEqual(tu.find_user(user_id)["credit"], 90)

    def test_duplicate_reserve_stock_command(self):
        user = _create_user()
        user_id = user["user_id"]
        tu.add_credit_to_user(user_id, 100)

        item = _create_item(10)
        item_id = item["item_id"]
        tu.add_stock(item_id, 5)

        order = _create_order(user_id)
        order_id = order["order_id"]
        tu.add_item_to_order(order_id, item_id, 1)

        tu.checkout_order(order_id)
        status = wait_for_checkout(order_id)
        self.assertEqual(status, "completed")

        stock_after_first = tu.find_item(item_id)["stock"]
        credit_after_first = tu.find_user(user_id)["credit"]

        tx_id = _get_order_tx_id(order_id)
        items = [{"item_id": item_id, "quantity": 1}]
        duplicate_cmd = build_reserve_stock(tx_id, order_id, items)
        _publish_to_kafka(STOCK_COMMANDS_TOPIC, duplicate_cmd)

        time.sleep(3)

        self.assertEqual(tu.find_item(item_id)["stock"], stock_after_first)
        self.assertEqual(tu.find_user(user_id)["credit"], credit_after_first)


class TestSagaConcurrency(SagaDockerTestCase):
    def test_concurrent_checkouts_no_oversell(self):
        item = _create_item(10)
        item_id = item["item_id"]
        tu.add_stock(item_id, 1)

        order_ids = []
        for _ in range(CONCURRENT_USERS):
            user = _create_user()
            user_id = user["user_id"]
            tu.add_credit_to_user(user_id, 100)
            order = _create_order(user_id)
            order_id = order["order_id"]
            tu.add_item_to_order(order_id, item_id, 1)
            order_ids.append(order_id)

        results: dict[str, str] = {}

        def do_checkout(oid: str) -> None:
            tu.checkout_order(oid)
            results[oid] = wait_for_checkout(oid, timeout=CHECKOUT_TIMEOUT)

        threads = [threading.Thread(target=do_checkout, args=(oid,)) for oid in order_ids]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        statuses = list(results.values())
        self.assertEqual(statuses.count("completed"), 1, msg=f"Statuses: {results}")
        self.assertEqual(tu.find_item(item_id)["stock"], 0)


class TestSagaStaleEvents(SagaDockerTestCase):
    def test_stale_event_ignored_after_checkout(self):
        user = _create_user()
        user_id = user["user_id"]
        tu.add_credit_to_user(user_id, 100)

        item = _create_item(10)
        item_id = item["item_id"]
        tu.add_stock(item_id, 5)

        order = _create_order(user_id)
        order_id = order["order_id"]
        tu.add_item_to_order(order_id, item_id, 1)

        tu.checkout_order(order_id)
        status = wait_for_checkout(order_id)
        self.assertEqual(status, "completed")

        stock_after = tu.find_item(item_id)["stock"]
        credit_after = tu.find_user(user_id)["credit"]

        stale_event = build_stock_reserved(str(uuid.uuid4()), order_id)
        _publish_to_kafka(STOCK_EVENTS_TOPIC, stale_event)

        time.sleep(3)

        self.assertEqual(tu.find_item(item_id)["stock"], stock_after)
        self.assertEqual(tu.find_user(user_id)["credit"], credit_after)


class TestSagaRecovery(SagaDockerTestCase):
    def test_order_service_restart_recovers_from_reserving_stock(self):
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

        resp = tu.checkout_order(order_id)
        self.assertEqual(resp.status_code, 202)
        self.assertTrue(wait_for_status(order_id, "reserving_stock"))

        _kill_service(ORDER_SERVICE)
        _start_service(ORDER_SERVICE)
        _start_service(STOCK_SERVICE)

        status = wait_for_checkout(order_id)
        self.assertEqual(status, "completed")
        self.assertEqual(tu.find_item(item_id)["stock"], 4)
        self.assertEqual(tu.find_user(user_id)["credit"], 90)

    def test_order_service_restart_recovers_from_processing_payment(self):
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

        resp = tu.checkout_order(order_id)
        self.assertEqual(resp.status_code, 202)
        self.assertTrue(wait_for_status(order_id, "processing_payment"))

        _kill_service(ORDER_SERVICE)
        _start_service(ORDER_SERVICE)
        _start_service(PAYMENT_SERVICE)

        status = wait_for_checkout(order_id)
        self.assertEqual(status, "completed")
        self.assertEqual(tu.find_item(item_id)["stock"], 4)
        self.assertEqual(tu.find_user(user_id)["credit"], 90)

    def test_order_service_restart_recovers_from_compensating(self):
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

            resp = tu.checkout_order(order_id)
            self.assertEqual(resp.status_code, 202)
            self.assertTrue(wait_for_status(order_id, "processing_payment", timeout=CHECKOUT_TIMEOUT))

            _stop_service(STOCK_SERVICE)

            tx_id = _get_order_tx_id(order_id)
            forced_failure = build_payment_failed(tx_id, order_id, "forced test failure")
            _publish_to_kafka(PAYMENT_EVENTS_TOPIC, forced_failure)

            self.assertTrue(wait_for_status(order_id, "compensating", timeout=CHECKOUT_TIMEOUT))

            _kill_service(ORDER_SERVICE)
            _start_service(ORDER_SERVICE, timeout=RECOVERY_WAIT)
            _start_service(STOCK_SERVICE, timeout=RECOVERY_WAIT)
            _start_service(PAYMENT_SERVICE, timeout=RECOVERY_WAIT)

            status = wait_for_checkout(order_id, timeout=CHECKOUT_TIMEOUT)
            self.assertEqual(status, "failed")
            self.assertEqual(tu.find_item(item_id)["stock"], 5)
            self.assertEqual(tu.find_user(user_id)["credit"], 5)
        finally:
            _ensure_services_started()

    def test_stock_service_timeout_recovery(self):
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

        resp = tu.checkout_order(order_id)
        self.assertEqual(resp.status_code, 202)
        self.assertTrue(wait_for_status(order_id, "reserving_stock"))

        time.sleep(SAGA_TIMEOUT_SECONDS + 5)
        _start_service(STOCK_SERVICE)

        status = wait_for_checkout(order_id)
        self.assertEqual(status, "completed")
        self.assertEqual(tu.find_item(item_id)["stock"], 4)
        self.assertEqual(tu.find_user(user_id)["credit"], 90)

    def test_payment_service_timeout_recovery(self):
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

        resp = tu.checkout_order(order_id)
        self.assertEqual(resp.status_code, 202)
        self.assertTrue(wait_for_status(order_id, "processing_payment"))

        time.sleep(SAGA_TIMEOUT_SECONDS + 5)
        _start_service(PAYMENT_SERVICE)

        status = wait_for_checkout(order_id)
        self.assertEqual(status, "completed")
        self.assertEqual(tu.find_item(item_id)["stock"], 4)
        self.assertEqual(tu.find_user(user_id)["credit"], 90)


if __name__ == "__main__":
    unittest.main()
