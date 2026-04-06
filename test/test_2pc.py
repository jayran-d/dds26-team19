"""
test_2pc.py

Tests for the 2PC checkout mode.

Assumes:
    - Services are running with TRANSACTION_MODE=2pc
    - Gateway is reachable at the URL configured in utils.py
"""

import subprocess
import sys
import threading
import time
import unittest
from functools import lru_cache
from pathlib import Path
from typing import Optional

import requests

TEST_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = TEST_DIR.parent

if str(TEST_DIR) not in sys.path:
    sys.path.insert(0, str(TEST_DIR))
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import utils as tu


# Max seconds to wait for a checkout to reach a terminal state.
CHECKOUT_TIMEOUT = 15
POLL_INTERVAL    = 0.5


def wait_for_checkout(order_id: str, timeout: int = CHECKOUT_TIMEOUT) -> str:
    """
    Poll GET /orders/status/<order_id> until status is 'completed' or 'failed'.
    Returns the final status string, or 'timeout' if deadline exceeded.
    """
    terminal = {"completed", "failed"}
    deadline = time.time() + timeout

    while time.time() < deadline:
        try:
            response = tu.get_order_status(order_id)
            status = response.get("status")
            if status in terminal:
                return status
        except Exception:
            pass
        time.sleep(POLL_INTERVAL)

    return "timeout"


def _start_checkout_in_background(order_id: str) -> tuple[threading.Thread, dict]:
    outcome: dict = {}

    def run() -> None:
        try:
            outcome["response"] = tu.checkout_order(order_id)
        except Exception as exc:
            outcome["error"] = exc

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    return thread, outcome


def _wait_for_background_checkout(
    thread: threading.Thread,
    outcome: dict,
    timeout: int = 30,
):
    thread.join(timeout)
    if thread.is_alive():
        raise AssertionError("background checkout request did not finish in time")
    if "error" in outcome:
        raise AssertionError(f"background checkout raised: {outcome['error']}")
    return outcome.get("response")


class Test2pc(unittest.TestCase):

    def test_checkout_success(self):
        """
        Happy path: user has enough credit, item has enough stock.
        Checkout should complete successfully.
        """
        print("\nRunning test_checkout_success...")
        # Setup user
        user = tu.create_user()
        user_id = user["user_id"]
        tu.add_credit_to_user(user_id, 50)

        # Setup item
        item = tu.create_item(10)
        item_id = item["item_id"]
        tu.add_stock(item_id, 5)

        # Create order with 2 of the item (cost = 20)
        order = tu.create_order(user_id)
        order_id = order["order_id"]
        tu.add_item_to_order(order_id, item_id, 2)

        # Checkout
        checkout_response = tu.checkout_order(order_id)
        self.assertEqual(checkout_response.status_code, 200)

        # Wait for completion
        status = wait_for_checkout(order_id)
        self.assertEqual(status, "completed", f"Expected completed, got: {status}")

        # Verify stock was deducted
        stock = tu.find_item(item_id)["stock"]
        self.assertEqual(stock, 3)

        # Verify credit was deducted
        credit = tu.find_user(user_id)["credit"]
        self.assertEqual(credit, 30)

        # Verify order is marked paid
        order_data = tu.find_order(order_id)
        self.assertTrue(order_data["paid"])

    def test_checkout_insufficient_stock(self):
        """
        Stock failure: item doesn't have enough stock.
        Checkout should fail, credit should be untouched.
        """
        print("\nRunning test_checkout_insufficient_stock...")
        # Setup user with plenty of credit
        user = tu.create_user()
        user_id = user["user_id"]
        tu.add_credit_to_user(user_id, 100)

        # Setup item with only 1 in stock
        item = tu.create_item(10)
        item_id = item["item_id"]
        tu.add_stock(item_id, 1)

        # Create order requesting 5 (more than available)
        order = tu.create_order(user_id)
        order_id = order["order_id"]
        tu.add_item_to_order(order_id, item_id, 5)

        # Checkout
        checkout_response = tu.checkout_order(order_id)
        self.assertEqual(checkout_response.status_code, 400)

        # Wait for failure
        status = wait_for_checkout(order_id)
        self.assertEqual(status, "failed", f"Expected failed, got: {status}")

        # Verify stock is unchanged (nothing was reserved)
        stock = tu.find_item(item_id)["stock"]
        self.assertEqual(stock, 1)

        # Verify credit is untouched
        credit = tu.find_user(user_id)["credit"]
        self.assertEqual(credit, 100)

    def test_checkout_insufficient_credit(self):
        """
        Payment failure: user doesn't have enough credit.
        Checkout should fail and reserved stock should be released.
        """
        print("\nRunning test_checkout_insufficient_credit...")
        # Setup user with insufficient credit
        user = tu.create_user()
        user_id = user["user_id"]
        tu.add_credit_to_user(user_id, 5)

        # Setup item
        item = tu.create_item(10)
        item_id = item["item_id"]
        tu.add_stock(item_id, 5)

        # Create order costing 20, user only has 5
        order = tu.create_order(user_id)
        order_id = order["order_id"]
        tu.add_item_to_order(order_id, item_id, 2)

        # Checkout
        checkout_response = tu.checkout_order(order_id)
        self.assertEqual(checkout_response.status_code, 400)

        # Wait for failure
        status = wait_for_checkout(order_id)
        self.assertEqual(status, "failed", f"Expected failed, got: {status}")

        # Verify stock was released back (rollback worked)
        stock = tu.find_item(item_id)["stock"]
        self.assertEqual(stock, 5)

        # Verify credit is untouched
        credit = tu.find_user(user_id)["credit"]
        self.assertEqual(credit, 5)

    def test_checkout_multiple_items(self):
        """
        Order with multiple different items — all must be reserved atomically.
        """
        print("\nRunning test_checkout_multiple_items...")

        user = tu.create_user()
        user_id = user["user_id"]
        tu.add_credit_to_user(user_id, 100)

        item1 = tu.create_item(10)
        item_id1 = item1["item_id"]
        tu.add_stock(item_id1, 5)

        item2 = tu.create_item(20)
        item_id2 = item2["item_id"]
        tu.add_stock(item_id2, 3)

        order = tu.create_order(user_id)
        order_id = order["order_id"]
        tu.add_item_to_order(order_id, item_id1, 2)  # cost 20
        tu.add_item_to_order(order_id, item_id2, 1)  # cost 20, total = 40

        checkout_response = tu.checkout_order(order_id)
        self.assertEqual(checkout_response.status_code, 200)

        status = wait_for_checkout(order_id)
        self.assertEqual(status, "completed", f"Expected completed, got: {status}")

        self.assertEqual(tu.find_item(item_id1)["stock"], 3)
        self.assertEqual(tu.find_item(item_id2)["stock"], 2)
        self.assertEqual(tu.find_user(user_id)["credit"], 60)
        self.assertTrue(tu.find_order(order_id)["paid"])

    def test_checkout_one_item_out_of_stock(self):
        """
        Multi-item order where one item is out of stock.
        Nothing should be deducted — all-or-nothing validation.
        """
        print("\nRunning test_checkout_one_item_out_of_stock...")
        user = tu.create_user()
        user_id = user["user_id"]
        tu.add_credit_to_user(user_id, 100)

        item1 = tu.create_item(10)
        item_id1 = item1["item_id"]
        tu.add_stock(item_id1, 5)

        item2 = tu.create_item(10)
        item_id2 = item2["item_id"]
        tu.add_stock(item_id2, 1)  # only 1 in stock

        order = tu.create_order(user_id)
        order_id = order["order_id"]
        tu.add_item_to_order(order_id, item_id1, 2)
        tu.add_item_to_order(order_id, item_id2, 5)  # requesting 5, only 1 available

        checkout_response = tu.checkout_order(order_id)
        self.assertEqual(checkout_response.status_code, 400)

        status = wait_for_checkout(order_id)
        self.assertEqual(status, "failed", f"Expected failed, got: {status}")

        # Neither item should have been touched
        self.assertEqual(tu.find_item(item_id1)["stock"], 5)
        self.assertEqual(tu.find_item(item_id2)["stock"], 1)
        self.assertEqual(tu.find_user(user_id)["credit"], 100)


# ── Docker / Recovery infrastructure ─────────────────────────────────────────

RECOVERY_CHECKOUT_TIMEOUT = 90
RECOVERY_WAIT             = 60

ORDER_SERVICE    = "order-service"
STOCK_SERVICE    = "stock-service"
PAYMENT_SERVICE  = "payment-service"
ORDER_DB_SERVICE = "order-db"
STOCK_DB_SERVICE = "stock-db"
PAYMENT_DB_SERVICE = "payment-db"
ORDER_DB_REPLICA_SERVICE = "order-db-replica"
STOCK_DB_REPLICA_SERVICE = "stock-db-replica"
PAYMENT_DB_REPLICA_SERVICE = "payment-db-replica"
REDIS_SENTINEL_1 = "redis-sentinel-1"
REDIS_SENTINEL_2 = "redis-sentinel-2"
REDIS_SENTINEL_3 = "redis-sentinel-3"
GATEWAY_SERVICE  = "gateway"
REDIS_PASSWORD   = "redis"

HTTP_SERVICES = {
    ORDER_SERVICE,
    STOCK_SERVICE,
    PAYMENT_SERVICE,
}

REDIS_SERVICES = {
    ORDER_DB_SERVICE,
    STOCK_DB_SERVICE,
    PAYMENT_DB_SERVICE,
    ORDER_DB_REPLICA_SERVICE,
    STOCK_DB_REPLICA_SERVICE,
    PAYMENT_DB_REPLICA_SERVICE,
}

SENTINEL_SERVICES = {
    REDIS_SENTINEL_1,
    REDIS_SENTINEL_2,
    REDIS_SENTINEL_3,
}
MASTER_PREFIXES = {
    ORDER_DB_SERVICE: "REDIS",
    STOCK_DB_SERVICE: "STOCK_REDIS",
    PAYMENT_DB_SERVICE: "PAYMENT_REDIS",
}


def _compose(*args: str, input_text: Optional[str] = None, check: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["docker", "compose", *args],
        cwd=PROJECT_ROOT,
        check=check,
        capture_output=True,
        text=True,
        input=input_text,
    )


@lru_cache(maxsize=1)
def _configured_services() -> set[str]:
    result = _compose("config", "--services")
    return {line.strip() for line in result.stdout.splitlines() if line.strip()}


def _service_exists(service: str) -> bool:
    return service in _configured_services()


def _kill_service(service: str) -> None:
    _compose("kill", service)
    time.sleep(1)


def _stop_service(service: str) -> None:
    _compose("stop", service)
    time.sleep(1)


def _start_service(service: str, timeout: int = RECOVERY_WAIT) -> None:
    _compose("start", service)
    time.sleep(1)
    if service in HTTP_SERVICES:
        _wait_for_service_http(service, timeout=timeout)
        _refresh_gateway()
    elif service in REDIS_SERVICES:
        _wait_for_redis(service, timeout=timeout)
    elif service in SENTINEL_SERVICES:
        _wait_for_sentinel(service, timeout=timeout)
    else:
        raise ValueError(f"Unknown service: {service}")
    _wait_for_order_transport_dependencies(timeout=timeout)


def _ensure_services_started() -> None:
    requested_services = [
        ORDER_DB_SERVICE,
        ORDER_DB_REPLICA_SERVICE,
        STOCK_DB_SERVICE,
        STOCK_DB_REPLICA_SERVICE,
        PAYMENT_DB_SERVICE,
        PAYMENT_DB_REPLICA_SERVICE,
        REDIS_SENTINEL_1,
        REDIS_SENTINEL_2,
        REDIS_SENTINEL_3,
        GATEWAY_SERVICE,
        ORDER_SERVICE,
        STOCK_SERVICE,
        PAYMENT_SERVICE,
    ]
    _compose(
        "up",
        "-d",
        *[service for service in requested_services if _service_exists(service)],
    )
    for service in REDIS_SERVICES:
        if _service_exists(service):
            _wait_for_redis(service)
    for service in SENTINEL_SERVICES:
        if _service_exists(service):
            _wait_for_sentinel(service)
    _wait_for_service_http(ORDER_SERVICE)
    _wait_for_service_http(STOCK_SERVICE)
    _wait_for_service_http(PAYMENT_SERVICE)
    _wait_for_order_transport_dependencies()
    _refresh_gateway()
    _wait_for_gateway_routes()


def _wait_for_service_http(service: str, timeout: int = RECOVERY_WAIT) -> None:
    paths = {
        ORDER_SERVICE:   "/status/non-existent-order",
        STOCK_SERVICE:   "/find/non-existent-item",
        PAYMENT_SERVICE: "/find_user/non-existent-user",
    }
    path = paths[service]
    deadline = time.time() + timeout
    while time.time() < deadline:
        probe = _compose(
            "exec", "-T", service, "python", "-c",
            (
                "import sys, urllib.request, urllib.error\n"
                f"url = 'http://127.0.0.1:5000{path}'\n"
                "try:\n"
                "    with urllib.request.urlopen(url, timeout=1) as r:\n"
                "        code = r.getcode()\n"
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


def _refresh_gateway() -> None:
    # Nginx resolves the compose service names on startup. After a participant
    # restart we bounce the gateway so it picks up the current container IPs
    # while still benefiting from upstream keepalive during normal operation.
    _compose("restart", GATEWAY_SERVICE)
    time.sleep(1)


def _wait_for_redis(service: str, timeout: int = RECOVERY_WAIT) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        probe = _compose(
            "exec",
            "-T",
            service,
            "redis-cli",
            "-a",
            REDIS_PASSWORD,
            "PING",
            check=False,
        )
        if probe.returncode == 0 and "PONG" in probe.stdout:
            return
        time.sleep(0.5)
    raise AssertionError(f"{service} did not become ready within {timeout}s")


def _wait_for_sentinel(service: str, timeout: int = RECOVERY_WAIT) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        probe = _compose(
            "exec",
            "-T",
            service,
            "redis-cli",
            "-p",
            "26379",
            "PING",
            check=False,
        )
        if probe.returncode == 0 and "PONG" in probe.stdout:
            return
        time.sleep(0.5)
    raise AssertionError(f"{service} did not become ready within {timeout}s")


def _wait_for_order_transport_dependencies(timeout: int = RECOVERY_WAIT) -> None:
    deadline = time.time() + timeout
    probe_script = (
        "from common.redis_client import create_redis_client\n"
        "create_redis_client('REDIS', socket_connect_timeout=1, socket_timeout=1, health_check_interval=1).ping()\n"
        "create_redis_client('STOCK_REDIS', socket_connect_timeout=1, socket_timeout=1, health_check_interval=1).ping()\n"
        "create_redis_client('PAYMENT_REDIS', socket_connect_timeout=1, socket_timeout=1, health_check_interval=1).ping()\n"
    )

    while time.time() < deadline:
        probe = _compose(
            "exec",
            "-T",
            ORDER_SERVICE,
            "python",
            "-c",
            probe_script,
            check=False,
        )
        if probe.returncode == 0:
            return
        time.sleep(0.5)

    raise AssertionError(
        f"{ORDER_SERVICE} did not reconnect to its configured Redis backends within {timeout}s"
    )


def _wait_for_gateway_routes(timeout: int = RECOVERY_WAIT) -> None:
    checks = (
        f"{tu.ORDER_URL}/orders/status/non-existent-order",
        f"{tu.STOCK_URL}/stock/find/non-existent-item",
        f"{tu.PAYMENT_URL}/payment/find_user/non-existent-user",
    )
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            if all(requests.get(url, timeout=1).status_code < 500 for url in checks):
                return
        except requests.RequestException:
            pass
        time.sleep(0.5)
    raise AssertionError(f"gateway routes did not become ready within {timeout}s")


def _get_order_status_direct(order_id: str) -> str:
    """Read status directly from order-db Redis, bypassing the HTTP gateway."""
    return _read_from_master("order-db", "get", f"order:{order_id}:status") or "pending"


def _wait_for_checkout_direct(order_id: str, timeout: int = RECOVERY_CHECKOUT_TIMEOUT) -> str:
    """Poll Redis directly (no HTTP) until a terminal status is set or timeout."""
    terminal = {"completed", "failed"}
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            status = _get_order_status_direct(order_id)
            if status in terminal:
                return status
        except Exception:
            pass
        time.sleep(0.5)
    return "timeout"


def _wait_for_2pc_field(
    order_id: str,
    field: str,
    expected_value: str,
    timeout: int = 30,
) -> bool:
    """
    Poll the order:{order_id}:2pcstate Redis hash directly until
    `field` equals `expected_value` or timeout is reached.
    Returns True if the condition was met.
    """
    deadline = time.time() + timeout
    while time.time() < deadline:
        if _read_from_master("order-db", "hget", f"order:{order_id}:2pcstate", field) == expected_value:
            return True
        time.sleep(0.3)
    return False


def _read_from_master(master_name: str, command: str, key: str, field: Optional[str] = None) -> str:
    env_prefix = MASTER_PREFIXES[master_name]
    read_script = (
        "import sys\n"
        "from common.redis_client import create_redis_client\n"
        "db = create_redis_client(\n"
        "    sys.argv[1],\n"
        "    socket_connect_timeout=1,\n"
        "    socket_timeout=1,\n"
        "    health_check_interval=1,\n"
        ")\n"
        "command = sys.argv[2]\n"
        "key = sys.argv[3]\n"
        "if command == 'get':\n"
        "    value = db.get(key)\n"
        "elif command == 'hget':\n"
        "    value = db.hget(key, sys.argv[4])\n"
        "else:\n"
        "    raise SystemExit(f'unsupported command: {command}')\n"
        "if value is not None:\n"
        "    sys.stdout.write(value.decode() if isinstance(value, bytes) else str(value))\n"
    )
    args = [env_prefix, command, key]
    if field is not None:
        args.append(field)
    result = _compose(
        "exec",
        "-T",
        ORDER_SERVICE,
        "python",
        "-c",
        read_script,
        *args,
    )
    return result.stdout.strip()


class TwoPC_DockerTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        _ensure_services_started()

    def setUp(self) -> None:
        _ensure_services_started()

    def tearDown(self) -> None:
        _ensure_services_started()


class Test2pcRecovery(TwoPC_DockerTestCase):

    def test_coordinator_crash_before_decision_resolves_completed(self):
        """
        Coordinator (order-service) crashes with DECISION_NONE while stock-service
        is down. After both restart: stock picks up PREPARE_STOCK from Kafka, both
        participants confirm READY, coordinator commits → completed.
        """
        print("\nRunning test_coordinator_crash_before_decision_resolves_completed...")
        user = tu.create_user()
        user_id = user["user_id"]
        tu.add_credit_to_user(user_id, 50)

        item = tu.create_item(10)
        item_id = item["item_id"]
        tu.add_stock(item_id, 5)

        order = tu.create_order(user_id)
        order_id = order["order_id"]
        tu.add_item_to_order(order_id, item_id, 2)

        # Stop stock so it cannot process PREPARE_STOCK
        _stop_service(STOCK_SERVICE)

        thread, outcome = _start_checkout_in_background(order_id)

        # Wait until the coordinator has recorded PAYMENT_READY before killing it,
        # so Redis is deterministically in the DECISION_NONE/PAYMENT_READY/STOCK_UNKNOWN state.
        self.assertTrue(
            _wait_for_2pc_field(order_id, "payment_state", "PAYMENT_READY"),
            "payment_state did not become PAYMENT_READY before the coordinator kill",
        )

        # Kill coordinator mid-flight: Redis has DECISION_NONE, PAYMENT_READY, STOCK_UNKNOWN
        _kill_service(ORDER_SERVICE)

        # Restart coordinator (recover_incomplete_2pc runs; still no decision since
        # stock hasn't replied yet), then stock (will pick up PREPARE_STOCK from Kafka)
        _start_service(ORDER_SERVICE)
        _start_service(STOCK_SERVICE)

        status = _wait_for_checkout_direct(order_id)
        self.assertEqual(status, "completed", f"Expected completed, got: {status}")
        response = _wait_for_background_checkout(thread, outcome)
        self.assertIn(response.status_code, {200, 502})
        self.assertEqual(tu.find_item(item_id)["stock"], 3)
        self.assertEqual(tu.find_user(user_id)["credit"], 30)
        self.assertTrue(tu.find_order(order_id)["paid"])

    def test_coordinator_crash_before_decision_resolves_failed(self):
        """
        Coordinator crashes with DECISION_NONE while payment-service is down.
        User has insufficient credit. After both restart: payment processes
        PREPARE_PAYMENT, fails → coordinator sends ABORT → stock locks released → failed.
        """
        print("\nRunning test_coordinator_crash_before_decision_resolves_failed...")
        user = tu.create_user()
        user_id = user["user_id"]
        tu.add_credit_to_user(user_id, 5)  # insufficient: order costs 20

        item = tu.create_item(10)
        item_id = item["item_id"]
        tu.add_stock(item_id, 5)

        order = tu.create_order(user_id)
        order_id = order["order_id"]
        tu.add_item_to_order(order_id, item_id, 2)

        # Stop payment so it cannot process PREPARE_PAYMENT
        _stop_service(PAYMENT_SERVICE)

        thread, outcome = _start_checkout_in_background(order_id)

        # Wait until the coordinator has recorded STOCK_READY before killing it.
        self.assertTrue(
            _wait_for_2pc_field(order_id, "stock_state", "STOCK_READY"),
            "stock_state did not become STOCK_READY before the coordinator kill",
        )

        # Kill coordinator: DECISION_NONE, STOCK_READY, PAYMENT_UNKNOWN
        _kill_service(ORDER_SERVICE)

        # Restart coordinator + payment
        _start_service(ORDER_SERVICE)
        _start_service(PAYMENT_SERVICE)

        # Payment processes PREPARE_PAYMENT → fails (insufficient credit) →
        # PAYMENT_PREPARE_FAILED → coordinator ABORTs → ABORT_STOCK sent → locks released
        status = _wait_for_checkout_direct(order_id)
        self.assertEqual(status, "failed", f"Expected failed, got: {status}")
        response = _wait_for_background_checkout(thread, outcome)
        self.assertIn(response.status_code, {400, 502})
        self.assertEqual(tu.find_item(item_id)["stock"], 5)
        self.assertEqual(tu.find_user(user_id)["credit"], 5)

    def test_stock_service_restart_during_prepare(self):
        """
        Stock-service is down when PREPARE_STOCK arrives. No coordinator crash.
        After stock restarts it picks up the pending command from Kafka and the
        transaction completes normally.
        """
        print("\nRunning test_stock_service_restart_during_prepare...")
        user = tu.create_user()
        user_id = user["user_id"]
        tu.add_credit_to_user(user_id, 50)

        item = tu.create_item(10)
        item_id = item["item_id"]
        tu.add_stock(item_id, 5)

        order = tu.create_order(user_id)
        order_id = order["order_id"]
        tu.add_item_to_order(order_id, item_id, 2)

        _stop_service(STOCK_SERVICE)

        thread, outcome = _start_checkout_in_background(order_id)

        self.assertTrue(
            _wait_for_2pc_field(order_id, "payment_state", "PAYMENT_READY"),
            "payment_state did not become PAYMENT_READY before stock restart",
        )

        # Restart stock — picks up PREPARE_STOCK from Kafka (auto_offset_reset="earliest")
        _start_service(STOCK_SERVICE)

        status = _wait_for_checkout_direct(order_id)
        self.assertEqual(status, "completed", f"Expected completed, got: {status}")
        response = _wait_for_background_checkout(thread, outcome)
        self.assertIn(response.status_code, {200, 502})
        self.assertEqual(tu.find_item(item_id)["stock"], 3)
        self.assertEqual(tu.find_user(user_id)["credit"], 30)
        self.assertTrue(tu.find_order(order_id)["paid"])

    def test_payment_service_restart_during_prepare(self):
        """
        Payment-service is down when PREPARE_PAYMENT arrives. No coordinator crash.
        After payment restarts it picks up the pending command from Kafka and the
        transaction completes normally.
        """
        print("\nRunning test_payment_service_restart_during_prepare...")
        user = tu.create_user()
        user_id = user["user_id"]
        tu.add_credit_to_user(user_id, 50)

        item = tu.create_item(10)
        item_id = item["item_id"]
        tu.add_stock(item_id, 5)

        order = tu.create_order(user_id)
        order_id = order["order_id"]
        tu.add_item_to_order(order_id, item_id, 2)

        _stop_service(PAYMENT_SERVICE)

        thread, outcome = _start_checkout_in_background(order_id)

        self.assertTrue(
            _wait_for_2pc_field(order_id, "stock_state", "STOCK_READY"),
            "stock_state did not become STOCK_READY before payment restart",
        )

        # Restart payment — picks up PREPARE_PAYMENT from Kafka
        _start_service(PAYMENT_SERVICE)

        status = _wait_for_checkout_direct(order_id)
        self.assertEqual(status, "completed", f"Expected completed, got: {status}")
        response = _wait_for_background_checkout(thread, outcome)
        self.assertIn(response.status_code, {200, 502})
        self.assertEqual(tu.find_item(item_id)["stock"], 3)
        self.assertEqual(tu.find_user(user_id)["credit"], 30)
        self.assertTrue(tu.find_order(order_id)["paid"])


if __name__ == "__main__":
    unittest.main()
