"""
Saga integration tests for TRANSACTION_MODE=saga.

Run from the repo root with:

    python3 test/test_kafka_saga.py

Assumptions:
    - Docker Compose stack is already up
    - TRANSACTION_MODE=saga
    - /orders/checkout/<order_id> waits for a terminal Saga result on this branch
    - Internal transport may be Kafka or Redis Streams; tests inject directly
      into the currently configured transport via compose helpers.

Coverage:
    - happy path and normal compensation
    - duplicate checkout and duplicate command idempotency
    - stale event safety
    - oversell prevention
    - orchestrator-service restart recovery
    - timeout recovery while stock/payment are unavailable

Database stop/kill recovery coverage lives in:

    python3 test/test_kafka_saga_databases.py

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
ORCHESTRATOR_SERVICE = "orchestrator-service"
STOCK_SERVICE = "stock-service"
PAYMENT_SERVICE = "payment-service"
ORDER_DB_SERVICE = "order-db"
ORCHESTRATOR_DB_SERVICE = "orchestrator-db"
STOCK_DB_SERVICE = "stock-db"
PAYMENT_DB_SERVICE = "payment-db"
GATEWAY_SERVICE = "gateway"
REDIS_PASSWORD = "redis"
HTTP_SERVICES = {
    ORDER_SERVICE,
    ORCHESTRATOR_SERVICE,
    STOCK_SERVICE,
    PAYMENT_SERVICE,
}
REDIS_SERVICES = {
    ORDER_DB_SERVICE,
    ORCHESTRATOR_DB_SERVICE,
    STOCK_DB_SERVICE,
    PAYMENT_DB_SERVICE,
}


# ── Compose helpers ───────────────────────────────────────────────────────────

def _compose(
    *args: str,
    input_text: str | None = None,
    check: bool = True,
) -> subprocess.CompletedProcess:
    result = subprocess.run(
        ["docker", "compose", *args],
        cwd=PROJECT_ROOT,
        check=False,
        capture_output=True,
        text=True,
        input=input_text,
    )
    if check and result.returncode != 0:
        cmd = " ".join(["docker", "compose", *args])
        details = result.stderr.strip() or result.stdout.strip() or f"exit code {result.returncode}"
        raise AssertionError(f"Compose command failed: {cmd}\n{details}")
    return result


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
    if service in HTTP_SERVICES:
        _wait_for_service_http(service, timeout=timeout)
        _refresh_gateway()
    elif service in REDIS_SERVICES:
        _wait_for_redis(service, timeout=timeout)
    else:
        raise ValueError(f"Unknown service: {service}")

    # The orchestrator owns the transport and status mirroring. A green HTTP
    # port alone is not enough after stop/kill tests; wait until it can resolve
    # and ping its Redis dependencies again.
    _wait_for_orchestrator_transport_dependencies(timeout=timeout)


def _ensure_services_started() -> None:
    _compose(
        "up",
        "-d",
        ORDER_DB_SERVICE,
        ORCHESTRATOR_DB_SERVICE,
        STOCK_DB_SERVICE,
        PAYMENT_DB_SERVICE,
        GATEWAY_SERVICE,
        ORCHESTRATOR_SERVICE,
        ORDER_SERVICE,
        STOCK_SERVICE,
        PAYMENT_SERVICE,
    )
    _wait_for_redis(ORDER_DB_SERVICE)
    _wait_for_redis(ORCHESTRATOR_DB_SERVICE)
    _wait_for_redis(STOCK_DB_SERVICE)
    _wait_for_redis(PAYMENT_DB_SERVICE)
    _wait_for_service_http(ORCHESTRATOR_SERVICE)
    _wait_for_service_http(ORDER_SERVICE)
    _wait_for_service_http(STOCK_SERVICE)
    _wait_for_service_http(PAYMENT_SERVICE)
    _wait_for_orchestrator_transport_dependencies()
    _refresh_gateway()
    _wait_for_gateway_routes()


def _wait_for_service_http(service: str, timeout: int = RECOVERY_WAIT) -> None:
    if service == ORCHESTRATOR_SERVICE:
        path = "/health"
    elif service == ORDER_SERVICE:
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


def _wait_for_orchestrator_transport_dependencies(timeout: int = RECOVERY_WAIT) -> None:
    deadline = time.time() + timeout
    probe_script = (
        "import os\n"
        "import sys\n"
        "import redis\n"
        "targets = [\n"
        "    (\n"
        "        os.environ['ORDER_REDIS_HOST'],\n"
        "        int(os.getenv('ORDER_REDIS_PORT', '6379')),\n"
        "        os.environ['ORDER_REDIS_PASSWORD'],\n"
        "        int(os.getenv('ORDER_REDIS_DB', '0')),\n"
        "    ),\n"
        "    (\n"
        "        os.environ['STOCK_REDIS_HOST'],\n"
        "        int(os.getenv('STOCK_REDIS_PORT', '6379')),\n"
        "        os.environ['STOCK_REDIS_PASSWORD'],\n"
        "        int(os.getenv('STOCK_REDIS_DB', '0')),\n"
        "    ),\n"
        "    (\n"
        "        os.environ['PAYMENT_REDIS_HOST'],\n"
        "        int(os.getenv('PAYMENT_REDIS_PORT', '6379')),\n"
        "        os.environ['PAYMENT_REDIS_PASSWORD'],\n"
        "        int(os.getenv('PAYMENT_REDIS_DB', '0')),\n"
        "    ),\n"
        "]\n"
        "for host, port, password, db in targets:\n"
        "    client = redis.Redis(\n"
        "        host=host,\n"
        "        port=port,\n"
        "        password=password,\n"
        "        db=db,\n"
        "        socket_connect_timeout=1,\n"
        "        socket_timeout=1,\n"
        "    )\n"
        "    client.ping()\n"
    )

    while time.time() < deadline:
        probe = _compose(
            "exec",
            "-T",
            ORCHESTRATOR_SERVICE,
            "python",
            "-c",
            probe_script,
            check=False,
        )
        if probe.returncode == 0:
            return
        time.sleep(0.5)

    raise AssertionError(
        f"{ORCHESTRATOR_SERVICE} did not reconnect to order-db/stock-db/payment-db within {timeout}s"
    )


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


def _start_checkout_request(order_id: str) -> tuple[threading.Thread, dict]:
    """
    Run checkout in a background thread so recovery tests can still stop/start
    services while the HTTP request is waiting for the Saga to finish.
    """
    result: dict[str, object] = {}

    def run() -> None:
        try:
            result["response"] = tu.checkout_order(order_id)
        except Exception as exc:  # noqa: BLE001 - surface request failure to the test
            result["error"] = exc

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    return thread, result


def _finish_checkout_request(
    thread: threading.Thread,
    result: dict,
    timeout: int = CHECKOUT_TIMEOUT + 15,
    allow_request_error: bool = False,
) -> requests.Response | None:
    thread.join(timeout)
    if thread.is_alive():
        raise AssertionError("checkout request thread did not finish in time")
    if "error" in result:
        if allow_request_error:
            return None
        raise AssertionError(f"checkout request failed: {result['error']}")
    response = result.get("response")
    if response is None:
        raise AssertionError("checkout request returned no response")
    return response


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
    """
    Legacy helper name kept for test compatibility.
    Publishes directly into the currently configured message transport.

    On this branch, that means writing a msgpack payload to the appropriate
    Redis Stream via a short Python snippet executed inside orchestrator-service.
    """
    publish_script = (
        "import json, sys\n"
        "import redis\n"
        "from msgspec import msgpack\n"
        "topic = sys.argv[1]\n"
        "message = json.loads(sys.stdin.read())\n"
        "host = 'stock-db' if topic.startswith('stock.') else 'payment-db'\n"
        "db = redis.Redis(host=host, port=6379, password='redis', db=0)\n"
        "db.xadd(topic, {'d': msgpack.encode(message)}, maxlen=100000, approximate=True)\n"
    )
    _compose(
        "exec",
        "-T",
        ORCHESTRATOR_SERVICE,
        "python",
        "-c",
        publish_script,
        topic,
        input_text=json.dumps(message),
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
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(wait_for_checkout(order_id), "completed")
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
        self.assertEqual(resp.status_code, 400)
        self.assertEqual(wait_for_checkout(order_id), "failed")
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
        self.assertEqual(resp.status_code, 400)
        self.assertEqual(wait_for_checkout(order_id), "failed")
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

        thread1, result1 = _start_checkout_request(order_id)
        thread2, result2 = _start_checkout_request(order_id)

        resp1 = _finish_checkout_request(thread1, result1)
        resp2 = _finish_checkout_request(thread2, result2)

        self.assertEqual(resp1.status_code, 200)
        self.assertEqual(resp2.status_code, 200)
        self.assertEqual(wait_for_checkout(order_id), "completed")
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
            response = tu.checkout_order(oid)
            results[oid] = "completed" if response.status_code == 200 else "failed"

        threads = [threading.Thread(target=do_checkout, args=(oid,)) for oid in order_ids]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        statuses = list(results.values())
        self.assertEqual(statuses.count("completed"), 1, msg=f"Statuses: {results}")
        self.assertEqual(statuses.count("failed"), CONCURRENT_USERS - 1, msg=f"Statuses: {results}")
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
    def test_orchestrator_restart_recovers_from_reserving_stock(self):
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

        thread, result = _start_checkout_request(order_id)
        self.assertTrue(wait_for_status(order_id, "reserving_stock", timeout=CHECKOUT_TIMEOUT))

        _kill_service(ORCHESTRATOR_SERVICE)
        _start_service(ORCHESTRATOR_SERVICE)
        _start_service(STOCK_SERVICE)

        _finish_checkout_request(
            thread,
            result,
            timeout=CHECKOUT_TIMEOUT + RECOVERY_WAIT,
            allow_request_error=True,
        )
        self.assertEqual(wait_for_checkout(order_id), "completed")
        self.assertEqual(tu.find_item(item_id)["stock"], 4)
        self.assertEqual(tu.find_user(user_id)["credit"], 90)

    def test_orchestrator_restart_recovers_from_processing_payment(self):
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

        thread, result = _start_checkout_request(order_id)
        self.assertTrue(wait_for_status(order_id, "processing_payment", timeout=CHECKOUT_TIMEOUT))

        _kill_service(ORCHESTRATOR_SERVICE)
        _start_service(ORCHESTRATOR_SERVICE)
        _start_service(PAYMENT_SERVICE)

        _finish_checkout_request(
            thread,
            result,
            timeout=CHECKOUT_TIMEOUT + RECOVERY_WAIT,
            allow_request_error=True,
        )
        self.assertEqual(wait_for_checkout(order_id), "completed")
        self.assertEqual(tu.find_item(item_id)["stock"], 4)
        self.assertEqual(tu.find_user(user_id)["credit"], 90)

    def test_orchestrator_restart_recovers_from_compensating(self):
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

            thread, result = _start_checkout_request(order_id)
            self.assertTrue(wait_for_status(order_id, "processing_payment", timeout=CHECKOUT_TIMEOUT))

            _stop_service(STOCK_SERVICE)

            tx_id = _get_order_tx_id(order_id)
            forced_failure = build_payment_failed(tx_id, order_id, "forced test failure")
            _publish_to_kafka(PAYMENT_EVENTS_TOPIC, forced_failure)

            self.assertTrue(wait_for_status(order_id, "compensating", timeout=CHECKOUT_TIMEOUT))

            _kill_service(ORCHESTRATOR_SERVICE)
            _start_service(ORCHESTRATOR_SERVICE, timeout=RECOVERY_WAIT)
            _start_service(STOCK_SERVICE, timeout=RECOVERY_WAIT)
            _start_service(PAYMENT_SERVICE, timeout=RECOVERY_WAIT)

            _finish_checkout_request(
                thread,
                result,
                timeout=CHECKOUT_TIMEOUT + RECOVERY_WAIT,
                allow_request_error=True,
            )
            self.assertEqual(wait_for_checkout(order_id, timeout=CHECKOUT_TIMEOUT), "failed")
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

        thread, result = _start_checkout_request(order_id)
        self.assertTrue(wait_for_status(order_id, "reserving_stock", timeout=CHECKOUT_TIMEOUT))

        time.sleep(SAGA_TIMEOUT_SECONDS + 5)
        _start_service(STOCK_SERVICE)

        resp = _finish_checkout_request(thread, result, timeout=CHECKOUT_TIMEOUT + RECOVERY_WAIT)
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(wait_for_checkout(order_id), "completed")
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

        thread, result = _start_checkout_request(order_id)
        self.assertTrue(wait_for_status(order_id, "processing_payment", timeout=CHECKOUT_TIMEOUT))

        time.sleep(SAGA_TIMEOUT_SECONDS + 5)
        _start_service(PAYMENT_SERVICE)

        resp = _finish_checkout_request(thread, result, timeout=CHECKOUT_TIMEOUT + RECOVERY_WAIT)
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(wait_for_checkout(order_id), "completed")
        self.assertEqual(tu.find_item(item_id)["stock"], 4)
        self.assertEqual(tu.find_user(user_id)["credit"], 90)


if __name__ == "__main__":
    unittest.main()
