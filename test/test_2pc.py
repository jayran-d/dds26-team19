"""
test_2pc.py

Async-aware tests for the 2PC Kafka checkout mode.

Assumes:
    - Services are running with USE_KAFKA=true
    - TRANSACTION_MODE=2pc
    - Gateway is reachable at the URL configured in utils.py
"""

import time
import unittest
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


class Test2pc(unittest.TestCase):

    def test_checkout_success(self):
        """
        Happy path: user has enough credit, item has enough stock.
        Checkout should complete successfully.
        """
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
        self.assertEqual(checkout_response.status_code, 202)

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
        self.assertEqual(checkout_response.status_code, 202)

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
        self.assertEqual(checkout_response.status_code, 202)

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
        self.assertEqual(checkout_response.status_code, 202)

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
        self.assertEqual(checkout_response.status_code, 202)

        status = wait_for_checkout(order_id)
        self.assertEqual(status, "failed", f"Expected failed, got: {status}")

        # Neither item should have been touched
        self.assertEqual(tu.find_item(item_id1)["stock"], 5)
        self.assertEqual(tu.find_item(item_id2)["stock"], 1)
        self.assertEqual(tu.find_user(user_id)["credit"], 100)


if __name__ == "__main__":
    unittest.main()