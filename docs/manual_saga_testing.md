# Manual Saga Testing Guide

This guide is for verifying the Kafka Saga implementation from the terminal and making sense of the logs while it runs.

It covers:
- how to watch the right logs
- how to run the automated Saga tests while watching those logs
- how to run a few manual scenarios yourself
- what you should expect to see for each scenario

This guide assumes:
- `USE_KAFKA=true`
- `TRANSACTION_MODE=saga`
- the Docker Compose stack is already running
- this branch uses the async checkout API (`POST /orders/checkout/<id>` returns `202`)

## 1. Start the Stack

From the repo root:

```bash
docker compose up --build -d
```

Confirm the main services are up:

```bash
docker compose ps
```

You should see:
- `gateway`
- `order-service`
- `stock-service`
- `payment-service`
- `kafka`
- `order-db`
- `stock-db`
- `payment-db`

## 2. Watch the Logs

Open three terminals and keep these running.

Order service:

```bash
docker compose logs -f order-service | rg "OrderKafka|\\[Saga\\]"
```

Stock service:

```bash
docker compose logs -f stock-service | rg "StockKafka|\\[StockSaga\\]"
```

Payment service:

```bash
docker compose logs -f payment-service | rg "PaymentKafka|\\[PaymentSaga\\]|Removing"
```

If you do not have `rg`, use `grep -E` instead.

Combined view:

```bash
docker compose logs -f order-service stock-service payment-service
```

## 3. The Most Important Log Lines

These are the key lines that show the Saga is progressing correctly.

Order service:
- `[Saga] started tx=... order=...`
- `[OrderKafka] event=STOCK_RESERVED order=... tx=...`
- `[Saga] stock reserved → processing payment tx=...`
- `[OrderKafka] event=PAYMENT_SUCCESS order=... tx=...`
- `[Saga] completed tx=... order=...`
- `[OrderKafka] event=PAYMENT_FAILED order=... tx=...`
- `[Saga] payment failed → compensating tx=...: ...`
- `[OrderKafka] event=STOCK_RELEASED order=... tx=...`
- `[Saga] compensation done → failed tx=...`
- `[Saga] recovery: found N in-flight Saga(s)`
- `[Saga] recovering tx=... order=... state=...`
- `[Saga] timeout detected tx=... order=... state=...`

Stock service:
- `[StockKafka] command=RESERVE_STOCK order=... tx=...`
- `[StockSaga] RESERVE_STOCK success tx=... order=...`
- `[StockSaga] RESERVE_STOCK failure tx=... order=...`
- `[StockKafka] command=RELEASE_STOCK order=... tx=...`
- `[StockSaga] RELEASE_STOCK done tx=... order=...`

Payment service:
- `[PaymentKafka] command=PROCESS_PAYMENT order=... tx=...`
- `Removing ... credit from user: ...`
- `[PaymentSaga] PROCESS_PAYMENT success tx=... order=...`
- `[PaymentSaga] PROCESS_PAYMENT failure tx=... order=...`
- `[PaymentKafka] command=REFUND_PAYMENT order=... tx=...`
- `[PaymentSaga] REFUND_PAYMENT done tx=... order=...`

## 4. Useful Inspection Commands

Check an order’s public status:

```bash
curl -s http://127.0.0.1:8000/orders/status/<order_id>
```

Check full order state:

```bash
curl -s http://127.0.0.1:8000/orders/find/<order_id>
```

Check stock:

```bash
curl -s http://127.0.0.1:8000/stock/find/<item_id>
```

Check user credit:

```bash
curl -s http://127.0.0.1:8000/payment/find_user/<user_id>
```

Read the active `tx_id` for an order directly from Redis:

```bash
docker compose exec -T order-db redis-cli -a redis GET order:<order_id>:tx_id
```

Read the current Saga status directly from Redis:

```bash
docker compose exec -T order-db redis-cli -a redis GET order:<order_id>:status
```

## 5. Run the Automated Saga Tests While Watching Logs

The easiest way to convince yourself that the tests are exercising the right paths is:

1. keep the three log terminals open
2. run either the whole suite or one test at a time
3. match the log lines to the scenario

Run the whole Saga suite:

```bash
python3 test/test_kafka_saga.py
```

Run only one scenario:

```bash
python3 -m unittest test.test_kafka_saga.TestSagaRecovery.test_order_service_restart_recovers_from_processing_payment
```

Recommended single tests to watch:
- `TestSagaBasic.test_happy_path`
- `TestSagaBasic.test_payment_failure_releases_stock`
- `TestSagaIdempotency.test_duplicate_checkout_same_order`
- `TestSagaConcurrency.test_concurrent_checkouts_no_oversell`
- `TestSagaRecovery.test_order_service_restart_recovers_from_reserving_stock`
- `TestSagaRecovery.test_order_service_restart_recovers_from_processing_payment`
- `TestSagaRecovery.test_stock_service_timeout_recovery`
- `TestSagaRecovery.test_payment_service_timeout_recovery`

## 6. Manual Scenario 1: Happy Path

Create a user:

```bash
curl -s -X POST http://127.0.0.1:8000/payment/create_user
```

Store the returned `user_id`, then add funds:

```bash
curl -s -X POST http://127.0.0.1:8000/payment/add_funds/<user_id>/100
```

Create an item:

```bash
curl -s -X POST http://127.0.0.1:8000/stock/item/create/10
```

Store the returned `item_id`, then add stock:

```bash
curl -s -X POST http://127.0.0.1:8000/stock/add/<item_id>/5
```

Create an order:

```bash
curl -s -X POST http://127.0.0.1:8000/orders/create/<user_id>
```

Store the returned `order_id`, then add the item:

```bash
curl -s -X POST http://127.0.0.1:8000/orders/addItem/<order_id>/<item_id>/2
```

Start checkout:

```bash
curl -i -X POST http://127.0.0.1:8000/orders/checkout/<order_id>
```

Poll until terminal:

```bash
watch -n 0.5 "curl -s http://127.0.0.1:8000/orders/status/<order_id>"
```

Expected result:
- order status becomes `completed`
- stock becomes `3`
- user credit becomes `80`
- order `paid=true`

Expected log flow:
- order: `[Saga] started ...`
- stock: `[StockKafka] command=RESERVE_STOCK ...`
- stock: `[StockSaga] RESERVE_STOCK success ...`
- order: `[OrderKafka] event=STOCK_RESERVED ...`
- order: `[Saga] stock reserved → processing payment ...`
- payment: `[PaymentKafka] command=PROCESS_PAYMENT ...`
- payment: `[PaymentSaga] PROCESS_PAYMENT success ...`
- order: `[OrderKafka] event=PAYMENT_SUCCESS ...`
- order: `[Saga] completed ...`

## 7. Manual Scenario 2: Payment Failure And Stock Compensation

Same setup as above, except:
- give the user only `5` credit
- order total must be greater than `5`

Expected result:
- order status becomes `failed`
- stock ends at its original value
- user credit stays unchanged
- order remains `paid=false`

Expected log flow:
- order: `[Saga] started ...`
- stock: `RESERVE_STOCK success`
- order: `stock reserved → processing payment`
- payment: `PROCESS_PAYMENT failure`
- order: `payment failed → compensating`
- stock: `RELEASE_STOCK done`
- order: `compensation done → failed`

## 8. Manual Scenario 3: Duplicate Checkout

Use a normal happy-path order, then send checkout twice quickly:

```bash
curl -i -X POST http://127.0.0.1:8000/orders/checkout/<order_id>
curl -i -X POST http://127.0.0.1:8000/orders/checkout/<order_id>
```

Expected result:
- only one transaction should have effect
- final stock should be reduced once
- final credit should be reduced once

What to verify:
- item stock changed exactly once
- user credit changed exactly once
- order ends `completed`

## 9. Manual Scenario 4: Timeout Recovery With Stock Down

Set up a normal payable order, then stop stock before checkout:

```bash
docker compose stop stock-service
curl -i -X POST http://127.0.0.1:8000/orders/checkout/<order_id>
```

Check status:

```bash
watch -n 0.5 "docker compose exec -T order-db redis-cli -a redis GET order:<order_id>:status"
```

You should see it remain in `reserving_stock`.

Wait more than the Saga timeout, then bring stock back:

```bash
sleep 35
docker compose start stock-service
```

Expected order logs:
- `[Saga] timeout detected tx=... state=reserving_stock`
- eventually normal reserve/payment/completed flow

Expected final result:
- order becomes `completed`
- stock and credit reflect one successful checkout

## 10. Manual Scenario 5: Timeout Recovery With Payment Down

Set up a normal payable order, then stop payment before checkout:

```bash
docker compose stop payment-service
curl -i -X POST http://127.0.0.1:8000/orders/checkout/<order_id>
```

Check status in Redis:

```bash
watch -n 0.5 "docker compose exec -T order-db redis-cli -a redis GET order:<order_id>:status"
```

You should see `processing_payment`.

Wait more than the Saga timeout, then bring payment back:

```bash
sleep 35
docker compose start payment-service
```

Expected order logs:
- `[Saga] timeout detected tx=... state=processing_payment`
- after payment returns, `PAYMENT_SUCCESS`
- order ends `completed`

## 11. Manual Scenario 6: Order Restart Recovery

### Recover from `reserving_stock`

1. stop stock:

```bash
docker compose stop stock-service
```

2. start checkout
3. confirm status is `reserving_stock`
4. kill order-service:

```bash
docker compose kill order-service
```

5. start order-service
6. start stock-service

Expected order logs after restart:
- `[Saga] recovery: found 1 in-flight Saga(s)`
- `[Saga] recovering tx=... state=reserving_stock`
- then normal completion flow

### Recover from `processing_payment`

1. stop payment
2. start checkout
3. confirm status is `processing_payment`
4. kill order-service
5. start order-service
6. start payment-service

Expected order logs after restart:
- `[Saga] recovery: found 1 in-flight Saga(s)`
- `[Saga] recovering tx=... state=processing_payment`
- then normal completion flow

## 12. How To Decide If A Scenario Really Passed

Do not rely only on “no exception”.

For each scenario, verify all of these:
- final order status is correct
- final item stock is correct
- final user credit is correct
- order `paid` flag is correct
- log sequence matches the expected Saga path

For success:
- stock reduced once
- credit reduced once
- order completed

For compensated failure:
- stock restored to original value
- credit unchanged
- order failed

For duplicate checkout:
- no double deduction

For timeout/recovery:
- status may stay in an in-flight state for a while
- after service returns, Saga must converge to the correct terminal state

## 13. Common Things That Mean Something Is Wrong

These are strong signals of a real bug:
- order stuck forever in `reserving_stock`, `processing_payment`, or `compensating`
- stock reduced twice for one order
- credit reduced twice for one order
- payment failed but stock never returned
- order completed even though payment failed
- stale or duplicate event changes a finished order

These are usually just harness/infrastructure symptoms:
- gateway returns HTML / 502 right after restarting a service
- a service takes a few seconds to come back after `docker compose start`

## 14. Minimal Command Set For A Demo

If you only want a short demo for someone else:

1. open the three log terminals from Section 2
2. run one happy-path checkout manually
3. run one payment-failure checkout manually
4. run:

```bash
python3 -m unittest test.test_kafka_saga.TestSagaRecovery.test_payment_service_timeout_recovery
```

That combination shows:
- normal success
- compensation
- timeout-based recovery

