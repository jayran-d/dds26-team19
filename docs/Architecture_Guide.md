
# Phase 1 Codebase

# **1\. Overview**

This project implements a distributed e-commerce system using microservices. Three services handle orders, stock, and payments. They communicate over HTTP (synchronous fallback) or Apache Kafka (async event-driven, default).

Phase 1 requires implementing two distributed transaction protocols — SAGA and 2PC — with single-container fault tolerance. Phase 2 will extract transaction coordination into a standalone orchestrator artifact.

# **2\. Quick Start**

## **Environment Variables**

| Variable | Description |
| :---- | :---- |
| **USE\_KAFKA** | true / false — enable Kafka or fall back to HTTP |
| **TRANSACTION\_MODE** | simple / saga / 2pc — which protocol to use |
| **KAFKA\_BOOTSTRAP\_SERVERS** | kafka:9092 (default) |
| **KAFKA\_NUM\_PARTITIONS** | 4 (default) |
| **REDIS\_HOST / PORT / PASSWORD / DB** | Redis connection settings per service |
| **GATEWAY\_URL** | http://gateway:80 — used by order service for HTTP fallback |

## **Running the Tests**

The original synchronous tests work against the HTTP fallback path (USE\_KAFKA=false):

python3 test/test\_microservices.py

The async Kafka tests verify the full event-driven flow (USE\_KAFKA=true):

python3 test/test\_kafka\_simple.py

# **3\. Architecture**

## **Services**

| Service | Responsibility |
| :---- | :---- |
| **order-service** | Owns checkout flow. Publishes commands, consumes events, drives the transaction state machine. |
| **stock-service** | Manages item stock. Consumes stock commands, publishes stock events. |
| **payment-service** | Manages user credit. Consumes payment commands, publishes payment events. |
| **gateway (nginx)** | Reverse proxy. Routes /orders/ → order-service, /stock/ → stock-service, /payment/ → payment-service. |
| **kafka** | Message broker. Holds all four topics. No custom config needed. |
| **redis (x3)** | Each service has its own Redis instance. Not shared across services. |

## **Kafka Topic Ownership**

Each service only writes to its own event topic and reads from its own command topic:

| Topic | Ownership |
| :---- | :---- |
| **stock.commands** | Order service writes → Stock service reads |
| **stock.events** | Stock service writes → Order service reads |
| **payment.commands** | Order service writes → Payment service reads |
| **payment.events** | Payment service writes → Order service reads |

# **4\. File Structure**

## **common/**

| File | Purpose |
| :---- | :---- |
| **kafka\_client.py** | KafkaProducerClient and KafkaConsumerClient wrappers. Handles broker connection retries, topic creation, and JSON encode/decode. No partition logic — Kafka assigns partitions automatically. |
| **messages.py** | Single source of truth for all topic names, message type constants, status state classes, and message builder functions. Import from here — never hardcode topic strings or message types elsewhere. |

## **order/**

| File | Purpose |
| :---- | :---- |
| **app.py** | Flask routes only. Knows nothing about saga or 2PC. Calls start\_checkout(order\_id, order\_entry) for the Kafka path. Synchronous HTTP fallback is inline for USE\_KAFKA=false. |
| **kafka\_worker.py** | Kafka plumbing and mode switching. Owns producer, consumer, event loop thread. Reads TRANSACTION\_MODE env var and delegates to the correct transaction\_modes/ module. |
| **transaction\_modes/simple.py** | Direct Kafka equivalent of the template HTTP checkout. No orchestration protocol. RESERVE\_STOCK → PROCESS\_PAYMENT → mark paid. Used for testing the Kafka foundation. |
| **transaction\_modes/saga.py** | Saga state machine. Handlers are placeholders — TODO\. |
| **transaction\_modes/two\_pc.py** | 2PC coordinator. Handlers are placeholders — TODO\. |

## **stock/**

| File | Purpose |
| :---- | :---- |
| **app.py** | Flask routes \+ apply\_stock\_delta() \+ get\_item\_from\_db(). Both are called directly by kafka\_worker.py — no duplication of business logic. |
| **kafka\_worker.py** | Consumes stock.commands. Handles RESERVE\_STOCK (all-or-nothing validation then subtract) and RELEASE\_STOCK (compensation: restore stock). Publishes to stock.events. |

## **payment/**

| File | Purpose |
| :---- | :---- |
| **app.py** | Flask routes \+ remove\_credit\_internal() \+ add\_credit\_internal(). Both called directly by kafka\_worker.py. |
| **kafka\_worker.py** | Consumes payment.commands. Handles PROCESS\_PAYMENT and REFUND\_PAYMENT. Publishes to payment.events. |

## **test/**

| File | Purpose |
| :---- | :---- |
| **test\_microservices.py** | Original template tests. Synchronous. Run against USE\_KAFKA=false. |
| **test\_kafka\_simple.py** | Async-aware tests for TRANSACTION\_MODE=simple. Polls /orders/status/\<id\> after each checkout and waits for completed or failed before asserting. |
| **utils.py** | Shared test helpers. Contains get\_order\_status() and find\_order() in addition to the original helpers. |

# **5\. Message Protocol (common/messages.py)**

Every Kafka message has the same base structure:

| Field | Description |
| :---- | :---- |
| **message\_id** | UUID — unique per message, used for idempotency checks |
| **tx\_id** | UUID — shared across all messages in one checkout flow |
| **order\_id** | The order this transaction belongs to |
| **type** | Message type constant (e.g. RESERVE\_STOCK, PAYMENT\_SUCCESS) |
| **timestamp** | Unix timestamp in milliseconds |
| **payload** | Domain-specific dict — items list, user\_id, amount, reason, etc. |

## **Saga Message Types**

| Type | Direction & Meaning |
| :---- | :---- |
| **RESERVE\_STOCK** | Order → Stock: subtract stock for all items (all-or-nothing) |
| **RELEASE\_STOCK** | Order → Stock: compensation — restore stock |
| **STOCK\_RESERVED** | Stock → Order: all items reserved successfully |
| **STOCK\_RESERVATION\_FAILED** | Stock → Order: at least one item had insufficient stock |
| **STOCK\_RELEASED** | Stock → Order: stock compensation confirmed |
| **PROCESS\_PAYMENT** | Order → Payment: charge the user |
| **REFUND\_PAYMENT** | Order → Payment: compensation — refund the user |
| **PAYMENT\_SUCCESS** | Payment → Order: charge successful |
| **PAYMENT\_FAILED** | Payment → Order: insufficient credit |
| **PAYMENT\_REFUNDED** | Payment → Order: refund confirmed |

## **2PC Message Types**

| Type | Direction & Meaning |
| :---- | :---- |
| **PREPARE\_STOCK / PAYMENT** | Order → Stock/Payment: tentatively hold resources |
| **COMMIT\_STOCK / PAYMENT** | Order → Stock/Payment: finalize the hold |
| **ABORT\_STOCK / PAYMENT** | Order → Stock/Payment: release the hold |
| **STOCK\_PREPARED / PAYMENT\_PREPARED** | Stock/Payment → Order: ready to commit |
| **STOCK\_PREPARE\_FAILED / PAYMENT\_PREPARE\_FAILED** | Stock/Payment → Order: cannot prepare |
| **STOCK\_COMMITTED / PAYMENT\_COMMITTED** | Stock/Payment → Order: committed |
| **STOCK\_ABORTED / PAYMENT\_ABORTED** | Stock/Payment → Order: aborted |

# **6\. Order Status State Machine**

Order status is stored in Redis as order:\<order\_id\>:status. Poll GET /orders/status/\<order\_id\> to read it. The state machine differs by protocol:

## **Saga States (SagaOrderStatus)**

| Status | Meaning |
| :---- | :---- |
| **pending** | Order created, checkout not yet called |
| **reserving\_stock** | RESERVE\_STOCK published, awaiting stock.events |
| **processing\_payment** | PROCESS\_PAYMENT published, awaiting payment.events |
| **compensating** | Compensation in flight (RELEASE\_STOCK or REFUND\_PAYMENT sent) |
| **completed** | Terminal success — stock deducted and payment taken |
| **failed** | Terminal failure — compensations complete |

## **2PC States (TwoPhaseOrderStatus)**

| Status | Meaning |
| :---- | :---- |
| **pending** | Checkout not yet called |
| **preparing\_stock** | PREPARE\_STOCK sent, awaiting response |
| **preparing\_payment** | PREPARE\_PAYMENT sent, awaiting response |
| **committing** | COMMIT sent to both services |
| **aborting** | ABORT sent to both services |
| **completed** | Terminal success — both committed |
| **failed** | Terminal failure — aborted |

# **7\. Simple Mode Flow (TRANSACTION\_MODE=simple)**

Simple mode is the Kafka equivalent of the original synchronous HTTP checkout. No saga or 2PC orchestration — just fire commands and react to events. Used to verify the Kafka plumbing works before implementing protocols.

## **Happy Path**

| 1\. | POST /orders/checkout/\<id\> | app.py calls start\_checkout() → simple.start\_checkout() publishes RESERVE\_STOCK to stock.commands |
| :---: | :---- | :---- |
| **2\.** | stock.commands | Stock service validates all items have sufficient stock (all-or-nothing), subtracts stock, publishes STOCK\_RESERVED to stock.events |
| **3\.** | stock.events → STOCK\_RESERVED | Order service event loop receives it, reads order from Redis, publishes PROCESS\_PAYMENT to payment.commands |
| **4\.** | payment.commands | Payment service deducts user credit, publishes PAYMENT\_SUCCESS to payment.events |
| **5\.** | payment.events → PAYMENT\_SUCCESS | Order service marks order paid=True in Redis, sets status=completed |
| **6\.** | GET /orders/status/\<id\> | Client polls and receives {status: completed} |

## **Stock Failure Path**

| 1\. | POST /orders/checkout/\<id\> | RESERVE\_STOCK published |
| :---: | :---- | :---- |
| **2\.** | stock.events → STOCK\_RESERVATION\_FAILED | Stock validation failed — nothing was deducted. Order service sets status=failed. No compensation needed. |
| **3\.** | GET /orders/status/\<id\> | Client receives {status: failed} |

## **Payment Failure Path (with rollback)**

| 1\. | POST /orders/checkout/\<id\> | RESERVE\_STOCK published |
| :---: | :---- | :---- |
| **2\.** | stock.events → STOCK\_RESERVED | Stock deducted successfully |
| **3\.** | payment.events → PAYMENT\_FAILED | Insufficient credit. Order service publishes RELEASE\_STOCK to restore stock. |
| **4\.** | stock.events → STOCK\_RELEASED | Stock restored. Order service sets status=failed. |
| **5\.** | GET /orders/status/\<id\> | Client receives {status: failed} |

# **8\. How kafka\_worker.py Works**

Each service has its own kafka\_worker.py. All follow the same pattern:

## **Order Service**

* init\_kafka(logger, db) — creates producer \+ consumer, starts background event loop thread

* Producer publishes to stock.commands and payment.commands

* Consumer subscribes to both stock.events and payment.events in one thread

* \_event\_loop() polls continuously and calls \_route\_event(msg) for each message

* \_route\_event() reads TRANSACTION\_MODE and delegates to the correct module in transaction\_modes/

* start\_checkout(order\_id, order\_entry) is the public API called by app.py

## **Stock and Payment Services**

* init\_kafka(logger) — creates producer \+ consumer, starts background consumer thread

* Consumer subscribes to a single commands topic

* \_route\_command() dispatches by message type to the correct handler

* Handlers call business logic functions imported from app.py (apply\_stock\_delta, remove\_credit\_internal, etc.)

* Results are published to the service's own events topic using builders from messages.py

# **9\. Implementing Saga and 2PC**

The handlers in transaction\_modes/saga.py and transaction\_modes/two\_pc.py are currently stubbed with pass. Each function has a TODO comment explaining exactly what it needs to do.

## **Each handler receives**

* msg — the full decoded Kafka message dict

* db — the Redis client (read order data, write status updates)

* publish — function(topic, message\_dict) to send a Kafka message

## **Useful helpers in messages.py**

* build\_reserve\_stock(tx\_id, order\_id, items) — returns ready-to-publish dict

* build\_process\_payment(tx\_id, order\_id, user\_id, amount)

* build\_release\_stock(tx\_id, order\_id, items) — compensation

* build\_refund\_payment(tx\_id, order\_id, user\_id, amount) — compensation

* All 2PC builders: build\_prepare\_stock, build\_commit\_stock, build\_abort\_stock, etc.

## **Redis keys written by order service**

| Key | Value |
| :---- | :---- |
| **order:\<id\>** | msgpack-encoded OrderValue — paid, items, user\_id, total\_cost |
| **order:\<id\>:status** | Current status string — see Section 6 |
| **order:\<id\>:tx\_id** | Transaction UUID — used by handlers to build follow-up messages |

# **10\. Known Limitations (Phase 1 TODO)**

* Saga and 2PC handlers are stubbed — simple mode only works end-to-end

* No fault tolerance yet — if a service crashes mid-transaction, state is lost

* No idempotency — duplicate Kafka messages will cause double-processing

* No timeout handling — a transaction can stay in reserving\_stock forever if stock service is down

# Kafka Debugging Guide

Useful commands when implementing and testing Saga and 2PC.
All commands assume your Kafka container is named `dds26-team19-kafka-1` and scripts live at `/opt/kafka/bin/`.

---

## 1. Find Your Kafka Container Name

```bash
docker ps | grep kafka
```

---

## 2. List All Topics

```bash
docker exec -it dds26-team19-kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --list
```

Expected topics:
```
stock.commands
stock.events
payment.commands
payment.events
```

---

## 3. Read All Messages From a Topic

```bash
# stock.commands
docker exec -it dds26-team19-kafka-1 /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic stock.commands \
  --from-beginning --timeout-ms 3000

# stock.events
docker exec -it dds26-team19-kafka-1 /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic stock.events \
  --from-beginning --timeout-ms 3000

# payment.commands
docker exec -it dds26-team19-kafka-1 /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic payment.commands \
  --from-beginning --timeout-ms 3000

# payment.events
docker exec -it dds26-team19-kafka-1 /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic payment.events \
  --from-beginning --timeout-ms 3000
```

> **Tip:** Run these right after triggering a checkout to trace the full message chain.
> If a topic is empty or missing messages, that's where the chain broke.

---

## 4. Check Consumer Group Lag

Lag > 0 means messages are sitting in the topic unread by that consumer group.

```bash
# Order service consumers (stock.events + payment.events)
docker exec -it dds26-team19-kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --describe --group order-service

# Stock service consumer (stock.commands)
docker exec -it dds26-team19-kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --describe --group stock-service

# Payment service consumer (payment.commands)
docker exec -it dds26-team19-kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --describe --group payment-service
```

What to look for:

| Column | Meaning |
|--------|---------|
| `LAG` | Messages in topic not yet consumed. Should be 0 at rest. |
| `CURRENT-OFFSET` | Last message this consumer processed |
| `LOG-END-OFFSET` | Last message written to topic |
| `CONSUMER-ID` | Which worker process owns this partition |

---

## 5. Check Service Logs

```bash
# All services at once
docker compose logs --tail=100 order-service
docker compose logs --tail=100 stock-service
docker compose logs --tail=100 payment-service

# Follow live (useful while running a test)
docker compose logs -f order-service
docker compose logs -f stock-service stock-service payment-service

# Filter for errors only
docker compose logs order-service | grep -i "error\|crash\|exception\|traceback"

# Filter for Kafka-specific logs
docker compose logs order-service | grep -i "OrderKafka\|kafka"
docker compose logs stock-service | grep -i "StockKafka\|kafka"
docker compose logs payment-service | grep -i "PaymentKafka\|kafka"
```

---

## 6. Check Order Status in Redis

Useful to see if the saga/2pc state machine is writing correctly.

```bash
# Get into the order service Redis
docker exec -it <order-redis-container> redis-cli -a <password>

# Then inside redis-cli:
GET order:<order_id>:status     # e.g. "reserving_stock", "completed", "failed"
GET order:<order_id>:tx_id      # the transaction UUID
GET <order_id>                  # raw msgpack — not human readable but confirms key exists
```

Or from outside the container:

```bash
docker exec -it <order-redis-container> redis-cli -a <password> GET order:<order_id>:status
```

---

## 7. Manually Produce a Test Message

Useful to test a single handler in isolation without going through the full checkout flow.

```bash
docker exec -it dds26-team19-kafka-1 /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 --topic stock.commands
```

Then paste a JSON message and hit Enter:
```json
{"message_id": "test-1", "tx_id": "test-tx-1", "order_id": "test-order-1", "type": "RESERVE_STOCK", "timestamp": 0, "payload": {"items": [{"item_id": "<real_item_id>", "quantity": 1}]}}
```

> **Note:** Use real item/order/user IDs that exist in Redis, otherwise handlers will fail silently.

---

## 8. Reset Consumer Group Offsets

If you want consumers to re-read all historical messages (e.g. after fixing a bug in a handler):

```bash
# Stop the service first, then reset
docker compose stop order-service

docker exec -it dds26-team19-kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --group order-service \
  --topic stock.events \
  --reset-offsets --to-earliest --execute

docker exec -it dds26-team19-kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --group order-service \
  --topic payment.events \
  --reset-offsets --to-earliest --execute

docker compose start order-service
```

> **Warning:** This replays all messages including old ones from previous test runs.
> Only use this in development, never in production.

---

## 9. Delete and Recreate Topics

Nuclear option — wipes all messages. Useful if topics have stale/corrupt data.

```bash
# Delete
docker exec -it dds26-team19-kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --delete --topic stock.commands

docker exec -it dds26-team19-kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --delete --topic stock.events

docker exec -it dds26-team19-kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --delete --topic payment.commands

docker exec -it dds26-team19-kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --delete --topic payment.events
```

Topics are recreated automatically when any service restarts (the `ensure_topics` call in `KafkaProducerClient.__init__`).

---

## 10. Debugging Checklist

When a test times out (status never reaches `completed` or `failed`):

**Step 1 — Is the message reaching the topic?**
Read `stock.commands` with `--from-beginning`. If the `RESERVE_STOCK` message isn't there, the order service isn't publishing. Check order service logs.

**Step 2 — Is the stock service consuming it?**
Check consumer group lag for `stock-service`. If lag > 0, the stock consumer is not running. Check stock service logs.

**Step 3 — Is the stock service replying?**
Read `stock.events`. If no `STOCK_RESERVED` or `STOCK_RESERVATION_FAILED`, the handler is crashing. Check stock service logs for errors.

**Step 4 — Is the order service consuming the reply?**
Check consumer group lag for `order-service`. If lag > 0, the order service consumer isn't running. Check order service logs.

**Step 5 — Is the handler writing to Redis?**
Check `order:<id>:status` in Redis. If the status hasn't changed from `reserving_stock`, the `_on_stock_reserved` handler ran but crashed before writing. Add a temporary `_logger.info()` at the top of the handler to confirm it's being called.

**Step 6 — Is payment working?**
Repeat steps 1-5 for `payment.commands` and `payment.events`.

---

## 11. Useful Log Level Tip

Gunicorn runs at INFO by default so `_logger.debug()` calls are silenced.
When debugging handlers, temporarily change `debug` to `info`:

```python
# In kafka_worker.py or transaction_modes/
_logger.info(f"[DEBUG] handler called: {msg.get('type')} order={msg.get('order_id')}")
```

Remember to change it back before submitting.