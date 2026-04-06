# Codebase Walkthrough

This document explains how the current branch is put together, with emphasis on the Redis primary/replica plus Sentinel topology that now backs the normal three-service architecture.

## 1. System shape

The system has three bounded business services:

- `order`: owns order state and coordinates checkout
- `stock`: owns inventory state
- `payment`: owns account balances

The public entry point is Nginx. Checkout is exposed as a synchronous HTTP request, but the internal coordination path is asynchronous and event-driven through Redis Streams.

Two transaction modes are implemented:

- Saga
- 2PC

The active mode is selected through `TRANSACTION_MODE`.

## 2. Request path

The main checkout flow is:

1. The client calls `POST /orders/checkout/<order_id>` on the gateway.
2. Nginx routes the request to `order/app.py`.
3. `order/app.py` loads the order from Redis and starts checkout.
4. `order/streams_worker.py` publishes commands to Redis Streams.
5. `stock/streams_worker.py` and `payment/streams_worker.py` consume those commands, apply local business logic, and publish reply events.
6. `order/streams_worker.py` consumes those reply events and advances the active Saga or 2PC state machine.
7. `order/checkout_notify.py` wakes the waiting HTTP handler once the order reaches a terminal state.

That gives the client a single blocking request while still keeping the distributed work decoupled internally.

## 3. Redis topology

Each bounded context has its own Redis deployment:

- `order-db` primary with `order-db-replica`
- `stock-db` primary with `stock-db-replica`
- `payment-db` primary with `payment-db-replica`

All three database pairs share one Sentinel quorum:

- `redis-sentinel-1`
- `redis-sentinel-2`
- `redis-sentinel-3`

### Why this matters

- service-local state remains isolated per bounded context
- a single Redis crash does not force the application back to a hardcoded dead host
- clients can reconnect to the elected primary after failover

### Where failover discovery lives

- `common/redis_client.py`

This file builds either:

- a direct `redis.Redis(...)` client when only `*_HOST` is configured
- a Sentinel-backed `master_for(...)` client when `*_SENTINELS` and `*_MASTER_NAME` are configured

That helper is now used by:

- `order/app.py`
- `payment/app.py`
- `stock/app.py`
- `order/streams_worker.py` for cross-context connections to stock and payment Redis

## 4. Configuration layout

### Compose files

- `docker/compose/docker-compose.yml`
  - baseline topology with one app instance per service plus the Redis HA layer
- `docker/compose/docker-compose.small.yml`
  - same logical topology, sized for local verification
- `docker/compose/docker-compose.medium.yml`
  - horizontal app-service scaling plus Redis HA
- `docker/compose/docker-compose.large.yml`
  - larger horizontal scaling plus Redis HA

### Environment files

- `env/order_redis.env`
- `env/stock_redis.env`
- `env/payment_redis.env`

These now include:

- `REDIS_SENTINELS`
- `REDIS_MASTER_NAME`

The older `REDIS_HOST` variables are still present as a direct-connection fallback.

### Gateway configuration

- `nginx/gateway_nginx.conf`
- `nginx/gateway_nginx.small.conf`
- `nginx/gateway_nginx.medium.conf`
- `nginx/gateway_nginx.large.conf`

The sized configs explicitly enumerate backend replicas so Nginx can spread traffic across them. In the small profile, that matters primarily for `order-service`, because `/orders/checkout/...` is the latency-sensitive path under load and now has three backend replicas plus a longer gateway read timeout.

## 5. Shared code

### `common/messages.py`

Defines the shared wire protocol:

- stream names
- command and event types
- message payload builders

### `common/streams_client.py`

Wraps Redis Streams behavior:

- publish
- blocking reads
- consumer-group acking
- orphaned pending-message recovery

### `common/worker_logging.py`

Normalizes restart-time and transient broker/database failures into clearer worker logs.

### `common/redis_client.py`

Owns Redis connection construction for both direct and Sentinel-backed modes.

## 6. Order service

### `order/app.py`

The HTTP layer for orders:

- create order
- add item
- find order
- get status
- checkout

It also owns the synchronous checkout wait path. The route blocks until the coordinator reaches a terminal result or times out.

### `order/checkout_notify.py`

This is the bridge between the background stream workers and the HTTP request thread. The worker completes the distributed transaction, and this module wakes the waiting request handler.

### `order/redis_helpers.py`

Small helper functions for order-state and 2PC Redis keys.

### `order/streams_worker.py`

This is the coordination core of the branch. It:

- starts the order-side consumers
- publishes commands to participant streams
- consumes stock and payment reply events
- dispatches to Saga or 2PC logic
- runs timeout and recovery loops
- reclaims orphaned pending stream messages

It also opens Sentinel-aware clients to:

- the order Redis primary
- the stock Redis primary
- the payment Redis primary

### `order/transactions_modes/saga/saga.py`

Implements the coordinator-side Saga state machine:

- reserve stock
- process payment
- compensate on failure
- recover unfinished Saga records

### `order/transactions_modes/saga/saga_record.py`

Stores durable Saga progress in Redis so a restarted coordinator can reconstruct in-flight work.

### `order/transactions_modes/two_pc.py`

Implements coordinator-side 2PC:

- prepare
- commit
- abort
- decision recovery

## 7. Stock service

### `stock/app.py`

Owns stock HTTP APIs and uses the shared Redis client helper for its local database connection.

### `stock/ledger.py`

Tracks participant-side command progress. The ledger is what keeps duplicate delivery and restart replay safe.

### `stock/streams_worker.py`

Consumes stock commands, applies local inventory operations, republishes replies, and reclaims orphaned pending messages after failures.

### `stock/transaction_modes/saga.py`

Implements Saga-side reserve and release behavior for inventory.

### `stock/transaction_modes/two_pc.py`

Implements prepare, commit, and abort for the stock participant.

## 8. Payment service

### `payment/app.py`

Owns user and balance APIs and uses the shared Redis client helper for its local database connection.

### `payment/ledger.py`

Participant ledger for payment operations.

### `payment/streams_worker.py`

Consumes payment commands, applies balance operations, republishes replies, and performs restart recovery.

### `payment/transactions_modes/saga.py`

Implements charge and refund behavior for Saga.

### `payment/transactions_modes/two_pc.py`

Implements prepare, commit, and abort for the payment participant.

## 9. Data ownership

### Order Redis

`order-db` stores:

- order records
- order status keys
- order transaction pointers
- Saga records
- 2PC coordinator state

### Stock Redis

`stock-db` stores:

- item price and stock data
- stock participant ledger entries
- stock command/event stream state

### Payment Redis

`payment-db` stores:

- user balances
- payment participant ledger entries
- payment command/event stream state

Replicas mirror the primaries and are promoted by Sentinel during failover.

## 10. Recovery model

The system relies on several layers of recovery:

- Redis Streams consumer groups for durable message delivery
- participant ledgers in `stock` and `payment` for idempotent replay
- coordinator-side durable transaction state in `order`
- orphan-claim loops for pending stream messages left behind by crashed consumers
- Redis replication plus Sentinel for primary failover per bounded context

This combination is what makes the branch robust under duplicate delivery, process restarts, and Redis failover.

## 11. Tests

### `test/test_microservices.py`

Basic end-to-end API smoke tests through the gateway.

### `test/test_kafka_saga.py`

Saga integration tests for the normal branch's asynchronous transport and recovery behavior.

### `test/test_kafka_saga_databases.py`

Database stop/kill recovery tests for Saga. These are especially relevant now that the Redis layer includes primaries, replicas, and Sentinel-driven reconnects.

### `test/test_2pc.py`

2PC integration tests for the checkout flow and coordinator recovery path.

## 12. Recommended reading order

If you want to understand the implementation quickly, read in this order:

1. `README.md`
2. `docs/explanation_architecture.md`
3. `common/messages.py`
4. `order/app.py`
5. `order/streams_worker.py`
6. `order/transactions_modes/saga/saga.py` or `order/transactions_modes/two_pc.py`
7. `stock/streams_worker.py`
8. `payment/streams_worker.py`
9. `common/redis_client.py`
