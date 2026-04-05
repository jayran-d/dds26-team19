# Codebase Walkthrough

This document explains the current codebase end to end: what each major file does, how requests move through the system, how Redis Streams are used, how Saga and 2PC differ, how atomicity and recovery are handled, and where performance optimizations live.

It reflects the current repository layout, including the moved configuration folders:

- Compose files live in `docker/compose/`
- Nginx configs live in `nginx/`

## 1. System Overview

This project is a distributed order-processing system with three business services:

- `order` coordinates checkout
- `stock` manages inventory
- `payment` manages user balances

The user-facing entry point is Nginx in front of the order service and the stock/payment services. The backend communication pattern is asynchronous and event-driven, with Redis used for both state storage and transport.

The architecture supports two transaction modes:

- Saga: the order service orchestrates a forward-only workflow with compensation if a later step fails
- 2PC: the order service coordinates a prepare / commit / abort workflow for stronger consistency

The active mode is selected by the `TRANSACTION_MODE` environment variable.

## 2. High-Level Request Flow

A checkout request follows this path:

1. The client sends `POST /orders/checkout/<order_id>` to Nginx.
2. Nginx forwards the request to the order service, stripping the `/orders` prefix.
3. `order/app.py` loads the order from Redis and delegates to `order/streams_worker.py`.
4. The order service publishes commands to Redis Streams.
5. Stock and payment services consume those commands, apply local business logic, and publish reply events.
6. The order service consumes reply events and advances the Saga or 2PC state machine.
7. The HTTP request returns once the order reaches a terminal state.

The request is synchronous from the client point of view, but the implementation is asynchronous internally.

## 3. File-by-File Map

### Root files

- `Makefile`
  - Convenience targets for `small`, `medium`, and `large` environments.
  - Wraps `docker compose` with the correct project name and Compose file.
  - `small-up`, `medium-up`, and `large-up` start the corresponding stack.

- `README.md`
  - Main entry point for the project overview.
  - Should point users to this guide for the detailed architecture explanation.

- `requirements.txt`
  - Shared Python dependencies.

- `deploy-charts-minikube.sh`
  - Installs the Redis / ingress dependencies needed for local Kubernetes testing.

- `deploy-charts-cluster.sh`
  - Same idea as the minikube script, but for a managed Kubernetes cluster.

### Compose and gateway configuration

- `docker/compose/docker-compose.yml`
  - Baseline Compose stack.
  - Defines the order, stock, payment, Redis, and gateway services.

- `docker/compose/docker-compose.small.yml`
  - Small profile: one instance of each app service.

- `docker/compose/docker-compose.medium.yml`
  - Medium profile: more replicas and a fixed CPU budget.

- `docker/compose/docker-compose.large.yml`
  - Large profile: maximum replica count and a larger CPU budget.

- `nginx/gateway_nginx.conf`
  - Baseline gateway config used by the default stack.

- `nginx/gateway_nginx.small.conf`
  - Gateway config for the small profile.

- `nginx/gateway_nginx.medium.conf`
  - Gateway config for the medium profile.

- `nginx/gateway_nginx.large.conf`
  - Gateway config for the large profile.

### Common code

- `common/messages.py`
  - Shared message protocol for the whole system.
  - Defines stream names, command/event constants, status constants, and message builder helpers.
  - This is the canonical definition of the wire protocol between services.

- `common/streams_client.py`
  - Redis Streams abstraction.
  - Wraps publish, read, ack, and orphan reclamation operations.
  - This is the core transport layer used instead of Kafka.

- `common/worker_logging.py`
  - Classifies transient worker exceptions.
  - Turns common Docker / Redis restart failures into clearer logs.

### Order service

- `order/app.py`
  - The external HTTP API for orders.
  - Implements create, find, status, add item, and checkout routes.
  - Coordinates the user-visible checkout response.

- `order/checkout_notify.py`
  - In-process synchronization between background transaction workers and the HTTP request handler.
  - Supports both `threading.Event` waiters and `asyncio.Future` waiters.

- `order/redis_helpers.py`
  - Small helper functions for order Redis access.
  - Includes the helper for the 2PC state key and status writes.

- `order/streams_worker.py`
  - Starts and manages the order service stream consumers.
  - Routes incoming stock/payment events to Saga or 2PC logic.
  - Runs timeout / recovery scanning for Saga.
  - Starts orphan recovery workers for Redis Streams PEL messages.

- `order/transactions_modes/saga/saga.py`
  - Saga coordinator for the order service.
  - Implements the Saga state machine, command publication, event routing, compensation, and recovery hooks.

- `order/transactions_modes/saga/saga_record.py`
  - Durable Saga state stored in Redis.
  - Tracks current state, last published command, awaited event, compensation flags, timeout, and active transaction ownership.

- `order/transactions_modes/two_pc.py`
  - 2PC coordinator for the order service.
  - Maintains the coordinator state machine, publishes prepare/commit/abort commands, and recovers incomplete decisions.

### Stock service

- `stock/app.py`
  - HTTP API for stock management.
  - Implements item creation, batch initialization, lookup, add stock, and subtract stock.
  - Exposes the business logic used by the message worker.

- `stock/ledger.py`
  - Durable participant ledger for stock commands.
  - Stores the local progress of each command so crashes can be replayed safely.

- `stock/streams_worker.py`
  - Redis Streams consumer / publisher loop for stock commands and events.
  - Replays unreplied ledger entries on startup.
  - Reclaims orphaned pending messages from crashed consumers.

- `stock/transaction_modes/saga.py`
  - Stock-side Saga participant logic.
  - Handles reserve and release commands, then publishes corresponding events.

- `stock/transaction_modes/two_pc.py`
  - Stock-side 2PC participant logic.
  - Handles prepare, commit, and abort commands.

### Payment service

- `payment/app.py`
  - HTTP API for users and balances.
  - Implements user creation, batch initialization, lookup, add funds, and pay.
  - Exposes the business logic used by the message worker.

- `payment/ledger.py`
  - Durable participant ledger for payment commands.
  - Same structure as `stock/ledger.py`, but namespaced for payment.

- `payment/streams_worker.py`
  - Redis Streams consumer / publisher loop for payment commands and events.
  - Replays unreplied ledger entries on startup.
  - Reclaims orphaned pending messages from crashed consumers.

- `payment/transactions_modes/saga.py`
  - Payment-side Saga participant logic.
  - Handles process-payment and refund commands.

- `payment/transactions_modes/two_pc.py`
  - Payment-side 2PC participant logic.
  - Handles prepare, commit, and abort commands.

### Tests and docs

- `test/`
  - Unit and integration tests for the checkout flows and service APIs.
  - Includes Saga and 2PC coverage.

- `tests_external/`
  - External stress / consistency test harnesses.

- `docs/compose_scaling.md`
  - Explains the small / medium / large Compose profiles and CPU budgets.

- `docs/architecture_guide.md`
  - Shorter architecture summary.

## 4. What Each Service Owns

### Order service owns coordination

The order service is the orchestrator. It does not directly change stock or payment state through local database writes. Instead, it:

- validates the request
- loads the order from Redis
- starts the selected transaction mode
- publishes commands to the participant services
- waits for the terminal result
- exposes the final status to the client

The order service is also responsible for making the checkout request appear synchronous to the client even though the underlying work is asynchronous.

### Stock service owns inventory state

The stock service is the authority for stock counts. It:

- stores item stock and price in Redis
- accepts business commands from Redis Streams
- applies inventory changes locally
- publishes reply events for the order service

### Payment service owns account balances

The payment service is the authority for user credit. It:

- stores user balances in Redis
- accepts payment commands from Redis Streams
- applies credit changes locally
- publishes reply events for the order service

## 5. Data Storage Model

### Order data

Order records are stored in the order Redis instance as msgpack-encoded `OrderValue` objects.

The order service also stores:

- `order:<order_id>:status`
- `order:<order_id>:tx_id`
- `order:<order_id>:2pcstate`
- Saga record state under `saga:record:<tx_id>`
- Saga active pointer under `saga:active:<order_id>`

### Stock data

Stock records are stored in the stock Redis instance. The current implementation stores item data in a Redis hash with fields like:

- `stock`
- `price`

The stock service also stores its participant ledger entries in Redis.

### Payment data

Payment records are stored in the payment Redis instance. The current implementation stores user balances as Redis values and also keeps participant ledger entries in Redis.

## 6. Redis Streams Design

Redis Streams are the transport layer for all cross-service messaging.

The topics are:

- `stock.commands`
- `stock.events`
- `payment.commands`
- `payment.events`

Ownership is split like this:

- Order writes commands
- Stock and payment read commands
- Stock and payment write events
- Order reads events

### Why Streams are used

Streams provide:

- append-only message logs
- consumer groups for load balancing
- pending-entry tracking for crash recovery
- explicit acknowledgements
- orphan claiming for stuck messages

### Why the wrapper exists

`common/streams_client.py` isolates all Redis Streams operations behind a small API:

- `publish()`
- `read_many()`
- `ack()` / `ack_many()`
- `claim_orphans()`

That keeps the service code focused on business logic rather than transport details.

## 7. How Checkout Works in Saga Mode

Saga mode is the default distributed transaction strategy for the order service.

### Step 1: checkout starts

`order/app.py` receives `POST /orders/checkout/<order_id>` and calls `start_checkout()` from `order/streams_worker.py`.

### Step 2: Saga record is created

`saga_start_checkout()` in `order/transactions_modes/saga/saga.py` creates a durable Saga record and claims the order as active.

This record stores:

- transaction id
- order id
- user id
- amount
- item list
- current state
- last command sent
- awaited event
- compensation flags
- timeout deadline

### Step 3: first command is published

The order service publishes `RESERVE_STOCK` to `stock.commands`.

### Step 4: stock service responds

The stock worker consumes the command, applies local logic, writes its ledger entry, updates stock if possible, and publishes one of:

- `STOCK_RESERVED`
- `STOCK_RESERVATION_FAILED`
- `STOCK_RELEASED`

### Step 5: order service advances the Saga

The order worker receives the stock event and routes it through `saga_route_order()`.

If stock succeeds:

- the Saga transitions to payment processing
- `PROCESS_PAYMENT` is published

If stock fails:

- the Saga transitions to failed
- checkout ends

If payment fails:

- the Saga transitions to compensating
- `RELEASE_STOCK` is published

### Step 6: compensation or completion

If compensation completes, the order becomes `failed`.
If payment succeeds, the order becomes `completed`.

### Why this is safe

Saga is safe because every state transition is durably written before the next command is published. If the service crashes after persisting the transition but before publishing, startup recovery can replay the missing command.

## 8. How Checkout Works in 2PC Mode

2PC mode uses a coordinator-driven prepare / commit / abort protocol.

### Step 1: checkout starts

`_2pc_start_checkout()` in `order/transactions_modes/two_pc.py` creates a new transaction id, stores initial coordinator state, and publishes prepare commands.

### Step 2: both participants prepare

The order service sends:

- `PREPARE_STOCK`
- `PREPARE_PAYMENT`

Each participant replies with either prepared or failed events.

### Step 3: coordinator decides

The order-side 2PC logic tracks:

- stock state
- payment state
- global decision
- commit confirmation states

When both participants are ready, the coordinator commits.
If either participant fails, it aborts.

### Step 4: commit or abort commands are sent

The order service publishes either:

- `COMMIT_STOCK` / `COMMIT_PAYMENT`
- `ABORT_STOCK` / `ABORT_PAYMENT`

### Step 5: confirmations arrive

The coordinator waits for both confirmations before marking the order terminal and notifying the waiting HTTP request.

### Why Lua is used here

The 2PC coordinator uses Redis Lua scripts to update state atomically. That prevents race conditions between concurrent event handlers and ensures the coordinator can make the commit/abort decision exactly once.

## 9. Atomicity and Lua Scripts

Lua is used where a state transition must be atomic inside Redis.

### Saga record atomic claim

`order/transactions_modes/saga/saga_record.py` uses Redis WATCH / MULTI / EXEC to atomically:

- claim the order as active
- create the Saga record

This prevents two simultaneous checkout requests for the same order from both starting independent transactions.

### Saga active-pointer cleanup

The active transaction pointer is cleared only if it still points to the current tx id. That prevents a finished transaction from deleting a newer one.

### Participant ledgers

`stock/ledger.py` and `payment/ledger.py` use durable ledger entries to make command handling idempotent and crash-safe.

The important gap is:

- `received` → command arrived
- `applied` → local business effect committed
- `replied` → reply event published

The transition from `applied` to `replied` is the crash-sensitive part.

A Lua script is used to mark an entry as `replied` atomically after the reply has been published.

### 2PC coordinator scripts

`order/transactions_modes/two_pc.py` uses Lua scripts to atomically:

- record participant readiness
- decide commit vs abort once
- track commit confirmation state

This avoids race conditions where multiple workers process incoming events concurrently.

## 10. checkout_notify and Async Waiting

`order/checkout_notify.py` is the bridge between background workers and the HTTP request handler.

### Why it exists

The order service needs to hold the HTTP request open until the async transaction finishes, but the actual transaction work happens in background threads.

### How it works

- `register_async(order_id)` creates an `asyncio.Future` attached to the current event loop.
- `notify(order_id)` resolves all waiters for that order.
- `unregister_async(order_id, fut)` removes the waiter when the request finishes or errors.

### Who calls it

- Saga terminal transitions call `checkout_notify.notify(order_id)`
- 2PC terminal transitions call `checkout_notify.notify(order_id)`

### Why this is thread-safe

The background workers run in non-async threads. `notify()` uses `loop.call_soon_threadsafe()` so the async future is resolved safely on the correct event loop.

## 11. Participant Ledgers

The stock and payment services both use a ledger pattern to make command handling durable and recoverable.

### Why ledgers exist

A participant can crash in the middle of processing a command. Without a ledger, it would be hard to know:

- whether the business effect was already applied
- whether the reply event was already published
- whether the command should be replayed after restart

### Ledger lifecycle

1. Command arrives
2. Ledger entry is created in `received`
3. Business state is updated locally
4. Ledger entry becomes `applied`
5. Reply event is published
6. Ledger entry becomes `replied`

### Startup recovery

On startup, each participant scans for `applied` entries that were never marked `replied`, then republishes the stored reply message.

This closes the crash window between applying a local effect and sending the response event.

## 12. Message Idempotency and Duplicate Handling

The system is built to tolerate duplicate delivery.

### Stream layer

Redis Streams consumer groups may redeliver pending messages after a crash or when orphan recovery claims them.

### Order-side dedup

The Saga record stores processed message ids so duplicate events can be ignored.

### Participant-side dedup

The ledger key is based on `tx_id` and `action_type`, and `SET NX` prevents the same command from overwriting a more advanced ledger entry.

### Why this matters

Without dedup, retries and recovery could cause duplicate deductions or duplicate refunds.

## 13. Performance Considerations

This system is tuned for throughput in a few important ways.

### Redis Streams instead of direct synchronous calls

Commands and events are batched and processed asynchronously. That allows service instances to scale independently.

### Multiple workers per service

The stream workers start several consumer threads per service, controlled by `CONSUMER_WORKERS`.

### Batch reads

Workers use `read_many()` with a batch size so they can acknowledge multiple messages together.

### Connection pooling

`redis-py` connection pools are shared across worker threads.

### Msgpack payloads

Stream messages are encoded with msgpack instead of verbose JSON.

### Upstream keepalive

The Nginx configs use upstream keepalive pools to reduce TCP connection churn.

### Less logging overhead

Access logging is disabled in the Nginx configs for the gateway path to reduce I/O.

### Why not retry everything

The gateway now avoids aggressive upstream retries on checkout flows because a retry can amplify load or duplicate side effects on non-idempotent operations.

## 14. Failure Handling

The design handles several failure classes.

### Client-visible business failures

Examples:

- item out of stock
- user has insufficient credit
- order already completed

These should return normal HTTP responses such as `400` or `200` depending on the outcome.

### Worker restarts

The workers can restart and recover pending stream messages from the PEL.

### Redis / dependency restarts

The workers scan for orphaned pending entries and replay unreplied ledger entries.

### Order-service recovery

The order service can recover in-flight Saga or 2PC state on startup.

### Nginx and Docker DNS behavior

The gateway configs use Docker DNS resolution so service names remain valid when container IPs change.

## 15. What Each API Does

### Order API

- `POST /create/<user_id>`
  - Create a blank order.

- `POST /batch_init/<n>/<n_items>/<n_users>/<item_price>`
  - Seed many example orders.

- `GET /find/<order_id>`
  - Return order details, including computed paid state.

- `GET /status/<order_id>`
  - Return the current checkout status.

- `POST /addItem/<order_id>/<item_id>/<quantity>`
  - Add an item to an order and update total cost.

- `POST /checkout/<order_id>`
  - Start Saga or 2PC checkout and wait for the terminal state.

### Stock API

- `POST /item/create/<price>`
  - Create a new stock item.

- `POST /batch_init/<n>/<starting_stock>/<item_price>`
  - Seed stock data.

- `GET /find/<item_id>`
  - Return current stock and price.

- `POST /add/<item_id>/<amount>`
  - Increase stock.

- `POST /subtract/<item_id>/<amount>`
  - Decrease stock.

### Payment API

- `POST /create_user`
  - Create a new user with zero credit.

- `POST /batch_init/<n>/<starting_money>`
  - Seed user balances.

- `GET /find_user/<user_id>`
  - Return current credit.

- `POST /add_funds/<user_id>/<amount>`
  - Increase credit.

- `POST /pay/<user_id>/<amount>`
  - Decrease credit.

## 16. Tests

The `test/` directory validates the main flows:

- happy path checkout
- out-of-stock failure
- insufficient-credit failure
- multiple items in one order
- duplicate checkout behavior
- Saga integration scenarios
- 2PC behavior and recovery

The external tests in `tests_external/` are intended for broader stress and consistency checks.

## 17. Operational Notes

- Only run one Compose profile at a time unless you change host ports.
- The root `make` target builds the small profile by default.
- Use `make small-up`, `make medium-up`, or `make large-up` to start the desired stack.
- Because the order service waits for terminal transaction completion, slow backend recovery can make checkout requests block until timeout.

## 18. Short Mental Model

If you want one concise mental model for the whole repo, it is this:

- Nginx routes incoming HTTP requests to the correct service.
- Order coordinates transactions.
- Stock and payment own their own data.
- Redis Streams carry commands and events.
- Ledgers and Saga records make recovery safe.
- Lua scripts close atomicity gaps.
- checkout_notify bridges background worker threads back to the async HTTP response.

That is the core of the system.
