# Phase 2 Orchestrator Plan

## Goal

Extract the current checkout coordination logic out of `order-service` and into a separate `orchestrator-service`, while keeping the external shopping-cart API unchanged.

The key constraint is that phase 2 should not throw away the useful work already done in phase 1:

- Keep Redis Streams as the transport.
- Keep stock/payment participant ledgers and their crash-replay behavior.
- Keep the same external `/orders`, `/stock`, and `/payment` APIs.
- Remove protocol logic from `order-service` application code.

## Recommended approach

Do **not** try to build a fully generic workflow engine before the deadline.

The pragmatic phase 2 design is:

1. Add a new `orchestrator-service` and `orchestrator-db`.
2. Move the current order-side Saga and 2PC coordinator logic into the orchestrator.
3. Make `order-service` call the orchestrator on checkout instead of coordinating stock/payment itself.
4. Keep stock and payment services mostly unchanged, because they already behave like durable participants.

This gives you a real orchestration artifact without rewriting the whole system.

## Current architecture snapshot

The current repo already has most of the hard parts needed for phase 2:

- `order/app.py`
  Owns the public order API and currently starts checkout coordination.
- `order/streams_worker.py`
  Connects to `stock-db` and `payment-db`, publishes commands, consumes events, replays orphans, and runs timeout recovery.
- `order/transactions_modes/saga/`
  Contains the order-side Saga coordinator and durable saga records.
- `order/transactions_modes/two_pc.py`
  Contains the order-side 2PC coordinator and restart recovery.
- `stock/transaction_modes/*` and `payment/transactions_modes/*`
  Already implement participant behavior with idempotent ledgers and reply replay.
- `common/streams_client.py`
  Already gives you reliable Redis Streams transport with consumer groups, PEL draining, and orphan claiming.

That means phase 2 is mostly an extraction and ownership change, not a fresh redesign.

## Target architecture

### Responsibilities after the refactor

`order-service`

- Still owns order CRUD and public `/orders/*` endpoints.
- Still stores the order itself in `order-db`.
- No longer coordinates Saga or 2PC.
- On checkout, sends an internal request to `orchestrator-service`.
- Waits for terminal status by polling `order-db` status, not by using in-process callbacks.

`orchestrator-service`

- Owns transaction start, protocol state, recovery, and duplicate-checkout protection.
- Publishes commands to `stock.commands` and `payment.commands`.
- Consumes `stock.events` and `payment.events`.
- Runs either Saga or 2PC depending on `TRANSACTION_MODE`.
- Stores orchestration state in `orchestrator-db`.
- Mirrors the human-facing order status into `order-db` so existing `/orders/status/<id>` keeps working.

`stock-service` and `payment-service`

- Stay as participants.
- Keep their current Lua-based atomic apply logic.
- Keep their current ledger-based replay of unreplied events.
- Do not need to know whether the coordinator lives in order-service or orchestrator-service.

### State ownership

`order-db`

- Order blobs.
- `order:<order_id>:status`
- Optionally `order:<order_id>:tx_id` for observability.

`orchestrator-db`

- Active transaction claims per order.
- Saga records or 2PC coordinator state.
- Event deduplication markers.
- Timeout metadata.
- Any internal transaction history you want for debugging.

`stock-db` and `payment-db`

- Service-local business data.
- Service-local ledgers.
- Redis Streams topics and consumer groups.

### Why this split is good

- It keeps the public API stable.
- It gives you a real orchestration artifact for phase 2.
- It preserves your event-driven design and fault-tolerance work.
- It minimizes changes to stock/payment, which are already in a good place.

## Recommended internal API

Add an internal HTTP API on the orchestrator. Keep it small.

### `POST /transactions/checkout`

Request body:

```json
{
  "order_id": "uuid",
  "user_id": "user-123",
  "amount": 40,
  "items": [
    {"item_id": "a", "quantity": 2},
    {"item_id": "b", "quantity": 1}
  ]
}
```

Suggested response:

```json
{
  "accepted": true,
  "tx_id": "tx-uuid",
  "status": "reserving_stock",
  "reason": "started"
}
```

Other valid `reason` values:

- `already_in_progress`
- `already_completed`
- `already_failed`

### `GET /transactions/order/<order_id>`

Optional but useful for debugging. It can return:

- `tx_id`
- `protocol`
- `state`
- `failure_reason`
- `updated_at`

### `GET /health`

Simple health endpoint for Compose/Kubernetes.

## Checkout flow after refactor

### Order-service flow

1. `POST /orders/checkout/<order_id>` reads the order snapshot from `order-db`.
2. If status is already terminal, return immediately.
3. If status is already in progress, wait by polling `order:<order_id>:status`.
4. Otherwise call `POST /transactions/checkout` on the orchestrator.
5. Poll `order:<order_id>:status` until it reaches `completed` or `failed`, or timeout.
6. Return the same external HTTP semantics you already use.

### Orchestrator flow

1. Accept checkout request.
2. Atomically claim the order so only one active transaction exists per order.
3. Persist coordinator state before publishing any command.
4. Publish the first command for Saga or 2PC.
5. Consume stock/payment events and advance the state machine.
6. Write the current user-facing status into `order-db`.
7. On terminal state, clear the active claim for that order.

## Protocol abstraction

The orchestration artifact should have a generic shell and protocol-specific adapters.

Suggested shape:

```python
class ProtocolAdapter:
    def start(self, tx, store, publish, logger): ...
    def handle_event(self, msg, store, publish, logger): ...
    def recover(self, store, publish, logger): ...
    def check_timeouts(self, store, publish, logger): ...
```

Implementations:

- `SagaAdapter`
- `TwoPhaseCommitAdapter`

Important note:

- The abstraction should be around the orchestrator runtime.
- The internal Redis layout does not need to be perfectly unified on day 1.
- It is fine if Saga and 2PC still use different state records internally, as long as order-service no longer owns them.

## Concrete file plan

### New files/directories

Create:

- `orchestrator/app.py`
- `orchestrator/streams_worker.py`
- `orchestrator/requirements.txt`
- `orchestrator/Dockerfile`
- `orchestrator/protocols/saga/`
- `orchestrator/protocols/two_pc.py`
- `env/orchestrator_redis.env`

Optional but useful:

- `orchestrator/store.py`
- `orchestrator/http_client.py`
- `orchestrator/redis_helpers.py`

### Files to reuse or move

Start by copying, then refactor:

- `order/streams_worker.py` -> `orchestrator/streams_worker.py`
- `order/transactions_modes/saga/` -> `orchestrator/protocols/saga/`
- `order/transactions_modes/two_pc.py` -> `orchestrator/protocols/two_pc.py`

Reuse with little or no change:

- `common/streams_client.py`
- `common/messages.py`
- `stock/transaction_modes/*`
- `payment/transactions_modes/*`
- `stock/ledger.py`
- `payment/ledger.py`

### Files to simplify

`order/app.py`

- Remove direct imports of `start_checkout`, `init_streams`, and `close_streams`.
- Replace in-process checkout orchestration with an HTTP call to the orchestrator.
- Replace `checkout_notify` waiting with polling on `order:<order_id>:status`.

`order/checkout_notify.py`

- Likely becomes unnecessary once order-service no longer coordinates locally.

### Deployment files to modify

- `docker-compose.yml`
- Possibly `k8s/*` later if you also want the orchestrator on Kubernetes.

## Fault tolerance requirements

This is the most important part of the phase 2 plan.

### 1. Persist-before-publish

The orchestrator must keep the same rule your current Saga already follows:

- Write the intended next state to durable storage first.
- Publish the command second.

If the orchestrator crashes after the write but before publish, restart recovery can replay the command.

### 2. One active transaction per order

Use an active-order claim key in `orchestrator-db`, similar to the current Saga active tx pointer.

This prevents:

- duplicate client retries starting two checkouts
- stale events from an old transaction advancing a new one

### 3. Participant idempotency stays in the participants

Keep the current ledger behavior in stock/payment:

- duplicate commands are safe
- crash after local apply but before event publish is safe
- restart can replay stored reply events

This is already one of the strongest parts of your current design. Do not remove it.

### 4. Event deduplication in the orchestrator

Keep dedup markers keyed by `message_id` in the orchestrator state store.

This protects against:

- stream re-delivery
- orphan claim reprocessing
- duplicate replies after participant restart

### 5. Recovery on orchestrator startup

On startup, the orchestrator must scan for all non-terminal transactions and replay the last intended command or decision.

Saga:

- replay `RESERVE_STOCK`
- replay `PROCESS_PAYMENT`
- replay `RELEASE_STOCK`

2PC:

- if no decision was persisted yet, recompute based on current participant states
- if decision is `COMMIT`, replay missing commit commands
- if decision is `ABORT`, replay missing abort commands

### 6. Timeout scanning

Keep a periodic timeout scan in the orchestrator.

Suggested behavior:

- Saga: replay the command that is still awaiting a reply.
- 2PC: replay prepare/commit/abort commands depending on the persisted stage.

### 7. Order-service crashes should not matter

The order-service should become a thin API shell.

If it crashes while a client is waiting:

- the transaction must still continue in the orchestrator
- the client can retry `POST /orders/checkout/<order_id>` or poll `/orders/status/<order_id>`

That is a nice phase 2 improvement over in-process waiting.

### 8. Orchestrator request uncertainty

If order-service calls the orchestrator and the HTTP request fails, the result is uncertain:

- the orchestrator might not have received it
- or it might have started the transaction and the response got lost

To handle this safely:

1. re-read `order:<order_id>:status`
2. if it moved to an in-progress state, wait
3. otherwise retry the start request once or return a timeout-style failure

The start endpoint must be idempotent by order id.

### 9. Keep human-facing status in order-db

This is the easiest way to preserve the existing public API.

The orchestrator should write statuses like:

- `reserving_stock`
- `processing_payment`
- `compensating`
- `preparing_stock`
- `preparing_payment`
- `committing`
- `aborting`
- `completed`
- `failed`

Then `GET /orders/status/<order_id>` and `GET /orders/find/<order_id>` keep working with minimal changes.

## Recommended build order

This order is optimized for getting a usable foundation quickly.

### Step 1: Scaffold the orchestrator service

- Add `orchestrator-service` and `orchestrator-db` to `docker-compose.yml`.
- Add basic `app.py`, Dockerfile, requirements, and health endpoint.
- Give orchestrator access to `order-db`, `stock-db`, and `payment-db`.

### Step 2: Move Saga first

- Copy the current order-side Saga coordinator into the orchestrator.
- Copy the current order-side streams worker into the orchestrator.
- Make the orchestrator consume `stock.events` and `payment.events`.
- Make it update `order-db` status directly.

Why Saga first:

- it is simpler than 2PC
- it will validate the service boundary
- it will prove the extraction strategy works

### Step 3: Rewire order-service checkout

- Replace local `start_checkout()` usage with an HTTP request to orchestrator.
- Replace `checkout_notify` with polling on `order-db` status.
- Remove order-service stream initialization for checkout coordination.

After this step, order-service should no longer be the coordinator.

### Step 4: Port 2PC into the orchestrator

- Move `order/transactions_modes/two_pc.py` into the orchestrator.
- Keep the same commit/abort replay logic.
- Keep the current participant-side prepare/commit/abort implementation.

### Step 5: Tighten observability and cleanup

- Log `tx_id`, `order_id`, protocol, and state transitions consistently.
- Add optional `GET /transactions/order/<order_id>` debug endpoint.
- Add clear startup recovery logs.

### Step 6: Test failure cases

At minimum test:

- successful Saga checkout
- Saga payment failure with stock compensation
- successful 2PC checkout
- 2PC prepare failure
- orchestrator restart during in-flight transaction
- stock restart after local apply but before reply publish
- payment restart after local apply but before reply publish
- duplicate checkout request on the same order

## Suggested first milestone for Claude

If another model is going to make the initial implementation, this is the safest first slice:

1. Add `orchestrator-service` and `orchestrator-db`.
2. Move only the Saga coordinator into the orchestrator.
3. Make `order-service` call the orchestrator and poll status.
4. Keep 2PC temporarily untouched or behind a clear TODO.
5. Get end-to-end Saga tests passing again.

That gives a working phase 2 foundation quickly and avoids burning tokens on premature generalization.

## Definition of done

You can consider phase 2 structurally complete when all of the following are true:

- `order-service` no longer contains Saga or 2PC coordination logic.
- `order-service` no longer consumes `stock.events` or `payment.events`.
- `orchestrator-service` owns transaction lifecycle and recovery.
- Stock/payment services remain idempotent participants.
- Public `/orders/*`, `/stock/*`, and `/payment/*` APIs still behave the same.
- `docker-compose up --build` starts the whole system with the orchestrator in the loop.
- Existing correctness tests still pass.
- At least a few crash/restart scenarios are tested explicitly.

## Things to avoid

- Do not replace the event-driven design with synchronous REST calls from orchestrator to stock/payment.
- Do not remove participant ledgers.
- Do not over-generalize into a domain-independent workflow engine unless there is still time after the extraction works.
- Do not keep a second copy of protocol logic in order-service "just in case".

The clean story for the interview is much stronger if the orchestrator is the only coordinator.
