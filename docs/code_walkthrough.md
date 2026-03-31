# Code Walkthrough

This is the best reading order if you want to understand the current system
without bouncing around the repo.

## 1. Deployment Shape

Start with:

- `docker-compose.yml`
- `gateway_nginx.conf`

What to look for:

- which containers exist
- which Redis belongs to which service
- how `order-service` depends on `orchestrator-service`
- health checks, restart policy, and worker settings

## 2. Shared Protocol

Then read:

- `common/messages.py`

Main things to understand:

- stream names: `stock.commands`, `stock.events`, `payment.commands`, `payment.events`
- user-facing status enums for Saga and 2PC
- command and event builders
- 2PC decision-state constants

If you understand this file, the rest of the code becomes much easier to read.

## 3. Shared Transport Layer

Then read:

- `common/streams_client.py`

Focus on these methods:

- `ensure_group()`
- `publish()`
- `read_many()`
- `ack_many()`
- `claim_orphans()`

What they mean:

- `read_many(... pending=False)` reads new messages
- `read_many(... pending=True)` drains already-read-but-unacked messages
- `ack_many()` removes processed messages from the Pending Entries List
- `claim_orphans()` steals old pending messages from dead consumers

## 4. Public Order API

Then read:

- `order/app.py`

Focus on:

- `get_order_from_db()`
- `get_order_status()`
- `_poll_for_terminal_status()`
- `_resolve_uncertain_checkout_start()`
- `checkout()`

What this file does:

- stores and loads orders
- exposes the external order API
- starts checkout by calling the orchestrator
- waits on `order:<id>:status` until the transaction finishes

Important idea:

`order-service` no longer coordinates Saga or 2PC itself.

## 5. Orchestrator Entry Point

Then read:

- `orchestrator/app.py`

Focus on:

- `POST /transactions/checkout`
- `GET /health`

This file is intentionally thin. It just validates input and delegates to the
Streams worker.

## 6. Orchestrator Runtime Wiring

Then read:

- `orchestrator/streams_worker.py`

Focus on:

- `init_streams()`
- `_route_event()`
- `_event_worker()`
- `_orphan_recovery_worker()`
- `_timeout_loop()`
- `start_checkout()`

This is the real coordinator runtime shell. It:

- opens Redis connections
- starts worker threads
- reads stock and payment events
- dispatches to Saga or 2PC code
- runs timeout recovery in Saga mode
- reclaims orphaned messages

## 7. Saga Coordinator Logic

Then read:

- `orchestrator/protocols/saga/saga_record.py`
- `orchestrator/protocols/saga/saga.py`

Read `saga_record.py` first.

Focus in `saga_record.py` on:

- `create_if_no_active()`
- `load_event_context()`
- `transition()`
- `mark_seen()`
- `get_timed_out()`

Focus in `saga.py` on:

- `saga_start_checkout()`
- `saga_route_order()`
- `_on_stock_reserved()`
- `_on_payment_failed()`
- `recover()`
- `check_timeouts()`

Key mental model:

- the saga record is the durable coordinator state
- state is written before the next command is published
- duplicate and stale events are dropped
- timeout recovery republishes the intended next command

## 8. 2PC Coordinator Logic

Then read:

- `orchestrator/protocols/two_pc.py`

Focus on:

- `_create_if_no_active()`
- `_update_participant_and_maybe_decide()`
- `_handle_commit_confirmation()`
- `_finish()`
- `recover_incomplete_2pc()`
- `_2pc_start_checkout()`
- `_2pc_route_order()`

Key mental model:

- `2pc:active:<order_id>` prevents duplicate active checkouts
- `order:<order_id>:2pcstate` stores participant readiness and decision state
- prepare replies drive `COMMIT` or `ABORT`
- commit/abort confirmations finalize the transaction
- recovery republishes unfinished work after a crash

## 9. Stock Participant

Then read:

- `stock/app.py`
- `stock/ledger.py`
- `stock/streams_worker.py`
- `stock/transaction_modes/saga.py`
- `stock/transaction_modes/two_pc.py`

Best order:

1. `stock/app.py`
2. `stock/ledger.py`
3. `stock/transaction_modes/saga.py`
4. `stock/transaction_modes/two_pc.py`
5. `stock/streams_worker.py`

Focus on:

- local stock mutation logic in `apply_stock_delta()`
- ledger states `received`, `applied`, `replied`
- command routing in the transaction mode files
- startup replay of unreplied entries

## 10. Payment Participant

Then read:

- `payment/app.py`
- `payment/ledger.py`
- `payment/transactions_modes/saga.py`
- `payment/transactions_modes/two_pc.py`
- `payment/streams_worker.py`

This is the same structure as stock, but for user credit.

## 11. Tests

Read these in this order:

- `test/test_microservices.py`
- `test/test_streams_simple.py`
- `test/test_streams_saga.py`
- `test/test_streams_saga_databases.py`
- `test/test_2pc.py`
- `test/test_2pc_databases.py`

Why this order:

- start with basic user-visible correctness
- then read Saga path and Saga chaos tests
- then read 2PC path and 2PC chaos tests
