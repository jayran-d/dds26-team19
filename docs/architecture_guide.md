# Architecture Guide

This branch supports both `saga` and `2pc` transaction modes on top of the same
Redis Streams transport.

## Current Split

The split still makes sense after the merge:

- `order/app.py`
  External HTTP API, request validation, and the blocking checkout wait path.

- `order/streams_worker.py`
  Transport wiring for order-service. It owns Redis Stream consumers, publish
  routing, timeout recovery startup hooks, and dispatches incoming events to the
  active transaction mode.

- `order/transactions_modes/saga/`
  Order-side Saga orchestration and durable saga record storage.

- `order/transactions_modes/two_pc.py`
  Order-side 2PC coordinator logic and recovery for incomplete 2PC decisions.

- `stock/transaction_modes/saga.py`
  Stock participant logic for Saga.

- `stock/transaction_modes/two_pc.py`
  Stock participant logic for 2PC prepare / commit / abort.

- `payment/transactions_modes/saga.py`
  Payment participant logic for Saga.

- `payment/transactions_modes/two_pc.py`
  Payment participant logic for 2PC prepare / commit / abort.

- `common/messages.py`
  Shared protocol constants and message builders for both modes.

## End-to-End Checkout

1. The client calls `POST /orders/checkout/{order_id}`.
2. `order/app.py` loads the order and current status, then delegates to
   `start_checkout()` in `order/streams_worker.py`.
3. `order/streams_worker.py` chooses either:
   - Saga start logic, or
   - 2PC start logic
4. Stock and payment services consume commands from their own Redis Streams,
   apply local business changes, and publish reply events.
5. Order consumes the reply events and advances the chosen protocol.
6. The HTTP request returns only once the order has reached a terminal state.

## Logging Guide

- `... crashed unexpectedly: ...`
  A real unexpected worker exception.

- `... dependency hostname is temporarily missing from Docker DNS; retrying`
  Expected during service or Redis restarts. Docker briefly removed the name.

- `... Redis closed the connection during a restart/shutdown; retrying`
  Expected while Redis is going away cleanly.

- `... Redis reset the connection; retrying`
  Expected during abrupt Redis restarts.

- `... dependency is not accepting connections yet; retrying`
  Expected while a dependent service is still booting.

- `gateway exited with code 0 (restarting)`
  Usually container lifecycle noise from local `docker compose up` / restart
  activity, not an application exception by itself.

- `host not found in upstream ...`
  Nginx started before a dependent service name was resolvable in Docker.
