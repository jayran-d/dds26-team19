# DDS26 Team 19

This branch contains our orchestrator-based implementation of the Distributed Data Systems shopping-cart project.

The system is a microservice application with decentralized data ownership, asynchronous coordination through Redis Streams, and two distributed transaction protocols:

- Saga
- Two-Phase Commit

The main goal of the implementation is to preserve consistency under concurrency and failures while still scaling across small, medium, and large deployments.

## What This Branch Contains

This version of the project moves the transaction coordination logic into a dedicated `orchestrator-service`.

Runtime services:

- `gateway`: public HTTP entrypoint on port `8000`
- `order-service`: order creation, order contents, and checkout API
- `orchestrator-service`: owns distributed transaction coordination
- `stock-service`: owns stock data and stock-side transaction effects
- `payment-service`: owns credit data and payment-side transaction effects

Databases:

- `order-db`
- `orchestrator-db`
- `stock-db`
- `payment-db`

Each service owns its own Redis database. There is no shared business database.

## Architecture Summary

### 1. Decentralized data ownership

Each service owns its own data and only applies its own local business changes:

- orders are stored in `order-db`
- orchestrator transaction state is stored in `orchestrator-db`
- stock is stored in `stock-db`
- user credit is stored in `payment-db`

This is the central consistency challenge of the project: we coordinate transactions across service boundaries without collapsing back into one shared data store.

### 2. Event-driven coordination

Internal communication between services uses Redis Streams.

Important streams:

- `stock.commands`
- `stock.events`
- `payment.commands`
- `payment.events`

That gives us:

- asynchronous processing
- consumer groups
- pending-entry recovery
- replay/recovery after failures

The shared message contract is defined in [common/messages.py](common/messages.py).

### 3. Order service stays thin

`order-service` owns order data, but it does not own the distributed protocol anymore.

Checkout flow at a high level:

1. client calls `POST /orders/checkout/<order_id>`
2. `order-service` validates the order and starts a transaction through `orchestrator-service`
3. `orchestrator-service` publishes commands to stock and payment
4. stock/payment workers apply local effects and publish reply events
5. the orchestrator advances the Saga or 2PC state machine
6. final checkout state is written durably into `order-db`
7. `order-service` waits on that shared durable state before returning `200` or `400`

This design is important because it keeps the external HTTP response aligned with the durable transaction result.

### 4. Saga and 2PC are both implemented

Saga:

- reserve stock first
- process payment second
- compensate stock if payment fails

2PC:

- coordinator creates a prepare phase
- participants respond ready or failed
- coordinator commits or aborts
- recovery logic completes incomplete transactions after crashes

The orchestrator protocol implementations are in:

- [orchestrator/protocols/saga/saga.py](orchestrator/protocols/saga/saga.py)
- [orchestrator/protocols/two_pc.py](orchestrator/protocols/two_pc.py)

### 5. Fault tolerance and idempotency

We assume duplicate messages, retries, and container failures are normal.

The main mechanisms are:

- participant-side ledgers in [stock/ledger.py](stock/ledger.py) and [payment/ledger.py](payment/ledger.py)
- durable order status in `order-db`
- transaction records in `orchestrator-db`
- stream orphan recovery
- timeout-based recovery loops

This is what allows the system to recover when a service or database is killed in the middle of a transaction.

### 6. Leader election for scaled orchestrators

Medium and large deployments run multiple orchestrator replicas. Event handling can be distributed, but timeout scanning and startup recovery should only be done by one leader at a time.

That is handled by:

- [orchestrator/leader_lease.py](orchestrator/leader_lease.py)

The lease is stored in `orchestrator-db` with TTL renewal, so another replica can take over after a leader failure.

## Most Important Files

If you want to inspect the implementation directly, these are the best entry points:

1. [common/messages.py](common/messages.py)
2. [common/streams_client.py](common/streams_client.py)
3. [order/app.py](order/app.py)
4. [orchestrator/app.py](orchestrator/app.py)
5. [orchestrator/streams_worker.py](orchestrator/streams_worker.py)
6. [orchestrator/leader_lease.py](orchestrator/leader_lease.py)
7. [orchestrator/protocols/saga/saga.py](orchestrator/protocols/saga/saga.py)
8. [orchestrator/protocols/saga/saga_record.py](orchestrator/protocols/saga/saga_record.py)
9. [orchestrator/protocols/two_pc.py](orchestrator/protocols/two_pc.py)
10. [stock/ledger.py](stock/ledger.py)
11. [payment/ledger.py](payment/ledger.py)

## Deployment

The repository includes three Docker Compose deployments:

- small
- medium
- large

Files:

- [docker/compose/docker-compose.small.yml](docker/compose/docker-compose.small.yml)
- [docker/compose/docker-compose.medium.yml](docker/compose/docker-compose.medium.yml)
- [docker/compose/docker-compose.large.yml](docker/compose/docker-compose.large.yml)

Gateway configs:

- [nginx/gateway_nginx.small.conf](nginx/gateway_nginx.small.conf)
- [nginx/gateway_nginx.medium.conf](nginx/gateway_nginx.medium.conf)
- [nginx/gateway_nginx.large.conf](nginx/gateway_nginx.large.conf)

Useful commands from the repo root:

```bash
make small-up-saga
make small-up-2pc
make medium-up-saga
make medium-up-2pc
make large-up-saga
make large-up-2pc
make small-down
make medium-down
make large-down
```

## Testing

Unit and protocol tests:

```bash
make unit-saga
make unit-2pc
```

Consistency harness:

```bash
make consistency
```

Stress setup and Locust:

```bash
make stress-init
make stress-locust
```

Headless Locust run:

```bash
make stress-headless
```

The complete testing notes are in [docs/testing_guide.md](docs/testing_guide.md).

## Additional Documentation

- Architecture explanation: [docs/explanation_architecture.md](docs/explanation_architecture.md)
- Testing guide: [docs/testing_guide.md](docs/testing_guide.md)
- Compose/scaling notes: [docs/compose_scaling.md](docs/compose_scaling.md)
