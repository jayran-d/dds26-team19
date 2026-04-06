# DDS26 Team 19

This branch contains our orchestrator-based implementation of the Distributed Data Systems shopping-cart project.

The system is a microservice application with decentralized data ownership, asynchronous coordination through Redis Streams, Redis primary/replica storage with Sentinel failover, and two distributed transaction protocols:

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

- `order-db` + `order-db-replica`
- `orchestrator-db` + `orchestrator-db-replica`
- `stock-db` + `stock-db-replica`
- `payment-db` + `payment-db-replica`

Each bounded context owns its own Redis high-availability group. There is no shared business database.
Services discover the current primary through the shared Redis Sentinel quorum
(`redis-sentinel-1/2/3`), so writes can move to a promoted replica after a primary failure.
Replication is still asynchronous, so consistency still depends on idempotency, replay, and durable recovery logic on top of the database layer.

## Architecture Summary

### 1. Decentralized data ownership

Each service owns its own data and only applies its own local business changes:

- orders are stored in the `order-db` replication group
- orchestrator transaction state is stored in the `orchestrator-db` replication group
- stock is stored in the `stock-db` replication group
- user credit is stored in the `payment-db` replication group

This is the central consistency challenge of the project: we coordinate transactions across service boundaries without collapsing back into one shared data store.

Each replication group is still one logical store owned by one bounded context. Adding replicas improves availability, but it does not change who owns the writes.

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
- Sentinel-based primary discovery in [common/redis_client.py](common/redis_client.py)
- Redis primary/replica failover through Sentinel
- AOF persistence on both primaries and replicas
- asynchronous replication, so replay and idempotency are still required after failover

This is what allows the system to recover when a service or database is killed in the middle of a transaction.

### 6. Leader election for scaled orchestrators

Medium and large deployments run multiple orchestrator replicas. Event handling can be distributed, but timeout scanning and startup recovery should only be done by one leader at a time.

That is handled by:

- [orchestrator/leader_lease.py](orchestrator/leader_lease.py)

The lease is stored in `orchestrator-db` with TTL renewal, so another replica can take over after a leader failure.

## Most Important Files

If you want to inspect the implementation directly, these are the best entry points:

1. [common/messages.py](common/messages.py)
2. [common/redis_client.py](common/redis_client.py)
3. [common/streams_client.py](common/streams_client.py)
4. [order/app.py](order/app.py)
5. [orchestrator/app.py](orchestrator/app.py)
6. [orchestrator/streams_worker.py](orchestrator/streams_worker.py)
7. [orchestrator/leader_lease.py](orchestrator/leader_lease.py)
8. [orchestrator/protocols/saga/saga.py](orchestrator/protocols/saga/saga.py)
9. [orchestrator/protocols/saga/saga_record.py](orchestrator/protocols/saga/saga_record.py)
10. [orchestrator/protocols/two_pc.py](orchestrator/protocols/two_pc.py)
11. [stock/ledger.py](stock/ledger.py)
12. [payment/ledger.py](payment/ledger.py)

## Deployment

The repository includes three Docker Compose deployments:

- small
- medium
- large

Files:

- [docker/compose/docker-compose.small.yml](docker/compose/docker-compose.small.yml)
- [docker/compose/docker-compose.medium.yml](docker/compose/docker-compose.medium.yml)
- [docker/compose/docker-compose.large.yml](docker/compose/docker-compose.large.yml)

Current high-level replica layout:

- `small`: 1 gateway, 1 orchestrator, 3 order-service replicas, 1 stock-service, 1 payment-service, 4 Redis primary/replica pairs, 3 Sentinels
- `medium`: 1 gateway, 2 orchestrators, 6 order-service replicas, 7 stock-service replicas, 7 payment-service replicas, 4 Redis primary/replica pairs, 3 Sentinels
- `large`: 1 gateway, 3 orchestrators, 12 order-service replicas, 14 stock-service replicas, 14 payment-service replicas, 4 Redis primary/replica pairs, 3 Sentinels

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

Each Compose profile now includes:

- one Redis primary and one replica for each bounded context
- three shared Redis Sentinel processes monitoring all four primaries
- application services configured to resolve primaries through Sentinel instead of a fixed Redis host
- a longer-lived `/orders/checkout/` gateway path so checkout waits can survive normal protocol latency
- in the `small` profile, a three-replica `order-service` pool to avoid forcing all stress traffic through one backend container

### Non-interactive kill from host (all possible service/db targets)

Small profile:

```bash
docker compose -p dds-small -f docker/compose/docker-compose.small.yml exec orchestrator-service sh -lc 'kill -TERM 1'
docker compose -p dds-small -f docker/compose/docker-compose.small.yml exec order-service sh -lc 'kill -TERM 1'
docker compose -p dds-small -f docker/compose/docker-compose.small.yml exec order-service-2 sh -lc 'kill -TERM 1'
docker compose -p dds-small -f docker/compose/docker-compose.small.yml exec order-service-3 sh -lc 'kill -TERM 1'
docker compose -p dds-small -f docker/compose/docker-compose.small.yml exec payment-service sh -lc 'kill -TERM 1'
docker compose -p dds-small -f docker/compose/docker-compose.small.yml exec stock-service sh -lc 'kill -TERM 1'
docker compose -p dds-small -f docker/compose/docker-compose.small.yml exec order-db sh -lc 'kill -TERM 1'
docker compose -p dds-small -f docker/compose/docker-compose.small.yml exec payment-db sh -lc 'kill -TERM 1'
docker compose -p dds-small -f docker/compose/docker-compose.small.yml exec stock-db sh -lc 'kill -TERM 1'
docker compose -p dds-small -f docker/compose/docker-compose.small.yml exec orchestrator-db sh -lc 'kill -TERM 1'
```

Replica databases can be targeted the same way, for example `order-db-replica`, `stock-db-replica`, `payment-db-replica`, and `orchestrator-db-replica`.
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
