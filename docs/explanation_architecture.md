# Architecture Explanation

This branch keeps the original three-service business split. The larger deployments add Redis primaries, replicas, and Sentinel-based primary discovery, while the `small` profile stays single-instance.

## 1. Bounded contexts

The architecture is intentionally split by ownership:

- `order` owns orders and checkout coordination
- `stock` owns inventory
- `payment` owns account balances

No service directly mutates another service's local state by sharing a database. Cross-service consistency only happens through explicit transaction messages.

## 2. Why checkout is synchronous outside but asynchronous inside

Clients want one request and one final answer. Internally, a distributed checkout cannot safely be a single local database transaction, because the stock and payment services each own their own state.

The architecture resolves that tension like this:

- the HTTP client sends one blocking checkout request
- `order` publishes commands over Redis Streams
- `stock` and `payment` execute their local steps independently
- both participants publish reply events
- `order` finishes the workflow and wakes the waiting request thread

So the external contract is synchronous, but the implementation stays event-driven and recoverable.

## 3. Transaction modes

### Saga

Saga is used when the system should proceed step by step and compensate if a later step fails.

Typical flow:

1. reserve stock
2. process payment
3. mark order complete

If payment fails after stock has already been reserved, the coordinator publishes a compensating stock-release command.

### 2PC

2PC is used when the coordinator wants a more explicit prepare/commit/abort sequence.

Typical flow:

1. prepare stock
2. prepare payment
3. commit both if all participants are ready
4. abort if any participant rejects or disappears long enough

Both modes are implemented in the same branch and selected by `TRANSACTION_MODE`.

## 4. Redis as both storage and transport

Redis is doing two jobs in this architecture:

- local state storage for each bounded context
- asynchronous transport through Redis Streams

This is why each business service has its own Redis deployment instead of one shared global database.

### Local state

- `order-db` stores orders, status keys, Saga records, and 2PC coordinator state
- `stock-db` stores items and the stock participant ledger
- `payment-db` stores user balances and the payment participant ledger

### Transport

Redis Streams carry:

- commands from `order` to `stock` and `payment`
- reply events from `stock` and `payment` back to `order`

Consumer groups, pending-entry reclaiming, and participant ledgers are what make the transport restart-safe.

## 5. Redis deployment layer

The important infrastructure change in this branch is that the scaled layouts use a replicated Redis topology, while `small` remains single-instance:

- `small`: `order-db`, `stock-db`, `payment-db`
- default / `medium` / `large`: `order-db` + `order-db-replica`, `stock-db` + `stock-db-replica`, `payment-db` + `payment-db-replica`

Three Sentinel nodes observe the HA primaries:

- `redis-sentinel-1`
- `redis-sentinel-2`
- `redis-sentinel-3`

### Why Sentinel is needed

Replication alone is not enough. Clients still need to know which node is currently writable. Sentinel provides:

- primary health monitoring
- quorum-based failover
- primary discovery for clients

Application code uses `common/redis_client.py` for both modes. In `small`, it connects directly through `*_REDIS_HOST`. In the HA layouts, it asks Sentinel for the current primary by logical name instead of hardcoding one fixed container hostname.

## 6. What failover changes in practice

Before this change, a service that connected to `order-db` assumed that container was the only writable node. If that process died, the client stayed pinned to a dead endpoint until the same container came back.

Now the contract depends on the profile:

- `small`: clients talk directly to the single configured Redis host
- HA layouts: clients talk to Sentinel, Sentinel identifies the current primary, and clients reconnect to the promoted primary after failover

That is the reason the branch now carries:

- `REDIS_SENTINELS`
- `REDIS_MASTER_NAME`
- the shared `common/redis_client.py`

## 7. Where reliability comes from

The system's reliability story comes from several layers working together.

### Layer 1: durable command/event transport

Redis Streams consumer groups keep messages durable until they are acknowledged.

### Layer 2: idempotent participant ledgers

`stock` and `payment` record whether a command was:

- seen
- applied
- replied to

That prevents duplicate delivery from reapplying business effects.

### Layer 3: durable coordinator state

`order` persists:

- Saga records
- active transaction pointers
- 2PC coordinator state

That allows the coordinator to recover in-flight work after restart.

### Layer 4: orphan reclaim

Worker code reclaims pending messages that were left behind by dead consumers.

### Layer 5: Redis failover

Replication plus Sentinel reduces the chance that one Redis crash leaves the application stuck on a dead database host.

## 8. Why the order service still matters the most

`order` is the coordinator. Even with replicated Redis, correctness still depends on `order` being the only component that decides how a checkout advances.

It is responsible for:

- starting the transaction
- publishing the next command
- interpreting participant replies
- deciding compensation, commit, or abort
- exposing terminal status to the client

The database HA layer protects the coordinator's state store, but it does not replace the coordination logic.

## 9. Gateway role

Nginx is deliberately thin:

- routes `/orders/...` to the order service
- routes `/stock/...` to the stock service
- routes `/payment/...` to the payment service

It does not coordinate transactions. Its job is to expose a single entry point and spread traffic across replicas in the sized profiles.

## 10. Scaling story

The small profile is aimed at correctness and local validation:

- one `order-service`
- one `stock-service`
- one `payment-service`
- three Redis primaries

It is the literal single-instance topology used by the local test targets.

The medium and large profiles keep the same Redis HA pattern and scale the app-service layer horizontally behind Nginx.

That means:

- business logic scales out through service replicas
- service-local state stays isolated per bounded context
- each bounded context still has a failover-capable Redis primary

## 11. Testing strategy

The branch is validated from three directions:

- API smoke tests in `test/test_microservices.py`
- transaction-mode integration tests in `test/test_kafka_saga.py` and `test/test_2pc.py`
- database stop/kill recovery tests in `test/test_kafka_saga_databases.py`

Those tests matter more now because the HA layer changes not just startup topology, but reconnection and recovery behavior under database interruption.

## 12. Design tradeoff

This architecture chooses operational simplicity over a separate message broker and separate SQL databases, but it still enforces service boundaries correctly:

- ownership stays explicit
- the workflow is auditable
- retries are handled intentionally
- failover is better than the old fixed-host Redis setup

That makes it a reasonable distributed-systems design for the assignment: simple enough to run locally, but explicit enough to demonstrate coordination, recovery, and fault tolerance.
