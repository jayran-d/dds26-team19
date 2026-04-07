# Distributed Data Systems Checkout System

This repository contains the "normal" three-service version of the project:

- `order` owns orders and coordinates checkout
- `stock` owns inventory
- `payment` owns user balances

The client-facing API is synchronous, but the transaction processing path is event-driven. `order` publishes commands over Redis Streams, `stock` and `payment` reply with events, and the selected transaction coordinator inside `order` drives the workflow to a terminal state.

## Project docs

- Codebase walkthrough: [docs/codebase_walkthrough.md](docs/codebase_walkthrough.md)
- Architecture explanation: [docs/explanation_architecture.md](docs/explanation_architecture.md)
- Compose scaling guide: [docs/compose_scaling.md](docs/compose_scaling.md)

## What changed in this branch

The branch now supports two Redis deployment modes:

- the `small` profile keeps one Redis primary per bounded context
- the default, `medium`, and `large` layouts use Redis primaries plus replicas and a shared Sentinel quorum
- service code connects through `common/redis_client.py`, which supports both direct-host and Sentinel-aware modes

That means the low-scale profile stays compliant with the single-instance requirement, while the larger profiles can still demonstrate Redis failover behavior.

## Runtime architecture

### Business services

- `order/app.py` exposes the order API and synchronous checkout endpoint
- `order/streams_worker.py` runs the Saga or 2PC coordinator over Redis Streams
- `stock/app.py` and `payment/app.py` expose their local business APIs
- `stock/streams_worker.py` and `payment/streams_worker.py` act as transaction participants

### Redis topology

Each bounded context still has its own Redis store:

- `small`: `order-db`, `stock-db`, `payment-db`
- default / `medium` / `large`: `order-db` + `order-db-replica`, `stock-db` + `stock-db-replica`, `payment-db` + `payment-db-replica`

The HA layouts share:

- `redis-sentinel-1`
- `redis-sentinel-2`
- `redis-sentinel-3`

Application containers use environment variables such as `REDIS_HOST`, `REDIS_SENTINELS`, and `REDIS_MASTER_NAME` so the same code can either connect directly in `small` or resolve the current primary in the HA layouts.

### Transaction modes

The repository supports two transaction modes:

- `saga`
- `2pc`

The active mode is controlled by `TRANSACTION_MODE`, and the same deployment topology supports both modes.

## Local development

### Default stack

The baseline stack is defined in `docker/compose/docker-compose.yml`.

Start it with:

```bash
TRANSACTION_MODE=saga docker compose -f docker/compose/docker-compose.yml up -d --build --force-recreate
```

Stop it with:

```bash
docker compose -f docker/compose/docker-compose.yml down -v --remove-orphans
```

### Sized profiles

Use the provided make targets for the small, medium, and large profiles:

```bash
make small-up-saga
make small-up-2pc

make medium-up-saga
make medium-up-2pc

make large-up-saga
make large-up-2pc
```

The small profile now keeps one `order-service`, one `stock-service`, one `payment-service`, and one Redis container per bounded context.

Inspect them with:

```bash
make small-ps
make medium-logs
make large-down
```

## Testing

The main local verification targets are:

```bash
make unit-saga
make unit-2pc
```

Those commands rebuild the small profile, start the single-instance Redis topology, and run the integration suites against it.

Useful direct smoke tests:

```bash
python3 -m unittest test.test_microservices -v
python3 -m unittest test.test_kafka_saga -v
python3 -m unittest test.test_kafka_saga_databases -v
python3 -m unittest test.test_2pc -v
```

## Operational notes

### Gateway

Nginx is the public entry point and listens on `localhost:8000`. Profile-specific gateway configs live in:

- `nginx/gateway_nginx.conf`
- `nginx/gateway_nginx.small.conf`
- `nginx/gateway_nginx.medium.conf`
- `nginx/gateway_nginx.large.conf`

### Shell access

Examples for the small profile:

```bash
docker compose -p dds-small -f docker/compose/docker-compose.small.yml exec order-service sh
docker compose -p dds-small -f docker/compose/docker-compose.small.yml exec order-db sh
```

### Graceful stop from inside a container

```bash
kill -TERM 1
```

### Host-side restart examples

```bash
docker compose -p dds-small -f docker/compose/docker-compose.small.yml restart order-service
docker compose -p dds-small -f docker/compose/docker-compose.small.yml restart order-db
```

## Repository layout

- `common/`: shared stream, message, worker-logging, and Redis client code for both direct and Sentinel-aware modes
- `docker/compose/`: baseline, small, medium, and large Compose stacks
- `env/`: Redis and transaction-mode environment settings
- `nginx/`: gateway configs per deployment profile
- `order/`: API, coordinator, and transaction implementations
- `payment/`: payment API and participant logic
- `stock/`: inventory API and participant logic
- `test/`: local integration and recovery test suites

## Kubernetes

The repository still contains `helm-config/` and `k8s/` assets for cluster deployment experiments, but the actively maintained local development and testing path in this branch is Docker Compose.
