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

The Redis layer is now highly available per bounded context:

- `order-db`, `stock-db`, and `payment-db` each run as a primary
- each primary has a dedicated replica container
- a shared three-node Redis Sentinel quorum tracks primaries and performs failover
- service code connects through Sentinel-aware clients in `common/redis_client.py`

That means application code no longer assumes one fixed Redis container per service. It asks Sentinel for the current primary and reconnects there after failover.

## Runtime architecture

### Business services

- `order/app.py` exposes the order API and synchronous checkout endpoint
- `order/streams_worker.py` runs the Saga or 2PC coordinator over Redis Streams
- `stock/app.py` and `payment/app.py` expose their local business APIs
- `stock/streams_worker.py` and `payment/streams_worker.py` act as transaction participants

### Redis topology

Each bounded context has its own Redis deployment:

- `order-db` + `order-db-replica`
- `stock-db` + `stock-db-replica`
- `payment-db` + `payment-db-replica`

The Sentinels are shared across all three databases:

- `redis-sentinel-1`
- `redis-sentinel-2`
- `redis-sentinel-3`

Application containers use environment variables such as `REDIS_SENTINELS` and `REDIS_MASTER_NAME` to discover the current primary instead of hardcoding a single host.

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

Those commands rebuild the small profile, start the HA Redis topology, and run the integration suites against it.

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
docker compose -p dds-small -f docker/compose/docker-compose.small.yml exec order-db-replica sh
docker compose -p dds-small -f docker/compose/docker-compose.small.yml exec redis-sentinel-1 sh
```

### Graceful stop from inside a container

```bash
kill -TERM 1
```

### Host-side restart examples

```bash
docker compose -p dds-small -f docker/compose/docker-compose.small.yml restart order-service
docker compose -p dds-small -f docker/compose/docker-compose.small.yml restart order-db
docker compose -p dds-small -f docker/compose/docker-compose.small.yml restart redis-sentinel-1
```

## Repository layout

- `common/`: shared stream, message, worker-logging, and Sentinel-aware Redis client code
- `docker/compose/`: baseline, small, medium, and large Compose stacks
- `env/`: Redis and transaction-mode environment settings
- `nginx/`: gateway configs per deployment profile
- `order/`: API, coordinator, and transaction implementations
- `payment/`: payment API and participant logic
- `stock/`: inventory API and participant logic
- `test/`: local integration and recovery test suites

## Kubernetes

The repository still contains `helm-config/` and `k8s/` assets for cluster deployment experiments, but the actively maintained local development and testing path in this branch is Docker Compose.
