# Docker Compose Scaling Profiles

This repository ships three Compose profiles:

- `docker/compose/docker-compose.small.yml`
- `docker/compose/docker-compose.medium.yml`
- `docker/compose/docker-compose.large.yml`

All three profiles expose the gateway on `http://localhost:8000`, keep the same bounded contexts (`order`, `orchestrator`, `stock`, `payment`), and use a dedicated Redis primary/replica pair for each context plus a shared three-node Sentinel quorum.

## Profile Summary

### Small

Local correctness and stress topology. The orchestrator, stock, and payment services stay singleton, but the gateway now fronts a small `order-service` pool so `/orders/checkout` load is not pinned to one backend container.

- `gateway`: 1 replica
- `orchestrator-service`: 1 replica
- `order-service`: 3 replicas
- `stock-service`: 1 replica
- `payment-service`: 1 replica
- Redis: 8 data nodes + 3 Sentinel nodes
  - `order-db` / `order-db-replica`
  - `orchestrator-db` / `orchestrator-db-replica`
  - `stock-db` / `stock-db-replica`
  - `payment-db` / `payment-db-replica`
  - `redis-sentinel-1/2/3`

There are no explicit `cpus:` limits in this profile.

Runtime settings:

- `orchestrator-service`: `CONSUMER_WORKERS=8`
- `stock-service`: `CONSUMER_WORKERS=8`
- `payment-service`: `CONSUMER_WORKERS=8`
- `order-service` calls the orchestrator directly at `http://orchestrator-service:5000`
- nginx load-balances `/orders/checkout` across `order-service`, `order-service-2`, and `order-service-3`

### Medium

Nominal **52.75 CPU** profile with multiple service replicas.

Replica layout:

- `gateway`: 1 replica
- `orchestrator-service`: 2 replicas
- `order-service`: 6 replicas
- `stock-service`: 7 replicas
- `payment-service`: 7 replicas
- Redis: 8 data nodes + 3 Sentinel nodes

CPU allocation:

- `gateway`: `1 x 2.0` = **2 CPUs**
- `orchestrator-service`: `2 x 2.0` = **4 CPUs**
- `order-service`: `6 x 2.0` = **12 CPUs**
- `stock-service`: `7 x 2.0` = **14 CPUs**
- `payment-service`: `7 x 2.0` = **14 CPUs**
- Redis primaries: `4 x 1.0` = **4 CPUs**
- Redis replicas: `4 x 0.5` = **2 CPUs**
- Redis Sentinels: `3 x 0.25` = **0.75 CPUs**

Total: **52.75 CPUs**

Runtime settings:

- `orchestrator-service`: `CONSUMER_WORKERS=8`
- `stock-service`: `CONSUMER_WORKERS=8`
- `payment-service`: `CONSUMER_WORKERS=8`
- `order-service` calls the orchestrator via the gateway at `http://gateway:80/orchestrator`

### Large

Nominal **91.3 CPU** profile for the highest replica count in this repo.

Replica layout:

- `gateway`: 1 replica
- `orchestrator-service`: 3 replicas
- `order-service`: 12 replicas
- `stock-service`: 14 replicas
- `payment-service`: 14 replicas
- Redis: 8 data nodes + 3 Sentinel nodes

CPU allocation:

- `gateway`: `1 x 2.0` = **2 CPUs**
- `orchestrator-service`: `3 x 2.0` = **6 CPUs**
- `order-service`: `12 x 2.0` = **24 CPUs**
- `stock-service`: `14 x 2.0` = **28 CPUs**
- `payment-service`: `14 x 2.0` = **28 CPUs**
- Redis primaries: `4 x 0.5` = **2 CPUs**
- Redis replicas: `4 x 0.25` = **1 CPU**
- Redis Sentinels: `3 x 0.1` = **0.3 CPUs**

Total: **91.3 CPUs**

Runtime settings:

- `orchestrator-service`: `CONSUMER_WORKERS=8`
- `stock-service`: `CONSUMER_WORKERS=8`
- `payment-service`: `CONSUMER_WORKERS=8`
- `order-service` calls the orchestrator via the gateway at `http://gateway:80/orchestrator`

## Gateway routing

Each profile uses its own nginx configuration:

- `nginx/gateway_nginx.small.conf`
- `nginx/gateway_nginx.medium.conf`
- `nginx/gateway_nginx.large.conf`

Routing behavior differs by size:

- `small`: the gateway fronts three `order-service` replicas plus single stock/payment/orchestrator instances, while `order-service` still calls the single orchestrator container directly
- `medium`: nginx defines explicit upstream pools for replicated services, including an internal `/orchestrator/` route
- `large`: same routing model as `medium`, with larger upstream pools

The `/orders/checkout` route has a dedicated longer read timeout in every profile because the order-service intentionally waits for durable terminal transaction state before replying.

Across all three profiles, Redis clients talk to Sentinel and resolve the
current primary for each bounded context dynamically. That means a database
container kill no longer requires clients to reconnect to a hardcoded Redis host.

The medium and large nginx configs use Docker DNS re-resolution (`resolve` with `127.0.0.11`) so upstream backends do not stay pinned to stale container IPs after restarts.

## Usage

Start a profile with the matching `Makefile` target:

```bash
make small-up
make medium-up
make large-up
```

Protocol-specific variants are also available:

```bash
make small-up-saga
make small-up-2pc
make medium-up-saga
make medium-up-2pc
make large-up-saga
make large-up-2pc
```

Stop and remove a profile with the matching `*-down` target:

```bash
make small-down
make medium-down
make large-down
```

These teardown commands run `docker compose down -v`, so they remove both containers and named volumes for that profile.
