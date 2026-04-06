# Docker Compose Scaling Profiles

This repository ships four Compose layouts:

- `docker/compose/docker-compose.yml`
- `docker/compose/docker-compose.small.yml`
- `docker/compose/docker-compose.medium.yml`
- `docker/compose/docker-compose.large.yml`

All four now use the same Redis high-availability pattern:

- one primary per bounded context
- one replica per bounded context
- one shared three-node Sentinel quorum

That means every profile includes:

- `order-db` and `order-db-replica`
- `stock-db` and `stock-db-replica`
- `payment-db` and `payment-db-replica`
- `redis-sentinel-1`, `redis-sentinel-2`, `redis-sentinel-3`

## 1. Baseline compose file

`docker/compose/docker-compose.yml` is the default non-profile stack.

Topology:

- 1 `gateway`
- 1 `order-service`
- 1 `stock-service`
- 1 `payment-service`
- 3 Redis primaries
- 3 Redis replicas
- 3 Sentinels

Typical startup:

```bash
TRANSACTION_MODE=saga docker compose -f docker/compose/docker-compose.yml up -d --build --force-recreate
```

Typical teardown:

```bash
docker compose -f docker/compose/docker-compose.yml down -v --remove-orphans
```

## 2. Small profile

File:

- `docker/compose/docker-compose.small.yml`

Project name:

- `dds-small`

Topology:

- 1 `gateway`
- 1 `order-service`
- 1 `stock-service`
- 1 `payment-service`
- 3 Redis primaries
- 3 Redis replicas
- 3 Sentinels

CPU limits:

- no explicit CPU limits are set in this profile

Use this profile for:

- correctness validation
- local debugging
- `make unit-saga`
- `make unit-2pc`

## 3. Medium profile

File:

- `docker/compose/docker-compose.medium.yml`

Project name:

- `dds-medium`

Topology:

- 1 `gateway`
- 7 `order-service` replicas total
- 7 `stock-service` replicas total
- 7 `payment-service` replicas total
- 3 Redis primaries
- 3 Redis replicas
- 3 Sentinels

CPU allocation in the current file:

- `gateway`: `2.0`
- `order-service` total: `7 x 2.0 = 14.0`
- `stock-service` total: `7 x 2.0 = 14.0`
- `payment-service` total: `7 x 2.0 = 14.0`
- Redis primaries total: `3 x 1.25 = 3.75`
- Redis replicas total: `3 x 0.5 = 1.5`
- Sentinels total: `3 x 0.25 = 0.75`

Total:

- `50.0 CPUs`

Use this profile for:

- throughput experiments that still fit into a fixed 50-CPU envelope

## 4. Large profile

File:

- `docker/compose/docker-compose.large.yml`

Project name:

- `dds-large`

Topology:

- 1 `gateway`
- 14 `order-service` replicas total
- 14 `stock-service` replicas total
- 14 `payment-service` replicas total
- 3 Redis primaries
- 3 Redis replicas
- 3 Sentinels

CPU allocation in the current file:

- `gateway`: `3.0`
- `order-service` total: `14 x 2.0 = 28.0`
- `stock-service` total: `14 x 2.0 = 28.0`
- `payment-service` total: `14 x 2.0 = 28.0`
- Redis primaries total: `3 x 0.75 = 2.25`
- Redis replicas total: `3 x 0.25 = 0.75`
- Sentinels total: `3 x 0.1 = 0.3`

Total:

- `90.3 CPUs`

Use this profile for:

- high-load runs on the larger local machine budget

## 5. Gateway routing

Each sized profile has its own Nginx config:

- `nginx/gateway_nginx.small.conf`
- `nginx/gateway_nginx.medium.conf`
- `nginx/gateway_nginx.large.conf`

The medium and large gateway configs explicitly list the backend replicas so traffic is spread across all service instances.

## 6. Profile commands

### Start

```bash
make small-up-saga
make small-up-2pc

make medium-up-saga
make medium-up-2pc

make large-up-saga
make large-up-2pc
```

### Build without starting

```bash
make small-build
make medium-build
make large-build
```

### Inspect

```bash
make small-ps
make medium-logs
make large-ps
```

### Tear down

```bash
make small-down
make medium-down
make large-down
```

## 7. Operational implications of the HA Redis layer

The Compose files now do more than just launch extra containers. They also change how the application connects:

- services wait for primaries, replicas, and Sentinels to become healthy
- environment files advertise Sentinel nodes and logical master names
- application clients connect through Sentinel instead of one fixed Redis host

That is why recovery testing should be done against these current Compose files, not against older single-Redis assumptions.
