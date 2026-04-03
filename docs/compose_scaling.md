# Docker Compose Scaling Profiles

This repository now includes three dedicated Docker Compose layouts for the 96-core environment:

- `docker-compose.small.yml`: 1 instance of each application service and 1 Redis per bounded context.
- `docker-compose.medium.yml`: fixed 50 CPU allocation.
- `docker-compose.large.yml`: fixed 90 CPU allocation.

## CPU budgets

### Small

Single-instance topology only. No explicit CPU limits are set in this profile.

### Medium

Total budget: **50 CPUs**

- `gateway`: 2 CPUs
- `order-service` replicas: 7 x 2 CPUs = 14 CPUs
- `stock-service` replicas: 7 x 2 CPUs = 14 CPUs
- `payment-service` replicas: 7 x 2 CPUs = 14 CPUs
- Redis databases: 3 x 2 CPUs = 6 CPUs

### Large

Total budget: **90 CPUs**

- `gateway`: 3 CPUs
- `order-service` replicas: 14 x 2 CPUs = 28 CPUs
- `stock-service` replicas: 14 x 2 CPUs = 28 CPUs
- `payment-service` replicas: 14 x 2 CPUs = 28 CPUs
- Redis databases: 3 x 1 CPU = 3 CPUs

## Gateway routing

Each scaling profile uses its own nginx config:

- `gateway_nginx.small.conf`
- `gateway_nginx.medium.conf`
- `gateway_nginx.large.conf`

The medium and large configs declare every backend replica explicitly and use Docker DNS re-resolution (`resolve` with `127.0.0.11`) so nginx does not stay pinned to a stale container IP.

## Usage

Use the `Makefile` targets:

```bash
make small-up
make medium-up
make large-up
```

Stop a cluster with the matching `*-down` target:

```bash
make medium-down
```

## Notes

- Run only one profile at a time unless you also change host port mappings.
- The first replica of each application service performs the image build; the other replicas reuse the same image tag.
- Existing `docker-compose.yml` is left unchanged for backwards compatibility.
