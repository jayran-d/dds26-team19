# Docker Compose Scaling Profiles

This repository now includes three dedicated Docker Compose layouts for the 96-core environment:

- `docker/compose/docker-compose.small.yml`: 1 instance of each application service and 1 Redis per bounded context.
- `docker/compose/docker-compose.medium.yml`: fixed 50 CPU allocation.
- `docker/compose/docker-compose.large.yml`: fixed 90 CPU allocation.

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

- `nginx/gateway_nginx.small.conf`
- `nginx/gateway_nginx.medium.conf`
- `nginx/gateway_nginx.large.conf`

The medium and large configs declare every backend replica explicitly and use Docker DNS re-resolution (`resolve` with `127.0.0.11`) so nginx does not stay pinned to a stale container IP.

## Usage

Use the profile name as the prefix: `small`, `medium`, or `large`.

### Start a profile

Build and start the selected profile with the default transaction mode (`saga`):

```bash
make small-up
make medium-up
make large-up
```

If you want to force a specific transaction mode, use the explicit targets:

```bash
make small-up-saga
make small-up-2pc

make medium-up-saga
make medium-up-2pc

make large-up-saga
make large-up-2pc
```

### Build images without starting containers

```bash
make small-build
make medium-build
make large-build
```

Or build for a specific transaction mode:

```bash
make small-build-saga
make small-build-2pc
```

### Inspect a running profile

Show running containers:

```bash
make medium-ps
```

Follow logs:

```bash
make medium-logs
```

### Stop and remove a profile

Stop a cluster and remove its containers and volumes with the matching `*-down` target:

```bash
make medium-down
```

### Rule of thumb

- Use `make <profile>-up` when you want to start working quickly with the default `saga` mode.
- Use `make <profile>-up-2pc` when you specifically need the 2PC flow.
- Use `make <profile>-build` if you only want fresh images without launching the stack.
- Use `make <profile>-ps` or `make <profile>-logs` to inspect an already running stack.
- Use `make <profile>-down` when you want to fully tear that stack down.

## Notes

- Run only one profile at a time unless you also change host port mappings.
- The first replica of each application service performs the image build; the other replicas reuse the same image tag.
- The baseline compose file is now located at `docker/compose/docker-compose.yml`.
