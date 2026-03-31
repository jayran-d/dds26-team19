# Testing Guide

This project supports three runtime modes through `env/transaction.env`:

- `simple`
- `saga`
- `2pc`

The transport is Redis Streams even though some test filenames still say
`kafka`.

## Daily Commands

### Clean reset

```bash
docker compose down -v
```

Use this when you want to remove Redis data volumes and start from a clean
state.

### Build and start

```bash
docker compose up --build -d
```

### Build and start with verbose logs

```bash
VERBOSE_LOGS=true docker compose up --build -d
```

### Build and start with logs minimized

```bash
VERBOSE_LOGS=false docker compose up --build -d
```

### Stop without deleting volumes

```bash
docker compose down
```

### Follow logs

```bash
docker compose logs -f gateway orchestrator-service order-service stock-service payment-service
```

### Follow only the last part of the logs

```bash
docker compose logs --tail=200 -f gateway orchestrator-service order-service stock-service payment-service
```

### Check service status

```bash
docker compose ps
```

## Switching Modes

1. Edit `env/transaction.env`.
2. Set `TRANSACTION_MODE` to the mode you want.
3. Rebuild the stack.

```bash
docker compose up --build -d
```

Do not skip the rebuild. The containers read the mode from environment on
startup.

## Test Files

| File | What it covers | Notes |
| --- | --- | --- |
| `test/test_microservices.py` | Basic API correctness | Good smoke test in any mode |
| `test/test_kafka_simple.py` | `simple` mode correctness | Historical name; now Redis Streams based |
| `test/test_kafka_saga.py` | Saga integration and recovery | Historical name; now Redis Streams based |
| `test/test_kafka_saga_databases.py` | Saga database stop/kill recovery | Good chaos suite |
| `test/test_2pc.py` | 2PC integration and recovery | Includes duplicate-checkout protection test |

## Recommended Test Matrix

### `simple` mode

Set:

```bash
TRANSACTION_MODE=simple
```

Run:

```bash
docker compose up --build -d
python3 -m unittest test.test_microservices
python3 -m unittest test.test_kafka_simple
```

### `saga` mode

Set:

```bash
TRANSACTION_MODE=saga
```

Run:

```bash
docker compose up --build -d
python3 -m unittest test.test_microservices
python3 -m unittest test.test_kafka_simple
python3 -m unittest test.test_kafka_saga
python3 -m unittest test.test_kafka_saga_databases
```

`test_kafka_simple.py` still matters in saga mode because it checks the basic
user-visible checkout behavior.

### `2pc` mode

Set:

```bash
TRANSACTION_MODE=2pc
```

Run:

```bash
docker compose up --build -d
python3 -m unittest test.test_microservices
python3 -m unittest test.test_2pc
```

## Current Naming Reality

The `kafka` test filenames are historical. They are testing Redis Streams now.
If you want to rename them later, a cleaner naming scheme would be:

- `test_streams_simple.py`
- `test_streams_saga.py`
- `test_streams_saga_databases.py`

I did not rename them in this documentation pass to avoid unnecessary churn in
imports and CI commands.

## Missing Test Worth Adding Next

The main missing suite is:

- `2pc` database stop/kill recovery

There is already a Saga database recovery suite. Adding the equivalent for 2PC
would strengthen the fault-tolerance story for the interview.

## Stress Testing

The external stress and consistency tests live under `tests_external/`.

### Install test dependencies

```bash
cd tests_external
pip install -r requirements.txt
```

### URLs

Check `tests_external/urls.json` first. It should point to your gateway.

### Stress test

```bash
cd tests_external/stress-test
python3 init_orders.py
locust -f locustfile.py --host="localhost"
```

Then open `http://localhost:8089/`.

### Consistency test

```bash
cd tests_external/consistency-test
python3 run_consistency_test.py
```

## Logging and Performance

For correctness debugging:

```bash
VERBOSE_LOGS=true docker compose up --build -d
```

For throughput and latency measurements:

```bash
VERBOSE_LOGS=false docker compose up --build -d
```

Logging materially affects results. In this project that is expected, not
surprising. The observed numbers you reported are consistent with the code:

- 2PC with logs: around 300 RPS
- 2PC without logs: around 480 RPS
- Saga with logs: around 470 RPS
- Saga without logs: around 600-700 RPS

That happens because access logs and worker logs add extra formatting, locking,
stdout/stderr I/O, and Docker log-driver overhead on the hot path.

For benchmark-style runs:

- keep logs off
- avoid restarting containers mid-run
- use the same mode and worker settings between runs
- note whether Redis volumes were warm or freshly recreated
