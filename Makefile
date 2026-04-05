COMPOSE ?= docker compose
PYTHON ?= python3
TRANSACTION_MODE ?= saga

SMALL_FILE := docker/compose/docker-compose.small.yml
MEDIUM_FILE := docker/compose/docker-compose.medium.yml
LARGE_FILE := docker/compose/docker-compose.large.yml

SMALL_PROJECT := dds-small
MEDIUM_PROJECT := dds-medium
LARGE_PROJECT := dds-large
CONSISTENCY_DIR := tests_external/consistency-test
STRESS_DIR := tests_external/stress-test
SMALL_TEST_ENV = COMPOSE_FILE=$(SMALL_FILE) COMPOSE_PROJECT_NAME=$(SMALL_PROJECT) TRANSACTION_MODE=$(TRANSACTION_MODE)
STRESS_HOST ?= http://localhost:8000
LOCUST_USERS ?= 1000
LOCUST_RATE ?= 100
LOCUST_RUNTIME ?= 30s

.PHONY: small-build small-up small-down small-logs small-ps \
        small-build-saga small-build-2pc small-up-saga small-up-2pc \
        medium-build medium-up medium-down medium-logs medium-ps \
        medium-build-saga medium-build-2pc medium-up-saga medium-up-2pc \
        large-build large-up large-down large-logs large-ps \
        large-build-saga large-build-2pc large-up-saga large-up-2pc \
        unit-saga unit-2pc consistency stress-init stress-locust stress-headless

small-build:
	TRANSACTION_MODE=$(TRANSACTION_MODE) $(COMPOSE) -p $(SMALL_PROJECT) -f $(SMALL_FILE) build

small-up:
	TRANSACTION_MODE=$(TRANSACTION_MODE) $(COMPOSE) -p $(SMALL_PROJECT) -f $(SMALL_FILE) up -d --build --force-recreate

small-down:
	$(COMPOSE) -p $(SMALL_PROJECT) -f $(SMALL_FILE) down -v

small-logs:
	$(COMPOSE) -p $(SMALL_PROJECT) -f $(SMALL_FILE) logs -f

small-ps:
	$(COMPOSE) -p $(SMALL_PROJECT) -f $(SMALL_FILE) ps

small-build-saga: TRANSACTION_MODE=saga
small-build-saga: small-build

small-build-2pc: TRANSACTION_MODE=2pc
small-build-2pc: small-build

small-up-saga: TRANSACTION_MODE=saga
small-up-saga: small-up

small-up-2pc: TRANSACTION_MODE=2pc
small-up-2pc: small-up

medium-build:
	TRANSACTION_MODE=$(TRANSACTION_MODE) $(COMPOSE) -p $(MEDIUM_PROJECT) -f $(MEDIUM_FILE) build

medium-up:
	TRANSACTION_MODE=$(TRANSACTION_MODE) $(COMPOSE) -p $(MEDIUM_PROJECT) -f $(MEDIUM_FILE) up -d --build --force-recreate

medium-down:
	$(COMPOSE) -p $(MEDIUM_PROJECT) -f $(MEDIUM_FILE) down -v

medium-logs:
	$(COMPOSE) -p $(MEDIUM_PROJECT) -f $(MEDIUM_FILE) logs -f

medium-ps:
	$(COMPOSE) -p $(MEDIUM_PROJECT) -f $(MEDIUM_FILE) ps

medium-build-saga: TRANSACTION_MODE=saga
medium-build-saga: medium-build

medium-build-2pc: TRANSACTION_MODE=2pc
medium-build-2pc: medium-build

medium-up-saga: TRANSACTION_MODE=saga
medium-up-saga: medium-up

medium-up-2pc: TRANSACTION_MODE=2pc
medium-up-2pc: medium-up

large-build:
	TRANSACTION_MODE=$(TRANSACTION_MODE) $(COMPOSE) -p $(LARGE_PROJECT) -f $(LARGE_FILE) build

large-up:
	TRANSACTION_MODE=$(TRANSACTION_MODE) $(COMPOSE) -p $(LARGE_PROJECT) -f $(LARGE_FILE) up -d --build --force-recreate

large-down:
	$(COMPOSE) -p $(LARGE_PROJECT) -f $(LARGE_FILE) down -v

large-logs:
	$(COMPOSE) -p $(LARGE_PROJECT) -f $(LARGE_FILE) logs -f

large-ps:
	$(COMPOSE) -p $(LARGE_PROJECT) -f $(LARGE_FILE) ps

large-build-saga: TRANSACTION_MODE=saga
large-build-saga: large-build

large-build-2pc: TRANSACTION_MODE=2pc
large-build-2pc: large-build

large-up-saga: TRANSACTION_MODE=saga
large-up-saga: large-up

large-up-2pc: TRANSACTION_MODE=2pc
large-up-2pc: large-up

unit-saga: TRANSACTION_MODE=saga
unit-saga:
	$(COMPOSE) -p $(SMALL_PROJECT) -f $(SMALL_FILE) down -v
	TRANSACTION_MODE=$(TRANSACTION_MODE) $(COMPOSE) -p $(SMALL_PROJECT) -f $(SMALL_FILE) up -d --build --force-recreate
	$(SMALL_TEST_ENV) $(PYTHON) -m unittest test.test_microservices -v
	$(SMALL_TEST_ENV) $(PYTHON) -m unittest test.test_streams_simple -v
	$(SMALL_TEST_ENV) $(PYTHON) -m unittest test.test_streams_saga -v
	$(SMALL_TEST_ENV) $(PYTHON) -m unittest test.test_streams_saga_databases -v

unit-2pc: TRANSACTION_MODE=2pc
unit-2pc:
	$(COMPOSE) -p $(SMALL_PROJECT) -f $(SMALL_FILE) down -v
	TRANSACTION_MODE=$(TRANSACTION_MODE) $(COMPOSE) -p $(SMALL_PROJECT) -f $(SMALL_FILE) up -d --build --force-recreate
	$(SMALL_TEST_ENV) $(PYTHON) -m unittest test.test_microservices -v
	$(SMALL_TEST_ENV) $(PYTHON) -m unittest test.test_2pc -v
	$(SMALL_TEST_ENV) $(PYTHON) -m unittest test.test_2pc_databases -v

consistency:
	cd $(CONSISTENCY_DIR) && $(PYTHON) run_consistency_test.py

stress-init:
	cd $(STRESS_DIR) && $(PYTHON) init_orders.py

stress-locust:
	cd $(STRESS_DIR) && locust -f locustfile.py --host="$(STRESS_HOST)"

stress-headless:
	cd $(STRESS_DIR) && locust -f locustfile.py --headless -u $(LOCUST_USERS) -r $(LOCUST_RATE) --run-time $(LOCUST_RUNTIME) --host="$(STRESS_HOST)" --only-summary
