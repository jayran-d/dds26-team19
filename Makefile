COMPOSE ?= docker compose

SMALL_FILE := docker-compose.small.yml
MEDIUM_FILE := docker-compose.medium.yml
LARGE_FILE := docker-compose.large.yml

SMALL_PROJECT := dds-small
MEDIUM_PROJECT := dds-medium
LARGE_PROJECT := dds-large

.PHONY: small-build small-up small-down small-logs small-ps \
        medium-build medium-up medium-down medium-logs medium-ps \
        large-build large-up large-down large-logs large-ps

small-build:
	$(COMPOSE) -p $(SMALL_PROJECT) -f $(SMALL_FILE) build

small-up:
	$(COMPOSE) -p $(SMALL_PROJECT) -f $(SMALL_FILE) up -d --build

small-down:
	$(COMPOSE) -p $(SMALL_PROJECT) -f $(SMALL_FILE) down

small-logs:
	$(COMPOSE) -p $(SMALL_PROJECT) -f $(SMALL_FILE) logs -f

small-ps:
	$(COMPOSE) -p $(SMALL_PROJECT) -f $(SMALL_FILE) ps

medium-build:
	$(COMPOSE) -p $(MEDIUM_PROJECT) -f $(MEDIUM_FILE) build

medium-up:
	$(COMPOSE) -p $(MEDIUM_PROJECT) -f $(MEDIUM_FILE) up -d --build

medium-down:
	$(COMPOSE) -p $(MEDIUM_PROJECT) -f $(MEDIUM_FILE) down

medium-logs:
	$(COMPOSE) -p $(MEDIUM_PROJECT) -f $(MEDIUM_FILE) logs -f

medium-ps:
	$(COMPOSE) -p $(MEDIUM_PROJECT) -f $(MEDIUM_FILE) ps

large-build:
	$(COMPOSE) -p $(LARGE_PROJECT) -f $(LARGE_FILE) build

large-up:
	$(COMPOSE) -p $(LARGE_PROJECT) -f $(LARGE_FILE) up -d --build

large-down:
	$(COMPOSE) -p $(LARGE_PROJECT) -f $(LARGE_FILE) down

large-logs:
	$(COMPOSE) -p $(LARGE_PROJECT) -f $(LARGE_FILE) logs -f

large-ps:
	$(COMPOSE) -p $(LARGE_PROJECT) -f $(LARGE_FILE) ps
