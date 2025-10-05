# Variables
BACKEND_IMAGE=erp-backend:latest
DEV_COMPOSE=docker-compose.dev.yml
PROD_COMPOSE=docker-compose.prod.yml

# =========================
# Development workflow
# =========================

.PHONY: build-backend-dev
build-backend-dev:
	docker build -t $(BACKEND_IMAGE) ./billingv3

.PHONY: compose-build-dev
compose-build-dev:
	docker compose -f $(DEV_COMPOSE) build

.PHONY: compose-up-dev
compose-up-dev:
	docker compose -f $(DEV_COMPOSE) up -d

.PHONY: dev
dev: build-backend-dev compose-build-dev compose-up-dev
	@echo "✅ Dev environment is up!"

.PHONY: compose-down-dev
compose-down-dev:
	docker compose -f $(DEV_COMPOSE) down

# =========================
# Production workflow
# =========================

.PHONY: build-backend-prod
build-backend-prod:
	docker build -t $(BACKEND_IMAGE) ./billingv3

.PHONY: compose-build-prod
compose-build-prod:
	docker compose -f $(PROD_COMPOSE) build

.PHONY: compose-up-prod
compose-up-prod:
	docker compose -f $(PROD_COMPOSE) up -d

.PHONY: prod
prod: build-backend-prod compose-build-prod compose-up-prod
	@echo "✅ Prod environment is up!"

.PHONY: compose-down-prod
compose-down-prod:
	docker compose -f $(PROD_COMPOSE) down
