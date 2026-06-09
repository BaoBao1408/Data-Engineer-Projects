.PHONY: help up down logs test lint format run-pipeline db-shell api-test

help:  ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

# ── Docker ────────────────────────────────────────────────────────────────────
up:  ## Start all services (postgres, airflow, api)
	docker compose up -d postgres
	@echo "Waiting for PostgreSQL to be ready..."
	@sleep 5
	docker compose up -d airflow-init
	@echo "Waiting for Airflow init..."
	@sleep 10
	docker compose up -d airflow-webserver airflow-scheduler api
	@echo ""
	@echo "  Airflow UI   → http://localhost:8080  (admin/admin)"
	@echo "  API docs     → http://localhost:8000/docs"
	@echo "  DB           → localhost:5432"

down:  ## Stop all services
	docker compose down

down-v:  ## Stop all services and remove volumes
	docker compose down -v

logs:  ## Follow all service logs
	docker compose logs -f

logs-api:  ## Follow API logs
	docker compose logs -f api

logs-scheduler:  ## Follow Airflow scheduler logs
	docker compose logs -f airflow-scheduler

# ── Pipeline ──────────────────────────────────────────────────────────────────
run-pipeline:  ## Run full pipeline manually (one-shot)
	docker compose --profile manual run --rm pipeline

run-pipeline-local:  ## Run pipeline locally (requires local Python env)
	PYTHONPATH=. python -m src.main

# ── Database ──────────────────────────────────────────────────────────────────
db-shell:  ## Open psql shell in geospatial DB
	docker compose exec postgres psql -U geouser -d geospatial

db-stats:  ## Show POI count stats
	docker compose exec postgres psql -U geouser -d geospatial \
		-c "SELECT category, COUNT(*) FROM poi GROUP BY category ORDER BY count DESC;"

db-spatial-check:  ## Verify PostGIS is working
	docker compose exec postgres psql -U geouser -d geospatial \
		-c "SELECT PostGIS_Version(); SELECT COUNT(*) FROM poi;"

# ── API ───────────────────────────────────────────────────────────────────────
api-test:  ## Test API health and nearby endpoint
	@echo "Health check:"
	curl -s http://localhost:8000/health | python3 -m json.tool
	@echo "\nNearby POIs (500m from HCMC center):"
	curl -s "http://localhost:8000/api/v1/poi/nearby?lon=106.6853&lat=10.7769&radius_m=500&limit=5" | python3 -m json.tool

# ── Testing ───────────────────────────────────────────────────────────────────
test:  ## Run unit tests
	pytest tests/unit/ -v --cov=src --cov-report=term-missing

test-integration:  ## Run integration tests (requires running DB)
	pytest tests/integration/ -v

lint:  ## Run linters
	ruff check src/ dags/ tests/
	black --check src/ dags/ tests/

format:  ## Auto-format code
	black src/ dags/ tests/
	isort src/ dags/ tests/

# ── Setup ─────────────────────────────────────────────────────────────────────
install:  ## Install Python dependencies locally
	pip install -r requirements.txt

setup-env:  ## Copy .env.example to .env
	cp .env.example .env
	@echo "Edit .env with your settings"
