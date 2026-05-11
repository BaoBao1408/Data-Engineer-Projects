# ──────────────────────────────────────────────────────────────────
# Makefile — AdStart Media Pipeline
# Usage: make <target>
# ──────────────────────────────────────────────────────────────────

.DEFAULT_GOAL := help
DOCKER_COMPOSE = docker compose

# ── Local (no Docker) ──────────────────────────────────────────
.PHONY: install
install:  ## Set up venv and install all dependencies (run once)
	python -m venv .venv
	.venv/bin/pip install --upgrade pip
	.venv/bin/pip install -e ".[dev]"
	@echo "\nDone. Activate with:  source .venv/bin/activate  (Linux/Mac)"
	@echo "                      .venv\\Scripts\\activate       (Windows)"

.PHONY: test
test:  ## Run pytest locally (venv must be active)
	PYTHONUTF8=1 pytest tests/ -v --tb=short

.PHONY: run
run:  ## Run pipeline locally for 2026-01-15 (test date with sample data)
	PYTHONUTF8=1 python src/orchestration/pipeline.py --date 2026-01-15

.PHONY: run-yesterday
run-yesterday:  ## Run pipeline for yesterday (production default)
	PYTHONUTF8=1 python src/orchestration/pipeline.py

# ── Docker ─────────────────────────────────────────────────────
.PHONY: build
build:  ## Build Docker image
	$(DOCKER_COMPOSE) build

.PHONY: docker-test
docker-test:  ## Run tests inside Docker
	$(DOCKER_COMPOSE) run --rm pipeline-test

.PHONY: docker-run
docker-run:  ## Run pipeline inside Docker (date=2026-01-15)
	$(DOCKER_COMPOSE) run --rm pipeline --date 2026-01-15

.PHONY: docker-run-date
docker-run-date:  ## Run pipeline for a specific date: make docker-run-date DATE=2026-01-20
	$(DOCKER_COMPOSE) run --rm pipeline --date $(DATE)

.PHONY: docker-shell
docker-shell:  ## Open interactive shell in the container
	$(DOCKER_COMPOSE) run --rm --entrypoint bash pipeline

.PHONY: logs
logs:  ## Show container logs
	docker logs adstart_pipeline

# ── Cleanup ────────────────────────────────────────────────────
.PHONY: clean
clean:  ## Remove warehouse DB and logs (keeps raw data)
	rm -f data/warehouse/warehouse.duckdb
	rm -f logs/pipeline_*.log
	@echo "Cleaned warehouse and logs."

.PHONY: clean-all
clean-all: clean  ## Also remove Docker image and venv
	docker rmi adstart-pipeline:latest 2>/dev/null || true
	rm -rf .venv
	@echo "Full cleanup done."

# ── Help ───────────────────────────────────────────────────────
.PHONY: help
help:  ## Show this help message
	@grep -E '^[a-zA-Z_-]+:.*##' $(MAKEFILE_LIST) | \
	  awk 'BEGIN {FS = ":.*##"}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'
