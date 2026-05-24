# ═══════════════════════════════════════════════════════════════
#  adstart Pipeline — Makefile
#  All commands in one place, for both local + AWS use
# ═══════════════════════════════════════════════════════════════
SHELL := /bin/bash
.DEFAULT_GOAL := help

# Load .env if present
-include .env
export

# ── Variables ────────────────────────────────────────────────────
PYTHON      := python3
PIP         := pip3
RUN_DATE    ?= $(shell date -u -d 'yesterday' +%Y-%m-%d 2>/dev/null || date -u -v-1d +%Y-%m-%d)
ACCOUNT_ID  ?= $(shell aws sts get-caller-identity --query Account --output text 2>/dev/null)
REGION      ?= eu-west-1

.PHONY: help install install-dev setup-aws upload-data run test test-unit test-integration \
        lint format typecheck clean teardown prefect-start prefect-deploy logs

# ── Help ──────────────────────────────────────────────────────────
help:  ## Show this help message
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-25s\033[0m %s\n", $$1, $$2}'
	@echo ""
	@echo "  Variables:"
	@echo "    RUN_DATE=$(RUN_DATE)"
	@echo "    ACCOUNT_ID=$(ACCOUNT_ID)"
	@echo "    REGION=$(REGION)"

# ── Install ───────────────────────────────────────────────────────
install:  ## Install production dependencies
	$(PIP) install -r requirements_aws.txt

install-dev:  ## Install dev + test dependencies
	$(PIP) install -r requirements_aws.txt
	$(PIP) install pytest pytest-mock moto[s3,glue,athena] ruff mypy

# ── AWS Setup ─────────────────────────────────────────────────────
setup-aws:  ## Create S3 buckets, Glue databases, IAM role, SNS topic
	@if [ -z "$(ACCOUNT_ID)" ]; then \
		echo "❌ AWS_ACCOUNT_ID not set. Run: export ACCOUNT_ID=123456789012"; \
		exit 1; \
	fi
	$(PYTHON) infrastructure/setup_aws.py \
		--account-id $(ACCOUNT_ID) \
		--region $(REGION)

setup-aws-dry:  ## Preview setup without creating real resources
	$(PYTHON) infrastructure/setup_aws.py \
		--account-id $(ACCOUNT_ID) \
		--region $(REGION) \
		--dry-run

upload-data:  ## Upload local sample CSVs to S3 (after setup-aws)
	$(PYTHON) infrastructure/upload_sample_data.py --date $(RUN_DATE)

upload-data-date:  ## Upload for a specific date: make upload-data-date RUN_DATE=2026-01-15
	$(PYTHON) infrastructure/upload_sample_data.py --date $(RUN_DATE)

# ── Run Pipeline ──────────────────────────────────────────────────
run:  ## Run pipeline for RUN_DATE (default: yesterday)
	$(PYTHON) -m src.orchestration.pipeline --date $(RUN_DATE)

run-local:  ## Run pipeline in local mode (DuckDB, no AWS required)
	PIPELINE_ENV=local $(PYTHON) -m src.orchestration.pipeline --date $(RUN_DATE)

backfill:  ## Backfill the last N days: make backfill DAYS=7
	DAYS ?= 7
	$(PYTHON) -m src.orchestration.pipeline --date $(RUN_DATE) --backfill-days $(DAYS)

# ── Tests ──────────────────────────────────────────────────────────
test:  ## Run all tests
	$(PYTHON) -m pytest tests/ -v

test-unit:  ## Run unit tests only (no AWS required)
	$(PYTHON) -m pytest tests/unit/ -v

test-integration:  ## Integration tests with moto (mock AWS)
	$(PYTHON) -m pytest tests/integration/ -v

test-cov:  ## Tests with coverage report
	$(PYTHON) -m pytest tests/ --cov=src --cov-report=term-missing --cov-report=html

# ── Code Quality ──────────────────────────────────────────────────
lint:  ## Lint with ruff
	ruff check src/ tests/

format:  ## Format with ruff
	ruff format src/ tests/

typecheck:  ## Type check with mypy
	mypy src/ --ignore-missing-imports

check:  ## Lint + format + typecheck
	$(MAKE) lint
	$(MAKE) format
	$(MAKE) typecheck

# ── Prefect ───────────────────────────────────────────────────────
prefect-start:  ## Start Prefect local server
	prefect server start

prefect-deploy:  ## Deploy flow to Prefect
	$(PYTHON) deployment/deploy.py

prefect-run:  ## Trigger a flow run immediately
	prefect deployment run 'adstart-daily-pipeline/daily' \
		--param run_date=$(RUN_DATE)

# ── Query / Inspect ───────────────────────────────────────────────
query-mart:  ## Query mart_daily_performance from Athena
	@aws athena start-query-execution \
		--query-string "SELECT * FROM mart_daily_performance WHERE report_date='$(RUN_DATE)' LIMIT 10" \
		--query-execution-context Database=$(GLUE_WAREHOUSE_DATABASE) \
		--result-configuration OutputLocation=s3://$(AWS_ATHENA_OUTPUT_BUCKET)/query-results/ \
		--output text

ls-raw:  ## List raw files on S3
	@aws s3 ls s3://$(AWS_RAW_BUCKET)/ --recursive | grep $(RUN_DATE)

ls-warehouse:  ## List warehouse Parquet files
	@aws s3 ls s3://$(AWS_WAREHOUSE_BUCKET)/ --recursive | grep $(RUN_DATE)

# ── Cleanup ───────────────────────────────────────────────────────
clean:  ## Remove local cache, logs, and build artifacts
	find . -type f -name "*.pyc" -delete
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name "htmlcov" -exec rm -rf {} + 2>/dev/null || true
	rm -f .coverage
	@echo "  ✓ Cache cleared"

teardown:  ## ⚠️  DELETE ALL AWS resources (dev/test only)
	@echo "⚠️  Are you sure you want to delete ALL resources? (yes/no)"
	@read ans && [ "$$ans" = "yes" ] && \
		$(PYTHON) infrastructure/teardown_aws.py \
			--account-id $(ACCOUNT_ID) \
			--region $(REGION) \
			--confirm || echo "Aborted."

# ── Logs ──────────────────────────────────────────────────────────
logs:  ## Tail the latest CloudWatch logs
	@aws logs tail /aws/adstart-pipeline/pipeline --follow --region $(REGION)