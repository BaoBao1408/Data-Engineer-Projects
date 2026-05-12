# ═══════════════════════════════════════════════════════════════
#  adstart Pipeline — Makefile
#  Tất cả commands trong 1 nơi, dùng cho local + AWS
# ═══════════════════════════════════════════════════════════════
SHELL := /bin/bash
.DEFAULT_GOAL := help

# Load .env nếu có
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
setup-aws:  ## Tạo S3 buckets, Glue databases, IAM role, SNS topic
	@if [ -z "$(ACCOUNT_ID)" ]; then \
		echo "❌ AWS_ACCOUNT_ID not set. Chạy: export ACCOUNT_ID=123456789012"; \
		exit 1; \
	fi
	$(PYTHON) infrastructure/setup_aws.py \
		--account-id $(ACCOUNT_ID) \
		--region $(REGION)

setup-aws-dry:  ## Preview setup mà không tạo resources thật
	$(PYTHON) infrastructure/setup_aws.py \
		--account-id $(ACCOUNT_ID) \
		--region $(REGION) \
		--dry-run

upload-data:  ## Upload local sample CSVs lên S3 (sau khi setup-aws)
	$(PYTHON) infrastructure/upload_sample_data.py --date $(RUN_DATE)

upload-data-date:  ## Upload cho ngày cụ thể: make upload-data-date RUN_DATE=2026-01-15
	$(PYTHON) infrastructure/upload_sample_data.py --date $(RUN_DATE)

# ── Run Pipeline ──────────────────────────────────────────────────
run:  ## Chạy pipeline cho RUN_DATE (default: hôm qua)
	$(PYTHON) -m src.orchestration.pipeline --date $(RUN_DATE)

run-local:  ## Chạy pipeline local mode (DuckDB, không cần AWS)
	PIPELINE_ENV=local $(PYTHON) -m src.orchestration.pipeline --date $(RUN_DATE)

backfill:  ## Backfill 7 ngày gần nhất: make backfill DAYS=7
	DAYS ?= 7
	$(PYTHON) -m src.orchestration.pipeline --date $(RUN_DATE) --backfill-days $(DAYS)

# ── Tests ──────────────────────────────────────────────────────────
test:  ## Chạy tất cả tests
	$(PYTHON) -m pytest tests/ -v

test-unit:  ## Chỉ chạy unit tests (không cần AWS)
	$(PYTHON) -m pytest tests/unit/ -v

test-integration:  ## Integration tests với moto (mock AWS)
	$(PYTHON) -m pytest tests/integration/ -v

test-cov:  ## Tests với coverage report
	$(PYTHON) -m pytest tests/ --cov=src --cov-report=term-missing --cov-report=html

# ── Code Quality ──────────────────────────────────────────────────
lint:  ## Lint với ruff
	ruff check src/ tests/

format:  ## Format với ruff
	ruff format src/ tests/

typecheck:  ## Type check với mypy
	mypy src/ --ignore-missing-imports

check:  ## Lint + format + typecheck
	$(MAKE) lint
	$(MAKE) format
	$(MAKE) typecheck

# ── Prefect ───────────────────────────────────────────────────────
prefect-start:  ## Start Prefect local server
	prefect server start

prefect-deploy:  ## Deploy flow lên Prefect
	$(PYTHON) deployment/deploy.py

prefect-run:  ## Trigger flow run ngay lập tức
	prefect deployment run 'adstart-daily-pipeline/daily' \
		--param run_date=$(RUN_DATE)

# ── Query / Inspect ───────────────────────────────────────────────
query-mart:  ## Query mart_daily_performance từ Athena
	@aws athena start-query-execution \
		--query-string "SELECT * FROM mart_daily_performance WHERE report_date='$(RUN_DATE)' LIMIT 10" \
		--query-execution-context Database=$(GLUE_WAREHOUSE_DATABASE) \
		--result-configuration OutputLocation=s3://$(AWS_ATHENA_OUTPUT_BUCKET)/query-results/ \
		--output text

ls-raw:  ## List raw files trên S3
	@aws s3 ls s3://$(AWS_RAW_BUCKET)/ --recursive | grep $(RUN_DATE)

ls-warehouse:  ## List warehouse Parquet files
	@aws s3 ls s3://$(AWS_WAREHOUSE_BUCKET)/ --recursive | grep $(RUN_DATE)

# ── Cleanup ───────────────────────────────────────────────────────
clean:  ## Xóa local cache, logs, build artifacts
	find . -type f -name "*.pyc" -delete
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name "htmlcov" -exec rm -rf {} + 2>/dev/null || true
	rm -f .coverage
	@echo "  ✓ Cache cleared"

teardown:  ## ⚠️  XÓA TẤT CẢ AWS resources (chỉ dev/test)
	@echo "⚠️  Bạn có chắc muốn xóa TẤT CẢ resources không? (yes/no)"
	@read ans && [ "$$ans" = "yes" ] && \
		$(PYTHON) infrastructure/teardown_aws.py \
			--account-id $(ACCOUNT_ID) \
			--region $(REGION) \
			--confirm || echo "Hủy bỏ."

# ── Logs ──────────────────────────────────────────────────────────
logs:  ## Xem CloudWatch logs gần nhất
	@aws logs tail /aws/adstart-pipeline/pipeline --follow --region $(REGION)
