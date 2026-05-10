# AdStart ETL Pipeline

Daily ETL pipeline: 3 operator CSV files → DuckDB facts → BI-ready mart.

## Architecture

```
adstart_etl/
├── config/                  # Settings + logging (environment-aware)
├── sql/
│   ├── raw/                 # Idempotent staging loads (1 file per operator)
│   ├── dimensions/          # SCD Type 1 dim_campaigns
│   ├── facts/               # fct_subscriptions, fct_billing, fct_clicks
│   ├── mart/                # mart_daily_performance
│   └── quality/             # SQL assertion checks
├── src/
│   ├── ingest/              # loaders.py + validator.py
│   ├── transformations/     # dimensions.py, subscriptions.py, billing_clicks_mart.py
│   ├── orchestration/       # pipeline.py (Prefect) + quality.py
│   └── utils/               # db.py (connection factory + SQL runner)
├── tests/
│   ├── unit/                # Schema + dimension tests
│   ├── integration/         # Full ETL scenario tests
│   └── fixtures/            # Shared conftest.py
├── schema.sql               # All DDL (CREATE TABLE IF NOT EXISTS)
└── deployment/              # Dockerfile, Terraform
```

## Key Design Decisions

| Concern | Decision |
|---|---|
| SQL lives in `.sql` files | Python is orchestration only — no inline SQL |
| Idempotency | Each SQL file `DELETE WHERE report_date = :run_date` before inserting |
| Attribution | Operator A/B: direct `rotate_id`. Operator B REN: `msisdn → SUB → rotate_id`. Operator C: `tracking_code → lookup → rotate_id` |
| Data quality | Bad tracking codes (`>3 chars`) logged, not dropped. Unattributed rows counted and surfaced |
| Config | Single `config/base.py` — swap paths for AWS (S3/Redshift) without touching business logic |

## Running Locally

```bash
pip install -r requirements.txt

# Run for yesterday
python -m src.orchestration.pipeline

# Run for a specific date
python -m src.orchestration.pipeline --date 2026-01-15

# Run tests
pytest tests/ -v
```

## AWS Migration Guide

| Local component | AWS equivalent |
|---|---|
| `duckdb.connect(db_path)` | Redshift `psycopg2` / Athena `boto3` |
| `read_csv_auto(file_path)` | S3 path via Glue DynamicFrame |
| Prefect `@flow` | Step Functions state machine |
| Prefect `@task` | Lambda function or Glue job |
| `pipeline_runs` table | DynamoDB run state + CloudWatch Logs |
| `mart_daily_performance` | Redshift materialized view or dbt model |
