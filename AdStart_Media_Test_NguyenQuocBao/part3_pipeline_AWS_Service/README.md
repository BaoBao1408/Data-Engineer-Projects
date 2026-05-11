# AdStart Media — Daily ETL Pipeline (Part 3)

> **Production-ready** · Operator billing data → DuckDB warehouse → Mart · Orchestrated by Prefect

---

## Table of Contents

1. [Architecture Overview](#1-architecture-overview)
2. [Project Structure](#2-project-structure)
3. [Quick Start — Docker (recommended)](#3-quick-start--docker-recommended)
4. [Quick Start — Local venv](#4-quick-start--local-venv)
5. [Pipeline Flow — Step by Step](#5-pipeline-flow--step-by-step)
6. [Warehouse Schema](#6-warehouse-schema)
7. [Known Data Issue: Unattributed Operator C Rows](#7-known-data-issue-unattributed-operator-c-rows)
8. [CI/CD — GitHub Actions](#8-cicd--github-actions)
9. [Makefile Commands](#9-makefile-commands)
10. [AWS Migration Path](#10-aws-migration-path)
11. [Bugs Fixed in This Version](#11-bugs-fixed-in-this-version)

---

## 1. Architecture Overview

```
data/raw/*.csv
      │
      ▼
┌─────────────────────────────────────────────────────────────────┐
│  INGEST + VALIDATE                  src/ingest/                  │
│  operator_A.csv ─┐                                               │
│  operator_B.csv ─┼──► raw_operator_a/b/c   (DuckDB staging)     │
│  operator_C.csv ─┘                                               │
│  campaigns.csv ──────► raw_campaigns                             │
│  clicks.csv ─────────► raw_clicks                                │
│  tracking_codes.csv ─► raw_tracking_codes                        │
│  page_events.csv ────► raw_page_events                           │
└─────────────────────────────────────────────────────────────────┘
      │  all rows pass null-rate + row-count validation
      ▼
┌─────────────────────────────────────────────────────────────────┐
│  DIMENSIONS                         src/transformations/         │
│  dim_campaigns  (upsert from raw_campaigns)                      │
└─────────────────────────────────────────────────────────────────┘
      │
      ▼
┌─────────────────────────────────────────────────────────────────┐
│  FACT TABLES                        src/transformations/         │
│                                                                  │
│  fct_subscriptions  ← operator_A: direct rotate_id              │
│                     ← operator_B: rotate_id on SUB rows only    │
│                     ← operator_C: tracking_code → rotate_id     │
│                                                                  │
│  fct_billing        ← billing events cross-joined to subs       │
│  fct_clicks         ← click funnel enriched with page_events    │
└─────────────────────────────────────────────────────────────────┘
      │
      ▼
┌─────────────────────────────────────────────────────────────────┐
│  MART                               sql/mart/                    │
│  mart_daily_performance  (pre-aggregated per campaign per day)   │
└─────────────────────────────────────────────────────────────────┘
      │
      ▼
┌─────────────────────────────────────────────────────────────────┐
│  QUALITY GATE                       src/orchestration/quality.py │
│  check_mart.sql + check_duplicates.sql                           │
│  → Raises ValueError and fails the Prefect flow on any breach   │
└─────────────────────────────────────────────────────────────────┘
      │
      ▼
data/warehouse/warehouse.duckdb
```

**Orchestration:** Prefect `@flow` / `@task` with retries.
**Storage:** DuckDB locally — swap to Redshift / Athena when scaling to AWS.
**Idempotency:** Every run deletes its own `report_date` partition before inserting — safe to re-run.

---

## 2. Project Structure

```
part3_pipeline_production_ready/
│
├── .github/
│   └── workflows/
│       └── ci.yml              ← CI/CD: lint → test → build/push → manual dispatch
│
├── deployment/
│   ├── Dockerfile              ← multi-stage (builder + runtime), PYTHONUTF8=1
│   └── instructor.txt          ← quick Docker command reference
│
├── config/
│   ├── __init__.py
│   ├── base.py                 ← all settings centralised (paths, thresholds, env)
│   └── logging_conf.py         ← structured logging, UTF-8 safe, file + stdout
│
├── src/
│   ├── ingest/
│   │   ├── loaders.py          ← reads CSV → DuckDB staging tables
│   │   └── validator.py        ← null-rate + row-count checks per table
│   ├── transformations/
│   │   ├── dimensions.py       ← builds dim_campaigns
│   │   ├── subscriptions.py    ← builds fct_subscriptions (3 operators)
│   │   └── billing_clicks_mart.py  ← builds fct_billing, fct_clicks, mart
│   ├── orchestration/
│   │   ├── pipeline.py         ← Prefect @flow — main entry point
│   │   └── quality.py          ← final quality gate (raises on failure)
│   └── utils/
│       └── db.py               ← DuckDB connection factory + SQL runner
│
├── sql/
│   ├── raw/                    ← staging SQL (operator_a/b/c.sql)
│   ├── dimensions/             ← dim_campaigns.sql
│   ├── facts/                  ← fct_subscriptions/billing/clicks per operator
│   ├── mart/                   ← mart_daily_performance.sql
│   └── quality/
│       ├── check_mart.sql      ← assertions: 0 rows returned = pass
│       └── check_duplicates.sql
│
├── tests/
│   ├── fixtures/conftest.py    ← shared in-memory DuckDB fixtures
│   ├── unit/test_dimensions.py
│   └── integration/test_pipeline.py
│
├── data/
│   ├── raw/                    ← source CSVs (input — do NOT commit .duckdb)
│   └── warehouse/              ← warehouse.duckdb (output — git-ignored)
│
├── logs/                       ← pipeline_YYYY-MM-DD.log files (git-ignored)
├── schema.sql                  ← ALL DDL — CREATE TABLE IF NOT EXISTS (idempotent)
├── pyproject.toml              ← project metadata + pytest config
├── requirements.txt
├── Makefile                    ← run `make help` to see all commands
├── docker-compose.yml
└── README.md
```

---

## 3. Quick Start — Docker (recommended)

No Python installation needed on your machine.

### Prerequisites

- Docker Desktop ≥ 24 running
- `data/raw/` folder populated with the 7 CSV files

### Build image

```bash
# From root of part3_pipeline_production_ready/
docker build -f deployment/Dockerfile -t adstart-pipeline .

# or via docker compose
docker compose build
```

### Run tests (8 tests, ~0.6 s)

```bash
docker compose run --rm pipeline-test
# Expected: 8 passed
```

### Run pipeline

```bash
# Process a specific date (sample data covers 2026-01-15)
docker compose run --rm pipeline --date 2026-01-15

# Process yesterday (production default — no args)
docker compose run --rm pipeline
```

Output warehouse: `data/warehouse/warehouse.duckdb`

### Inspect results

```bash
python - << 'EOF'
import duckdb
conn = duckdb.connect("data/warehouse/warehouse.duckdb")
print(conn.execute("""
    SELECT report_date, operator, service_name,
           total_clicks, total_subscriptions, total_first_bills, total_renewals,
           ROUND(total_revenue, 2)              AS revenue_gbp,
           ROUND(sub_conversion_rate * 100, 2)  AS sub_cvr_pct
    FROM mart_daily_performance
    ORDER BY report_date DESC, total_revenue DESC
""").fetchdf().to_string(index=False))
conn.close()
EOF
```

### Debug shell

```bash
docker compose run --rm --entrypoint bash pipeline
# Inside container:
pytest tests/ -v
python src/orchestration/pipeline.py --date 2026-01-15
```

---

## 4. Quick Start — Local venv

```bash
# 1. Create venv
python -m venv .venv
source .venv/bin/activate         # Linux / Mac
# .venv\Scripts\activate          # Windows

# 2. Install dependencies
pip install -e ".[dev]"

# 3. Run tests
pytest tests/ -v

# 4. Run pipeline
PYTHONUTF8=1 python src/orchestration/pipeline.py --date 2026-01-15
```

> **Windows note:** Always prefix with `set PYTHONUTF8=1 &&` or add it to your `.env` to avoid Unicode errors with the `═` log characters.

---

## 5. Pipeline Flow — Step by Step

When you run `pipeline.py --date 2026-01-15`, Prefect executes these tasks in order:

```
[task] ingest-operator-a    ─┐
[task] ingest-operator-b    ─┤ retries=3, delay=60s
[task] ingest-operator-c    ─┤ (handles S3 eventual consistency on AWS)
[task] ingest-static        ─┘
         │
         ▼
[task] build-dim-campaigns
         │
         ▼
[task] build-fct-subscriptions   ← depends on dims
         │
         ▼
[task] build-fct-billing         ← depends on fct_subscriptions
         │
         ▼
[task] build-fct-clicks          ← depends on fct_subscriptions
         │
         ▼
[task] build-mart                ← aggregates all facts
         │
         ▼
[task] quality-checks            ← FAILS HARD if any assertion fails
```

**Expected log output (success):**

```
INFO  [raw_operator_a] Loaded 3,194 rows — checks passed.
INFO  [raw_operator_b] Loaded 3,273 rows — checks passed.
WARN  [raw_operator_c] 62 DELIVERED rows have tracking_code > 3 chars — will be unattributed.
INFO  [raw_operator_c] Loaded 741 rows — checks passed.
INFO  [dim_campaigns] 10 rows total.
WARN  [fct_subscriptions] 62 operator_C DELIVERED rows unattributed for 2026-01-15.
INFO  [fct_subscriptions] 58 rows inserted for 2026-01-15.
INFO  [fct_billing] 75 rows inserted for 2026-01-15.
INFO  [fct_clicks] 198 rows inserted for 2026-01-15.
INFO  [mart_daily_performance] 10 rows inserted for 2026-01-15.
INFO  All quality checks passed. Mart has 10 campaign rows.
```

The WARNING about 62 unattributed rows is **expected and by design** — see Section 7 below.

---

## 6. Warehouse Schema

| Table | Type | Key | Description |
|---|---|---|---|
| `raw_operator_a/b/c` | Staging | `_loaded_date` | Daily append from CSVs |
| `raw_campaigns` | Staging | — | Full-refresh reference |
| `raw_clicks` | Staging | `rotate_id` | Full-refresh |
| `raw_tracking_codes` | Staging | `code` | Full-refresh, 30-min validity window |
| `raw_page_events` | Staging | `event_id` | Full-refresh |
| `dim_campaigns` | Dimension | `campaign_id` | Upsert, SCD-0 |
| `fct_subscriptions` | Fact | `subscription_id` | Partitioned by `report_date` |
| `fct_billing` | Fact | `billing_id` | `is_first_bill`, `billing_sequence` |
| `fct_clicks` | Fact | `rotate_id` | Enriched with page funnel flags |
| `mart_daily_performance` | Mart | `(report_date, campaign_id)` | Pre-aggregated for BI |
| `pipeline_runs` | Audit | `run_id` | Step-level audit trail |

---

## 7. Known Data Issue: Unattributed Operator C Rows

### What happens

Operator C uses a 3-character tracking code to link subscriptions back to campaigns. Due to a **bug in the operator's SMS parser**, ~13% of `DELIVERED` rows arrive with a `tracking_code` longer than 3 characters (e.g. `"A3B_extra"`).

The pipeline filters these out at transformation:

```sql
-- sql/facts/fct_subscriptions_operator_c.sql
WHERE LENGTH(oc.tracking_code) <= 3   -- guard against SMS parser suffix bug
```

These 62 rows (on 2026-01-15) **exist in `raw_operator_c`** (staging) but **never reach** `fct_subscriptions` or `mart_daily_performance`.

---

### Impact on BI metrics

Yes — the mart metrics are **understated for operator C**:

| Metric | Impact |
|---|---|
| `total_subscriptions` | Under-counted by N unattributed subs |
| `total_first_bills` | Under-counted (no sub → no first bill join) |
| `total_renewals` | Under-counted (future renewals of unattributed subs also lost) |
| `total_revenue` | Under-counted (first bill + all renewal revenue missing) |
| `sub_conversion_rate` | Numerator understated → rate too low for operator C campaigns |
| `bill_conversion_rate` | Same issue |

In the sample data this is **62 out of ~120 operator C subscriptions** — roughly **50% of operator C revenue is invisible to the mart**. In production, the severity depends on how widespread the parser bug is.

---

### Real-world solution for BI

The standard industry approach is a **3-layer strategy**:

#### Layer 1 — Quarantine table (preserve the data)

Add a `fct_unattributed_events` table to capture what was excluded:

```sql
CREATE TABLE IF NOT EXISTS fct_unattributed_events (
    event_id          VARCHAR PRIMARY KEY,
    operator          VARCHAR NOT NULL,           -- 'operator_C'
    source_table      VARCHAR NOT NULL,           -- 'raw_operator_c'
    msisdn            VARCHAR,
    raw_tracking_code VARCHAR,
    event_time        TIMESTAMPTZ,
    report_date       DATE NOT NULL,
    unattributed_reason VARCHAR NOT NULL,
    -- 'tracking_code_too_long' | 'no_matching_code' | 'code_expired'
    loaded_at         TIMESTAMPTZ DEFAULT now()
);
```

The `fct_subscriptions_operator_c.sql` INSERT already excludes these rows — add a parallel INSERT INTO `fct_unattributed_events` for the rows that fail the join or the length check. This costs nothing to implement but preserves 100% of the data for recovery.

#### Layer 2 — Extend the mart for BI transparency

Add `unattributed_subscriptions` and `unattributed_revenue_est` columns to `mart_daily_performance`:

```sql
-- mart_daily_performance additions
unattributed_subscriptions  INTEGER  DEFAULT 0,
unattributed_revenue_est    DECIMAL(12,4) DEFAULT 0,
-- estimated using avg revenue per attributed sub for same operator/service
attribution_rate            DECIMAL(6,4)
-- = total_subscriptions / (total_subscriptions + unattributed_subscriptions)
```

BI dashboards then show:
- **Attributed revenue** — confirmed, campaign-linked
- **Unattributed revenue (est.)** — approximate but visible
- **Attribution rate %** — key health metric; alert if < 90%

This way analysts see the **full economic picture** and know when data quality is degrading.

#### Layer 3 — Alert and fix at source

Add a quality check that **fails the pipeline** (or sends an alert) if the unattributed rate exceeds a threshold:

```sql
-- sql/quality/check_attribution_rate.sql
SELECT 'operator_c_attribution_rate_below_threshold' AS check_name,
       COUNT(*) AS failing_rows
FROM (
    SELECT
        SUM(CASE WHEN LENGTH(tracking_code) <= 3 THEN 1 ELSE 0 END)  AS attributed,
        SUM(CASE WHEN LENGTH(tracking_code) >  3 THEN 1 ELSE 0 END)  AS unattributed
    FROM raw_operator_c
    WHERE delivery_status = 'DELIVERED'
      AND _loaded_date = :run_date
) t
WHERE CAST(unattributed AS FLOAT) / NULLIF(attributed + unattributed, 0) > 0.20
-- Fail if > 20% unattributed (current baseline is ~13%; 20% = regression)
```

The real long-term fix is to push back to the operator to repair their SMS parser — the `tracking_code` should always be exactly 3 characters for `DELIVERED` events. Until then, the quarantine table and estimated revenue columns ensure the BI layer is **honest and complete** rather than silently wrong.

---

### Summary decision matrix

| Approach | Effort | BI accuracy | Recommended for |
|---|---|---|---|
| Current (exclude + warn) | Done | Revenue understated | Development/testing only |
| Quarantine table | Low | Full data preserved | **Minimum for production** |
| Mart with unattributed columns | Medium | Transparent estimates | **Standard production** |
| Alert on attribution rate | Low | Proactive detection | **Always add** |
| Fix SMS parser at operator | External | Perfect | Long-term goal |

---

## 8. CI/CD — GitHub Actions

File: `.github/workflows/ci.yml`

```
Push to main / develop
        │
        ▼
  ┌── lint ─────────────────────────────────────────────────────┐
  │  ruff check src/ config/ tests/                             │
  └─────────────────────────────────────────────────────────────┘
        │ pass
        ▼
  ┌── test ─────────────────────────────────────────────────────┐
  │  docker build → pytest inside container                     │
  │  8 tests (unit + integration)                               │
  └─────────────────────────────────────────────────────────────┘
        │ pass, main branch only
        ▼
  ┌── build-and-push ───────────────────────────────────────────┐
  │  docker buildx → push to GHCR                              │
  │  tags: latest | sha-<commit> | YYYY-MM-DD                  │
  └─────────────────────────────────────────────────────────────┘
        │ workflow_dispatch only
        ▼
  ┌── run-pipeline (manual backfill) ──────────────────────────┐
  │  Actions → CI/CD Pipeline → Run workflow → input run_date  │
  └─────────────────────────────────────────────────────────────┘
```

### Required GitHub secret

| Secret | Value |
|---|---|
| `GITHUB_TOKEN` | Auto-provided by GitHub — no action needed |

### Manual backfill via UI

1. GitHub repo → **Actions** → **CI/CD Pipeline**
2. Click **Run workflow**
3. Enter `run_date` (e.g. `2026-01-15`) → **Run workflow**

---

## 9. Makefile Commands

```bash
make help           # show all commands

# Local
make install        # create .venv + install deps (run once)
make test           # pytest locally
make run            # run pipeline --date 2026-01-15
make run-yesterday  # run pipeline for yesterday

# Docker
make build          # docker compose build
make docker-test    # pytest inside container
make docker-run     # pipeline --date 2026-01-15 in container
make docker-run-date DATE=2026-01-20   # specific date
make docker-shell   # interactive bash in container
make logs           # docker logs adstart_pipeline

# Cleanup
make clean          # remove warehouse.duckdb + logs
make clean-all      # also remove Docker image + .venv
```

---

## 10. AWS Migration Path

All swap points are marked with `# AWS:` comments in the code.

| Local | AWS Equivalent |
|---|---|
| `data/raw/*.csv` | S3: `s3://adstart-raw/operator_X/date=YYYY-MM-DD/` |
| `warehouse.duckdb` | Redshift or Athena + S3 Parquet |
| Prefect `@flow` | Step Functions state machine |
| Prefect `@task` with `retries=3` | Glue job or Lambda with Step Functions retry |
| `logs/` folder | CloudWatch Logs (via `awslogs` Docker driver) |
| Quality `raise ValueError` | SNS topic alert → PagerDuty / Slack |
| `pipeline_runs` table | DynamoDB run-state table |

**Uncomment in `requirements.txt` when deploying:**

```
awswrangler>=3.5.0
boto3>=1.34.0
pyarrow>=14.0.0
```

---

## 11. Bugs Fixed in This Version

| File | Bug | Fix |
|---|---|---|
| `sql/quality/check_mart.sql` | Comment contained `;` → `quality.py` split the SQL incorrectly → `Parser Error: syntax error at or near "alert"` | Replaced `;` with `,` in the comment |
| `deployment/Dockerfile` | `apt-get install -y --build-essential` (double-dash) → exit code 100 | Removed `--` prefix from package names |
| `deployment/Dockerfile` | Missing `PYTHONUTF8=1` → Windows cp1252 UnicodeEncodeError on `═` characters in log messages | Added `ENV PYTHONUTF8=1` to both stages |
| `deployment/Dockerfile` | `pytest` not installed inside image → `docker compose run pipeline-test` fails | Added `pytest pytest-cov anyio` to builder stage |
| `docker-compose.yml` | Missing `PYTHONUTF8=1` in environment | Added to both services |
| `.github/workflow/` | Folder named `workflow` (singular) → GitHub Actions ignores it entirely | Renamed to `.github/workflows/` (plural) |
| `.github/workflows/ci.yml` | File was empty | Rewritten: lint → test → build/push → manual dispatch |
