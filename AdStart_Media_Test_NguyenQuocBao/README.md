# AdStart Media — Data Engineer Test Submission
### Nguyen Quoc Bao

> **GitHub Repository:** https://github.com/BaoBao1408/Data-Engineer-Projects/tree/main/AdStart_Media_Test_NguyenQuocBao
>
> *(If you prefer not to access the link, all key answers and design decisions are documented in this summary.)*

---

## What Was Delivered

This submission covers all 5 parts of the Data Engineer test for AdStart Media's mobile advertising platform (UK, January 2026). The dataset involves 7 source tables across 3 operators and a shared click/campaign infrastructure.

```
AdStart_Media_Test_NguyenQuocBao/
├── Part1_2_SQL/              # SQL answers for Parts 1 & 2 with screenshots
├── Part4_5_DataValidateAndChanging/  # Written answers for Parts 4 & 5
├── part3_pipeline/           # Initial working pipeline (v1)
├── part3_pipeline_AWS_Service/       # AWS-adapted architecture sketch
├── part3_pipeline_production_ready/  # Full production pipeline (v2)
│   ├── src/                  # Layered ETL source code
│   ├── sql/                  # DDL for all mart tables
│   ├── config/               # Environment-based config
│   ├── tests/                # pytest test suite
│   ├── deployment/           # Docker + docker-compose
│   ├── docs/                 # Architecture documentation
│   └── Makefile              # One-command run/test/deploy
├── screenshots/              # Query result screenshots
├── DE_test_answers.md        # Full written answers (Parts 1–5)
└── schema.sql                # Source schema reference
```

---

## Part 1 — Data Exploration

**Goal:** Understand data quality, operator-specific formats, and edge cases before building anything.

### Key Findings

| Finding | Table | Impact |
|---------|-------|--------|
| `msisdn` 89.6% null | `page_events` | Expected by design — only `ENTRY` events carry msisdn |
| `rotate_id` 75.9% null | `operator_b` | Expected — only `SUB` rows have an active session |
| 13% of tracking codes > 3 chars | `operator_c` | **Attribution loss** — 62 delivered events cannot be linked to any campaign |
| 82 bills arrive before subscription | `operator_a` | Race condition — up to 120s delay between subscribe and bill confirmation |

### Operator-A Event Code Interpretation

| event_code | Meaning | Success Rate | Amount |
|-----------|---------|-------------|--------|
| 1 | Subscribe (opt-in) | ~48% | £0.00 |
| 2 | Bill (recurring charge) | ~51% | £1.99–£3.49 |
| 3 | Unsubscribe | 100% | £0.00 |

The 100% success rate on unsubscribes (event_code=3) is particularly notable — cancellations are always acknowledged regardless of account state, which is standard telco practice.

### Operator-B Attribution Limitation

Operator B only sends `rotate_id` on `SUB` rows. `REN` (renewal) and `UNSUB` rows have no session link. This means **revenue from renewals can only be attributed indirectly**, via `msisdn` chaining: `REN.msisdn → SUB.msisdn → SUB.rotate_id → campaign`. This is architecturally fragile if a user re-subscribes.

### Operator-C Tracking Code Issue

96 of 741 rows (13%) contain codes longer than 3 characters, making the `JOIN` to `tracking_codes.code` impossible. Root cause analysis:

- **8 rows (8.3%):** Likely user typos — the first 3 characters match a valid code
- **88 rows (91.7%):** Completely unknown prefix — suggests an upstream SMS parser is appending network identifiers or session tokens before storing the value

---

## Part 2 — Data Modeling

**Goal:** Unified daily view of subscriptions, first bills, revenue, and conversion across all 3 operators and all partners.

### Schema Design — Modified Star Schema

```
dim_campaigns  (campaign metadata — single source of truth)
      │
      ├──► fct_clicks          (grain: 1 row per click, with funnel flags)
      │         │
      ├──► fct_subscriptions   (grain: 1 row per opt-in, all 3 operators unified)
      │         │
      └──► fct_billing         (grain: 1 row per charge, is_first_bill pre-computed)

All three ──► mart_daily_performance  (pre-aggregated, refreshed nightly)
```

### Key Design Decisions

**1. Surrogate keys on fact tables** — each operator uses a different ID format. A generated UUID as the primary key allows true unification without collision risk. `source_transaction_id` retains the original for traceability.

**2. `attribution_method` column** — records *how* each subscription was attributed: `direct_rotate_id`, `tracking_code_lookup`, or `unattributed`. This is critical for debugging metric anomalies and giving the business a confidence level on reported numbers.

**3. Pre-computed flags** — `is_first_bill`, `billing_sequence`, and funnel flags (`has_page_view`, `has_cta_click`, `has_entry`, `has_subscription`, `has_first_bill`) are computed at load time. This converts expensive window function queries into simple `WHERE is_first_bill = TRUE` filters.

**4. `report_date` as STORED GENERATED column** — computed once at insert from the timestamp, enabling fast date partitioning with zero overhead at query time.

**5. Denormalized dimension keys** — `operator`, `service_name`, `partner_id` are copied into every fact table. Slicing by any of these axes never requires a join back to `dim_campaigns`.

### Attribution Logic by Operator

```
operator_a (event_code=1):
    rotate_id present → direct insert
    attribution_method = 'direct_rotate_id'

operator_b (transaction_type='SUB'):
    rotate_id present → direct insert
    attribution_method = 'direct_rotate_id'

    REN rows: no rotate_id — chain via msisdn:
    REN.msisdn → most recent SUB row ≤ billed_at → subscription_id

operator_c (delivery_status='DELIVERED'):
    No rotate_id — must look up:
    tracking_code → JOIN tracking_codes ON code = tracking_code
                    AND received_time BETWEEN created_at AND expired_at
    If found:     attribution_method = 'tracking_code_lookup'
    If not found: attribution_method = 'unattributed'
                  → row goes to unattributed_events table (not silently dropped)
```

**Unattributed events table** — a dedicated staging table captures every event that cannot be attributed (expired codes, length > 3 chars, orphaned billing). These rows are not lost — they are queryable for monitoring and potential manual recovery.

---

## Part 3 — ETL Pipeline

Two versions were built, each representing a stage of maturity.

### v1 — Working Pipeline (`part3_pipeline/`)

A functional Python pipeline demonstrating the core logic:
- File ingestion per operator
- Staging and validation
- Attribution resolution
- Fact table population

### v2 — Production-Ready Pipeline (`part3_pipeline_production_ready/`)

A fully layered, deployable system with the following characteristics:

**Architecture — 4 Layers**

```
[INGEST]     Raw files → staging (schema check, null check, domain validation)
[TRANSFORM]  Staging → enriched (attribution resolution, sequence computation)
[LOAD]       Enriched → fact tables (upsert with idempotency key)
[MART]       Fact tables → mart_daily_performance (nightly aggregation)
```

**Tooling Choices**

| Concern | Choice | Rationale |
|---------|--------|-----------|
| Orchestration | Airflow (DAG per operator) | Native retry, SLA alerts, dependency graph, UI for on-call |
| Transformation | Python + SQLAlchemy | dbt-compatible SQL patterns; avoids heavyweight tooling for 3-operator scope |
| Storage | PostgreSQL (partitioned by `report_date`) | Fits the current data volume; straightforward to migrate to BigQuery/Redshift |
| Containerisation | Docker + docker-compose | One-command local run for reviewers; same image to production |
| Testing | pytest | Unit tests on attribution logic, integration tests on full pipeline run |
| CI | GitHub Actions | Runs tests on every push; blocks merge on failure |

**Idempotency** — every load step uses an `idempotency_key` (`operator + source_date + source_transaction_id`). Running the pipeline twice on the same day is safe — duplicate rows are detected and skipped, not inserted twice.

**Late-arriving files** — the DAG uses a sensor with a configurable timeout. If a file does not arrive by SLA, the sensor times out and raises a critical alert rather than silently skipping. Once the file arrives, the DAG can be manually triggered and the idempotency logic ensures no double-counting.

**Partial failure recovery** — each layer writes to its own staging table before promoting to facts. A failure mid-pipeline leaves the previous layer intact. Re-running resumes from the failed layer.

---

## Part 4 — Data Validation

Validation is applied at three distinct stages:

### At the source (before staging)
- Schema check (expected columns present)
- Row count plausibility (not empty, not suspiciously small)
- Primary key uniqueness within the file (catches duplicate delivery)
- Date range sanity (`received_time` within expected window ±1 day)
- Domain value checks (`event_code` ∈ {1,2,3}; `transaction_type` ∈ {SUB, REN, UNSUB}; etc.)
- `amount >= 0` (negative amounts indicate unhandled refund events)

### During transformation (staging → fact)
- `not_null` on all NOT NULL columns
- `unique` on surrogate keys
- Referential integrity (`campaign_id` exists in `dim_campaigns`; `rotate_id` in `fct_clicks`)
- Custom: no msisdn should have `is_first_bill = TRUE` more than once
- Custom: all operator_b `REN` rows should resolve to a subscription (orphan billing = join failure)

### In the final output (mart layer)
- Conversion rates bounded at [0, 1] — exceeding 1.0 indicates a join fan-out bug
- Cumulative revenue non-decreasing day-over-day
- All expected `(report_date, operator)` combinations present

### Ongoing monitoring
- Daily revenue per operator: alert if deviation from 7-day rolling average exceeds ±40%
- Subscription count: alert if drops below 50% of 7-day average
- Operator C attribution match rate: alert if drops below 80% (baseline ~87%)
- Silent source detection: sensor timeout + daily audit query on staging loaded dates

**Alerting tiers:** Critical (PagerDuty) → Warning (Slack #data-alerts) → Info (daily summary post)

---

## Part 5 — Platform Improvements

Six concrete proposals for improving the platform's data collection layer:

| # | Proposal | Problem Solved | Complexity |
|---|----------|----------------|-----------|
| 1 | Add `rotate_id` to all operator B billing rows | Removes indirect msisdn attribution chain; eliminates resubscription edge case | Operator-side change |
| 2 | Enforce 3-char tracking codes at SMS ingestion | Prevents 13% attribution loss on operator C; moves failure to recoverable stage | Platform-side |
| 3 | Denormalise `partner_id` onto `clicks` at insert time | Point-in-time attribution; removes join; prevents silent changes if campaign reassigned | Schema addition |
| 4 | Add `funnel_session_id` to `page_events` | Links all events from a single user visit; fixes multi-visit ambiguity | Frontend change |
| 5 | Capture `msisdn` on `CLICK_CTA` where already known | Enables re-engagement vs new acquisition segmentation | GDPR review needed |
| 6 | Check `expired_at` at SMS receipt time | Makes expiry-related attribution loss explicit and measurable rather than silent | Platform-side logic |

The highest-value quick win is **#2** — it requires no operator coordination, is entirely platform-side, and immediately recovers attribution for 13% of operator C's delivered events.

---

## How to Run Locally

```bash
# Clone the repository
git clone https://github.com/BaoBao1408/Data-Engineer-Projects.git
cd AdStart_Media_Test_NguyenQuocBao/part3_pipeline_production_ready

# Copy environment config
cp .env.example .env

# Start all services (Postgres + pipeline)
docker-compose up --build

# Run the full pipeline
make run

# Run tests
make test
```

---

## A Note on Approach

This test was treated as a real engineering problem rather than an academic exercise.

The Part 1 SQL queries were written to go beyond counting rows — each query was designed to expose the *business logic* behind the numbers (why are 75.9% of rotate_ids null? what does a 100% unsubscribe success rate actually mean for the platform?).

The data model in Part 2 was designed with the assumption that the schema will outlive the test — the `attribution_method` audit column, the `unattributed_events` staging table, and the `billing_sequence` ordinal exist because real pipelines break in ways that only become visible months later.

The production pipeline was rebuilt from scratch after the initial version worked — not because it was required, but because the gap between "runs once" and "runs reliably every day" is where the actual engineering lives.

---

*Nguyen Quoc Bao — May 2026*
*GitHub: https://github.com/BaoBao1408/Data-Engineer-Projects/tree/main/AdStart_Media_Test_NguyenQuocBao*