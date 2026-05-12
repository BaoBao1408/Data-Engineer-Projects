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
[RAW STAGING]                [DIMENSION]            [FACT TABLES]             [MART]
raw_operator_a  ─┐
raw_operator_b  ─┤           dim_campaigns  ────►  fct_clicks
raw_operator_c  ─┤                │          ────►  fct_subscriptions   ────►  mart_daily_performance
raw_campaigns   ─┤                │          ────►  fct_billing
raw_clicks      ─┤
raw_tracking    ─┘
raw_page_events ─┘

pipeline_runs  (audit log — every step recorded with status, rows_processed, error)
```

The full DDL is in `part3_pipeline_production_ready/schema.sql`. Written in DuckDB syntax (PostgreSQL equivalent is near-identical — only `VARCHAR` vs `UUID` type differs; DuckDB stores all IDs as `VARCHAR` to avoid UUID casting overhead on ingestion).

### Tables and Their Roles

| Table | Grain | Key Columns |
|-------|-------|-------------|
| `raw_operator_a/b/c` | 1 row per source CSV row | `_loaded_date` partition column added at ingest |
| `raw_campaigns/clicks/tracking_codes/page_events` | Mirror of source | Typed landing zone before any transformation |
| `dim_campaigns` | 1 row per campaign | `operator`, `service_name`, `service_model`, `partner_id` |
| `fct_subscriptions` | 1 row per opt-in | `rotate_id` (nullable for op_c), `attribution_method`, `report_date` |
| `fct_billing` | 1 row per charge | `is_first_bill`, `billing_sequence`, `subscription_id` (FK to fct_subscriptions) |
| `fct_clicks` | 1 row per click | `has_page_view/cta_click/entry/subscription/first_bill` (pre-computed funnel flags) |
| `mart_daily_performance` | 1 row per day × campaign | `sub_conversion_rate`, `bill_conversion_rate`, `total_revenue` |
| `pipeline_runs` | 1 row per pipeline step execution | `run_id`, `step`, `status`, `rows_processed`, `error_message` |

### Key Design Decisions

**1. Explicit raw staging layer** — raw tables mirror the CSV headers exactly (including the `received_time` → `event_time`/`clicked_at`/`created_at` renaming to avoid reserved keyword collision in DuckDB). This means a failed transform never corrupts the source data — re-running is always safe.

**2. `pipeline_runs` audit table** — every step (ingest, transform, load, mart) writes a row with `status`, `rows_processed`, and `error_message`. This gives full observability without needing an external orchestrator to inspect.

**3. Surrogate keys on fact tables** — each operator uses a different ID format (`transaction_id` for A/B, `message_id` for C). Generated string keys allow true unification. `source_transaction_id` retains the original for traceability.

**4. `attribution_method` column** — records *how* each subscription was attributed: `direct_rotate_id`, `tracking_code_lookup`, or `unattributed`. Critical for debugging metric anomalies and for giving the business a confidence level on reported numbers.

**5. Pre-computed flags** — `is_first_bill`, `billing_sequence`, and funnel flags on `fct_clicks` are computed at load time via window functions, then stored. Dashboard queries use `WHERE is_first_bill = TRUE` instead of re-running `ROW_NUMBER() OVER (...)` on every read.

**6. `report_date` as explicit `DATE NOT NULL`** — derived from each timestamp at transform time and stored as a plain `DATE` column. Enables fast date-range filtering and daily aggregation without casting timestamps at query time. (PostgreSQL source DDL uses `GENERATED ALWAYS AS (subscribed_at::DATE) STORED` for the same effect with zero ETL overhead.)

**7. Denormalized dimension keys** — `operator`, `service_name`, `partner_id` are copied into every fact table. Slicing by any of these axes never requires a join back to `dim_campaigns`.

### Attribution Logic by Operator

```
operator_a (event_code=1, status='SUCCESS'):
    rotate_id always present in source → direct insert to fct_subscriptions
    attribution_method = 'direct_rotate_id'

operator_b (transaction_type='SUB'):
    rotate_id present on SUB rows → direct insert to fct_subscriptions
    attribution_method = 'direct_rotate_id'

    REN rows (billing): no rotate_id — resolve via msisdn chain:
    REN.msisdn → most recent SUB row WHERE subscribed_at <= billed_at
              → inherit subscription_id from that SUB row
    (edge case: user resubscribes → take MAX(subscribed_at) ≤ billed_at)

operator_c (delivery_status='DELIVERED'):
    No rotate_id in source. Resolve via:
    operator_c.tracking_code
        JOIN raw_tracking_codes tc ON tc.code = operator_c.tracking_code
            AND operator_c.received_time BETWEEN tc.created_at AND tc.expired_at
            AND LENGTH(operator_c.tracking_code) = 3   ← critical filter
    If match found:  rotate_id = tc.rotate_id
                     attribution_method = 'tracking_code_lookup'
    If no match:     rotate_id = NULL
                     attribution_method = 'unattributed'
                     → row written to unattributed_events (not silently dropped)

Unattributed causes:
    - tracking_code LENGTH > 3 (96 rows, 13% of operator_c)
    - code expired (received_time > expired_at = created_at + 30 min)
    - code not found in tracking_codes at all
```

**`unattributed_events` table** — rows that cannot be attributed are written here rather than discarded. They remain queryable for monitoring (`attribution_match_rate` metric) and potential manual recovery if the tracking code issue is fixed upstream.

---

## Part 3 — ETL Pipeline

Two versions were built, each representing a stage of maturity.

### v1 — Working Pipeline (`part3_pipeline/`)

A functional Python pipeline demonstrating the core logic end-to-end:
- File ingestion per operator into raw staging tables
- Validation (schema, nulls, domain values)
- Attribution resolution per operator
- Fact table population

### v2 — Production-Ready Pipeline (`part3_pipeline_production_ready/`)

A fully layered, deployable system built on the same schema (`schema.sql`) with additional production concerns addressed.

**Architecture — 4 Layers matching the schema**

```
[INGEST]     CSV files ──► raw_operator_a/b/c, raw_campaigns, raw_clicks,
                           raw_tracking_codes, raw_page_events
                           (schema check, null check, domain validation at this step)

[TRANSFORM]  raw_* ──► enriched layer
                       (attribution resolution, billing sequence computation,
                        funnel flag pre-computation)

[LOAD]       enriched ──► dim_campaigns, fct_clicks, fct_subscriptions, fct_billing
                          (upsert with idempotency key = operator + source_date + transaction_id)

[MART]       fact tables ──► mart_daily_performance
                             (nightly aggregation, conversion rates pre-computed)

Every step ──► pipeline_runs (status, rows_processed, error_message logged)
```

**Tooling Choices**

| Concern | Choice | Rationale |
|---------|--------|-----------|
| Orchestration | Python scheduler + `pipeline_runs` table | Self-contained audit log; no Airflow dependency for this scale; re-runnable from any failed step |
| Transformation | Python + DuckDB/SQLAlchemy | DuckDB handles CSV → SQL in one step; near-zero setup; same logic portable to Postgres |
| Storage | DuckDB (local) / PostgreSQL (production) | Schema identical between both; DuckDB for fast local iteration, Postgres for multi-user production access |
| Containerisation | Docker + docker-compose | One-command local run; same image to production; Postgres service included |
| Testing | pytest | Unit tests on attribution logic, integration tests on full pipeline run |
| CI | GitHub Actions | Runs tests on every push; blocks merge on failure |
| AWS path | S3 (raw) → Glue (catalog) → Athena/Redshift | Documented in `part3_pipeline_AWS_Service/` — same logical layers, different execution engine |

**Idempotency** — every load step uses a composite idempotency key (`operator + source_date + source_transaction_id`). Running the pipeline twice on the same day is safe — `INSERT OR IGNORE` / `ON CONFLICT DO NOTHING` prevents duplicate rows.

**Late-arriving files** — the ingest step checks file presence before proceeding. If a file is missing, the step writes `status = 'failed'` to `pipeline_runs` with `error_message = 'source file not found'` and halts. No downstream step runs against incomplete data. Re-triggering once the file arrives is safe due to idempotency.

**Partial failure recovery** — because each layer writes to its own raw/staging table before promoting to facts, a failure mid-pipeline leaves prior layers intact. The `pipeline_runs` table records exactly which step failed, so re-running skips already-completed steps and resumes from the failure point.

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