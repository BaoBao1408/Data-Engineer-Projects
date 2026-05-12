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
├── Part1_2_SQL/                      # SQL answers for Parts 1 & 2 with screenshots
├── Part4_5_DataValidateAndChanging/  # Written answers for Parts 4 & 5
├── part3_pipeline/                   # Initial working pipeline (v1)
├── part3_pipeline_AWS_Service/       # AWS-adapted architecture sketch
├── part3_pipeline_production_ready/  # Full production pipeline (v2)
│   ├── src/                  # Layered ETL source code
│   ├── sql/                  # DDL for all mart tables (schema.sql)
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

## Part 1 — Data Exploration & Pain Points Discovered

**Goal:** Understand data quality, operator-specific formats, and hidden edge cases before building anything. All findings below are verified directly from SQL queries against the actual CSV data.

### Summary of All 7 Pain Points

| # | Pain Point | Table(s) | Rows Affected | Business Impact |
|---|-----------|----------|---------------|----------------|
| 1 | `received_time` extends to Feb–Mar 2026 | `operator_a`, `operator_b` | 805 + 1,312 rows | Filtering by Jan 2026 silently drops ~£4,355 in real renewal revenue |
| 2 | Operator B `SUB` charges money — not free opt-in | `operator_b` | 788 rows | 25.6% of total revenue (£2,235) misclassified if treated as £0 activation |
| 3 | Operator B `REN` has no `rotate_id` → indirect attribution | `operator_b` | 2,286 rows | Attribution chain breaks for any re-subscriber in production |
| 4 | 13% of operator C tracking codes are > 3 chars | `operator_c` | 96 rows | 62 `DELIVERED` events lose campaign attribution entirely |
| 5 | 25 valid codes arrived after the `expired_at` window | `operator_c` | 25 rows (15 DELIVERED) | Silent loss — looks identical to "code not found" without explicit check |
| 6 | 82 bills arrive before subscribe (race condition) | `operator_a` | 82 rows | Event ordering by `received_time` marks these bills as orphans |
| 7 | `msisdn` auto-inferred as `BIGINT` by DuckDB | All operator tables | All rows | Leading zeros in UK numbers dropped → cross-table JOINs silently fail |

---

### Pain Point 1 — received_time extends beyond January 2026

Operator A has 805 rows with `received_time` up to **08 March 2026**. Operator B has 1,312 rows up to **07 March 2026**. These are not late-arriving January events — they are genuine **renewal charges** from users who subscribed in January and continued paying.

```
operator_a Feb:  341 SUCCESS bills (£944.59) + 261 FAILED + 107 PENDING
operator_a Mar:   10 SUCCESS bills  (£28.40) +   5 FAILED +   3 PENDING
operator_b Feb: 1,161 REN (£3,282.39) + 115 UNSUB
operator_b Mar:    35 REN   (£100.15) +   1 UNSUB
```

A pipeline filtering `WHERE DATE_TRUNC('month', received_time) = '2026-01-01'` would silently discard **~£4,355 in real revenue** (≈29% of observable total). The mart schema resolves this by storing `report_date` per event row rather than filtering by file-load date.

---

### Pain Point 2 — Operator B SUB is not a free opt-in

Unlike Operator A where `event_code=1` always has `amount=0.00`, **Operator B charges on the first `SUB` row** — the initial subscription is simultaneously the first billing event.

```
transaction_type | rows | unique_msisdn | total_revenue | avg_amount
SUB              |  788 |           788 |     £2,235.62 |      £2.84
REN              | 2,286 |          788 |     £6,487.64 |      £2.84
UNSUB            |  199 |           199 |         £0.00 |      £0.00
```

Revenue split: **SUB = 25.6%, REN = 74.4%**. Treating `SUB` as a zero-amount activation (following Operator A's convention) drops £2,235 from reported revenue. The mart's `fct_billing` captures all `amount > 0` rows regardless of `transaction_type`.

---

### Pain Point 3 — Operator B REN attribution chain via msisdn

Operator B only populates `rotate_id` on `SUB` rows. All 2,286 `REN` rows have `rotate_id = NULL`. To link a renewal back to a campaign the pipeline must chain:

```
REN.msisdn → most recent SUB for same msisdn WHERE SUB.received_time ≤ REN.received_time
           → SUB.rotate_id → clicks.campaign_id
```

In this dataset: 0 orphan REN rows, every msisdn has exactly 1 SUB — the chain is safe here. But the design is fragile at scale: a user who unsubscribes and resubscribes creates two SUB rows. Without the `MAX(subscribed_at) ≤ billed_at` guard, a March renewal could be attributed to the January campaign the user no longer belongs to. The mart schema handles this edge case explicitly.

---

### Pain Point 4 — Operator C tracking code length > 3 characters

Users submit a 3-character code via SMS. `tracking_codes` stores codes as exactly 3 characters. 96 of 741 rows (13%) have longer codes — the JOIN breaks.

```
code_length | rows | delivered
          3 |  645 |       408   ← attributable
          4 |   50 |        31   ← attribution lost
          5 |   46 |        31   ← attribution lost
```

Of the 96 unattributable rows, **62 are `DELIVERED`** — the user was successfully subscribed and charged, but the platform cannot link this revenue to any campaign or partner. This is not ghost revenue — the money is real — but it is invisible to BI reporting.

Root cause split:
- **8 rows (8.3%):** First 3 characters match a valid code → likely user typos
- **88 rows (91.7%):** Prefix also unknown → upstream SMS parser likely appending session tokens

---

### Pain Point 5 — Valid codes arriving after the 30-minute expiry window

Even among 3-character codes, **25 rows arrived after `expired_at`** (`created_at + 30 minutes`), of which 15 were `DELIVERED`. Without an explicit `expired_at` check, these silently become `unattributed` — indistinguishable from "code not found." The fix is to classify `failure_reason = 'expired_code'` separately so monitoring can alert when expiry losses trend upward independently of code-length losses.

---

### Pain Point 6 — Bills arriving before subscribe (race condition)

82 cases where `event_code=2` (bill) arrived before `event_code=1` (subscribe) for the same `rotate_id`:

```
cases | min_sec_early | max_sec_early | avg_sec_early
   82 |             7 |           120 |            67
```

The 7–120 second range points to async pipeline processing — billing fires on a fast path, subscription confirmation travels through a heavier validation path. A pipeline ordering events strictly by `received_time` marks these 82 bills as orphans. Fix: buffer unmatched billing events for 2 minutes before failing the join — the 120-second maximum means this resolves 100% of observed cases.

---

### Pain Point 7 — msisdn stored as BIGINT

DuckDB's `read_csv_auto()` infers `msisdn` as `BIGINT`. UK mobile numbers starting with `07` lose the leading zero when stored as integer (`07911123456` → `7911123456`). Any JOIN between tables on msisdn would silently return 0 matches for those numbers.

Fix: explicit `VARCHAR NOT NULL` declaration in all raw staging DDL rather than relying on type inference.

---

### Operator-A Event Codes — What They Mean

| event_code | Meaning | Rows | Success Rate | Amount |
|-----------|---------|------|-------------|--------|
| 1 | Subscribe (opt-in activation) | 917 | 48.5% | £0.00 always |
| 2 | Bill (recurring charge) | 2,160 | 51.5% | £1.99–£3.49 |
| 3 | Unsubscribe (cancellation) | 117 | 100% | £0.00 always |

The 100% success rate on `event_code=3` is architecturally meaningful — cancellations are always acknowledged by the operator regardless of billing state. The platform never needs to retry an unsubscribe.

---

## Part 2 — Data Modeling

**Goal:** Unified daily view of subscriptions, first bills, revenue, and conversion across all 3 operators and all partners.

### From Source Schema to Mart — What Changed and Why

The source dataset has 7 tables with no shared analytics layer. The mart schema adds 3 layers on top without modifying the source structure.

#### Source Tables — Issues for Analytics

| Table | Rows | Key Issue for Analytics |
|-------|------|------------------------|
| `campaigns` | 10 | Clean — no issues |
| `clicks` | 6,000 | `partner_id` not present — requires JOIN through `campaigns` for every partner-level query |
| `tracking_codes` | 1,175 | `expired_at` not validated at SMS receipt time |
| `page_events` | 7,291 | `msisdn` 89.6% null (by design); `msisdn` type is BIGINT (leading-zero risk) |
| `operator_a` | 3,194 | `received_time` extends to Mar 2026; `msisdn` BIGINT; race condition in event ordering |
| `operator_b` | 3,273 | `rotate_id` NULL on 75.9% of rows; `SUB` amount mistakable as £0 |
| `operator_c` | 741 | 13% of `tracking_code` values unattributable; no `rotate_id` at all |

#### Schema Changes — Column Additions and Type Changes

| Change | Source | Mart | Why This Matters for BI |
|--------|--------|------|------------------------|
| `msisdn` type | `BIGINT` (auto-inferred) | `VARCHAR NOT NULL` | Preserve UK leading zeros; safe cross-table JOIN |
| Timestamp rename | `received_time TIMESTAMP` | `event_time / clicked_at / billed_at / subscribed_at TIMESTAMPTZ` | Avoid DuckDB reserved keyword; semantic clarity per table |
| Add `report_date` | Not in source | `DATE NOT NULL` (derived at transform) | `WHERE report_date = '2026-01-15'` — no timestamp casting in every query |
| Add `attribution_method` | Not in source | `VARCHAR NOT NULL` in `fct_subscriptions` | `'direct_rotate_id'` / `'tracking_code_lookup'` / `'expired_code'` / `'unattributed'` — debug metric jumps without re-running pipeline |
| Add `is_first_bill` | Not in source | `BOOLEAN NOT NULL DEFAULT FALSE` in `fct_billing` | Pre-computed via `ROW_NUMBER()` once at load — `WHERE is_first_bill = TRUE` needs no window function at query time |
| Add `billing_sequence` | Not in source | `SMALLINT NOT NULL` in `fct_billing` | Ordinal position of charge for this msisdn+service — enables churn by billing cycle (does LTV drop at cycle 3? cycle 5?) |
| Add funnel flags | Not in source | `has_page_view / has_cta_click / has_entry / has_subscription / has_first_bill BOOLEAN` in `fct_clicks` | Conversion rate = `SUM(has_subscription::INT) / COUNT(*)` — no joins, no subqueries |
| Add `_loaded_date` | Not in source | `DATE` in all `raw_*` tables | Partition key for incremental loads; idempotency anchor |
| Add surrogate keys | Operator-specific IDs (`transaction_id` / `message_id`) | `subscription_id` / `billing_id VARCHAR` in fact tables | Each operator uses different ID namespace; surrogate key enables true cross-operator unification |
| Add `source_transaction_id` | Original ID only | Surrogate key + `source_transaction_id VARCHAR` | Retains operator's original ID for audit trace back to raw CSV |
| Denormalize `operator`, `service_name`, `partner_id` | Only in `campaigns` table | Copied into every fact table | Every slice-by-partner or slice-by-service query needs zero JOINs |
| Add `failure_reason` | Not in source | `VARCHAR` in `unattributed_events` | `'invalid_code_length'` / `'expired_code'` / `'code_not_found'` — distinguishes 3 separate failure modes for operator C |
| Add `pipeline_runs` table | Not in source | New audit table | Every step logs `status`, `rows_processed`, `error_message` — full observability without external orchestrator |

---

### Full Schema Architecture

```
[SOURCE CSVs]           [RAW STAGING — typed landing zone]
                                                              [DIMENSION]
campaigns.csv    ──►   raw_campaigns  ───────────────────►   dim_campaigns
clicks.csv       ──►   raw_clicks     ───────────────────►        │
tracking_codes   ──►   raw_tracking_codes                         │         [FACT TABLES]           [MART]
page_events.csv  ──►   raw_page_events                            ├────────► fct_clicks
operator_A.csv   ──►   raw_operator_a  ──────────────────►        ├────────► fct_subscriptions  ──► mart_daily_performance
operator_B.csv   ──►   raw_operator_b  ──────────────────►        └────────► fct_billing
operator_C.csv   ──►   raw_operator_c  ──► unattributed_events
                                           (failure_reason recorded)

pipeline_runs  (audit: run_id, step, status, rows_processed, error_message, started_at, finished_at)
```

### Tables and Their Roles

| Table | Grain | Key Added Columns | Purpose |
|-------|-------|-------------------|---------|
| `raw_operator_a/b/c` | 1 row per source CSV row | `_loaded_date`, `msisdn VARCHAR` | Typed landing zone; safe to re-ingest |
| `raw_campaigns/clicks/tracking_codes/page_events` | Mirror of source | `_loaded_date`, type corrections | Immutable raw copy before any transform |
| `dim_campaigns` | 1 row per campaign | `loaded_at` | Single source of truth for campaign metadata |
| `fct_subscriptions` | 1 row per opt-in | `subscription_id`, `attribution_method`, `report_date` | All 3 operators unified; attribution method recorded |
| `fct_billing` | 1 row per charge | `billing_id`, `is_first_bill`, `billing_sequence`, `report_date` | Revenue with first-bill and churn cycle metrics pre-computed |
| `fct_clicks` | 1 row per click | All 5 funnel flags, `report_date` | Conversion funnel pre-flattened — no joins at BI query time |
| `mart_daily_performance` | 1 row per day × campaign | All metrics pre-aggregated, conversion rates stored | Dashboard layer — `SELECT * FROM mart WHERE report_date = today` |
| `unattributed_events` | 1 row per unattributable operator_c event | `failure_reason` | Not silently dropped — visible in monitoring |
| `pipeline_runs` | 1 row per step execution | `status`, `rows_processed`, `error_message` | Audit log; supports re-run from failure point |

### Attribution Logic by Operator

```
operator_a (event_code=1, status='SUCCESS'):
    rotate_id always present → direct insert to fct_subscriptions
    attribution_method = 'direct_rotate_id'

operator_b (transaction_type='SUB'):
    rotate_id present → direct insert to fct_subscriptions
    attribution_method = 'direct_rotate_id'

    REN rows: no rotate_id — resolve via msisdn chain:
    REN.msisdn → MAX(subscribed_at) WHERE subscribed_at ≤ billed_at
              → inherit subscription_id and campaign_id from that SUB row

operator_c (delivery_status='DELIVERED'):
    No rotate_id — resolve via tracking code lookup:

    CASE 1 — LENGTH(tracking_code) > 3:
        → unattributed_events, failure_reason = 'invalid_code_length'
        (96 rows, 62 DELIVERED — biggest attribution loss)

    CASE 2 — LENGTH = 3, BUT received_time > expired_at:
        → unattributed_events, failure_reason = 'expired_code'
        (25 rows, 15 DELIVERED — monitored separately from case 1)

    CASE 3 — LENGTH = 3, within expiry, match found in tracking_codes:
        rotate_id = tc.rotate_id
        attribution_method = 'tracking_code_lookup'

    CASE 4 — LENGTH = 3, within expiry, no match:
        → unattributed_events, failure_reason = 'code_not_found'
```

### Why DuckDB for This Pipeline

`read_csv_auto()` reads all 7 CSV files in one step with schema inference — no separate ingest tool required. The same SQL logic for attribution resolution, window functions, and aggregations runs on PostgreSQL in production with no dialect changes. The entire pipeline runs in a single Docker container without a separate database service, making local development and CI identical.

The one place where DuckDB's inference caused a real problem is `msisdn` being read as `BIGINT`. Corrected by explicit `VARCHAR` declarations in raw staging DDL rather than relying on auto-detection.

---

## Part 3 — ETL Pipeline

### v1 — Working Pipeline (`part3_pipeline/`)

End-to-end functional: file ingestion per operator into raw staging tables, validation, attribution resolution, and fact table population.

### v2 — Production-Ready Pipeline (`part3_pipeline_production_ready/`)

**Architecture — 4 Layers**

```
[INGEST]     CSV files ──► raw_* tables
                           Checks: schema, row count > 0, PK uniqueness,
                           date range sanity, domain values, msisdn→VARCHAR cast

[TRANSFORM]  raw_* ──► enriched intermediate
                       Attribution resolution per operator (all 3 paths above)
                       ROW_NUMBER() → is_first_bill + billing_sequence
                       EXISTS subqueries → funnel flags per rotate_id
                       Unattributable rows → unattributed_events with failure_reason
                       Race condition buffer: retry unmatched billing events with 2-min tolerance

[LOAD]       enriched ──► dim_campaigns, fct_clicks, fct_subscriptions, fct_billing
                          ON CONFLICT DO NOTHING
                          idempotency key = (operator, source_date, source_transaction_id)

[MART]       fact tables ──► mart_daily_performance
                             sub_conversion_rate = total_subscriptions / NULLIF(total_clicks, 0)
                             bill_conversion_rate = total_first_bills / NULLIF(total_clicks, 0)

Every step ──► pipeline_runs
```

**Tooling**

| Concern | Choice | Rationale |
|---------|--------|-----------|
| Orchestration | Python + `pipeline_runs` table | Self-contained audit log; re-runnable from any failed step |
| Transformation | Python + DuckDB / SQLAlchemy | CSV → SQL in one step; same logic ports to Postgres |
| Storage | DuckDB (local) / PostgreSQL (production) | Identical schema; DuckDB for iteration, Postgres for production |
| Containerisation | Docker + docker-compose | One-command run; no environment drift |
| Testing | pytest | Unit tests on attribution logic; integration tests end-to-end |
| CI | GitHub Actions | Tests on every push; blocks merge on failure |
| AWS path | S3 → Glue → Athena / Redshift | Documented in `part3_pipeline_AWS_Service/` |

**Idempotency** — `ON CONFLICT DO NOTHING` on composite key `(operator, source_date, source_transaction_id)`. Running twice is safe.

**Late-arriving files** — missing file writes `status='failed'` to `pipeline_runs` and halts. No downstream step runs against incomplete data.

**Partial failure recovery** — `pipeline_runs` records exactly which step failed. Re-running skips completed steps.

**Race condition handling** — unmatched billing events are retried once with a 2-minute timestamp tolerance. Resolves all 82 observed cases (max observed delay: 120 seconds).

---

## Part 4 — Data Validation

### At the source (before staging)

- Schema check: expected column names and count present
- Row count plausibility: file has at least N rows (empty file = silent failure)
- Primary key uniqueness within the file (duplicate file delivery)
- Date range sanity: `received_time` within expected window ±1 day
- Domain value checks: `event_code` ∈ {1,2,3}; `transaction_type` ∈ {SUB, REN, UNSUB}; `delivery_status` ∈ {DELIVERED, SMSC\_QUEUED, FAILED}
- `amount >= 0` — negative amounts indicate unhandled refund events
- `msisdn` format: castable to VARCHAR without data loss

### During transformation (staging → fact)

- `not_null` on all `NOT NULL` columns
- `unique` on surrogate keys
- Referential integrity: `campaign_id` in `dim_campaigns`, `rotate_id` in `fct_clicks`
- Custom: no msisdn has `is_first_bill = TRUE` more than once per service
- Custom: all operator\_b `REN` rows resolve to a subscription (orphan = join failure)
- Custom: `billing_sequence` is contiguous per msisdn+service (gaps = missing rows)

### In the final output (mart layer)

- Conversion rates bounded at \[0, 1\] — exceeding 1.0 indicates JOIN fan-out
- Cumulative revenue non-decreasing day-over-day
- All expected `(report_date, operator)` combinations present

### Ongoing monitoring

- Daily revenue per operator: alert if deviation from 7-day rolling average exceeds ±40%
- Subscription count: alert if drops below 50% of 7-day average
- Operator C attribution match rate: alert if drops below 80% (dataset baseline: 87%)
- Operator C expiry rate: **separate alert** — if `expired_code` failures trend up independently, it signals platform clock drift
- Silent source detection: daily audit on `pipeline_runs` for expected `(run_date, operator)` rows

**Alerting tiers:** Critical (PagerDuty) → Warning (Slack `#data-alerts`) → Info (daily summary)

---

## Part 5 — Platform Improvements

| # | Proposal | Pain Point Fixed | Complexity |
|---|----------|-----------------|-----------|
| 1 | Enforce 3-char max on tracking codes at SMS ingestion | Pain 4: 13% operator C attribution loss | Platform-side only |
| 2 | Log `failure_reason` at SMS receipt (`expired_code` vs `invalid_length`) | Pain 5: silent expiry losses undistinguishable | Platform-side only |
| 3 | Add `rotate_id` to all operator B billing rows | Pain 3: REN attribution via fragile msisdn chain | Requires operator B coordination |
| 4 | Denormalize `partner_id` onto `clicks` at insert time | Every partner query requires JOIN through `campaigns` | Source schema addition |
| 5 | Add `funnel_session_id` to `page_events` | Multiple visits per rotate\_id are ambiguous | Frontend change |
| 6 | Capture `msisdn` on `CLICK_CTA` where already available | Cannot distinguish re-engagement vs new acquisition | GDPR review needed |

**Highest ROI:** Proposal 1 — no operator coordination, entirely platform-side, immediately recovers attribution for 62 delivered events (13.2% of all operator C deliveries).

---

## How to Run Locally

```bash
git clone https://github.com/BaoBao1408/Data-Engineer-Projects.git
cd AdStart_Media_Test_NguyenQuocBao/part3_pipeline_production_ready

cp .env.example .env
docker-compose up --build

make run    # full pipeline
make test   # run test suite
```

---

## A Note on Approach

This test was treated as a real engineering problem, not an academic exercise.

The Part 1 SQL queries were written to expose business logic behind the numbers: why are 75.9% of rotate\_ids null in operator B? What does a 100% unsubscribe success rate mean operationally? Is operator B's `SUB` amount actually zero the way operator A's is? It is not — and that distinction accounts for 25.6% of observable revenue.

The schema in Part 2 was designed with the assumption it will outlive the test. The `attribution_method` audit column, the `unattributed_events` staging table with `failure_reason`, the `billing_sequence` ordinal, and the explicit `msisdn VARCHAR` type declaration all exist because real pipelines break in ways that only become visible months later — and by then, the original engineer is no longer around to explain why it was built the way it was.

The production pipeline was rebuilt from scratch after the initial version worked — not because it was required, but because the gap between "runs once" and "runs reliably every day without someone checking it manually" is where the actual engineering lives.

---

*Nguyen Quoc Bao — May 2026*
*GitHub: https://github.com/BaoBao1408/Data-Engineer-Projects/tree/main/AdStart_Media_Test_NguyenQuocBao*