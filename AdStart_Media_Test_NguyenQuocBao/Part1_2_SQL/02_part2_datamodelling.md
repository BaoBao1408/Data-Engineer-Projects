

## Part 2 — Data Modeling

### Goal

A unified daily reporting layer that tracks subscriptions, first charges, revenue, and conversion across all three operators, sliced by operator, service, and partner.

---

### Proposed schema

#### Layer 1 — Staging (raw copies, minimal transformation)

These tables are 1:1 snapshots of each source file, loaded daily with a `loaded_date` partition. They change only by appending new partitions. No business logic is applied here.

```sql
stg_operator_a (
  transaction_id      UUID,
  rotate_id           UUID,
  msisdn              VARCHAR,
  received_time       TIMESTAMP,
  event_code          INT,           -- 1=subscribe, 2=bill, 3=unsubscribe
  status              VARCHAR,
  amount              NUMERIC(10,2),
  currency            VARCHAR(3),
  loaded_date         DATE           -- partition key, added by pipeline
)

stg_operator_b (
  transaction_id      UUID,
  rotate_id           UUID,          -- NULL except on SUB rows
  msisdn              VARCHAR,
  received_time       TIMESTAMP,
  transaction_type    VARCHAR,       -- SUB, REN, UNSUB
  package_id          VARCHAR,
  amount              NUMERIC(10,2),
  currency            VARCHAR(3),
  loaded_date         DATE
)

stg_operator_c (
  message_id          UUID,
  msisdn              VARCHAR,
  received_time       TIMESTAMP,
  tracking_code       VARCHAR,
  service_id          VARCHAR,
  delivery_status     VARCHAR,       -- DELIVERED, SMSC_QUEUED, FAILED
  loaded_date         DATE
)
```

#### Layer 2 — Enriched event tables (one per operator)

These tables normalise each operator's events into a common shape and resolve the attribution chain back to rotate_id, campaign, service, and partner.

```sql
-- Operator A events, normalised
enr_operator_a_events (
  transaction_id      UUID          NOT NULL,  -- source PK
  rotate_id           UUID          NOT NULL,  -- direct FK to dim_clicks
  msisdn              VARCHAR       NOT NULL,
  received_time       TIMESTAMP     NOT NULL,
  event_type          VARCHAR       NOT NULL,  -- SUBSCRIBE / BILL / UNSUBSCRIBE
  status              VARCHAR       NOT NULL,  -- SUCCESS / FAILED / PENDING
  amount              NUMERIC(10,2) NOT NULL,
  currency            VARCHAR(3)    NOT NULL,
  -- resolved attribution
  campaign_id         UUID,                    -- via clicks
  service_name        VARCHAR,                 -- via campaigns
  operator            VARCHAR       DEFAULT 'operator_a',
  partner_id          UUID,                    -- via campaigns
  event_date          DATE          NOT NULL   -- derived from received_time
)

-- Operator B events, normalised
enr_operator_b_events (
  transaction_id      UUID          NOT NULL,
  msisdn              VARCHAR       NOT NULL,
  received_time       TIMESTAMP     NOT NULL,
  event_type          VARCHAR       NOT NULL,  -- SUBSCRIBE / BILL / UNSUBSCRIBE
  amount              NUMERIC(10,2) NOT NULL,
  currency            VARCHAR(3)    NOT NULL,
  package_id          VARCHAR,
  -- attribution: resolved only for SUBSCRIBE rows via rotate_id
  -- for REN/UNSUB rows, resolved by joining to the msisdn's first SUB
  rotate_id           UUID,                    -- NULL for REN/UNSUB
  campaign_id         UUID,
  service_name        VARCHAR,
  operator            VARCHAR       DEFAULT 'operator_b',
  partner_id          UUID,
  event_date          DATE          NOT NULL
)

-- Operator C events, normalised
enr_operator_c_events (
  message_id          UUID          NOT NULL,
  msisdn              VARCHAR       NOT NULL,
  received_time       TIMESTAMP     NOT NULL,
  tracking_code_raw   VARCHAR,                 -- original code from SMS
  tracking_code_clean VARCHAR,                 -- normalised (trim, upper)
  delivery_status     VARCHAR       NOT NULL,  -- DELIVERED / SMSC_QUEUED / FAILED
  service_id          VARCHAR,
  -- attribution: resolved via tracking_codes.code → clicks
  rotate_id           UUID,                    -- NULL if code not found / length mismatch
  campaign_id         UUID,
  service_name        VARCHAR,
  operator            VARCHAR       DEFAULT 'operator_c',
  partner_id          UUID,
  attribution_status  VARCHAR,                 -- MATCHED / UNMATCHED / EXPIRED
  event_date          DATE          NOT NULL
)
```

#### Layer 3 — Fact tables (grain: one row per meaningful business event)

```sql
-- One row per subscription activation (first subscription per msisdn per service)
fact_subscriptions (
  sub_id              UUID          NOT NULL  PRIMARY KEY,  -- surrogate key
  msisdn              VARCHAR       NOT NULL,
  operator            VARCHAR       NOT NULL,  -- operator_a / operator_b / operator_c
  service_name        VARCHAR       NOT NULL,
  partner_id          UUID,
  campaign_id         UUID,
  rotate_id           UUID,                    -- NULL for operator B REN/UNSUB chain
  sub_date            DATE          NOT NULL,
  sub_timestamp       TIMESTAMP     NOT NULL,
  status              VARCHAR       NOT NULL,  -- SUCCESS / FAILED / PENDING
  is_first_sub        BOOLEAN       NOT NULL,  -- TRUE if first sub for this msisdn+service
  source_transaction_id VARCHAR     NOT NULL   -- traceability back to staging
)

-- One row per billing attempt
fact_billing_events (
  billing_id          UUID          NOT NULL  PRIMARY KEY,
  msisdn              VARCHAR       NOT NULL,
  operator            VARCHAR       NOT NULL,
  service_name        VARCHAR       NOT NULL,
  partner_id          UUID,
  campaign_id         UUID,
  rotate_id           UUID,
  billing_date        DATE          NOT NULL,
  billing_timestamp   TIMESTAMP     NOT NULL,
  amount              NUMERIC(10,2) NOT NULL,
  currency            VARCHAR(3)    NOT NULL,
  status              VARCHAR       NOT NULL,  -- SUCCESS / FAILED / PENDING
  is_first_bill       BOOLEAN       NOT NULL,  -- TRUE if first successful charge
  source_transaction_id VARCHAR     NOT NULL
)

-- Clicks fact — entry point for conversion funnel
fact_clicks (
  rotate_id           UUID          NOT NULL  PRIMARY KEY,
  campaign_id         UUID          NOT NULL,
  operator            VARCHAR       NOT NULL,
  service_name        VARCHAR       NOT NULL,
  partner_id          UUID,
  pub_id              VARCHAR,
  click_date          DATE          NOT NULL,
  click_timestamp     TIMESTAMP     NOT NULL,
  -- funnel flags, populated by pipeline after joining events
  had_view            BOOLEAN       DEFAULT FALSE,
  had_cta_click       BOOLEAN       DEFAULT FALSE,
  had_entry           BOOLEAN       DEFAULT FALSE,
  reached_subscribe   BOOLEAN       DEFAULT FALSE,  -- any operator sub event linked
  reached_first_bill  BOOLEAN       DEFAULT FALSE   -- any successful first bill linked
)
```

#### Layer 4 — Aggregated daily mart (final query-facing layer)

```sql
-- Daily summary, one row per (date, operator, service, partner)
mart_daily_metrics (
  metric_date         DATE          NOT NULL,
  operator            VARCHAR       NOT NULL,
  service_name        VARCHAR       NOT NULL,
  partner_id          UUID,                    -- NULL = unknown/unattributed
  -- volume
  total_clicks        INT           NOT NULL  DEFAULT 0,
  total_views         INT           NOT NULL  DEFAULT 0,
  total_cta_clicks    INT           NOT NULL  DEFAULT 0,
  total_entries       INT           NOT NULL  DEFAULT 0,
  -- subscriptions
  total_subscriptions INT           NOT NULL  DEFAULT 0,  -- any status
  successful_subs     INT           NOT NULL  DEFAULT 0,  -- status = SUCCESS
  -- billing
  first_bills         INT           NOT NULL  DEFAULT 0,  -- is_first_bill = TRUE, SUCCESS
  total_bills         INT           NOT NULL  DEFAULT 0,
  successful_bills    INT           NOT NULL  DEFAULT 0,
  -- revenue
  revenue_gbp         NUMERIC(12,2) NOT NULL  DEFAULT 0,  -- successful bills only
  -- conversion
  click_to_sub_rate   NUMERIC(6,4),                       -- successful_subs / total_clicks
  click_to_bill_rate  NUMERIC(6,4),                       -- first_bills / total_clicks
  PRIMARY KEY (metric_date, operator, service_name, partner_id)
)
```

---

### How the tables relate

```
dim_clicks ─────── fact_subscriptions
      │                    │
      │              fact_billing_events
      │
dim_campaigns ─── (operator, service_name, partner_id lookups)

staging tables → enr_* tables → fact_* tables → mart_daily_metrics
```

The `rotate_id` is the spine that connects clicks to operator activity. Operator A always provides it. Operator B provides it only on SUB rows; for REN rows the pipeline resolves attribution via `msisdn → first SUB → rotate_id`. Operator C resolves it via `tracking_code → tracking_codes.code → rotate_id`.

---

### Trade-offs

**What this design makes easy:**
- Daily metrics by any slice (operator, service, partner) are pre-aggregated and fast to query.
- Funnel analysis (click → view → CTA → entry → subscribe → bill) is possible via `fact_clicks`.
- Each layer can be rebuilt independently; staging is never modified.
- Operator-specific nuances are absorbed in the enrichment layer; the mart is uniform.

**What it would struggle with:**
- **Multi-touch attribution**: A user who clicks twice gets two rotate_ids. The design links each billing event to one originating click. If requirements change to split credit across clicks, the model needs significant extension.
- **Operator B's attribution gap**: REN rows are attributed to the first SUB's campaign. If a user migrates services or a partner changes mid-subscription, the attribution becomes stale.
- **Operator C unmatched codes**: ~13% of operator C events have codes that don't match `tracking_codes.code`. These end up with `attribution_status = UNMATCHED` and `rotate_id = NULL`. They appear in the mart under `partner_id = NULL`, which can inflate the "unattributed" bucket.
- **Schema changes at source**: If an operator adds a new event type or renames a field, the enrichment SQL breaks. Adding a schema validation step at ingestion (see Part 4) is the mitigation.
- **Late-arriving data**: If an operator resends a correction days later, inserting into the fact tables could create duplicates. The pipeline needs an upsert strategy on `source_transaction_id`.

---