-- schema.sql — All DDL is idempotent: safe to run multiple times.
-- DuckDB syntax. PostgreSQL equivalent is near-identical.

-- ─────────────────────────────────────────────
-- PIPELINE RUN TRACKING (audit log)
-- AWS: CloudWatch Logs + DynamoDB run state table
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id          VARCHAR     PRIMARY KEY,
    run_date        DATE        NOT NULL,
    step            VARCHAR     NOT NULL,
    status          VARCHAR     NOT NULL,   -- 'running' | 'success' | 'failed'
    rows_processed  INTEGER,
    error_message   VARCHAR,
    started_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at     TIMESTAMPTZ
);

-- ─────────────────────────────────────────────
-- RAW STAGING TABLES (typed landing zone)
-- Column names align with source CSV headers.
-- NOTE: operator_a source column is "received_time"; staged as "event_time"
--       to avoid DuckDB reserved-keyword collision with "timestamp".
-- AWS: S3 raw prefix crawled by Glue → Glue Catalog tables
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS raw_operator_a (
    transaction_id  VARCHAR,
    rotate_id       VARCHAR,
    msisdn          VARCHAR,
    event_code      INTEGER,       -- 1=subscribe, 2=bill, 3=unsubscribe
    status          VARCHAR,       -- SUCCESS | FAILED | PENDING
    amount          DOUBLE,
    currency        VARCHAR,
    event_time      TIMESTAMPTZ,   -- source column: received_time
    _loaded_date    DATE
);

CREATE TABLE IF NOT EXISTS raw_operator_b (
    transaction_id    VARCHAR,
    rotate_id         VARCHAR,     -- NULL for REN/UNSUB rows (by design)
    msisdn            VARCHAR,
    transaction_type  VARCHAR,     -- SUB | REN | UNSUB
    amount            DOUBLE,
    currency          VARCHAR,
    created_at        TIMESTAMPTZ, -- source column: received_time
    _loaded_date      DATE
);

CREATE TABLE IF NOT EXISTS raw_operator_c (
    message_id       VARCHAR,
    tracking_code    VARCHAR,
    msisdn           VARCHAR,
    delivery_status  VARCHAR,      -- DELIVERED | SMSC_QUEUED | FAILED
    service_id       VARCHAR,
    received_time    TIMESTAMPTZ,
    _loaded_date     DATE
);

CREATE TABLE IF NOT EXISTS raw_campaigns (
    id            VARCHAR,
    country       VARCHAR,
    operator      VARCHAR,
    service_name  VARCHAR,
    service_model VARCHAR,
    partner_id    VARCHAR,
    status        VARCHAR,
    created_at    TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS raw_clicks (
    rotate_id    VARCHAR,
    campaign_id  VARCHAR,
    pub_id       VARCHAR,
    clicked_at   TIMESTAMPTZ  -- source column: received_time
);

CREATE TABLE IF NOT EXISTS raw_tracking_codes (
    rotate_id   VARCHAR,
    code        VARCHAR,
    service_id  VARCHAR,
    created_at  TIMESTAMPTZ,
    expired_at  TIMESTAMPTZ   -- = created_at + 30 min
);

CREATE TABLE IF NOT EXISTS raw_page_events (
    event_id    VARCHAR,
    rotate_id   VARCHAR,
    campaign_id VARCHAR,
    event_type  VARCHAR,       -- VIEW | CLICK_CTA | ENTRY
    msisdn      VARCHAR,       -- NULL unless event_type = 'ENTRY'
    device_type VARCHAR,
    created_at  TIMESTAMPTZ   -- source column: received_time
);

-- ─────────────────────────────────────────────
-- DIMENSION
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dim_campaigns (
    campaign_id    VARCHAR     PRIMARY KEY,
    operator       VARCHAR     NOT NULL,
    service_name   VARCHAR     NOT NULL,
    service_model  VARCHAR     NOT NULL,   -- subscription | one-off
    partner_id     VARCHAR     NOT NULL,
    status         VARCHAR     NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL,
    loaded_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ─────────────────────────────────────────────
-- FACT TABLES
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS fct_subscriptions (
    subscription_id       VARCHAR     PRIMARY KEY,
    operator              VARCHAR     NOT NULL,
    source_transaction_id VARCHAR     NOT NULL,
    rotate_id             VARCHAR,
    campaign_id           VARCHAR     NOT NULL REFERENCES dim_campaigns(campaign_id),
    service_name          VARCHAR     NOT NULL,
    partner_id            VARCHAR     NOT NULL,
    msisdn                VARCHAR     NOT NULL,
    subscribed_at         TIMESTAMPTZ NOT NULL,
    report_date           DATE        NOT NULL,
    attribution_method    VARCHAR     NOT NULL, -- direct_rotate_id | tracking_code_lookup | unattributed
    loaded_at             TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS fct_billing (
    billing_id            VARCHAR      PRIMARY KEY,
    operator              VARCHAR      NOT NULL,
    source_transaction_id VARCHAR      NOT NULL,
    subscription_id       VARCHAR      REFERENCES fct_subscriptions(subscription_id),
    campaign_id           VARCHAR      NOT NULL REFERENCES dim_campaigns(campaign_id),
    service_name          VARCHAR      NOT NULL,
    partner_id            VARCHAR      NOT NULL,
    msisdn                VARCHAR      NOT NULL,
    amount                DECIMAL(10,2) NOT NULL,
    currency              VARCHAR      NOT NULL DEFAULT 'GBP',
    billed_at             TIMESTAMPTZ  NOT NULL,
    report_date           DATE         NOT NULL,
    is_first_bill         BOOLEAN      NOT NULL DEFAULT FALSE,
    billing_sequence      SMALLINT     NOT NULL DEFAULT 1,
    billing_status        VARCHAR      NOT NULL,
    loaded_at             TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS fct_clicks (
    rotate_id        VARCHAR     PRIMARY KEY,
    campaign_id      VARCHAR     NOT NULL REFERENCES dim_campaigns(campaign_id),
    service_name     VARCHAR     NOT NULL,
    operator         VARCHAR     NOT NULL,
    partner_id       VARCHAR     NOT NULL,
    pub_id           VARCHAR,
    clicked_at       TIMESTAMPTZ NOT NULL,
    report_date      DATE        NOT NULL,
    has_page_view    BOOLEAN     NOT NULL DEFAULT FALSE,
    has_cta_click    BOOLEAN     NOT NULL DEFAULT FALSE,
    has_entry        BOOLEAN     NOT NULL DEFAULT FALSE,
    has_subscription BOOLEAN     NOT NULL DEFAULT FALSE,
    has_first_bill   BOOLEAN     NOT NULL DEFAULT FALSE,
    loaded_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ─────────────────────────────────────────────
-- MART (pre-aggregated, refreshed daily)
-- AWS: dbt model on Athena, or Redshift materialized view
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS mart_daily_performance (
    report_date           DATE          NOT NULL,
    campaign_id           VARCHAR       NOT NULL REFERENCES dim_campaigns(campaign_id),
    operator              VARCHAR       NOT NULL,
    service_name          VARCHAR       NOT NULL,
    partner_id            VARCHAR       NOT NULL,
    total_clicks          INTEGER       NOT NULL DEFAULT 0,
    total_page_views      INTEGER       NOT NULL DEFAULT 0,
    total_cta_clicks      INTEGER       NOT NULL DEFAULT 0,
    total_entries         INTEGER       NOT NULL DEFAULT 0,
    total_subscriptions   INTEGER       NOT NULL DEFAULT 0,
    total_first_bills     INTEGER       NOT NULL DEFAULT 0,
    total_renewals        INTEGER       NOT NULL DEFAULT 0,
    total_revenue         DECIMAL(12,4) NOT NULL DEFAULT 0,
    currency              VARCHAR       NOT NULL DEFAULT 'GBP',
    sub_conversion_rate   DECIMAL(8,6),
    bill_conversion_rate  DECIMAL(8,6),
    loaded_at             TIMESTAMPTZ   NOT NULL DEFAULT now(),
    PRIMARY KEY (report_date, campaign_id)
);