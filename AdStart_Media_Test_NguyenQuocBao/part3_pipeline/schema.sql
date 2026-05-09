-- schema.sql
-- All DDL is idempotent: safe to run multiple times.
-- DuckDB syntax. PostgreSQL equivalent is identical except:
--   gen_random_uuid() → gen_random_uuid() (same, needs pgcrypto)
--   TIMESTAMPTZ       → TIMESTAMPTZ (same)
--   SMALLINT          → SMALLINT (same)

-- ─────────────────────────────────────────────
-- PIPELINE RUN TRACKING (audit log)
-- AWS equivalent: CloudWatch Logs + DynamoDB run state table
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id          VARCHAR PRIMARY KEY,
    run_date        DATE        NOT NULL,
    step            VARCHAR     NOT NULL,
    status          VARCHAR     NOT NULL,  -- 'running' | 'success' | 'failed'
    rows_processed  INTEGER,
    error_message   VARCHAR,
    started_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at     TIMESTAMPTZ
);

-- ─────────────────────────────────────────────
-- RAW STAGING TABLES (typed landing zone)
-- AWS equivalent: S3 raw prefix → Glue Catalog tables
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS raw_operator_a (
    transaction_id  VARCHAR,
    rotate_id       VARCHAR,
    msisdn          VARCHAR,
    event_code      INTEGER,
    status          VARCHAR,
    amount          DOUBLE,
    timestamp       TIMESTAMPTZ,
    _loaded_date    DATE
);

CREATE TABLE IF NOT EXISTS raw_operator_b (
    transaction_id    VARCHAR,
    rotate_id         VARCHAR,
    msisdn            VARCHAR,
    transaction_type  VARCHAR,
    amount            DOUBLE,
    created_at        TIMESTAMPTZ,
    _loaded_date      DATE
);

CREATE TABLE IF NOT EXISTS raw_operator_c (
    message_id       VARCHAR,
    tracking_code    VARCHAR,
    msisdn           VARCHAR,
    delivery_status  VARCHAR,
    received_time    TIMESTAMPTZ,
    _loaded_date     DATE
);

CREATE TABLE IF NOT EXISTS raw_campaigns (
    id            VARCHAR,
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
    clicked_at   TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS raw_tracking_codes (
    code        VARCHAR,
    rotate_id   VARCHAR,
    created_at  TIMESTAMPTZ,
    expired_at  TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS raw_page_events (
    rotate_id   VARCHAR,
    event_type  VARCHAR,
    created_at  TIMESTAMPTZ
);

-- ─────────────────────────────────────────────
-- DIMENSION
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dim_campaigns (
    campaign_id    VARCHAR PRIMARY KEY,
    operator       VARCHAR NOT NULL,
    service_name   VARCHAR NOT NULL,
    service_model  VARCHAR NOT NULL,
    partner_id     VARCHAR NOT NULL,
    status         VARCHAR NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL,
    loaded_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ─────────────────────────────────────────────
-- FACT TABLES
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS fct_subscriptions (
    subscription_id       VARCHAR PRIMARY KEY,
    operator              VARCHAR NOT NULL,
    source_transaction_id VARCHAR NOT NULL,
    rotate_id             VARCHAR,
    campaign_id           VARCHAR NOT NULL REFERENCES dim_campaigns(campaign_id),
    service_name          VARCHAR NOT NULL,
    partner_id            VARCHAR NOT NULL,
    msisdn                VARCHAR NOT NULL,
    subscribed_at         TIMESTAMPTZ NOT NULL,
    report_date           DATE NOT NULL,
    attribution_method    VARCHAR NOT NULL,
    loaded_at             TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS fct_billing (
    billing_id            VARCHAR PRIMARY KEY,
    operator              VARCHAR NOT NULL,
    source_transaction_id VARCHAR NOT NULL,
    subscription_id       VARCHAR REFERENCES fct_subscriptions(subscription_id),
    campaign_id           VARCHAR NOT NULL REFERENCES dim_campaigns(campaign_id),
    service_name          VARCHAR NOT NULL,
    partner_id            VARCHAR NOT NULL,
    msisdn                VARCHAR NOT NULL,
    amount                DECIMAL(10,2) NOT NULL,
    currency              VARCHAR NOT NULL DEFAULT 'GBP',
    billed_at             TIMESTAMPTZ NOT NULL,
    report_date           DATE NOT NULL,
    is_first_bill         BOOLEAN NOT NULL DEFAULT FALSE,
    billing_sequence      SMALLINT NOT NULL DEFAULT 1,
    billing_status        VARCHAR NOT NULL,
    loaded_at             TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS fct_clicks (
    rotate_id        VARCHAR PRIMARY KEY,
    campaign_id      VARCHAR NOT NULL REFERENCES dim_campaigns(campaign_id),
    service_name     VARCHAR NOT NULL,
    operator         VARCHAR NOT NULL,
    partner_id       VARCHAR NOT NULL,
    pub_id           VARCHAR,
    clicked_at       TIMESTAMPTZ NOT NULL,
    report_date      DATE NOT NULL,
    has_page_view    BOOLEAN NOT NULL DEFAULT FALSE,
    has_cta_click    BOOLEAN NOT NULL DEFAULT FALSE,
    has_entry        BOOLEAN NOT NULL DEFAULT FALSE,
    has_subscription BOOLEAN NOT NULL DEFAULT FALSE,
    has_first_bill   BOOLEAN NOT NULL DEFAULT FALSE,
    loaded_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ─────────────────────────────────────────────
-- MART (pre-aggregated, refreshed daily)
-- AWS equivalent: materialized view in Redshift, or dbt model on Athena
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS mart_daily_performance (
    report_date           DATE    NOT NULL,
    campaign_id           VARCHAR NOT NULL REFERENCES dim_campaigns(campaign_id),
    operator              VARCHAR NOT NULL,
    service_name          VARCHAR NOT NULL,
    partner_id            VARCHAR NOT NULL,
    total_clicks          INTEGER NOT NULL DEFAULT 0,
    total_page_views      INTEGER NOT NULL DEFAULT 0,
    total_cta_clicks      INTEGER NOT NULL DEFAULT 0,
    total_entries         INTEGER NOT NULL DEFAULT 0,
    total_subscriptions   INTEGER NOT NULL DEFAULT 0,
    total_first_bills     INTEGER NOT NULL DEFAULT 0,
    total_renewals        INTEGER NOT NULL DEFAULT 0,
    total_revenue         DECIMAL(12,4) NOT NULL DEFAULT 0,
    currency              VARCHAR NOT NULL DEFAULT 'GBP',
    sub_conversion_rate   DECIMAL(8,6),
    bill_conversion_rate  DECIMAL(8,6),
    loaded_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (report_date, campaign_id)
);
