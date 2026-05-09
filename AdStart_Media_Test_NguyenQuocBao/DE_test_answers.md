# DE Test — Full Answer

**Dataset**: Mobile advertising platform, UK, January 2026  
**Engine used**: Python / pandas

Context dataset

campaigns.csv           10 rows
clicks.csv           6,000 rows
tracking_codes.csv   1,175 rows
page_events.csv      7,291 rows
operator_A.csv       3,194 rows
operator_B.csv       3,273 rows
operator_C.csv         741 rows

Period: January 2026 | Country: GB | Currency: GBP

0.1 DDL — Create table PostgreSQL

DROP TABLE IF EXISTS campaigns CASCADE;
CREATE TABLE campaigns (
    id          UUID        PRIMARY KEY,
    country     CHAR(2)     NOT NULL DEFAULT 'GB',
    operator    TEXT        NOT NULL CHECK (operator IN ('operator_A', 'operator_B', 'operator_C')),
    service_name TEXT       NOT NULL,
    service_model TEXT      NOT NULL CHECK (service_model IN ('one-off', 'subscription')),
    partner_id  UUID        NOT NULL,
    status      TEXT        NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL
);

-- =========================
-- clicks
-- =========================
DROP TABLE IF EXISTS clicks CASCADE;
CREATE TABLE clicks (
    rotate_id       UUID        PRIMARY KEY,
    campaign_id     UUID        NOT NULL REFERENCES campaigns(id),
    received_time   TIMESTAMPTZ NOT NULL,
    pub_id          TEXT
);

-- =========================
-- tracking_codes
-- =========================
DROP TABLE IF EXISTS tracking_codes CASCADE;
CREATE TABLE tracking_codes (
    rotate_id   UUID        NOT NULL REFERENCES clicks(rotate_id),
    code        CHAR(3)     NOT NULL,
    service_id  TEXT        NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL,
    expired_at  TIMESTAMPTZ NOT NULL GENERATED ALWAYS AS (created_at + INTERVAL '30 minutes') STORED,
    PRIMARY KEY (rotate_id, code)
);

CREATE INDEX idx_tracking_codes_code ON tracking_codes(code);

-- =========================
-- page_events
-- =========================
DROP TABLE IF EXISTS page_events CASCADE;
CREATE TABLE page_events (
    event_id        UUID        PRIMARY KEY,
    rotate_id       UUID        NOT NULL REFERENCES clicks(rotate_id),
    campaign_id     UUID        NOT NULL REFERENCES campaigns(id),
    event_type      TEXT        NOT NULL CHECK (event_type IN ('VIEW', 'CLICK_CTA', 'ENTRY')),
    received_time   TIMESTAMPTZ NOT NULL,
    msisdn          TEXT,       -- NULL khi event_type != 'ENTRY'
    device_type     TEXT        CHECK (device_type IN ('mobile', 'desktop', 'tablet'))
);

CREATE INDEX idx_page_events_rotate_id ON page_events(rotate_id);
CREATE INDEX idx_page_events_campaign_id ON page_events(campaign_id);

-- =========================
-- operator_a
-- =========================
DROP TABLE IF EXISTS operator_a CASCADE;
CREATE TABLE operator_a (
    transaction_id  UUID        PRIMARY KEY,
    rotate_id       UUID        NOT NULL REFERENCES clicks(rotate_id),
    msisdn          TEXT        NOT NULL,
    received_time   TIMESTAMPTZ NOT NULL,
    event_code      SMALLINT    NOT NULL CHECK (event_code IN (1, 2, 3)),
    -- 1 = subscribe, 2 = bill, 3 = unsubscribe
    status          TEXT        NOT NULL CHECK (status IN ('SUCCESS', 'FAILED', 'PENDING')),
    amount          NUMERIC(10,2) NOT NULL DEFAULT 0,
    currency        CHAR(3)     NOT NULL DEFAULT 'GBP'
);

CREATE INDEX idx_operator_a_rotate_id ON operator_a(rotate_id);
CREATE INDEX idx_operator_a_msisdn    ON operator_a(msisdn);

-- =========================
-- operator_b
-- =========================
DROP TABLE IF EXISTS operator_b CASCADE;
CREATE TABLE operator_b (
    transaction_id      UUID        PRIMARY KEY,
    rotate_id           UUID        REFERENCES clicks(rotate_id),
    -- NULL cho REN và UNSUB rows; chỉ có ở SUB rows
    msisdn              TEXT        NOT NULL,
    received_time       TIMESTAMPTZ NOT NULL,
    transaction_type    TEXT        NOT NULL CHECK (transaction_type IN ('SUB', 'REN', 'UNSUB')),
    -- SUB = opt-in (no charge), REN = weekly charge, UNSUB = cancelled
    package_id          TEXT,
    amount              NUMERIC(10,2) NOT NULL DEFAULT 0,
    currency            CHAR(3)     NOT NULL DEFAULT 'GBP'
);

CREATE INDEX idx_operator_b_rotate_id ON operator_b(rotate_id) WHERE rotate_id IS NOT NULL;
CREATE INDEX idx_operator_b_msisdn    ON operator_b(msisdn);

-- =========================
-- operator_c
-- =========================
DROP TABLE IF EXISTS operator_c CASCADE;
CREATE TABLE operator_c (
    message_id      UUID        PRIMARY KEY,
    msisdn          TEXT        NOT NULL,
    received_time   TIMESTAMPTZ NOT NULL,
    tracking_code   TEXT        NOT NULL,
    -- join to tracking_codes.code; user-submitted qua SMS nên có thể sai
    service_id      TEXT        NOT NULL,
    delivery_status TEXT        NOT NULL CHECK (delivery_status IN ('DELIVERED', 'SMSC_QUEUED', 'FAILED'))
    -- DELIVERED = subscription + charge xảy ra cùng lúc
);

CREATE INDEX idx_operator_c_tracking_code ON operator_c(tracking_code);
CREATE INDEX idx_operator_c_msisdn        ON operator_c(msisdn);

0.2 Load data from CSV into PostgreSQL
Step (FK dependency)

1. campaigns          ← PK ID
2. clicks             ← FK → campaigns
3. tracking_codes     ← FK → clicks
4. page_events        ← FK → clicks, campaigns
5. operator_a         ← FK → clicks
6. operator_b         ← FK → clicks (nullable)
7. operator_c         ← non fk directly

Double check data inserted into table
SELECT 'campaigns'    , COUNT(*) FROM campaigns
UNION ALL SELECT 'clicks'        , COUNT(*) FROM clicks
UNION ALL SELECT 'tracking_codes', COUNT(*) FROM tracking_codes
UNION ALL SELECT 'page_events'   , COUNT(*) FROM page_events
UNION ALL SELECT 'operator_a'    , COUNT(*) FROM operator_a
UNION ALL SELECT 'operator_b'    , COUNT(*) FROM operator_b
UNION ALL SELECT 'operator_c'    , COUNT(*) FROM operator_c;

![alt text](image-4.png)

** \COPY trong psql **

\COPY campaigns(id,country,operator,service_name,service_model,partner_id,status,created_at)
FROM '/path/to/campaigns.csv' WITH (FORMAT csv, HEADER true, NULL '');

\COPY clicks(rotate_id,campaign_id,received_time,pub_id)
FROM '/path/to/clicks.csv' WITH (FORMAT csv, HEADER true, NULL '');

\COPY tracking_codes(rotate_id,code,service_id,created_at,expired_at)
FROM '/path/to/tracking_codes.csv' WITH (FORMAT csv, HEADER true, NULL '');

\COPY page_events(event_id,rotate_id,campaign_id,event_type,received_time,msisdn,device_type)
FROM '/path/to/page_events.csv' WITH (FORMAT csv, HEADER true, NULL '');

\COPY operator_a(transaction_id,rotate_id,msisdn,received_time,event_code,status,amount,currency)
FROM '/path/to/operator_A.csv' WITH (FORMAT csv, HEADER true, NULL '');

\COPY operator_b(transaction_id,rotate_id,msisdn,received_time,transaction_type,package_id,amount,currency)
FROM '/path/to/operator_B.csv' WITH (FORMAT csv, HEADER true, NULL '');

\COPY operator_c(message_id,msisdn,received_time,tracking_code,service_id,delivery_status)
FROM '/path/to/operator_C.csv' WITH (FORMAT csv, HEADER true, NULL '');

---

## Part 1 — Data Exploration

### 1.1 — Row counts and null/empty rates
<!-- sql -->
WITH null_summary AS (
    -- campaigns
    SELECT 'campaigns' AS tbl, 'id'           AS col, COUNT(*) AS total, SUM(CASE WHEN id IS NULL OR TRIM(id::TEXT) = '' THEN 1 ELSE 0 END) AS nulls FROM campaigns
	UNION ALL SELECT 'campaigns','country',     COUNT(*), SUM(CASE WHEN country      IS NULL OR TRIM(country)=''      THEN 1 ELSE 0 END) FROM campaigns
    UNION ALL SELECT 'campaigns','operator',    COUNT(*), SUM(CASE WHEN operator     IS NULL OR TRIM(operator)=''     THEN 1 ELSE 0 END) FROM campaigns
    UNION ALL SELECT 'campaigns','service_name',COUNT(*), SUM(CASE WHEN service_name IS NULL OR TRIM(service_name)='' THEN 1 ELSE 0 END) FROM campaigns
    UNION ALL SELECT 'campaigns','service_model',COUNT(*),SUM(CASE WHEN service_model IS NULL OR TRIM(service_model)='' THEN 1 ELSE 0 END) FROM campaigns
    UNION ALL SELECT 'campaigns','partner_id',  COUNT(*), SUM(CASE WHEN partner_id   IS NULL THEN 1 ELSE 0 END) FROM campaigns
    UNION ALL SELECT 'campaigns','status',      COUNT(*), SUM(CASE WHEN status       IS NULL OR TRIM(status)=''       THEN 1 ELSE 0 END) FROM campaigns
    UNION ALL SELECT 'campaigns','created_at',  COUNT(*), SUM(CASE WHEN created_at   IS NULL THEN 1 ELSE 0 END) FROM campaigns
    -- clicks
    UNION ALL SELECT 'clicks','rotate_id',    COUNT(*), SUM(CASE WHEN rotate_id     IS NULL THEN 1 ELSE 0 END) FROM clicks
    UNION ALL SELECT 'clicks','campaign_id',  COUNT(*), SUM(CASE WHEN campaign_id   IS NULL THEN 1 ELSE 0 END) FROM clicks
    UNION ALL SELECT 'clicks','received_time',COUNT(*), SUM(CASE WHEN received_time IS NULL THEN 1 ELSE 0 END) FROM clicks
    UNION ALL SELECT 'clicks','pub_id',       COUNT(*), SUM(CASE WHEN pub_id        IS NULL OR TRIM(pub_id)='' THEN 1 ELSE 0 END) FROM clicks
    -- tracking_codes
    UNION ALL SELECT 'tracking_codes','rotate_id', COUNT(*), SUM(CASE WHEN rotate_id  IS NULL THEN 1 ELSE 0 END) FROM tracking_codes
    UNION ALL SELECT 'tracking_codes','code',      COUNT(*), SUM(CASE WHEN code        IS NULL OR TRIM(code)='' THEN 1 ELSE 0 END) FROM tracking_codes
    UNION ALL SELECT 'tracking_codes','service_id',COUNT(*), SUM(CASE WHEN service_id  IS NULL OR TRIM(service_id)='' THEN 1 ELSE 0 END) FROM tracking_codes
    UNION ALL SELECT 'tracking_codes','created_at',COUNT(*), SUM(CASE WHEN created_at  IS NULL THEN 1 ELSE 0 END) FROM tracking_codes
    UNION ALL SELECT 'tracking_codes','expired_at',COUNT(*), SUM(CASE WHEN expired_at  IS NULL THEN 1 ELSE 0 END) FROM tracking_codes
    -- page_events
    UNION ALL SELECT 'page_events','event_id',     COUNT(*), SUM(CASE WHEN event_id      IS NULL THEN 1 ELSE 0 END) FROM page_events
    UNION ALL SELECT 'page_events','rotate_id',    COUNT(*), SUM(CASE WHEN rotate_id     IS NULL THEN 1 ELSE 0 END) FROM page_events
    UNION ALL SELECT 'page_events','campaign_id',  COUNT(*), SUM(CASE WHEN campaign_id   IS NULL THEN 1 ELSE 0 END) FROM page_events
    UNION ALL SELECT 'page_events','event_type',   COUNT(*), SUM(CASE WHEN event_type    IS NULL OR TRIM(event_type)='' THEN 1 ELSE 0 END) FROM page_events
    UNION ALL SELECT 'page_events','received_time',COUNT(*), SUM(CASE WHEN received_time IS NULL THEN 1 ELSE 0 END) FROM page_events
    UNION ALL SELECT 'page_events','msisdn',       COUNT(*), SUM(CASE WHEN msisdn        IS NULL OR TRIM(msisdn)='' THEN 1 ELSE 0 END) FROM page_events
    UNION ALL SELECT 'page_events','device_type',  COUNT(*), SUM(CASE WHEN device_type   IS NULL OR TRIM(device_type)='' THEN 1 ELSE 0 END) FROM page_events
    -- operator_a
    UNION ALL SELECT 'operator_a','transaction_id',COUNT(*), SUM(CASE WHEN transaction_id IS NULL THEN 1 ELSE 0 END) FROM operator_a
    UNION ALL SELECT 'operator_a','rotate_id',     COUNT(*), SUM(CASE WHEN rotate_id      IS NULL THEN 1 ELSE 0 END) FROM operator_a
    UNION ALL SELECT 'operator_a','msisdn',        COUNT(*), SUM(CASE WHEN msisdn         IS NULL OR TRIM(msisdn)='' THEN 1 ELSE 0 END) FROM operator_a
    UNION ALL SELECT 'operator_a','received_time', COUNT(*), SUM(CASE WHEN received_time  IS NULL THEN 1 ELSE 0 END) FROM operator_a
    UNION ALL SELECT 'operator_a','event_code',    COUNT(*), SUM(CASE WHEN event_code     IS NULL THEN 1 ELSE 0 END) FROM operator_a
    UNION ALL SELECT 'operator_a','status',        COUNT(*), SUM(CASE WHEN status         IS NULL OR TRIM(status)='' THEN 1 ELSE 0 END) FROM operator_a
    UNION ALL SELECT 'operator_a','amount',        COUNT(*), SUM(CASE WHEN amount         IS NULL THEN 1 ELSE 0 END) FROM operator_a
    UNION ALL SELECT 'operator_a','currency',      COUNT(*), SUM(CASE WHEN currency       IS NULL OR TRIM(currency)='' THEN 1 ELSE 0 END) FROM operator_a
    -- operator_b
    UNION ALL SELECT 'operator_b','transaction_id',  COUNT(*), SUM(CASE WHEN transaction_id   IS NULL THEN 1 ELSE 0 END) FROM operator_b
    UNION ALL SELECT 'operator_b','rotate_id',       COUNT(*), SUM(CASE WHEN rotate_id        IS NULL THEN 1 ELSE 0 END) FROM operator_b
    UNION ALL SELECT 'operator_b','msisdn',          COUNT(*), SUM(CASE WHEN msisdn           IS NULL OR TRIM(msisdn)='' THEN 1 ELSE 0 END) FROM operator_b
    UNION ALL SELECT 'operator_b','received_time',   COUNT(*), SUM(CASE WHEN received_time    IS NULL THEN 1 ELSE 0 END) FROM operator_b
    UNION ALL SELECT 'operator_b','transaction_type',COUNT(*), SUM(CASE WHEN transaction_type IS NULL OR TRIM(transaction_type)='' THEN 1 ELSE 0 END) FROM operator_b
    UNION ALL SELECT 'operator_b','package_id',      COUNT(*), SUM(CASE WHEN package_id       IS NULL OR TRIM(package_id)='' THEN 1 ELSE 0 END) FROM operator_b
    UNION ALL SELECT 'operator_b','amount',          COUNT(*), SUM(CASE WHEN amount           IS NULL THEN 1 ELSE 0 END) FROM operator_b
    UNION ALL SELECT 'operator_b','currency',        COUNT(*), SUM(CASE WHEN currency         IS NULL OR TRIM(currency)='' THEN 1 ELSE 0 END) FROM operator_b
    -- operator_c
    UNION ALL SELECT 'operator_c','message_id',     COUNT(*), SUM(CASE WHEN message_id      IS NULL THEN 1 ELSE 0 END) FROM operator_c
    UNION ALL SELECT 'operator_c','msisdn',         COUNT(*), SUM(CASE WHEN msisdn          IS NULL OR TRIM(msisdn)='' THEN 1 ELSE 0 END) FROM operator_c
    UNION ALL SELECT 'operator_c','received_time',  COUNT(*), SUM(CASE WHEN received_time   IS NULL THEN 1 ELSE 0 END) FROM operator_c
    UNION ALL SELECT 'operator_c','tracking_code',  COUNT(*), SUM(CASE WHEN tracking_code   IS NULL OR TRIM(tracking_code)='' THEN 1 ELSE 0 END) FROM operator_c
    UNION ALL SELECT 'operator_c','service_id',     COUNT(*), SUM(CASE WHEN service_id      IS NULL OR TRIM(service_id)='' THEN 1 ELSE 0 END) FROM operator_c
    UNION ALL SELECT 'operator_c','delivery_status',COUNT(*), SUM(CASE WHEN delivery_status IS NULL OR TRIM(delivery_status)='' THEN 1 ELSE 0 END) FROM operator_c
)
SELECT
    tbl                                         AS "table",
    col                                         AS "column",
    total                                       AS "total_rows",
    nulls                                       AS "null_or_empty_count",
    ROUND(100.0 * nulls / total, 2)             AS "null_pct"
FROM null_summary
WHERE nulls > 0        
ORDER BY tbl, null_pct DESC;
![alt text](image.png)

FROM campaigns;
| Table | Rows | Columns with missing values |
|-------|------|-----------------------------|
| campaigns | 10 | None |
| clicks | 6,000 | None |
| tracking_codes | 1,175 | None |
| page_events | 7,291 | `msisdn`: 6,533 null (89.6%) |
| operator_A | 3,194 | None |
| operator_B | 3,273 | `rotate_id`: 2,485 null (75.9%) |
| operator_C | 741 | None |

**Summary of columns with missing values:**

- **`page_events.msisdn`** — 89.6% null. This is by design: the README states msisdn is only populated on `ENTRY` events. Since VIEW and CLICK_CTA events are the majority, most rows will naturally have no msisdn. Not a data quality issue.
- **`operator_B.rotate_id`** — 75.9% null. Also by design: the README states rotate_id is only populated on `SUB` rows. All 788 SUB rows have it; all REN (2,286) and UNSUB (199) rows do not. This is the platform's architecture choice — billing events are not directly linked to clicks.

All other tables are complete with no nulls.

---

### 1.2 — operator_A: event_code and status distribution

-- Count by event_code
<!-- SQL -->
SELECT
    event_code,
    COUNT(*)                                    AS total_rows,
    SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) AS success_count,
    SUM(CASE WHEN status = 'FAILED'  THEN 1 ELSE 0 END) AS failed_count,
    SUM(CASE WHEN status = 'PENDING' THEN 1 ELSE 0 END) AS pending_count,
    ROUND(AVG(amount), 4)                       AS avg_amount,
    MIN(amount)                                 AS min_amount,
    MAX(amount)                                 AS max_amount
FROM operator_a
GROUP BY event_code
ORDER BY event_code;

![alt text](image-1.png)

-- Count by STATUS
<!-- SQL -->
SELECT
    status,
    COUNT(*)                                    AS total_rows,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct_of_total
FROM operator_a
GROUP BY status
ORDER BY total_rows DESC;
![alt text](image-3.png)

-- Cross-tab: event_code × status (pivot manually)
SELECT
    event_code,
    COUNT(*)                                                        AS total,
    SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END)            AS success,
    SUM(CASE WHEN status = 'FAILED'  THEN 1 ELSE 0 END)            AS failed,
    SUM(CASE WHEN status = 'PENDING' THEN 1 ELSE 0 END)            AS pending,
    ROUND(100.0 * SUM(CASE WHEN status='SUCCESS' THEN 1 ELSE 0 END) / COUNT(*), 1) AS success_rate_pct
FROM operator_a
GROUP BY event_code
ORDER BY event_code;

![alt text](image-2.png)

**event_code counts:**

| event_code | Count |
|-----------|-------|
| 1 | 917 |
| 2 | 2,160 |
| 3 | 117 |

**status counts:**

| status | Count |
|--------|-------|
| SUCCESS | 1,674 |
| FAILED | 1,083 |
| PENDING | 437 |

**Cross-tabulation (event_code × status):**

| event_code | FAILED | PENDING | SUCCESS |
|-----------|--------|---------|---------|
| 1 | 336 | 136 | 445 |
| 2 | 747 | 301 | 1,112 |
| 3 | 0 | 0 | 117 |

**Interpretation:**

- **event_code = 1 (Subscribe)**: 917 rows — user opts in. Only ~48% succeed. Failed/pending means the operator tried to activate but the handset rejected or the network was slow. Amount is always 0.00 — this is a free activation step.
- **event_code = 2 (Bill)**: 2,160 rows — the recurring charge attempt. Multiple bills per subscriber (one per billing cycle). ~51% success rate. Amount ranges from £1.99–£3.49. The high fail rate is typical for mobile billing (insufficient credit, number not reachable).
- **event_code = 3 (Unsubscribe)**: 117 rows — cancellation. 100% SUCCESS status, which makes sense: an unsubscribe request is terminal and always acknowledged regardless of account state. Amount always 0.00.

---

### 1.3 — operator_B: transaction_type × rotate_id populated

| transaction_type | rotate_id = NULL | rotate_id = present | Total |
|-----------------|-----------------|---------------------|-------|
| REN | 2,286 | 0 | 2,286 |
| SUB | 0 | 788 | 788 |
| UNSUB | 199 | 0 | 199 |
| **Total** | **2,485** | **788** | **3,273** |

**What this pattern tells us:**

Operator B only sends `rotate_id` on `SUB` (subscription) rows — the initial opt-in moment when there is a live click session. Subsequent `REN` (renewal/charge) and `UNSUB` (cancellation) events happen days or weeks later with no active session, so there is no rotate_id to attach.

This is the fundamental join limitation for operator B: **revenue attribution is only possible at subscription time**. To link a renewal charge back to a click, you must first find the SUB row for the same msisdn, then trace that back via its rotate_id. There is no direct join between a billing event and a click.

Note also: `amount` on SUB rows is 0.00 (free opt-in), actual charges only appear on REN rows.

---

### 1.4 — operator_C: tracking_code length > 3 characters

**Length distribution:**

| Code length | Count |
|-------------|-------|
| 3 chars | 645 |
| 4 chars | 50 |
| 5 chars | 96 |

**Count of codes longer than 3 characters: 96**

**Sample examples:**

| tracking_code | length | delivery_status |
|---------------|--------|-----------------|
| JWGT | 4 | SMSC_QUEUED |
| IKVE | 4 | SMSC_QUEUED |
| GIQZ | 4 | DELIVERED |
| SHPD | 4 | DELIVERED |
| YHXXK | 5 | DELIVERED |
| UBEZL | 5 | FAILED |
| ZSNG | 4 | DELIVERED |
| UUYQU | 5 | DELIVERED |

**What this might indicate:**

The `tracking_codes` table defines codes as 3-character alphanumeric strings. Codes longer than 3 characters in `operator_C` will **fail to join** to `tracking_codes.code`, severing the link between the SMS event and the originating click.

Likely causes:
1. **User typo**: The user manually typed the code shown on screen and added an extra character. A 4-char code like "JWGT" when the real code is "JWG" could be a typo with an extra keystroke.
2. **System concatenation bug**: The operator's SMS parsing may be appending a suffix (service ID suffix, network code) to the tracking code before storing it.
3. **Expired code reuse**: The user waited for a code to expire and retried — the platform may have issued a new 4-char code as a collision-avoidance measure.
4. **Case/padding artifact**: Less likely since codes appear clean, but normalization issues could add characters.

This represents a data quality gap: ~13% of operator C records (96 out of 741) cannot be attributed back to a click.

---

### 1.5 — Events per service (VIEW, CLICK_CTA, ENTRY)

Joined `page_events → campaigns` on `campaign_id`.

| service_name | VIEW | CLICK_CTA | ENTRY |
|-------------|------|-----------|-------|
| service_1 | 822 | 449 | 156 |
| service_2 | 841 | 470 | 161 |
| service_3 | 861 | 463 | 155 |
| service_4 | 827 | 455 | 150 |
| service_5 | 871 | 474 | 136 |
| **Total** | **4,222** | **2,311** | **758** |

The funnel is consistent across all services: roughly 55% of views lead to a CTA click, and ~33% of CTA clicks lead to an ENTRY (msisdn submission). No service is a significant outlier, suggesting the traffic distribution is uniform across the five services.

---

### 1.6 — Bills arriving before subscription in operator_A

**Cases found: 82**

Sample (rotate_id, bill_time, sub_time, seconds_early):

| rotate_id | bill_time | sub_time | seconds early |
|-----------|-----------|----------|---------------|
| 40283dbe-... | 2026-01-02 13:37:07 | 2026-01-02 13:37:19 | 12s |
| cbea7974-... | 2026-01-02 22:28:30 | 2026-01-02 22:29:14 | 44s |
| 1d67f0f8-... | 2026-01-02 22:49:15 | 2026-01-02 22:51:07 | 112s |
| b99394cb-... | 2026-01-03 07:08:42 | 2026-01-03 07:09:34 | 52s |

All 82 cases show bills arriving **12–120 seconds** before the subscribe event, with an average of ~67 seconds.

**What might cause this in a real system:**

1. **Clock skew / out-of-order delivery**: The operator's billing system and subscription system run on different servers with slightly different clocks or different processing queues. Both events happen at roughly the same moment, but the bill notification is processed and delivered to the platform before the subscribe notification due to routing differences.
2. **Async pipeline with different latencies**: Subscribe events may go through a heavier validation path (checking regulatory opt-in rules, consent logging) while bill attempts are processed on a lighter, faster path. The bill fires before the subscription confirmation is written.
3. **Retry without status check**: The billing engine may initiate a charge as soon as the user submits their number, before waiting for the formal subscription acknowledgement. If the charge succeeds first, the bill arrives before the subscribe confirmation.
4. **Batch vs real-time**: If subscribe events are batched hourly but bill events are sent in real-time, late batches would appear after bills even if the subscription logically came first.

The 12–120 second range suggests this is a race condition in near-real-time processing, not a fundamentally broken ordering. A robust pipeline should use **event ordering by msisdn** rather than relying on received_time to determine sequence.

---

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

## Part 3 — ETL Pipeline

### Pipeline steps: raw files to analytical tables

**Step 1 — Ingestion (landing zone)**

Each operator drops a CSV file into a cloud storage bucket (e.g., GCS or S3) at a known path pattern like `gs://raw-data/{operator}/{date}/file.csv`. The pipeline watches for file arrival — either via a storage event trigger (Pub/Sub, S3 Event, EventBridge) or a sensor that polls every 15 minutes.

Tool choice: **Cloud storage native events + an orchestrator sensor**. Storage events are low-latency and eliminate polling overhead. A sensor in the orchestrator acts as a fallback and makes the trigger observable.

**Step 2 — Staging load**

The raw CSV is loaded as-is into the staging table for that operator, with a `loaded_date` column appended by the pipeline. No transformation — the goal is to preserve the exact bytes received for auditability.

Tool: **dbt seed or a simple Python ingest script using pandas → BigQuery/Snowflake load API**. For larger files, a direct `COPY INTO` or `LOAD DATA` command is preferable over row-by-row inserts.

**Step 3 — Enrichment (transformation layer)**

SQL models transform each staging table into the enriched event table: normalising event type names, resolving rotate_id for operator B REN rows (via a `msisdn → first SUB` lookup), resolving rotate_id for operator C (via `tracking_code → tracking_codes.code`), and joining to `campaigns` and `clicks` to fill in `service_name`, `partner_id`, `campaign_id`.

Tool choice: **dbt**. dbt is the right tool here because:
- Transformations are pure SQL, version-controlled, and testable.
- dbt's ref() makes lineage between staging → enriched → fact → mart explicit and auditable.
- Built-in tests (not_null, unique, accepted_values, relationships) run automatically after each model.
- It separates transformation logic from orchestration logic cleanly.

Alternative considered: Spark/PySpark. Overkill for this data volume (tens of thousands of rows daily). Adds infrastructure complexity with no benefit.

**Step 4 — Fact table population**

dbt models materialised as incremental tables (`materialized='incremental'`, `unique_key='source_transaction_id'`). Incrementality ensures the pipeline only processes new rows each day and is safe to re-run (idempotent). `is_first_bill` and `is_first_sub` flags are derived using `ROW_NUMBER() OVER (PARTITION BY msisdn, service_name ORDER BY received_time)`.

**Step 5 — Mart refresh**

The `mart_daily_metrics` table is rebuilt for the trailing 3 days on each run (to catch late-arriving data). This is a `DELETE + INSERT` pattern scoped to a date range.

**Step 6 — Data quality checks**

Run dbt tests after each layer. On failure, the DAG stops and alerts fire (see Part 4). The mart is not updated until all upstream tests pass.

---

### Orchestration

Tool choice: **Apache Airflow (managed, e.g., Cloud Composer or Astronomer)**.

Why Airflow:
- DAG structure maps naturally to the pipeline steps (sensor → ingest → enrich → test → mart).
- Task-level retry with configurable backoff handles transient failures.
- Native integration with cloud storage sensors, dbt Cloud/CLI, and alerting hooks.
- Observable: UI shows per-task run history, logs, and SLA breach tracking.

Alternative considered: dbt Cloud's built-in scheduler. Simpler to operate but lacks the file-arrival sensor and has fewer hooks for custom alerting. Good for teams with only dbt transformations and no ingestion step.

---

### Handling failure scenarios

**File arrives late:**
The storage sensor has a configurable `timeout` (e.g., 6 hours after midnight). If the file doesn't arrive by then, the sensor task times out, marks itself as a SLA miss, and sends an alert. The downstream tasks are never triggered. When the file eventually arrives the next day, the pipeline can be manually triggered for the missing date — the incremental models handle backfill safely.

**File with unexpected content:**
A schema validation step runs between ingestion and staging load. It checks column names, data types, and row count plausibility (see Part 4). If validation fails, the file is moved to a `rejected/` prefix in storage and the DAG halts with an alert. The staging table is never written, so downstream tables are unaffected.

**Step fails halfway through:**
Airflow retries the failed task automatically (3 retries with exponential backoff). If all retries fail, the DAG stops at that task — downstream tasks don't execute. The incremental models and upsert logic mean a clean re-run from any step is safe: re-running an already-completed ingest step does nothing harmful because the staging load checks for `loaded_date` deduplification.

**Pipeline runs twice on the same day:**
Prevented at two levels: (1) the Airflow DAG has `catchup=False` and a `max_active_runs=1` guard, so two runs for the same logical date cannot overlap. (2) the incremental dbt models use `unique_key` on `source_transaction_id`, so any duplicate rows from a double-load are silently deduped by an upsert. The mart refresh is scoped to a date range and is idempotent.

---

### Monitoring

- **Airflow SLA misses**: Alert if the DAG hasn't completed by 08:00 UTC.
- **dbt test failures**: Alert on any `not_null`, `unique`, or `relationships` test failure.
- **Row count anomaly check**: If today's row count for any operator is less than 30% of the trailing 7-day average, alert. Catches silent file truncation.
- **Revenue spike/drop check**: If daily revenue deviates by >50% from the prior 7-day average, alert. Catches billing system outages or duplicate loads.

---

## Part 4 — Data Validation

### At the source (raw file, before staging)

These checks run on the freshly received CSV before it touches any database table:

- **Schema check**: Expected column names and count are present. Catches operator-side schema changes before they corrupt downstream tables.
- **Row count plausibility**: File has at least N rows (e.g., 10). An empty or near-empty file is suspicious and should not be silently loaded.
- **Primary key uniqueness**: `transaction_id` / `message_id` has no duplicates within the file. A file sent twice with the same IDs is a duplicate delivery.
- **Date range sanity**: All `received_time` values fall within the expected load date window ±1 day. Catches misconfigured operator systems sending stale data.
- **Known value domains**: `event_code` ∈ {1, 2, 3} for operator A; `transaction_type` ∈ {SUB, REN, UNSUB} for operator B; `delivery_status` ∈ {DELIVERED, SMSC_QUEUED, FAILED} for operator C.
- **Amount non-negative**: `amount >= 0` everywhere. Negative amounts would indicate refunds that the model doesn't currently handle.

If any check fails: reject the file, move to `rejected/`, alert on-call.

---

### During transformation (staging → enriched → fact)

dbt tests applied after each model:

- **not_null** on all NOT NULL columns (transaction_id, msisdn, received_time, event_type, status, event_date).
- **unique** on surrogate keys (sub_id, billing_id, rotate_id in fact_clicks).
- **relationships**: `campaign_id` in fact tables must exist in `campaigns`. `rotate_id` in operator A enriched must exist in `clicks`. (Outer-joined rows that fail are captured in the `attribution_status` flag, not silently lost.)
- **accepted_values** on status, event_type, operator, currency.
- **Custom test — bill sequencing**: No msisdn should have a `fact_billing_events` row with `is_first_bill = TRUE` more than once. Catches model logic errors.
- **Custom test — operator B attribution**: All `fact_billing_events` rows from operator B with `event_type = BILL` should have a corresponding `fact_subscriptions` row for the same msisdn. Orphaned bills (charges with no subscription) indicate a join failure.

---

### In the final output (mart layer)

- **Revenue monotonicity** (soft check): Cumulative monthly revenue should not decrease day-over-day. A decrease means a row was deleted or a correction was applied incorrectly.
- **Conversion rate bounds**: `click_to_sub_rate` and `click_to_bill_rate` should not exceed 1.0 (more conversions than clicks would indicate a join fan-out bug).
- **Zero-click day detection**: If `total_clicks = 0` for any operator on a business day, alert. Likely a missing file.
- **Partition completeness**: Each `(metric_date, operator)` combination expected for the period should have a row in the mart. A missing combination indicates a pipeline gap.

---

### Catching problems over time (monitoring layer)

**Anomaly detection on key metrics:**

A lightweight daily check compares each metric against a rolling baseline:
- Daily revenue per operator: alert if deviation from 7-day average exceeds ±40%.
- Daily subscription count: alert if drops below 50% of 7-day average.
- Attribution rate for operator C: alert if `MATCHED` rate drops below 80% (currently ~87%). A drop indicates the tracking code system is degrading.
- Operator B REN attribution rate: alert if the share of REN rows successfully linked to a campaign drops.

**Silent source detection:**

If an operator's file does not arrive by the SLA time, the pipeline alerts immediately (storage sensor timeout). Additionally, a daily audit query checks whether any `loaded_date` is missing for the expected operators in the staging tables.

**Alerting routing:**

- Critical (schema failure, no file): PagerDuty/Slack `#data-alerts-critical` — on-call data engineer.
- Warning (row count anomaly, metric drift): Slack `#data-alerts-warning` — team channel, no page.
- Informational (daily pipeline success summary with row counts): Slack `#data-ops`.

---

## Part 5 — If You Could Change Anything

### 1. Add `rotate_id` to all operator B billing events

**Problem**: Operator B's REN rows have no `rotate_id`. Attribution is indirect — find the SUB row for the same msisdn and borrow its rotate_id. This breaks if a user subscribes twice to the same service (second subscription has a different campaign) or if there's a data gap in the SUB row.

**Proposed change**: Operator B should include `rotate_id` on all rows, not just SUB. Alternatively, include a `sub_transaction_id` foreign key that points back to the originating SUB row. This keeps attribution deterministic and removes the msisdn-based join.

**Trade-off**: Requires a change on the operator's side, which may involve negotiation. However, it's a pure data enrichment — no change to the billing logic itself.

---

### 2. Store `tracking_code` with explicit length validation and normalisation

**Problem**: 96 of 741 operator C records (13%) have tracking codes longer than 3 characters. These fail to join to `tracking_codes.code` and lose attribution entirely. The platform has no mechanism to detect or correct this at submission time.

**Proposed change**: 
- When displaying the tracking code to the user on the page, enforce a 3-character code in the UI (the field already exists — just add `maxlength="3"` to the input if SMS is entered via a web form).
- On the platform's inbound SMS parser, truncate or reject codes longer than 3 characters and log a rejection reason. This moves the failure to a recoverable stage (user is prompted to resend) rather than a silent attribution loss.
- In `tracking_codes`, add a `display_code` (always exactly 3 chars) separate from a `lookup_code` that includes common typo variants (e.g., if "JWG" is the real code, "JWGT" gets mapped to it via a fuzzy-match table).

**Trade-off**: Fuzzy matching introduces risk of false positives (two users sending similar wrong codes get attributed to the same session). The conservative approach is to just reject and prompt re-entry.

---

### 3. Add `partner_id` directly to `clicks`

**Problem**: To slice metrics by partner, the pipeline joins `clicks → campaigns → partner_id`. This is a simple join, but it means partner identity is only knowable at query time via a join. If a campaign is reassigned to a different partner after the click was recorded, historical attribution changes silently.

**Proposed change**: Denormalise `partner_id` onto `clicks` at insertion time. The click row captures the state of the campaign at the moment the user clicked. This preserves point-in-time attribution and avoids the join.

**Trade-off**: Slight schema redundancy. The `partner_id` in `clicks` could drift from `campaigns.partner_id` if not kept in sync. The solution is to treat `clicks.partner_id` as immutable once written — never update it.

---

### 4. Add a `funnel_session_id` to page_events

**Problem**: `page_events` uses `rotate_id` to link to a click, which works. But if a user navigates back and then forward (generating a new page VIEW but no new click), the second VIEW cannot be linked to a click. More importantly, there is no way to determine whether a VIEW and a CLICK_CTA in the same session belong together or are from separate visits by the same rotate_id on different days.

**Proposed change**: Add a `session_id` (generated client-side, e.g., a UUID stored in sessionStorage) to all page_events. This allows grouping all events from a single continuous user visit, independent of the click session.

**Trade-off**: Requires a frontend change. Slightly more complexity in the ETL to handle the session dimension. Worth it for accurate funnel analysis.

---

### 5. Capture `msisdn` on CLICK_CTA events (not just ENTRY)

**Problem**: `msisdn` is only populated on `ENTRY` events. This means we cannot tell whether a user who clicked the CTA but never submitted their number was a known subscriber who abandoned, or a new user. Knowing whether a CTA-clicker's msisdn was already in the subscriber base would allow much richer segmentation (re-engagement vs new acquisition funnels).

**Proposed change**: For flows where the user is already identified (e.g., returning via a pre-filled landing page with msisdn in the URL parameter), capture msisdn on CLICK_CTA as well. This should only be done where the msisdn is already available to the page (not prompt for it earlier than needed, for regulatory reasons).

**Trade-off**: Privacy/GDPR consideration — capturing msisdn at an earlier funnel stage requires confirming that the user has already consented. If the msisdn comes from a URL parameter passed by the operator, this is typically already consented. The platform's legal team should review before implementation.

---

### 6. Add `expired_at` validation at SMS receipt time for operator C

**Problem**: `tracking_codes.expired_at` is 30 minutes after creation. If a user submits their code late, the tracking code has expired and the platform cannot reliably attribute the SMS to the click. Currently there is no rejection mechanism — the code just arrives in operator C's table, the join to `tracking_codes` succeeds (if the code length is right) but the session has expired.

**Proposed change**: On the inbound SMS processing side, check `expired_at` at receipt time. If `received_time > expired_at`, log the event as `attribution_status = EXPIRED` rather than `MATCHED`, and surface this in monitoring. This does not change whether the user gets their service (that's an operator decision) but makes the attribution failure explicit and measurable rather than a hidden data quality issue.

**Trade-off**: No change required from operators. This is a platform-side improvement to the enrichment logic. Low risk.
