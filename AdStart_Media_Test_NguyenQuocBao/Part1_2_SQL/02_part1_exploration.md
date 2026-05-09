# DE Test — Full Answer

**Dataset**: Mobile advertising platform, UK, January 2026  
**Engine used**: Python / postgres

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

![1.1 — Row counts and null/empty rates](screenshots\1_1.png) 

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

![Count by event_code](1_2_eventcode.png)

-- Count by STATUS
<!-- SQL -->
SELECT
    status,
    COUNT(*)                                    AS total_rows,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct_of_total
FROM operator_a
GROUP BY status
ORDER BY total_rows DESC;
![Count by STATUS](screenshots\1_2_countstatus.png)

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

![event_code × status](screenshots\1_2_eventcodestatus.png)

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
<!-- sql -->
SELECT
    transaction_type,
    CASE WHEN rotate_id IS NULL THEN 'empty' ELSE 'populated' END              AS rotate_id_status,
    COUNT(*)                                                                    AS row_count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY transaction_type), 2) AS pct_within_type
FROM operator_b
GROUP BY transaction_type, rotate_id_status
ORDER BY transaction_type, rotate_id_status;

-- amount stats to understand money
SELECT
    transaction_type,
    COUNT(*)                                               AS total_rows,
    SUM(CASE WHEN rotate_id IS NOT NULL THEN 1 ELSE 0 END) AS has_rotate_id,
    SUM(CASE WHEN rotate_id IS NULL     THEN 1 ELSE 0 END) AS no_rotate_id,
    ROUND(AVG(amount), 4)                                  AS avg_amount,
    SUM(amount)                                            AS total_amount
FROM operator_b
GROUP BY transaction_type
ORDER BY transaction_type;

![transaction_type x rotate_id](screenshots\1_3_transactiontype.png)

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
<!-- sql -->
-- ============================================================
-- 1.4 — operator_C: tracking_code values longer than 3 characters
-- ============================================================

-- QUERY 1: How many? (quantity + summary percent)
SELECT
    SUM(CASE WHEN LENGTH(tracking_code) = 3 THEN 1 ELSE 0 END)     AS valid_3chars,
    SUM(CASE WHEN LENGTH(tracking_code) > 3 THEN 1 ELSE 0 END)     AS invalid_over3,
    COUNT(*)                                                         AS total_rows,
    ROUND(100.0 *
        SUM(CASE WHEN LENGTH(tracking_code) > 3 THEN 1 ELSE 0 END)
        / COUNT(*), 2)                                               AS invalid_pct,
    -- Among invalid tracking codes, some rows are still marked as DELIVERED.
	-- This means billing/subscription may have succeeded, but attribution to a campaign was lost.
    SUM(CASE WHEN LENGTH(tracking_code) > 3
             AND delivery_status = 'DELIVERED' THEN 1 ELSE 0 END)   AS invalid_and_delivered,
    ROUND(100.0 *
        SUM(CASE WHEN LENGTH(tracking_code) > 3
                 AND delivery_status = 'DELIVERED' THEN 1 ELSE 0 END)
        / NULLIF(SUM(CASE WHEN delivery_status = 'DELIVERED'
                          THEN 1 ELSE 0 END), 0), 2)                 AS pct_of_all_delivered
FROM operator_c;

![How many? (quantity + summary percent)](screenshots\1_4_how_many.png)

-- QUERY 2:  length — join - click or not?
SELECT
    LENGTH(tracking_code)                                            AS code_length,
    COUNT(*)                                                         AS row_count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2)              AS pct_of_total,
    SUM(CASE WHEN delivery_status = 'DELIVERED'   THEN 1 ELSE 0 END) AS delivered,
    SUM(CASE WHEN delivery_status = 'SMSC_QUEUED' THEN 1 ELSE 0 END) AS queued,
    SUM(CASE WHEN delivery_status = 'FAILED'      THEN 1 ELSE 0 END) AS failed,
    CASE WHEN LENGTH(tracking_code) = 3
         THEN 'attributable'
         ELSE 'attribution lost — no join possible'
    END                                                              AS attribution_status
FROM operator_c
GROUP BY code_length
ORDER BY code_length;

![length — join - click or not?](screenshots\1_4_lengthjoinclickedornot.png)

-- QUERY 3: Show a few examples + check typo hypothesis
-- prefix_match: if LEFT(code,3) = tracking_codes → user type extrar character
-- if prefix is NULL too → code is wrong / format 
SELECT
    oc.tracking_code,
    LENGTH(oc.tracking_code)                        AS code_length,
    oc.delivery_status,
    oc.service_id,
    LEFT(oc.tracking_code, 3)                       AS prefix_3chars,
    tc.code                                         AS prefix_match,   -- NULL = không phải typo
    CASE WHEN tc.code IS NOT NULL
         THEN 'TYPO — extra chars after valid code'
         ELSE 'WRONG CODE — prefix also unknown'
    END                                             AS likely_cause
FROM operator_c oc
LEFT JOIN tracking_codes tc
       ON LEFT(oc.tracking_code, 3) = tc.code
WHERE LENGTH(oc.tracking_code) > 3
ORDER BY likely_cause, oc.delivery_status
LIMIT 15;

![length — join - click or not?](screenshots\1_4_few_examples.png)

-- QUERY 4: Summary — TYPO vs WRONG CODE
SELECT
    CASE WHEN tc.code IS NOT NULL
         THEN 'TYPO — extra chars after valid code'
         ELSE 'WRONG CODE — prefix also unknown'
    END                                             AS failure_type,
    COUNT(*)                                        AS count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct,
    SUM(CASE WHEN oc.delivery_status = 'DELIVERED'
             THEN 1 ELSE 0 END)                     AS delivered_count
FROM operator_c oc
LEFT JOIN tracking_codes tc
       ON LEFT(oc.tracking_code, 3) = tc.code
WHERE LENGTH(oc.tracking_code) > 3
GROUP BY failure_type
ORDER BY count DESC;

![Summary — TYPO vs WRONG CODE](screenshots\1_4_summary.png)

### Findings

96 out of 741 rows (13.0%) contain `tracking_code` values longer than 3 characters.  
None of these values can successfully join to `tracking_codes.code`, resulting in complete attribution loss.

More importantly, 62 of these invalid rows still have `delivery_status = 'DELIVERED'`.  
This means the user was successfully charged or subscribed, but the platform could not attribute the revenue back to the originating click or campaign. As a result, approximately 13.2% of all delivered revenue events lose attribution visibility.

Prefix analysis reveals two distinct root causes:

- **8 rows (8.3%)** are likely simple user typos.  
  The first 3 characters match a valid tracking code in `tracking_codes`, indicating the user probably entered extra trailing characters after the correct code.

- **88 rows (91.7%)** contain completely unknown codes.  
  Even the first 3-character prefix does not match any valid code. This strongly suggests an upstream system issue, where the operator's SMS parser or external integration appended additional suffixes (such as network identifiers or session tokens) before storing the tracking code.

### Business Impact

This issue directly affects attribution accuracy and campaign performance measurement.  
Revenue events exist in the system, but cannot be linked back to campaigns, publishers, or clicks, leading to underreported campaign ROI and incomplete conversion analytics.

---

### 1.5 — Events per service (VIEW, CLICK_CTA, ENTRY)

<!-- sql -->
-- Join page_events -> campaigns to take service_name
-- Pivot by hand into 1 row per service
SELECT
    c.service_name,
    SUM(CASE WHEN pe.event_type = 'VIEW'      THEN 1 ELSE 0 END) AS view_count,
    SUM(CASE WHEN pe.event_type = 'CLICK_CTA' THEN 1 ELSE 0 END) AS click_cta_count,
    SUM(CASE WHEN pe.event_type = 'ENTRY'     THEN 1 ELSE 0 END) AS entry_count,
    COUNT(*)                                                       AS total_events,
    -- % funnel
    ROUND(100.0 * SUM(CASE WHEN pe.event_type = 'CLICK_CTA' THEN 1 ELSE 0 END)
               / NULLIF(SUM(CASE WHEN pe.event_type = 'VIEW' THEN 1 ELSE 0 END), 0), 1) AS cta_over_view_pct,
    ROUND(100.0 * SUM(CASE WHEN pe.event_type = 'ENTRY' THEN 1 ELSE 0 END)
               / NULLIF(SUM(CASE WHEN pe.event_type = 'CLICK_CTA' THEN 1 ELSE 0 END), 0), 1) AS entry_over_cta_pct
FROM page_events pe
JOIN campaigns c ON pe.campaign_id = c.id
GROUP BY c.service_name
ORDER BY c.service_name;

![Events per service (VIEW, CLICK_CTA, ENTRY)](screenshots\1_5.png)

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

<!-- sql -->
```sql
-- Find all case bill arrived before subscribe within the same rotate_id
WITH bill_events AS (
    SELECT
        rotate_id,
        received_time AS bill_time,
        transaction_id AS bill_transaction_id
    FROM operator_a
    WHERE event_code = 2
),
sub_events AS (
    SELECT
        rotate_id,
        received_time AS sub_time,
        transaction_id AS sub_transaction_id
    FROM operator_a
    WHERE event_code = 1
)
SELECT
    b.rotate_id,
    b.bill_transaction_id,
    s.sub_transaction_id,
    b.bill_time,
    s.sub_time,
    EXTRACT(EPOCH FROM (s.sub_time - b.bill_time))::INT AS seconds_early
FROM bill_events b
JOIN sub_events s ON b.rotate_id = s.rotate_id
WHERE b.bill_time < s.sub_time
ORDER BY seconds_early ASC;

-- Summary stats
WITH bill_events AS (
    SELECT rotate_id, received_time AS bill_time
    FROM operator_a WHERE event_code = 2
),
sub_events AS (
    SELECT rotate_id, received_time AS sub_time
    FROM operator_a WHERE event_code = 1
),
early_bills AS (
    SELECT
        b.rotate_id,
        b.bill_time,
        s.sub_time,
        EXTRACT(EPOCH FROM (s.sub_time - b.bill_time))::INT AS seconds_early
    FROM bill_events b
    JOIN sub_events s ON b.rotate_id = s.rotate_id
    WHERE b.bill_time < s.sub_time
)
SELECT
    COUNT(*)                          AS total_cases,
    MIN(seconds_early)                AS min_seconds_early,
    MAX(seconds_early)                AS max_seconds_early,
    ROUND(AVG(seconds_early), 1)      AS avg_seconds_early,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY seconds_early) AS median_seconds_early
FROM early_bills;
```

![Events per service (VIEW, CLICK_CTA, ENTRY)](screenshots/1_6.png)

**Cases found: 82**

### Interpretation

82 cases were identified where billing events (`event_code = 2`) arrived before subscribe events (`event_code = 1`) for the same `rotate_id`.

The delay ranges from 7 to 120 seconds (median: 71 seconds), which strongly suggests a near-real-time race condition rather than corrupted data.

Possible causes include:

- clock skew between distributed systems
- asynchronous pipelines with different processing latencies
- fire-and-forget billing logic
- batch subscription delivery vs real-time billing delivery

### Engineering Recommendation

The pipeline should not rely solely on `received_time` to determine event order.

A practical solution is to temporarily hold unmatched billing events in staging for up to 2 minutes before retrying the subscription join.

This buffering approach would successfully resolve all detected cases because the maximum observed delay is only 120 seconds.

---