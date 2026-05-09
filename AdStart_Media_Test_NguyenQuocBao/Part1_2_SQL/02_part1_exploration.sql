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