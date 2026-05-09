# DE Test — Part 2: Data Modeling
> Dataset: Mobile Advertising Platform | UK, January 2026  
> Goal: Unified daily view of Subscriptions, First Bills, Revenue, Conversion across 3 operators

---

## 0. Tư duy thiết kế trước khi viết DDL

Trước khi chạm vào bàn phím, cần trả lời 4 câu hỏi:

| Câu hỏi | Trả lời cho bài này |
|---------|-------------------|
| **Ai đọc data này?** | Business/BI team — cần slice theo operator, service, partner |
| **Grain của mỗi metric là gì?** | Subscription = 1 row/user/service; Billing = 1 row/transaction; Click = 1 row/click |
| **Điểm đau của data nguồn là gì?** | 3 operators, 3 format khác nhau, link về click theo 3 cách khác nhau |
| **Trade-off chấp nhận được?** | Denormalize một số dimension để tránh join nhiều tầng khi query |

**Kiến trúc đề xuất: 3 lớp**

```
[RAW / STAGING]          [UNIFIED FACTS]             [AGGREGATED MART]
operator_a          ─┐
operator_b          ─┼──►  fct_subscriptions  ─┐
operator_c          ─┘      fct_billing        ─┼──►  mart_daily_performance
                            fct_clicks         ─┘
clicks / campaigns  ──►  dim_campaigns
```

- **Raw layer**: đã có — 7 bảng gốc
- **Unified Facts**: chuẩn hoá và resolve attribution, đây là lớp quan trọng nhất
- **Mart**: pre-aggregate cho dashboard/BI, refresh hàng ngày

---

## 1. dim_campaigns — Dimension chính

### Tại sao cần lớp này?

Bảng `campaigns` gốc đã có, nhưng các fact table sẽ cần join vào để lấy
`service_name`, `partner_id`, `operator` — là 3 trục slice chính của bài toán.
Thay vì để BI tool join 3–4 tầng mỗi khi query, ta tạo một dimension rõ ràng
làm single source of truth cho tất cả metadata của campaign.

### DDL

```sql
CREATE TABLE dim_campaigns (
    campaign_id    UUID        PRIMARY KEY,
    -- FK → campaigns.id; surrogate key không cần thiết vì UUID đã globally unique

    operator       TEXT        NOT NULL,
    -- 'operator_A' | 'operator_B' | 'operator_C'
    -- Denormalize thẳng vào đây vì operator là fixed attribute của campaign,
    -- không bao giờ thay đổi sau khi campaign được tạo.

    service_name   TEXT        NOT NULL,
    -- Tên dịch vụ — đây là trục slice quan trọng nhất của business reporting.
    -- Denormalize để tránh join thêm vào bảng ngoài.

    service_model  TEXT        NOT NULL,
    -- 'one-off' | 'subscription'
    -- Quan trọng để filter: one-off sẽ không có REN/billing cycle

    partner_id     UUID        NOT NULL,
    -- Đối tác mua traffic — trục slice thứ 3 theo yêu cầu bài.
    -- Giữ nguyên UUID, không join thêm bảng partner vì dataset không có.

    status         TEXT        NOT NULL,
    -- 'active' | 'paused' | etc. — dùng để filter campaign còn hiệu lực

    created_at     TIMESTAMPTZ NOT NULL,
    -- Ngày campaign được tạo — có thể dùng để cohort analysis sau này

    loaded_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
    -- Audit column: biết khi nào row này được load vào mart
);
```

> **Mindset — Tại sao denormalize `service_name` và `operator` vào đây?**
>
> Trong OLTP (transactional), normalize 100% là đúng — tránh update anomaly.
> Trong OLAP/reporting layer, query phải chạy nhanh, đơn giản. Nếu BI tool
> phải join qua 4 bảng để biết `service_name` của một billing row, query sẽ
> chậm và dễ sai. Dimension table đóng vai trò "pre-resolved lookup" — trade
> một ít storage để đổi lấy query simplicity và performance.

---

## 2. fct_subscriptions — Fact table trung tâm

### Grain: 1 row = 1 sự kiện opt-in của 1 user vào 1 service

Đây là bảng quan trọng nhất. Nó phải hấp thụ subscription events từ cả 3
operators dù format khác nhau hoàn toàn:

| Operator | Subscription event | Có rotate_id? |
|----------|-------------------|---------------|
| operator_a | `event_code = 1` | ✅ Luôn có |
| operator_b | `transaction_type = 'SUB'` | ✅ Luôn có |
| operator_c | `delivery_status = 'DELIVERED'` | ❌ Phải lookup qua `tracking_code` |

### DDL

```sql
CREATE TABLE fct_subscriptions (
    subscription_id       UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    -- Surrogate key — quan trọng: KHÔNG dùng transaction_id của operator
    -- làm PK vì sau này có thể cần merge/dedup cross-operator.
    -- gen_random_uuid() là Postgres native, không cần extension.

    operator              TEXT        NOT NULL,
    -- 'operator_A' | 'operator_B' | 'operator_C'
    -- Denormalize vào đây để query không cần join thêm khi filter by operator.

    source_transaction_id TEXT        NOT NULL,
    -- ID gốc từ operator table: operator_a.transaction_id, operator_b.transaction_id,
    -- operator_c.message_id. Giữ lại để audit và trace back về raw data.

    rotate_id             UUID,
    -- FK → clicks.rotate_id — NULLABLE vì operator_c phải lookup qua tracking_code
    -- trước, nếu không tìm được (expired hoặc ambiguous) thì để NULL.
    -- Đây là "attribution field" — cần cho conversion metric.

    campaign_id           UUID        NOT NULL REFERENCES dim_campaigns(campaign_id),
    -- Resolved từ rotate_id → clicks.campaign_id.
    -- NOT NULL vì mọi subscription đều phải thuộc về một campaign.

    service_name          TEXT        NOT NULL,
    -- Denormalize từ dim_campaigns — tránh join khi slice by service.

    partner_id            UUID        NOT NULL,
    -- Denormalize từ dim_campaigns — tránh join khi slice by partner.

    msisdn                TEXT        NOT NULL,
    -- Phone number (anonymised trong production).
    -- Đây là user identifier thực sự — dùng để link billing rows về đây.

    subscribed_at         TIMESTAMPTZ NOT NULL,
    -- Thời điểm opt-in — quan trọng nhất để aggregate theo ngày.

    report_date           DATE        NOT NULL GENERATED ALWAYS AS (subscribed_at::DATE) STORED,
    -- Pre-computed date column để partition và GROUP BY nhanh hơn.
    -- STORED = Postgres tính 1 lần lúc insert, không tính lại mỗi khi query.
    -- GENERATED AS ... STORED dùng được vì ::DATE là immutable (khác với INTERVAL).

    attribution_method    TEXT        NOT NULL,
    -- 'direct_rotate_id' | 'tracking_code_lookup' | 'unattributed'
    -- Ghi lại HOW attribution được resolved — cực kỳ quan trọng để debug
    -- và để business biết confidence level của số liệu.

    loaded_at             TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_fct_sub_campaign_date  ON fct_subscriptions(campaign_id, report_date);
CREATE INDEX idx_fct_sub_msisdn         ON fct_subscriptions(msisdn);
CREATE INDEX idx_fct_sub_rotate_id      ON fct_subscriptions(rotate_id) WHERE rotate_id IS NOT NULL;
CREATE INDEX idx_fct_sub_report_date    ON fct_subscriptions(report_date);
-- Index riêng theo report_date vì đây là cột filter phổ biến nhất trong daily reporting.
```

> **Mindset — Tại sao surrogate key thay vì dùng transaction_id của operator?**
>
> `transaction_id` của operator_a là UUID, operator_b cũng UUID, operator_c
> dùng `message_id`. Nếu dùng trực tiếp làm PK, sẽ không thể có 1 bảng thống
> nhất (3 namespace khác nhau, có thể trùng nhau về mặt lý thuyết). Surrogate
> key do chúng ta tự gen cho phép bảng này là thực sự unified. `source_transaction_id`
> giữ lại nguyên gốc để traceability.

> **Mindset — Tại sao `attribution_method` column?**
>
> Đây là column mà junior DE hay bỏ qua, nhưng senior DE sẽ luôn thêm vào.
> Khi business hỏi "tại sao số subscription hôm nay tăng 20%?", nếu không có
> column này, rất khó debug xem là do data thực hay do attribution logic thay
> đổi. Nó cũng giúp filter: "chỉ show subscriptions có attribution confident
> (direct rotate_id)", tránh inflated numbers.

---

## 3. fct_billing — Billing transactions

### Grain: 1 row = 1 lần charge thành công

Khác với subscription (opt-in), billing là recurring event. Một user có thể
được charge nhiều lần. Vì vậy phải tách ra bảng riêng — nếu nhét vào
`fct_subscriptions` sẽ vi phạm grain.

| Operator | Billing event | Amount |
|----------|--------------|--------|
| operator_a | `event_code = 2, status = 'SUCCESS'` | Có |
| operator_b | `transaction_type = 'REN', amount > 0` | Có |
| operator_c | `delivery_status = 'DELIVERED'` | Không có amount — subscription + charge xảy ra cùng lúc |

### DDL

```sql
CREATE TABLE fct_billing (
    billing_id            UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    -- Surrogate key, cùng lý do như fct_subscriptions.

    operator              TEXT        NOT NULL,

    source_transaction_id TEXT        NOT NULL,
    -- ID gốc từ operator để trace back.

    subscription_id       UUID        REFERENCES fct_subscriptions(subscription_id),
    -- FK về fct_subscriptions — NULLABLE vì operator_b REN không có rotate_id,
    -- phải link qua msisdn → SUB → subscription_id.
    -- Nếu không resolve được (orphan billing), để NULL thay vì drop row.

    campaign_id           UUID        NOT NULL REFERENCES dim_campaigns(campaign_id),
    -- Resolved từ subscription_id → campaign_id. NOT NULL vì revenue luôn phải
    -- thuộc về một campaign để business có thể slice by partner/service.

    service_name          TEXT        NOT NULL,
    partner_id            UUID        NOT NULL,
    -- Denormalize từ dim_campaigns — cùng lý do như fct_subscriptions.

    msisdn                TEXT        NOT NULL,

    amount                NUMERIC(10,2) NOT NULL,
    currency              CHAR(3)       NOT NULL DEFAULT 'GBP',

    billed_at             TIMESTAMPTZ NOT NULL,
    report_date           DATE        NOT NULL GENERATED ALWAYS AS (billed_at::DATE) STORED,

    is_first_bill         BOOLEAN     NOT NULL DEFAULT FALSE,
    -- Pre-computed flag: TRUE nếu đây là lần charge đầu tiên của msisdn này
    -- cho service này. Tính bằng ROW_NUMBER() khi populate bảng.
    -- Metric "First Bill" trong yêu cầu bài cần flag này.

    billing_sequence      SMALLINT    NOT NULL DEFAULT 1,
    -- Thứ tự của billing event cho msisdn + service: 1 = first, 2 = second, v.v.
    -- Cho phép cohort analysis: "user nào bị churn sau lần charge thứ 3?"

    billing_status        TEXT        NOT NULL,
    -- 'SUCCESS' | 'FAILED' | 'PENDING'
    -- operator_c không có status riêng → DELIVERED = 'SUCCESS' implicit.

    loaded_at             TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_fct_billing_campaign_date   ON fct_billing(campaign_id, report_date);
CREATE INDEX idx_fct_billing_subscription_id ON fct_billing(subscription_id) WHERE subscription_id IS NOT NULL;
CREATE INDEX idx_fct_billing_msisdn          ON fct_billing(msisdn);
CREATE INDEX idx_fct_billing_is_first_bill   ON fct_billing(report_date) WHERE is_first_bill = TRUE;
-- Partial index trên first bill vì đây là filter cực kỳ phổ biến
-- nhưng chỉ ~15-20% rows là first bill.
```

> **Mindset — Tại sao `is_first_bill` là pre-computed flag thay vì tính khi query?**
>
> Có thể tính runtime bằng:
> ```sql
> ROW_NUMBER() OVER (PARTITION BY msisdn, service_name ORDER BY billed_at) = 1
> ```
> Nhưng mỗi lần query sẽ scan toàn bộ bảng billing để tính window function —
> rất tốn kém khi data grow. Pre-compute 1 lần khi load, lưu flag → query chỉ
> cần `WHERE is_first_bill = TRUE`. Trade-off: nếu historical data bị re-process
> (nhận late-arriving rows), phải recalculate flag. Đây là trade-off có thể
> chấp nhận được vì first-bill là stable metric, ít khi cần recompute.

> **Mindset — Tại sao `billing_sequence` thay vì chỉ `is_first_bill`?**
>
> `is_first_bill` trả lời câu hỏi hiện tại. `billing_sequence` trả lời câu hỏi
> tương lai: "average lifetime value", "churn at which billing cycle". Thêm 2
> bytes SMALLINT ngay từ đầu rẻ hơn nhiều so với migrate sau khi bảng đã có
> hàng triệu rows.

---

## 4. fct_clicks — Enriched click table

### Grain: 1 row = 1 click (không đổi từ bảng gốc)

Bảng `clicks` gốc đã có grain đúng. Bảng này là phiên bản enriched với các
flag pre-computed để tính conversion metric.

```sql
CREATE TABLE fct_clicks (
    rotate_id             UUID        PRIMARY KEY,
    -- Giữ nguyên từ clicks.rotate_id — đây là natural key duy nhất của click.

    campaign_id           UUID        NOT NULL REFERENCES dim_campaigns(campaign_id),
    service_name          TEXT        NOT NULL,
    operator              TEXT        NOT NULL,
    partner_id            UUID        NOT NULL,
    -- Tất cả denormalize từ dim_campaigns — vì đây là bảng base cho conversion
    -- metric, phải slice được ngay mà không cần join thêm.

    pub_id                TEXT,
    -- Publisher ID — giữ nguyên từ clicks. Nullable vì một số clicks không có.

    clicked_at            TIMESTAMPTZ NOT NULL,
    report_date           DATE        NOT NULL GENERATED ALWAYS AS (clicked_at::DATE) STORED,

    -- === CONVERSION FLAGS ===
    has_page_view         BOOLEAN     NOT NULL DEFAULT FALSE,
    -- TRUE nếu có ít nhất 1 page_events row với event_type='VIEW' cho rotate_id này.

    has_cta_click         BOOLEAN     NOT NULL DEFAULT FALSE,
    -- TRUE nếu có page_event 'CLICK_CTA' — user đã tương tác với nút subscribe.

    has_entry             BOOLEAN     NOT NULL DEFAULT FALSE,
    -- TRUE nếu có page_event 'ENTRY' — user đã nhập thông tin (msisdn).

    has_subscription      BOOLEAN     NOT NULL DEFAULT FALSE,
    -- TRUE nếu có row trong fct_subscriptions với rotate_id này.

    has_first_bill        BOOLEAN     NOT NULL DEFAULT FALSE,
    -- TRUE nếu subscription đó đã có successful first charge.
    -- Đây là conversion metric cuối cùng — "paying subscriber".

    loaded_at             TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_fct_clicks_campaign_date ON fct_clicks(campaign_id, report_date);
CREATE INDEX idx_fct_clicks_pub_id        ON fct_clicks(pub_id) WHERE pub_id IS NOT NULL;
CREATE INDEX idx_fct_clicks_report_date   ON fct_clicks(report_date);
```

> **Mindset — Tại sao conversion funnel flags thay vì tính từ page_events join mỗi lần query?**
>
> Câu hỏi conversion phổ biến nhất: "Tỷ lệ click → subscription của campaign X
> tuần này là bao nhiêu?" Nếu tính runtime, query phải JOIN fct_clicks →
> page_events (7K rows) → fct_subscriptions (vài nghìn rows) → GROUP BY.
> Với data tháng, query này có thể mất vài giây. Với pre-computed flags, chỉ
> cần: `SELECT SUM(has_subscription::INT) * 1.0 / COUNT(*) FROM fct_clicks WHERE
> report_date BETWEEN ... AND campaign_id = ...` — cực nhanh.
>
> Trade-off: mỗi khi nhận late-arriving subscription data, phải UPDATE
> fct_clicks. Nhưng đây là trường hợp ít xảy ra và có thể handle bằng batch job.

---

## 5. mart_daily_performance — Pre-aggregated reporting mart

### Grain: 1 row = 1 ngày × 1 campaign × 1 operator × 1 service × 1 partner

Đây là bảng cuối cùng mà BI tool (Looker, Tableau, Metabase) đọc. Refresh
hàng ngày sau khi tất cả fact tables đã được load.

```sql
CREATE TABLE mart_daily_performance (
    report_date           DATE        NOT NULL,
    campaign_id           UUID        NOT NULL REFERENCES dim_campaigns(campaign_id),
    operator              TEXT        NOT NULL,
    service_name          TEXT        NOT NULL,
    partner_id            UUID        NOT NULL,

    -- === VOLUME ===
    total_clicks          INT         NOT NULL DEFAULT 0,
    total_page_views      INT         NOT NULL DEFAULT 0,
    total_cta_clicks      INT         NOT NULL DEFAULT 0,
    total_entries         INT         NOT NULL DEFAULT 0,

    -- === SUBSCRIPTION METRICS ===
    total_subscriptions   INT         NOT NULL DEFAULT 0,
    -- Count distinct users subscribed trên ngày này.

    -- === BILLING METRICS ===
    total_first_bills     INT         NOT NULL DEFAULT 0,
    -- Count subscriptions có successful first charge trên ngày này.

    total_renewals        INT         NOT NULL DEFAULT 0,
    -- Count operator_b REN + operator_a event_code=2 lần 2 trở đi.

    -- === REVENUE ===
    total_revenue         NUMERIC(12,4) NOT NULL DEFAULT 0,
    currency              CHAR(3)       NOT NULL DEFAULT 'GBP',
    -- Chỉ tính amount từ SUCCESSFUL billing events.

    -- === CONVERSION RATES ===
    -- Pre-computed để BI tool không cần tính division với risk chia 0.
    sub_conversion_rate   NUMERIC(8,6),
    -- total_subscriptions / NULLIF(total_clicks, 0)
    -- NULLIF tránh division by zero khi không có click nào trong ngày.

    bill_conversion_rate  NUMERIC(8,6),
    -- total_first_bills / NULLIF(total_clicks, 0)

    PRIMARY KEY (report_date, campaign_id),

    loaded_at             TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_mart_daily_date       ON mart_daily_performance(report_date);
CREATE INDEX idx_mart_daily_operator   ON mart_daily_performance(operator, report_date);
CREATE INDEX idx_mart_daily_partner    ON mart_daily_performance(partner_id, report_date);
CREATE INDEX idx_mart_daily_service    ON mart_daily_performance(service_name, report_date);
```

> **Mindset — Tại sao có mart layer khi đã có fact tables?**
>
> Fact tables có grain thấp (row-level). Dashboard cần aggregate. Có 2 lựa chọn:
>
> - **Option A**: BI tool tự aggregate từ fact tables mỗi khi user mở dashboard.
>   → Flexible nhưng chậm khi data scale. BI tools thường không optimize tốt.
>
> - **Option B**: Pre-aggregate 1 lần mỗi đêm vào mart table.
>   → Nhanh, ổn định, nhưng cần maintain thêm 1 job. Dashboard chỉ là
>   `SELECT * FROM mart_daily_performance WHERE report_date = CURRENT_DATE - 1`.
>
> Với yêu cầu bài (daily metrics), Option B rõ ràng phù hợp hơn. Mart table
> cũng làm dễ dàng việc expose data ra ngoài (API, Google Sheets export) mà
> không lo performance.

---

## 6. Attribution Logic — Phần khó nhất của thiết kế

Đây là phần mà recruiter sẽ hỏi sâu nhất. Cần giải thích rõ.

### Làm thế nào để populate `fct_subscriptions.rotate_id`?

```
operator_a (event_code=1):
    rotate_id có sẵn → Direct insert, attribution_method = 'direct_rotate_id'

operator_b (transaction_type='SUB'):
    rotate_id có sẵn → Direct insert, attribution_method = 'direct_rotate_id'

operator_c (delivery_status='DELIVERED'):
    Không có rotate_id. Phải lookup:
    operator_c.tracking_code
        → JOIN tracking_codes tc ON tc.code = operator_c.tracking_code
           AND operator_c.received_time BETWEEN tc.created_at AND tc.expired_at
        → tc.rotate_id
    Nếu tìm được: attribution_method = 'tracking_code_lookup'
    Nếu không (expired, nhiều match): attribution_method = 'unattributed'
```

### Làm thế nào để populate `fct_billing.subscription_id` cho operator_b REN?

```sql
-- operator_b REN không có rotate_id, chỉ có msisdn.
-- Chain: REN.msisdn → SUB row cùng msisdn → subscription_id

UPDATE fct_billing b
SET subscription_id = sub.subscription_id
FROM fct_subscriptions sub
WHERE b.operator = 'operator_B'
  AND b.source_event_type = 'REN'
  AND b.msisdn = sub.msisdn
  AND sub.operator = 'operator_B'
  AND sub.subscribed_at <= b.billed_at;
-- Điều kiện thời gian: billing chỉ có thể sau subscription
```

> **Điều này quan trọng**: Nếu một user unsubscribe rồi resubscribe, có thể có
> 2 SUB rows cho cùng msisdn. Phải lấy SUB gần nhất trước billing event:
> ```sql
> AND sub.subscribed_at = (
>     SELECT MAX(subscribed_at) FROM fct_subscriptions
>     WHERE msisdn = b.msisdn AND operator = 'operator_B'
>     AND subscribed_at <= b.billed_at
> )
> ```

---

## 7. Sơ đồ quan hệ cuối cùng

```
dim_campaigns
    │  (campaign_id)
    ├──◄── fct_clicks ────────────── (conversion funnel flags)
    │         │ (rotate_id)
    │         │
    ├──◄── fct_subscriptions ──────── (msisdn, attribution_method)
    │         │ (subscription_id)
    │         │
    └──◄── fct_billing ────────────── (is_first_bill, billing_sequence, amount)

All three ──► mart_daily_performance  (pre-aggregated daily)
```

---

## 8. Trade-offs — Phải nói với recruiter

### Điểm mạnh của thiết kế này

| Điểm mạnh | Giải thích |
|-----------|-----------|
| **Single source of truth** | Mọi operator đều được normalize vào cùng 1 schema |
| **Attribution traceable** | `attribution_method` + `source_transaction_id` → audit trail đầy đủ |
| **Query-friendly** | Denormalize dimension key → slice by operator/service/partner không cần join |
| **Pre-computed flags** | `is_first_bill`, `has_subscription`, `billing_sequence` → dashboard queries nhanh |
| **Extensible grain** | Grain thấp ở fact layer → có thể re-aggregate theo bất kỳ dimension nào sau này |

### Điểm yếu / khi nào design này gặp khó khăn

| Tình huống | Vấn đề | Hướng xử lý |
|-----------|--------|-------------|
| **Thêm operator_D** | Phải viết thêm ETL populate logic. Schema không thay đổi. | Schema đã extensible, chỉ cần thêm ETL job |
| **operator đổi format** | `source_transaction_id` + `attribution_method` giúp isolate ảnh hưởng | Re-run ETL cho operator đó, không ảnh hưởng operator khác |
| **Late-arriving billing data** | `is_first_bill` và `billing_sequence` phải recalculate | Trigger hoặc scheduled recalculation job cho msisdn bị ảnh hưởng |
| **User resubscribes** | Attribution logic phức tạp hơn (2 SUB rows cùng msisdn) | Điều kiện `MAX(subscribed_at) <= billed_at` đã handle |
| **Denormalization inconsistency** | Nếu campaign đổi `partner_id` sau khi đã có fact rows | Campaign attributes thực tế không bao giờ thay đổi sau khi active — đây là safe assumption cho mobile ads |
| **mart stale data** | mart_daily_performance chỉ fresh sau khi ETL chạy | Dùng SLA: mart sẵn sàng trước 8am mỗi ngày; real-time cần query trực tiếp fact tables |

### Khi nào cần thiết kế lại?

- **Real-time reporting** (< 1 minute latency): mart pattern này không đủ nhanh.
  Cần chuyển sang event streaming (Kafka → Flink → materialized view).
- **Cross-currency revenue** (nếu expand ra ngoài GBP): cần thêm bảng
  `dim_exchange_rates` và tính `amount_usd` tại load time.
- **Data volume tăng 100x**: cần partition `fct_billing` và `fct_clicks` theo
  `report_date` để query pruning hiệu quả. DDL hiện tại đã có `report_date` là
  STORED generated column — chỉ cần thêm `PARTITION BY RANGE (report_date)`.

---

## 9. Câu SQL mẫu để verify design

### Subscriptions per day per operator

```sql
SELECT
    report_date,
    operator,
    service_name,
    partner_id,
    COUNT(*)  AS total_subscriptions
FROM fct_subscriptions
WHERE report_date BETWEEN '2026-01-01' AND '2026-01-31'
GROUP BY 1, 2, 3, 4
ORDER BY 1, 2;
```

### Revenue per day per partner

```sql
SELECT
    report_date,
    partner_id,
    service_name,
    SUM(amount)              AS total_revenue,
    COUNT(*)                 AS total_charges,
    SUM(is_first_bill::INT)  AS first_bills
FROM fct_billing
WHERE billing_status = 'SUCCESS'
  AND report_date BETWEEN '2026-01-01' AND '2026-01-31'
GROUP BY 1, 2, 3
ORDER BY 1, total_revenue DESC;
```

### Conversion funnel per campaign

```sql
SELECT
    c.campaign_id,
    c.operator,
    c.service_name,
    COUNT(*)                                                AS total_clicks,
    SUM(has_page_view::INT)                                 AS reached_view,
    SUM(has_entry::INT)                                     AS reached_entry,
    SUM(has_subscription::INT)                              AS subscribed,
    SUM(has_first_bill::INT)                                AS first_billed,
    ROUND(100.0 * SUM(has_subscription::INT) / COUNT(*), 2) AS sub_rate_pct,
    ROUND(100.0 * SUM(has_first_bill::INT)   / COUNT(*), 2) AS bill_rate_pct
FROM fct_clicks c
WHERE report_date BETWEEN '2026-01-01' AND '2026-01-31'
GROUP BY 1, 2, 3
ORDER BY sub_rate_pct DESC;
```

### Daily summary từ mart (cho dashboard)

```sql
SELECT
    report_date,
    operator,
    SUM(total_subscriptions)  AS subscriptions,
    SUM(total_first_bills)    AS first_bills,
    SUM(total_revenue)        AS revenue,
    ROUND(AVG(sub_conversion_rate) * 100, 2) AS avg_sub_conv_pct
FROM mart_daily_performance
WHERE report_date = CURRENT_DATE - 1
GROUP BY 1, 2
ORDER BY 1, 2;
```

---

*End of Part 2 — Data Modeling*
