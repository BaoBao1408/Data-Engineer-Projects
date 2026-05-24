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

**What might cause this in a real system:**

1. **Clock skew / out-of-order delivery**: The operator's billing system and subscription system run on different servers with slightly different clocks or different processing queues. Both events happen at roughly the same moment, but the bill notification is processed and delivered to the platform before the subscribe notification due to routing differences.
2. **Async pipeline with different latencies**: Subscribe events may go through a heavier validation path (checking regulatory opt-in rules, consent logging) while bill attempts are processed on a lighter, faster path. The bill fires before the subscription confirmation is written.
3. **Retry without status check**: The billing engine may initiate a charge as soon as the user submits their number, before waiting for the formal subscription acknowledgement. If the charge succeeds first, the bill arrives before the subscribe confirmation.
4. **Batch vs real-time**: If subscribe events are batched hourly but bill events are sent in real-time, late batches would appear after bills even if the subscription logically came first.

The 12–120 second range suggests this is a race condition in near-real-time processing, not a fundamentally broken ordering. A robust pipeline should use **event ordering by msisdn** rather than relying on received_time to determine sequence.
