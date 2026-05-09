

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

