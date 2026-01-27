# Phase 03 – Cloud Data Pipeline (Planned)

## Overview
Phase 03 extends the OLTP & SQL analytics foundation into a **cloud-native data pipeline**, following modern Data Engineering best practices.

This phase demonstrates how transactional data is:
- Extracted incrementally
- Landed in object storage
- Modeled in a cloud data warehouse
- Orchestrated using Airflow
- Prepared for BI consumption

---

## Architecture Overview

PostgreSQL (OLTP)
→ Google Cloud Storage (Raw Data Lake)
→ BigQuery (Staging & Analytics)
→ dbt (Transformations)
→ BI / Reporting

---

## Data Flow

1. **Extract**
   - Incremental extract from PostgreSQL
   - Based on `created_at` / `order_date`

2. **Load**
   - Raw data stored in GCS (Parquet)
   - Partitioned by ingestion date

3. **Warehouse**
   - External tables in BigQuery (raw)
   - Typed staging tables
   - Analytics marts (facts & dimensions)

4. **Transform**
   - dbt handles:
     - Business logic
     - Aggregations
     - Testing & documentation

5. **Orchestration**
   - Airflow schedules & monitors:
     - Extract → Load → Transform
     - Failure handling & retries

---

## Key Engineering Concepts

- Incremental data ingestion
- Data lake → warehouse separation
- Schema-on-read vs schema-on-write
- Partitioning & clustering in BigQuery
- ELT architecture
- Orchestration readiness

---

## Future Enhancements

- Change Data Capture (CDC)
- Late-arriving data handling
- Slowly Changing Dimensions (SCD)
- Cost optimization strategies
- Multi-environment deployment (dev / prod)

---

## Status
🚧 Planned – Architecture & design completed  
Implementation will follow after Phase 02 validation

## 🙌 Author
## 🙌 Name: Quoc Bao
## 🙌 Email: Baoquocnguyen1408@gmail.com