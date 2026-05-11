
---

# Part 3 Pipeline — Full Code Run Flow

## 1. Open Project

```bash
cd part_pipeline_production_ready
```

---

# 2. Create Virtual Environment (uv)

## Install uv first (if not installed)

### Windows PowerShell

```powershell
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
```

### Verify

```bash
uv --version
```

---

# 3. Create venv

```bash
uv venv
```

---

# 4. Activate venv

## Windows PowerShell

```powershell
.venv\Scripts\activate
```

## Linux / Mac

```bash
source .venv/bin/activate
```

---

# 5. Install Dependencies

```bash
uv pip install -r requirements.txt
```

---

# 6. Verify Environment

```bash
python --version
```

```bash
pip list
```

Verify important packages exist:

- duckdb
- pandas
- pyarrow
- pytest
- prefect

---

# 7. Verify Raw Data Exists

Check:

```text
data/raw/
```

Expected files:

```text
campaigns.csv
clicks.csv
operator_A.csv
operator_B.csv
operator_C.csv
page_events.csv
tracking_codes.csv
```

---
# INSTALL dependencies
uv pip install -e ".[dev]"
---

## 2. TEST RUN PREPIPELINE

```bash
pytest tests/ -v
```

EXPECTED RESULT: ALL tests PASSED.

# 8. Run Pipeline Locally

```bash
python src/orchestration/pipeline.py --date 2026-01-15
```

---

# 9. Expected Pipeline Flow

Pipeline executes:

```text
Raw CSV Files
    ↓
Ingestion Layer
    ↓
raw_* tables
    ↓
Transformation Layer
    ↓
dim_* tables
    ↓
fct_* tables
    ↓
mart_daily_performance
    ↓
warehouse.duckdb
```

---

# 10. Expected Warehouse Output

Check:

```text
data/warehouse/
```

Expected:

```text
warehouse.duckdb
```

---

# 11. Open DuckDB Warehouse

## Option A — VSCode Extension

Install extension:

```text
DuckDB SQL Tools
```

Then:

```text
Ctrl + Shift + P
→ DuckDB: Open Database
→ select warehouse.duckdb
```

---

# 12. Query Warehouse

## Show Tables

```sql
SHOW TABLES;
```

Expected tables:

```text
raw_campaigns
raw_clicks
raw_operator_a
raw_operator_b
raw_operator_c
raw_page_events
raw_tracking_codes

dim_campaigns

fct_clicks
fct_subscriptions
fct_billing

mart_daily_performance

pipeline_runs
```

---

# 13. Example BI Query

```sql
SELECT *
FROM mart_daily_performance;
```

---

# 14. Example Revenue Query

```sql
SELECT
    operator,
    SUM(amount) AS revenue
FROM fct_billing
GROUP BY 1;
```

---

# 15. Run Tests

```bash
pytest tests/ -v
```

---

# 16. Docker Build

## Verify Docker Installed

```bash
docker --version
```

```bash
docker compose version
```

---

# 17. Build Docker Image

```bash
docker compose build
```

Expected:

```text
Successfully tagged adstart_pipeline
```

---

# 18. Run Docker Container

```bash
docker compose up
```

---

# 19. Expected Docker Flow

Container executes:

```text
pytest
    ↓
pipeline.py
    ↓
DuckDB warehouse build
```

---

# 20. Check Running Containers

```bash
docker ps
```

Expected:

```text
adstart_pipeline
```

---

# 21. View Docker Logs

```bash
docker logs adstart_pipeline
```

---

# 22. Enter Running Container

```bash
docker exec -it adstart_pipeline bash
```

---

# 23. Query DuckDB Inside Container

```bash
python
```

```python
import duckdb

conn = duckdb.connect(
    "data/warehouse/warehouse.duckdb"
)

conn.execute("SHOW TABLES").fetchall()
```

---

# 24. Stop Containers

```bash
docker compose down
```

---

# 25. Rebuild After Code Changes

```bash
docker compose up --build
```

---

# 26. Warehouse Architecture Summary

## Raw Layer

```text
raw_*
```

Stores:
- original operator data
- ingestion staging
- replay/debug lineage

---

## Dimension Layer

```text
dim_campaigns
```

Stores:
- campaign metadata
- partner
- operator
- service

---

## Fact Layer

```text
fct_clicks
fct_subscriptions
fct_billing
```

Stores:
- business events
- acquisition
- monetization
- funnel activity

---

## Mart Layer

```text
mart_daily_performance
```

Stores:
- BI-ready aggregated metrics
- daily revenue
- conversion rates
- subscriptions
- first bills

---

# 27. Final End-to-End Flow

```text
7 CSV source files
        ↓
Dockerized ETL Pipeline
        ↓
DuckDB Warehouse
        ↓
Dimension + Fact Modeling
        ↓
Business Mart
        ↓
BI / Reporting Ready
```

---

# 28. Production Engineering Concepts Demonstrated

This project demonstrates:

- ELT warehouse architecture
- layered data modeling
- analytical fact/dimension design
- DuckDB warehouse engineering
- SQL transformations
- orchestration pipelines
- automated testing
- Docker containerization
- reproducible runtime environments
- BI-ready marts
- data lineage and replayability

