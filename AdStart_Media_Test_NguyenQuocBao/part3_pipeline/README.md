# Part 3 — ETL Pipeline

Pipeline xử lý daily operator files → unified analytical tables → mart.

## Setup

```bash
pip install -r requirements.txt
```

## Chạy pipeline

```bash
# Xử lý ngày hôm qua (default)
python pipeline.py

# Xử lý ngày cụ thể
python pipeline.py --date 2026-01-15

# Prefect UI (optional — xem flow visualization)
prefect server start   # terminal khác
python pipeline.py
# Mở http://localhost:4200
```

## Cấu trúc file

```
part3_pipeline/
├── config.py       — paths, thresholds
├── schema.sql      — DDL tất cả tables (idempotent)
├── ingest.py       — đọc CSV files → staging tables
├── transform.py    — staging → fact tables → mart
├── pipeline.py     — Prefect flow (orchestration)
├── aws_notes.md    — map từng component sang AWS
└── requirements.txt
```

## Xem kết quả sau khi chạy

```python
import duckdb
conn = duckdb.connect("warehouse.duckdb")

# Daily summary
conn.execute("SELECT * FROM mart_daily_performance ORDER BY report_date DESC").df()

# Pipeline run history
conn.execute("SELECT * FROM pipeline_runs ORDER BY started_at DESC").df()

# Attribution breakdown
conn.execute("""
    SELECT attribution_method, COUNT(*) as count
    FROM fct_subscriptions
    GROUP BY 1
""").df()
```

## Handle failure cases

| Tình huống | Behavior |
|---|---|
| File không có | Task retry 3 lần × 60s → mark failed, log error |
| Pipeline chạy 2 lần | DELETE WHERE report_date → INSERT lại (idempotent) |
| Step giữa chừng fail | Prefect mark step failed, các step sau không chạy |
| Operator C tracking miss | Log warning, insert với campaign_id = NULL (không crash) |
| Quality check fail | Raise error, ghi vào pipeline_runs, không cập nhật mart |
