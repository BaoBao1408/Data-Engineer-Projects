# AdStart Pipeline — Chạy từ Zero đến Warehouse

## Yêu cầu
- Python ≥ 3.11
- [uv](https://github.com/astral-sh/uv) (cài 1 lần: `curl -LsSf https://astral.sh/uv/install.sh | sh`)
- CSV files trong `data/raw/` (campaigns.csv, clicks.csv, operator_A.csv, operator_B.csv, operator_C.csv, page_events.csv, tracking_codes.csv)

---

## 1. Setup môi trường (chỉ cần làm 1 lần)

```bash
# Tạo venv với uv
uv venvunv

# Activate
source .venv/bin/activate          # Mac / Linux
# .venv\Scripts\activate           # Windows

# Cài dependencies
uv pip install -e ".[dev]"
```

---

## 2. Chạy tests (kiểm tra logic trước khi chạy pipeline)

```bash
pytest tests/ -v
```

Kết quả mong đợi: tất cả tests PASSED.

---

## 3. Chạy pipeline

```bash
# Chạy cho ngày cụ thể (khuyến nghị khi test lần đầu)
python src/orchestration/pipeline.py --date 2026-01-15

# Chạy cho hôm qua (production default)
python src/orchestration/pipeline.py
```

Warehouse được ghi ra: `data/warehouse/warehouse.duckdb`

---

## 4. Xem kết quả trong warehouse

```bash
python - <<'EOF'
import duckdb
conn = duckdb.connect("data/warehouse/warehouse.duckdb")

print("\n=== mart_daily_performance ===")
rows = conn.execute("""
    SELECT report_date, operator, service_name,
           total_clicks, total_subscriptions, total_first_bills,
           total_renewals, ROUND(total_revenue,2) AS revenue_gbp,
           ROUND(sub_conversion_rate*100,2) AS sub_cvr_pct
    FROM mart_daily_performance
    ORDER BY report_date DESC, total_revenue DESC
    LIMIT 20
""").fetchdf()
print(rows.to_string(index=False))
conn.close()
EOF
```

---

## 5. Chạy lại cùng ngày (idempotent — an toàn)

```bash
python src/orchestration/pipeline.py --date 2026-01-15
# Không tạo duplicate — DELETE + INSERT per run_date
```

---

## Cấu trúc folder

```
part3_pipeline_production_ready/
├── config/
│   ├── base.py              # tất cả settings tập trung
│   └── logging_conf.py      # logging setup
├── src/
│   ├── ingest/
│   │   ├── loaders.py       # đọc CSV → staging tables
│   │   └── validator.py     # null-rate + row-count checks
│   ├── transformations/
│   │   ├── dimensions.py    # dim_campaigns
│   │   ├── subscriptions.py # fct_subscriptions (3 operators)
│   │   └── billing_clicks_mart.py  # fct_billing, fct_clicks, mart
│   ├── orchestration/
│   │   ├── pipeline.py      # Prefect flow — entry point
│   │   └── quality.py       # final quality gate
│   └── utils/
│       └── db.py            # DuckDB connection factory
├── tests/
│   ├── fixtures/conftest.py # shared fixtures
│   ├── unit/test_dimensions.py
│   └── integration/test_pipeline.py
├── sql/                     # reference SQL queries
├── data/
│   ├── raw/                 # source CSVs (input)
│   └── warehouse/           # warehouse.duckdb (output)
├── schema.sql               # DDL — tất cả CREATE TABLE IF NOT EXISTS
├── pyproject.toml           # uv / pip project file
└── RUN.md                   # file này
```

---

## AWS Migration (khi cần scale)

| Local | AWS |
|-------|-----|
| `data/raw/*.csv` | S3 `s3://adstart-raw/operator_X/date=YYYY-MM-DD/` |
| `data/warehouse/warehouse.duckdb` | Redshift hoặc Athena + S3 Parquet |
| Prefect `@flow` | Step Functions state machine |
| Prefect `@task` | Glue job hoặc Lambda |
| `logs/` | CloudWatch Logs |
| Quality alerts | SNS topic |

Swap points đã được đánh dấu bằng comment `# AWS:` trong code.
