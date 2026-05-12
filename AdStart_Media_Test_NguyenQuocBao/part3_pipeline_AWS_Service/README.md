# Part 3 — adstart Data Pipeline (AWS Edition)

> **DuckDB → S3 + Glue + Athena** | Real-world AWS data pipeline workflow  
> Mỗi concept đều có giải thích "tại sao" để áp dụng vào công việc thực tế.

---

## Mục lục

1. [Architecture Overview](#1-architecture-overview)
2. [AWS Account Setup từ 0](#2-aws-account-setup-từ-0)
3. [IAM Roles & Security](#3-iam-roles--security)
4. [Cài đặt môi trường local](#4-cài-đặt-môi-trường-local)
5. [Deploy AWS Resources](#5-deploy-aws-resources)
6. [Run Flow End-to-End](#6-run-flow-end-to-end)
7. [S3 Data Layout](#7-s3-data-layout)
8. [Glue + Athena Query Guide](#8-glue--athena-query-guide)
9. [Monitoring & Alerts](#9-monitoring--alerts)
10. [So sánh Local vs AWS](#10-so-sánh-local-vs-aws)
11. [Troubleshooting](#11-troubleshooting)
12. [Chi phí ước tính](#12-chi-phí-ước-tính)

---

## 1. Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        DAILY PIPELINE FLOW                              │
│                                                                         │
│  Operators                 AWS                           BI / Analytics  │
│  ─────────                 ───                           ──────────────  │
│                            ┌──────────────┐                             │
│  operator_A.csv ──────────▶│              │                             │
│  operator_B.csv ──────────▶│  S3 Raw      │                             │
│  operator_C.csv ──────────▶│  Bucket      │──────┐                      │
│  campaigns.csv  ──────────▶│  (CSV files) │      │                      │
│                            └──────────────┘      │                      │
│                                                   │ awswrangler          │
│                                                   ▼                      │
│                            ┌──────────────────────────────┐             │
│                            │  S3 Warehouse Bucket          │             │
│                            │  (Parquet, partitioned)       │             │
│                            │                               │             │
│                            │  raw/          ← Stage 1      │             │
│                            │    raw_operator_a/            │             │
│                            │    raw_operator_b/            │             │
│                            │    raw_operator_c/            │             │
│                            │    raw_campaigns/             │             │
│                            │                               │             │
│                            │  dimensions/   ← Stage 2      │             │
│                            │    dim_campaigns/             │             │
│                            │                               │             │
│                            │  facts/        ← Stage 3      │             │
│                            │    fct_subscriptions/         │◀───────┐    │
│                            │    fct_billing/               │        │    │
│                            │    fct_clicks/                │        │    │
│                            │    fct_unattributed_events/   │        │    │
│                            │                               │        │    │
│                            │  mart/         ← Stage 4      │        │    │
│                            │    mart_daily_performance/    │        │    │
│                            └───────────────────────────────┘        │    │
│                                        │                            │    │
│                                        │ Glue Catalog              │    │
│                                        ▼  (auto-register)          │    │
│                            ┌──────────────────┐                    │    │
│                            │  AWS Glue         │                    │    │
│                            │  Catalog          │──────Athena───────▶│    │
│                            │  adstart_raw      │                    │    │
│                            │  adstart_warehouse│    SQL Queries     │    │
│                            └──────────────────┘    Metabase/Looker  │    │
│                                                                      │    │
│                            ┌──────────────┐                         │    │
│                            │  SNS Alerts  │◀─── Quality Checks ─────┘    │
│                            └──────────────┘                              │
└─────────────────────────────────────────────────────────────────────────┘
```

### Công nghệ stack

| Component    | Local (Dev)   | AWS (Production)      | Tại sao chọn                          |
|-------------|--------------|----------------------|---------------------------------------|
| Storage     | Local CSV     | **S3**               | Durability 99.999999999%, cheap       |
| Warehouse   | DuckDB file   | **S3 Parquet**       | Columnar = fast Athena queries        |
| Catalog     | —             | **AWS Glue Catalog** | Schema registry, Athena cần để query  |
| Query engine| DuckDB        | **AWS Athena**       | Serverless SQL, trả tiền per query    |
| Transform   | pandas        | pandas + awswrangler | Same logic, chỉ đổi I/O layer         |
| Orchestrate | Prefect local | **Prefect**          | Retry, scheduling, monitoring         |
| Alerts      | logs          | **SNS + Email**      | Notification khi pipeline fail        |

---

## 2. AWS Account Setup từ 0

### Bước 1 — Tạo tài khoản AWS

1. Vào **https://aws.amazon.com** → click **"Create an AWS Account"**
2. Điền email, tên account, mật khẩu
3. Chọn plan: **Free Tier** (12 tháng free tier, đủ để practice)
4. Điền thông tin thanh toán (credit card — chỉ charge nếu vượt free tier)
5. Xác minh điện thoại → chọn **"Basic support" (free)**

> **Tip**: Đặt tên account có ý nghĩa, ví dụ: `yourname-learning` hoặc `adstart-dev`

### Bước 2 — Bảo mật Root account (QUAN TRỌNG)

Root account có quyền tuyệt đối — phải khóa ngay sau khi tạo xong.

```
AWS Console → IAM → Dashboard → Security recommendations
```

**Checklist bảo mật root:**

- [x] **Enable MFA cho root account** (bắt buộc)
  - IAM → Security credentials → Multi-factor authentication → Assign MFA
  - Dùng app: Google Authenticator hoặc Authy
  
- [x] **Không tạo Access Keys cho root** — dùng IAM users/roles thay thế
  
- [x] **Set billing alert** (tránh surprise bill)
  - Billing → Budgets → Create budget
  - Đặt $10/month alert để biết sớm nếu có chi phí bất thường

### Bước 3 — Tạo IAM Admin User cho daily use

**Không bao giờ dùng root account để làm việc hàng ngày.**

```
IAM → Users → Create user
```

**Cấu hình:**
```
Username        : adstart-admin
Access type     : ✅ Provide user access to the AWS Management Console
Console password: Custom password (strong, 16+ chars)
MFA             : Enable (bắt buộc)

Permissions:
  Attach policies directly:
  ✅ AdministratorAccess  ← Chỉ dùng cho learning, production cần granular hơn
```

**Tạo Access Keys cho CLI/SDK:**
```
IAM → Users → adstart-admin → Security credentials
→ Create access key → "CLI" use case
→ Download .csv (lưu an toàn, chỉ xem 1 lần)
```

### Bước 4 — Configure AWS CLI

```bash
# Cài AWS CLI
# macOS
brew install awscli

# Linux
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
unzip awscliv2.zip && sudo ./aws/install

# Windows
winget install Amazon.AWSCLI

# Configure với credentials vừa tạo
aws configure --profile adstart-dev
# AWS Access Key ID     : AKIAIOSFODNN7EXAMPLE
# AWS Secret Access Key : wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
# Default region name   : eu-west-1
# Default output format : json

# Verify
aws sts get-caller-identity --profile adstart-dev
# Output:
# {
#     "UserId": "AIDAIOSFODNN7EXAMPLE",
#     "Account": "123456789012",     ← Lưu Account ID này
#     "Arn": "arn:aws:iam::123456789012:user/adstart-admin"
# }
```

---

## 3. IAM Roles & Security

### Nguyên tắc Least Privilege

Pipeline chỉ cần quyền vừa đủ — không cần AdministratorAccess khi chạy.

### Pipeline IAM Role (`adstart-pipeline-role`)

Role này được `setup_aws.py` tạo tự động. Giải thích từng permission:

```json
{
  "Version": "2012-10-17",
  "Statement": [

    // S3: Read raw CSV + Write/Read warehouse Parquet + Write Athena results
    {
      "Sid": "S3PipelineAccess",
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",       // Đọc file từ S3
        "s3:PutObject",       // Ghi file lên S3
        "s3:DeleteObject",    // Xóa partition cũ khi overwrite
        "s3:ListBucket",      // List files (awswrangler cần)
        "s3:GetBucketLocation"// Xác định region của bucket
      ],
      "Resource": [
        "arn:aws:s3:::adstart-raw-*",        // Raw bucket
        "arn:aws:s3:::adstart-raw-*/*",
        "arn:aws:s3:::adstart-warehouse-*",  // Warehouse bucket
        "arn:aws:s3:::adstart-warehouse-*/*",
        "arn:aws:s3:::adstart-athena-results-*",
        "arn:aws:s3:::adstart-athena-results-*/*"
      ]
    },

    // Glue: CRUD operations cho Catalog tables
    // awswrangler.s3.to_parquet() với database= cần những quyền này
    {
      "Sid": "GlueCatalogAccess",
      "Effect": "Allow",
      "Action": [
        "glue:CreateTable", "glue:UpdateTable", "glue:GetTable",
        "glue:CreatePartition", "glue:BatchCreatePartition",
        "glue:GetPartition", "glue:UpdatePartition"
      ],
      "Resource": [
        "arn:aws:glue:*:*:catalog",
        "arn:aws:glue:*:*:database/adstart_*",
        "arn:aws:glue:*:*:table/adstart_*/*"
      ]
    },

    // Athena: Chạy queries + lấy kết quả
    {
      "Sid": "AthenaAccess",
      "Effect": "Allow",
      "Action": [
        "athena:StartQueryExecution",
        "athena:GetQueryExecution",
        "athena:GetQueryResults"
      ],
      "Resource": "*"
    },

    // SNS: Publish alert khi pipeline fail
    {
      "Sid": "SNSPublish",
      "Effect": "Allow",
      "Action": ["sns:Publish"],
      "Resource": "arn:aws:sns:*:*:adstart-*"
    }
  ]
}
```

### Trust Policy — Ai được assume role này?

```json
{
  "Statement": [{
    "Effect": "Allow",
    "Principal": {
      "Service": [
        "ec2.amazonaws.com",         // EC2 instance chạy pipeline
        "ecs-tasks.amazonaws.com",   // ECS container task
        "lambda.amazonaws.com"       // Lambda function trigger
      ]
    },
    "Action": "sts:AssumeRole"
  }]
}
```

### Bảo mật connection từ local machine

```bash
# KHÔNG dùng:
export AWS_ACCESS_KEY_ID=AKIA...      # Hardcode credentials trong shell → nguy hiểm

# KHÔNG dùng:
# Điền credentials vào code source → commit lên git → leak

# NÊN dùng Option 1: Named profile
export AWS_PROFILE=adstart-dev
python -m src.orchestration.pipeline --date 2026-01-15

# NÊN dùng Option 2: Assume role từ profile
# ~/.aws/config
[profile adstart-pipeline]
role_arn = arn:aws:iam::123456789012:role/adstart-pipeline-role
source_profile = adstart-dev
region = eu-west-1

export AWS_PROFILE=adstart-pipeline

# NÊN dùng Option 3: Instance Profile (EC2) — tự động, không cần config
# IAM → EC2 → Attach role → adstart-pipeline-role
# Code chạy trên EC2 tự lấy credentials từ metadata service
```

### .gitignore (bắt buộc)

```gitignore
# KHÔNG BAO GIỜ commit những file này
.env
.env.*
!.env.example
*.pem
*.key
aws-credentials.csv
data/
logs/
warehouse/
```

---

## 4. Cài đặt môi trường local

### Requirements

- Python 3.11+
- Git
- AWS CLI v2

### Setup

```bash
# 1. Clone project
git clone <repo-url>
cd part3_pipeline_aws

# 2. Tạo virtual environment
python3 -m venv .venv
source .venv/bin/activate          # Linux/macOS
# .venv\Scripts\activate           # Windows

# 3. Cài dependencies
pip install -r requirements_aws.txt

# 4. Copy và điền .env
cp .env.example .env
# Mở .env và điền:
#   PIPELINE_ENV=local   ← Bắt đầu với local để test trước
#   (AWS settings để trống khi local mode)

# 5. Test local mode (không cần AWS)
make run-local
# hoặc
PIPELINE_ENV=local python -m src.orchestration.pipeline --date 2026-01-15

# 6. Chạy tests
make test-unit
```

---

## 5. Deploy AWS Resources

### Lần đầu setup (chạy 1 lần)

```bash
# Bước 1: Lấy Account ID
export ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
echo "Account ID: $ACCOUNT_ID"

# Bước 2: Preview trước (dry run — không tạo gì cả)
make setup-aws-dry ACCOUNT_ID=$ACCOUNT_ID REGION=eu-west-1

# Output:
# [DRY-RUN] Would create s3://adstart-raw-123456789012
# [DRY-RUN] Would create s3://adstart-warehouse-123456789012
# [DRY-RUN] Would create Glue database: adstart_raw
# [DRY-RUN] Would create IAM role: adstart-pipeline-role
# ...

# Bước 3: Tạo resources thật (sau khi đã review dry run)
make setup-aws ACCOUNT_ID=$ACCOUNT_ID REGION=eu-west-1

# Bước 4: .env được tạo tự động — kiểm tra lại
cat .env

# Bước 5: Switch sang AWS mode
# Sửa .env: PIPELINE_ENV=aws
```

### Resources được tạo

```
S3 Buckets:
  adstart-raw-123456789012          ← Raw CSV từ operators
  adstart-warehouse-123456789012    ← Parquet warehouse
  adstart-athena-results-123456789012 ← Athena query results

Glue Databases:
  adstart_raw          ← Raw tables (Parquet)
  adstart_warehouse    ← Facts + Mart tables

IAM:
  Role: adstart-pipeline-role
  Policy: adstart-pipeline-policy (inline)

SNS:
  Topic: adstart-pipeline-alerts
```

---

## 6. Run Flow End-to-End

### Toàn bộ flow từ 0 đến end

```
┌─────────────────────────────────────────────────────────┐
│  STEP-BY-STEP GUIDE: 0 → Production Pipeline on AWS     │
└─────────────────────────────────────────────────────────┘

NGÀY 1: AWS Account + Setup
═══════════════════════════

Step 0: Tạo AWS account + IAM admin user + MFA
  → https://aws.amazon.com → Create Account

Step 1: Configure AWS CLI
  → aws configure --profile adstart-dev

Step 2: Clone project + install deps
  → git clone ... && pip install -r requirements_aws.txt

Step 3: Setup AWS resources
  → make setup-aws ACCOUNT_ID=123456789012

Step 4: Upload sample data
  → make upload-data RUN_DATE=2026-01-15

Step 5: Chạy pipeline lần đầu
  → make run RUN_DATE=2026-01-15

Step 6: Query kết quả trong Athena console

NGÀY 2+: Daily operations
════════════════════════

# Chạy thủ công
make run                              # Default: hôm qua
make run RUN_DATE=2026-01-20         # Ngày cụ thể

# Backfill 7 ngày
make backfill DAYS=7

# Monitor
make logs                             # CloudWatch logs
```

### Chi tiết từng bước

#### Bước 1 — Verify environment

```bash
# Kiểm tra AWS credentials
aws sts get-caller-identity
# {
#   "Account": "123456789012",
#   "Arn": "arn:aws:iam::123456789012:user/adstart-admin"
# }

# Kiểm tra buckets đã tồn tại
aws s3 ls | grep adstart
# 2026-01-15 10:00:00 adstart-raw-123456789012
# 2026-01-15 10:00:00 adstart-warehouse-123456789012
# 2026-01-15 10:00:00 adstart-athena-results-123456789012

# Kiểm tra .env
cat .env | grep PIPELINE_ENV
# PIPELINE_ENV=aws
```

#### Bước 2 — Upload sample data lên S3

```bash
# Upload tất cả files cho ngày 2026-01-15
make upload-data RUN_DATE=2026-01-15

# Output:
# Uploading to s3://adstart-raw-123456789012/ for date=2026-01-15
#   ✓ Uploaded operator_A.csv → s3://adstart-raw-123456789012/operator_a/date=2026-01-15/data.csv
#   ✓ Uploaded operator_B.csv → s3://adstart-raw-123456789012/operator_b/date=2026-01-15/data.csv
#   ✓ Uploaded operator_C.csv → s3://adstart-raw-123456789012/operator_c/date=2026-01-15/data.csv
#   ✓ Uploaded campaigns.csv  → s3://adstart-raw-123456789012/static/campaigns.csv
#   ✓ Uploaded clicks.csv     → s3://adstart-raw-123456789012/static/clicks.csv
#   ✓ Uploaded tracking_codes.csv
#   ✓ Uploaded page_events.csv

# Verify trên S3
make ls-raw RUN_DATE=2026-01-15
```

#### Bước 3 — Run pipeline

```bash
make run RUN_DATE=2026-01-15

# === Pipeline output ===
# ════════════════════════════════════════
#  Pipeline starting: run_date = 2026-01-15
#  Environment      : aws
#  Raw bucket       : s3://adstart-raw-123456789012/
#  Warehouse bucket : s3://adstart-warehouse-123456789012/
#  Athena DB (raw)  : adstart_raw
#  Athena DB (wh)   : adstart_warehouse
# ════════════════════════════════════════
#
# [ Stage 1/5 ] Ingesting raw files ...
#   Reading s3://adstart-raw-123456789012/operator_a/date=2026-01-15/data.csv
#   [raw_operator_a] Loaded 1,247 rows — checks passed.
#   Written 1,247 rows → s3://adstart-warehouse-123456789012/raw/raw_operator_a/
#   ...
#
# [ Stage 2/5 ] Building dimension tables ...
#   [dim_campaigns] 24 rows total.
#
# [ Stage 3/5 ] Building fact tables ...
#   [fct_subscriptions] 892 rows inserted for 2026-01-15.
#   [fct_billing] Tổng 445 rows written for 2026-01-15.
#   [fct_clicks] 3,218 rows written for 2026-01-15.
#
# [ Stage 4/5 ] Building mart tables ...
#   [mart_daily_performance] 12 campaign rows for 2026-01-15.
#
# [ Stage 5/5 ] Running quality checks ...
#   ✓ Quality: 8/8 checks passed
#
# ════════════════════════════════════════
#  Pipeline COMPLETED: 2026-01-15 ✓
# ════════════════════════════════════════
```

#### Bước 4 — Verify trên S3 + Athena

```bash
# List files trên S3
make ls-warehouse RUN_DATE=2026-01-15
# s3://adstart-warehouse-123456789012/facts/fct_subscriptions/report_date=2026-01-15/...parquet
# s3://adstart-warehouse-123456789012/mart/mart_daily_performance/report_date=2026-01-15/...parquet

# Query Athena (AWS Console hoặc CLI)
aws athena start-query-execution \
  --query-string "SELECT campaign_id, total_clicks, total_subscriptions, total_revenue
                  FROM mart_daily_performance
                  WHERE report_date = '2026-01-15'
                  ORDER BY total_revenue DESC" \
  --query-execution-context Database=adstart_warehouse \
  --result-configuration OutputLocation=s3://adstart-athena-results-123456789012/
```

#### Bước 5 — Kiểm tra Athena Console

```
AWS Console → Athena → Query editor
  Database: adstart_warehouse

  SELECT
    report_date,
    campaign_id,
    operator,
    total_clicks,
    total_subscriptions,
    sub_conversion_rate,
    total_revenue
  FROM mart_daily_performance
  WHERE report_date = '2026-01-15'
  ORDER BY total_revenue DESC;
```

---

## 7. S3 Data Layout

```
s3://adstart-raw-123456789012/
│
├── operator_a/
│   ├── date=2026-01-14/
│   │   └── data.csv
│   └── date=2026-01-15/
│       └── data.csv         ← Hive partition format (Athena tự detect)
│
├── operator_b/
│   └── date=2026-01-15/
│       └── data.csv
│
├── operator_c/
│   └── date=2026-01-15/
│       └── data.csv
│
└── static/
    ├── campaigns.csv
    ├── clicks.csv
    ├── tracking_codes.csv
    └── page_events.csv

s3://adstart-warehouse-123456789012/
│
├── raw/                              ← Layer 0: Raw Parquet (validated)
│   ├── raw_operator_a/
│   │   └── _loaded_date=2026-01-15/
│   │       └── part-0000.parquet
│   ├── raw_operator_b/
│   ├── raw_operator_c/
│   ├── raw_campaigns/
│   └── raw_clicks/
│
├── dimensions/                       ← Layer 1: Dimension tables
│   └── dim_campaigns/
│       └── part-0000.parquet         ← No date partition (static)
│
├── facts/                            ← Layer 2: Fact tables
│   ├── fct_subscriptions/
│   │   └── report_date=2026-01-15/
│   │       └── part-0000.parquet
│   ├── fct_billing/
│   ├── fct_clicks/
│   └── fct_unattributed_events/
│
└── mart/                             ← Layer 3: Aggregated mart
    └── mart_daily_performance/
        └── report_date=2026-01-15/
            └── part-0000.parquet
```

---

## 8. Glue + Athena Query Guide

### Xem tables trong Athena Console

```sql
-- List tất cả tables trong warehouse database
SHOW TABLES IN adstart_warehouse;

-- Xem schema của fct_subscriptions
DESCRIBE adstart_warehouse.fct_subscriptions;

-- Query mart
SELECT
    report_date,
    campaign_id,
    operator,
    total_clicks,
    total_subscriptions,
    ROUND(sub_conversion_rate * 100, 2) AS sub_cvr_pct,
    total_revenue
FROM adstart_warehouse.mart_daily_performance
WHERE report_date = '2026-01-15'
ORDER BY total_revenue DESC;

-- Revenue theo operator theo tuần
SELECT
    DATE_TRUNC('week', CAST(report_date AS DATE)) AS week_start,
    operator,
    SUM(total_revenue) AS weekly_revenue,
    SUM(total_subscriptions) AS weekly_subs
FROM adstart_warehouse.mart_daily_performance
WHERE report_date >= '2026-01-01'
GROUP BY 1, 2
ORDER BY 1, 3 DESC;

-- Attribution rate của operator_C
SELECT
    e.report_date,
    COUNT(*) AS unattributed_events,
    COUNT(*) * 100.0 / NULLIF(r.total_delivered, 0) AS unattr_rate_pct
FROM adstart_warehouse.fct_unattributed_events e
JOIN (
    SELECT _loaded_date, COUNT(*) AS total_delivered
    FROM adstart_raw.raw_operator_c
    WHERE delivery_status = 'DELIVERED'
    GROUP BY _loaded_date
) r ON r._loaded_date = e.report_date
WHERE e.report_date = '2026-01-15'
GROUP BY 1, r.total_delivered;
```

### Tối ưu Athena queries (tiết kiệm chi phí)

```sql
-- ✅ LUÔN filter theo partition column trước
SELECT * FROM fct_subscriptions
WHERE report_date = '2026-01-15'    -- Filter partition → chỉ scan 1 file

-- ❌ Tránh full table scan
SELECT * FROM fct_subscriptions     -- Scan TẤT CẢ partitions = tốn tiền
WHERE YEAR(subscribed_at) = 2026

-- ✅ SELECT chỉ cột cần thiết (Parquet columnar = chỉ scan cột được select)
SELECT subscription_id, msisdn, campaign_id
FROM fct_subscriptions
WHERE report_date = '2026-01-15'

-- ❌ Tránh SELECT *
SELECT * FROM fct_subscriptions     -- Scan tất cả cột
```

---

## 9. Monitoring & Alerts

### SNS Email Alerts

Pipeline tự động gửi alert khi quality check fail:

```json
{
  "pipeline": "adstart-data-pipeline",
  "run_date": "2026-01-15",
  "status": "QUALITY_FAILURE",
  "failures": [
    {
      "check": "operator_c_attribution_rate",
      "failing_rows": 1,
      "details": "Attribution rate: 65.2% (threshold 80%)"
    }
  ]
}
```

### CloudWatch Logs

```bash
# Stream logs realtime
make logs

# Filter errors
aws logs filter-log-events \
  --log-group-name /aws/adstart-pipeline/pipeline \
  --filter-pattern "ERROR"
```

---

## 10. So sánh Local vs AWS

| Aspect         | Local (DuckDB)          | AWS (S3+Athena)              |
|---------------|------------------------|------------------------------|
| Startup cost  | 0                       | ~5 phút setup               |
| Per-run cost  | 0                       | ~$0.01-0.05/day             |
| Data size     | Giới hạn RAM/disk       | Unlimited                   |
| Query speed   | Nhanh (in-process)      | 1-10s latency               |
| Concurrency   | Single process          | Nhiều queries song song     |
| Sharing data  | Phải copy file          | Share Athena query          |
| Durability    | Local disk              | 99.999999999%               |
| Dev workflow  | Instant feedback        | Upload → wait               |

**Khi nào dùng Local mode:**
- Development + unit tests
- Prototype transformation logic
- CI/CD pipeline checks

**Khi nào dùng AWS mode:**
- Data > 1GB
- Multiple analysts cần query cùng lúc
- Production pipeline với scheduling
- Audit trail + data versioning cần thiết

---

## 11. Troubleshooting

### `NoCredentialsError`
```bash
# Nguyên nhân: AWS credentials chưa config
aws configure --profile adstart-dev
# Hoặc: kiểm tra .env có AWS_PROFILE chưa
```

### `NoSuchBucket`
```bash
# Nguyên nhân: Chưa chạy setup_aws.py
make setup-aws ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
```

### `Glue table không tồn tại` trong Athena
```bash
# awswrangler tự tạo table khi write_table() chạy lần đầu
# Nếu vẫn lỗi, chạy lại pipeline và check logs
make run RUN_DATE=2026-01-15
```

### `AccessDenied` khi write S3
```bash
# Kiểm tra IAM policy có đủ quyền không
aws iam simulate-principal-policy \
  --policy-source-arn arn:aws:iam::ACCOUNT_ID:role/adstart-pipeline-role \
  --action-names s3:PutObject \
  --resource-arns arn:aws:s3:::adstart-warehouse-ACCOUNT_ID/*
```

### Athena query fail `HIVE_PARTITION_SCHEMA_MISMATCH`
```bash
# Xảy ra khi schema thay đổi sau khi table đã tồn tại
# Chạy MSCK REPAIR TABLE trong Athena console:
MSCK REPAIR TABLE adstart_warehouse.fct_subscriptions;
```

---

## 12. Chi phí ước tính

Với volume sample data (~1-5MB/ngày):

| Service     | Usage                    | Cost/month  |
|------------|--------------------------|-------------|
| S3 Storage | ~150MB/tháng             | ~$0.003     |
| S3 Requests| ~10K PUT/GET requests    | ~$0.05      |
| Athena     | ~10 queries × 5MB/query  | ~$0.002     |
| Glue Catalog| Miễn phí đến 1M objects | $0.00       |
| SNS        | <1K messages/tháng       | $0.00       |
| **Total**  |                          | **< $0.10** |

> **Free Tier** (12 tháng đầu): S3 5GB + 20K GET requests, đủ để practice thoải mái.

---

## Project Structure

```
part3_pipeline_aws/
├── .env.example                    ← Copy → .env, điền values
├── .gitignore
├── Makefile                        ← Tất cả commands
├── README.md                       ← File này
├── requirements_aws.txt
│
├── config/
│   ├── base.py                     ← Settings (LOCAL/AWS switch)
│   └── logging_conf.py
│
├── src/
│   ├── ingest/
│   │   ├── loaders.py              ← S3 CSV → validate → S3 Parquet
│   │   └── validator.py            ← Null rate + row count checks
│   │
│   ├── transformations/
│   │   ├── dimensions.py           ← dim_campaigns (SCD-0)
│   │   ├── subscriptions.py        ← fct_subscriptions (3-operator attribution)
│   │   └── billing_clicks_mart.py  ← fct_billing, fct_clicks, mart
│   │
│   ├── orchestration/
│   │   ├── pipeline.py             ← Prefect flow (main entrypoint)
│   │   └── quality.py             ← Post-build assertions + SNS alerts
│   │
│   └── utils/
│       ├── aws_warehouse.py        ← AWSWarehouse (S3+Athena facade)
│       └── s3_utils.py             ← S3 helper functions
│
├── infrastructure/
│   ├── setup_aws.py                ← Tạo S3, Glue, IAM, SNS
│   ├── upload_sample_data.py       ← Upload CSV lên S3
│   └── teardown_aws.py             ← Cleanup (dev only)
│
└── tests/
    ├── unit/
    │   └── test_transformations.py ← Unit tests (no AWS needed)
    └── integration/
        └── test_pipeline_aws.py    ← Integration tests (moto mock)
```