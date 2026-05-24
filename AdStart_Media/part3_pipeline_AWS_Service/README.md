# Part 3 — adstart Data Pipeline (AWS Edition)

> **DuckDB → S3 + Glue + Athena** | Real-world AWS data pipeline workflow  
> Every concept includes a "why" explanation for direct application to real work.

---

## Table of Contents

1. [Architecture Overview](#1-architecture-overview)
2. [AWS Account Setup from Scratch](#2-aws-account-setup-from-scratch)
3. [IAM Roles & Security](#3-iam-roles--security)
4. [Local Environment Setup](#4-local-environment-setup)
5. [Deploy AWS Resources](#5-deploy-aws-resources)
6. [Run Flow End-to-End](#6-run-flow-end-to-end)
7. [S3 Data Layout](#7-s3-data-layout)
8. [Glue + Athena Query Guide](#8-glue--athena-query-guide)
9. [Monitoring & Alerts](#9-monitoring--alerts)
10. [Local vs AWS Comparison](#10-local-vs-aws-comparison)
11. [Troubleshooting](#11-troubleshooting)
12. [Estimated Costs](#12-estimated-costs)

---

## 1. Architecture Overview
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

### Technology Stack

| Component    | Local (Dev)   | AWS (Production)      | Why this choice                           |
|-------------|--------------|----------------------|-------------------------------------------|
| Storage     | Local CSV     | **S3**               | 99.999999999% durability, cheap           |
| Warehouse   | DuckDB file   | **S3 Parquet**       | Columnar = fast Athena queries            |
| Catalog     | —             | **AWS Glue Catalog** | Schema registry, required by Athena       |
| Query engine| DuckDB        | **AWS Athena**       | Serverless SQL, pay per query             |
| Transform   | pandas        | pandas + awswrangler | Same logic, only the I/O layer changes    |
| Orchestrate | Prefect local | **Prefect**          | Retry, scheduling, monitoring             |
| Alerts      | logs          | **SNS + Email**      | Notifications when pipeline fails         |

---

## 2. AWS Account Setup from Scratch

### Step 1 — Create an AWS Account

1. Go to **https://aws.amazon.com** → click **"Create an AWS Account"**
2. Enter your email, account name, and password
3. Choose plan: **Free Tier** (12 months free tier, sufficient for practice)
4. Enter payment details (credit card — only charged if free tier is exceeded)
5. Verify phone number → select **"Basic support" (free)**

> **Tip**: Give the account a meaningful name, e.g. `yourname-learning` or `adstart-dev`

### Step 2 — Secure the Root Account (IMPORTANT)

The root account has absolute permissions — lock it down immediately after creation.
AWS Console → IAM → Dashboard → Security recommendations

**Root account security checklist:**

- [x] **Enable MFA for the root account** (mandatory)
  - IAM → Security credentials → Multi-factor authentication → Assign MFA
  - Recommended apps: Google Authenticator or Authy
  
- [x] **Do not create Access Keys for root** — use IAM users/roles instead
  
- [x] **Set a billing alert** (avoid surprise bills)
  - Billing → Budgets → Create budget
  - Set a $10/month alert to catch unexpected charges early

### Step 3 — Create an IAM Admin User for Daily Use

**Never use the root account for day-to-day work.**
IAM → Users → Create user

**Configuration:**
Username        : adstart-admin
Access type     : ✅ Provide user access to the AWS Management Console
Console password: Custom password (strong, 16+ chars)
MFA             : Enable (mandatory)
Permissions:
Attach policies directly:
✅ AdministratorAccess  ← Use only for learning; production needs more granular permissions

**Create Access Keys for CLI/SDK:**
IAM → Users → adstart-admin → Security credentials
→ Create access key → "CLI" use case
→ Download .csv (store safely — only visible once)

### Step 4 — Configure AWS CLI

```bash
# Install AWS CLI
# macOS
brew install awscli

# Linux
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
unzip awscliv2.zip && sudo ./aws/install

# Windows
winget install Amazon.AWSCLI

# Configure with the credentials just created
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
#     "Account": "123456789012",     ← Save this Account ID
#     "Arn": "arn:aws:iam::123456789012:user/adstart-admin"
# }
```

---

## 3. IAM Roles & Security

### Principle of Least Privilege

The pipeline only needs the permissions it requires — AdministratorAccess is not needed at runtime.

### Pipeline IAM Role (`adstart-pipeline-role`)

This role is created automatically by `setup_aws.py`. Each permission is explained below:

```json
{
  "Version": "2012-10-17",
  "Statement": [

    // S3: Read raw CSV + Write/Read warehouse Parquet + Write Athena results
    {
      "Sid": "S3PipelineAccess",
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",        // Read files from S3
        "s3:PutObject",        // Write files to S3
        "s3:DeleteObject",     // Delete old partition when overwriting
        "s3:ListBucket",       // List files (required by awswrangler)
        "s3:GetBucketLocation" // Identify the bucket's region
      ],
      "Resource": [
        "arn:aws:s3:::adstart-raw-*",
        "arn:aws:s3:::adstart-raw-*/*",
        "arn:aws:s3:::adstart-warehouse-*",
        "arn:aws:s3:::adstart-warehouse-*/*",
        "arn:aws:s3:::adstart-athena-results-*",
        "arn:aws:s3:::adstart-athena-results-*/*"
      ]
    },

    // Glue: CRUD operations on Catalog tables
    // awswrangler.s3.to_parquet() with database= requires these permissions
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

    // Athena: Run queries + retrieve results
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

    // SNS: Publish alert when pipeline fails
    {
      "Sid": "SNSPublish",
      "Effect": "Allow",
      "Action": ["sns:Publish"],
      "Resource": "arn:aws:sns:*:*:adstart-*"
    }
  ]
}
```

### Trust Policy — Who can assume this role?

```json
{
  "Statement": [{
    "Effect": "Allow",
    "Principal": {
      "Service": [
        "ec2.amazonaws.com",         // EC2 instance running the pipeline
        "ecs-tasks.amazonaws.com",   // ECS container task
        "lambda.amazonaws.com"       // Lambda function trigger
      ]
    },
    "Action": "sts:AssumeRole"
  }]
}
```

### Securing the connection from a local machine

```bash
# DO NOT use:
export AWS_ACCESS_KEY_ID=AKIA...      # Hardcoded credentials in shell → dangerous

# DO NOT use:
# Credentials in source code → committed to git → leaked

# DO use — Option 1: Named profile
export AWS_PROFILE=adstart-dev
python -m src.orchestration.pipeline --date 2026-01-15

# DO use — Option 2: Assume role from profile
# ~/.aws/config
[profile adstart-pipeline]
role_arn = arn:aws:iam::123456789012:role/adstart-pipeline-role
source_profile = adstart-dev
region = eu-west-1

export AWS_PROFILE=adstart-pipeline

# DO use — Option 3: Instance Profile (EC2) — automatic, no config needed
# IAM → EC2 → Attach role → adstart-pipeline-role
# Code running on EC2 automatically retrieves credentials from the metadata service
```

### .gitignore (mandatory)

```gitignore
# NEVER commit these files
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

## 4. Local Environment Setup

### Requirements

- Python 3.11+
- Git
- AWS CLI v2

### Setup

```bash
# 1. Clone the project
git clone <repo-url>
cd part3_pipeline_aws

# 2. Create a virtual environment
python3 -m venv .venv
source .venv/bin/activate          # Linux/macOS
# .venv\Scripts\activate           # Windows

# 3. Install dependencies
pip install -r requirements_aws.txt

# 4. Copy and fill in .env
cp .env.example .env
# Open .env and set:
#   PIPELINE_ENV=local   ← Start with local to test first
#   (Leave AWS settings blank for local mode)

# 5. Test local mode (no AWS required)
make run-local
# or
PIPELINE_ENV=local python -m src.orchestration.pipeline --date 2026-01-15

# 6. Run tests
make test-unit
```

---

## 5. Deploy AWS Resources

### First-time setup (run once)

```bash
# Step 1: Get Account ID
export ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
echo "Account ID: $ACCOUNT_ID"

# Step 2: Preview first (dry run — creates nothing)
make setup-aws-dry ACCOUNT_ID=$ACCOUNT_ID REGION=eu-west-1

# Output:
# [DRY-RUN] Would create s3://adstart-raw-123456789012
# [DRY-RUN] Would create s3://adstart-warehouse-123456789012
# [DRY-RUN] Would create Glue database: adstart_raw
# [DRY-RUN] Would create IAM role: adstart-pipeline-role
# ...

# Step 3: Create real resources (after reviewing the dry run)
make setup-aws ACCOUNT_ID=$ACCOUNT_ID REGION=eu-west-1

# Step 4: .env is created automatically — review it
cat .env

# Step 5: Switch to AWS mode
# Edit .env: PIPELINE_ENV=aws
```

### Resources created
S3 Buckets:
adstart-raw-123456789012              ← Raw CSV from operators
adstart-warehouse-123456789012        ← Parquet warehouse
adstart-athena-results-123456789012   ← Athena query results
Glue Databases:
adstart_raw          ← Raw tables (Parquet)
adstart_warehouse    ← Facts + Mart tables
IAM:
Role: adstart-pipeline-role
Policy: adstart-pipeline-policy (inline)
SNS:
Topic: adstart-pipeline-alerts

---

## 6. Run Flow End-to-End

### Full flow from 0 to end
┌─────────────────────────────────────────────────────────┐
│  STEP-BY-STEP GUIDE: 0 → Production Pipeline on AWS     │
└─────────────────────────────────────────────────────────┘
DAY 1: AWS Account + Setup
═══════════════════════════
Step 0: Create AWS account + IAM admin user + MFA
→ https://aws.amazon.com → Create Account
Step 1: Configure AWS CLI
→ aws configure --profile adstart-dev
Step 2: Clone project + install deps
→ git clone ... && pip install -r requirements_aws.txt
Step 3: Set up AWS resources
→ make setup-aws ACCOUNT_ID=123456789012
Step 4: Upload sample data
→ make upload-data RUN_DATE=2026-01-15
Step 5: Run pipeline for the first time
→ make run RUN_DATE=2026-01-15
Step 6: Query results in the Athena console
DAY 2+: Daily operations
════════════════════════
Run manually
make run                              # Default: yesterday
make run RUN_DATE=2026-01-20          # Specific date
Backfill 7 days
make backfill DAYS=7
Monitor
make logs                             # CloudWatch logs

### Step-by-step details

#### Step 1 — Verify environment

```bash
# Check AWS credentials
aws sts get-caller-identity
# {
#   "Account": "123456789012",
#   "Arn": "arn:aws:iam::123456789012:user/adstart-admin"
# }

# Check buckets exist
aws s3 ls | grep adstart
# 2026-01-15 10:00:00 adstart-raw-123456789012
# 2026-01-15 10:00:00 adstart-warehouse-123456789012
# 2026-01-15 10:00:00 adstart-athena-results-123456789012

# Check .env
cat .env | grep PIPELINE_ENV
# PIPELINE_ENV=aws
```

#### Step 2 — Upload sample data to S3

```bash
# Upload all files for 2026-01-15
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

# Verify on S3
make ls-raw RUN_DATE=2026-01-15
```

#### Step 3 — Run pipeline

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
#   [fct_billing] Total 445 rows written for 2026-01-15.
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

#### Step 4 — Verify on S3 + Athena

```bash
# List files on S3
make ls-warehouse RUN_DATE=2026-01-15
# s3://adstart-warehouse-123456789012/facts/fct_subscriptions/report_date=2026-01-15/...parquet
# s3://adstart-warehouse-123456789012/mart/mart_daily_performance/report_date=2026-01-15/...parquet

# Query Athena (AWS Console or CLI)
aws athena start-query-execution \
  --query-string "SELECT campaign_id, total_clicks, total_subscriptions, total_revenue
                  FROM mart_daily_performance
                  WHERE report_date = '2026-01-15'
                  ORDER BY total_revenue DESC" \
  --query-execution-context Database=adstart_warehouse \
  --result-configuration OutputLocation=s3://adstart-athena-results-123456789012/
```

#### Step 5 — Check results in the Athena Console
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

---

## 7. S3 Data Layout
s3://adstart-raw-123456789012/
│
├── operator_a/
│   ├── date=2026-01-14/
│   │   └── data.csv
│   └── date=2026-01-15/
│       └── data.csv         ← Hive partition format (auto-detected by Athena)
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

---

## 8. Glue + Athena Query Guide

### Viewing tables in the Athena Console

```sql
-- List all tables in the warehouse database
SHOW TABLES IN adstart_warehouse;

-- View the schema of fct_subscriptions
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

-- Revenue by operator by week
SELECT
    DATE_TRUNC('week', CAST(report_date AS DATE)) AS week_start,
    operator,
    SUM(total_revenue) AS weekly_revenue,
    SUM(total_subscriptions) AS weekly_subs
FROM adstart_warehouse.mart_daily_performance
WHERE report_date >= '2026-01-01'
GROUP BY 1, 2
ORDER BY 1, 3 DESC;

-- operator_C attribution rate
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

### Optimising Athena queries (cost savings)

```sql
-- ✅ ALWAYS filter by the partition column first
SELECT * FROM fct_subscriptions
WHERE report_date = '2026-01-15'    -- Partition filter → scans only 1 file

-- ❌ Avoid full table scans
SELECT * FROM fct_subscriptions     -- Scans ALL partitions = expensive
WHERE YEAR(subscribed_at) = 2026

-- ✅ SELECT only the columns you need (Parquet is columnar = only selected columns are scanned)
SELECT subscription_id, msisdn, campaign_id
FROM fct_subscriptions
WHERE report_date = '2026-01-15'

-- ❌ Avoid SELECT *
SELECT * FROM fct_subscriptions     -- Scans all columns
```

---

## 9. Monitoring & Alerts

### SNS Email Alerts

The pipeline automatically sends an alert when a quality check fails:

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
# Stream logs in real time
make logs

# Filter for errors
aws logs filter-log-events \
  --log-group-name /aws/adstart-pipeline/pipeline \
  --filter-pattern "ERROR"
```

---

## 10. Local vs AWS Comparison

| Aspect         | Local (DuckDB)          | AWS (S3+Athena)               |
|---------------|------------------------|-------------------------------|
| Startup cost  | 0                       | ~5 minutes setup              |
| Per-run cost  | 0                       | ~$0.01–0.05/day               |
| Data size     | Limited by RAM/disk     | Unlimited                     |
| Query speed   | Fast (in-process)       | 1–10s latency                 |
| Concurrency   | Single process          | Multiple parallel queries     |
| Sharing data  | Must copy files         | Share an Athena query         |
| Durability    | Local disk              | 99.999999999%                 |
| Dev workflow  | Instant feedback        | Upload → wait                 |

**When to use Local mode:**
- Development + unit tests
- Prototyping transformation logic
- CI/CD pipeline checks

**When to use AWS mode:**
- Data > 1 GB
- Multiple analysts need to query simultaneously
- Production pipeline with scheduling
- Audit trail + data versioning required

---

## 11. Troubleshooting

### `NoCredentialsError`
```bash
# Cause: AWS credentials not configured
aws configure --profile adstart-dev
# Or: check whether AWS_PROFILE is set in .env
```

### `NoSuchBucket`
```bash
# Cause: setup_aws.py has not been run yet
make setup-aws ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
```

### `Glue table does not exist` in Athena
```bash
# awswrangler creates the table automatically on the first write_table() call
# If the error persists, re-run the pipeline and check the logs
make run RUN_DATE=2026-01-15
```

### `AccessDenied` when writing to S3
```bash
# Check whether the IAM policy grants sufficient permissions
aws iam simulate-principal-policy \
  --policy-source-arn arn:aws:iam::ACCOUNT_ID:role/adstart-pipeline-role \
  --action-names s3:PutObject \
  --resource-arns arn:aws:s3:::adstart-warehouse-ACCOUNT_ID/*
```

### Athena query fails with `HIVE_PARTITION_SCHEMA_MISMATCH`
```bash
# Occurs when the schema changes after the table already exists
# Run MSCK REPAIR TABLE in the Athena console:
MSCK REPAIR TABLE adstart_warehouse.fct_subscriptions;
```

---

## 12. Estimated Costs

With sample data volume (~1–5 MB/day):

| Service      | Usage                     | Cost/month  |
|-------------|---------------------------|-------------|
| S3 Storage  | ~150 MB/month             | ~$0.003     |
| S3 Requests | ~10K PUT/GET requests     | ~$0.05      |
| Athena      | ~10 queries × 5 MB/query  | ~$0.002     |
| Glue Catalog| Free up to 1M objects     | $0.00       |
| SNS         | <1K messages/month        | $0.00       |
| **Total**   |                           | **< $0.10** |

> **Free Tier** (first 12 months): S3 5 GB + 20K GET requests — more than enough for practice.

---

## Project Structure
```
part3_pipeline_aws/
├── .env.example                    ← Copy → .env and fill in values
├── .gitignore
├── Makefile                        ← All commands in one place
├── README.md                       ← This file
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
│   │   └── quality.py              ← Post-build assertions + SNS alerts
│   │
│   └── utils/
│       ├── aws_warehouse.py        ← AWSWarehouse (S3+Athena facade)
│       └── s3_utils.py             ← S3 helper functions
│
├── infrastructure/
│   ├── setup_aws.py                ← Create S3, Glue, IAM, SNS
│   ├── upload_sample_data.py       ← Upload CSVs to S3
│   └── teardown_aws.py             ← Cleanup (dev only)
│
└── tests/
    ├── unit/
    │   └── test_transformations.py ← Unit tests (no AWS needed)
    └── integration/
        └── test_pipeline_aws.py    ← Integration tests (moto mock)
```