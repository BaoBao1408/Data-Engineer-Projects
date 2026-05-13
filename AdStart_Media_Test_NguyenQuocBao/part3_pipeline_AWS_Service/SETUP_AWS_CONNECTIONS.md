# SETUP_AWS_CONNECTIONS.md
# Kết nối S3 + Glue + Athena — Hướng dẫn từ số 0

> Dành cho người **chưa biết gì về AWS**.
> Sau khi làm theo guide này, chỉ cần `docker-compose run pipeline run` là
> data chạy từ S3 qua Glue vào Athena hoàn toàn tự động.

---

## Bức tranh tổng thể

```
Bạn làm gì                     Code Python tương ứng
──────────────────────────────────────────────────────────────────────
[AWS Console]                   [file trong project]

Tạo S3 bucket "raw"         →   AWS_RAW_BUCKET       → loaders.py
Tạo S3 bucket "warehouse"   →   AWS_WAREHOUSE_BUCKET → aws_warehouse.py
Tạo S3 bucket "athena"      →   AWS_ATHENA_OUTPUT_BUCKET → aws_warehouse.py
Tạo Glue DB "adstart_raw"   →   GLUE_RAW_DATABASE    → aws_warehouse.py
Tạo Glue DB "adstart_wh"    →   GLUE_WAREHOUSE_DATABASE → aws_warehouse.py
Tạo IAM User + Access Key   →   AWS_ACCESS_KEY_ID    → boto3 (tự đọc)
Chọn region                 →   AWS_REGION           → config/base.py

Điền tất cả vào .env  →  docker-compose run pipeline run  →  XONG
```

---

## PHẦN 1 — Tạo tài khoản AWS (bỏ qua nếu đã có)

```
1. Vào https://aws.amazon.com → "Create an AWS Account"
2. Đăng ký bằng email + credit card (free tier 12 tháng)
3. Đăng nhập vào https://console.aws.amazon.com
4. Lấy Account ID: click tên bạn góc phải trên → copy 12 số
   VD: 123456789012  ← lưu lại, cần dùng nhiều lần
```

---

## PHẦN 2 — Chọn Region (1 lần, không đổi nữa)

> Region = trung tâm dữ liệu nơi AWS lưu tất cả resources của bạn.
> **Tất cả services phải cùng region** — nếu khác region sẽ không đọc được nhau.

```
Góc phải trên AWS Console → chọn region:

  Nếu ở Việt Nam/Singapore  → ap-southeast-1 (Singapore)
  Nếu ở UK/Europe           → eu-west-1      (Ireland)
  Nếu ở US                  → us-east-1      (N. Virginia)
```

**Ghi lại region bạn chọn** — điền vào `.env` sau:
```
AWS_REGION=ap-southeast-1    ← ví dụ chọn Singapore
```

**Kết nối với code:**
```
.env: AWS_REGION=ap-southeast-1
         ↓ đọc bởi
config/base.py dòng 130:
    s.aws_region = os.getenv("AWS_REGION", "eu-west-1")
         ↓ dùng bởi
aws_warehouse.py dòng 257:
    return boto3.Session(region_name=self.settings.aws_region)
         ↓ boto3 Session này được dùng cho TẤT CẢ: S3 + Glue + Athena
```

---

## PHẦN 3 — Tạo S3 Buckets

> **S3 là gì?** = Ổ cứng trên cloud. Lưu files (CSV, Parquet).
> Project dùng **3 buckets** cho 3 mục đích khác nhau.

### ❶ Bucket lưu raw CSV từ operators

```
AWS Console → Services → S3 → "Create bucket" (nút cam)
```

Điền form:

```
┌─────────────────────────────────────────────────────────────┐
│ Bucket name:  adstart-raw-123456789012                      │
│               ↑ đổi 123456789012 = Account ID của bạn      │
│                                                             │
│ AWS Region:   ap-southeast-1  ← PHẢI trùng với bước 2      │
│                                                             │
│ Block Public Access:  ✅ Block all public access            │
│                       (tick tất cả 4 ô)                    │
│                                                             │
│ Bucket Versioning:  Enable                                  │
│ (giữ lịch sử khi file bị ghi đè — phục hồi được)           │
│                                                             │
│ Default encryption:  SSE-S3 (miễn phí)                     │
└─────────────────────────────────────────────────────────────┘
→ Click "Create bucket"
```

**Kết nối với code — vẽ luồng đầy đủ:**
```
Bucket name: "adstart-raw-123456789012"
       │
       │ điền vào
       ▼
.env dòng 1:
    AWS_RAW_BUCKET=adstart-raw-123456789012
       │
       │ đọc bởi
       ▼
config/base.py dòng 127:
    s.raw_bucket = os.getenv("AWS_RAW_BUCKET", "")
    # settings.raw_bucket = "adstart-raw-123456789012"
       │
       │ dùng bởi
       ▼
src/ingest/loaders.py dòng 56:
    s3.get_object(Bucket=settings.raw_bucket, Key=key)
    # Đọc file: s3://adstart-raw-123456789012/operator_a/date=2026-01-15/data.csv
       │
       │ và bởi
       ▼
infrastructure/upload_sample_data.py:
    s3.upload_file(local_path, raw_bucket, s3_key)
    # Upload CSV lên: s3://adstart-raw-123456789012/operator_a/date=.../data.csv
```

---

### ❷ Bucket lưu Parquet warehouse

```
S3 → "Create bucket"
```

```
┌─────────────────────────────────────────────────────────────┐
│ Bucket name:  adstart-warehouse-123456789012                │
│ AWS Region:   ap-southeast-1  ← cùng region                │
│ Block Public Access:  ✅ tất cả                             │
│ Versioning:   Disable (không cần)                           │
│ Encryption:   SSE-S3                                        │
└─────────────────────────────────────────────────────────────┘
→ Click "Create bucket"
```

**Kết nối với code:**
```
Bucket name: "adstart-warehouse-123456789012"
       │
       ▼
.env:
    AWS_WAREHOUSE_BUCKET=adstart-warehouse-123456789012
       │
       ▼
config/base.py dòng 40 + 128:
    warehouse_bucket: str = ""
    s.warehouse_bucket = os.getenv("AWS_WAREHOUSE_BUCKET", "")
       │
       │ dùng để xây dựng S3 paths:
       ▼
config/base.py dòng 99–113:
    def s3_raw_table_path(self, table):
        return f"s3://{self.warehouse_bucket}/raw/{table}/"
        # → "s3://adstart-warehouse-123456789012/raw/raw_operator_a/"

    def s3_fact_path(self, table):
        return f"s3://{self.warehouse_bucket}/facts/{table}/"
        # → "s3://adstart-warehouse-123456789012/facts/fct_subscriptions/"

    def s3_mart_path(self, table):
        return f"s3://{self.warehouse_bucket}/mart/{table}/"
        # → "s3://adstart-warehouse-123456789012/mart/mart_daily_performance/"
       │
       ▼
aws_warehouse.py dòng 157–176:
    wr.s3.to_parquet(
        df   = df,
        path = s3_path,   # ← path từ config ở trên
        # Ghi Parquet + tự đăng ký table vào Glue Catalog
    )
```

Sau khi pipeline chạy, bucket này tự có cấu trúc:
```
adstart-warehouse-123456789012/
├── raw/
│   ├── raw_operator_a/_loaded_date=2026-01-15/part-0.parquet
│   ├── raw_operator_b/...
│   └── raw_operator_c/...
├── dimensions/
│   └── dim_campaigns/part-0.parquet
├── facts/
│   ├── fct_subscriptions/report_date=2026-01-15/part-0.parquet
│   ├── fct_billing/...
│   └── fct_clicks/...
└── mart/
    └── mart_daily_performance/report_date=2026-01-15/part-0.parquet
```

---

### ❸ Bucket lưu kết quả Athena query

```
S3 → "Create bucket"
```

```
┌─────────────────────────────────────────────────────────────┐
│ Bucket name:  adstart-athena-results-123456789012           │
│ AWS Region:   ap-southeast-1  ← cùng region                │
│ Block Public Access:  ✅ tất cả                             │
│ Versioning:   Disable                                       │
│ Encryption:   SSE-S3                                        │
└─────────────────────────────────────────────────────────────┘
→ Click "Create bucket"
```

Thêm lifecycle rule để tự xóa sau 7 ngày (tiết kiệm chi phí):
```
S3 → bucket "adstart-athena-results-123456789012"
  → tab "Management"
  → "Create lifecycle rule"

  Rule name: expire-query-results-7d
  Prefix:    query-results/
  ✅ Expire current versions of objects  →  After: 7 days
→ "Create rule"
```

**Kết nối với code:**
```
Bucket name: "adstart-athena-results-123456789012"
       │
       ▼
.env:
    AWS_ATHENA_OUTPUT_BUCKET=adstart-athena-results-123456789012
       │
       ▼
config/base.py dòng 54–55 + 91–93:
    athena_output_bucket: str = ""
    athena_output_prefix: str = "query-results"

    @property
    def athena_s3_output(self) -> str:
        return f"s3://{self.athena_output_bucket}/{self.athena_output_prefix}/"
        # → "s3://adstart-athena-results-123456789012/query-results/"
       │
       ▼
aws_warehouse.py dòng 231–238:
    wr.athena.read_sql_query(
        sql       = "SELECT * FROM mart_daily_performance WHERE ...",
        database  = "adstart_warehouse",
        s3_output = self.settings.athena_s3_output,
        # ↑ Athena lưu kết quả vào đây tạm thời
        # ↑ awswrangler tự download về pandas DataFrame
    )
```

---

## PHẦN 4 — Tạo Glue Databases

> **Glue Catalog là gì?** = Danh bạ schema.
> Glue biết "file Parquet trong S3 kia có cột gì, kiểu gì".
> Athena hỏi Glue trước khi query S3.
> **Bạn không cần tạo tables** — code tự tạo khi ghi data lần đầu.

### ❶ Database cho raw tables

```
AWS Console → AWS Glue → "Databases" (menu trái) → "Add database"

Database name:  adstart_raw
Description:    Raw Parquet tables (từ operator CSV files)
Location:       (để trống)
→ "Create database"
```

**Kết nối với code:**
```
Database name: "adstart_raw"
       │
       ▼
.env:
    GLUE_RAW_DATABASE=adstart_raw
       │
       ▼
config/base.py dòng 50 + 132:
    glue_raw_database: str = "adstart_raw"
    s.glue_raw_database = os.getenv("GLUE_RAW_DATABASE", "adstart_raw")
       │
       ▼
aws_warehouse.py dòng 125–128:
    def _glue_db(self, layer: str) -> str:
        if layer == "raw":
            return self.settings.glue_raw_database   # → "adstart_raw"
        return self.settings.glue_warehouse_database
       │
       ▼
aws_warehouse.py dòng 169–170 (khi write raw tables):
    wr.s3.to_parquet(
        database = self._glue_db("raw"),  # → "adstart_raw"
        table    = "raw_operator_a",
        # awswrangler TỰ TẠO table "raw_operator_a" trong Glue DB "adstart_raw"
        # Trỏ tới: s3://adstart-warehouse-.../raw/raw_operator_a/
    )
```

Sau khi pipeline chạy lần đầu, Glue sẽ tự có tables:
```
adstart_raw:
  raw_operator_a      ← raw CSV operator A (đã validate, Parquet)
  raw_operator_b
  raw_operator_c
  raw_campaigns
  raw_clicks
  raw_tracking_codes
  raw_page_events
```

---

### ❷ Database cho warehouse tables

```
AWS Glue → Databases → "Add database"

Database name:  adstart_warehouse
Description:    Fact tables + Mart tables (Athena queryable)
→ "Create database"
```

**Kết nối với code:**
```
Database name: "adstart_warehouse"
       │
       ▼
.env:
    GLUE_WAREHOUSE_DATABASE=adstart_warehouse
       │
       ▼
config/base.py dòng 51 + 133:
    glue_warehouse_database: str = "adstart_warehouse"
    s.glue_warehouse_database = os.getenv("GLUE_WAREHOUSE_DATABASE", ...)
       │
       ▼
Khi write facts/mart:
    wr.s3.to_parquet(
        database = "adstart_warehouse",   ← Glue tạo tables ở đây
        table    = "fct_subscriptions",
    )
Khi query:
    wr.athena.read_sql_query(
        sql      = "SELECT * FROM fct_subscriptions ...",
        database = "adstart_warehouse",   ← Athena tìm table ở đây
    )
```

Sau pipeline lần đầu, Glue sẽ có:
```
adstart_warehouse:
  dim_campaigns
  fct_subscriptions
  fct_billing
  fct_clicks
  fct_unattributed_events
  mart_daily_performance    ← table chính cho BI/analytics
```

---

## PHẦN 5 — Setup Athena

> **Athena là gì?** = SQL engine chạy trực tiếp trên S3.
> Không cần server. Trả tiền theo bytes đọc ($5/TB).

**Chỉ cần 1 bước:**
```
AWS Console → Athena → "Settings" (góc phải) → "Manage"

Query result location and encryption:
  Query result location: s3://adstart-athena-results-123456789012/query-results/
  ↑ Điền bucket bạn tạo ở Phần 3 ❸

→ "Save"
```

> Bước này cần để dùng Athena Query Editor trong Console.
> Code Python tự truyền location này qua `s3_output` — không cần làm gì thêm.

---

## PHẦN 6 — Tạo IAM User (quyền truy cập cho code)

> **IAM User là gì?** = Tài khoản cho code Python đăng nhập vào AWS.
> Giống username/password nhưng cho machines.

### ❶ Tạo User

```
AWS Console → IAM → Users → "Create user"

User name: adstart-pipeline-user
→ "Next"

Permissions: "Attach policies directly"
Tìm và tick 3 policies:
  ✅ AmazonS3FullAccess
  ✅ AWSGlueConsoleFullAccess
  ✅ AmazonAthenaFullAccess

→ "Next" → "Create user"
```

### ❷ Tạo Access Key

```
IAM → Users → adstart-pipeline-user
→ tab "Security credentials"
→ "Create access key"
→ Use case: "Local code"
→ "Create access key"

⚠️  COPY NGAY, chỉ xem được 1 lần:
   Access key ID:       AKIAIOSFODNN7EXAMPLE
   Secret access key:   wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
```

**Kết nối với code:**
```
Access Key ID + Secret Key
       │
       ▼
.env:
    AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE
    AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
       │
       │ boto3 TỰ ĐỌC từ env vars — KHÔNG cần viết vào code
       ▼
aws_warehouse.py dòng 255–258:
    def _boto3_session(self):
        import boto3
        return boto3.Session(region_name=self.settings.aws_region)
        # boto3 tự tìm credentials theo thứ tự:
        # 1. Env vars AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY  ← từ .env
        # 2. ~/.aws/credentials file
        # 3. IAM Role nếu chạy trên EC2/ECS
```

---

## PHẦN 7 — Điền .env (kết nối tất cả lại)

Mở terminal trong thư mục project:

```bash
cp .env.example .env
```

Mở file `.env` và điền như sau:

```bash
# ── Chế độ chạy ──────────────────────────────────────────────────
PIPELINE_ENV=aws          # ← đổi từ "local" thành "aws"

# ── Region (Phần 2) ──────────────────────────────────────────────
AWS_REGION=ap-southeast-1    # ← region bạn đã chọn

# ── S3 Buckets (Phần 3) ──────────────────────────────────────────
# ❶ Bucket raw CSV (Phần 3 ❶)
AWS_RAW_BUCKET=adstart-raw-123456789012

# ❷ Bucket Parquet warehouse (Phần 3 ❷)
AWS_WAREHOUSE_BUCKET=adstart-warehouse-123456789012

# ❸ Bucket Athena results (Phần 3 ❸)
AWS_ATHENA_OUTPUT_BUCKET=adstart-athena-results-123456789012

# ── Glue Databases (Phần 4) ──────────────────────────────────────
# ❶ Database raw (Phần 4 ❶)
GLUE_RAW_DATABASE=adstart_raw

# ❷ Database warehouse (Phần 4 ❷)
GLUE_WAREHOUSE_DATABASE=adstart_warehouse

# ── IAM Credentials (Phần 6) ─────────────────────────────────────
AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE
AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
```

> **Thay `123456789012`** bằng Account ID 12 số của bạn ở khắp nơi.

---

## PHẦN 8 — Chạy bằng Docker (từ 0 đến có data)

### Bước 1 — Build Docker image (1 lần)

```bash
docker-compose build
```

Đợi ~3-5 phút. Image download Python packages, build xong ~800MB.

---

### Bước 2 — Verify kết nối AWS

```bash
docker-compose run --rm setup
```

Output mong đợi:
```
  ✓ Credentials OK — Account: 123456789012
  ✓ s3://adstart-raw-123456789012 — OK
  ✓ s3://adstart-warehouse-123456789012 — OK
  ✓ Glue database: adstart_raw — OK
  ✓ Glue database: adstart_warehouse — OK
  Setup COMPLETE ✓
```

Nếu lỗi → xem mục Troubleshooting bên dưới.

---

### Bước 3 — Upload sample data lên S3

```bash
docker-compose run --rm pipeline upload --date 2026-01-15
```

Output:
```
  ✓ operator_A.csv → s3://adstart-raw-.../operator_a/date=2026-01-15/data.csv
  ✓ operator_B.csv → s3://adstart-raw-.../operator_b/date=2026-01-15/data.csv
  ✓ operator_C.csv → s3://adstart-raw-.../operator_c/date=2026-01-15/data.csv
  ✓ campaigns.csv  → s3://adstart-raw-.../static/campaigns.csv
  ✓ clicks.csv     → s3://adstart-raw-.../static/clicks.csv
```

---

### Bước 4 — Chạy pipeline (data vào S3 + Glue + Athena)

```bash
docker-compose run --rm pipeline run --date 2026-01-15
```

Output:
```
  ╔══════════════════════════════════════════════════════╗
  ║     adstart Data Pipeline — AWS Edition              ║
  ╚══════════════════════════════════════════════════════╝

  [ Stage 1/5 ] Ingesting raw files ...
    ✓ raw_operator_a : 1,247 rows → s3://adstart-warehouse-.../raw/
    ✓ raw_operator_b : 983 rows
    ✓ raw_operator_c : 2,104 rows

  [ Stage 2/5 ] Building dimensions ...
    ✓ dim_campaigns : 24 rows

  [ Stage 3/5 ] Building facts ...
    ✓ fct_subscriptions    : 892 rows
    ✓ fct_billing          : 445 rows
    ✓ fct_clicks           : 3,218 rows

  [ Stage 4/5 ] Building mart ...
    ✓ mart_daily_performance : 12 campaign rows

  [ Stage 5/5 ] Quality checks ...
    ✓ 8/8 checks passed

  Pipeline COMPLETED: 2026-01-15 ✓
```

---

### Bước 5 — Query data bằng Athena

Vào AWS Console → Athena → Query Editor:

```sql
-- Database: adstart_warehouse
SELECT
    report_date,
    operator,
    campaign_id,
    total_clicks,
    total_subscriptions,
    ROUND(sub_conversion_rate * 100, 2) AS cvr_pct,
    ROUND(total_revenue, 2)             AS revenue_gbp
FROM mart_daily_performance
WHERE report_date = '2026-01-15'
ORDER BY revenue_gbp DESC;
```

---

## Troubleshooting

### Lỗi: `NoCredentialsError`
```
Nguyên nhân: AWS credentials chưa được set
Fix:
  1. Kiểm tra .env có AWS_ACCESS_KEY_ID và AWS_SECRET_ACCESS_KEY chưa
  2. Chắc chắn .env được truyền vào container:
     docker-compose run --rm pipeline run ...
     (file docker-compose.yml có dòng env_file: - .env)
```

### Lỗi: `NoSuchBucket`
```
Nguyên nhân: Bucket chưa được tạo hoặc tên sai
Fix:
  1. Vào S3 Console kiểm tra bucket đã tồn tại chưa
  2. Kiểm tra tên bucket trong .env có đúng chính xác không
  3. Kiểm tra region trong .env trùng với region tạo bucket không
```

### Lỗi: `AccessDenied`
```
Nguyên nhân: IAM User chưa có đủ quyền
Fix:
  IAM → Users → adstart-pipeline-user → Permissions
  → Attach policies:
    ✅ AmazonS3FullAccess
    ✅ AWSGlueConsoleFullAccess
    ✅ AmazonAthenaFullAccess
```

### Lỗi: `EntityNotFoundException: Database adstart_raw`
```
Nguyên nhân: Glue database chưa được tạo
Fix:
  AWS Glue → Databases → Add database → "adstart_raw"
  AWS Glue → Databases → Add database → "adstart_warehouse"
```

### Lỗi: Athena query `Table not found`
```
Nguyên nhân: Pipeline chưa chạy lần đầu để tạo tables
Fix:
  Chạy pipeline ít nhất 1 lần → Glue tables tự được tạo
  docker-compose run --rm pipeline run --date 2026-01-15
```

---

## Chi phí ước tính (free tier)

```
12 tháng đầu (AWS Free Tier):
  S3:     5GB storage + 20,000 GET requests — MIỄN PHÍ
  Glue:   1 triệu objects — MIỄN PHÍ
  Athena: không có free tier — $5/TB scanned
          ~ 1 query × 5MB = $0.000025 ≈ 0 đồng

Sau free tier:
  S3 storage:  $0.023/GB/month  → ~150MB/tháng ≈ $0.003
  Athena:      10 queries × 5MB = $0.0002/tháng
  TOTAL:       < $0.10/tháng
```
