# AWS Migration Map — từ local stack → production AWS

Tài liệu này giải thích **tại sao** mỗi thành phần local được chọn,
và nó map sang AWS component nào khi đưa lên production.

---

## Local stack → AWS stack (1-to-1 mapping)

| Local component | AWS equivalent | Tại sao chuyển |
|---|---|---|
| `data/` folder | S3 bucket (`s3://adstart-raw/`) | Scale, durability, event triggers |
| DuckDB file | Amazon Redshift hoặc Athena + S3 Parquet | Concurrent users, TB-scale |
| Prefect `@flow` | AWS Step Functions state machine | Managed retry, visual monitoring |
| Prefect `@task` | AWS Glue ETL job hoặc Lambda | Serverless, auto-scaling |
| `logs/` folder | CloudWatch Logs | Centralized, alertable |
| Quality checks | CloudWatch Alarms + SNS | Alert qua email/Slack tự động |
| `pipeline.py --date` | EventBridge scheduled rule | Chạy tự động hàng ngày 6am |

---

## Bước migrate cụ thể

### 1. Ingest: Local file read → S3 event trigger

```python
# LOCAL (hiện tại):
from_csv_auto('data/operator_a.csv')

# AWS — Lambda triggered by S3 PutObject event:
import boto3
s3 = boto3.client('s3')
obj = s3.get_object(Bucket='adstart-raw', Key=f'operator_a/{run_date}/data.csv')
df = pd.read_csv(obj['Body'])
```

S3 event rule trong Terraform:
```hcl
resource "aws_s3_bucket_notification" "operator_a_trigger" {
  bucket = "adstart-raw"
  lambda_function {
    lambda_function_arn = aws_lambda_function.ingest.arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = "operator_a/"
  }
}
```

### 2. Transform: DuckDB SQL → AWS Glue job

```python
# LOCAL (hiện tại):
conn.execute("INSERT INTO fct_subscriptions SELECT ...")

# AWS Glue — PySpark:
from awsglue.context import GlueContext
from pyspark.context import SparkContext

sc = SparkContext()
glueContext = GlueContext(sc)

raw_a = glueContext.create_dynamic_frame.from_catalog(
    database="adstart_raw",
    table_name="operator_a"
)
# Transform logic giống y hệt, chỉ đổi SQL sang Spark DataFrame API
# Hoặc dùng Glue với DynamicFrame.toDF() rồi spark.sql()
```

### 3. Orchestration: Prefect → Step Functions

```json
// Step Functions state machine definition
{
  "Comment": "AdStart daily pipeline",
  "StartAt": "IngestOperators",
  "States": {
    "IngestOperators": {
      "Type": "Parallel",
      "Branches": [
        {"StartAt": "IngestA", "States": {"IngestA": {"Type": "Task", "Resource": "arn:aws:lambda:...:ingest_a"}}},
        {"StartAt": "IngestB", "States": {"IngestB": {"Type": "Task", "Resource": "arn:aws:lambda:...:ingest_b"}}},
        {"StartAt": "IngestC", "States": {"IngestC": {"Type": "Task", "Resource": "arn:aws:lambda:...:ingest_c"}}}
      ],
      "Retry": [{"ErrorEquals": ["States.ALL"], "MaxAttempts": 3, "IntervalSeconds": 60}],
      "Next": "BuildDimensions"
    },
    "BuildDimensions": {
      "Type": "Task",
      "Resource": "arn:aws:glue:...:jobs/build_dim_campaigns",
      "Next": "BuildFacts"
    }
  }
}
```

### 4. Storage: DuckDB → Athena + S3 Parquet (cost-effective option)

```python
# Write Parquet to S3 instead of DuckDB file
import awswrangler as wr

wr.s3.to_parquet(
    df=df_subscriptions,
    path=f"s3://adstart-warehouse/fct_subscriptions/date={run_date}/",
    dataset=True,
    partition_cols=["report_date"],
    mode="overwrite_partitions",  # IDEMPOTENCY — same as DELETE + INSERT
    database="adstart_warehouse",
    table="fct_subscriptions",
)

# Query via Athena (serverless, pay per query):
result = wr.athena.read_sql_query(
    sql="SELECT * FROM mart_daily_performance WHERE report_date = '2026-01-15'",
    database="adstart_warehouse",
)
```

### 5. Scheduling: CLI script → EventBridge

```python
# Tạo schedule chạy mỗi ngày 6am UTC:
import boto3
events = boto3.client('events')
events.put_rule(
    Name='adstart-daily-pipeline',
    ScheduleExpression='cron(0 6 * * ? *)',  # 6am UTC daily
    State='ENABLED',
)
```

---

## Thứ tự học để practice AWS

```
Week 1: S3 basics — upload file, read với boto3, bucket policies
Week 2: Lambda — trigger từ S3 event, đọc file, ghi vào DynamoDB
Week 3: Glue — tạo crawler, ETL job PySpark, Glue catalog
Week 4: Athena — query S3 Parquet, tạo views, connect Metabase
Week 5: Step Functions — orchestrate Lambda + Glue, retry logic
Week 6: CloudWatch — logs, alarms, SNS alerts khi pipeline fail
```

## Estimate cost cho pipeline này (UK data, ~1M rows/ngày)

| Service | Usage | Cost/month |
|---|---|---|
| S3 | ~1GB raw + Parquet | ~$0.02 |
| Lambda ingest | 3 runs/day × 30 days | ~$0.50 |
| Glue ETL | 3 DPU × 10 min/day | ~$4.40 |
| Athena queries | 10 queries/day × 30 days | ~$1.50 |
| Step Functions | 30 executions | ~$0.75 |
| CloudWatch | Logs + metrics | ~$1.00 |
| **Total** | | **~$8/month** |

So sánh: Redshift smallest cluster = ~$180/month — không cần thiết cho dataset này.
Athena + S3 Parquet là lựa chọn cost-effective nhất cho pipeline batch hàng ngày.
