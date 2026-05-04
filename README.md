<div align="center">

# ⚙️ Data Engineer Projects

<img src="https://readme-typing-svg.demolab.com?font=Fira+Code&weight=600&size=22&pause=1000&color=FF6B35&center=true&vCenter=true&width=700&lines=Data+Engineering+%7C+ETL+%2F+ELT+Pipelines;Airflow+%7C+dbt+%7C+Spark+%7C+Databricks;Azure+%7C+AWS+%7C+GCP+Cloud+Platforms;From+Raw+Data+to+Production-Ready+Insights" alt="Typing SVG" />

<br/>

[![GitHub](https://img.shields.io/badge/GitHub-BaoBao1408-181717?style=flat-square&logo=github)](https://github.com/BaoBao1408)
![Focus](https://img.shields.io/badge/Focus-Data%20Engineering-FF6B35?style=flat-square)
![Stack](https://img.shields.io/badge/Stack-Python%20%7C%20Airflow%20%7C%20dbt%20%7C%20Spark-3776AB?style=flat-square)
![Cloud](https://img.shields.io/badge/Cloud-Azure%20%7C%20AWS%20%7C%20GCP-0078D4?style=flat-square&logo=microsoftazure&logoColor=white)
![Language](https://img.shields.io/badge/Python-91.7%25-3776AB?style=flat-square&logo=python&logoColor=white)
![License](https://img.shields.io/badge/License-MIT-green?style=flat-square)
![Commits](https://img.shields.io/badge/Commits-115+-orange?style=flat-square)

</div>

---

## 👨‍💻 About

A collection of **production-style Data Engineering projects** covering the full modern data stack — from raw ingestion and web scraping, through transformation and orchestration, to cloud-based deployment and analytics delivery.

**Core focus areas:** Batch & streaming pipelines · ELT with dbt · Workflow orchestration · Cloud data platforms · OLTP/OLAP architecture

> *"Data Engineering is the foundation everything else is built on."*

---

## 🗂️ Projects

### 🔄 Orchestration & Transformation

| Project | Description | Stack | Status |
|---------|-------------|-------|--------|
| [🌀 Airflow + dbt + Postgres](./Airflow%20-%20DBT-%20Postgres) | End-to-end pipeline: Airflow orchestrates dbt models transforming data in PostgreSQL. Includes DAG scheduling, incremental models, and data quality tests | `Airflow` `dbt` `PostgreSQL` `Python` `Docker` | ✅ Complete |
| [🧱 DBT Project](./DBT_Project) | Standalone dbt project — modular SQL transformations with sources, staging, marts layers and schema tests | `dbt` `PostgreSQL` `SQL` `Jinja` | ✅ Complete |

### ☁️ Cloud Data Platforms

| Project | Description | Stack | Status |
|---------|-------------|-------|--------|
| [🛒 Ecommerce — Azure Databricks End-to-End](./Ecommerce%20-%20Azure%20Databricks%20End-to-End%20Dat...) | Full ELT pipeline on Azure: ingestion → Delta Lake → Databricks notebooks → curated data layer | `Azure Databricks` `Delta Lake` `PySpark` `Azure Data Lake` | ✅ Complete |
| [✈️ Flights — Databricks × dbt End-to-End](./Flights%20-%20DATABRICKS%20x%20DBT%20End-To-End%20Data%20E...) | Flight data pipeline combining Databricks compute with dbt transformations — medallion architecture | `Databricks` `dbt` `PySpark` `Delta Lake` `SQL` | ✅ Complete |
| [🚕 NYC Taxi — Azure Data Factory + Databricks](./NYC%20Taxi%20-%20Azure_Data_Factory-Data_Bricks-Del...) | Classic NYC Taxi dataset processed via ADF pipelines into Databricks, Delta tables, analytical layer | `Azure Data Factory` `Databricks` `Delta Lake` `Azure` | ✅ Complete |
| [🏙️ Smart City — AWS Project](./Smart%20City%20-%20AWS%20project%20Pycharm%20-%20Docker%20-%20K...) | IoT-style smart city data pipeline on AWS — containerized services, orchestration with Kubernetes | `AWS` `Docker` `Kubernetes` `Python` `PyCharm` | ✅ Complete |

### 🛍️ E-commerce Data Pipelines

| Project | Description | Stack | Status |
|---------|-------------|-------|--------|
| [🏗️ Ecommerce OLTP Platform](./ecommerce_oltp_platform) | Relational database design for e-commerce — normalized schema, transactions, indexing strategy | `PostgreSQL` `PLpgSQL` `SQL` `Python` | ✅ Complete |
| [⚙️ Ecommerce OLTP — Cloud Pipeline (Phase 3)](./Ecommerce_OLTP_Platform) | Phase 3: migrate on-prem OLTP to cloud pipeline with CI/CD and automated ingestion | `PostgreSQL` `Python` `Cloud Storage` `Docker` | ✅ Complete |
| [🛒 Tiki Product Pipeline](./Tiki_Product_Pipeline-main) | Automated data pipeline ingesting Tiki product catalog — transform, load, and model for analytics | `Python` `Airflow` `PostgreSQL` `dbt` | ✅ Complete |
| [💄 Glamira — User Behavior Analytics](./Glamira%20-%20User%20Behavior) | User behavior event pipeline for Glamira jewelry — clickstream ingestion, sessionization, funnel analysis | `Python` `SQL` `Airflow` `PostgreSQL` | ✅ Complete |

### 🕷️ Data Collection & Scraping

| Project | Description | Stack | Status |
|---------|-------------|-------|--------|
| [🔍 CrawData](./CrawData) | Web scraping framework — modular scrapers with retry logic, deduplication, and structured output | `Python` `BeautifulSoup` `Requests` `Selenium` | ✅ Complete |
| [📦 Craw 200k Tiki Products](./Craw_200k_Products_Tiki_Project) | Scraped 200,000+ product listings from Tiki — distributed crawling, rate limiting, Postgres storage | `Python` `Scrapy` `PostgreSQL` `Docker` | ✅ Complete |

### 📊 Analytics & BI

| Project | Description | Stack | Status |
|---------|-------------|-------|--------|
| [🛍️ Ecommerce — Knime to Power BI](./Ecommerce%20With%20Knime%20To%20Power%20BI) | No-code/low-code data workflow in Knime, delivering cleaned datasets directly to Power BI dashboards | `Knime` `Power BI` `SQL` `Excel` | ✅ Complete |
| [🎙️ Podcast Analytics](./Podcast) | Podcast performance analytics pipeline — download metrics, listener trends, episode scoring | `Python` `SQL` `PostgreSQL` | ✅ Complete |

### 🔧 Tooling & Infrastructure

| Project | Description | Stack | Status |
|---------|-------------|-------|--------|
| [🐧 Linux Project](./Linux_Project) | Shell scripting, cron jobs, file system automation and system monitoring scripts for DE workflows | `Linux` `Bash` `Shell` `Cron` | ✅ Complete |
| [🐍 Python Project](./PythonProject) | Core Python utilities — data processing helpers, API clients, reusable DE components | `Python` `Pandas` `NumPy` | ✅ Complete |

---

## 🛠️ Tech Stack

### 🔄 Orchestration & Transformation
![Airflow](https://img.shields.io/badge/Apache%20Airflow-017CEE?style=flat-square&logo=apacheairflow&logoColor=white)
![dbt](https://img.shields.io/badge/dbt-FF694B?style=flat-square&logo=dbt&logoColor=white)
![Spark](https://img.shields.io/badge/Apache%20Spark-E25A1C?style=flat-square&logo=apachespark&logoColor=white)
![Kafka](https://img.shields.io/badge/Apache%20Kafka-231F20?style=flat-square&logo=apachekafka&logoColor=white)

### ☁️ Cloud Platforms
![Azure](https://img.shields.io/badge/Microsoft%20Azure-0078D4?style=flat-square&logo=microsoftazure&logoColor=white)
![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=flat-square&logo=databricks&logoColor=white)
![AWS](https://img.shields.io/badge/AWS-232F3E?style=flat-square&logo=amazonaws&logoColor=white)
![GCP](https://img.shields.io/badge/Google%20Cloud-4285F4?style=flat-square&logo=googlecloud&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta%20Lake-003366?style=flat-square)

### 🗄️ Databases & Storage
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-4169E1?style=flat-square&logo=postgresql&logoColor=white)
![SQL](https://img.shields.io/badge/SQL-CC2927?style=flat-square&logo=microsoftsqlserver&logoColor=white)
![Redis](https://img.shields.io/badge/Redis-DC382D?style=flat-square&logo=redis&logoColor=white)
![Azure Data Lake](https://img.shields.io/badge/Azure%20Data%20Lake-0078D4?style=flat-square&logo=microsoftazure&logoColor=white)

### 🐍 Languages & Processing
![Python](https://img.shields.io/badge/Python-3776AB?style=flat-square&logo=python&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-E25A1C?style=flat-square&logo=apachespark&logoColor=white)
![Pandas](https://img.shields.io/badge/Pandas-150458?style=flat-square&logo=pandas&logoColor=white)
![PLpgSQL](https://img.shields.io/badge/PLpgSQL-4169E1?style=flat-square&logo=postgresql&logoColor=white)
![Bash](https://img.shields.io/badge/Bash-4EAA25?style=flat-square&logo=gnubash&logoColor=white)

### 📦 Infrastructure & DevOps
![Docker](https://img.shields.io/badge/Docker-2496ED?style=flat-square&logo=docker&logoColor=white)
![Kubernetes](https://img.shields.io/badge/Kubernetes-326CE5?style=flat-square&logo=kubernetes&logoColor=white)
![Linux](https://img.shields.io/badge/Linux-FCC624?style=flat-square&logo=linux&logoColor=black)
![Git](https://img.shields.io/badge/Git-F05032?style=flat-square&logo=git&logoColor=white)
![VSCode](https://img.shields.io/badge/VS%20Code-007ACC?style=flat-square&logo=visualstudiocode&logoColor=white)

### 📊 Analytics & BI
![Power BI](https://img.shields.io/badge/Power%20BI-F2C811?style=flat-square&logo=powerbi&logoColor=black)
![Knime](https://img.shields.io/badge/Knime-FDD800?style=flat-square&logo=knime&logoColor=black)
![Azure Data Factory](https://img.shields.io/badge/Azure%20Data%20Factory-0078D4?style=flat-square&logo=microsoftazure&logoColor=white)

---

## 🏗️ Architecture Patterns Used

```
Batch ELT (most projects)
  Raw Source → Ingestion Layer → Staging (dbt) → Marts → BI / Analytics

Medallion Architecture (Databricks projects)
  Bronze (raw) → Silver (cleaned) → Gold (aggregated) → Consumption

Workflow Orchestration
  Airflow DAGs → Task dependencies → SLA monitoring → Alerting

Cloud-Native Pipeline
  Cloud Storage → Data Factory / Glue → Databricks / EMR → Delta / S3 → Power BI
```

---

## 📁 Repository Structure

```
Data-Engineer-Projects/
│
├── 📂 Airflow - DBT - Postgres/          # Orchestration + transformation
├── 📂 CrawData/                          # Web scraping framework
├── 📂 Craw_200k_Products_Tiki_Project/   # 200k product dataset
├── 📂 DBT_Project/                       # Standalone dbt models
├── 📂 Ecommerce - Azure Databricks.../   # Azure end-to-end ELT
├── 📂 Ecommerce With Knime To Power BI/  # No-code BI pipeline
├── 📂 Ecommerce_OLTP_Platform/           # Cloud migration (Phase 3)
├── 📂 Flights - DATABRICKS x DBT.../    # Medallion architecture
├── 📂 Glamira - User Behavior/           # Clickstream analytics
├── 📂 Linux_Project/                     # Shell & automation scripts
├── 📂 NYC Taxi - Azure.../               # ADF + Databricks pipeline
├── 📂 Podcast/                           # Podcast analytics pipeline
├── 📂 PythonProject/                     # Reusable Python utilities
├── 📂 Smart City - AWS.../               # AWS + Docker + K8s
├── 📂 Tiki_Product_Pipeline-main/        # Tiki product ingestion
├── 📂 ecommerce_oltp_platform/           # OLTP schema & transactions
│
└── README.md
```

---

## 🧭 Next Steps

```
✅  Batch ETL / ELT pipelines across Azure, AWS, GCP
✅  Orchestration with Airflow + dbt
✅  OLTP design & cloud migration
✅  Web scraping at scale (200k+ records)

🔨  Streaming pipeline with Kafka + Spark Structured Streaming
🔨  Data lakehouse on GCP (BigQuery + dbt + Looker)
📋  Real-time CDC with Debezium + Kafka → Snowflake
📋  DataOps: automated testing (Great Expectations) + CI/CD for dbt
📋  AI/ML feature store integration (bridging DE ↔ ML Engineering)
```

---

## 📊 GitHub Stats

<div align="center">

![GitHub Stats](https://github-readme-stats.vercel.app/api?username=BaoBao1408&show_icons=true&theme=tokyonight&hide_border=true&count_private=true)

![Top Languages](https://github-readme-stats.vercel.app/api/top-langs/?username=BaoBao1408&layout=compact&theme=tokyonight&hide_border=true)

</div>

---

## 📬 Contact

<div align="center">

[![LinkedIn](https://img.shields.io/badge/LinkedIn-Connect-0A66C2?style=for-the-badge&logo=linkedin&logoColor=white)](https://www.linkedin.com/in/nguy%E1%BB%85n-qu%E1%BB%91c-b%E1%BA%A3o-morgan-6459a6307/)
[![GitHub](https://img.shields.io/badge/GitHub-Follow-181717?style=for-the-badge&logo=github&logoColor=white)](https://github.com/BaoBao1408)
[![Email](https://img.shields.io/badge/Email-Contact-EA4335?style=for-the-badge&logo=gmail&logoColor=white)](mailto:baoquocnguyen1408@email.com)

</div>

---

<div align="center">

*Turning raw data into reliable, scalable, production pipelines.*

⭐ **Star this repo** if you find it useful — it helps more than you think.

</div>
