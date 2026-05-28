# Enterprise Data Platform
### KPMG-Style Financial Data Engineering Platform

Production-ready end-to-end data platform for financial document processing, ETL, Knowledge Graph, and RAG — built against the KPMG Senior Data Engineer job requirements.

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          DATA SOURCES                                       │
│   PDF Reports  │  Excel Financials  │  Word Documents  │  REST APIs  │  DB  │
└────────┬───────┴──────────┬─────────┴────────┬─────────┴──────┬──────┴──────┘
         │                  │                  │                 │
         ▼                  ▼                  ▼                 ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                     INGESTION LAYER  (src/ingestion/)                       │
│   PDFExtractor │ ExcelExtractor │ WordExtractor │ AzureBlobConnector        │
└────────────────────────────────┬────────────────────────────────────────────┘
                                 │ raw files
                                 ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│              AZURE DATA LAKE STORAGE GEN2  (Medallion Architecture)         │
│   ┌──────────────┐   ┌──────────────────┐   ┌────────────────────────────┐ │
│   │  raw/        │──▶│  processed/      │──▶│  curated/                  │ │
│   │  (Bronze)    │   │  (Silver)        │   │  (Gold - analytics-ready)  │ │
│   └──────────────┘   └──────────────────┘   └────────────────────────────┘ │
└────────────────────────────────┬────────────────────────────────────────────┘
                                 │ ETL
                                 ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│              ETL PIPELINE  (src/etl/)   [Orchestrated by Airflow / ADF]    │
│                                                                             │
│   DocumentETLPipeline                    FinancialRatioPipeline             │
│   ├── bronze_ingest()                    ├── compute_ratios_for_entity()    │
│   ├── silver_clean()  ← DataQuality      └── upsert_ratios()               │
│   ├── gold_aggregate()                                                      │
│   └── warehouse_load()                   GLAnomalyDetector                 │
│                                          └── detect_and_flag()             │
└────────────────────────────────┬────────────────────────────────────────────┘
                                 │
              ┌──────────────────┼───────────────────┐
              ▼                  ▼                    ▼
┌──────────────────┐  ┌──────────────────┐  ┌──────────────────────────────┐
│  AZURE SQL DB    │  │  NEO4J / COSMOS  │  │  CHROMADB (Vector Store)     │
│  (Warehouse)     │  │  (Knowledge Graph│  │  (RAG Embeddings)            │
│                  │  │                  │  │                              │
│  financial.*     │  │  Entity nodes    │  │  Document chunks             │
│  audit.*         │  │  Engagement nodes│  │  Financial reports           │
│  risk.*          │  │  AUDITED_BY rels │  │  Audit findings              │
│  pipeline.*      │  │  TRANSACTS_WITH  │  │                              │
└─────────┬────────┘  └──────────────────┘  └──────────────────────────────┘
          │                                               │
          └──────────────────────┬────────────────────────┘
                                 ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                    FASTAPI APPLICATION  (src/api/)                          │
│                                                                             │
│   POST /api/v1/ingest/upload       Upload documents                        │
│   POST /api/v1/pipeline/run        Trigger ETL pipeline                    │
│   POST /api/v1/query/ask           RAG question answering                  │
│   POST /api/v1/query/retrieve      Vector similarity search                │
│   POST /api/v1/graph/nodes         Create Knowledge Graph nodes            │
│   GET  /api/v1/graph/related/{id}  Find related entities                   │
│   GET  /health                     Service health check                    │
│   GET  /metrics                    Prometheus metrics                      │
└─────────────────────────────────────────────────────────────────────────────┘

CI/CD: GitHub Actions → Azure Container Registry → AKS
IaC:   Terraform (AKS, Azure SQL, ADLS Gen2, Cosmos DB, Key Vault, ACR)
```

---

## Tech Stack

| Layer | Local Dev | Azure Production |
|-------|-----------|-----------------|
| Object Storage | MinIO | Azure Data Lake Storage Gen2 |
| Relational DB | PostgreSQL 16 | Azure SQL Database (S3/P2) |
| Knowledge Graph | Neo4j Community | Azure Cosmos DB (Gremlin API) |
| Orchestration | Apache Airflow | Azure Data Factory + ADF triggers |
| Vector Store | ChromaDB | Azure AI Search / ChromaDB on AKS |
| Embeddings/LLM | OpenAI | Azure OpenAI Service |
| Container Registry | Local Docker | Azure Container Registry |
| Kubernetes | Docker Compose | Azure Kubernetes Service |
| Secrets | .env file | Azure Key Vault |
| Monitoring | Prometheus + Grafana | Azure Monitor + Log Analytics |
| CI/CD | GitHub Actions local | GitHub Actions → ACR → AKS |

---

## Quick Start (Local Dev)

```bash
# 1. Clone
git clone https://github.com/baobao1408/Financial-Azure-Data-Platform
cd Financial-Azure-Data-Platform

# 2. One-command bootstrap (Docker required)
bash scripts/setup.sh

# 3. Verify
curl http://localhost:8000/health
```

### Service URLs after setup

| Service | URL | Credentials |
|---------|-----|-------------|
| **API Docs (Swagger)** | http://localhost:8000/docs | — |
| **Airflow** | http://localhost:8080 | admin / admin |
| **Neo4j Browser** | http://localhost:7474 | neo4j / neo4j_pass |
| **MinIO Console** | http://localhost:9001 | minio_admin / minio_secret |
| **Grafana** | http://localhost:3000 | admin / grafana_pass |
| **Prometheus** | http://localhost:9090 | — |

---

## Flow – How Data Moves End-to-End

### 1. Document Upload & Ingestion
```
User/System uploads file (PDF/Excel/Word)
  → POST /api/v1/ingest/upload
  → File stored to MinIO/ADLS raw/ zone
  → Record created in financial.documents (status=PENDING)
  → If ingest_to_rag=true: text extracted + chunked + embedded → ChromaDB
```

### 2. Airflow DAG (Daily 02:00 AM)
```
financial_data_pipeline DAG:

[start]
  └─► [ingest_documents]          scan PENDING docs → extract text
          ├─► [run_etl_pipeline]   Bronze→Silver→Gold, persist Parquet
          │       ├─► [compute_financial_ratios]  VAS ratio engine
          │       └─► [detect_gl_anomalies]       Z-score + rule flags
          ├─► [index_knowledge_graph]  Neo4j: Entity/Engagement nodes + rels
          └─► [index_rag_documents]    Embed new docs → ChromaDB
                      └─► [data_quality_checks] Great Expectations rules
                                  └─► [notify_completion]
[end]
```

### 3. ETL – Medallion Architecture
```
Bronze  raw Parquet + lineage cols (_run_id, _source_file, _ingested_at)
  ↓  silver_clean()
Silver  deduplicated, null-dropped, snake_case cols, schema validated
  ↓  gold_aggregate()
Gold    KPI-enriched, business aggregations applied
  ↓  warehouse_load()
Warehouse  financial.* tables (Azure SQL / PostgreSQL)
```

### 4. RAG Query Flow
```
User question
  → POST /api/v1/query/ask
  → EmbeddingService.embed_single(question)  [OpenAI / Azure OpenAI]
  → VectorStore.search()   top-K cosine similarity in ChromaDB
  → Build context from retrieved chunks
  → LLM.chat(system_prompt + context + question)
  → Return answer + source citations
```

### 5. Knowledge Graph
```
financial.entities → Neo4j :Entity nodes
audit.engagements  → Neo4j :Engagement nodes
                       + (:Entity)-[:HAS_ENGAGEMENT]→(:Engagement)
gl_transactions (INTERCOMPANY) → (:Entity)-[:TRANSACTS_WITH]→(:Entity)

Query: GET /api/v1/graph/related/{entity_id}?max_hops=2
       GET /api/v1/graph/path?from_id=VCB&to_id=VIC
```

---

## Database Schema (Financial Domain)

```
financial schema:
  entities           – Vietnamese companies (VCB, HPG, FPT, Vingroup…)
  industry_codes     – VSIC codes (Banking, Insurance, Real Estate…)
  currencies         – ISO 4217 (VND, USD, EUR, SGD…)
  accounts           – Chart of Accounts (VAS/IFRS: Circular 200)
  fiscal_periods     – Annual / Quarterly / Monthly periods
  financial_statements – BS / IS / CF / Equity Changes
  balance_sheet_items  – Line items with VAS codes (B01–B99)
  income_statement_items – VAS codes (01–100)
  cash_flow_items    – Operating / Investing / Financing
  financial_ratios   – 25+ KPIs: ROE, ROA, D/E, Current Ratio…
  gl_transactions    – General Ledger with anomaly_score column
  documents          – Document registry (extraction status, RAG indexed)

audit schema:
  engagements        – KPMG audit engagements (STATUTORY, DUE_DILIGENCE…)
  findings           – Audit findings (MISSTATEMENT, FRAUD_RISK…)

risk schema:
  risk_assessments   – Inherent / Control / Detection risk scores
  risk_factors       – Granular risk items per area

pipeline schema:
  pipeline_runs      – ETL lineage and run history
  data_quality_checks – Per-column rule results
```

---

## ⚙️ Azure Cloud Configuration – Files to Update

> **All Azure credentials must be set before deploying to production.**
> Never commit real secrets to Git. Use Azure Key Vault for production.

### File 1: `.env` (copy from `.env.example`)

```bash
# ── Azure Data Lake Storage Gen2 ──────────────────────────────
# Get from: Azure Portal → Storage Account → Access Keys
AZURE_STORAGE_ACCOUNT_NAME=yourstorageaccount        # ← CHANGE
AZURE_STORAGE_ACCOUNT_KEY=base64key==                # ← CHANGE (or use MI)
ADLS_CONTAINER_RAW=raw
ADLS_CONTAINER_PROCESSED=processed
ADLS_CONTAINER_CURATED=curated

# ── Azure SQL Database ─────────────────────────────────────────
# Get from: Azure Portal → SQL Database → Connection Strings
AZURE_SQL_SERVER=edp-sql-server.database.windows.net # ← CHANGE
AZURE_SQL_DATABASE=edp-warehouse                     # ← CHANGE
AZURE_SQL_USER=edp_admin                             # ← CHANGE
AZURE_SQL_PASSWORD=your-strong-password              # ← CHANGE

# ── Azure Cosmos DB (Gremlin) ──────────────────────────────────
# Get from: Portal → Cosmos DB → Keys
COSMOS_GREMLIN_ENDPOINT=wss://edp-cosmos.gremlin.cosmos.azure.com:443/  # ← CHANGE
COSMOS_GREMLIN_KEY=primarykey==                      # ← CHANGE
COSMOS_DATABASE=edp-graph
COSMOS_GRAPH=entities

# ── Azure Service Principal (for SDK / Terraform) ─────────────
# Create: az ad sp create-for-rbac --name "edp-sp" --role Contributor
AZURE_CLIENT_ID=xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx # ← CHANGE
AZURE_CLIENT_SECRET=your-sp-secret                   # ← CHANGE
AZURE_TENANT_ID=xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx # ← CHANGE
AZURE_SUBSCRIPTION_ID=xxxxxxxx-xxxx-xxxx-xxxx-xxxxxx # ← CHANGE

# ── Azure Key Vault ────────────────────────────────────────────
AZURE_KEY_VAULT_URL=https://edp-kv.vault.azure.net/  # ← CHANGE

# ── Azure OpenAI ──────────────────────────────────────────────
AZURE_OPENAI_ENDPOINT=https://edp-openai.openai.azure.com/  # ← CHANGE
AZURE_OPENAI_API_KEY=your-azure-openai-key           # ← CHANGE
```

### File 2: `infrastructure/terraform/variables.tf`

```hcl
# Change these for your Azure subscription:
variable "resource_group_name"  { default = "rg-edp-prod" }      # ← CHANGE
variable "location"             { default = "southeastasia" }     # Singapore
variable "project_prefix"       { default = "edp" }              # ← CHANGE (must be globally unique for storage)
variable "aad_admin_object_id"  {}   # az ad user show --id your@email --query id
variable "aad_admin_username"   { default = "your@kpmg.com.vn" } # ← CHANGE
```

**Then run:**
```bash
cd infrastructure/terraform
terraform init \
  -backend-config="storage_account_name=<YOUR_TF_STATE_SA>" \
  -backend-config="access_key=<YOUR_SA_KEY>"
terraform plan -var="sql_admin_password=<STRONG_PASS>"
terraform apply
```

### File 3: `.github/workflows/cd.yml` – GitHub Secrets Required

Go to **GitHub → Settings → Secrets → Actions**, add:

| Secret Name | Value | How to get |
|-------------|-------|------------|
| `ACR_LOGIN_SERVER` | `edpregistry.azurecr.io` | `terraform output acr_login_server` |
| `ACR_USERNAME` | service principal client ID | `az ad sp list --display-name edp-sp` |
| `ACR_PASSWORD` | service principal secret | from SP creation |
| `AZURE_CREDENTIALS` | JSON SP credentials | `az ad sp create-for-rbac --sdk-auth` |
| `AKS_CLUSTER_NAME` | `edp-aks` | `terraform output aks_cluster_name` |
| `AKS_RESOURCE_GROUP` | `rg-edp-prod` | your resource group |
| `DATABASE_URL` | Azure SQL connection string | Portal → SQL DB → Connection strings |
| `API_URL` | `https://your-api.azurecontainerapps.io` | after first deploy |

### File 4: `src/config.py` – Production ENV switch

When deploying, set the environment variable:
```bash
ENV=production   # Enables: Azure SDK, Azure SQL, Cosmos DB Gremlin, ACR
```
All Azure clients in `src/ingestion/connectors/` automatically switch from
MinIO/PostgreSQL/Neo4j-local to Azure services when `ENV=production`.

### File 5: `docker-compose.yml` – Not used in production

In production, services run on **AKS** via Kubernetes manifests at
`infrastructure/k8s/`. Docker Compose is local dev only.

---

## Running Tests

```bash
# Unit tests (no services required)
pytest tests/unit/ -v --cov=src

# Integration tests (requires Docker Compose running)
docker-compose up -d postgres-warehouse neo4j minio chromadb
pytest tests/integration/ -v
```

---

## Project Structure

```
enterprise-data-platform/
├── .github/workflows/       CI (ci.yml) + CD (cd.yml)
├── docker/                  Dockerfiles per service
├── docker-compose.yml       Full local stack
├── src/
│   ├── config.py            Pydantic settings (env → config)
│   ├── ingestion/
│   │   ├── connectors/      azure_blob, sql (PostgreSQL/Azure SQL)
│   │   └── extractors/      pdf, excel, word
│   ├── etl/
│   │   └── pipelines/       document_pipeline (Bronze→Silver→Gold)
│   │                        financial_pipeline (ratios + GL anomaly)
│   ├── warehouse/
│   │   ├── migrations/      001_financial_schema.sql (VAS/IFRS schema)
│   │   └── models/          SQLAlchemy ORM models
│   ├── knowledge_graph/     Neo4j client (→ Cosmos Gremlin in prod)
│   ├── rag/                 Embedding + ChromaDB + LLM chain
│   ├── api/                 FastAPI app + routers
│   └── quality/             Data quality validator
├── airflow/dags/            financial_pipeline_dag.py
├── infrastructure/
│   └── terraform/           main.tf, variables.tf, outputs.tf
├── tests/
│   ├── unit/                test_financial_pipeline.py
│   └── integration/         test_api.py
├── scripts/
│   ├── setup.sh             One-command local bootstrap
│   └── seed_financial_data.py  VCB, Vingroup, HPG, FPT, MBB…
├── monitoring/              prometheus.yml
├── requirements*.txt
└── .env.example             All Azure config variables documented
```
