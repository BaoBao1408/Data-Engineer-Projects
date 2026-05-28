#!/usr/bin/env bash
# ══════════════════════════════════════════════════════════════════════
# Enterprise Data Platform – Local Dev Bootstrap
# Usage: bash scripts/setup.sh
# ══════════════════════════════════════════════════════════════════════
set -euo pipefail

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
info()    { echo -e "${GREEN}[INFO]${NC} $*"; }
warn()    { echo -e "${YELLOW}[WARN]${NC} $*"; }
error()   { echo -e "${RED}[ERROR]${NC} $*"; exit 1; }

# ─── Prerequisites ────────────────────────────────────────────────────
info "Checking prerequisites..."
command -v docker      >/dev/null 2>&1 || error "Docker not installed"
command -v docker-compose >/dev/null 2>&1 || command -v docker >/dev/null 2>&1 || error "Docker Compose not installed"
command -v python3     >/dev/null 2>&1 || error "Python 3 not installed"
info "Prerequisites OK"

# ─── Environment file ────────────────────────────────────────────────
if [ ! -f .env ]; then
  cp .env.example .env
  info "Created .env from .env.example – please review and update values"
fi

# ─── Python virtual environment ──────────────────────────────────────
if [ ! -d .venv ]; then
  info "Creating Python virtual environment..."
  python3 -m venv .venv
fi
source .venv/bin/activate

info "Installing Python dependencies..."
pip install --quiet --upgrade pip
pip install --quiet -r requirements.txt \
                    -r requirements-api.txt \
                    -r requirements-ingestion.txt \
                    -r requirements-etl.txt
info "Python deps installed"

# ─── Pull Docker images ───────────────────────────────────────────────
info "Pulling Docker images (this may take a few minutes)..."
docker-compose pull --quiet

# ─── Start infrastructure services first ─────────────────────────────
info "Starting infrastructure services..."
docker-compose up -d \
  postgres-warehouse postgres-airflow \
  neo4j minio chromadb redis

# ─── Wait for services ───────────────────────────────────────────────
info "Waiting for PostgreSQL to be ready..."
until docker-compose exec -T postgres-warehouse \
  pg_isready -U edp_user -d edp_warehouse >/dev/null 2>&1; do
  sleep 2; echo -n "."
done; echo ""
info "PostgreSQL ready"

info "Waiting for Neo4j to be ready..."
until curl -sf http://localhost:7474 >/dev/null 2>&1; do
  sleep 3; echo -n "."; done; echo ""
info "Neo4j ready"

# ─── Run database migrations ─────────────────────────────────────────
info "Running database migrations..."
PGPASSWORD=edp_pass psql \
  -h localhost -p 5432 \
  -U edp_user -d edp_warehouse \
  -f src/warehouse/migrations/001_financial_schema.sql \
  >/dev/null 2>&1 && info "Migrations applied" || warn "Migrations may have already been applied"

# ─── Seed sample data ────────────────────────────────────────────────
info "Seeding sample financial data..."
python scripts/seed_financial_data.py && info "Seed data loaded" || warn "Seed failed – check logs"

# ─── Start application services ──────────────────────────────────────
info "Starting application services (API + Airflow)..."
docker-compose up -d api airflow-init
sleep 5
docker-compose up -d airflow-webserver airflow-scheduler airflow-worker
docker-compose up -d prometheus grafana

# ─── Health check ─────────────────────────────────────────────────────
info "Running health check..."
sleep 10
if curl -sf http://localhost:8000/health >/dev/null 2>&1; then
  info "✅ API is healthy: http://localhost:8000"
else
  warn "API not yet ready – run: docker-compose logs api"
fi

# ─── Summary ─────────────────────────────────────────────────────────
echo ""
echo -e "${GREEN}════════════════════════════════════════════════════${NC}"
echo -e "${GREEN}  Enterprise Data Platform – Local Dev Ready!${NC}"
echo -e "${GREEN}════════════════════════════════════════════════════${NC}"
echo ""
echo "  Service        URL                          Credentials"
echo "  ─────────────────────────────────────────────────────────"
echo "  API Docs       http://localhost:8000/docs"
echo "  Airflow        http://localhost:8080          admin / admin"
echo "  Neo4j Browser  http://localhost:7474          neo4j / neo4j_pass"
echo "  MinIO Console  http://localhost:9001          minio_admin / minio_secret"
echo "  Grafana        http://localhost:3000          admin / grafana_pass"
echo "  Prometheus     http://localhost:9090"
echo ""
echo "  Next steps:"
echo "    1. Upload a financial PDF: POST http://localhost:8000/api/v1/ingest/upload"
echo "    2. Trigger Airflow DAG:    http://localhost:8080 → financial_data_pipeline"
echo "    3. Query the RAG:          POST http://localhost:8000/api/v1/query/ask"
echo "    4. Explore KG:             http://localhost:7474"
echo ""
