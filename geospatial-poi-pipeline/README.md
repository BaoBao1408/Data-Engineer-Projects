# Geospatial POI Pipeline

A production-grade geospatial data pipeline that extracts, normalizes, deduplicates, and serves **Point of Interest (POI)** data for Ho Chi Minh City from OpenStreetMap — orchestrated by Apache Airflow and stored in PostGIS.

> **Live result:** 18,824 OSM POIs loaded into PostGIS after spatial deduplication of 20,116 raw records.

---

## Architecture Overview

```
OpenStreetMap (Overpass API)
         │
         ▼
  ┌─────────────┐
  │ OSMExtractor│  Overpass QL query → GeoDataFrame (EPSG:4326)
  └──────┬──────┘
         │ raw GeoDataFrame
         ▼
  ┌─────────────────┐
  │  PostGISWriter  │  Bronze layer → raw_poi (JSONB + WKT)
  └──────┬──────────┘
         │
         ▼
  ┌──────────────┐
  │ POINormalizer│  Fix geometry, normalize category/name/address
  └──────┬───────┘
         │ normalized GeoDataFrame
         ▼
  ┌───────────────────┐
  │SpatialDeduplicator│  Phase 1: in-memory (radius 20m)
  │                   │  Phase 2: cross-batch vs PostGIS ST_DWithin
  └──────┬────────────┘
         │ deduplicated GeoDataFrame
         ▼
  ┌─────────────────┐
  │  PostGISWriter  │  Silver layer → poi (PostGIS Point EPSG:4326)
  └──────┬──────────┘
         │
         ▼
  ┌──────────────┐
  │  FastAPI     │  /api/v1/poi/nearby  (ST_DWithin)
  │              │  /api/v1/poi/bbox    (ST_MakeEnvelope)
  └──────────────┘
```

**Airflow DAG** `poi_pipeline_hcmc_daily` runs the full pipeline daily at 02:00 ICT:

```
start → extract_osm → normalize_poi → dedup_poi → load_to_postgis → log_run_summary → end
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Orchestration | Apache Airflow 2.9 (LocalExecutor) |
| Data storage | PostgreSQL 15 + PostGIS 3 |
| Geospatial processing | GeoPandas, Shapely, PyProj |
| Pipeline transport | Apache Parquet (via `/tmp`) + XCom |
| API | FastAPI + psycopg2 |
| Source data | OpenStreetMap via Overpass API |
| Containerization | Docker Compose |
| CI/CD | GitHub Actions |

---

## Project Structure

```
geospatial-poi-pipeline/
├── dags/
│   └── poi_pipeline_dag.py       # Airflow DAG definition
├── src/
│   ├── ingest/
│   │   ├── osm_extractor.py      # Overpass API → GeoDataFrame
│   │   └── geojson_loader.py     # Local GeoJSON loader (for seed/test)
│   ├── transform/
│   │   ├── normalizer.py         # Geometry fix, category/name normalization
│   │   └── deduplicator.py       # Two-phase spatial dedup
│   ├── load/
│   │   └── postgis_writer.py     # Bulk insert to PostGIS (Bronze + Silver)
│   ├── quality/
│   │   └── validator.py          # Data quality checks & QualityReport
│   └── api/
│       └── main.py               # FastAPI spatial query endpoints
├── config/
│   └── settings.py               # Env-var based config (DB, OSM, pipeline)
├── scripts/
│   └── seed_data.py              # Local seed pipeline (bypasses Overpass)
├── sql/
│   └── schema/001_create_tables.sql  # PostGIS schema
├── data/
│   └── seed/hcmc_poi_seed.geojson    # 55 HCMC POIs for local testing
├── docker/
│   ├── Dockerfile.airflow
│   ├── Dockerfile.api
│   └── Dockerfile.pipeline
├── tests/
│   ├── unit/
│   └── integration/
├── docker-compose.yml
└── requirements-*.txt
```

---

## Database Schema

### Bronze layer — `raw_poi`
Raw ingestion table. Stores full payload as JSONB for replayability.

```sql
raw_poi (id, source, source_id, raw_data JSONB, ingested_at, batch_id UUID, is_processed)
```

### Silver layer — `poi`
Normalized, deduplicated POI records with PostGIS geometry.

```sql
poi (id, source, source_id, name, name_en, category, subcategory,
     address_raw, street, ward, district, city, province, country,
     phone, website,
     geom GEOMETRY(Point, 4326),   -- spatial index via GIST
     accuracy_meters, is_active, created_at, updated_at)
```

Key indexes:
- `GIST(geom)` — powers all `ST_DWithin` / `ST_Within` queries
- `idx_poi_category`, `idx_poi_district`, `idx_poi_active`

### Audit tables
- `pipeline_run` — per-run audit log (records read/written/skipped, status, metadata JSONB)
- `dedup_log` — records which POIs were merged and why

---

## Pipeline Flow — Code Level

### 1. `extract_osm` — `src/ingest/osm_extractor.py`

Builds an Overpass QL query for HCMC bounding box `(10.60, 106.40, 11.20, 107.10)`, hits the API with retry + exponential backoff, and parses nodes/ways into a GeoDataFrame. Categories mapped from OSM `amenity`/`shop`/`tourism` tags.

```python
extractor = OSMExtractor(overpass_url, timeout=60)
gdf = extractor.extract(bbox=(10.60, 106.40, 11.20, 107.10))
# → GeoDataFrame, ~20k rows, geometry=Point EPSG:4326
```

Result is written to **Bronze** (`raw_poi`) via `PostGISWriter.write_raw()` and serialized to Parquet in `/tmp` for the next task via XCom.

### 2. `normalize_poi` — `src/transform/normalizer.py`

Five normalization steps run in sequence:

1. **Geometry fix** — drops null geometries, runs `make_valid()` on invalids, filters to Vietnam bbox `(102–110°E, 8–24°N)`
2. **Name normalization** — strips whitespace, applies Unicode NFC normalization (critical for Vietnamese diacritics)
3. **Category normalization** — maps aliases (`"coffee"→"cafe"`, `"drugstore"→"pharmacy"`, etc.)
4. **Address normalization** — extracts district from `address_raw` using Vietnamese regex patterns when `district` is null; defaults `city` to `"Ho Chi Minh City"`
5. **Drop invalid** — removes records with no name AND no meaningful category

### 3. `dedup_poi` — `src/transform/deduplicator.py`

Two-phase deduplication:

**Phase 1 — In-memory (within current batch):**
- Groups by category to reduce comparisons
- Reprojects to EPSG:3857 (metric) for accurate distance calculation
- For each pair within 20m radius: keeps the record with more non-null metadata fields (`_completeness` score)
- Removed 1,292 duplicates from 20,116 → 18,824 records

**Phase 2 — Cross-batch (against existing DB):**
- For each record, queries PostGIS via `ST_DWithin(..., 20m)` using the spatial index
- Records with a nearby existing POI in the same category are skipped
- Uses `psycopg2.extras.RealDictCursor` for efficient batch queries

Result serialized to Parquet and pushed via XCom.

### 4. `load_to_postgis` — `src/load/postgis_writer.py`

Bulk insert using `psycopg2.extras.execute_values` with a custom template that calls `ST_SetSRID(ST_GeomFromText(%s), 4326)` inline — converting WKT geometry to PostGIS without a separate round-trip. `ON CONFLICT DO NOTHING` ensures idempotency.

```python
# 18,824 rows inserted in ~6 seconds
writer.write_poi(gdf)  # batch_size=500 per execute_values page
```

### 5. `log_run_summary`

Pulls XCom values from all upstream tasks and writes a single audit record to `pipeline_run` with full metadata (dag_run_id, execution_date, records at each stage).

---

## API Endpoints — `src/api/main.py`

FastAPI app with PostGIS spatial queries.

### `GET /api/v1/poi/nearby`
Find POIs within a radius using `ST_DWithin` on the spatial index.

```
GET /api/v1/poi/nearby?lon=106.6977&lat=10.7769&radius_m=500&category=cafe&limit=20
```

Response: GeoJSON FeatureCollection with `distance_m` for each feature.

### `GET /api/v1/poi/bbox`
Return all POIs in a map viewport bounding box using `&&` bbox operator.

```
GET /api/v1/poi/bbox?min_lon=106.68&min_lat=10.76&max_lon=106.72&max_lat=10.80
```

### `GET /api/v1/stats`
POI counts grouped by category and city.

### `GET /health`
Health check — also verifies PostGIS connection and returns PostGIS version.

---

## Quick Start

### Prerequisites
- Docker Desktop
- PostgreSQL 15 with PostGIS extension (local, port 5432)

### 1. Clone & configure

```bash
git clone https://github.com/baoquocnguyen1408/geospatial-poi-pipeline
cd geospatial-poi-pipeline
cp .env.example .env
# Edit .env: DB_PASSWORD, DB_NAME, etc.
```

### 2. Initialize database schema

```sql
-- Run in pgAdmin or psql against your 'geospatial' database
\i sql/schema/001_create_tables.sql
```

### 3. Start services

```bash
docker compose up -d
```

Services started:
- `airflow-webserver` → http://localhost:8080 (admin/admin)
- `geospatial_api` → http://localhost:8000
- `airflow-scheduler` (background)
- `airflow_postgres` (Airflow metadata DB, port 5434)

### 4. Seed test data (optional — bypasses Overpass API)

```bash
docker compose run --rm pipeline python scripts/seed_data.py
# Loads 55 HCMC POIs from data/seed/hcmc_poi_seed.geojson
```

Flags:
```bash
--dry-run   # Print normalized records, skip DB write
--clear     # Truncate poi/raw_poi before seeding
```

### 5. Trigger full pipeline

In Airflow UI → DAGs → `poi_pipeline_hcmc_daily` → click **▶ Trigger DAG**.

Or wait for scheduled run at 02:00 ICT daily.

Expected result: ~18–20k POIs loaded into `poi` table depending on current OSM data.

### 6. Verify

```sql
SELECT COUNT(*) FROM poi;
SELECT category, COUNT(*) FROM poi GROUP BY category ORDER BY count DESC;
SELECT district, COUNT(*) FROM poi GROUP BY district ORDER BY count DESC LIMIT 10;
```

---

## Configuration

All config via environment variables (`config/settings.py`):

| Variable | Default | Description |
|---|---|---|
| `DB_HOST` | `localhost` | PostgreSQL host |
| `DB_PORT` | `5432` | PostgreSQL port |
| `DB_NAME` | `geospatial` | Database name |
| `DB_USER` | `postgres` | Database user |
| `DB_PASSWORD` | `123456` | Database password |
| `OSM_OVERPASS_URL` | `https://overpass-api.de/api/interpreter` | Overpass API endpoint |
| `OSM_TIMEOUT` | `60` | Overpass request timeout (seconds) |
| `TARGET_BBOX` | `10.60,106.40,11.20,107.10` | Extraction bounding box (S,W,N,E) |
| `TARGET_CITY` | `Ho Chi Minh City` | Default city for normalization |
| `DEDUP_RADIUS_M` | `20` | Spatial dedup radius in meters |
| `BATCH_SIZE` | `500` | `execute_values` page size |

---

## Performance Notes

| Step | Records | Duration | Notes |
|---|---|---|---|
| `extract_osm` | 20,116 raw | ~15 min | Overpass API response time |
| `normalize_poi` | 20,116 → 20,116 | ~8 sec | Pandas vectorized ops |
| `dedup_poi` phase 1 | 20,116 → 18,824 | ~11 min | O(n²) per category group |
| `dedup_poi` phase 2 | 18,824 cross-batch | ~22 min | 18,824 × ST_DWithin queries |
| `load_to_postgis` | 18,824 inserts | ~6 sec | execute_values batch 500/page |

**Optimization opportunity:** Phase 2 cross-batch dedup makes one DB round-trip per record. For scale beyond 50k records, replace with a single bulk `ST_DWithin` spatial join using a PostGIS temp table.

---

## Data Quality

`src/quality/validator.py` runs `POIValidator` checks before loading:

- Zero tolerance for null geometry
- Max 5% invalid geometries (auto-fixed via `make_valid()`)
- Max 10% records outside Vietnam bounding box
- Duplicate `source_id` detection per batch

Results reported in `QualityReport` dataclass and logged to `pipeline_run.metadata`.

---

## Running Tests

```bash
# Unit tests
docker compose run --rm pipeline python -m pytest tests/unit/ -v

# Integration tests (requires live DB)
docker compose run --rm pipeline python -m pytest tests/integration/ -v
```

---

## CI/CD

GitHub Actions workflows in `.github/workflows/`:

- `ci.yml` — runs on PR: lint, unit tests, Docker build check
- `cd.yml` — runs on `main` merge: build and push images

---

## Extending to Other Cities

Change `TARGET_BBOX` and `TARGET_CITY` in `.env`:

```bash
# Bangkok
TARGET_BBOX=13.50,100.30,14.00,100.90
TARGET_CITY=Bangkok

# Hanoi
TARGET_BBOX=20.90,105.70,21.20,106.00
TARGET_CITY=Hanoi
```

No code changes required — the pipeline and schema are city-agnostic.