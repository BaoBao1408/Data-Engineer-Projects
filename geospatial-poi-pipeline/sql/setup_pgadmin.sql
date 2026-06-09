
-- ============================================================
--  CHẠY FILE NÀY TRONG pgAdmin TRƯỚC KHI docker compose up
--  Kết nối vào server "PostgreSQL 5432" → Query Tool
-- ============================================================

-- ── Bước 1: Tạo database cho pipeline ────────────────────────
CREATE DATABASE geospatial
    ENCODING 'UTF8'
    LC_COLLATE 'en_US.UTF-8'
    LC_CTYPE 'en_US.UTF-8';

-- ── Bước 2: Tạo database cho Airflow metadata ────────────────
CREATE DATABASE airflow
    ENCODING 'UTF8'
    LC_COLLATE 'en_US.UTF-8'
    LC_CTYPE 'en_US.UTF-8';

-- ── Bước 3: Kết nối vào database "geospatial" rồi chạy phần dưới ──
-- (Trong pgAdmin: click vào database "geospatial" → Query Tool)

-- Enable PostGIS extension
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS postgis_topology;

-- Verify PostGIS hoạt động
SELECT PostGIS_Version();
-- Expected: "3.x USE_GEOS=1 USE_PROJ=1 USE_STATS=1"

-- ── Bước 4: Chạy schema migration ────────────────────────────
-- Copy nội dung file sql/schema/001_create_tables.sql và paste vào Query Tool
-- Hoặc dùng pgAdmin: Tools → Query Tool → Open File → chọn 001_create_tables.sql

-- ── Kiểm tra sau khi chạy ─────────────────────────────────────
SELECT table_name FROM information_schema.tables
WHERE table_schema = 'public'
ORDER BY table_name;
-- Expected: dedup_log, pipeline_run, point_address, poi, raw_poi, street_imagery

-- Verify spatial index tồn tại
SELECT indexname, tablename FROM pg_indexes
WHERE indexname LIKE 'idx_%_geom';
-- Expected: idx_poi_geom, idx_pa_geom, idx_imagery_geom

Bước 4 — Start Docker: 
docker compose down
docker compose build --no-cache
docker compose up -d postgres-airflow
Start-Sleep -Seconds 10
docker compose up -d airflow-init
Start-Sleep -Seconds 20
docker compose up -d airflow-webserver airflow-scheduler api
docker compose ps

Bước 5 — Verify kết nối:
Start-Sleep -Seconds 30   # chờ health check pass
curl http://localhost:8000/health
# Expected: {"status":"ok","postgis_version":"3.x..."}

----------------------------------------------------------------------------------

Bước tiếp theo — Verify & Run
Bước 1 — Test API health:
powershellStart-Sleep -Seconds 30   # chờ health check pass
curl http://localhost:8000/health
# Expected: {"status":"ok","postgis_version":"3.x..."}
Bước 2 — Mở Airflow UI:
http://localhost:8080
Username: admin
Password: admin
Vào DAGs → tìm poi_pipeline_hcmc_daily → Toggle ON → nhấn ▶ Run
Bước 3 — Chạy pipeline thủ công:
powershelldocker compose --profile manual run --rm pipeline
Bước 4 — Verify data trong pgAdmin:
sql-- Chạy trong pgAdmin → geospatial database
SELECT category, COUNT(*) as count
FROM poi
GROUP BY category
ORDER BY count DESC;

docker compose run --rm --build pipeline python scripts/seed_data.py