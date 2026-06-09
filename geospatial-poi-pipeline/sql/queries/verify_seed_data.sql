-- ============================================================
--  sql/queries/verify_seed_data.sql
--  Chạy trong pgAdmin sau khi seed để verify pipeline hoạt động đúng
--  Kết nối vào database "geospatial" → Query Tool
-- ============================================================

-- ── 1. Tổng số POI đã load ────────────────────────────────────
SELECT COUNT(*) AS total_poi FROM poi;
-- Expected: ~50-55 records

-- ── 2. Phân bố theo category ──────────────────────────────────
SELECT
    category,
    COUNT(*) AS count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) AS pct
FROM poi
GROUP BY category
ORDER BY count DESC;

-- ── 3. Phân bố theo quận ──────────────────────────────────────
SELECT
    COALESCE(district, '(unknown)') AS district,
    COUNT(*) AS count
FROM poi
GROUP BY district
ORDER BY count DESC;

-- ── 4. Kiểm tra geometry hợp lệ ──────────────────────────────
SELECT
    COUNT(*) AS total,
    COUNT(*) FILTER (WHERE ST_IsValid(geom)) AS valid_geom,
    COUNT(*) FILTER (WHERE geom IS NULL) AS null_geom,
    ST_AsText(ST_Extent(geom)) AS bbox
FROM poi;

-- ── 5. Spatial query: POI trong vòng 1km từ Bến Thành ────────
SELECT
    name,
    category,
    district,
    ROUND(
        ST_Distance(
            geom::geography,
            ST_SetSRID(ST_MakePoint(106.6977, 10.7769), 4326)::geography
        )::numeric, 0
    ) AS distance_m
FROM poi
WHERE ST_DWithin(
    geom::geography,
    ST_SetSRID(ST_MakePoint(106.6977, 10.7769), 4326)::geography,
    1000  -- 1km radius
)
ORDER BY distance_m;

-- ── 6. Spatial query: đếm POI theo category trong bán kính 2km từ Q1 ──
SELECT
    category,
    COUNT(*) AS count
FROM poi
WHERE ST_DWithin(
    geom::geography,
    ST_SetSRID(ST_MakePoint(106.7000, 10.7757), 4326)::geography,
    2000  -- 2km
)
GROUP BY category
ORDER BY count DESC;

-- ── 7. Kiểm tra raw_poi (Bronze layer) ───────────────────────
SELECT
    source,
    COUNT(*) AS count,
    MIN(ingested_at) AS first_ingested,
    MAX(ingested_at) AS last_ingested
FROM raw_poi
GROUP BY source;

-- ── 8. Pipeline audit log ─────────────────────────────────────
SELECT
    run_type,
    status,
    records_read,
    records_written,
    records_skipped,
    started_at,
    finished_at,
    EXTRACT(EPOCH FROM (finished_at - started_at))::int AS elapsed_s
FROM pipeline_run
ORDER BY started_at DESC
LIMIT 10;

-- ── 9. Kiểm tra spatial index được dùng (EXPLAIN ANALYZE) ────
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT name, category
FROM poi
WHERE ST_DWithin(
    geom::geography,
    ST_SetSRID(ST_MakePoint(106.7000, 10.7757), 4326)::geography,
    500
);
-- Kết quả nên thấy: "Index Scan using idx_poi_geom"

-- ── 10. POI có đầy đủ thông tin nhất ─────────────────────────
SELECT
    name, category, district,
    (CASE WHEN phone IS NOT NULL THEN 1 ELSE 0 END +
     CASE WHEN website IS NOT NULL THEN 1 ELSE 0 END +
     CASE WHEN address_raw IS NOT NULL THEN 1 ELSE 0 END +
     CASE WHEN ward IS NOT NULL THEN 1 ELSE 0 END) AS completeness_score
FROM poi
ORDER BY completeness_score DESC, name
LIMIT 20;
