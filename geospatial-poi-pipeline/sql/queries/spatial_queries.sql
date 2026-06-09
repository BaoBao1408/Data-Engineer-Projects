-- ============================================================
--  spatial_queries.sql
--  Common PostGIS query patterns for POI and Address data
-- ============================================================

-- ── 1. Proximity search: find POIs within N meters ───────────
-- ST_DWithin uses spatial index — much faster than ST_Distance filter
SELECT
    id,
    name,
    category,
    district,
    ST_AsGeoJSON(geom)::json AS geometry,
    ST_Distance(
        geom::geography,
        ST_SetSRID(ST_MakePoint(:lon, :lat), 4326)::geography
    ) AS distance_m
FROM poi
WHERE
    is_active = TRUE
    AND ST_DWithin(
        geom::geography,
        ST_SetSRID(ST_MakePoint(:lon, :lat), 4326)::geography,
        :radius_meters           -- e.g. 500
    )
ORDER BY distance_m
LIMIT :limit;


-- ── 2. Bounding box search ───────────────────────────────────
-- Faster than radius for map viewport queries
SELECT
    id, name, category,
    ST_AsGeoJSON(geom)::json AS geometry
FROM poi
WHERE
    is_active = TRUE
    AND geom && ST_MakeEnvelope(:min_lon, :min_lat, :max_lon, :max_lat, 4326)
ORDER BY name;


-- ── 3. Spatial join: count POIs per district ─────────────────
SELECT
    district,
    category,
    COUNT(*) AS poi_count
FROM poi
WHERE is_active = TRUE
GROUP BY district, category
ORDER BY district, poi_count DESC;


-- ── 4. Deduplicate: find POIs within 20m of each other ───────
-- Used in deduplication step to detect near-duplicates
SELECT
    a.id        AS poi_a,
    b.id        AS poi_b,
    a.name      AS name_a,
    b.name      AS name_b,
    a.source    AS source_a,
    b.source    AS source_b,
    ST_Distance(a.geom::geography, b.geom::geography) AS distance_m
FROM poi a
JOIN poi b ON
    a.id < b.id
    AND ST_DWithin(a.geom::geography, b.geom::geography, 20)
    AND a.category = b.category
WHERE a.is_active = TRUE AND b.is_active = TRUE
ORDER BY distance_m;


-- ── 5. Street imagery nearest to a POI ───────────────────────
SELECT
    si.capture_id,
    si.file_path,
    si.captured_at,
    si.heading_deg,
    ST_Distance(si.geom::geography, p.geom::geography) AS distance_m
FROM street_imagery si
CROSS JOIN LATERAL (
    SELECT geom FROM poi WHERE id = :poi_id
) p
WHERE
    si.is_qc_passed = TRUE
    AND ST_DWithin(si.geom::geography, p.geom::geography, 100)
ORDER BY distance_m
LIMIT 5;


-- ── 6. Export as GeoJSON FeatureCollection ───────────────────
SELECT json_build_object(
    'type', 'FeatureCollection',
    'features', json_agg(
        json_build_object(
            'type', 'Feature',
            'geometry', ST_AsGeoJSON(geom)::json,
            'properties', json_build_object(
                'id', id,
                'name', name,
                'category', category,
                'district', district,
                'city', city
            )
        )
    )
) AS geojson
FROM poi
WHERE
    is_active = TRUE
    AND city = :city_name;
