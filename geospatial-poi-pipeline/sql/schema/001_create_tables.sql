-- ============================================================
--  001_create_tables.sql
--  Core schema for Geospatial POI Pipeline
-- ============================================================

-- ── Raw ingestion layer (Bronze) ─────────────────────────────
CREATE TABLE IF NOT EXISTS raw_poi (
    id              BIGSERIAL PRIMARY KEY,
    source          VARCHAR(50)  NOT NULL,           -- 'osm', 'geojson', 'api'
    source_id       VARCHAR(100),                    -- original ID from source
    raw_data        JSONB        NOT NULL,            -- full raw payload
    ingested_at     TIMESTAMPTZ  DEFAULT NOW(),
    batch_id        UUID         DEFAULT gen_random_uuid(),
    is_processed    BOOLEAN      DEFAULT FALSE
);

CREATE INDEX idx_raw_poi_source    ON raw_poi(source);
CREATE INDEX idx_raw_poi_batch     ON raw_poi(batch_id);
CREATE INDEX idx_raw_poi_processed ON raw_poi(is_processed);

-- ── Normalized POI layer (Silver) ────────────────────────────
CREATE TABLE IF NOT EXISTS poi (
    id              BIGSERIAL    PRIMARY KEY,
    source          VARCHAR(50)  NOT NULL,
    source_id       VARCHAR(100),
    name            TEXT,
    name_en         TEXT,
    category        VARCHAR(100),                    -- restaurant, cafe, hospital, etc.
    subcategory     VARCHAR(100),
    address_raw     TEXT,
    street          TEXT,
    ward            TEXT,                            -- phường
    district        TEXT,                            -- quận/huyện
    city            TEXT,
    province        TEXT,
    country         VARCHAR(10)  DEFAULT 'VN',
    postal_code     VARCHAR(20),
    phone           TEXT,
    website         TEXT,
    geom            GEOMETRY(Point, 4326) NOT NULL, -- WGS84 lon/lat
    accuracy_meters FLOAT,                           -- GPS accuracy
    is_verified     BOOLEAN      DEFAULT FALSE,
    is_active       BOOLEAN      DEFAULT TRUE,
    raw_poi_id      BIGINT       REFERENCES raw_poi(id),
    created_at      TIMESTAMPTZ  DEFAULT NOW(),
    updated_at      TIMESTAMPTZ  DEFAULT NOW()
);

-- Spatial index — critical for ST_DWithin / ST_Within queries
CREATE INDEX idx_poi_geom      ON poi USING GIST(geom);
CREATE INDEX idx_poi_category  ON poi(category);
CREATE INDEX idx_poi_city      ON poi(city);
CREATE INDEX idx_poi_district  ON poi(district);
CREATE INDEX idx_poi_active    ON poi(is_active) WHERE is_active = TRUE;

-- ── Point Address layer (Silver) ─────────────────────────────
CREATE TABLE IF NOT EXISTS point_address (
    id              BIGSERIAL    PRIMARY KEY,
    full_address    TEXT         NOT NULL,
    street_number   TEXT,
    street_name     TEXT,
    ward            TEXT,
    district        TEXT,
    city            TEXT,
    province        TEXT,
    country         VARCHAR(10)  DEFAULT 'VN',
    geom            GEOMETRY(Point, 4326) NOT NULL,
    source          VARCHAR(50),
    confidence      FLOAT,                           -- geocoding confidence 0-1
    created_at      TIMESTAMPTZ  DEFAULT NOW()
);

CREATE INDEX idx_pa_geom     ON point_address USING GIST(geom);
CREATE INDEX idx_pa_district ON point_address(district);

-- ── Street Imagery metadata (Silver) ─────────────────────────
CREATE TABLE IF NOT EXISTS street_imagery (
    id              BIGSERIAL    PRIMARY KEY,
    capture_id      VARCHAR(100) UNIQUE NOT NULL,
    file_path       TEXT         NOT NULL,           -- S3 path or local path
    captured_at     TIMESTAMPTZ,
    heading_deg     FLOAT,                           -- camera heading 0-360
    pitch_deg       FLOAT,                           -- camera pitch
    roll_deg        FLOAT,
    altitude_m      FLOAT,
    geom            GEOMETRY(Point, 4326) NOT NULL,
    is_360          BOOLEAN      DEFAULT FALSE,
    resolution_px   VARCHAR(20),
    quality_score   FLOAT,                           -- 0-1 from QA model
    is_qc_passed    BOOLEAN      DEFAULT FALSE,
    created_at      TIMESTAMPTZ  DEFAULT NOW()
);

CREATE INDEX idx_imagery_geom       ON street_imagery USING GIST(geom);
CREATE INDEX idx_imagery_captured   ON street_imagery(captured_at);
CREATE INDEX idx_imagery_qc_passed  ON street_imagery(is_qc_passed);

-- ── Deduplication log ────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dedup_log (
    id              BIGSERIAL    PRIMARY KEY,
    kept_poi_id     BIGINT       REFERENCES poi(id),
    removed_source  VARCHAR(50),
    removed_source_id VARCHAR(100),
    distance_m      FLOAT,
    reason          TEXT,
    deduped_at      TIMESTAMPTZ  DEFAULT NOW()
);

-- ── Pipeline run audit log ───────────────────────────────────
CREATE TABLE IF NOT EXISTS pipeline_run (
    id              UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    dag_id          VARCHAR(100),
    run_type        VARCHAR(50)  NOT NULL,           -- 'osm_extract', 'normalize', 'dedup', 'full'
    status          VARCHAR(20)  NOT NULL,           -- 'running', 'success', 'failed'
    started_at      TIMESTAMPTZ  DEFAULT NOW(),
    finished_at     TIMESTAMPTZ,
    records_read    INTEGER      DEFAULT 0,
    records_written INTEGER      DEFAULT 0,
    records_skipped INTEGER      DEFAULT 0,
    error_message   TEXT,
    metadata        JSONB
);

-- ── Auto-update updated_at trigger ───────────────────────────
CREATE OR REPLACE FUNCTION update_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER poi_updated_at
    BEFORE UPDATE ON poi
    FOR EACH ROW EXECUTE FUNCTION update_updated_at();
