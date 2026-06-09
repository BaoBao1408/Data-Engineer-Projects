"""
tests/integration/test_pipeline_integration.py
Integration tests — require a running PostGIS instance.
Run with: pytest tests/integration/ -v

These tests use the DB credentials from environment variables.
In CI, a PostGIS service container is spun up automatically.
"""
import os
import pytest
import geopandas as gpd
import psycopg2
from shapely.geometry import Point

# Skip all integration tests if no DB is available
pytestmark = pytest.mark.skipif(
    not os.getenv("DB_HOST"),
    reason="DB_HOST not set — skipping integration tests"
)

DB_DSN = (
    f"postgresql://{os.getenv('DB_USER', 'geouser')}:"
    f"{os.getenv('DB_PASSWORD', 'geopassword')}@"
    f"{os.getenv('DB_HOST', 'localhost')}:"
    f"{os.getenv('DB_PORT', '5432')}/"
    f"{os.getenv('DB_NAME', 'geospatial')}"
)


@pytest.fixture(scope="session")
def db_conn():
    conn = psycopg2.connect(DB_DSN)
    yield conn
    conn.close()


@pytest.fixture(scope="session")
def sample_gdf():
    """Minimal GeoDataFrame with 3 HCMC POIs."""
    return gpd.GeoDataFrame({
        "source": ["test", "test", "test"],
        "source_id": ["test/1", "test/2", "test/3"],
        "name": ["Test Cafe", "Test Restaurant", "Test Hospital"],
        "name_en": ["Test Cafe", None, "Test Hospital"],
        "category": ["cafe", "restaurant", "hospital"],
        "address_raw": ["123 Test St, Q1", None, "789 Test Rd, Q3"],
        "district": ["Quan 1", "Quan 2", "Quan 3"],
        "city": ["Ho Chi Minh City"] * 3,
        "country": ["VN"] * 3,
        "geometry": [
            Point(106.6853, 10.7769),
            Point(106.7000, 10.7800),
            Point(106.6720, 10.7650),
        ],
    }, geometry="geometry", crs="EPSG:4326")


class TestPostGISWriter:

    def test_write_poi(self, sample_gdf, db_conn):
        from src.load.postgis_writer import PostGISWriter
        writer = PostGISWriter(DB_DSN)
        written = writer.write_poi(sample_gdf)
        assert written == 3

    def test_spatial_index_exists(self, db_conn):
        with db_conn.cursor() as cur:
            cur.execute("""
                SELECT indexname FROM pg_indexes
                WHERE tablename = 'poi' AND indexname = 'idx_poi_geom';
            """)
            result = cur.fetchone()
        assert result is not None, "Spatial index idx_poi_geom not found"

    def test_postgis_version(self, db_conn):
        with db_conn.cursor() as cur:
            cur.execute("SELECT PostGIS_Version();")
            version = cur.fetchone()[0]
        assert version is not None
        assert "3." in version or "2." in version


class TestSpatialQueries:

    def test_st_dwithin_query(self, db_conn, sample_gdf):
        """Verify ST_DWithin returns nearby POIs."""
        from src.load.postgis_writer import PostGISWriter
        writer = PostGISWriter(DB_DSN)
        writer.write_poi(sample_gdf)

        # Query near the first test POI (106.6853, 10.7769)
        with db_conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*) FROM poi
                WHERE ST_DWithin(
                    geom::geography,
                    ST_SetSRID(ST_MakePoint(106.6853, 10.7769), 4326)::geography,
                    1000  -- 1km radius
                )
                AND source = 'test';
            """)
            count = cur.fetchone()[0]

        assert count >= 1, "ST_DWithin should find at least 1 nearby test POI"

    def test_geojson_export(self, db_conn):
        """Verify GeoJSON export query works."""
        with db_conn.cursor() as cur:
            cur.execute("""
                SELECT ST_AsGeoJSON(geom)::json
                FROM poi
                WHERE source = 'test'
                LIMIT 1;
            """)
            result = cur.fetchone()

        assert result is not None
        geojson = result[0]
        assert geojson["type"] == "Point"
        assert len(geojson["coordinates"]) == 2


class TestDeduplicator:

    def test_dedup_finds_nearby(self, sample_gdf):
        """Verify SpatialDeduplicator uses ST_DWithin correctly."""
        from src.load.postgis_writer import PostGISWriter
        from src.transform.deduplicator import SpatialDeduplicator

        # First write the base records
        writer = PostGISWriter(DB_DSN)
        writer.write_poi(sample_gdf)

        # Now try to insert near-duplicates (same location ± 5m)
        near_dup = gpd.GeoDataFrame({
            "source": ["test_dup"],
            "source_id": ["test/dup/1"],
            "name": ["Test Cafe Duplicate"],
            "category": ["cafe"],
            "city": ["Ho Chi Minh City"],
            "country": ["VN"],
            "geometry": [Point(106.68531, 10.77691)],  # ~1.5m away
        }, geometry="geometry", crs="EPSG:4326")

        dedup = SpatialDeduplicator(DB_DSN, radius_meters=20)
        new_gdf, dup_count = dedup.filter_new_records(near_dup)

        assert dup_count >= 1, "Deduplicator should detect 1 near-duplicate"
