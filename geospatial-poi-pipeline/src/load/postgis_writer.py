"""
src/load/postgis_writer.py
Bulk-insert normalized POI GeoDataFrame into PostGIS.
Uses COPY for performance, falls back to INSERT on error.
"""
import json
import logging
import uuid
from typing import Optional
import geopandas as gpd
import psycopg2
import psycopg2.extras

logger = logging.getLogger(__name__)


class PostGISWriter:
    """
    Write GeoDataFrame records to PostGIS poi table.

    Performance notes:
    - Uses psycopg2 execute_values for batch inserts (~10x faster than single INSERT)
    - Spatial index on geom is maintained automatically by PostgreSQL
    - Logs audit record to pipeline_run table
    """

    def __init__(self, conn_string: str, batch_size: int = 500):
        self.conn_string = conn_string
        self.batch_size = batch_size

    def write_raw(self, gdf: gpd.GeoDataFrame, batch_id: Optional[str] = None) -> int:
        """
        Write raw GeoDataFrame to raw_poi (Bronze layer).
        Returns number of rows inserted.
        """
        if gdf.empty:
            return 0

        bid = batch_id or str(uuid.uuid4())
        rows = []
        for _, row in gdf.iterrows():
            raw_data = {
                k: v for k, v in row.items()
                if k not in ("geometry",) and not str(k).startswith("_")
            }
            # Serialize geometry as WKT for raw storage
            raw_data["geometry_wkt"] = row.geometry.wkt if row.geometry else None
            rows.append((
                row.get("source", "unknown"),
                row.get("source_id"),
                json.dumps(raw_data, default=str),
                bid,
            ))

        with psycopg2.connect(self.conn_string) as conn:
            with conn.cursor() as cur:
                psycopg2.extras.execute_values(
                    cur,
                    """
                    INSERT INTO raw_poi (source, source_id, raw_data, batch_id)
                    VALUES %s
                    """,
                    rows,
                    page_size=self.batch_size
                )
            conn.commit()

        logger.info(f"Wrote {len(rows)} rows to raw_poi (batch {bid})")
        return len(rows)

    def write_poi(self, gdf: gpd.GeoDataFrame) -> int:
        """
        Write normalized GeoDataFrame to poi table (Silver layer).
        Geometry stored as PostGIS Point in EPSG:4326.
        Returns number of rows inserted.
        """
        if gdf.empty:
            logger.warning("Empty GeoDataFrame, nothing to write")
            return 0

        rows = []
        for _, row in gdf.iterrows():
            if row.geometry is None:
                continue

            # Convert Point geometry to WKT for ST_GeomFromText
            geom_wkt = row.geometry.wkt  # e.g. "POINT (106.6853 10.7769)"

            rows.append((
                row.get("source", "osm"),
                row.get("source_id"),
                row.get("name"),
                row.get("name_en"),
                row.get("category", "other"),
                row.get("subcategory"),
                row.get("address_raw"),
                row.get("street"),
                row.get("ward"),
                row.get("district"),
                row.get("city", "Ho Chi Minh City"),
                row.get("province"),
                row.get("country", "VN"),
                row.get("postal_code"),
                row.get("phone"),
                row.get("website"),
                geom_wkt,
                row.get("accuracy_meters"),
            ))

        inserted = 0
        with psycopg2.connect(self.conn_string) as conn:
            with conn.cursor() as cur:
                # Batch insert using execute_values
                psycopg2.extras.execute_values(
                    cur,
                    """
                    INSERT INTO poi (
                        source, source_id, name, name_en, category, subcategory,
                        address_raw, street, ward, district, city, province,
                        country, postal_code, phone, website,
                        geom, accuracy_meters
                    )
                    VALUES %s
                    ON CONFLICT DO NOTHING
                    """,
                    rows,   # dùng thẳng list rows gốc, đã đủ 18 phần tử
                    template="""(
                        %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s,
                        ST_SetSRID(ST_GeomFromText(%s), 4326),
                        %s
                    )""",
                    page_size=self.batch_size
                )
                inserted = len(rows)
            conn.commit()

        logger.info(f"Wrote {inserted} POI records to PostGIS")
        return inserted

    def log_pipeline_run(
        self,
        run_type: str,
        status: str,
        records_read: int = 0,
        records_written: int = 0,
        records_skipped: int = 0,
        error_message: Optional[str] = None,
        metadata: Optional[dict] = None,
    ) -> str:
        """Insert a pipeline run audit record."""
        run_id = str(uuid.uuid4())
        with psycopg2.connect(self.conn_string) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO pipeline_run
                        (id, run_type, status, records_read, records_written,
                         records_skipped, error_message, metadata, finished_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
                    """,
                    (
                        run_id, run_type, status,
                        records_read, records_written, records_skipped,
                        error_message, json.dumps(metadata or {}),
                    )
                )
            conn.commit()
        logger.info(f"Pipeline run logged: {run_id} [{run_type}] {status}")
        return run_id
