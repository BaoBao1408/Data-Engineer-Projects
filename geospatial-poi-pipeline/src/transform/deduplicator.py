"""
src/transform/deduplicator.py
Spatial deduplication using PostGIS ST_DWithin.
Removes near-duplicate POIs within a configurable radius.
"""
import logging
from typing import Optional
import geopandas as gpd
import pandas as pd
import psycopg2
import psycopg2.extras
from shapely.geometry import Point

logger = logging.getLogger(__name__)


class SpatialDeduplicator:
    """
    Two-phase deduplication:
    1. In-batch: deduplicate within the current GeoDataFrame (before DB insert).
    2. Cross-batch: deduplicate against existing DB records using ST_DWithin.
    """

    def __init__(self, conn_string: str, radius_meters: float = 20.0):
        self.conn_string = conn_string
        self.radius_meters = radius_meters

    # ── Phase 1: In-memory deduplication ─────────────────────────────────────

    def dedup_in_memory(self, gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        """
        Remove near-duplicates within a GeoDataFrame.
        Strategy: for same category + within radius, keep the one with more metadata.
        """
        logger.info(f"In-memory dedup: {len(gdf)} records, radius={self.radius_meters}m")

        if len(gdf) == 0:
            return gdf

        # Add a completeness score: number of non-null fields
        meta_cols = ["name", "address_raw", "phone", "website", "district"]
        available_cols = [c for c in meta_cols if c in gdf.columns]
        gdf = gdf.copy()
        gdf["_completeness"] = gdf[available_cols].notna().sum(axis=1)
        gdf["_keep"] = True

        # Group by category to reduce comparisons
        categories = gdf["category"].unique() if "category" in gdf.columns else ["all"]

        for cat in categories:
            if cat == "all":
                subset = gdf
            else:
                subset = gdf[gdf["category"] == cat]

            if len(subset) < 2:
                continue

            # Reproject to metric CRS for accurate distance calc
            subset_metric = subset.to_crs("EPSG:3857")

            for i, row_i in subset_metric.iterrows():
                if not gdf.at[i, "_keep"]:
                    continue
                for j, row_j in subset_metric.iterrows():
                    if i >= j or not gdf.at[j, "_keep"]:
                        continue
                    dist = row_i.geometry.distance(row_j.geometry)
                    if dist <= self.radius_meters:
                        # Keep the more complete record
                        if gdf.at[i, "_completeness"] >= gdf.at[j, "_completeness"]:
                            gdf.at[j, "_keep"] = False
                        else:
                            gdf.at[i, "_keep"] = False

        removed = (~gdf["_keep"]).sum()
        logger.info(f"In-memory dedup removed {removed} near-duplicates")

        result = gdf[gdf["_keep"]].drop(columns=["_keep", "_completeness"])
        return result.reset_index(drop=True)

    # ── Phase 2: Cross-batch dedup against DB ────────────────────────────────

    def get_existing_nearby(
        self,
        lon: float,
        lat: float,
        category: Optional[str] = None,
        radius_m: Optional[float] = None,
    ) -> list:
        """
        Query PostGIS for existing POIs near a given point.
        Uses ST_DWithin on the spatial index for performance.
        """
        radius = radius_m or self.radius_meters
        with psycopg2.connect(self.conn_string) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                query = """
                    SELECT id, name, source, source_id, category,
                           ST_Distance(
                               geom::geography,
                               ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography
                           ) AS distance_m
                    FROM poi
                    WHERE
                        is_active = TRUE
                        AND ST_DWithin(
                            geom::geography,
                            ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography,
                            %s
                        )
                        {category_filter}
                    ORDER BY distance_m
                    LIMIT 10;
                """
                cat_filter = "AND category = %s" if category else ""
                query = query.format(category_filter=cat_filter)

                params = [lon, lat, lon, lat, radius]
                if category:
                    params.append(category)

                cur.execute(query, params)
                return cur.fetchall()

    def filter_new_records(self, gdf: gpd.GeoDataFrame) -> tuple:
        """
        Filter out records that already exist in the DB.
        Returns (new_gdf, duplicate_count).
        """
        logger.info(f"Cross-batch dedup check for {len(gdf)} records")
        new_records = []
        dup_count = 0

        for _, row in gdf.iterrows():
            lon = row.geometry.x
            lat = row.geometry.y
            category = row.get("category")

            nearby = self.get_existing_nearby(lon, lat, category)
            if nearby:
                dup_count += 1
                logger.debug(
                    f"Duplicate found for {row.get('name')} — "
                    f"existing: {nearby[0]['name']} at {nearby[0]['distance_m']:.1f}m"
                )
            else:
                new_records.append(row)

        logger.info(f"Cross-batch dedup: {dup_count} duplicates, {len(new_records)} new")

        if not new_records:
            return gpd.GeoDataFrame(columns=gdf.columns, geometry="geometry", crs=gdf.crs), dup_count

        result = gpd.GeoDataFrame(new_records, geometry="geometry", crs=gdf.crs)
        return result.reset_index(drop=True), dup_count
