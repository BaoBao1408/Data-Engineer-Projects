"""
src/ingest/geojson_loader.py
Load POI data from local or S3 GeoJSON files.
"""
import json
import logging
import os
from pathlib import Path
from typing import Union
import geopandas as gpd

logger = logging.getLogger(__name__)


class GeoJSONLoader:
    """
    Load GeoJSON FeatureCollection files into GeoDataFrame.

    Supports:
      - Local file path
      - Directory scan (loads all .geojson files)
    """

    REQUIRED_FIELDS = ["geometry"]

    def load_file(self, path: Union[str, Path]) -> gpd.GeoDataFrame:
        """Load a single GeoJSON file."""
        path = Path(path)
        if not path.exists():
            raise FileNotFoundError(f"GeoJSON file not found: {path}")

        logger.info(f"Loading GeoJSON: {path}")
        gdf = gpd.read_file(str(path))

        if gdf.crs is None:
            logger.warning("No CRS found, assuming EPSG:4326")
            gdf = gdf.set_crs("EPSG:4326")
        elif gdf.crs.to_epsg() != 4326:
            logger.info(f"Reprojecting from {gdf.crs} to EPSG:4326")
            gdf = gdf.to_crs("EPSG:4326")

        # Normalize column names to lowercase
        gdf.columns = [c.lower() for c in gdf.columns]

        # Add source tracking
        gdf["source"] = "geojson"
        gdf["source_file"] = path.name

        logger.info(f"Loaded {len(gdf)} features from {path.name}")
        return gdf

    def load_directory(self, directory: Union[str, Path]) -> gpd.GeoDataFrame:
        """Load all .geojson files in a directory and concatenate."""
        directory = Path(directory)
        files = list(directory.glob("*.geojson")) + list(directory.glob("*.json"))

        if not files:
            logger.warning(f"No GeoJSON files found in {directory}")
            return gpd.GeoDataFrame()

        gdfs = []
        for f in files:
            try:
                gdfs.append(self.load_file(f))
            except Exception as e:
                logger.error(f"Failed to load {f}: {e}")

        if not gdfs:
            return gpd.GeoDataFrame()

        combined = gpd.pd.concat(gdfs, ignore_index=True)
        result = gpd.GeoDataFrame(combined, geometry="geometry", crs="EPSG:4326")
        logger.info(f"Combined {len(result)} features from {len(gdfs)} files")
        return result

    def validate(self, gdf: gpd.GeoDataFrame) -> dict:
        """
        Basic data quality checks on loaded GeoDataFrame.
        Returns a dict of validation results.
        """
        total = len(gdf)
        null_geom = gdf.geometry.isna().sum()
        invalid_geom = (~gdf.geometry.is_valid).sum()
        out_of_bbox = 0

        # Vietnam approximate bbox
        if total > 0:
            bounds = gdf.geometry.total_bounds  # minx, miny, maxx, maxy
            vn_bbox = (102.0, 8.0, 110.0, 24.0)
            if bounds[0] < vn_bbox[0] or bounds[1] < vn_bbox[1]:
                out_of_bbox = gdf[
                    (gdf.geometry.x < vn_bbox[0]) | (gdf.geometry.y < vn_bbox[1])
                ].shape[0]

        return {
            "total_features": total,
            "null_geometry": int(null_geom),
            "invalid_geometry": int(invalid_geom),
            "out_of_expected_bbox": out_of_bbox,
            "passed": null_geom == 0 and invalid_geom == 0,
        }
