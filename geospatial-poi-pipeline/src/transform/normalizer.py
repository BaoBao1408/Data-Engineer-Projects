"""
src/transform/normalizer.py
Normalize raw POI data: coordinates, address, category, encoding.
"""
import logging
import re
import unicodedata
from typing import Optional
import geopandas as gpd
import pandas as pd
from shapely.geometry import Point
from shapely.validation import make_valid

logger = logging.getLogger(__name__)

# Vietnamese district/ward name patterns
DISTRICT_PATTERNS = [
    r"qu[aậ]n\s+(\d+|[a-zA-ZÀ-ỹ\s]+)",
    r"huy[eệ]n\s+([a-zA-ZÀ-ỹ\s]+)",
    r"th[aà]nh\s+ph[oố]\s+([a-zA-ZÀ-ỹ\s]+)",
]

CATEGORY_NORMALIZATION = {
    "eatery": "restaurant",
    "food": "restaurant",
    "eating": "restaurant",
    "coffee": "cafe",
    "coffeehouse": "cafe",
    "drugstore": "pharmacy",
    "drug_store": "pharmacy",
    "chemist": "pharmacy",
    "doctor": "clinic",
    "medical": "clinic",
    "shop": "retail",
    "store": "retail",
}


class POINormalizer:
    """
    Transform raw GeoDataFrame (from OSM or GeoJSON) into clean normalized records.
    """

    def normalize(self, gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        """Run all normalization steps."""
        logger.info(f"Normalizing {len(gdf)} POI records")
        gdf = gdf.copy()

        gdf = self._fix_geometry(gdf)
        gdf = self._normalize_names(gdf)
        gdf = self._normalize_category(gdf)
        gdf = self._normalize_address(gdf)
        gdf = self._add_coordinate_columns(gdf)
        gdf = self._drop_invalid(gdf)

        logger.info(f"Normalization complete: {len(gdf)} valid records")
        return gdf

    # ── Geometry ─────────────────────────────────────────────────────────────

    def _fix_geometry(self, gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        """Fix invalid geometries and remove nulls."""
        null_count = gdf.geometry.isna().sum()
        if null_count:
            logger.warning(f"Dropping {null_count} records with null geometry")
            gdf = gdf.dropna(subset=["geometry"])

        invalid_mask = ~gdf.geometry.is_valid
        if invalid_mask.any():
            logger.warning(f"Fixing {invalid_mask.sum()} invalid geometries")
            gdf.loc[invalid_mask, "geometry"] = gdf.loc[invalid_mask, "geometry"].apply(
                make_valid
            )

        # Filter to Vietnam approximate bounding box
        vn_minx, vn_miny, vn_maxx, vn_maxy = 102.0, 8.0, 110.0, 24.0
        before = len(gdf)
        gdf = gdf[
            (gdf.geometry.x >= vn_minx) & (gdf.geometry.x <= vn_maxx) &
            (gdf.geometry.y >= vn_miny) & (gdf.geometry.y <= vn_maxy)
        ]
        removed = before - len(gdf)
        if removed:
            logger.warning(f"Removed {removed} records outside Vietnam bbox")

        return gdf

    # ── Names ─────────────────────────────────────────────────────────────────

    def _normalize_names(self, gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        if "name" in gdf.columns:
            gdf["name"] = gdf["name"].apply(self._clean_text)
        if "name_en" in gdf.columns:
            gdf["name_en"] = gdf["name_en"].apply(self._clean_text)
        return gdf

    def _clean_text(self, value: Optional[str]) -> Optional[str]:
        if pd.isna(value) or not value:
            return None
        # Strip extra whitespace
        value = " ".join(str(value).split())
        # Normalize unicode (NFC for Vietnamese)
        value = unicodedata.normalize("NFC", value)
        return value.strip() or None

    # ── Category ─────────────────────────────────────────────────────────────

    def _normalize_category(self, gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        if "category" not in gdf.columns:
            gdf["category"] = "other"
            return gdf

        gdf["category"] = (
            gdf["category"]
            .fillna("other")
            .str.lower()
            .str.strip()
            .replace(CATEGORY_NORMALIZATION)
        )
        return gdf

    # ── Address ───────────────────────────────────────────────────────────────

    def _normalize_address(self, gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        # Ensure address columns exist
        for col in ["address_raw", "street", "ward", "district", "city", "province"]:
            if col not in gdf.columns:
                gdf[col] = None

        # Try to extract district from raw address if district is null
        mask = gdf["district"].isna() & gdf["address_raw"].notna()
        if mask.any():
            gdf.loc[mask, "district"] = gdf.loc[mask, "address_raw"].apply(
                self._extract_district
            )

        # Default city
        if "city" in gdf.columns:
            gdf["city"] = gdf["city"].fillna("Ho Chi Minh City")

        gdf["country"] = "VN"
        return gdf

    def _extract_district(self, address: Optional[str]) -> Optional[str]:
        if not address:
            return None
        for pattern in DISTRICT_PATTERNS:
            match = re.search(pattern, str(address), re.IGNORECASE)
            if match:
                return match.group(0).strip()
        return None

    # ── Coordinates ───────────────────────────────────────────────────────────

    def _add_coordinate_columns(self, gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        """Add explicit lat/lon columns for easier access."""
        gdf["lon"] = gdf.geometry.x
        gdf["lat"] = gdf.geometry.y
        return gdf

    # ── Drop invalid ─────────────────────────────────────────────────────────

    def _drop_invalid(self, gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        """Drop records that are still unusable after normalization."""
        before = len(gdf)
        # Must have a name OR category to be useful
        gdf = gdf[gdf["name"].notna() | (gdf["category"] != "other")]
        after = len(gdf)
        if before != after:
            logger.info(f"Dropped {before - after} records with no name and no category")
        return gdf.reset_index(drop=True)
