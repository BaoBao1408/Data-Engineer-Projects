"""
tests/unit/test_normalizer.py
Unit tests for POINormalizer.
"""
import pytest
import geopandas as gpd
import pandas as pd
from shapely.geometry import Point

from src.transform.normalizer import POINormalizer


@pytest.fixture
def sample_gdf():
    """Create a sample GeoDataFrame with mixed quality data."""
    data = {
        "source": ["osm", "osm", "geojson", "osm"],
        "source_id": ["node/1", "node/2", "file/1", "node/3"],
        "name": ["Phở Hòa", None, "  Café ABC  ", "Restaurant XYZ"],
        "category": ["restaurant", "cafe", "CAFE", "eatery"],
        "address_raw": [
            "123 Nguyễn Trãi, Quận 1",
            "456 Lê Lợi",
            None,
            "789 Đinh Tiên Hoàng, Quận 3",
        ],
        "geometry": [
            Point(106.6853, 10.7769),   # HCMC — valid
            Point(106.7000, 10.7800),   # HCMC — valid
            Point(106.6900, 10.7900),   # HCMC — valid
            Point(0.0, 0.0),            # Out of bbox — should be dropped
        ],
    }
    return gpd.GeoDataFrame(data, geometry="geometry", crs="EPSG:4326")


class TestPOINormalizer:

    def setup_method(self):
        self.normalizer = POINormalizer()

    def test_drops_out_of_bbox(self, sample_gdf):
        result = self.normalizer.normalize(sample_gdf)
        # The (0, 0) point is outside Vietnam bbox — should be dropped
        assert len(result) == 3

    def test_category_normalization(self, sample_gdf):
        result = self.normalizer.normalize(sample_gdf)
        categories = result["category"].tolist()
        # "eatery" → "restaurant", "CAFE" → "cafe"
        assert "eatery" not in categories
        assert "CAFE" not in categories
        assert "restaurant" in categories
        assert "cafe" in categories

    def test_name_whitespace_stripped(self, sample_gdf):
        result = self.normalizer.normalize(sample_gdf)
        names = result["name"].dropna().tolist()
        for name in names:
            assert name == name.strip()

    def test_coordinate_columns_added(self, sample_gdf):
        result = self.normalizer.normalize(sample_gdf)
        assert "lon" in result.columns
        assert "lat" in result.columns
        # Check HCMC coordinates are reasonable
        assert result["lon"].between(106.0, 107.5).all()
        assert result["lat"].between(10.0, 11.5).all()

    def test_country_default(self, sample_gdf):
        result = self.normalizer.normalize(sample_gdf)
        assert (result["country"] == "VN").all()

    def test_empty_geodataframe(self):
        empty = gpd.GeoDataFrame(columns=["geometry", "name", "category"],
                                  geometry="geometry", crs="EPSG:4326")
        result = self.normalizer.normalize(empty)
        assert len(result) == 0

    def test_null_geometry_dropped(self):
        gdf = gpd.GeoDataFrame({
            "name": ["Valid POI", "Null Geom POI"],
            "category": ["restaurant", "cafe"],
            "geometry": [Point(106.6853, 10.7769), None],
        }, geometry="geometry", crs="EPSG:4326")
        result = self.normalizer.normalize(gdf)
        assert result["geometry"].notna().all()
