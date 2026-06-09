"""
tests/unit/test_osm_extractor.py
Unit tests for OSMExtractor — mocks Overpass HTTP requests.
"""
import pytest
from unittest.mock import MagicMock, patch
import geopandas as gpd

from src.ingest.osm_extractor import OSMExtractor


MOCK_OVERPASS_RESPONSE = {
    "elements": [
        {
            "type": "node",
            "id": 123456,
            "lat": 10.7769,
            "lon": 106.6853,
            "tags": {
                "amenity": "restaurant",
                "name": "Phở Hòa",
                "name:en": "Pho Hoa",
                "addr:street": "Pasteur",
                "addr:city": "Quan 3",
                "phone": "+84 28 1234 5678",
            },
        },
        {
            "type": "node",
            "id": 789012,
            "lat": 10.7800,
            "lon": 106.7000,
            "tags": {
                "amenity": "cafe",
                "name": "Highland Coffee",
            },
        },
        {
            "type": "way",
            "id": 111111,
            "center": {"lat": 10.7850, "lon": 106.6900},
            "tags": {
                "amenity": "hospital",
                "name": "Bệnh viện Chợ Rẫy",
            },
        },
        {
            "type": "node",
            "id": 999999,
            # No lat/lon — should be skipped
            "tags": {"amenity": "parking"},
        },
        {
            "type": "node",
            "id": 888888,
            "lat": 10.7700,
            "lon": 106.6800,
            "tags": {},  # No tags — should be skipped
        },
    ]
}


@pytest.fixture
def extractor():
    return OSMExtractor(overpass_url="https://mock-overpass.test", timeout=30)


@patch("src.ingest.osm_extractor.requests.Session.post")
def test_extract_returns_geodataframe(mock_post, extractor):
    mock_resp = MagicMock()
    mock_resp.json.return_value = MOCK_OVERPASS_RESPONSE
    mock_resp.raise_for_status = MagicMock()
    mock_post.return_value = mock_resp

    bbox = (10.60, 106.40, 11.20, 107.10)
    gdf = extractor.extract(bbox)

    assert isinstance(gdf, gpd.GeoDataFrame)
    assert gdf.crs.to_epsg() == 4326


@patch("src.ingest.osm_extractor.requests.Session.post")
def test_extract_correct_record_count(mock_post, extractor):
    mock_resp = MagicMock()
    mock_resp.json.return_value = MOCK_OVERPASS_RESPONSE
    mock_resp.raise_for_status = MagicMock()
    mock_post.return_value = mock_resp

    gdf = extractor.extract((10.60, 106.40, 11.20, 107.10))
    # 5 elements: 2 skipped (no lat/lon, no tags) → 3 valid
    assert len(gdf) == 3


@patch("src.ingest.osm_extractor.requests.Session.post")
def test_extract_category_mapping(mock_post, extractor):
    mock_resp = MagicMock()
    mock_resp.json.return_value = MOCK_OVERPASS_RESPONSE
    mock_resp.raise_for_status = MagicMock()
    mock_post.return_value = mock_resp

    gdf = extractor.extract((10.60, 106.40, 11.20, 107.10))
    categories = set(gdf["category"].tolist())

    assert "restaurant" in categories
    assert "cafe" in categories
    assert "hospital" in categories


@patch("src.ingest.osm_extractor.requests.Session.post")
def test_extract_source_tracking(mock_post, extractor):
    mock_resp = MagicMock()
    mock_resp.json.return_value = MOCK_OVERPASS_RESPONSE
    mock_resp.raise_for_status = MagicMock()
    mock_post.return_value = mock_resp

    gdf = extractor.extract((10.60, 106.40, 11.20, 107.10))

    assert (gdf["source"] == "osm").all()
    assert gdf["source_id"].str.startswith(("node/", "way/")).all()


@patch("src.ingest.osm_extractor.requests.Session.post")
def test_extract_empty_response(mock_post, extractor):
    mock_resp = MagicMock()
    mock_resp.json.return_value = {"elements": []}
    mock_resp.raise_for_status = MagicMock()
    mock_post.return_value = mock_resp

    gdf = extractor.extract((10.60, 106.40, 11.20, 107.10))

    assert isinstance(gdf, gpd.GeoDataFrame)
    assert len(gdf) == 0


@patch("src.ingest.osm_extractor.requests.Session.post")
def test_retry_on_failure(mock_post, extractor):
    import requests as req

    # Fail twice, succeed on third
    mock_success = MagicMock()
    mock_success.json.return_value = {"elements": []}
    mock_success.raise_for_status = MagicMock()

    mock_post.side_effect = [
        req.RequestException("Timeout"),
        req.RequestException("Timeout"),
        mock_success,
    ]

    # Should not raise
    gdf = extractor.extract((10.60, 106.40, 11.20, 107.10))
    assert isinstance(gdf, gpd.GeoDataFrame)
    assert mock_post.call_count == 3
