"""
src/ingest/osm_extractor.py
Pull POI data from OpenStreetMap via Overpass API.
Returns a GeoDataFrame with geometry + properties.
"""
import logging
import time
from typing import Optional
import requests
import geopandas as gpd
from shapely.geometry import Point
import pandas as pd

logger = logging.getLogger(__name__)

# OSM amenity tags to extract (maps to our category)
OSM_CATEGORY_MAP = {
    "restaurant": "restaurant",
    "cafe": "cafe",
    "fast_food": "fast_food",
    "bar": "bar",
    "hospital": "hospital",
    "clinic": "clinic",
    "pharmacy": "pharmacy",
    "school": "school",
    "university": "university",
    "bank": "bank",
    "atm": "atm",
    "fuel": "fuel",
    "hotel": "hotel",
    "supermarket": "supermarket",
    "convenience": "convenience",
    "mall": "mall",
    "parking": "parking",
}

OVERPASS_QUERY_TEMPLATE = """
[out:json][timeout:{timeout}];
(
  node["amenity"]({south},{west},{north},{east});
  way["amenity"]({south},{west},{north},{east});
  node["shop"]({south},{west},{north},{east});
  node["tourism"~"hotel|guest_house|hostel"]({south},{west},{north},{east});
);
out center tags;
"""


class OSMExtractor:
    """
    Extract POI features from OpenStreetMap via Overpass API.

    Usage:
        extractor = OSMExtractor(overpass_url=..., timeout=60)
        gdf = extractor.extract(bbox=(10.60, 106.40, 11.20, 107.10))
    """

    def __init__(self, overpass_url: str, timeout: int = 60):
        self.overpass_url = overpass_url
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "GeoSpatialPOIPipeline/1.0"})

    def build_query(self, bbox: tuple) -> str:
        south, west, north, east = bbox
        return OVERPASS_QUERY_TEMPLATE.format(
            timeout=self.timeout,
            south=south, west=west, north=north, east=east
        )

    def fetch_raw(self, bbox: tuple, retries: int = 3) -> dict:
        """Hit Overpass API with retry + backoff."""
        query = self.build_query(bbox)
        for attempt in range(1, retries + 1):
            try:
                logger.info(f"Overpass request attempt {attempt} for bbox {bbox}")
                resp = self.session.post(
                    self.overpass_url,
                    data={"data": query},
                    timeout=self.timeout + 10
                )
                resp.raise_for_status()
                data = resp.json()
                logger.info(f"Fetched {len(data.get('elements', []))} elements from OSM")
                return data
            except requests.RequestException as e:
                logger.warning(f"Attempt {attempt} failed: {e}")
                if attempt < retries:
                    time.sleep(2 ** attempt)
                else:
                    raise

    def parse_element(self, el: dict) -> Optional[dict]:
        """Parse a single OSM element into a flat dict."""
        tags = el.get("tags", {})
        if not tags:
            return None

        # Get coordinates — nodes have lat/lon directly, ways have center
        if el["type"] == "node":
            lat, lon = el.get("lat"), el.get("lon")
        elif el["type"] == "way":
            center = el.get("center", {})
            lat, lon = center.get("lat"), center.get("lon")
        else:
            return None

        if lat is None or lon is None:
            return None

        category = (
            OSM_CATEGORY_MAP.get(tags.get("amenity", ""))
            or OSM_CATEGORY_MAP.get(tags.get("shop", ""))
            or tags.get("amenity")
            or tags.get("shop")
            or tags.get("tourism")
            or "other"
        )

        return {
            "source": "osm",
            "source_id": f"{el['type']}/{el['id']}",
            "name": tags.get("name") or tags.get("name:vi") or tags.get("name:en"),
            "name_en": tags.get("name:en"),
            "category": category,
            "address_raw": tags.get("addr:full"),
            "street": tags.get("addr:street"),
            "ward": tags.get("addr:suburb"),
            "district": tags.get("addr:city") or tags.get("addr:district"),
            "city": tags.get("addr:city"),
            "phone": tags.get("phone") or tags.get("contact:phone"),
            "website": tags.get("website") or tags.get("contact:website"),
            "lat": lat,
            "lon": lon,
            "raw_tags": tags,
        }

    def extract(self, bbox: tuple) -> gpd.GeoDataFrame:
        """
        Full extraction: Overpass API → GeoDataFrame.

        Returns:
            GeoDataFrame with geometry column (Point, EPSG:4326)
        """
        raw = self.fetch_raw(bbox)
        elements = raw.get("elements", [])

        records = []
        skipped = 0
        for el in elements:
            parsed = self.parse_element(el)
            if parsed:
                records.append(parsed)
            else:
                skipped += 1

        logger.info(f"Parsed {len(records)} POIs, skipped {skipped} elements")

        if not records:
            return gpd.GeoDataFrame(columns=["geometry"], geometry="geometry", crs="EPSG:4326")

        df = pd.DataFrame(records)
        geometry = [Point(row.lon, row.lat) for row in df.itertuples()]
        gdf = gpd.GeoDataFrame(df, geometry=geometry, crs="EPSG:4326")
        gdf.drop(columns=["lat", "lon"], inplace=True)

        logger.info(f"GeoDataFrame shape: {gdf.shape}")
        return gdf
