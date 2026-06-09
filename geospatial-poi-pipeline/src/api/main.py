"""
src/api/main.py
FastAPI app exposing spatial query endpoints.
"""
import json
import logging
from typing import List, Optional
import psycopg2
import psycopg2.extras
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from config.settings import db as db_config

logger = logging.getLogger(__name__)

app = FastAPI(
    title="Geospatial POI API",
    description="Spatial query API for POI and Address data — powered by PostGIS",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)


def get_conn():
    return psycopg2.connect(db_config.dsn)


# ── Response Models ───────────────────────────────────────────────────────────

class POIFeature(BaseModel):
    id: int
    name: Optional[str]
    category: Optional[str]
    district: Optional[str]
    city: Optional[str]
    phone: Optional[str]
    website: Optional[str]
    distance_m: Optional[float]
    geometry: dict


class GeoJSONResponse(BaseModel):
    type: str = "FeatureCollection"
    total: int
    features: List[dict]


# ── Endpoints ─────────────────────────────────────────────────────────────────

@app.get("/health")
def health():
    """Health check — also verifies DB connection."""
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT PostGIS_Version();")
                version = cur.fetchone()[0]
        return {"status": "ok", "postgis_version": version}
    except Exception as e:
        raise HTTPException(status_code=503, detail=str(e))


@app.get("/api/v1/poi/nearby", response_model=GeoJSONResponse)
def nearby_poi(
    lon: float = Query(..., description="Longitude (e.g. 106.6853)", ge=102.0, le=110.0),
    lat: float = Query(..., description="Latitude (e.g. 10.7769)", ge=8.0, le=24.0),
    radius_m: float = Query(500, description="Search radius in meters", ge=10, le=10000),
    category: Optional[str] = Query(None, description="Filter by category"),
    limit: int = Query(20, ge=1, le=100),
):
    """
    Find POIs within a radius of a given point.
    Uses PostGIS ST_DWithin on spatial index for fast lookup.
    """
    try:
        with get_conn() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cat_filter = "AND category = %(category)s" if category else ""
                cur.execute(
                    f"""
                    SELECT
                        id, name, category, district, city, phone, website,
                        ST_AsGeoJSON(geom)::json AS geometry,
                        ST_Distance(
                            geom::geography,
                            ST_SetSRID(ST_MakePoint(%(lon)s, %(lat)s), 4326)::geography
                        ) AS distance_m
                    FROM poi
                    WHERE
                        is_active = TRUE
                        AND ST_DWithin(
                            geom::geography,
                            ST_SetSRID(ST_MakePoint(%(lon)s, %(lat)s), 4326)::geography,
                            %(radius_m)s
                        )
                        {cat_filter}
                    ORDER BY distance_m
                    LIMIT %(limit)s;
                    """,
                    {"lon": lon, "lat": lat, "radius_m": radius_m,
                     "category": category, "limit": limit}
                )
                rows = cur.fetchall()

        features = [
            {
                "type": "Feature",
                "geometry": row["geometry"],
                "properties": {k: v for k, v in row.items() if k != "geometry"},
            }
            for row in rows
        ]
        return {"type": "FeatureCollection", "total": len(features), "features": features}

    except Exception as e:
        logger.error(f"nearby_poi error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/poi/bbox", response_model=GeoJSONResponse)
def poi_in_bbox(
    min_lon: float = Query(..., ge=102.0, le=110.0),
    min_lat: float = Query(..., ge=8.0, le=24.0),
    max_lon: float = Query(..., ge=102.0, le=110.0),
    max_lat: float = Query(..., ge=8.0, le=24.0),
    category: Optional[str] = None,
    limit: int = Query(200, ge=1, le=1000),
):
    """Return all POIs within a bounding box (for map viewport queries)."""
    try:
        with get_conn() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cat_filter = "AND category = %(category)s" if category else ""
                cur.execute(
                    f"""
                    SELECT id, name, category, district, city,
                           ST_AsGeoJSON(geom)::json AS geometry
                    FROM poi
                    WHERE
                        is_active = TRUE
                        AND geom && ST_MakeEnvelope(%(min_lon)s, %(min_lat)s,
                                                     %(max_lon)s, %(max_lat)s, 4326)
                        {cat_filter}
                    ORDER BY name
                    LIMIT %(limit)s;
                    """,
                    {"min_lon": min_lon, "min_lat": min_lat,
                     "max_lon": max_lon, "max_lat": max_lat,
                     "category": category, "limit": limit}
                )
                rows = cur.fetchall()

        features = [
            {"type": "Feature", "geometry": row["geometry"],
             "properties": {k: v for k, v in row.items() if k != "geometry"}}
            for row in rows
        ]
        return {"type": "FeatureCollection", "total": len(features), "features": features}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/stats")
def pipeline_stats():
    """Return POI counts by category and city."""
    try:
        with get_conn() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("""
                    SELECT category, city, COUNT(*) AS count
                    FROM poi WHERE is_active = TRUE
                    GROUP BY category, city
                    ORDER BY count DESC
                    LIMIT 50;
                """)
                return {"stats": cur.fetchall()}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
