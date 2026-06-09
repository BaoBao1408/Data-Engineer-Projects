"""
config/settings.py
Centralized configuration using environment variables.
"""
import os
from dataclasses import dataclass


@dataclass
class DBConfig:
    host: str = os.getenv("DB_HOST", "localhost")
    port: int = int(os.getenv("DB_PORT", "5432"))
    name: str = os.getenv("DB_NAME", "geospatial")
    user: str = os.getenv("DB_USER", "geouser")
    password: str = os.getenv("DB_PASSWORD", "geopassword")

    @property
    def dsn(self) -> str:
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.name}"

    @property
    def async_dsn(self) -> str:
        return f"postgresql+asyncpg://{self.user}:{self.password}@{self.host}:{self.port}/{self.name}"


@dataclass
class OSMConfig:
    overpass_url: str = os.getenv(
        "OSM_OVERPASS_URL", "https://overpass-api.de/api/interpreter"
    )
    timeout_seconds: int = int(os.getenv("OSM_TIMEOUT", "60"))
    # Ho Chi Minh City bounding box: south, west, north, east
    default_bbox: str = os.getenv("TARGET_BBOX", "10.60,106.40,11.20,107.10")
    default_city: str = os.getenv("TARGET_CITY", "Ho Chi Minh City")

    @property
    def bbox_tuple(self) -> tuple:
        parts = [float(x) for x in self.default_bbox.split(",")]
        return tuple(parts)  # (south, west, north, east)


@dataclass
class PipelineConfig:
    dedup_radius_meters: float = float(os.getenv("DEDUP_RADIUS_M", "20"))
    batch_size: int = int(os.getenv("BATCH_SIZE", "500"))
    data_dir: str = os.getenv("DATA_DIR", "/app/data")
    log_level: str = os.getenv("LOG_LEVEL", "INFO")


# Singleton instances
db = DBConfig()
osm = OSMConfig()
pipeline = PipelineConfig()
