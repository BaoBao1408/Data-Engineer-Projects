"""
src/main.py
Pipeline entry point — runs full ETL: OSM Extract → Normalize → Dedup → Load.
Can be triggered directly (docker run) or by Airflow.
"""
import logging
import os
import sys
from datetime import datetime

from config.settings import db, osm, pipeline
from src.ingest.osm_extractor import OSMExtractor
from src.transform.normalizer import POINormalizer
from src.transform.deduplicator import SpatialDeduplicator
from src.load.postgis_writer import PostGISWriter

logging.basicConfig(
    level=getattr(logging, pipeline.log_level),
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("pipeline.main")


def run_pipeline(bbox: tuple = None, city: str = None) -> dict:
    """
    Full pipeline run:
      1. Extract POIs from OpenStreetMap
      2. Normalize (clean names, categories, addresses, geometry)
      3. Deduplicate (in-memory + cross-batch against DB)
      4. Load into PostGIS
      5. Log audit record

    Returns dict with run statistics.
    """
    bbox = bbox or osm.bbox_tuple
    city = city or osm.default_city
    start = datetime.utcnow()

    extractor = OSMExtractor(osm.overpass_url, osm.timeout_seconds)
    normalizer = POINormalizer()
    deduplicator = SpatialDeduplicator(db.dsn, pipeline.dedup_radius_meters)
    writer = PostGISWriter(db.dsn, pipeline.batch_size)

    stats = {
        "city": city,
        "bbox": bbox,
        "raw_extracted": 0,
        "after_normalize": 0,
        "after_dedup_memory": 0,
        "after_dedup_db": 0,
        "written": 0,
        "status": "running",
        "error": None,
    }

    try:
        # ── Step 1: Extract ──────────────────────────────────────────────────
        logger.info(f"[1/4] Extracting OSM POIs for {city} bbox={bbox}")
        raw_gdf = extractor.extract(bbox)
        stats["raw_extracted"] = len(raw_gdf)
        logger.info(f"      Extracted: {len(raw_gdf)} records")

        if raw_gdf.empty:
            logger.warning("No data extracted — aborting pipeline")
            stats["status"] = "success_empty"
            return stats

        # Write raw to Bronze layer
        writer.write_raw(raw_gdf)

        # ── Step 2: Normalize ────────────────────────────────────────────────
        logger.info("[2/4] Normalizing...")
        norm_gdf = normalizer.normalize(raw_gdf)
        stats["after_normalize"] = len(norm_gdf)
        logger.info(f"      After normalize: {len(norm_gdf)} records")

        # ── Step 3: Deduplication ────────────────────────────────────────────
        logger.info("[3/4] Deduplicating (in-memory)...")
        deduped_gdf = deduplicator.dedup_in_memory(norm_gdf)
        stats["after_dedup_memory"] = len(deduped_gdf)
        logger.info(f"      After in-memory dedup: {len(deduped_gdf)} records")

        logger.info("[3/4] Deduplicating (cross-batch against DB)...")
        new_gdf, dup_count = deduplicator.filter_new_records(deduped_gdf)
        stats["after_dedup_db"] = len(new_gdf)
        logger.info(f"      After DB dedup: {len(new_gdf)} new records ({dup_count} duplicates)")

        # ── Step 4: Load ─────────────────────────────────────────────────────
        logger.info("[4/4] Loading into PostGIS...")
        written = writer.write_poi(new_gdf)
        stats["written"] = written
        logger.info(f"      Written: {written} records")

        stats["status"] = "success"

    except Exception as e:
        logger.error(f"Pipeline failed: {e}", exc_info=True)
        stats["status"] = "failed"
        stats["error"] = str(e)

    finally:
        elapsed = (datetime.utcnow() - start).total_seconds()
        stats["elapsed_seconds"] = round(elapsed, 2)

        writer.log_pipeline_run(
            run_type="full_osm_extract",
            status=stats["status"],
            records_read=stats["raw_extracted"],
            records_written=stats["written"],
            records_skipped=stats["raw_extracted"] - stats["written"],
            error_message=stats.get("error"),
            metadata=stats,
        )

    logger.info(f"Pipeline complete in {stats['elapsed_seconds']}s | status={stats['status']}")
    return stats


if __name__ == "__main__":
    result = run_pipeline()
    if result["status"] == "failed":
        sys.exit(1)
    print(f"Done: {result}")
