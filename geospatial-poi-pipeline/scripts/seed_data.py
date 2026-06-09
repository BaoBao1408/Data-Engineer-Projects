"""
scripts/seed_data.py
────────────────────────────────────────────────────────────────────────────────
Seed the PostGIS database with local GeoJSON test data.
Bypasses OSM/Overpass API — dùng để test toàn bộ pipeline khi không có live source.

Usage (từ project root, sau khi docker compose up):
    python scripts/seed_data.py
    python scripts/seed_data.py --file data/seed/hcmc_poi_seed.geojson
    python scripts/seed_data.py --dry-run       # chỉ in stats, không write DB
    python scripts/seed_data.py --clear         # xóa hết data cũ trước khi seed

Hoặc chạy trong Docker:
    docker compose run --rm pipeline python scripts/seed_data.py
"""
import argparse
import logging
import os
import sys
from pathlib import Path

# Ensure project root is in path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("seed_data")


def clear_tables(conn_string: str):
    """Truncate poi và raw_poi để fresh start."""
    import psycopg2
    logger.warning("Clearing existing data from poi, raw_poi, pipeline_run...")
    with psycopg2.connect(conn_string) as conn:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE dedup_log CASCADE;")
            cur.execute("TRUNCATE TABLE poi CASCADE;")
            cur.execute("TRUNCATE TABLE raw_poi CASCADE;")
            cur.execute("DELETE FROM pipeline_run WHERE run_type = 'seed';")
        conn.commit()
    logger.info("Tables cleared.")


def run_seed(geojson_path: Path, dry_run: bool = False, clear: bool = False):
    from config.settings import db, pipeline
    from src.ingest.geojson_loader import GeoJSONLoader
    from src.transform.normalizer import POINormalizer
    from src.transform.deduplicator import SpatialDeduplicator
    from src.load.postgis_writer import PostGISWriter

    logger.info(f"=== Seed Data Pipeline ===")
    logger.info(f"Source : {geojson_path}")
    logger.info(f"DB     : {db.host}:{db.port}/{db.name}")
    logger.info(f"Dry run: {dry_run}")

    # ── Load GeoJSON ──────────────────────────────────────────────────────────
    loader = GeoJSONLoader()
    gdf = loader.load_file(geojson_path)

    # Add required fields that OSMExtractor normally provides
    if "source" not in gdf.columns:
        gdf["source"] = "seed"
    if "raw_tags" not in gdf.columns:
        gdf["raw_tags"] = None

    logger.info(f"Loaded {len(gdf)} features from {geojson_path.name}")

    # Validate
    report = loader.validate(gdf)
    logger.info(f"Validation: {report}")
    if not report["passed"]:
        logger.warning("Validation issues found — continuing with normalization anyway")

    # ── Normalize ─────────────────────────────────────────────────────────────
    normalizer = POINormalizer()
    norm_gdf = normalizer.normalize(gdf)
    logger.info(f"After normalize: {len(norm_gdf)} records")

    # ── Stats preview ─────────────────────────────────────────────────────────
    if "category" in norm_gdf.columns:
        cat_counts = norm_gdf["category"].value_counts()
        logger.info(f"Category breakdown:\n{cat_counts.to_string()}")

    if "district" in norm_gdf.columns:
        dist_counts = norm_gdf["district"].value_counts()
        logger.info(f"District breakdown:\n{dist_counts.to_string()}")

    if dry_run:
        logger.info("DRY RUN — skipping DB write.")
        print(norm_gdf[["name", "category", "district", "lat", "lon"]].to_string())
        return

    # ── Clear if requested ────────────────────────────────────────────────────
    if clear:
        clear_tables(db.dsn)

    # ── Write to DB ───────────────────────────────────────────────────────────
    writer = PostGISWriter(db.dsn, pipeline.batch_size)

    # Bronze: raw_poi
    batch_id = str(__import__("uuid").uuid4())
    raw_written = writer.write_raw(gdf, batch_id=batch_id)
    logger.info(f"Bronze (raw_poi): {raw_written} rows written")

    # Dedup in-memory only (no cross-batch since we just cleared)
    deduplicator = SpatialDeduplicator(db.dsn, pipeline.dedup_radius_meters)
    deduped_gdf = deduplicator.dedup_in_memory(norm_gdf)
    logger.info(f"After in-memory dedup: {len(deduped_gdf)} records")

    # Silver: poi
    written = writer.write_poi(deduped_gdf)
    logger.info(f"Silver (poi): {written} rows written")

    # Audit log
    writer.log_pipeline_run(
        run_type="seed",
        status="success",
        records_read=len(gdf),
        records_written=written,
        records_skipped=len(gdf) - written,
        metadata={
            "source_file": str(geojson_path),
            "batch_id": batch_id,
            "after_normalize": len(norm_gdf),
            "after_dedup": len(deduped_gdf),
        },
    )

    logger.info("=" * 60)
    logger.info(f"Seed complete: {written} POIs loaded into PostGIS")
    logger.info("Verify in pgAdmin:")
    logger.info("  SELECT category, COUNT(*) FROM poi GROUP BY category ORDER BY count DESC;")
    logger.info("  SELECT COUNT(*) FROM raw_poi;")


def main():
    parser = argparse.ArgumentParser(description="Seed PostGIS with local GeoJSON POI data")
    parser.add_argument(
        "--file",
        type=Path,
        default=PROJECT_ROOT / "data" / "seed" / "hcmc_poi_seed.geojson",
        help="Path to GeoJSON seed file",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print normalized data without writing to DB",
    )
    parser.add_argument(
        "--clear",
        action="store_true",
        help="Clear existing poi/raw_poi data before seeding",
    )
    args = parser.parse_args()

    if not args.file.exists():
        logger.error(f"GeoJSON file not found: {args.file}")
        sys.exit(1)

    run_seed(args.file, dry_run=args.dry_run, clear=args.clear)


if __name__ == "__main__":
    main()
