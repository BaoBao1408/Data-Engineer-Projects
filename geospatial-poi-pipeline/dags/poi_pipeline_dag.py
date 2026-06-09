"""
dags/poi_pipeline_dag.py
Airflow DAG — daily OSM POI extraction and loading for Ho Chi Minh City.

Schedule: Daily at 02:00 ICT (19:00 UTC)
"""
from datetime import datetime, timedelta
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

logger = logging.getLogger(__name__)

# ── Dag defaults ───────────────────────────────────────────────────────────────
default_args = {
    "owner": "data-team",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}

# ── Task functions ─────────────────────────────────────────────────────────────

def extract_osm(**context):
    """Extract raw POIs from OpenStreetMap."""
    from config.settings import osm, db
    from src.ingest.osm_extractor import OSMExtractor
    from src.load.postgis_writer import PostGISWriter

    extractor = OSMExtractor(osm.overpass_url, osm.timeout_seconds)
    writer = PostGISWriter(db.dsn)

    bbox = osm.bbox_tuple
    logger.info(f"Extracting OSM for bbox={bbox}")
    gdf = extractor.extract(bbox)

    # Push to XCom for next task
    context["ti"].xcom_push("record_count", len(gdf))

    # Write raw to Bronze
    batch_id = writer.write_raw(gdf)

    # Serialize GeoDataFrame to parquet in /tmp for next task
    tmp_path = f"/tmp/raw_poi_{context['ds_nodash']}.parquet"
    gdf.to_parquet(tmp_path, index=False)
    context["ti"].xcom_push("raw_path", tmp_path)

    logger.info(f"Extracted {len(gdf)} records → {tmp_path}")
    return len(gdf)


def normalize_poi(**context):
    """Normalize raw POI GeoDataFrame."""
    import geopandas as gpd
    from src.transform.normalizer import POINormalizer

    raw_path = context["ti"].xcom_pull(task_ids="extract_osm", key="raw_path")
    gdf = gpd.read_parquet(raw_path)

    normalizer = POINormalizer()
    norm_gdf = normalizer.normalize(gdf)

    out_path = f"/tmp/norm_poi_{context['ds_nodash']}.parquet"
    norm_gdf.to_parquet(out_path, index=False)
    context["ti"].xcom_push("norm_path", out_path)

    logger.info(f"Normalized: {len(norm_gdf)} records → {out_path}")
    return len(norm_gdf)


def dedup_poi(**context):
    """Deduplicate: in-memory + cross-batch against DB."""
    import geopandas as gpd
    from config.settings import db, pipeline
    from src.transform.deduplicator import SpatialDeduplicator

    norm_path = context["ti"].xcom_pull(task_ids="normalize_poi", key="norm_path")
    gdf = gpd.read_parquet(norm_path)

    deduplicator = SpatialDeduplicator(db.dsn, pipeline.dedup_radius_meters)

    # Phase 1: in-memory
    gdf = deduplicator.dedup_in_memory(gdf)

    # Phase 2: against DB
    new_gdf, dup_count = deduplicator.filter_new_records(gdf)

    out_path = f"/tmp/dedup_poi_{context['ds_nodash']}.parquet"
    new_gdf.to_parquet(out_path, index=False)
    context["ti"].xcom_push("dedup_path", out_path)
    context["ti"].xcom_push("dup_count", dup_count)

    logger.info(f"Dedup complete: {len(new_gdf)} new records, {dup_count} duplicates")
    return len(new_gdf)


def load_to_postgis(**context):
    """Load deduped GeoDataFrame to PostGIS."""
    import geopandas as gpd
    from config.settings import db
    from src.load.postgis_writer import PostGISWriter

    dedup_path = context["ti"].xcom_pull(task_ids="dedup_poi", key="dedup_path")
    gdf = gpd.read_parquet(dedup_path)

    writer = PostGISWriter(db.dsn)
    written = writer.write_poi(gdf)

    logger.info(f"Loaded {written} records into PostGIS")
    return written


def log_run_summary(**context):
    """Log pipeline run summary to audit table."""
    from config.settings import db
    from src.load.postgis_writer import PostGISWriter

    ti = context["ti"]
    raw_count = ti.xcom_pull(task_ids="extract_osm") or 0
    norm_count = ti.xcom_pull(task_ids="normalize_poi") or 0
    written = ti.xcom_pull(task_ids="load_to_postgis") or 0
    dup_count = ti.xcom_pull(task_ids="dedup_poi", key="dup_count") or 0

    writer = PostGISWriter(db.dsn)
    writer.log_pipeline_run(
        run_type="airflow_daily",
        status="success",
        records_read=raw_count,
        records_written=written,
        records_skipped=dup_count,
        metadata={
            "dag_run_id": context["run_id"],
            "execution_date": context["ds"],
            "normalized": norm_count,
        },
    )
    logger.info(f"Summary: raw={raw_count}, normalized={norm_count}, "
                f"written={written}, duplicates={dup_count}")


# ── DAG definition ─────────────────────────────────────────────────────────────
with DAG(
    dag_id="poi_pipeline_hcmc_daily",
    default_args=default_args,
    description="Daily OSM POI extraction pipeline for Ho Chi Minh City",
    schedule_interval="0 19 * * *",     # 02:00 ICT = 19:00 UTC
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["geospatial", "poi", "osm", "hcmc"],
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)

    t_extract = PythonOperator(
        task_id="extract_osm",
        python_callable=extract_osm,
    )

    t_normalize = PythonOperator(
        task_id="normalize_poi",
        python_callable=normalize_poi,
    )

    t_dedup = PythonOperator(
        task_id="dedup_poi",
        python_callable=dedup_poi,
    )

    t_load = PythonOperator(
        task_id="load_to_postgis",
        python_callable=load_to_postgis,
    )

    t_summary = PythonOperator(
        task_id="log_run_summary",
        python_callable=log_run_summary,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # ── Pipeline flow ──────────────────────────────────────────────────────────
    start >> t_extract >> t_normalize >> t_dedup >> t_load >> t_summary >> end
