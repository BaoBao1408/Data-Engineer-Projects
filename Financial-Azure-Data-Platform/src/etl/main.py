"""ETL Service – standalone runner for Bronze→Silver→Gold pipeline."""
import sys
from loguru import logger
from src.etl.pipelines.financial_pipeline import FinancialRatioPipeline, GLAnomalyDetector
from src.ingestion.connectors.sql_connector import get_warehouse


def run_ratio_computation(fiscal_year: int):
    warehouse = get_warehouse()
    pipeline  = FinancialRatioPipeline()
    entities  = warehouse.execute(
        "SELECT DISTINCT entity_id FROM financial.entities WHERE is_active=TRUE"
    )
    for row in entities:
        ratios = pipeline.compute_ratios_for_entity(str(row["entity_id"]), fiscal_year)
        if ratios:
            pipeline.upsert_ratios(ratios)
    logger.info(f"Ratio computation done for FY{fiscal_year}")


def run_anomaly_detection(fiscal_year: int):
    warehouse = get_warehouse()
    detector  = GLAnomalyDetector()
    entities  = warehouse.execute(
        "SELECT DISTINCT entity_id FROM financial.entities WHERE is_active=TRUE"
    )
    total = 0
    for row in entities:
        flagged = detector.detect_and_flag(str(row["entity_id"]), fiscal_year)
        total += len(flagged)
    logger.info(f"Anomaly detection done: {total} flagged")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--task",  choices=["ratios", "anomalies", "all"], default="all")
    parser.add_argument("--year",  type=int, default=2024)
    args = parser.parse_args()

    if args.task in ("ratios",    "all"): run_ratio_computation(args.year)
    if args.task in ("anomalies", "all"): run_anomaly_detection(args.year)
