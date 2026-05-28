"""
Ingestion Service – watches Data Lake raw zone and processes new files.
Runs as a standalone Docker container, triggered by Airflow or cron.
"""
import os
import sys
import time
from pathlib import Path

from loguru import logger

from src.config import get_settings
from src.ingestion.connectors.azure_blob_connector import get_data_lake
from src.ingestion.connectors.sql_connector import get_warehouse
from src.ingestion.extractors.excel_extractor import ExcelExtractor
from src.ingestion.extractors.pdf_extractor import PDFExtractor
from src.ingestion.extractors.word_extractor import WordExtractor

settings = get_settings()

EXTRACTOR_MAP = {
    "pdf":  PDFExtractor(),
    "xlsx": ExcelExtractor(),
    "xls":  ExcelExtractor(),
    "docx": WordExtractor(),
}


def process_pending_documents() -> dict:
    warehouse  = get_warehouse()
    data_lake  = get_data_lake()

    pending = warehouse.execute("""
        SELECT document_id, file_name, file_type, blob_zone
        FROM financial.documents
        WHERE extraction_status = 'PENDING'
        ORDER BY ingested_at ASC
        LIMIT 100
    """)

    stats = {"processed": 0, "failed": 0, "skipped": 0}

    for doc in pending:
        ext = doc["file_type"].lower()
        extractor = EXTRACTOR_MAP.get(ext)

        if not extractor:
            warehouse.execute(
                "UPDATE financial.documents SET extraction_status='SKIPPED' WHERE document_id=:did",
                {"did": str(doc["document_id"])},
            )
            stats["skipped"] += 1
            continue

        import tempfile
        try:
            raw = data_lake.download_bytes(doc["file_name"], zone=doc["blob_zone"])
            with tempfile.NamedTemporaryFile(suffix=f".{ext}", delete=False) as tmp:
                tmp.write(raw)
                tmp_path = tmp.name

            result = extractor.extract(tmp_path)
            os.unlink(tmp_path)

            word_count  = getattr(result, "word_count",  0) or 0
            page_count  = getattr(result, "page_count",  0) or 0

            warehouse.execute("""
                UPDATE financial.documents
                SET extraction_status='SUCCESS',
                    word_count=:wc, page_count=:pc, processed_at=NOW()
                WHERE document_id=:did
            """, {"wc": word_count, "pc": page_count, "did": str(doc["document_id"])})
            stats["processed"] += 1
            logger.info(f"✓ {doc['file_name']} ({word_count} words)")

        except Exception as e:
            warehouse.execute(
                "UPDATE financial.documents SET extraction_status='FAILED' WHERE document_id=:did",
                {"did": str(doc["document_id"])},
            )
            stats["failed"] += 1
            logger.error(f"✗ {doc['file_name']}: {e}")

    return stats


if __name__ == "__main__":
    logger.info("Ingestion service started")
    poll_interval = int(os.getenv("POLL_INTERVAL_SECONDS", "60"))

    while True:
        stats = process_pending_documents()
        logger.info(f"Cycle complete: {stats}")
        time.sleep(poll_interval)
