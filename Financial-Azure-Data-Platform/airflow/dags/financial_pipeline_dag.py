"""
Airflow DAG: Financial Data Pipeline
Schedule: Daily at 02:00 AM ICT

Flow:
    1. ingest_documents   → Scan blob storage for new files → extract text
    2. run_etl_bronze     → Raw data → Bronze layer
    3. run_etl_silver     → Bronze → Silver (clean + validate)
    4. run_etl_gold       → Silver → Gold (aggregate + KPIs)
    5. compute_ratios     → Calculate financial ratios per entity
    6. detect_anomalies   → GL anomaly detection
    7. index_knowledge_graph → Build/update Neo4j KG from financial entities
    8. index_rag          → Embed & store new documents in ChromaDB
    9. data_quality_check → Great Expectations suite
   10. notify_on_failure  → Alert on Slack / email
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule

# ─── Default Args ─────────────────────────────────────────────────────────────
DEFAULT_ARGS = {
    "owner":            "edp-team",
    "depends_on_past":  False,
    "email":            ["data-alerts@kpmg.com.vn"],
    "email_on_failure": True,
    "email_on_retry":   False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}

# ─── Task Functions ───────────────────────────────────────────────────────────

def scan_and_ingest_documents(**context):
    """Scan Data Lake raw zone for unprocessed documents and ingest."""
    import sys
    sys.path.insert(0, "/opt/airflow/src")
    from ingestion.connectors.azure_blob_connector import get_data_lake
    from ingestion.connectors.sql_connector import get_warehouse
    from ingestion.extractors.pdf_extractor import PDFExtractor
    from ingestion.extractors.excel_extractor import ExcelExtractor
    from ingestion.extractors.word_extractor import WordExtractor

    warehouse = get_warehouse()
    data_lake  = get_data_lake()

    # Get unprocessed documents
    pending_docs = warehouse.execute("""
        SELECT document_id, file_name, file_type, blob_uri, blob_zone
        FROM financial.documents
        WHERE extraction_status = 'PENDING'
        ORDER BY ingested_at ASC
        LIMIT 50
    """)

    processed = 0
    failed = 0

    for doc in pending_docs:
        try:
            raw_bytes = data_lake.download_bytes(doc["file_name"], zone=doc["blob_zone"])
            import tempfile, os
            ext = doc["file_type"].lower()
            with tempfile.NamedTemporaryFile(suffix=f".{ext}", delete=False) as tmp:
                tmp.write(raw_bytes)
                tmp_path = tmp.name

            word_count = 0
            page_count = 0

            if ext == "pdf":
                extractor = PDFExtractor()
                result = extractor.extract(tmp_path)
                word_count = result.word_count
                page_count = result.page_count
            elif ext in ("xlsx", "xls"):
                extractor = ExcelExtractor()
                result = extractor.extract(tmp_path)
                word_count = sum(s.row_count for s in result.sheets.values())
            elif ext == "docx":
                extractor = WordExtractor()
                result = extractor.extract(tmp_path)
                word_count = result.word_count

            os.unlink(tmp_path)

            warehouse.execute("""
                UPDATE financial.documents
                SET extraction_status = 'SUCCESS',
                    word_count = :wc,
                    page_count = :pc,
                    processed_at = NOW()
                WHERE document_id = :did
            """, {"wc": word_count, "pc": page_count, "did": str(doc["document_id"])})
            processed += 1

        except Exception as e:
            warehouse.execute("""
                UPDATE financial.documents
                SET extraction_status = 'FAILED'
                WHERE document_id = :did
            """, {"did": str(doc["document_id"])})
            failed += 1
            print(f"[WARN] Failed to process {doc['file_name']}: {e}")

    print(f"Document ingestion: {processed} success, {failed} failed")
    context["ti"].xcom_push(key="docs_processed", value=processed)
    return processed


def run_etl_pipeline(**context):
    """Run Bronze → Silver → Gold ETL for any pending parquet blobs."""
    import sys
    sys.path.insert(0, "/opt/airflow/src")
    from ingestion.connectors.azure_blob_connector import get_data_lake
    from etl.pipelines.document_pipeline import DocumentETLPipeline

    data_lake = get_data_lake()
    pipeline  = DocumentETLPipeline()

    # List processed-zone blobs awaiting Gold load
    blobs = list(data_lake.list_blobs(prefix="*/silver/", zone="processed"))
    blobs = [b for b in blobs if b["name"].endswith(".parquet")]

    print(f"Found {len(blobs)} silver blobs to promote to Gold")
    runs_completed = []

    for blob in blobs[:20]:   # cap per run
        try:
            raw = data_lake.download_bytes(blob["name"], zone="processed")
            import pandas as pd, io
            df = pd.read_parquet(io.BytesIO(raw))

            # Derive target table from blob path: {pipeline}/silver/{run_id}/cleaned.parquet
            parts = blob["name"].split("/")
            pipeline_name = parts[0] if parts else "unknown"

            ctx = pipeline.run(
                df=df,
                pipeline_name=pipeline_name,
                source_file=blob["name"],
                target_table=f"financial.documents_staging",
            )
            runs_completed.append(ctx.run_id)
        except Exception as e:
            print(f"[WARN] ETL failed for {blob['name']}: {e}")

    print(f"ETL complete. Runs: {runs_completed}")
    return runs_completed


def compute_financial_ratios(**context):
    """Compute financial ratios for all entities with data in current year."""
    import sys
    sys.path.insert(0, "/opt/airflow/src")
    from ingestion.connectors.sql_connector import get_warehouse
    from etl.pipelines.financial_pipeline import FinancialRatioPipeline

    warehouse = get_warehouse()
    pipeline  = FinancialRatioPipeline()
    target_year = datetime.now().year - 1   # Prior completed fiscal year

    entities = warehouse.execute("""
        SELECT DISTINCT entity_id
        FROM financial.financial_statements fs
        JOIN financial.fiscal_periods fp ON fp.period_id = fs.period_id
        WHERE fp.fiscal_year = :year
          AND fs.statement_type IN ('BALANCE_SHEET', 'INCOME_STATEMENT')
    """, {"year": target_year})

    computed = 0
    for row in entities:
        ratios = pipeline.compute_ratios_for_entity(
            entity_id=str(row["entity_id"]),
            fiscal_year=target_year,
        )
        if ratios:
            pipeline.upsert_ratios(ratios)
            computed += 1

    print(f"Computed ratios for {computed} entities (FY{target_year})")
    return computed


def detect_gl_anomalies(**context):
    """Run GL anomaly detection for current fiscal year."""
    import sys
    sys.path.insert(0, "/opt/airflow/src")
    from ingestion.connectors.sql_connector import get_warehouse
    from etl.pipelines.financial_pipeline import GLAnomalyDetector

    warehouse = get_warehouse()
    detector  = GLAnomalyDetector()
    target_year = datetime.now().year

    entities = warehouse.execute("""
        SELECT DISTINCT entity_id
        FROM financial.gl_transactions
        WHERE EXTRACT(YEAR FROM transaction_date) = :year
          AND anomaly_flag = FALSE
    """, {"year": target_year})

    total_flagged = 0
    for row in entities:
        flagged = detector.detect_and_flag(str(row["entity_id"]), target_year)
        total_flagged += len(flagged)

    print(f"GL anomaly detection: {total_flagged} transactions flagged")
    return total_flagged


def index_knowledge_graph(**context):
    """
    Build Knowledge Graph from financial entities, engagements, and relationships.
    Nodes: Entity, Person, Engagement, Finding
    Rels : AUDITED_BY, HAS_ENGAGEMENT, FOUND_IN, RELATED_PARTY
    """
    import sys
    sys.path.insert(0, "/opt/airflow/src")
    from ingestion.connectors.sql_connector import get_warehouse
    from knowledge_graph.neo4j_client import get_neo4j, GraphNode, GraphRelationship

    warehouse = get_warehouse()
    neo4j     = get_neo4j()

    # 1. Upsert Entity nodes
    entities = warehouse.execute("""
        SELECT entity_id, entity_code, legal_name, short_name, entity_type,
               industry_code, functional_currency, stock_exchange, ticker_symbol,
               country, province, is_active
        FROM financial.entities WHERE is_active = TRUE
    """)

    entity_nodes = [
        GraphNode(
            id=str(e["entity_id"]),
            label="Entity",
            properties={
                "code":          e["entity_code"],
                "name":          e["legal_name"],
                "short_name":    e.get("short_name", ""),
                "type":          e["entity_type"],
                "industry":      e.get("industry_code", ""),
                "currency":      e["functional_currency"],
                "exchange":      e.get("stock_exchange", ""),
                "ticker":        e.get("ticker_symbol", ""),
                "country":       e.get("country", "VN"),
                "is_active":     str(e["is_active"]),
            },
        )
        for e in entities
    ]
    neo4j.upsert_nodes_batch(entity_nodes)
    print(f"KG: Upserted {len(entity_nodes)} Entity nodes")

    # 2. Upsert Engagement nodes + AUDITED_BY relationships
    engagements = warehouse.execute("""
        SELECT eng.engagement_id, eng.engagement_code, eng.entity_id,
               eng.engagement_type, eng.fiscal_year, eng.status,
               eng.audit_opinion, eng.partner_in_charge
        FROM audit.engagements eng
    """)

    eng_nodes = [
        GraphNode(
            id=str(eng["engagement_id"]),
            label="Engagement",
            properties={
                "code":       eng["engagement_code"],
                "type":       eng["engagement_type"],
                "year":       str(eng["fiscal_year"]),
                "status":     eng["status"],
                "opinion":    eng.get("audit_opinion", ""),
                "partner":    eng.get("partner_in_charge", ""),
            },
        )
        for eng in engagements
    ]
    neo4j.upsert_nodes_batch(eng_nodes)

    rels = [
        GraphRelationship(
            from_id=str(eng["entity_id"]),
            to_id=str(eng["engagement_id"]),
            rel_type="HAS_ENGAGEMENT",
            properties={"fiscal_year": str(eng["fiscal_year"])},
        )
        for eng in engagements
    ]
    neo4j.upsert_relationships_batch(rels)
    print(f"KG: Upserted {len(eng_nodes)} Engagement nodes + {len(rels)} HAS_ENGAGEMENT rels")

    # 3. Related-party relationships from GL intercompany transactions
    interco = warehouse.execute("""
        SELECT DISTINCT entity_id, counterparty_id
        FROM financial.gl_transactions
        WHERE transaction_type = 'INTERCOMPANY'
          AND counterparty_id IS NOT NULL
    """)
    interco_rels = [
        GraphRelationship(
            from_id=str(r["entity_id"]),
            to_id=str(r["counterparty_id"]),
            rel_type="TRANSACTS_WITH",
            properties={"transaction_type": "INTERCOMPANY"},
        )
        for r in interco
    ]
    if interco_rels:
        neo4j.upsert_relationships_batch(interco_rels)
        print(f"KG: Upserted {len(interco_rels)} TRANSACTS_WITH relationships")

    return {
        "entities": len(entity_nodes),
        "engagements": len(eng_nodes),
        "relationships": len(rels) + len(interco_rels),
    }


def index_rag_documents(**context):
    """Embed and store newly extracted documents in ChromaDB for RAG."""
    import sys
    sys.path.insert(0, "/opt/airflow/src")
    from ingestion.connectors.sql_connector import get_warehouse
    from ingestion.connectors.azure_blob_connector import get_data_lake
    from ingestion.extractors.pdf_extractor import PDFExtractor
    from ingestion.extractors.word_extractor import WordExtractor
    from rag.rag_pipeline import RAGPipeline

    warehouse = get_warehouse()
    data_lake  = get_data_lake()
    rag        = RAGPipeline()

    docs = warehouse.execute("""
        SELECT document_id, file_name, file_type, blob_zone
        FROM financial.documents
        WHERE extraction_status = 'SUCCESS'
          AND rag_indexed = FALSE
        LIMIT 30
    """)

    indexed = 0
    for doc in docs:
        try:
            raw = data_lake.download_bytes(doc["file_name"], zone=doc["blob_zone"])
            import tempfile, os
            ext = doc["file_type"].lower()
            with tempfile.NamedTemporaryFile(suffix=f".{ext}", delete=False) as tmp:
                tmp.write(raw)
                tmp_path = tmp.name

            if ext == "pdf":
                chunks = PDFExtractor().extract_to_chunks(tmp_path)
            elif ext == "docx":
                chunks = WordExtractor().extract_to_chunks(tmp_path)
            else:
                chunks = []

            os.unlink(tmp_path)

            if chunks:
                rag.ingest_chunks(chunks)

            warehouse.execute("""
                UPDATE financial.documents
                SET rag_indexed = TRUE WHERE document_id = :did
            """, {"did": str(doc["document_id"])})
            indexed += 1

        except Exception as e:
            print(f"[WARN] RAG indexing failed for {doc['file_name']}: {e}")

    print(f"RAG indexed {indexed} documents")
    return indexed


def run_data_quality_checks(**context):
    """Run data quality checks on critical financial tables."""
    import sys
    sys.path.insert(0, "/opt/airflow/src")
    from ingestion.connectors.sql_connector import get_warehouse
    from quality.validators.schema_validator import DataQualityValidator

    warehouse = get_warehouse()
    validator = DataQualityValidator()
    import pandas as pd

    # Check 1: Financial ratios completeness
    ratios_df = pd.DataFrame(warehouse.execute("""
        SELECT entity_id, net_profit_margin, return_on_equity, current_ratio
        FROM financial.financial_ratios
        WHERE computed_at > NOW() - INTERVAL '2 days'
    """))

    if not ratios_df.empty:
        result = validator.validate(ratios_df, {
            "_table": [{"type": "row_count", "min": 1}],
            "entity_id": [{"type": "not_null"}],
        })
        print(f"Ratios QC: pass_rate={result.pass_rate:.2%}")

    # Check 2: No duplicate GL journal entries in last 24h
    gl_df = pd.DataFrame(warehouse.execute("""
        SELECT journal_entry_no, COUNT(*) as cnt
        FROM financial.gl_transactions
        WHERE created_at > NOW() - INTERVAL '1 day'
        GROUP BY journal_entry_no HAVING COUNT(*) > 1
    """))

    if not gl_df.empty:
        print(f"[WARN] {len(gl_df)} duplicate GL journal entries detected!")
        context["ti"].xcom_push(key="dq_warnings", value=len(gl_df))
    else:
        print("GL duplicate check: PASSED")

    return True


def notify_completion(**context):
    """Log pipeline completion summary."""
    ti = context["ti"]
    docs     = ti.xcom_pull(task_ids="ingest_documents",    key="docs_processed") or 0
    flagged  = ti.xcom_pull(task_ids="detect_gl_anomalies") or 0
    kg_stats = ti.xcom_pull(task_ids="index_knowledge_graph") or {}

    print(f"""
    ══════════════════════════════════════
    Financial Pipeline Run Complete
    ══════════════════════════════════════
    Documents processed  : {docs}
    GL transactions flagged: {flagged}
    KG entities updated  : {kg_stats.get('entities', 0)}
    KG relationships     : {kg_stats.get('relationships', 0)}
    Execution date       : {context['ds']}
    ══════════════════════════════════════
    """)


# ─── DAG Definition ───────────────────────────────────────────────────────────

with DAG(
    dag_id="financial_data_pipeline",
    default_args=DEFAULT_ARGS,
    description="End-to-end financial data pipeline: ingest → ETL → KG → RAG → QC",
    schedule_interval="0 2 * * *",        # Daily 02:00 AM
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["financial", "etl", "kpmg", "production"],
    doc_md=__doc__,
) as dag:

    start = EmptyOperator(task_id="start")
    end   = EmptyOperator(task_id="end", trigger_rule=TriggerRule.ALL_DONE)

    t_ingest = PythonOperator(
        task_id="ingest_documents",
        python_callable=scan_and_ingest_documents,
    )

    t_etl = PythonOperator(
        task_id="run_etl_pipeline",
        python_callable=run_etl_pipeline,
    )

    t_ratios = PythonOperator(
        task_id="compute_financial_ratios",
        python_callable=compute_financial_ratios,
    )

    t_anomaly = PythonOperator(
        task_id="detect_gl_anomalies",
        python_callable=detect_gl_anomalies,
    )

    t_kg = PythonOperator(
        task_id="index_knowledge_graph",
        python_callable=index_knowledge_graph,
    )

    t_rag = PythonOperator(
        task_id="index_rag_documents",
        python_callable=index_rag_documents,
    )

    t_dq = PythonOperator(
        task_id="data_quality_checks",
        python_callable=run_data_quality_checks,
    )

    t_notify = PythonOperator(
        task_id="notify_completion",
        python_callable=notify_completion,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # ─── Task Dependencies (DAG Graph) ────────────────────────────────────
    #
    #   start
    #     └─► ingest_documents
    #               └─► run_etl_pipeline ──► compute_financial_ratios ─┐
    #                                    └─► detect_gl_anomalies       ├─► data_quality_checks ─► notify ─► end
    #               └─► index_knowledge_graph ──────────────────────────┤
    #               └─► index_rag_documents ─────────────────────────────┘

    start >> t_ingest
    t_ingest >> t_etl >> [t_ratios, t_anomaly]
    t_ingest >> [t_kg, t_rag]
    [t_ratios, t_anomaly, t_kg, t_rag] >> t_dq >> t_notify >> end
