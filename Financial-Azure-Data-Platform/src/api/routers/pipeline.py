"""Pipeline, RAG Query, and Knowledge Graph routers."""
from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

# ─── Pipeline Router ──────────────────────────────────────────────────────────
router = APIRouter()


class PipelineRunRequest(BaseModel):
    source_blob: str
    pipeline_name: str
    target_table: str
    schema: str = "public"
    upsert_key: Optional[str] = None


class PipelineRunResponse(BaseModel):
    run_id: str
    pipeline_name: str
    status: str
    stages_completed: list[str]
    metrics: dict
    errors: list[str]


@router.post("/run", response_model=PipelineRunResponse)
async def run_pipeline(request: PipelineRunRequest):
    """Trigger an ETL pipeline run for a blob in the Data Lake."""
    import pandas as pd
    from src.ingestion.connectors.azure_blob_connector import get_data_lake
    from src.etl.pipelines.document_pipeline import DocumentETLPipeline

    data_lake = get_data_lake()

    # Download blob to DataFrame
    try:
        raw_bytes = data_lake.download_bytes(request.source_blob, zone="raw")
    except Exception as e:
        raise HTTPException(status_code=404, detail=f"Blob not found: {e}")

    import io
    if request.source_blob.endswith(".parquet"):
        df = pd.read_parquet(io.BytesIO(raw_bytes))
    elif request.source_blob.endswith(".csv"):
        df = pd.read_csv(io.BytesIO(raw_bytes))
    else:
        raise HTTPException(status_code=400, detail="Unsupported format for pipeline. Use .parquet or .csv")

    pipeline = DocumentETLPipeline()
    ctx = pipeline.run(
        df=df,
        pipeline_name=request.pipeline_name,
        source_file=request.source_blob,
        target_table=request.target_table,
        schema=request.schema,
        upsert_key=request.upsert_key,
    )

    return PipelineRunResponse(
        run_id=ctx.run_id,
        pipeline_name=ctx.pipeline_name,
        status="completed" if not ctx.errors else "completed_with_errors",
        stages_completed=ctx.stages_completed,
        metrics=ctx.metrics,
        errors=ctx.errors,
    )


@router.get("/runs")
async def list_pipeline_runs():
    """List recent pipeline runs from the warehouse."""
    from src.ingestion.connectors.sql_connector import get_warehouse
    warehouse = get_warehouse()
    try:
        runs = warehouse.execute("""
            SELECT run_id, pipeline_name, status, started_at, completed_at
            FROM pipeline_runs
            ORDER BY started_at DESC
            LIMIT 50
        """)
        return {"runs": runs}
    except Exception as e:
        return {"runs": [], "note": str(e)}
