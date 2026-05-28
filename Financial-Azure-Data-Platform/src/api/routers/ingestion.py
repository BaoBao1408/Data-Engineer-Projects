"""Ingestion router – upload and ingest documents into the Data Lake."""
import uuid
from typing import Annotated, Literal

from fastapi import APIRouter, File, Form, HTTPException, UploadFile, status
from pydantic import BaseModel

from src.ingestion.connectors.azure_blob_connector import get_data_lake
from src.ingestion.extractors.excel_extractor import ExcelExtractor
from src.ingestion.extractors.pdf_extractor import PDFExtractor
from src.ingestion.extractors.word_extractor import WordExtractor
from src.rag.rag_pipeline import RAGPipeline

router = APIRouter()


class IngestionResponse(BaseModel):
    file_name: str
    file_size_bytes: int
    blob_uri: str
    chunks_stored: int
    status: str


@router.post("/upload", response_model=IngestionResponse)
async def upload_document(
    file: UploadFile = File(...),
    ingest_to_rag: bool = Form(default=True),
    zone: Literal["raw", "processed", "curated"] = Form(default="raw"),
):
    """
    Upload a document (PDF/Excel/Word) to the Data Lake and optionally
    ingest into the RAG vector store for Q&A.
    """
    if not file.filename:
        raise HTTPException(status_code=400, detail="No file provided")

    content = await file.read()
    file_ext = file.filename.rsplit(".", 1)[-1].lower()

    if file_ext not in ("pdf", "xlsx", "xls", "docx", "csv"):
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported file type: .{file_ext}",
        )

    # 1. Upload raw file to Data Lake
    run_id = uuid.uuid4().hex[:8]
    blob_name = f"uploads/{run_id}/{file.filename}"
    data_lake = get_data_lake()
    uri = data_lake.upload_bytes(
        content, blob_name, zone=zone, content_type=file.content_type or "application/octet-stream"
    )

    # 2. Extract text and ingest into RAG
    chunks_stored = 0
    if ingest_to_rag:
        import tempfile, os
        with tempfile.NamedTemporaryFile(suffix=f".{file_ext}", delete=False) as tmp:
            tmp.write(content)
            tmp_path = tmp.name

        try:
            rag = RAGPipeline()
            if file_ext == "pdf":
                extractor = PDFExtractor()
                chunks = extractor.extract_to_chunks(tmp_path)
            elif file_ext in ("xlsx", "xls"):
                extractor = ExcelExtractor()
                doc = extractor.extract(tmp_path)
                text = "\n".join(
                    sheet.dataframe.to_string()
                    for sheet in doc.sheets.values()
                )
                chunks_stored = rag.ingest_text(text, source=file.filename, source_type="xlsx")
                chunks = []
            elif file_ext == "docx":
                extractor = WordExtractor()
                chunks = extractor.extract_to_chunks(tmp_path)
            else:
                chunks = []

            if chunks:
                chunks_stored = rag.ingest_chunks(chunks)
        finally:
            os.unlink(tmp_path)

    return IngestionResponse(
        file_name=file.filename,
        file_size_bytes=len(content),
        blob_uri=uri,
        chunks_stored=chunks_stored,
        status="success",
    )


@router.get("/list")
async def list_uploaded_files(zone: str = "raw", prefix: str = "uploads/"):
    """List files in the Data Lake."""
    data_lake = get_data_lake()
    blobs = list(data_lake.list_blobs(prefix=prefix, zone=zone))
    return {"zone": zone, "count": len(blobs), "files": blobs}
