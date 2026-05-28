"""
PDF Extractor – extract text, tables, and metadata from PDF documents.
Supports both text PDFs and scanned PDFs (via OCR fallback).
Used for ingesting financial reports, audit documents, compliance files.
"""
import hashlib
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import pdfplumber
from loguru import logger


@dataclass
class PageContent:
    page_number: int
    text: str
    tables: list[list[list[str]]]   # list of tables → rows → cells
    width: float
    height: float
    char_count: int = 0

    def __post_init__(self):
        self.char_count = len(self.text)


@dataclass
class PDFDocument:
    file_path: str
    file_name: str
    file_size_bytes: int
    page_count: int
    metadata: dict
    pages: list[PageContent]
    full_text: str
    tables: list[dict]
    checksum_md5: str
    extraction_status: str = "success"
    error_message: Optional[str] = None

    @property
    def word_count(self) -> int:
        return len(self.full_text.split())

    @property
    def has_tables(self) -> bool:
        return len(self.tables) > 0


class PDFExtractor:
    """
    Extract structured content from PDF files.

    Hierarchy:
        PDFDocument
        └── pages: [PageContent, ...]
            ├── text  (cleaned plain text)
            └── tables (raw cell matrix)
    """

    # Characters to strip from extracted text
    _NOISE_PATTERN = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]")

    def __init__(
        self,
        extract_tables: bool = True,
        clean_text: bool = True,
        table_settings: Optional[dict] = None,
    ):
        self.extract_tables = extract_tables
        self.clean_text = clean_text
        self.table_settings = table_settings or {
            "vertical_strategy": "lines",
            "horizontal_strategy": "lines",
            "snap_tolerance": 3,
        }

    def extract(self, file_path: str | Path) -> PDFDocument:
        """
        Main entry point. Extract full document structure from a PDF.
        Returns PDFDocument dataclass ready for downstream ETL.
        """
        file_path = Path(file_path)
        if not file_path.exists():
            raise FileNotFoundError(f"PDF not found: {file_path}")

        file_size = file_path.stat().st_size
        checksum = self._compute_md5(file_path)

        logger.info(f"Extracting PDF: {file_path.name} ({file_size:,} bytes)")

        try:
            pages: list[PageContent] = []
            all_tables: list[dict] = []

            with pdfplumber.open(file_path) as pdf:
                page_count = len(pdf.pages)
                raw_metadata = pdf.metadata or {}
                metadata = self._clean_metadata(raw_metadata)

                for i, page in enumerate(pdf.pages):
                    page_content = self._extract_page(page, i + 1)
                    pages.append(page_content)

                    # Collect tables with page reference
                    for j, table in enumerate(page_content.tables):
                        all_tables.append({
                            "page": i + 1,
                            "table_index": j,
                            "rows": len(table),
                            "cols": len(table[0]) if table else 0,
                            "data": table,
                        })

            full_text = "\n\n".join(
                f"[PAGE {p.page_number}]\n{p.text}" for p in pages
            )

            doc = PDFDocument(
                file_path=str(file_path),
                file_name=file_path.name,
                file_size_bytes=file_size,
                page_count=page_count,
                metadata=metadata,
                pages=pages,
                full_text=full_text,
                tables=all_tables,
                checksum_md5=checksum,
            )

            logger.info(
                f"Extracted {doc.page_count} pages, "
                f"{doc.word_count} words, "
                f"{len(doc.tables)} tables from {file_path.name}"
            )
            return doc

        except Exception as exc:
            logger.error(f"PDF extraction failed for {file_path.name}: {exc}")
            return PDFDocument(
                file_path=str(file_path),
                file_name=file_path.name,
                file_size_bytes=file_size,
                page_count=0,
                metadata={},
                pages=[],
                full_text="",
                tables=[],
                checksum_md5=checksum,
                extraction_status="failed",
                error_message=str(exc),
            )

    def _extract_page(self, page, page_number: int) -> PageContent:
        # Extract text
        raw_text = page.extract_text(x_tolerance=2, y_tolerance=2) or ""
        text = self._clean_text(raw_text) if self.clean_text else raw_text

        # Extract tables
        tables = []
        if self.extract_tables:
            try:
                raw_tables = page.extract_tables(self.table_settings)
                for table in (raw_tables or []):
                    cleaned = [
                        [str(cell).strip() if cell is not None else "" for cell in row]
                        for row in table
                    ]
                    tables.append(cleaned)
            except Exception as e:
                logger.warning(f"Table extraction failed on page {page_number}: {e}")

        return PageContent(
            page_number=page_number,
            text=text,
            tables=tables,
            width=float(page.width),
            height=float(page.height),
        )

    def _clean_text(self, text: str) -> str:
        text = self._NOISE_PATTERN.sub("", text)
        # Normalize whitespace but preserve paragraph breaks
        lines = [line.strip() for line in text.splitlines()]
        # Remove empty lines clusters > 2 consecutive
        cleaned_lines = []
        empty_count = 0
        for line in lines:
            if not line:
                empty_count += 1
                if empty_count <= 2:
                    cleaned_lines.append(line)
            else:
                empty_count = 0
                cleaned_lines.append(line)
        return "\n".join(cleaned_lines).strip()

    def _clean_metadata(self, raw: dict) -> dict:
        """Normalize PDF metadata fields."""
        return {
            k: (v.strip() if isinstance(v, str) else str(v))
            for k, v in raw.items()
            if v is not None
        }

    @staticmethod
    def _compute_md5(file_path: Path) -> str:
        md5 = hashlib.md5()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                md5.update(chunk)
        return md5.hexdigest()

    def extract_to_chunks(
        self, file_path: str | Path, chunk_size: int = 1000, overlap: int = 200
    ) -> list[dict]:
        """
        Extract PDF and split into overlapping text chunks for RAG ingestion.
        Returns list of chunk dicts with metadata.
        """
        doc = self.extract(file_path)
        if doc.extraction_status != "success":
            return []

        chunks = []
        text = doc.full_text
        words = text.split()

        for i in range(0, len(words), chunk_size - overlap):
            chunk_words = words[i: i + chunk_size]
            chunk_text = " ".join(chunk_words)

            chunks.append({
                "chunk_index": len(chunks),
                "text": chunk_text,
                "word_count": len(chunk_words),
                "source": doc.file_name,
                "source_type": "pdf",
                "checksum": doc.checksum_md5,
                "metadata": {
                    "file_name": doc.file_name,
                    "page_count": doc.page_count,
                    **doc.metadata,
                },
            })

        logger.info(f"Generated {len(chunks)} chunks from {doc.file_name}")
        return chunks
