"""RAG Query router – document Q&A via LLM + vector retrieval."""
from typing import Optional

from fastapi import APIRouter
from pydantic import BaseModel

router = APIRouter()


class QueryRequest(BaseModel):
    question: str
    top_k: int = 5
    source_filter: Optional[str] = None
    collection: str = "documents"


class QueryResponse(BaseModel):
    question: str
    answer: str
    sources: list[dict]
    tokens_used: int


@router.post("/ask", response_model=QueryResponse)
async def ask_question(request: QueryRequest):
    """
    Ask a question against ingested documents using RAG.
    Returns LLM-generated answer with source citations.
    """
    from src.rag.rag_pipeline import RAGPipeline

    rag = RAGPipeline(
        collection_name=request.collection,
        top_k=request.top_k,
    )

    response = rag.generate(
        question=request.question,
        source_filter=request.source_filter,
    )

    return QueryResponse(
        question=response.question,
        answer=response.answer,
        sources=[
            {
                "source": c.source,
                "score": round(c.score, 4),
                "text_preview": c.text[:200] + "..." if len(c.text) > 200 else c.text,
            }
            for c in response.sources
        ],
        tokens_used=response.tokens_used,
    )


@router.post("/retrieve")
async def retrieve_chunks(request: QueryRequest):
    """
    Retrieve top-K relevant chunks WITHOUT generating an LLM answer.
    Useful for debugging retrieval quality.
    """
    from src.rag.rag_pipeline import RAGPipeline

    rag = RAGPipeline(collection_name=request.collection, top_k=request.top_k)
    chunks = rag.retrieve(request.question, source_filter=request.source_filter)

    return {
        "question": request.question,
        "retrieved": [
            {
                "chunk_id": c.chunk_id,
                "source": c.source,
                "score": round(c.score, 4),
                "text": c.text,
                "metadata": c.metadata,
            }
            for c in chunks
        ],
    }


@router.get("/stats")
async def rag_stats(collection: str = "documents"):
    """Return RAG vector store statistics."""
    from src.rag.rag_pipeline import RAGPipeline
    rag = RAGPipeline(collection_name=collection)
    return rag.stats()
