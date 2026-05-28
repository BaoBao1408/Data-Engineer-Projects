"""
RAG (Retrieval-Augmented Generation) Pipeline.
Ingests document chunks → embeds → stores in ChromaDB → retrieves for LLM context.

Architecture:
    Documents → Chunks → Embeddings → ChromaDB
                                          ↓
    User Query → Embed → Similarity Search → Context → LLM → Answer
"""
import hashlib
from dataclasses import dataclass, field
from typing import Any, Optional

import chromadb
from chromadb.config import Settings as ChromaSettings
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain_openai import AzureOpenAIEmbeddings, OpenAIEmbeddings
from loguru import logger
from openai import AzureOpenAI, OpenAI

from src.config import get_settings

settings = get_settings()


@dataclass
class RetrievedChunk:
    chunk_id: str
    text: str
    source: str
    score: float
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class RAGResponse:
    question: str
    answer: str
    sources: list[RetrievedChunk]
    context_used: str
    model: str
    tokens_used: int = 0


class EmbeddingService:
    """Unified embedding service: OpenAI or Azure OpenAI."""

    def __init__(self):
        self._use_azure = bool(settings.openai.azure_openai_endpoint)
        if self._use_azure:
            self._client = AzureOpenAI(
                azure_endpoint=settings.openai.azure_openai_endpoint,
                api_key=settings.openai.azure_openai_api_key,
                api_version=settings.openai.azure_openai_api_version,
            )
        else:
            self._client = OpenAI(api_key=settings.openai.openai_api_key)

    def embed(self, texts: list[str]) -> list[list[float]]:
        """Embed a list of texts. Returns list of vectors."""
        if not texts:
            return []

        # Batch in chunks of 100 (API limit)
        all_embeddings = []
        batch_size = 100
        for i in range(0, len(texts), batch_size):
            batch = texts[i: i + batch_size]
            response = self._client.embeddings.create(
                model=settings.openai.embedding_model,
                input=batch,
            )
            all_embeddings.extend([e.embedding for e in response.data])
            logger.debug(f"Embedded {len(batch)} texts (batch {i // batch_size + 1})")

        return all_embeddings

    def embed_single(self, text: str) -> list[float]:
        return self.embed([text])[0]


class VectorStore:
    """ChromaDB vector store wrapper."""

    def __init__(self, collection_name: str = "documents"):
        self._collection_name = collection_name
        self._client = chromadb.HttpClient(
            host=settings.vector_store.chroma_host,
            port=settings.vector_store.chroma_port,
        )
        self._collection = self._get_or_create_collection()

    def _get_or_create_collection(self):
        return self._client.get_or_create_collection(
            name=self._collection_name,
            metadata={"hnsw:space": "cosine"},   # Cosine similarity
        )

    def add_chunks(
        self,
        chunks: list[dict],
        embeddings: list[list[float]],
    ) -> int:
        """Add text chunks with precomputed embeddings to the store."""
        if not chunks:
            return 0

        ids = [
            c.get("chunk_id")
            or hashlib.md5(c["text"].encode()).hexdigest()
            for c in chunks
        ]
        documents = [c["text"] for c in chunks]
        metadatas = [
            {
                "source": c.get("source", ""),
                "source_type": c.get("source_type", ""),
                "chunk_index": c.get("chunk_index", 0),
                **{
                    k: str(v)
                    for k, v in (c.get("metadata") or {}).items()
                },
            }
            for c in chunks
        ]

        self._collection.upsert(
            ids=ids,
            embeddings=embeddings,
            documents=documents,
            metadatas=metadatas,
        )
        logger.info(
            f"Stored {len(chunks)} chunks in collection '{self._collection_name}'"
        )
        return len(chunks)

    def search(
        self,
        query_embedding: list[float],
        n_results: int = 5,
        where: Optional[dict] = None,
    ) -> list[RetrievedChunk]:
        """Similarity search. Returns top-N chunks."""
        kwargs: dict = {"query_embeddings": [query_embedding], "n_results": n_results}
        if where:
            kwargs["where"] = where

        results = self._collection.query(**kwargs)

        chunks = []
        for i, (doc_id, text, meta, distance) in enumerate(
            zip(
                results["ids"][0],
                results["documents"][0],
                results["metadatas"][0],
                results["distances"][0],
            )
        ):
            chunks.append(RetrievedChunk(
                chunk_id=doc_id,
                text=text,
                source=meta.get("source", ""),
                score=1.0 - float(distance),  # Convert distance to similarity
                metadata=meta,
            ))

        return chunks

    def delete_by_source(self, source: str) -> int:
        """Remove all chunks from a specific source document."""
        results = self._collection.get(where={"source": source})
        if results["ids"]:
            self._collection.delete(ids=results["ids"])
            logger.info(f"Deleted {len(results['ids'])} chunks from source: {source}")
            return len(results["ids"])
        return 0

    def count(self) -> int:
        return self._collection.count()


class RAGPipeline:
    """
    End-to-end RAG pipeline:
        Ingest: chunks → embed → store
        Query:  question → embed → retrieve → generate
    """

    SYSTEM_PROMPT = """You are an expert data analyst assistant for KPMG.
Answer questions based ONLY on the provided context from internal documents.
If the context doesn't contain enough information, say so clearly.
Always cite the source documents you used.
Be concise, accurate, and professional."""

    def __init__(
        self,
        collection_name: str = "documents",
        top_k: int = 5,
        max_context_tokens: int = 3000,
    ):
        self.top_k = top_k
        self.max_context_tokens = max_context_tokens
        self.embedder = EmbeddingService()
        self.vector_store = VectorStore(collection_name)

        # LLM client
        if settings.openai.azure_openai_endpoint:
            self._llm = AzureOpenAI(
                azure_endpoint=settings.openai.azure_openai_endpoint,
                api_key=settings.openai.azure_openai_api_key,
                api_version=settings.openai.azure_openai_api_version,
            )
        else:
            self._llm = OpenAI(api_key=settings.openai.openai_api_key)

    # ─── Ingestion ────────────────────────────────────────────────────────────

    def ingest_chunks(self, chunks: list[dict]) -> int:
        """Embed and store document chunks."""
        if not chunks:
            return 0

        texts = [c["text"] for c in chunks]
        logger.info(f"Embedding {len(texts)} chunks…")
        embeddings = self.embedder.embed(texts)
        stored = self.vector_store.add_chunks(chunks, embeddings)
        logger.info(f"RAG ingestion complete: {stored} chunks stored")
        return stored

    def ingest_text(
        self,
        text: str,
        source: str,
        source_type: str = "text",
        chunk_size: int = 800,
        chunk_overlap: int = 150,
        metadata: Optional[dict] = None,
    ) -> int:
        """Split raw text into chunks, embed, and store."""
        splitter = RecursiveCharacterTextSplitter(
            chunk_size=chunk_size,
            chunk_overlap=chunk_overlap,
            separators=["\n\n", "\n", ". ", " "],
        )
        texts = splitter.split_text(text)
        chunks = [
            {
                "text": t,
                "source": source,
                "source_type": source_type,
                "chunk_index": i,
                "metadata": metadata or {},
            }
            for i, t in enumerate(texts)
        ]
        return self.ingest_chunks(chunks)

    # ─── Query ────────────────────────────────────────────────────────────────

    def retrieve(
        self,
        question: str,
        n_results: Optional[int] = None,
        source_filter: Optional[str] = None,
    ) -> list[RetrievedChunk]:
        """Embed question and retrieve similar chunks."""
        query_vec = self.embedder.embed_single(question)
        where = {"source": source_filter} if source_filter else None
        return self.vector_store.search(
            query_vec, n_results or self.top_k, where=where
        )

    def generate(
        self,
        question: str,
        retrieved_chunks: Optional[list[RetrievedChunk]] = None,
        source_filter: Optional[str] = None,
    ) -> RAGResponse:
        """Retrieve context and generate an answer using LLM."""
        if retrieved_chunks is None:
            retrieved_chunks = self.retrieve(question, source_filter=source_filter)

        # Build context string
        context_parts = []
        for i, chunk in enumerate(retrieved_chunks):
            context_parts.append(
                f"[Source {i + 1}: {chunk.source}]\n{chunk.text}"
            )
        context = "\n\n---\n\n".join(context_parts)

        # Truncate to token budget (rough estimate: 1 token ≈ 4 chars)
        max_chars = self.max_context_tokens * 4
        if len(context) > max_chars:
            context = context[:max_chars] + "\n...[truncated]"

        user_message = (
            f"Context:\n{context}\n\n"
            f"Question: {question}\n\n"
            "Answer:"
        )

        response = self._llm.chat.completions.create(
            model=settings.openai.llm_model,
            messages=[
                {"role": "system", "content": self.SYSTEM_PROMPT},
                {"role": "user", "content": user_message},
            ],
            temperature=0.1,
            max_tokens=1500,
        )

        answer = response.choices[0].message.content or ""
        tokens_used = response.usage.total_tokens if response.usage else 0

        return RAGResponse(
            question=question,
            answer=answer,
            sources=retrieved_chunks,
            context_used=context,
            model=settings.openai.llm_model,
            tokens_used=tokens_used,
        )

    def stats(self) -> dict:
        return {
            "collection": self.vector_store._collection_name,
            "total_chunks": self.vector_store.count(),
        }
