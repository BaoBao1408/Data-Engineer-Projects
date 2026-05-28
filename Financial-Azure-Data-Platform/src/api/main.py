"""
Enterprise Data Platform – FastAPI Application.

Endpoints:
    /health                  – Health check (all services)
    /api/v1/ingest/*         – Document ingestion
    /api/v1/pipeline/*       – ETL pipeline triggers
    /api/v1/query/*          – RAG query interface
    /api/v1/graph/*          – Knowledge graph operations
    /metrics                 – Prometheus metrics
"""
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from loguru import logger
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST
from starlette.responses import Response

from src.api.routers import ingestion, pipeline, query, graph
from src.config import get_settings

settings = get_settings()

# ─── Prometheus Metrics ───────────────────────────────────────────────────────
REQUEST_COUNT = Counter(
    "edp_http_requests_total",
    "Total HTTP requests",
    ["method", "endpoint", "status_code"],
)
REQUEST_LATENCY = Histogram(
    "edp_http_request_duration_seconds",
    "HTTP request latency",
    ["method", "endpoint"],
    buckets=[0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0],
)


# ─── Lifespan ─────────────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup / shutdown lifecycle hooks."""
    logger.info(f"Starting EDP API [{settings.env}]")

    # Startup: warm up connections
    try:
        from src.ingestion.connectors.sql_connector import get_warehouse
        from src.knowledge_graph.neo4j_client import get_neo4j

        warehouse = get_warehouse()
        if warehouse.health_check():
            logger.info("✓ Warehouse DB connected")
        else:
            logger.warning("✗ Warehouse DB unreachable")

        neo4j = get_neo4j()
        if neo4j.health_check():
            logger.info("✓ Neo4j connected")
            neo4j.create_indexes()
        else:
            logger.warning("✗ Neo4j unreachable")

        from src.ingestion.connectors.azure_blob_connector import get_data_lake
        get_data_lake()
        logger.info("✓ Data Lake connected")

    except Exception as e:
        logger.error(f"Startup error (non-fatal): {e}")

    yield

    # Shutdown
    logger.info("Shutting down EDP API")
    try:
        from src.knowledge_graph.neo4j_client import _neo4j_client
        if _neo4j_client:
            _neo4j_client.close()
    except Exception:
        pass


# ─── App ─────────────────────────────────────────────────────────────────────
app = FastAPI(
    title="Enterprise Data Platform API",
    description="""
    KPMG-style Enterprise Data Platform.
    
    **Features:**
    - Multi-source document ingestion (PDF, Excel, Word)
    - Medallion ETL pipeline (Bronze → Silver → Gold)
    - Knowledge Graph (Neo4j / Azure Cosmos DB Gremlin)
    - RAG pipeline for document Q&A
    - Azure Data Lake Storage integration
    - Data quality validation
    """,
    version="1.0.0",
    docs_url="/docs" if not settings.is_production else None,
    redoc_url="/redoc" if not settings.is_production else None,
    lifespan=lifespan,
)

# ─── Middleware ───────────────────────────────────────────────────────────────
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"] if settings.is_development else ["https://kpmg.com.vn"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def metrics_middleware(request: Request, call_next):
    """Track request count and latency."""
    start = time.perf_counter()
    response = await call_next(request)
    duration = time.perf_counter() - start

    path = request.url.path
    REQUEST_COUNT.labels(
        method=request.method,
        endpoint=path,
        status_code=response.status_code,
    ).inc()
    REQUEST_LATENCY.labels(method=request.method, endpoint=path).observe(duration)

    response.headers["X-Request-Duration"] = f"{duration:.4f}s"
    return response


@app.middleware("http")
async def api_key_middleware(request: Request, call_next):
    """Simple API key auth for non-health, non-docs endpoints."""
    excluded = ["/health", "/docs", "/redoc", "/openapi.json", "/metrics"]
    if any(request.url.path.startswith(e) for e in excluded):
        return await call_next(request)

    api_key = request.headers.get("X-API-Key") or request.query_params.get("api_key")
    if settings.is_production and api_key != settings.api_key:
        return JSONResponse(
            status_code=status.HTTP_401_UNAUTHORIZED,
            content={"detail": "Invalid or missing API key"},
        )
    return await call_next(request)


# ─── Routers ─────────────────────────────────────────────────────────────────
app.include_router(ingestion.router, prefix="/api/v1/ingest", tags=["Ingestion"])
app.include_router(pipeline.router, prefix="/api/v1/pipeline", tags=["Pipeline"])
app.include_router(query.router, prefix="/api/v1/query", tags=["RAG Query"])
app.include_router(graph.router, prefix="/api/v1/graph", tags=["Knowledge Graph"])


# ─── Core Endpoints ───────────────────────────────────────────────────────────
@app.get("/health", tags=["Health"])
async def health_check():
    """Comprehensive health check for all services."""
    checks: dict[str, dict] = {}

    # Warehouse
    try:
        from src.ingestion.connectors.sql_connector import get_warehouse
        ok = get_warehouse().health_check()
        checks["warehouse_db"] = {"status": "healthy" if ok else "unhealthy"}
    except Exception as e:
        checks["warehouse_db"] = {"status": "unhealthy", "error": str(e)}

    # Neo4j
    try:
        from src.knowledge_graph.neo4j_client import get_neo4j
        ok = get_neo4j().health_check()
        checks["neo4j"] = {"status": "healthy" if ok else "unhealthy"}
    except Exception as e:
        checks["neo4j"] = {"status": "unhealthy", "error": str(e)}

    # Data Lake
    try:
        from src.ingestion.connectors.azure_blob_connector import get_data_lake
        dl = get_data_lake()
        checks["data_lake"] = {"status": "healthy", "type": "azure" if dl._use_azure else "minio"}
    except Exception as e:
        checks["data_lake"] = {"status": "unhealthy", "error": str(e)}

    # ChromaDB
    try:
        import chromadb
        c = chromadb.HttpClient(
            host=settings.vector_store.chroma_host,
            port=settings.vector_store.chroma_port,
        )
        c.heartbeat()
        checks["chromadb"] = {"status": "healthy"}
    except Exception as e:
        checks["chromadb"] = {"status": "unhealthy", "error": str(e)}

    overall = all(v["status"] == "healthy" for v in checks.values())
    http_status = status.HTTP_200_OK if overall else status.HTTP_503_SERVICE_UNAVAILABLE

    return JSONResponse(
        status_code=http_status,
        content={
            "status": "healthy" if overall else "degraded",
            "environment": settings.env,
            "version": "1.0.0",
            "services": checks,
        },
    )


@app.get("/metrics", tags=["Monitoring"])
async def prometheus_metrics():
    """Expose Prometheus metrics."""
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/", tags=["Root"])
async def root():
    return {
        "name": "Enterprise Data Platform",
        "version": "1.0.0",
        "docs": "/docs",
        "health": "/health",
    }
