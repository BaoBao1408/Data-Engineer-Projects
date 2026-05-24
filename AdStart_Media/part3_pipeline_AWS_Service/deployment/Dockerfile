FROM python:3.12-slim AS builder

# System settings
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONUTF8=1 \
    PIP_NO_CACHE_DIR=1

# Working directory
WORKDIR /app

# Install system deps
RUN apt-get update && apt-get install -y build-essential curl git && rm -rf /var/lib/apt/lists/*

# Copy dependency files first (better caching)
COPY requirements.txt .
COPY pyproject.toml .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt && pip install --no-cache-dir pytest pytest-cov anyio

# ─────────────────────────────────────────────────────────────────
# Stage 2: runtime — lean final image
# ─────────────────────────────────────────────────────────────────

FROM python:3.12-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONUTF8=1 \
    PYTHONPATH=/app

WORKDIR /app

# Copy installed packages from builder
COPY --from=builder /usr/local/lib/python3.12/site-packages /usr/local/lib/python3.12/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin

# Copy project
COPY . .

# Create runtime directories
RUN mkdir -p data/raw data/warehouse logs

# Healthcheck: verify DuckDB importable (fast, no I/O)
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 CMD python -c "import duckdb; print('ok')" || exit 1

# Default command
ENTRYPOINT ["python", "src/orchestration/pipeline.py"]
CMD []