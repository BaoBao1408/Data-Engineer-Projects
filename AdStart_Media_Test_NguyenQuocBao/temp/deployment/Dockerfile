FROM python:3.12.9

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV PIPELINE_ENV=prod
ENV PYTHONPATH=/app

# Default: run yesterday's pipeline
CMD ["python", "-m", "src.orchestration.pipeline"]
