"""Pytest configuration and shared fixtures."""
import os
import pytest

# Force test environment before any imports
os.environ.setdefault("ENV", "testing")
os.environ.setdefault("DATABASE_URL", "sqlite:///./test.db")
os.environ.setdefault("NEO4J_URI", "bolt://localhost:7687")
os.environ.setdefault("NEO4J_PASSWORD", "neo4j_pass")
os.environ.setdefault("MINIO_ENDPOINT", "localhost:9000")
os.environ.setdefault("MINIO_ACCESS_KEY", "minio_admin")
os.environ.setdefault("MINIO_SECRET_KEY", "minio_secret")
os.environ.setdefault("MINIO_SECURE", "false")
os.environ.setdefault("SECRET_KEY", "test-secret")
os.environ.setdefault("API_KEY", "test-api-key")


@pytest.fixture(scope="session")
def sample_financial_df():
    import pandas as pd
    return pd.DataFrame({
        "entity_code":       ["VCB", "HPG", "FPT", "MBB"],
        "fiscal_year":       [2024, 2024, 2024, 2024],
        "total_revenue":     [100_000_000_000, 130_000_000_000, 52_000_000_000, 65_000_000_000],
        "net_income":        [ 20_000_000_000,   8_000_000_000,  6_500_000_000, 14_000_000_000],
        "total_assets":      [1_800_000_000_000, 120_000_000_000, 40_000_000_000, 700_000_000_000],
        "total_equity":      [  150_000_000_000,  60_000_000_000, 18_000_000_000,  70_000_000_000],
        "currency":          ["VND", "VND", "VND", "VND"],
    })


@pytest.fixture(scope="session")
def sample_pdf_bytes():
    """Minimal valid PDF bytes for testing extractors."""
    return (
        b"%PDF-1.4\n1 0 obj<</Type/Catalog/Pages 2 0 R>>endobj\n"
        b"2 0 obj<</Type/Pages/Kids[3 0 R]/Count 1>>endobj\n"
        b"3 0 obj<</Type/Page/MediaBox[0 0 612 792]/Parent 2 0 R"
        b"/Contents 4 0 R>>endobj\n"
        b"4 0 obj<</Length 44>>stream\n"
        b"BT /F1 12 Tf 100 700 Td (Test financial report) Tj ET\n"
        b"endstream\nendobj\nxref\n0 5\ntrailer<</Size 5/Root 1 0 R>>\n"
        b"startxref\n0\n%%EOF"
    )


@pytest.fixture
def mock_warehouse(mocker):
    """Mock warehouse connector for unit tests."""
    mock = mocker.MagicMock()
    mock.health_check.return_value = True
    mock.execute.return_value = []
    mock.execute_many.return_value = 0
    mock.bulk_insert_df.return_value = 0
    return mock


@pytest.fixture
def mock_data_lake(mocker):
    """Mock data lake connector."""
    mock = mocker.MagicMock()
    mock._use_azure = False
    mock.upload_bytes.return_value = "s3://raw/test/file.parquet"
    mock.download_bytes.return_value = b""
    mock.list_blobs.return_value = iter([])
    return mock


@pytest.fixture
def mock_neo4j(mocker):
    """Mock Neo4j client."""
    mock = mocker.MagicMock()
    mock.health_check.return_value = True
    mock.upsert_node.return_value = {"id": "test-node"}
    mock.upsert_nodes_batch.return_value = 1
    mock.upsert_relationships_batch.return_value = 1
    return mock
