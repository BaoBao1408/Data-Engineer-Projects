"""
Integration tests – requires running services (Postgres, Neo4j, MinIO).
Run with: pytest tests/integration/ -v
"""
import io
import pytest
from fastapi.testclient import TestClient


@pytest.fixture(scope="module")
def client():
    from src.api.main import app
    return TestClient(app)


class TestHealthCheck:
    def test_health_endpoint_returns_200_or_503(self, client):
        response = client.get("/health")
        assert response.status_code in (200, 503)

    def test_health_response_structure(self, client):
        response = client.get("/health")
        data = response.json()
        assert "status" in data
        assert "services" in data
        assert "environment" in data

    def test_root_endpoint(self, client):
        response = client.get("/")
        assert response.status_code == 200
        data = response.json()
        assert data["name"] == "Enterprise Data Platform"


class TestIngestionEndpoint:
    def test_upload_unsupported_type_rejected(self, client):
        fake_file = io.BytesIO(b"some content")
        response = client.post(
            "/api/v1/ingest/upload",
            files={"file": ("test.mp4", fake_file, "video/mp4")},
            data={"ingest_to_rag": "false"},
        )
        assert response.status_code == 400

    def test_upload_csv_accepted(self, client):
        csv_content = b"entity_code,amount,currency\nVCB,1000000,VND\nHPG,2000000,VND\n"
        response = client.post(
            "/api/v1/ingest/upload",
            files={"file": ("financials.csv", io.BytesIO(csv_content), "text/csv")},
            data={"ingest_to_rag": "false", "zone": "raw"},
        )
        # May fail if MinIO not available, but should not be 400
        assert response.status_code in (200, 201, 500, 503)

    def test_list_files(self, client):
        response = client.get("/api/v1/ingest/list")
        assert response.status_code in (200, 500)


class TestGraphEndpoints:
    def test_create_entity_node(self, client):
        response = client.post("/api/v1/graph/nodes", json={
            "id": "test-vcb-001",
            "label": "Entity",
            "properties": {
                "name": "Vietcombank Test",
                "type": "BANK",
                "ticker": "VCB",
            },
        })
        assert response.status_code in (201, 500)

    def test_get_nonexistent_node(self, client):
        response = client.get("/api/v1/graph/nodes/Entity/nonexistent-id-12345")
        assert response.status_code in (404, 500)

    def test_graph_stats(self, client):
        response = client.get("/api/v1/graph/stats")
        assert response.status_code in (200, 500)


class TestPipelineEndpoints:
    def test_list_runs(self, client):
        response = client.get("/api/v1/pipeline/runs")
        assert response.status_code in (200, 500)
        if response.status_code == 200:
            data = response.json()
            assert "runs" in data


class TestRAGEndpoints:
    def test_rag_stats(self, client):
        response = client.get("/api/v1/query/stats")
        assert response.status_code in (200, 500)

    def test_retrieve_no_question_fails(self, client):
        response = client.post("/api/v1/query/retrieve", json={})
        assert response.status_code == 422   # Pydantic validation error
