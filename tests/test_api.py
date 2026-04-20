"""
Tests for FastAPI API endpoints.

Covers all API endpoints: health, pipelines, failure analysis,
sandbox testing, approve, reject, and error handling.
"""



class TestHealthEndpoint:
    """Tests for GET /health."""

    def test_health_returns_200(self, api_client):
        resp = api_client.get("/health")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "healthy"
        assert "version" in data
        assert "ai_backend" in data
        assert "ai_available" in data

    def test_health_includes_db_info(self, api_client):
        resp = api_client.get("/health")
        data = resp.json()
        assert data["database"] == "duckdb"


class TestPipelinesEndpoint:
    """Tests for GET /pipelines."""

    def test_list_pipelines(self, api_client):
        resp = api_client.get("/pipelines")
        assert resp.status_code == 200
        pipelines = resp.json()
        assert len(pipelines) >= 4
        names = [p["name"] for p in pipelines]
        assert "daily_order_ingestion" in names

    def test_pipeline_has_metadata(self, api_client):
        resp = api_client.get("/pipelines")
        pipeline = resp.json()[0]
        assert "name" in pipeline
        assert "status" in pipeline
        assert "owner" in pipeline


class TestFailureAnalysisEndpoint:
    """Tests for POST /failures/analyze."""

    def test_analyze_data_quality(self, api_client, sample_failure_request):
        resp = api_client.post("/failures/analyze", json=sample_failure_request)
        assert resp.status_code == 200
        data = resp.json()
        assert data["state"] == "awaiting_approval"
        assert data["classification"]["category"] == "data_quality"
        assert data["classification"]["confidence"] >= 0.8
        assert len(data["remediations"]) >= 2
        assert data["id"] != ""

    def test_analyze_schema_drift(self, api_client):
        resp = api_client.post("/failures/analyze", json={
            "pipeline_name": "revenue_aggregation",
            "error_message": "Column 'revenue' not found in table 'daily_sales'",
        })
        assert resp.status_code == 200
        data = resp.json()
        assert data["classification"]["category"] == "schema_drift"

    def test_analyze_sql_error(self, api_client):
        resp = api_client.post("/failures/analyze", json={
            "pipeline_name": "test_pipeline",
            "error_message": "Syntax error at or near 'FORM'",
            "pipeline_sql": "SELECT * FORM orders",
        })
        assert resp.status_code == 200
        data = resp.json()
        assert data["classification"]["category"] == "sql_error"

    def test_analyze_has_ai_analysis(self, api_client, sample_failure_request):
        resp = api_client.post("/failures/analyze", json=sample_failure_request)
        data = resp.json()
        assert data["ai_analysis"] != ""

    def test_analyze_missing_fields_422(self, api_client):
        resp = api_client.post("/failures/analyze", json={
            "pipeline_name": "test",
        })
        assert resp.status_code == 422

    def test_analyze_empty_error_422(self, api_client):
        resp = api_client.post("/failures/analyze", json={
            "pipeline_name": "test",
            "error_message": "",
        })
        assert resp.status_code == 422


class TestGetFailureEndpoint:
    """Tests for GET /failures/{id}."""

    def test_get_existing_failure(self, api_client, sample_failure_request):
        # Create a failure first
        create_resp = api_client.post("/failures/analyze", json=sample_failure_request)
        failure_id = create_resp.json()["id"]

        resp = api_client.get(f"/failures/{failure_id}")
        assert resp.status_code == 200
        assert resp.json()["id"] == failure_id

    def test_get_nonexistent_failure_404(self, api_client):
        resp = api_client.get("/failures/nonexistent")
        assert resp.status_code == 404


class TestSandboxEndpoint:
    """Tests for POST /failures/{id}/sandbox-test."""

    def test_sandbox_test_success(self, api_client, sample_failure_request):
        create_resp = api_client.post("/failures/analyze", json=sample_failure_request)
        failure_id = create_resp.json()["id"]

        resp = api_client.post(f"/failures/{failure_id}/sandbox-test?remediation_index=0")
        assert resp.status_code == 200
        data = resp.json()
        assert "success" in data
        assert "validation_checks" in data

    def test_sandbox_nonexistent_failure(self, api_client):
        resp = api_client.post("/failures/nonexistent/sandbox-test")
        assert resp.status_code == 404

    def test_sandbox_invalid_index(self, api_client, sample_failure_request):
        create_resp = api_client.post("/failures/analyze", json=sample_failure_request)
        failure_id = create_resp.json()["id"]

        resp = api_client.post(f"/failures/{failure_id}/sandbox-test?remediation_index=99")
        assert resp.status_code == 400


class TestApproveEndpoint:
    """Tests for POST /failures/{id}/approve."""

    def test_approve_success(self, api_client, sample_failure_request):
        create_resp = api_client.post("/failures/analyze", json=sample_failure_request)
        failure_id = create_resp.json()["id"]

        resp = api_client.post(f"/failures/{failure_id}/approve", json={
            "remediation_index": 0,
            "approver": "admin",
        })
        assert resp.status_code == 200
        data = resp.json()
        assert data["state"] == "applied"
        assert data["approver"] == "admin"

    def test_approve_nonexistent_404(self, api_client):
        resp = api_client.post("/failures/nonexistent/approve")
        assert resp.status_code == 404

    def test_cannot_approve_already_applied(self, api_client, sample_failure_request):
        create_resp = api_client.post("/failures/analyze", json=sample_failure_request)
        failure_id = create_resp.json()["id"]

        # Approve first time
        api_client.post(f"/failures/{failure_id}/approve")

        # Try to approve again
        resp = api_client.post(f"/failures/{failure_id}/approve")
        assert resp.status_code == 400


class TestRejectEndpoint:
    """Tests for POST /failures/{id}/reject."""

    def test_reject_success(self, api_client, sample_failure_request):
        create_resp = api_client.post("/failures/analyze", json=sample_failure_request)
        failure_id = create_resp.json()["id"]

        resp = api_client.post(f"/failures/{failure_id}/reject")
        assert resp.status_code == 200
        assert resp.json()["state"] == "rejected"

    def test_reject_nonexistent_404(self, api_client):
        resp = api_client.post("/failures/nonexistent/reject")
        assert resp.status_code == 404

    def test_cannot_reject_applied(self, api_client, sample_failure_request):
        create_resp = api_client.post("/failures/analyze", json=sample_failure_request)
        failure_id = create_resp.json()["id"]

        # Approve (transitions to applied)
        api_client.post(f"/failures/{failure_id}/approve")

        # Try to reject
        resp = api_client.post(f"/failures/{failure_id}/reject")
        assert resp.status_code == 400


class TestWorkflowIntegration:
    """Integration tests for the full workflow."""

    def test_full_workflow_approve(self, api_client, sample_failure_request):
        """Test the full happy path: analyze → sandbox → approve."""
        # Step 1: Analyze
        resp = api_client.post("/failures/analyze", json=sample_failure_request)
        assert resp.status_code == 200
        failure_id = resp.json()["id"]
        assert resp.json()["state"] == "awaiting_approval"

        # Step 2: Sandbox test
        resp = api_client.post(f"/failures/{failure_id}/sandbox-test?remediation_index=0")
        assert resp.status_code == 200

        # Step 3: Approve
        resp = api_client.post(f"/failures/{failure_id}/approve", json={
            "remediation_index": 0,
            "approver": "sre-lead",
        })
        assert resp.status_code == 200
        assert resp.json()["state"] == "applied"

    def test_full_workflow_reject(self, api_client, sample_failure_request):
        """Test the reject path: analyze → reject."""
        resp = api_client.post("/failures/analyze", json=sample_failure_request)
        failure_id = resp.json()["id"]

        resp = api_client.post(f"/failures/{failure_id}/reject")
        assert resp.status_code == 200
        assert resp.json()["state"] == "rejected"
