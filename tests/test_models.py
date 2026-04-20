"""
Tests for Pydantic Models.

Covers model validation, enum values, default factories,
constraint bounds, and serialization for all request/response schemas.
"""

import pytest
from pydantic import ValidationError

from app.models import (
    ClassificationResult,
    FailureAnalysisRequest,
    FailureCategory,
    FailureRecord,
    HealthResponse,
    PipelineInfo,
    PipelineStatus,
    RemediationAction,
    RemediationProposal,
    SandboxResult,
    Severity,
    WorkflowState,
)


class TestFailureCategory:
    """Tests for FailureCategory enum."""

    def test_all_categories_exist(self):
        expected = [
            "schema_drift", "data_quality", "sql_error", "timeout",
            "dependency_failure", "resource_exhaustion", "permission_error", "unknown",
        ]
        for cat in expected:
            assert cat in [c.value for c in FailureCategory]

    def test_category_count(self):
        assert len(FailureCategory) == 8


class TestSeverity:
    """Tests for Severity enum."""

    def test_all_levels(self):
        levels = [s.value for s in Severity]
        assert "critical" in levels
        assert "high" in levels
        assert "medium" in levels
        assert "low" in levels
        assert "info" in levels


class TestWorkflowState:
    """Tests for WorkflowState enum."""

    def test_all_states(self):
        states = [s.value for s in WorkflowState]
        expected = [
            "pending", "analyzing", "awaiting_approval",
            "sandbox_testing", "approved", "applied",
            "rejected", "rolled_back",
        ]
        for s in expected:
            assert s in states

    def test_state_count(self):
        assert len(WorkflowState) == 8


class TestFailureAnalysisRequest:
    """Tests for FailureAnalysisRequest validation."""

    def test_valid_request(self):
        req = FailureAnalysisRequest(
            pipeline_name="daily_orders",
            error_message="Column not found",
        )
        assert req.pipeline_name == "daily_orders"
        assert req.error_context == ""
        assert req.pipeline_sql == ""

    def test_full_request(self):
        req = FailureAnalysisRequest(
            pipeline_name="revenue_agg",
            error_message="Type mismatch",
            error_context="Row 42 in batch 7",
            pipeline_sql="SELECT * FROM t",
        )
        assert req.pipeline_sql == "SELECT * FROM t"

    def test_empty_error_message_rejected(self):
        with pytest.raises(ValidationError):
            FailureAnalysisRequest(
                pipeline_name="test",
                error_message="",
            )

    def test_missing_pipeline_name_rejected(self):
        with pytest.raises(ValidationError):
            FailureAnalysisRequest(
                error_message="Some error",
            )


class TestClassificationResult:
    """Tests for ClassificationResult validation."""

    def test_valid_classification(self):
        result = ClassificationResult(
            category=FailureCategory.SCHEMA_DRIFT,
            confidence=0.92,
            reasoning="Schema drift detected",
            matched_patterns=["missing_column"],
        )
        assert result.confidence == 0.92

    def test_confidence_bounds(self):
        with pytest.raises(ValidationError):
            ClassificationResult(
                category=FailureCategory.UNKNOWN,
                confidence=1.5,
            )

    def test_confidence_lower_bound(self):
        with pytest.raises(ValidationError):
            ClassificationResult(
                category=FailureCategory.UNKNOWN,
                confidence=-0.1,
            )

    def test_default_patterns(self):
        result = ClassificationResult(
            category=FailureCategory.UNKNOWN,
            confidence=0.5,
        )
        assert result.matched_patterns == []
        assert result.reasoning == ""


class TestRemediationProposal:
    """Tests for RemediationProposal validation."""

    def test_valid_proposal(self):
        p = RemediationProposal(
            rank=1,
            action=RemediationAction.RETRY,
            description="Retry the pipeline",
        )
        assert p.rank == 1
        assert p.auto_executable is False
        assert p.sql_fix == ""

    def test_rank_must_be_positive(self):
        with pytest.raises(ValidationError):
            RemediationProposal(
                rank=0,
                action=RemediationAction.RETRY,
                description="Test",
            )

    def test_all_actions_valid(self):
        actions = [a.value for a in RemediationAction]
        expected = [
            "retry", "quarantine_bad_rows", "rollback",
            "apply_schema_migration", "skip_and_alert",
            "fix_sql", "increase_resources", "fix_permissions",
        ]
        for a in expected:
            assert a in actions


class TestSandboxResult:
    """Tests for SandboxResult validation."""

    def test_success_result(self):
        r = SandboxResult(
            success=True,
            rows_affected=10,
            execution_time_ms=5.2,
            preview_data=[{"id": 1}],
            validation_checks=["safety: PASSED"],
        )
        assert r.rows_quarantined == 0
        assert r.error_message == ""

    def test_failure_result(self):
        r = SandboxResult(
            success=False,
            error_message="Syntax error",
            validation_checks=["execution: FAILED"],
        )
        assert r.rows_affected == 0


class TestFailureRecord:
    """Tests for FailureRecord validation."""

    def test_default_state(self):
        r = FailureRecord(
            id="abc123",
            pipeline_name="test_pipeline",
            error_message="Error",
        )
        assert r.state == WorkflowState.PENDING
        assert r.classification is None
        assert r.remediations == []
        assert r.created_at != ""

    def test_full_record(self):
        r = FailureRecord(
            id="xyz789",
            pipeline_name="revenue_agg",
            error_message="Timeout",
            state=WorkflowState.APPLIED,
            approver="admin",
        )
        assert r.state == WorkflowState.APPLIED
        assert r.approver == "admin"


class TestHealthResponse:
    """Tests for HealthResponse validation."""

    def test_health_response(self):
        h = HealthResponse(
            version="1.0.0",
            ai_backend="none",
            ai_available=True,
            active_failures=3,
        )
        assert h.status == "healthy"
        assert h.database == "duckdb"


class TestPipelineInfo:
    """Tests for PipelineInfo validation."""

    def test_pipeline_info(self):
        p = PipelineInfo(
            name="daily_orders",
            status=PipelineStatus.HEALTHY,
            owner="Data Engineering",
            tables=["orders"],
        )
        assert p.schedule == ""
        assert p.description == ""

    def test_all_statuses(self):
        for status in PipelineStatus:
            p = PipelineInfo(name="test", status=status)
            assert p.status == status
