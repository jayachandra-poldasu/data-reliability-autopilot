"""
Tests for the Remediation Engine module.

Covers remediation proposals for all failure categories, SQL fix generation,
quarantine SQL, schema migration SQL, and edge cases.
"""

from app.remediation import propose_remediations
from app.models import (
    ClassificationResult,
    FailureCategory,
    RemediationAction,
    Severity,
)


class TestRemediationProposals:
    """Tests for remediation proposal generation."""

    def test_schema_drift_remediations(self):
        classification = ClassificationResult(
            category=FailureCategory.SCHEMA_DRIFT,
            confidence=0.92,
            reasoning="Schema drift detected",
            matched_patterns=["missing_column"],
        )
        proposals = propose_remediations(classification)
        assert len(proposals) >= 3
        assert proposals[0].rank == 1
        assert proposals[0].action == RemediationAction.APPLY_SCHEMA_MIGRATION

    def test_data_quality_remediations(self):
        classification = ClassificationResult(
            category=FailureCategory.DATA_QUALITY,
            confidence=0.90,
            reasoning="Data quality issue",
            matched_patterns=["null_violation"],
        )
        proposals = propose_remediations(classification)
        assert len(proposals) >= 3
        assert proposals[0].action == RemediationAction.QUARANTINE_BAD_ROWS

    def test_sql_error_remediations(self):
        classification = ClassificationResult(
            category=FailureCategory.SQL_ERROR,
            confidence=0.95,
            reasoning="SQL error",
            matched_patterns=["sql_syntax_error"],
        )
        proposals = propose_remediations(classification)
        assert len(proposals) >= 2
        assert proposals[0].action == RemediationAction.FIX_SQL

    def test_timeout_remediations(self):
        classification = ClassificationResult(
            category=FailureCategory.TIMEOUT,
            confidence=0.93,
            reasoning="Timeout",
            matched_patterns=["execution_timeout"],
        )
        proposals = propose_remediations(classification)
        assert proposals[0].action == RemediationAction.RETRY

    def test_dependency_remediations(self):
        classification = ClassificationResult(
            category=FailureCategory.DEPENDENCY_FAILURE,
            confidence=0.90,
            reasoning="Dependency failed",
            matched_patterns=["connection_failure"],
        )
        proposals = propose_remediations(classification)
        assert proposals[0].action == RemediationAction.RETRY

    def test_resource_exhaustion_remediations(self):
        classification = ClassificationResult(
            category=FailureCategory.RESOURCE_EXHAUSTION,
            confidence=0.93,
            reasoning="OOM",
            matched_patterns=["out_of_memory"],
        )
        proposals = propose_remediations(classification)
        assert proposals[0].action == RemediationAction.INCREASE_RESOURCES

    def test_permission_error_remediations(self):
        classification = ClassificationResult(
            category=FailureCategory.PERMISSION_ERROR,
            confidence=0.94,
            reasoning="Permission denied",
            matched_patterns=["permission_denied"],
        )
        proposals = propose_remediations(classification)
        assert proposals[0].action == RemediationAction.FIX_PERMISSIONS

    def test_unknown_remediations(self):
        classification = ClassificationResult(
            category=FailureCategory.UNKNOWN,
            confidence=0.3,
            reasoning="Unknown",
            matched_patterns=[],
        )
        proposals = propose_remediations(classification)
        assert len(proposals) >= 2
        assert proposals[0].action == RemediationAction.RETRY


class TestRemediationRanking:
    """Tests for remediation ranking and metadata."""

    def test_ranks_are_sequential(self):
        classification = ClassificationResult(
            category=FailureCategory.DATA_QUALITY,
            confidence=0.90,
            reasoning="Test",
            matched_patterns=["null_violation"],
        )
        proposals = propose_remediations(classification)
        for i, p in enumerate(proposals):
            assert p.rank == i + 1

    def test_all_proposals_have_descriptions(self):
        for category in FailureCategory:
            classification = ClassificationResult(
                category=category,
                confidence=0.90,
                reasoning="Test",
                matched_patterns=[],
            )
            proposals = propose_remediations(classification)
            for p in proposals:
                assert len(p.description) > 10
                assert len(p.estimated_impact) > 5

    def test_risk_levels_are_valid(self):
        classification = ClassificationResult(
            category=FailureCategory.SCHEMA_DRIFT,
            confidence=0.90,
            reasoning="Test",
            matched_patterns=["missing_column"],
        )
        proposals = propose_remediations(classification)
        for p in proposals:
            assert p.risk_level in Severity


class TestSQLFixGeneration:
    """Tests for SQL fix generation in remediations."""

    def test_type_conversion_fix(self):
        classification = ClassificationResult(
            category=FailureCategory.DATA_QUALITY,
            confidence=0.93,
            reasoning="Type conversion",
            matched_patterns=["type_conversion_failure"],
        )
        proposals = propose_remediations(
            classification,
            pipeline_sql="SELECT * FROM source_table",
        )
        # FIX_SQL proposal should have SQL
        fix_proposals = [p for p in proposals if p.action == RemediationAction.FIX_SQL]
        assert len(fix_proposals) > 0
        assert "TRY_CAST" in fix_proposals[0].sql_fix

    def test_null_violation_fix(self):
        classification = ClassificationResult(
            category=FailureCategory.DATA_QUALITY,
            confidence=0.90,
            reasoning="Null violation",
            matched_patterns=["null_violation"],
        )
        proposals = propose_remediations(
            classification,
            pipeline_sql="SELECT * FROM source_table",
        )
        fix_proposals = [p for p in proposals if p.action == RemediationAction.FIX_SQL]
        if fix_proposals:
            assert "NULL" in fix_proposals[0].sql_fix.upper()

    def test_duplicate_key_fix(self):
        classification = ClassificationResult(
            category=FailureCategory.DATA_QUALITY,
            confidence=0.92,
            reasoning="Duplicate key",
            matched_patterns=["duplicate_key"],
        )
        proposals = propose_remediations(
            classification,
            pipeline_sql="SELECT * FROM source_table",
        )
        fix_proposals = [p for p in proposals if p.action == RemediationAction.FIX_SQL]
        if fix_proposals:
            assert "ROW_NUMBER" in fix_proposals[0].sql_fix or "DISTINCT" in fix_proposals[0].sql_fix

    def test_no_sql_fix_without_pipeline_sql(self):
        classification = ClassificationResult(
            category=FailureCategory.SQL_ERROR,
            confidence=0.95,
            reasoning="Syntax error",
            matched_patterns=["sql_syntax_error"],
        )
        proposals = propose_remediations(classification, pipeline_sql="")
        fix_proposals = [p for p in proposals if p.action == RemediationAction.FIX_SQL]
        # Without pipeline SQL, fix_sql should be empty
        assert fix_proposals[0].sql_fix == ""

    def test_quarantine_sql_generation(self):
        classification = ClassificationResult(
            category=FailureCategory.DATA_QUALITY,
            confidence=0.90,
            reasoning="Type conversion",
            matched_patterns=["type_conversion_failure"],
        )
        proposals = propose_remediations(
            classification,
            pipeline_sql="SELECT * FROM source_table",
        )
        quarantine = [p for p in proposals if p.action == RemediationAction.QUARANTINE_BAD_ROWS]
        assert len(quarantine) > 0
        assert "quarantine" in quarantine[0].sql_fix.lower() or "TRY_CAST" in quarantine[0].sql_fix

    def test_schema_migration_sql(self):
        classification = ClassificationResult(
            category=FailureCategory.SCHEMA_DRIFT,
            confidence=0.92,
            reasoning="Missing column",
            matched_patterns=["missing_column"],
        )
        proposals = propose_remediations(classification)
        migration = [p for p in proposals if p.action == RemediationAction.APPLY_SCHEMA_MIGRATION]
        assert len(migration) > 0
        assert "ALTER TABLE" in migration[0].sql_fix or "column" in migration[0].sql_fix.lower()

    def test_division_by_zero_fix(self):
        classification = ClassificationResult(
            category=FailureCategory.SQL_ERROR,
            confidence=0.92,
            reasoning="Division by zero",
            matched_patterns=["division_by_zero"],
        )
        proposals = propose_remediations(
            classification,
            pipeline_sql="SELECT a/b FROM t",
        )
        fix_proposals = [p for p in proposals if p.action == RemediationAction.FIX_SQL]
        assert len(fix_proposals) > 0
        assert "denominator" in fix_proposals[0].sql_fix.lower() or "0" in fix_proposals[0].sql_fix
