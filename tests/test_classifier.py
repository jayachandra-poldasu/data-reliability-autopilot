"""
Tests for the Failure Classifier module.

Covers all 7 failure categories with positive and negative pattern matching,
confidence scoring, multi-pattern boosting, and edge cases.
"""

from app.classifier import classify_failure, get_severity_for_category, FAILURE_PATTERNS
from app.models import FailureCategory, Severity


class TestSchemaClassification:
    """Tests for schema drift classification."""

    def test_missing_column(self, sample_error_messages):
        result = classify_failure(sample_error_messages["schema_drift_missing_col"])
        assert result.category == FailureCategory.SCHEMA_DRIFT
        assert result.confidence >= 0.85
        assert "missing_column" in result.matched_patterns

    def test_type_mismatch(self, sample_error_messages):
        result = classify_failure(sample_error_messages["schema_drift_type_mismatch"])
        assert result.category == FailureCategory.SCHEMA_DRIFT
        assert result.confidence >= 0.90
        assert "type_mismatch" in result.matched_patterns

    def test_explicit_schema_drift(self, sample_error_messages):
        result = classify_failure(sample_error_messages["schema_drift_explicit"])
        assert result.category == FailureCategory.SCHEMA_DRIFT
        assert result.confidence >= 0.90

    def test_missing_table(self):
        result = classify_failure("Table 'staging_orders' does not exist")
        assert result.category == FailureCategory.SCHEMA_DRIFT
        assert "missing_table" in result.matched_patterns

    def test_column_modification(self):
        result = classify_failure("Added column 'email' to source table")
        assert result.category == FailureCategory.SCHEMA_DRIFT
        assert "column_modification" in result.matched_patterns


class TestDataQualityClassification:
    """Tests for data quality classification."""

    def test_null_violation(self, sample_error_messages):
        result = classify_failure(sample_error_messages["data_quality_null"])
        assert result.category == FailureCategory.DATA_QUALITY
        assert "null_violation" in result.matched_patterns

    def test_duplicate_key(self, sample_error_messages):
        result = classify_failure(sample_error_messages["data_quality_duplicate"])
        assert result.category == FailureCategory.DATA_QUALITY
        assert "duplicate_key" in result.matched_patterns

    def test_type_conversion(self, sample_error_messages):
        result = classify_failure(sample_error_messages["data_quality_type_conv"])
        assert result.category == FailureCategory.DATA_QUALITY
        assert "type_conversion_failure" in result.matched_patterns
        assert result.confidence >= 0.90

    def test_invalid_data(self, sample_error_messages):
        result = classify_failure(sample_error_messages["data_quality_invalid"])
        assert result.category == FailureCategory.DATA_QUALITY
        assert "invalid_data" in result.matched_patterns

    def test_value_out_of_range(self, sample_error_messages):
        result = classify_failure(sample_error_messages["data_quality_range"])
        assert result.category == FailureCategory.DATA_QUALITY
        assert "value_out_of_range" in result.matched_patterns

    def test_quality_check_failure(self):
        result = classify_failure("Data quality check failed for table orders")
        assert result.category == FailureCategory.DATA_QUALITY
        assert "quality_check_failure" in result.matched_patterns


class TestSQLErrorClassification:
    """Tests for SQL error classification."""

    def test_syntax_error(self, sample_error_messages):
        result = classify_failure(sample_error_messages["sql_syntax"])
        assert result.category == FailureCategory.SQL_ERROR
        assert "sql_syntax_error" in result.matched_patterns

    def test_parse_error(self, sample_error_messages):
        result = classify_failure(sample_error_messages["sql_parse"])
        assert result.category == FailureCategory.SQL_ERROR
        assert "sql_parse_error" in result.matched_patterns

    def test_division_by_zero(self, sample_error_messages):
        result = classify_failure(sample_error_messages["sql_division"])
        assert result.category == FailureCategory.SQL_ERROR
        assert "division_by_zero" in result.matched_patterns

    def test_unknown_function(self):
        result = classify_failure("Function 'CUSTOM_AGG' not found")
        assert result.category == FailureCategory.SQL_ERROR
        assert "unknown_function" in result.matched_patterns


class TestTimeoutClassification:
    """Tests for timeout classification."""

    def test_basic_timeout(self, sample_error_messages):
        result = classify_failure(sample_error_messages["timeout"])
        assert result.category == FailureCategory.TIMEOUT
        assert result.confidence >= 0.90

    def test_deadline_exceeded(self, sample_error_messages):
        result = classify_failure(sample_error_messages["timeout_deadline"])
        assert result.category == FailureCategory.TIMEOUT
        assert "deadline_exceeded" in result.matched_patterns

    def test_query_cancelled_timeout(self):
        result = classify_failure("Query cancelled due to timeout after 120s")
        assert result.category == FailureCategory.TIMEOUT


class TestDependencyClassification:
    """Tests for dependency failure classification."""

    def test_connection_refused(self, sample_error_messages):
        result = classify_failure(sample_error_messages["dependency_conn"])
        assert result.category == FailureCategory.DEPENDENCY_FAILURE
        assert "connection_failure" in result.matched_patterns

    def test_upstream_failure(self, sample_error_messages):
        result = classify_failure(sample_error_messages["dependency_upstream"])
        assert result.category == FailureCategory.DEPENDENCY_FAILURE

    def test_source_not_found(self, sample_error_messages):
        result = classify_failure(sample_error_messages["dependency_source"])
        assert result.category == FailureCategory.DEPENDENCY_FAILURE

    def test_service_unreachable(self):
        result = classify_failure("Service 'auth-api' unreachable")
        assert result.category == FailureCategory.DEPENDENCY_FAILURE


class TestResourceExhaustionClassification:
    """Tests for resource exhaustion classification."""

    def test_out_of_memory(self, sample_error_messages):
        result = classify_failure(sample_error_messages["resource_oom"])
        assert result.category == FailureCategory.RESOURCE_EXHAUSTION
        assert "out_of_memory" in result.matched_patterns

    def test_disk_full(self, sample_error_messages):
        result = classify_failure(sample_error_messages["resource_disk"])
        assert result.category == FailureCategory.RESOURCE_EXHAUSTION
        assert "disk_full" in result.matched_patterns


class TestPermissionClassification:
    """Tests for permission error classification."""

    def test_permission_denied(self, sample_error_messages):
        result = classify_failure(sample_error_messages["permission_denied"])
        assert result.category == FailureCategory.PERMISSION_ERROR
        assert "permission_denied" in result.matched_patterns

    def test_forbidden(self, sample_error_messages):
        result = classify_failure(sample_error_messages["permission_forbidden"])
        assert result.category == FailureCategory.PERMISSION_ERROR


class TestEdgeCases:
    """Tests for edge cases and special scenarios."""

    def test_unknown_error(self, sample_error_messages):
        result = classify_failure(sample_error_messages["unknown"])
        assert result.category == FailureCategory.UNKNOWN
        assert result.confidence <= 0.5

    def test_empty_error(self):
        result = classify_failure("")
        assert result.category == FailureCategory.UNKNOWN
        assert result.confidence == 0.0

    def test_combined_context(self):
        result = classify_failure(
            error_message="Pipeline failed",
            error_context="Column 'revenue' not found",
            pipeline_sql="SELECT revenue FROM sales",
        )
        assert result.category == FailureCategory.SCHEMA_DRIFT

    def test_multi_pattern_confidence_boost(self):
        # Error that matches multiple patterns in same category
        result = classify_failure(
            "Null constraint violation on id AND duplicate key entry found"
        )
        assert result.category == FailureCategory.DATA_QUALITY
        assert result.confidence > 0.90

    def test_reasoning_contains_info(self):
        result = classify_failure("Column 'revenue' not found")
        assert "Schema" in result.reasoning or "schema" in result.reasoning
        assert len(result.reasoning) > 20

    def test_severity_mapping(self):
        assert get_severity_for_category(FailureCategory.RESOURCE_EXHAUSTION) == Severity.CRITICAL
        assert get_severity_for_category(FailureCategory.SCHEMA_DRIFT) == Severity.HIGH
        assert get_severity_for_category(FailureCategory.UNKNOWN) == Severity.MEDIUM

    def test_all_categories_have_patterns(self):
        """Ensure every category except UNKNOWN has at least one pattern."""
        categories_with_patterns = set()
        for _, category, _, _, _ in FAILURE_PATTERNS:
            categories_with_patterns.add(category)
        for cat in FailureCategory:
            if cat != FailureCategory.UNKNOWN:
                assert cat in categories_with_patterns, f"No patterns for {cat}"
