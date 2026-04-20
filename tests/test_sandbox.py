"""
Tests for the Sandbox Validator module.

Covers sandbox execution, safety checks, SQL validation,
sample data loading, quarantine detection, and error handling.
"""

from app.sandbox import run_sandbox_test, validate_sql_safety


class TestSQLSafety:
    """Tests for SQL safety validation."""

    def test_safe_select(self):
        is_safe, reason = validate_sql_safety("SELECT * FROM source_table")
        assert is_safe is True

    def test_safe_try_cast(self):
        is_safe, reason = validate_sql_safety(
            "SELECT id, TRY_CAST(amount AS INTEGER) FROM source_table"
        )
        assert is_safe is True

    def test_empty_sql(self):
        is_safe, reason = validate_sql_safety("")
        assert is_safe is False
        assert "Empty" in reason

    def test_whitespace_only(self):
        is_safe, reason = validate_sql_safety("   ")
        assert is_safe is False

    def test_forbidden_drop_database(self):
        is_safe, reason = validate_sql_safety("DROP DATABASE production")
        assert is_safe is False
        assert "DROP DATABASE" in reason

    def test_forbidden_attach(self):
        is_safe, reason = validate_sql_safety("ATTACH '/etc/passwd' AS secrets")
        assert is_safe is False

    def test_forbidden_copy(self):
        is_safe, reason = validate_sql_safety("COPY TO '/tmp/data.csv'")
        assert is_safe is False

    def test_forbidden_install(self):
        is_safe, reason = validate_sql_safety("INSTALL httpfs")
        assert is_safe is False


class TestSandboxExecution:
    """Tests for sandbox SQL execution."""

    def test_basic_select(self):
        result = run_sandbox_test("SELECT * FROM source_table")
        assert result.success is True
        assert result.rows_affected == 10
        assert len(result.preview_data) > 0

    def test_try_cast_query(self):
        result = run_sandbox_test(
            "SELECT id, name, TRY_CAST(amount AS INTEGER) AS amount, "
            "CASE WHEN TRY_CAST(amount AS INTEGER) IS NULL THEN 'quarantined' "
            "ELSE 'valid' END AS _row_status "
            "FROM source_table"
        )
        assert result.success is True
        assert result.rows_affected == 10
        assert result.rows_quarantined > 0  # some rows have non-numeric amounts

    def test_filter_query(self):
        result = run_sandbox_test(
            "SELECT * FROM source_table WHERE TRY_CAST(amount AS INTEGER) IS NOT NULL"
        )
        assert result.success is True
        assert result.rows_affected < 10  # some rows filtered out

    def test_aggregation_query(self):
        result = run_sandbox_test(
            "SELECT COUNT(*) as cnt, "
            "SUM(CASE WHEN TRY_CAST(amount AS INTEGER) IS NULL THEN 1 ELSE 0 END) as bad_rows "
            "FROM source_table"
        )
        assert result.success is True
        assert result.rows_affected == 1

    def test_invalid_sql(self):
        result = run_sandbox_test("SELECT * FORM source_table")
        assert result.success is False
        assert result.error_message != ""

    def test_nonexistent_table(self):
        result = run_sandbox_test("SELECT * FROM nonexistent_table")
        assert result.success is False
        assert "error" in result.error_message.lower() or "failed" in result.error_message.lower()


class TestSandboxWithCustomData:
    """Tests for sandbox with custom sample data."""

    def test_custom_data(self):
        data = [
            {"id": "1", "name": "Test", "value": "100"},
            {"id": "2", "name": "Bad", "value": "invalid"},
        ]
        result = run_sandbox_test(
            "SELECT * FROM source_table",
            sample_data=data,
        )
        assert result.success is True
        assert result.rows_affected == 2

    def test_custom_setup_sql(self):
        setup = (
            "CREATE TABLE source_table (id INTEGER, val VARCHAR);"
            "INSERT INTO source_table VALUES (1, 'hello');"
            "INSERT INTO source_table VALUES (2, 'world')"
        )
        result = run_sandbox_test(
            "SELECT * FROM source_table",
            setup_sql=setup,
        )
        assert result.success is True
        assert result.rows_affected == 2


class TestSandboxValidation:
    """Tests for sandbox validation checks."""

    def test_validation_checks_present(self):
        result = run_sandbox_test("SELECT * FROM source_table")
        assert len(result.validation_checks) > 0
        assert any("safety_check" in c for c in result.validation_checks)
        assert any("execution" in c.lower() for c in result.validation_checks)

    def test_execution_time_recorded(self):
        result = run_sandbox_test("SELECT * FROM source_table")
        assert result.execution_time_ms > 0

    def test_preview_data_limited(self):
        result = run_sandbox_test("SELECT * FROM source_table")
        assert len(result.preview_data) <= 10

    def test_row_limit_check(self):
        result = run_sandbox_test(
            "SELECT * FROM source_table",
            max_rows=3,
        )
        assert result.success is False
        assert "max rows" in result.error_message.lower() or "exceeds" in result.error_message.lower()

    def test_forbidden_sql_blocked(self):
        result = run_sandbox_test("DROP DATABASE production")
        assert result.success is False
        assert "Safety check failed" in result.error_message
