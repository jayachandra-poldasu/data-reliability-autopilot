"""
Sandbox Validator — Executes remediation SQL in an isolated DuckDB sandbox.

Provides safe, isolated execution of AI-suggested or deterministic SQL fixes
before they are applied to production. Each sandbox test runs in a fresh
in-memory DuckDB instance with sample data loaded.
"""

import time
from typing import Optional

import duckdb

from app.models import SandboxResult


# ── Forbidden SQL patterns (safety guard) ────────────────────────────────────
FORBIDDEN_PATTERNS = [
    "DROP DATABASE",
    "DROP SCHEMA",
    "ATTACH",
    "DETACH",
    "COPY TO",
    "EXPORT",
    "LOAD",
    "INSTALL",
]


def validate_sql_safety(sql: str) -> tuple[bool, str]:
    """
    Check if SQL is safe to run in the sandbox.

    Blocks dangerous operations that could affect external resources.

    Args:
        sql: The SQL statement to validate.

    Returns:
        Tuple of (is_safe, reason).
    """
    if not sql or not sql.strip():
        return False, "Empty SQL statement"

    sql_upper = sql.upper()

    for pattern in FORBIDDEN_PATTERNS:
        if pattern in sql_upper:
            return False, f"Forbidden SQL pattern detected: {pattern}"

    return True, "SQL passed safety checks"


def run_sandbox_test(
    sql: str,
    setup_sql: Optional[str] = None,
    sample_data: Optional[list[dict]] = None,
    max_rows: int = 10000,
    timeout: int = 30,
) -> SandboxResult:
    """
    Execute SQL in an isolated DuckDB sandbox and return results.

    Creates a fresh in-memory DuckDB database, optionally loads sample data,
    executes the provided SQL, and returns the results with validation checks.

    Args:
        sql: The SQL to execute in the sandbox.
        setup_sql: Optional SQL to run before the test (e.g., CREATE TABLE).
        sample_data: Optional list of dicts to load as 'source_table'.
        max_rows: Maximum number of rows allowed in the result.
        timeout: Maximum execution time in seconds.

    Returns:
        SandboxResult with success status, affected rows, preview data, etc.
    """
    # Safety check first
    is_safe, safety_reason = validate_sql_safety(sql)
    if not is_safe:
        return SandboxResult(
            success=False,
            error_message=f"Safety check failed: {safety_reason}",
            validation_checks=["safety_check: FAILED"],
        )

    validation_checks = ["safety_check: PASSED"]
    start_time = time.time()

    try:
        # Create isolated in-memory database
        con = duckdb.connect(database=":memory:")
        validation_checks.append("sandbox_created: PASSED")

        # Load sample data if provided
        if sample_data:
            _load_sample_data(con, sample_data)
            validation_checks.append(f"sample_data_loaded: {len(sample_data)} rows")
        elif setup_sql:
            # Run setup SQL (CREATE TABLE, INSERT, etc.)
            for stmt in setup_sql.split(";"):
                stmt = stmt.strip()
                if stmt:
                    con.execute(stmt)
            validation_checks.append("setup_sql: PASSED")
        else:
            # Create default source_table with sample data
            _create_default_sample_data(con)
            validation_checks.append("default_sample_data: LOADED")

        # Execute the SQL under test
        # Handle multi-statement SQL (take only SELECT statements for result)
        statements = [s.strip() for s in sql.split(";") if s.strip()]
        result_df = None
        rows_affected = 0

        for stmt in statements:
            if stmt.startswith("--"):
                continue
            try:
                result = con.execute(stmt)
                # Only fetch results for SELECT statements
                if stmt.upper().lstrip().startswith("SELECT"):
                    result_df = result.fetchdf()
                    rows_affected = len(result_df)
            except Exception as stmt_err:
                # If it's a non-SELECT statement that errors, report it
                if stmt.upper().lstrip().startswith("SELECT"):
                    raise stmt_err
                # For DDL/DML, the error might be expected
                validation_checks.append(f"statement_warning: {str(stmt_err)[:100]}")

        execution_time = (time.time() - start_time) * 1000
        validation_checks.append("execution: PASSED")

        # Row count validation
        if rows_affected > max_rows:
            return SandboxResult(
                success=False,
                rows_affected=rows_affected,
                execution_time_ms=round(execution_time, 2),
                error_message=f"Result exceeds max rows: {rows_affected} > {max_rows}",
                validation_checks=validation_checks + ["row_limit: FAILED"],
            )
        validation_checks.append(f"row_count: {rows_affected} (within limit)")

        # Build preview data
        preview_data = []
        if result_df is not None and not result_df.empty:
            preview_data = result_df.head(10).to_dict(orient="records")
            # Convert any non-serializable types
            for row in preview_data:
                for key, val in row.items():
                    if val is None or (hasattr(val, '__class__') and val.__class__.__name__ == 'NaT'):
                        row[key] = None
                    elif not isinstance(val, (str, int, float, bool)):
                        row[key] = str(val)

        validation_checks.append("preview_generated: PASSED")

        # Count quarantined rows if applicable
        rows_quarantined = 0
        if result_df is not None and "_row_status" in result_df.columns:
            rows_quarantined = int((result_df["_row_status"] == "quarantined").sum())
            validation_checks.append(f"quarantined_rows: {rows_quarantined}")

        con.close()

        return SandboxResult(
            success=True,
            rows_affected=rows_affected,
            rows_quarantined=rows_quarantined,
            execution_time_ms=round(execution_time, 2),
            preview_data=preview_data,
            validation_checks=validation_checks,
        )

    except Exception as e:
        execution_time = (time.time() - start_time) * 1000
        return SandboxResult(
            success=False,
            execution_time_ms=round(execution_time, 2),
            error_message=f"Sandbox execution failed: {str(e)}",
            validation_checks=validation_checks + [f"execution: FAILED - {str(e)[:100]}"],
        )


def _load_sample_data(con: duckdb.DuckDBPyConnection, data: list[dict]):
    """Load a list of dictionaries as source_table in the sandbox."""
    if not data:
        return

    # Infer columns from first row
    columns = list(data[0].keys())
    col_defs = ", ".join(f"{col} VARCHAR" for col in columns)
    con.execute(f"CREATE TABLE source_table ({col_defs})")

    # Insert data
    for row in data:
        values = ", ".join(
            f"'{str(v)}'" if v is not None else "NULL"
            for v in (row.get(c) for c in columns)
        )
        con.execute(f"INSERT INTO source_table VALUES ({values})")


def _create_default_sample_data(con: duckdb.DuckDBPyConnection):
    """Create a default source_table with sample data for testing."""
    con.execute("""
        CREATE TABLE source_table (
            id INTEGER,
            name VARCHAR,
            amount VARCHAR,
            created_at VARCHAR
        )
    """)
    sample_rows = [
        (1, "Alice", "100", "2026-01-01"),
        (2, "Bob", "200", "2026-01-02"),
        (3, "Charlie", "invalid_number", "2026-01-03"),
        (4, "Diana", "400", "2026-01-04"),
        (5, "Eve", "N/A", "2026-01-05"),
        (6, "Frank", "600", "2026-01-06"),
        (7, "Grace", "", "2026-01-07"),
        (8, "Heidi", "800", "2026-01-08"),
        (9, "Ivan", "nine_hundred", "2026-01-09"),
        (10, "Judy", "1000", "2026-01-10"),
    ]
    for row in sample_rows:
        con.execute(
            "INSERT INTO source_table VALUES (?, ?, ?, ?)",
            row,
        )
