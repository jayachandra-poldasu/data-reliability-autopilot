"""
Shared test fixtures for the Data Reliability Autopilot test suite.
"""

import os
import pytest
from unittest.mock import patch

from fastapi.testclient import TestClient


@pytest.fixture(autouse=True)
def reset_settings_cache():
    """Reset the settings cache before each test."""
    from app.config import get_settings
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


@pytest.fixture
def test_settings():
    """Settings configured for testing (no AI backend)."""
    with patch.dict(os.environ, {
        "AUTOPILOT_AI_BACKEND": "none",
        "AUTOPILOT_DB_PATH": ":memory:",
        "AUTOPILOT_DEBUG": "true",
    }):
        from app.config import get_settings
        get_settings.cache_clear()
        settings = get_settings()
        yield settings
        get_settings.cache_clear()


@pytest.fixture
def api_client(test_settings):
    """FastAPI test client with settings configured for testing."""
    from app.main import app, failure_store
    failure_store.clear()
    client = TestClient(app)
    yield client
    failure_store.clear()


@pytest.fixture
def sample_error_messages():
    """Common error messages for testing classification."""
    return {
        "schema_drift_missing_col": "Column 'revenue' not found in table 'daily_sales'",
        "schema_drift_type_mismatch": "Expected type INTEGER but got VARCHAR in column 'amount'",
        "schema_drift_explicit": "Schema mismatch detected between source and target",
        "data_quality_null": "Null constraint violation on column 'customer_id'",
        "data_quality_duplicate": "Duplicate key violation: id=42 already exists",
        "data_quality_type_conv": "Could not convert string 'invalid_number' to INTEGER",
        "data_quality_invalid": "Invalid data format in record 15",
        "data_quality_range": "Value out of range for column 'age': -5",
        "sql_syntax": "Syntax error at or near 'FORM' — did you mean 'FROM'?",
        "sql_parse": "Parser error: unexpected token at position 42",
        "sql_division": "Division by zero in expression: revenue / count",
        "timeout": "Query timed out after 300 seconds",
        "timeout_deadline": "Deadline exceeded for operation 'aggregate_daily'",
        "dependency_conn": "Connection refused to database host db-primary:5432",
        "dependency_upstream": "Upstream service 'user-api' failed with 503",
        "dependency_source": "Source file 's3://bucket/data.csv' not found",
        "resource_oom": "Out of memory: cannot allocate 2GB for hash table",
        "resource_disk": "Disk full: no space left on device /data",
        "permission_denied": "Permission denied accessing table 'sensitive_data'",
        "permission_forbidden": "Forbidden: insufficient privileges for DELETE",
        "unknown": "Something went wrong during processing",
    }


@pytest.fixture
def sample_pipeline_sql():
    """Sample SQL queries for testing."""
    return {
        "insert_orders": (
            "INSERT INTO orders SELECT id, name, "
            "CAST(amount AS INTEGER) FROM read_csv_auto('data/raw.csv')"
        ),
        "select_revenue": (
            "SELECT region, SUM(revenue) FROM daily_revenue "
            "GROUP BY region ORDER BY revenue DESC"
        ),
        "bad_syntax": "SELECT * FORM orders WHERE id > 10",
    }


@pytest.fixture
def sample_failure_request():
    """A sample failure analysis request body."""
    return {
        "pipeline_name": "daily_order_ingestion",
        "error_message": "Could not convert string 'invalid_number' to INTEGER",
        "error_context": "Row 15 in batch 42",
        "pipeline_sql": (
            "INSERT INTO orders SELECT id, name, "
            "CAST(amount AS INTEGER) FROM source_table"
        ),
    }
