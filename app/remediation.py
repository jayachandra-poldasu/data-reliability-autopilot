"""
Remediation Engine — Proposes ranked recovery actions for pipeline failures.

Given a failure classification, generates ranked remediation proposals
including retry, quarantine, rollback, schema migration, SQL fix, and more.
"""

from app.models import (
    ClassificationResult,
    FailureCategory,
    RemediationAction,
    RemediationProposal,
    Severity,
)


def propose_remediations(
    classification: ClassificationResult,
    error_message: str = "",
    pipeline_sql: str = "",
) -> list[RemediationProposal]:
    """
    Generate ranked remediation proposals based on failure classification.

    Args:
        classification: The classified failure result.
        error_message: Original error message for context.
        pipeline_sql: The failing SQL query, if applicable.

    Returns:
        List of RemediationProposal ordered by priority rank.
    """
    strategies = _get_strategies(classification.category)

    proposals = []
    for rank, (action, description, impact, risk, auto) in enumerate(strategies, start=1):
        sql_fix = ""
        if action == RemediationAction.FIX_SQL and pipeline_sql:
            sql_fix = _generate_sql_fix(classification, error_message, pipeline_sql)
        elif action == RemediationAction.QUARANTINE_BAD_ROWS and pipeline_sql:
            sql_fix = _generate_quarantine_sql(classification)
        elif action == RemediationAction.APPLY_SCHEMA_MIGRATION:
            sql_fix = _generate_schema_migration_sql(classification)

        proposals.append(
            RemediationProposal(
                rank=rank,
                action=action,
                description=description,
                estimated_impact=impact,
                sql_fix=sql_fix,
                risk_level=risk,
                auto_executable=auto,
            )
        )

    return proposals


def _get_strategies(category: FailureCategory):
    """Return ordered remediation strategies for a failure category."""
    strategies = {
        FailureCategory.SCHEMA_DRIFT: [
            (RemediationAction.APPLY_SCHEMA_MIGRATION,
             "Apply automatic schema migration to align source and target schemas.",
             "Pipeline will resume with updated schema", Severity.MEDIUM, False),
            (RemediationAction.QUARANTINE_BAD_ROWS,
             "Quarantine rows that don't conform to the expected schema.",
             "Partial data processed; non-conforming rows isolated", Severity.LOW, True),
            (RemediationAction.ROLLBACK,
             "Rollback to the previous schema version and reprocess.",
             "Data reverts to previous state", Severity.HIGH, False),
            (RemediationAction.SKIP_AND_ALERT,
             "Skip the current batch and alert the data engineering team.",
             "Current batch skipped", Severity.LOW, True),
        ],
        FailureCategory.DATA_QUALITY: [
            (RemediationAction.QUARANTINE_BAD_ROWS,
             "Isolate rows failing quality checks into a quarantine table.",
             "Clean data processed; bad rows available for review", Severity.LOW, True),
            (RemediationAction.FIX_SQL,
             "Apply data cleansing SQL: TRY_CAST for types, COALESCE for nulls.",
             "Data quality issues resolved via SQL transformation", Severity.MEDIUM, False),
            (RemediationAction.RETRY,
             "Retry the pipeline with enhanced error handling.",
             "Pipeline re-executes with stricter validation", Severity.LOW, True),
            (RemediationAction.SKIP_AND_ALERT,
             "Skip affected records and notify the data quality team.",
             "Pipeline continues with partial data", Severity.LOW, True),
        ],
        FailureCategory.SQL_ERROR: [
            (RemediationAction.FIX_SQL,
             "Analyze and fix the SQL syntax or semantic error.",
             "Query will execute correctly after fix", Severity.MEDIUM, False),
            (RemediationAction.ROLLBACK,
             "Rollback any partial results and restore pre-execution state.",
             "No data corruption; ready for corrected SQL", Severity.LOW, False),
            (RemediationAction.SKIP_AND_ALERT,
             "Skip the failing query and alert the development team.",
             "Downstream tables may have missing data", Severity.LOW, True),
        ],
        FailureCategory.TIMEOUT: [
            (RemediationAction.RETRY,
             "Retry with increased timeout limits and optimized execution.",
             "May succeed with more time", Severity.LOW, True),
            (RemediationAction.INCREASE_RESOURCES,
             "Scale up compute resources or optimize the query.",
             "Improved performance prevents future timeouts", Severity.MEDIUM, False),
            (RemediationAction.SKIP_AND_ALERT,
             "Skip current execution and alert operations team.",
             "Current batch delayed", Severity.LOW, True),
        ],
        FailureCategory.DEPENDENCY_FAILURE: [
            (RemediationAction.RETRY,
             "Retry after backoff period.",
             "Pipeline resumes if dependency recovered", Severity.LOW, True),
            (RemediationAction.SKIP_AND_ALERT,
             "Skip current run and alert platform team.",
             "Pipeline paused until dependency restored", Severity.LOW, True),
            (RemediationAction.ROLLBACK,
             "Rollback partial writes and wait for dependency.",
             "Clean state maintained", Severity.MEDIUM, False),
        ],
        FailureCategory.RESOURCE_EXHAUSTION: [
            (RemediationAction.INCREASE_RESOURCES,
             "Increase memory, disk, or compute capacity.",
             "Pipeline will have sufficient resources", Severity.MEDIUM, False),
            (RemediationAction.RETRY,
             "Retry with reduced batch size.",
             "Smaller batches reduce peak resource usage", Severity.LOW, True),
            (RemediationAction.SKIP_AND_ALERT,
             "Skip and alert infrastructure team.",
             "Pipeline paused until resources available", Severity.LOW, True),
        ],
        FailureCategory.PERMISSION_ERROR: [
            (RemediationAction.FIX_PERMISSIONS,
             "Grant required permissions or update service account roles.",
             "Pipeline will have proper access", Severity.MEDIUM, False),
            (RemediationAction.RETRY,
             "Retry after verifying permissions restored.",
             "May succeed if permissions were temporarily revoked", Severity.LOW, True),
            (RemediationAction.SKIP_AND_ALERT,
             "Skip and escalate to security team.",
             "Pipeline paused; security team notified", Severity.LOW, True),
        ],
        FailureCategory.UNKNOWN: [
            (RemediationAction.RETRY,
             "Retry with verbose logging enabled.",
             "May resolve transient issues", Severity.LOW, True),
            (RemediationAction.SKIP_AND_ALERT,
             "Skip and create incident ticket for investigation.",
             "Pipeline paused; investigation ticket created", Severity.LOW, True),
            (RemediationAction.ROLLBACK,
             "Rollback to last known good state.",
             "Safe recovery to clean state", Severity.LOW, False),
        ],
    }
    return strategies.get(category, strategies[FailureCategory.UNKNOWN])


def _generate_sql_fix(classification, error_message, pipeline_sql):
    """Generate a deterministic SQL fix based on classification patterns."""
    patterns = classification.matched_patterns

    if "type_conversion_failure" in patterns or "type_mismatch" in patterns:
        return (
            "SELECT id, name, TRY_CAST(amount AS INTEGER) AS amount, "
            "CASE WHEN TRY_CAST(amount AS INTEGER) IS NULL THEN 'quarantined' "
            "ELSE 'valid' END AS _row_status "
            "FROM source_table"
        )
    if "null_violation" in patterns:
        return (
            "SELECT *, CASE WHEN id IS NULL OR name IS NULL "
            "THEN 'quarantined' ELSE 'valid' END AS _row_status "
            "FROM source_table WHERE id IS NOT NULL"
        )
    if "duplicate_key" in patterns:
        return (
            "SELECT * FROM ("
            "SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY created_at DESC) AS rn "
            "FROM source_table) sub WHERE rn = 1"
        )
    if "division_by_zero" in patterns:
        return (
            "SELECT *, CASE WHEN denominator = 0 THEN NULL "
            "ELSE numerator / denominator END AS result FROM source_table"
        )
    return f"-- Original SQL needs manual review:\n-- {pipeline_sql[:200]}"


def _generate_quarantine_sql(classification):
    """Generate SQL to quarantine bad rows."""
    patterns = classification.matched_patterns
    if "type_conversion_failure" in patterns or "type_mismatch" in patterns:
        return (
            "SELECT * FROM source_table WHERE TRY_CAST(amount AS INTEGER) IS NOT NULL;\n"
            "-- Quarantined:\n"
            "SELECT *, 'type_conversion_failure' AS quarantine_reason "
            "FROM source_table WHERE TRY_CAST(amount AS INTEGER) IS NULL"
        )
    if "null_violation" in patterns:
        return (
            "SELECT * FROM source_table WHERE id IS NOT NULL AND name IS NOT NULL;\n"
            "-- Quarantined:\n"
            "SELECT *, 'null_violation' AS quarantine_reason "
            "FROM source_table WHERE id IS NULL OR name IS NULL"
        )
    return "SELECT * FROM source_table WHERE 1=1"


def _generate_schema_migration_sql(classification):
    """Generate SQL for schema migration."""
    patterns = classification.matched_patterns
    if "missing_column" in patterns:
        return (
            "ALTER TABLE target_table ADD COLUMN IF NOT EXISTS "
            "new_column VARCHAR DEFAULT 'N/A'"
        )
    if "type_mismatch" in patterns:
        return (
            "ALTER TABLE target_table ADD COLUMN IF NOT EXISTS amount_new DOUBLE;\n"
            "UPDATE target_table SET amount_new = TRY_CAST(amount AS DOUBLE)"
        )
    return (
        "SELECT column_name, data_type FROM information_schema.columns "
        "WHERE table_name = 'target_table'"
    )
