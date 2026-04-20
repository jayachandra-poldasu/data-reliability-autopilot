"""
Failure Classifier — Deterministic classification of pipeline failures.

Uses pattern matching to classify pipeline failures into categories
(schema_drift, data_quality, sql_error, timeout, etc.) with confidence scores.
Each classification includes matched pattern names and reasoning.
"""

import re
from app.models import ClassificationResult, FailureCategory, Severity


# ── Pattern Definitions ──────────────────────────────────────────────────────
# Each pattern: (regex, category, confidence_boost, pattern_name, severity)

FAILURE_PATTERNS: list[tuple[re.Pattern, FailureCategory, float, str, Severity]] = [
    # Schema Drift patterns
    (
        re.compile(r"column\s+['\"]?\w+['\"]?\s+(not found|does not exist|missing)", re.IGNORECASE),
        FailureCategory.SCHEMA_DRIFT,
        0.90,
        "missing_column",
        Severity.HIGH,
    ),
    (
        re.compile(r"expected\s+type\s+\w+\s+but\s+got\s+\w+", re.IGNORECASE),
        FailureCategory.SCHEMA_DRIFT,
        0.92,
        "type_mismatch",
        Severity.HIGH,
    ),
    (
        re.compile(r"schema\s+(mismatch|changed|drift|evolution)", re.IGNORECASE),
        FailureCategory.SCHEMA_DRIFT,
        0.95,
        "explicit_schema_drift",
        Severity.HIGH,
    ),
    (
        re.compile(r"(added|removed|renamed)\s+column", re.IGNORECASE),
        FailureCategory.SCHEMA_DRIFT,
        0.88,
        "column_modification",
        Severity.MEDIUM,
    ),
    (
        re.compile(r"table\s+['\"]?\w+['\"]?\s+(not found|does not exist)", re.IGNORECASE),
        FailureCategory.SCHEMA_DRIFT,
        0.85,
        "missing_table",
        Severity.CRITICAL,
    ),
    # Data Quality patterns
    (
        re.compile(r"(null|none)\s+(constraint|violation|not\s+allowed)", re.IGNORECASE),
        FailureCategory.DATA_QUALITY,
        0.90,
        "null_violation",
        Severity.HIGH,
    ),
    (
        re.compile(r"duplicate\s+(key|entry|record|row)", re.IGNORECASE),
        FailureCategory.DATA_QUALITY,
        0.92,
        "duplicate_key",
        Severity.HIGH,
    ),
    (
        re.compile(r"(check\s+constraint|constraint\s+violation)", re.IGNORECASE),
        FailureCategory.DATA_QUALITY,
        0.88,
        "constraint_violation",
        Severity.MEDIUM,
    ),
    (
        re.compile(r"(invalid|malformed|corrupt)\s+(data|value|format|record)", re.IGNORECASE),
        FailureCategory.DATA_QUALITY,
        0.85,
        "invalid_data",
        Severity.MEDIUM,
    ),
    (
        re.compile(r"could\s+not\s+convert\s+string\s+.*\s+to\s+\w+", re.IGNORECASE),
        FailureCategory.DATA_QUALITY,
        0.93,
        "type_conversion_failure",
        Severity.HIGH,
    ),
    (
        re.compile(r"(data\s+quality|validation)\s+(check|test)\s+failed", re.IGNORECASE),
        FailureCategory.DATA_QUALITY,
        0.94,
        "quality_check_failure",
        Severity.HIGH,
    ),
    (
        re.compile(r"value\s+out\s+of\s+range", re.IGNORECASE),
        FailureCategory.DATA_QUALITY,
        0.86,
        "value_out_of_range",
        Severity.MEDIUM,
    ),
    # SQL Error patterns
    (
        re.compile(r"syntax\s+error\s+(at|near|in)", re.IGNORECASE),
        FailureCategory.SQL_ERROR,
        0.95,
        "sql_syntax_error",
        Severity.HIGH,
    ),
    (
        re.compile(r"(parser|parse)\s+error", re.IGNORECASE),
        FailureCategory.SQL_ERROR,
        0.90,
        "sql_parse_error",
        Severity.HIGH,
    ),
    (
        re.compile(r"ambiguous\s+column\s+(name|reference)", re.IGNORECASE),
        FailureCategory.SQL_ERROR,
        0.88,
        "ambiguous_column",
        Severity.MEDIUM,
    ),
    (
        re.compile(r"division\s+by\s+zero", re.IGNORECASE),
        FailureCategory.SQL_ERROR,
        0.92,
        "division_by_zero",
        Severity.MEDIUM,
    ),
    (
        re.compile(r"(function|aggregate)\s+['\"]?\w+['\"]?\s+(not found|does not exist|unknown)", re.IGNORECASE),
        FailureCategory.SQL_ERROR,
        0.87,
        "unknown_function",
        Severity.HIGH,
    ),
    # Timeout patterns
    (
        re.compile(r"(timeout|timed\s+out|time\s+limit\s+exceeded)", re.IGNORECASE),
        FailureCategory.TIMEOUT,
        0.93,
        "execution_timeout",
        Severity.HIGH,
    ),
    (
        re.compile(r"(query|operation)\s+(cancelled|canceled)\s+due\s+to\s+timeout", re.IGNORECASE),
        FailureCategory.TIMEOUT,
        0.95,
        "query_timeout",
        Severity.HIGH,
    ),
    (
        re.compile(r"(deadline|ttl)\s+exceeded", re.IGNORECASE),
        FailureCategory.TIMEOUT,
        0.90,
        "deadline_exceeded",
        Severity.MEDIUM,
    ),
    # Dependency Failure patterns
    (
        re.compile(r"(connection|connect)\s+(refused|failed|reset|timeout)", re.IGNORECASE),
        FailureCategory.DEPENDENCY_FAILURE,
        0.90,
        "connection_failure",
        Severity.HIGH,
    ),
    (
        re.compile(r"(upstream|downstream|dependency)\s+.*?(failed|unavailable|error)", re.IGNORECASE),
        FailureCategory.DEPENDENCY_FAILURE,
        0.92,
        "dependency_unavailable",
        Severity.HIGH,
    ),
    (
        re.compile(r"(service|host|endpoint)\s+.*?(unreachable|unavailable)", re.IGNORECASE),
        FailureCategory.DEPENDENCY_FAILURE,
        0.88,
        "service_unreachable",
        Severity.HIGH,
    ),
    (
        re.compile(r"(source|input)\s+(file|table|dataset)\s+.*?(not found|missing|unavailable)", re.IGNORECASE),
        FailureCategory.DEPENDENCY_FAILURE,
        0.85,
        "missing_source",
        Severity.HIGH,
    ),
    # Resource Exhaustion patterns
    (
        re.compile(r"(out\s+of\s+memory|oom|memory\s+exceeded)", re.IGNORECASE),
        FailureCategory.RESOURCE_EXHAUSTION,
        0.93,
        "out_of_memory",
        Severity.CRITICAL,
    ),
    (
        re.compile(r"(disk\s+full|no\s+space|storage\s+quota)", re.IGNORECASE),
        FailureCategory.RESOURCE_EXHAUSTION,
        0.92,
        "disk_full",
        Severity.CRITICAL,
    ),
    (
        re.compile(r"(cpu|compute)\s+(limit|quota)\s+exceeded", re.IGNORECASE),
        FailureCategory.RESOURCE_EXHAUSTION,
        0.88,
        "cpu_limit",
        Severity.HIGH,
    ),
    # Permission Error patterns
    (
        re.compile(r"(permission|access)\s+denied", re.IGNORECASE),
        FailureCategory.PERMISSION_ERROR,
        0.94,
        "permission_denied",
        Severity.HIGH,
    ),
    (
        re.compile(r"(unauthorized|forbidden|403)", re.IGNORECASE),
        FailureCategory.PERMISSION_ERROR,
        0.90,
        "unauthorized_access",
        Severity.HIGH,
    ),
    (
        re.compile(r"(insufficient|lacking)\s+(privileges|permissions)", re.IGNORECASE),
        FailureCategory.PERMISSION_ERROR,
        0.92,
        "insufficient_privileges",
        Severity.HIGH,
    ),
]


# ── Severity Mapping ─────────────────────────────────────────────────────────

CATEGORY_DEFAULT_SEVERITY: dict[FailureCategory, Severity] = {
    FailureCategory.SCHEMA_DRIFT: Severity.HIGH,
    FailureCategory.DATA_QUALITY: Severity.MEDIUM,
    FailureCategory.SQL_ERROR: Severity.HIGH,
    FailureCategory.TIMEOUT: Severity.MEDIUM,
    FailureCategory.DEPENDENCY_FAILURE: Severity.HIGH,
    FailureCategory.RESOURCE_EXHAUSTION: Severity.CRITICAL,
    FailureCategory.PERMISSION_ERROR: Severity.HIGH,
    FailureCategory.UNKNOWN: Severity.MEDIUM,
}


def classify_failure(
    error_message: str,
    error_context: str = "",
    pipeline_sql: str = "",
) -> ClassificationResult:
    """
    Classify a pipeline failure based on error patterns.

    Combines error_message, error_context, and pipeline_sql to match
    against known failure patterns. Returns the highest-confidence match
    with all matched pattern names.

    Args:
        error_message: The primary error message from the pipeline.
        error_context: Additional context (stack trace, logs, etc.).
        pipeline_sql: The SQL query that failed, if applicable.

    Returns:
        ClassificationResult with category, confidence, reasoning, and matched patterns.
    """
    combined_text = f"{error_message} {error_context} {pipeline_sql}".strip()

    if not combined_text:
        return ClassificationResult(
            category=FailureCategory.UNKNOWN,
            confidence=0.0,
            reasoning="No error information provided",
            matched_patterns=[],
        )

    # Track all matches per category
    category_matches: dict[FailureCategory, list[tuple[float, str, Severity]]] = {}

    for pattern, category, confidence, pattern_name, severity in FAILURE_PATTERNS:
        if pattern.search(combined_text):
            if category not in category_matches:
                category_matches[category] = []
            category_matches[category].append((confidence, pattern_name, severity))

    if not category_matches:
        return ClassificationResult(
            category=FailureCategory.UNKNOWN,
            confidence=0.3,
            reasoning=f"No known patterns matched the error: {error_message[:100]}",
            matched_patterns=[],
        )

    # Pick the category with the highest single-pattern confidence
    best_category = None
    best_confidence = 0.0
    best_severity = Severity.MEDIUM

    for category, matches in category_matches.items():
        max_conf = max(m[0] for m in matches)
        if max_conf > best_confidence:
            best_confidence = max_conf
            best_category = category
            best_severity = max(
                (m[2] for m in matches),
                key=lambda s: ["info", "low", "medium", "high", "critical"].index(s.value),
            )

    # Boost confidence if multiple patterns match the same category
    matched_for_best = category_matches[best_category]
    if len(matched_for_best) > 1:
        best_confidence = min(1.0, best_confidence + 0.03 * (len(matched_for_best) - 1))

    all_patterns = [m[1] for m in matched_for_best]

    reasoning = _build_reasoning(best_category, all_patterns, best_severity, error_message)

    return ClassificationResult(
        category=best_category,
        confidence=round(best_confidence, 2),
        reasoning=reasoning,
        matched_patterns=all_patterns,
    )


def get_severity_for_category(category: FailureCategory) -> Severity:
    """Get the default severity for a failure category."""
    return CATEGORY_DEFAULT_SEVERITY.get(category, Severity.MEDIUM)


def _build_reasoning(
    category: FailureCategory,
    patterns: list[str],
    severity: Severity,
    error_message: str,
) -> str:
    """Build a human-readable reasoning string for the classification."""
    category_descriptions = {
        FailureCategory.SCHEMA_DRIFT: "Schema change detected — table or column structure has diverged from expectations",
        FailureCategory.DATA_QUALITY: "Data quality issue — values violate constraints, types, or business rules",
        FailureCategory.SQL_ERROR: "SQL execution error — syntax, semantic, or runtime SQL problem",
        FailureCategory.TIMEOUT: "Execution timeout — query or operation exceeded time limits",
        FailureCategory.DEPENDENCY_FAILURE: "Dependency failure — upstream service, file, or connection is unavailable",
        FailureCategory.RESOURCE_EXHAUSTION: "Resource exhaustion — system ran out of memory, disk, or compute capacity",
        FailureCategory.PERMISSION_ERROR: "Permission error — insufficient access rights for the operation",
    }

    desc = category_descriptions.get(category, "Unknown failure type")
    pattern_str = ", ".join(patterns)

    return (
        f"{desc}. "
        f"Matched patterns: [{pattern_str}]. "
        f"Severity: {severity.value}. "
        f"Error excerpt: {error_message[:120]}"
    )
