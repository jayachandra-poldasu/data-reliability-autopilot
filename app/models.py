"""
Pydantic models for API request/response schemas.

Defines the data contracts for the Data Reliability Autopilot API, including
failure analysis, remediation proposals, sandbox validation, and workflow states.
"""

from datetime import datetime, timezone
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


# ── Enums ────────────────────────────────────────────────────────────────────


class FailureCategory(str, Enum):
    """Categories of pipeline failures."""
    SCHEMA_DRIFT = "schema_drift"
    DATA_QUALITY = "data_quality"
    SQL_ERROR = "sql_error"
    TIMEOUT = "timeout"
    DEPENDENCY_FAILURE = "dependency_failure"
    RESOURCE_EXHAUSTION = "resource_exhaustion"
    PERMISSION_ERROR = "permission_error"
    UNKNOWN = "unknown"


class Severity(str, Enum):
    """Failure severity levels."""
    CRITICAL = "critical"
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"
    INFO = "info"


class RemediationAction(str, Enum):
    """Types of remediation actions."""
    RETRY = "retry"
    QUARANTINE_BAD_ROWS = "quarantine_bad_rows"
    ROLLBACK = "rollback"
    APPLY_SCHEMA_MIGRATION = "apply_schema_migration"
    SKIP_AND_ALERT = "skip_and_alert"
    FIX_SQL = "fix_sql"
    INCREASE_RESOURCES = "increase_resources"
    FIX_PERMISSIONS = "fix_permissions"


class WorkflowState(str, Enum):
    """State machine for failure remediation workflow."""
    PENDING = "pending"
    ANALYZING = "analyzing"
    AWAITING_APPROVAL = "awaiting_approval"
    SANDBOX_TESTING = "sandbox_testing"
    APPROVED = "approved"
    APPLIED = "applied"
    REJECTED = "rejected"
    ROLLED_BACK = "rolled_back"


class PipelineStatus(str, Enum):
    """Status of a data pipeline."""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    FAILED = "failed"
    RECOVERING = "recovering"


# ── Request Models ───────────────────────────────────────────────────────────


class FailureAnalysisRequest(BaseModel):
    """Request payload for pipeline failure analysis."""
    pipeline_name: str = Field(
        ...,
        description="Name of the failing pipeline",
        examples=["daily_order_ingestion"],
    )
    error_message: str = Field(
        ...,
        min_length=1,
        description="Error message from the pipeline failure",
        examples=["Column 'revenue' expected type DOUBLE but got VARCHAR"],
    )
    error_context: str = Field(
        default="",
        description="Additional context (SQL query, stack trace, etc.)",
    )
    pipeline_sql: str = Field(
        default="",
        description="The SQL query that failed, if applicable",
    )


class ApprovalRequest(BaseModel):
    """Request payload for approving a remediation."""
    remediation_index: int = Field(
        default=0,
        ge=0,
        description="Index of the chosen remediation option (0-based)",
    )
    approver: str = Field(
        default="operator",
        description="Name or ID of the person approving",
    )


# ── Response Models ──────────────────────────────────────────────────────────


class ClassificationResult(BaseModel):
    """Result of failure classification."""
    category: FailureCategory = Field(..., description="Classified failure category")
    confidence: float = Field(
        ...,
        ge=0.0,
        le=1.0,
        description="Classification confidence score (0.0 to 1.0)",
    )
    reasoning: str = Field(default="", description="Explanation of classification")
    matched_patterns: list[str] = Field(
        default_factory=list,
        description="Pattern names that matched this failure",
    )


class RemediationProposal(BaseModel):
    """A single proposed remediation action."""
    rank: int = Field(..., ge=1, description="Priority rank (1 = highest)")
    action: RemediationAction = Field(..., description="Type of remediation action")
    description: str = Field(..., description="Human-readable description of the action")
    estimated_impact: str = Field(
        default="",
        description="Expected impact of applying this action",
    )
    sql_fix: str = Field(
        default="",
        description="SQL fix to execute, if applicable",
    )
    risk_level: Severity = Field(
        default=Severity.LOW,
        description="Risk level of applying this remediation",
    )
    auto_executable: bool = Field(
        default=False,
        description="Whether this action can be executed automatically",
    )


class SandboxResult(BaseModel):
    """Result of sandbox validation."""
    success: bool = Field(..., description="Whether the sandbox test passed")
    rows_affected: int = Field(default=0, description="Number of rows affected")
    rows_quarantined: int = Field(default=0, description="Number of rows quarantined")
    execution_time_ms: float = Field(default=0.0, description="Execution time in ms")
    error_message: str = Field(default="", description="Error message if failed")
    preview_data: list[dict] = Field(
        default_factory=list,
        description="Preview of the result data (first 10 rows)",
    )
    validation_checks: list[str] = Field(
        default_factory=list,
        description="List of validation checks performed",
    )


class FailureRecord(BaseModel):
    """Complete record of a pipeline failure and its resolution."""
    id: str = Field(..., description="Unique failure ID")
    pipeline_name: str
    error_message: str
    error_context: str = ""
    pipeline_sql: str = ""
    state: WorkflowState = WorkflowState.PENDING
    classification: Optional[ClassificationResult] = None
    remediations: list[RemediationProposal] = Field(default_factory=list)
    selected_remediation: Optional[int] = None
    sandbox_result: Optional[SandboxResult] = None
    ai_analysis: str = ""
    approver: str = ""
    created_at: str = Field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat(),
    )
    updated_at: str = Field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat(),
    )


class PipelineInfo(BaseModel):
    """Information about a data pipeline."""
    name: str
    status: PipelineStatus
    last_run: str = ""
    schedule: str = ""
    owner: str = ""
    tables: list[str] = Field(default_factory=list)
    description: str = ""


class HealthResponse(BaseModel):
    """API health check response."""
    status: str = "healthy"
    version: str
    ai_backend: str
    ai_available: bool
    database: str = "duckdb"
    active_failures: int = 0
