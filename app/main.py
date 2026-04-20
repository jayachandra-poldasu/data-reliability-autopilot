"""
FastAPI application — Data Reliability Autopilot API.

Provides endpoints for pipeline failure analysis, remediation proposals,
sandbox testing, and human-in-the-loop approval workflows.
"""

import uuid
from datetime import timezone
from datetime import datetime

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from app import __app_name__, __version__
from app.ai_engine import check_ai_health, get_ai_analysis
from app.classifier import classify_failure
from app.config import get_settings
from app.database import get_db_connection, get_pipeline_list, initialize_pipelines
from app.models import (
    ApprovalRequest,
    FailureAnalysisRequest,
    FailureRecord,
    HealthResponse,
    PipelineInfo,
    SandboxResult,
    WorkflowState,
)
from app.remediation import propose_remediations
from app.sandbox import run_sandbox_test


# ── App Setup ────────────────────────────────────────────────────────────────

settings = get_settings()

app = FastAPI(
    title=__app_name__,
    version=__version__,
    description=(
        "AI-powered pipeline failure classification, remediation, "
        "and sandbox validation with human-in-the-loop controls."
    ),
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── In-Memory State ──────────────────────────────────────────────────────────
# Stores failure records (in production, this would be a persistent store)
failure_store: dict[str, FailureRecord] = {}

# Initialize DuckDB with sample pipelines
db_con = get_db_connection()
initialize_pipelines(db_con)


# ── Health ───────────────────────────────────────────────────────────────────

@app.get("/health", response_model=HealthResponse, tags=["System"])
def health_check():
    """Health check endpoint with AI backend status."""
    return HealthResponse(
        status="healthy",
        version=__version__,
        ai_backend=settings.ai_backend.value,
        ai_available=check_ai_health(settings),
        active_failures=len(failure_store),
    )


# ── Pipelines ────────────────────────────────────────────────────────────────

@app.get("/pipelines", response_model=list[PipelineInfo], tags=["Pipelines"])
def list_pipelines():
    """List all registered data pipelines and their current status."""
    return get_pipeline_list(db_con)


# ── Failure Analysis ─────────────────────────────────────────────────────────

@app.post("/failures/analyze", response_model=FailureRecord, tags=["Failures"])
def analyze_failure(request: FailureAnalysisRequest):
    """
    Analyze a pipeline failure: classify, propose remediations, and generate AI analysis.

    The failure goes through: pending → analyzing → awaiting_approval.
    """
    failure_id = str(uuid.uuid4())[:8]

    # Create the failure record
    record = FailureRecord(
        id=failure_id,
        pipeline_name=request.pipeline_name,
        error_message=request.error_message,
        error_context=request.error_context,
        pipeline_sql=request.pipeline_sql,
        state=WorkflowState.ANALYZING,
    )

    # Step 1: Classify the failure
    classification = classify_failure(
        error_message=request.error_message,
        error_context=request.error_context,
        pipeline_sql=request.pipeline_sql,
    )
    record.classification = classification

    # Step 2: Propose remediations
    remediations = propose_remediations(
        classification=classification,
        error_message=request.error_message,
        pipeline_sql=request.pipeline_sql,
    )
    record.remediations = remediations

    # Step 3: AI analysis
    ai_analysis = get_ai_analysis(
        classification=classification,
        error_message=request.error_message,
        pipeline_name=request.pipeline_name,
        pipeline_sql=request.pipeline_sql,
        settings=settings,
    )
    record.ai_analysis = ai_analysis

    # Transition to awaiting_approval
    record.state = WorkflowState.AWAITING_APPROVAL
    record.updated_at = datetime.now(timezone.utc).isoformat()

    # Store the record
    failure_store[failure_id] = record

    return record


@app.get("/failures/{failure_id}", response_model=FailureRecord, tags=["Failures"])
def get_failure(failure_id: str):
    """Get the current state of a failure record."""
    if failure_id not in failure_store:
        raise HTTPException(status_code=404, detail=f"Failure {failure_id} not found")
    return failure_store[failure_id]


# ── Sandbox Testing ──────────────────────────────────────────────────────────

@app.post(
    "/failures/{failure_id}/sandbox-test",
    response_model=SandboxResult,
    tags=["Sandbox"],
)
def sandbox_test(failure_id: str, remediation_index: int = 0):
    """
    Run the selected remediation SQL in an isolated DuckDB sandbox.

    Validates the fix before human approval. The failure must be in
    'awaiting_approval' state.
    """
    if failure_id not in failure_store:
        raise HTTPException(status_code=404, detail=f"Failure {failure_id} not found")

    record = failure_store[failure_id]

    if record.state not in (WorkflowState.AWAITING_APPROVAL, WorkflowState.SANDBOX_TESTING):
        raise HTTPException(
            status_code=400,
            detail=f"Cannot sandbox test in state: {record.state.value}",
        )

    if remediation_index >= len(record.remediations):
        raise HTTPException(
            status_code=400,
            detail=f"Invalid remediation index: {remediation_index}",
        )

    remediation = record.remediations[remediation_index]

    if not remediation.sql_fix:
        return SandboxResult(
            success=True,
            validation_checks=["no_sql_fix: This remediation does not require SQL execution"],
        )

    # Transition to sandbox_testing
    record.state = WorkflowState.SANDBOX_TESTING
    record.selected_remediation = remediation_index
    record.updated_at = datetime.now(timezone.utc).isoformat()

    # Run in sandbox
    result = run_sandbox_test(
        sql=remediation.sql_fix,
        max_rows=settings.sandbox_max_rows,
        timeout=settings.sandbox_timeout,
    )

    record.sandbox_result = result
    record.updated_at = datetime.now(timezone.utc).isoformat()

    # Stay in sandbox_testing state (awaiting approval decision)
    if result.success:
        record.state = WorkflowState.AWAITING_APPROVAL

    failure_store[failure_id] = record

    return result


# ── Approval Workflow ────────────────────────────────────────────────────────

@app.post("/failures/{failure_id}/approve", response_model=FailureRecord, tags=["Workflow"])
def approve_failure(failure_id: str, request: ApprovalRequest = None):
    """
    Approve the selected remediation for a failure.

    Transitions the failure from 'awaiting_approval' to 'approved',
    then to 'applied' after execution.
    """
    if request is None:
        request = ApprovalRequest()

    if failure_id not in failure_store:
        raise HTTPException(status_code=404, detail=f"Failure {failure_id} not found")

    record = failure_store[failure_id]

    if record.state not in (WorkflowState.AWAITING_APPROVAL, WorkflowState.SANDBOX_TESTING):
        raise HTTPException(
            status_code=400,
            detail=f"Cannot approve in state: {record.state.value}",
        )

    record.selected_remediation = request.remediation_index
    record.approver = request.approver
    record.state = WorkflowState.APPROVED
    record.updated_at = datetime.now(timezone.utc).isoformat()

    # Simulate applying the remediation
    record.state = WorkflowState.APPLIED
    record.updated_at = datetime.now(timezone.utc).isoformat()

    failure_store[failure_id] = record

    return record


@app.post("/failures/{failure_id}/reject", response_model=FailureRecord, tags=["Workflow"])
def reject_failure(failure_id: str):
    """
    Reject the proposed remediation for a failure.

    Transitions the failure to 'rejected' state.
    """
    if failure_id not in failure_store:
        raise HTTPException(status_code=404, detail=f"Failure {failure_id} not found")

    record = failure_store[failure_id]

    if record.state in (WorkflowState.APPLIED, WorkflowState.ROLLED_BACK):
        raise HTTPException(
            status_code=400,
            detail=f"Cannot reject in state: {record.state.value}",
        )

    record.state = WorkflowState.REJECTED
    record.updated_at = datetime.now(timezone.utc).isoformat()

    failure_store[failure_id] = record

    return record
