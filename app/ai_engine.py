"""
AI Engine — Pluggable LLM integration for enhanced failure analysis.

Supports Ollama (local, default), OpenAI, or deterministic-only (none) mode.
Generates contextual analysis narratives when an LLM backend is available,
falls back to deterministic analysis when no LLM is configured.
"""

import logging
from typing import Optional

import requests

from app.config import AIBackend, Settings, get_settings
from app.models import ClassificationResult, FailureCategory

logger = logging.getLogger(__name__)


def get_ai_analysis(
    classification: ClassificationResult,
    error_message: str,
    pipeline_name: str = "",
    pipeline_sql: str = "",
    settings: Optional[Settings] = None,
) -> str:
    """
    Generate an AI-powered analysis narrative for a pipeline failure.

    Uses the configured AI backend (Ollama, OpenAI, or none) to generate
    a contextual analysis. Falls back to deterministic analysis on failure.

    Args:
        classification: The classified failure result.
        error_message: Original error message.
        pipeline_name: Name of the failing pipeline.
        pipeline_sql: The failing SQL query, if applicable.
        settings: Optional settings override (for testing).

    Returns:
        Analysis narrative string.
    """
    if settings is None:
        settings = get_settings()

    if settings.ai_backend == AIBackend.NONE:
        return _deterministic_analysis(classification, error_message, pipeline_name)

    prompt = _build_prompt(classification, error_message, pipeline_name, pipeline_sql)

    try:
        if settings.ai_backend == AIBackend.OLLAMA:
            return _call_ollama(prompt, settings)
        elif settings.ai_backend == AIBackend.OPENAI:
            return _call_openai(prompt, settings)
    except Exception as e:
        logger.warning(f"AI backend failed ({settings.ai_backend.value}): {e}")
        return _deterministic_analysis(classification, error_message, pipeline_name)

    return _deterministic_analysis(classification, error_message, pipeline_name)


def check_ai_health(settings: Optional[Settings] = None) -> bool:
    """
    Check if the configured AI backend is available.

    Returns:
        True if the AI backend is reachable, False otherwise.
    """
    if settings is None:
        settings = get_settings()

    if settings.ai_backend == AIBackend.NONE:
        return True  # "none" backend is always available

    try:
        if settings.ai_backend == AIBackend.OLLAMA:
            # Check Ollama health by hitting the base URL
            base_url = settings.ollama_url.replace("/api/generate", "")
            resp = requests.get(base_url, timeout=5)
            return resp.status_code == 200
        elif settings.ai_backend == AIBackend.OPENAI:
            return bool(settings.openai_api_key)
    except Exception:
        return False

    return False


def _build_prompt(
    classification: ClassificationResult,
    error_message: str,
    pipeline_name: str,
    pipeline_sql: str,
) -> str:
    """Build a structured prompt for LLM analysis."""
    return f"""You are an expert SRE Data Reliability Engineer analyzing a pipeline failure.

PIPELINE: {pipeline_name or 'unknown'}
FAILURE CATEGORY: {classification.category.value}
CONFIDENCE: {classification.confidence}
MATCHED PATTERNS: {', '.join(classification.matched_patterns)}

ERROR MESSAGE:
{error_message}

{f'FAILING SQL:{chr(10)}{pipeline_sql}' if pipeline_sql else ''}

CLASSIFICATION REASONING:
{classification.reasoning}

Provide a concise analysis covering:
1. ROOT CAUSE: What specifically went wrong
2. IMPACT: What data or systems are affected
3. RECOMMENDED ACTION: The safest remediation path
4. PREVENTION: How to prevent this in the future

Keep the response under 200 words. Be specific and actionable."""


def _call_ollama(prompt: str, settings: Settings) -> str:
    """Call Ollama API for local LLM inference."""
    response = requests.post(
        settings.ollama_url,
        json={
            "model": settings.ollama_model,
            "prompt": prompt,
            "stream": False,
        },
        timeout=settings.ollama_timeout,
    )
    response.raise_for_status()
    return response.json().get("response", "").strip()


def _call_openai(prompt: str, settings: Settings) -> str:
    """Call OpenAI API for cloud LLM inference."""
    response = requests.post(
        "https://api.openai.com/v1/chat/completions",
        headers={
            "Authorization": f"Bearer {settings.openai_api_key}",
            "Content-Type": "application/json",
        },
        json={
            "model": settings.openai_model,
            "messages": [
                {"role": "system", "content": "You are an expert SRE data reliability engineer."},
                {"role": "user", "content": prompt},
            ],
            "max_tokens": 500,
            "temperature": 0.3,
        },
        timeout=settings.openai_timeout,
    )
    response.raise_for_status()
    data = response.json()
    return data["choices"][0]["message"]["content"].strip()


def _deterministic_analysis(
    classification: ClassificationResult,
    error_message: str,
    pipeline_name: str,
) -> str:
    """Generate a deterministic analysis without LLM."""
    category_advice = {
        FailureCategory.SCHEMA_DRIFT: (
            "Schema drift detected. The source data structure has changed in a way that "
            "breaks the pipeline. Review the upstream data source for column additions, "
            "removals, or type changes. Apply a schema migration or update the pipeline "
            "to handle the new schema. Consider adding schema validation checks at ingestion."
        ),
        FailureCategory.DATA_QUALITY: (
            "Data quality issue detected. The incoming data contains values that violate "
            "constraints or expectations (nulls, type mismatches, duplicates, or out-of-range values). "
            "Quarantine the affected rows and process clean data. Add data quality checks "
            "and monitoring at the source level to catch issues earlier."
        ),
        FailureCategory.SQL_ERROR: (
            "SQL execution error detected. The query contains syntax errors, references "
            "to non-existent objects, or semantic issues. Review and correct the SQL statement. "
            "Consider adding SQL validation in the CI/CD pipeline to catch errors before deployment."
        ),
        FailureCategory.TIMEOUT: (
            "Execution timeout detected. The query or operation exceeded time limits, likely "
            "due to data volume growth or missing indexes. Optimize the query, add appropriate "
            "indexes, or increase the timeout threshold. Consider partitioning large tables."
        ),
        FailureCategory.DEPENDENCY_FAILURE: (
            "Dependency failure detected. An upstream service, file, or database connection "
            "is unavailable. Verify the dependency status and retry after a backoff period. "
            "Add circuit breakers and fallback mechanisms for critical dependencies."
        ),
        FailureCategory.RESOURCE_EXHAUSTION: (
            "Resource exhaustion detected. The system ran out of memory, disk space, or "
            "compute capacity. Scale up resources or optimize the workload. Implement "
            "resource monitoring and alerting to prevent future exhaustion."
        ),
        FailureCategory.PERMISSION_ERROR: (
            "Permission error detected. The pipeline lacks required access rights. "
            "Review and update IAM policies, service account roles, or access control lists. "
            "Implement least-privilege access and regular permission audits."
        ),
        FailureCategory.UNKNOWN: (
            "Unknown failure type. The error does not match known patterns. "
            "Enable verbose logging and investigate manually. Consider adding new "
            "classification patterns once the root cause is identified."
        ),
    }

    advice = category_advice.get(
        classification.category,
        category_advice[FailureCategory.UNKNOWN],
    )

    return (
        f"[Deterministic Analysis] Pipeline: {pipeline_name or 'unknown'} | "
        f"Category: {classification.category.value} | "
        f"Confidence: {classification.confidence} | "
        f"Patterns: {', '.join(classification.matched_patterns) or 'none'}\n\n"
        f"{advice}"
    )
