"""
Tests for the AI Engine module.

Covers all three backends (Ollama, OpenAI, none), prompt building,
health checks, fallback behavior, and deterministic analysis.
"""

import pytest
from unittest.mock import patch, MagicMock

from app.ai_engine import (
    get_ai_analysis,
    check_ai_health,
    _build_prompt,
    _deterministic_analysis,
)
from app.config import AIBackend, Settings
from app.models import ClassificationResult, FailureCategory


@pytest.fixture
def sample_classification():
    return ClassificationResult(
        category=FailureCategory.DATA_QUALITY,
        confidence=0.93,
        reasoning="Type conversion failure detected",
        matched_patterns=["type_conversion_failure"],
    )


@pytest.fixture
def none_settings():
    return Settings(
        ai_backend=AIBackend.NONE,
        db_path=":memory:",
    )


@pytest.fixture
def ollama_settings():
    return Settings(
        ai_backend=AIBackend.OLLAMA,
        ollama_url="http://localhost:11434/api/generate",
        ollama_model="llama3",
        db_path=":memory:",
    )


@pytest.fixture
def openai_settings():
    return Settings(
        ai_backend=AIBackend.OPENAI,
        openai_api_key="test-key-123",
        openai_model="gpt-4o-mini",
        db_path=":memory:",
    )


class TestDeterministicAnalysis:
    """Tests for deterministic (no-AI) analysis."""

    def test_none_backend_returns_analysis(self, sample_classification, none_settings):
        result = get_ai_analysis(
            classification=sample_classification,
            error_message="Could not convert string",
            pipeline_name="daily_orders",
            settings=none_settings,
        )
        assert "[Deterministic Analysis]" in result
        assert "daily_orders" in result
        assert "data_quality" in result

    def test_all_categories_have_advice(self, none_settings):
        for category in FailureCategory:
            classification = ClassificationResult(
                category=category,
                confidence=0.90,
                reasoning="Test",
                matched_patterns=[],
            )
            result = _deterministic_analysis(classification, "test error", "test_pipeline")
            assert len(result) > 50
            assert category.value in result

    def test_deterministic_includes_patterns(self, none_settings):
        classification = ClassificationResult(
            category=FailureCategory.SCHEMA_DRIFT,
            confidence=0.92,
            reasoning="Test",
            matched_patterns=["missing_column", "type_mismatch"],
        )
        result = _deterministic_analysis(classification, "test", "pipeline")
        assert "missing_column" in result
        assert "type_mismatch" in result


class TestPromptBuilding:
    """Tests for LLM prompt construction."""

    def test_prompt_contains_context(self, sample_classification):
        prompt = _build_prompt(
            sample_classification,
            "Type conversion error",
            "daily_orders",
            "SELECT CAST(x AS INT) FROM t",
        )
        assert "daily_orders" in prompt
        assert "data_quality" in prompt
        assert "type_conversion_failure" in prompt
        assert "CAST" in prompt

    def test_prompt_without_sql(self, sample_classification):
        prompt = _build_prompt(
            sample_classification,
            "Error message",
            "pipeline",
            "",
        )
        assert "FAILING SQL" not in prompt

    def test_prompt_has_structure(self, sample_classification):
        prompt = _build_prompt(sample_classification, "err", "pipe", "sql")
        assert "ROOT CAUSE" in prompt
        assert "IMPACT" in prompt
        assert "RECOMMENDED ACTION" in prompt
        assert "PREVENTION" in prompt


class TestOllamaBackend:
    """Tests for Ollama backend (mocked HTTP)."""

    @patch("app.ai_engine.requests.post")
    def test_ollama_success(self, mock_post, sample_classification, ollama_settings):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "response": "Root cause: type mismatch in data pipeline."
        }
        mock_resp.raise_for_status = MagicMock()
        mock_post.return_value = mock_resp

        result = get_ai_analysis(
            classification=sample_classification,
            error_message="Type error",
            settings=ollama_settings,
        )
        assert "type mismatch" in result
        mock_post.assert_called_once()

    @patch("app.ai_engine.requests.post")
    def test_ollama_failure_falls_back(self, mock_post, sample_classification, ollama_settings):
        mock_post.side_effect = Exception("Connection refused")

        result = get_ai_analysis(
            classification=sample_classification,
            error_message="Error",
            settings=ollama_settings,
        )
        assert "[Deterministic Analysis]" in result

    @patch("app.ai_engine.requests.get")
    def test_ollama_health_available(self, mock_get, ollama_settings):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_get.return_value = mock_resp

        assert check_ai_health(ollama_settings) is True

    @patch("app.ai_engine.requests.get")
    def test_ollama_health_unavailable(self, mock_get, ollama_settings):
        mock_get.side_effect = Exception("Connection refused")
        assert check_ai_health(ollama_settings) is False


class TestOpenAIBackend:
    """Tests for OpenAI backend (mocked HTTP)."""

    @patch("app.ai_engine.requests.post")
    def test_openai_success(self, mock_post, sample_classification, openai_settings):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "choices": [{"message": {"content": "AI analysis: check data types."}}]
        }
        mock_resp.raise_for_status = MagicMock()
        mock_post.return_value = mock_resp

        result = get_ai_analysis(
            classification=sample_classification,
            error_message="Error",
            settings=openai_settings,
        )
        assert "data types" in result

    @patch("app.ai_engine.requests.post")
    def test_openai_failure_falls_back(self, mock_post, sample_classification, openai_settings):
        mock_post.side_effect = Exception("API error")

        result = get_ai_analysis(
            classification=sample_classification,
            error_message="Error",
            settings=openai_settings,
        )
        assert "[Deterministic Analysis]" in result

    def test_openai_health_with_key(self, openai_settings):
        assert check_ai_health(openai_settings) is True

    def test_openai_health_without_key(self):
        settings = Settings(ai_backend=AIBackend.OPENAI, openai_api_key="", db_path=":memory:")
        assert check_ai_health(settings) is False


class TestHealthChecks:
    """Tests for AI health check functionality."""

    def test_none_backend_always_healthy(self, none_settings):
        assert check_ai_health(none_settings) is True
