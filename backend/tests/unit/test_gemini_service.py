from unittest.mock import MagicMock, patch
import pytest
from google.genai import errors as genai_errors
from app.schemas.gemini import GeminiInvestigationResponse
from app.services.gemini_service import GeminiUnavailableError, generate_investigation_explanation

_MINIMAL_GEMINI_JSON = (
    '{"title": "t", "summary": "s", "probable_root_causes": [], '
    '"what_happened": [], "source_code_findings": [], '
    '"recommended_actions": [], "uncertainties": []}'
)


def _server_error(status_code=503):
    return genai_errors.ServerError(
        status_code,
        {"message": "This model is currently experiencing high demand.", "status": "UNAVAILABLE"},
        None,
    )

def test_generate_investigation_explanation_returns_structured_response():
    gemini_json = """
    {
        "title": "Database timeout",
        "summary": "The payment service encountered a database timeout.",
        "probable_root_causes": [
            {
                "title": "Database operation timeout",
                "explanation": "The available evidence indicates a database timeout.",
                "evidence_ids": [1]
            }
        ],
        "what_happened": [
            "The payment service attempted a database operation.",
            "The operation timed out."
        ],
        "source_code_findings": [
            {
                "file": "srv/worker.py",
                "line_number": 42,
                "function": "run",
                "explanation": "The matched source location is associated with the failure.",
                "evidence_ids": [1]
            }
        ],
        "recommended_actions": [
            "Inspect database query latency.",
            "Check database connection-pool utilization."
        ],
        "uncertainties": [
            "Database performance metrics were not supplied."
        ]
    }
    """

    mock_response = MagicMock()
    mock_response.text = gemini_json

    context = {
        "analysis_id": 1,
        "investigation_path": "simple",
        "evidence": [
            {
                "id": 1,
                "event_type": "exception",
                "severity": "ERROR",
                "service": "payment-api",
                "representative_line": "database timeout"
            }
        ]
    }

    with patch(
        "app.services.gemini_service._client.models.generate_content",
        return_value=mock_response,
    ) as generate_content:
        result = generate_investigation_explanation(context)

    assert isinstance(result, GeminiInvestigationResponse)
    assert result.title == "Database timeout"
    assert result.probable_root_causes[0].evidence_ids == [1]
    assert result.source_code_findings[0].file == "srv/worker.py"
    assert result.source_code_findings[0].line_number == 42
    assert len(result.recommended_actions) == 2

    generate_content.assert_called_once()


def test_generate_investigation_explanation_disables_automatic_function_calling():
    """Devflo never registers tools/functions on this request, so there is
    nothing for Automatic Function Calling to dispatch - it must be
    explicitly disabled rather than left at the SDK's default-enabled
    behavior (which otherwise logs an AFC warning and recommends
    Chat.send_message for no reason here)."""
    mock_response = MagicMock()
    mock_response.text = '{"title": "t", "summary": "s", "probable_root_causes": [], "what_happened": [], "source_code_findings": [], "recommended_actions": [], "uncertainties": []}'

    with patch(
        "app.services.gemini_service._client.models.generate_content",
        return_value=mock_response,
    ) as generate_content:
        generate_investigation_explanation({"analysis_id": 1})

    _, kwargs = generate_content.call_args
    assert kwargs["config"].automatic_function_calling.disable is True


def test_generate_investigation_explanation_retries_transient_server_error(monkeypatch):
    """A 503 ("high demand") is transient - a subsequent attempt within the
    bounded retry budget that succeeds must return the real result, not
    fail the caller."""
    monkeypatch.setattr("app.services.gemini_service.sleep", lambda seconds: None)
    mock_response = MagicMock()
    mock_response.text = _MINIMAL_GEMINI_JSON

    with patch(
        "app.services.gemini_service._client.models.generate_content",
        side_effect=[_server_error(), mock_response],
    ) as generate_content:
        result = generate_investigation_explanation({"analysis_id": 1})

    assert isinstance(result, GeminiInvestigationResponse)
    assert generate_content.call_count == 2


def test_generate_investigation_explanation_raises_unavailable_after_exhausting_retries(monkeypatch):
    """If Gemini stays unavailable for the whole bounded retry budget, the
    caller (finalize) must get a distinguishable GeminiUnavailableError, not
    a raw SDK exception nor a hang - that is what lets finalize complete
    the deterministic result instead of failing the whole analysis."""
    monkeypatch.setattr("app.services.gemini_service.sleep", lambda seconds: None)

    with patch(
        "app.services.gemini_service._client.models.generate_content",
        side_effect=[_server_error(), _server_error(), _server_error()],
    ) as generate_content:
        with pytest.raises(GeminiUnavailableError):
            generate_investigation_explanation({"analysis_id": 1})

    assert generate_content.call_count == 3


def test_generate_investigation_explanation_does_not_retry_client_error():
    """A 4xx (bad request, auth, quota) is not transient - retrying it
    cannot succeed, so it must propagate immediately rather than burn the
    retry budget or be mistaken for a temporary-unavailability signal."""
    client_error = genai_errors.ClientError(400, {"message": "bad request", "status": "INVALID_ARGUMENT"}, None)

    with patch(
        "app.services.gemini_service._client.models.generate_content",
        side_effect=client_error,
    ) as generate_content:
        with pytest.raises(genai_errors.ClientError):
            generate_investigation_explanation({"analysis_id": 1})

    generate_content.assert_called_once()