from copy import deepcopy
import json
from unittest.mock import MagicMock, PropertyMock, patch
import pytest
from google.genai import errors as genai_errors
from app.schemas.gemini import GeminiInvestigationResponse
from app.services.gemini_service import (
    GeminiUnavailableError,
    _REQUEST_TIMEOUT_SECONDS,
    generate_investigation_explanation,
)
from app.services import gemini_service

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


def _client_error(status_code):
    return genai_errors.ClientError(
        status_code, {"message": "client error", "status": "ERROR"}, None
    )


def _mock_response(text=_MINIMAL_GEMINI_JSON):
    mock_response = MagicMock()
    mock_response.text = text
    return mock_response


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
    with patch(
        "app.services.gemini_service._client.models.generate_content",
        return_value=_mock_response(),
    ) as generate_content:
        generate_investigation_explanation({"analysis_id": 1})

    _, kwargs = generate_content.call_args
    assert kwargs["config"].automatic_function_calling.disable is True


def test_generate_investigation_explanation_sets_a_finite_request_timeout():
    """Without a per-request timeout, a stalled connection can block a
    Celery worker indefinitely and never reach the retry logic below."""
    with patch(
        "app.services.gemini_service._client.models.generate_content",
        return_value=_mock_response(),
    ) as generate_content:
        generate_investigation_explanation({"analysis_id": 1})

    _, kwargs = generate_content.call_args
    assert kwargs["config"].http_options.timeout == _REQUEST_TIMEOUT_SECONDS * 1000


def test_request_timeout_still_set_after_a_retry(monkeypatch):
    monkeypatch.setattr("app.services.gemini_service.sleep", lambda seconds: None)

    with patch(
        "app.services.gemini_service._client.models.generate_content",
        side_effect=[_server_error(), _mock_response()],
    ) as generate_content:
        generate_investigation_explanation({"analysis_id": 1})

    _, kwargs = generate_content.call_args
    assert kwargs["config"].http_options.timeout == _REQUEST_TIMEOUT_SECONDS * 1000


# --- 1/2: 5xx (ServerError) retry policy --------------------------------


def test_generate_investigation_explanation_retries_transient_server_error(monkeypatch):
    """A 503 ("high demand") is transient - a subsequent attempt within the
    bounded retry budget that succeeds must return the real result, not
    fail the caller."""
    monkeypatch.setattr("app.services.gemini_service.sleep", lambda seconds: None)

    with patch(
        "app.services.gemini_service._client.models.generate_content",
        side_effect=[_server_error(), _mock_response()],
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


# --- 3/4: 429 (ClientError) is now retried, unlike other 4xx ------------


def test_generate_investigation_explanation_retries_429_client_error(monkeypatch):
    """429 (rate limiting) is a ClientError in google.genai, but it IS
    transient - it must be retried like a 5xx, not treated as an ordinary
    non-retryable 4xx."""
    monkeypatch.setattr("app.services.gemini_service.sleep", lambda seconds: None)

    with patch(
        "app.services.gemini_service._client.models.generate_content",
        side_effect=[_client_error(429), _mock_response()],
    ) as generate_content:
        result = generate_investigation_explanation({"analysis_id": 1})

    assert isinstance(result, GeminiInvestigationResponse)
    assert generate_content.call_count == 2


def test_generate_investigation_explanation_raises_unavailable_after_exhausting_429_retries(monkeypatch):
    monkeypatch.setattr("app.services.gemini_service.sleep", lambda seconds: None)

    with patch(
        "app.services.gemini_service._client.models.generate_content",
        side_effect=[_client_error(429), _client_error(429), _client_error(429)],
    ) as generate_content:
        with pytest.raises(GeminiUnavailableError):
            generate_investigation_explanation({"analysis_id": 1})

    assert generate_content.call_count == 3


# --- 5/6: ordinary non-429 4xx is NOT retried, but IS converted ---------


def test_generate_investigation_explanation_does_not_retry_400_but_raises_unavailable():
    """A 400 (bad request) is not transient - retrying it cannot succeed,
    so it must NOT burn the retry budget. But it must also never escape as
    a raw SDK exception - the finalizer only catches GeminiUnavailableError,
    so anything else would incorrectly fail an otherwise-complete
    deterministic investigation."""
    with patch(
        "app.services.gemini_service._client.models.generate_content",
        side_effect=_client_error(400),
    ) as generate_content:
        with pytest.raises(GeminiUnavailableError):
            generate_investigation_explanation({"analysis_id": 1})

    generate_content.assert_called_once()


@pytest.mark.parametrize("status_code", [401, 403])
def test_generate_investigation_explanation_does_not_retry_auth_errors(status_code):
    with patch(
        "app.services.gemini_service._client.models.generate_content",
        side_effect=_client_error(status_code),
    ) as generate_content:
        with pytest.raises(GeminiUnavailableError):
            generate_investigation_explanation({"analysis_id": 1})

    generate_content.assert_called_once()


# --- 7/8: unexpected transport/SDK exceptions from generate_content ----


def test_generate_investigation_explanation_retries_unexpected_transport_error(monkeypatch):
    """A generic network/timeout/SDK exception (not a genai_errors type at
    all) raised by the external generate_content(...) call must still be
    bounded-retried, then converted - never left to propagate raw."""
    monkeypatch.setattr("app.services.gemini_service.sleep", lambda seconds: None)

    with patch(
        "app.services.gemini_service._client.models.generate_content",
        side_effect=[ConnectionError("connection reset"), ConnectionError("connection reset"), ConnectionError("connection reset")],
    ) as generate_content:
        with pytest.raises(GeminiUnavailableError):
            generate_investigation_explanation({"analysis_id": 1})

    assert generate_content.call_count == 3


def test_generate_investigation_explanation_transport_error_then_success(monkeypatch):
    """A transport exception followed by a successful call within the
    retry budget must return the real, valid result."""
    monkeypatch.setattr("app.services.gemini_service.sleep", lambda seconds: None)

    with patch(
        "app.services.gemini_service._client.models.generate_content",
        side_effect=[TimeoutError("timed out"), _mock_response()],
    ) as generate_content:
        result = generate_investigation_explanation({"analysis_id": 1})

    assert isinstance(result, GeminiInvestigationResponse)
    assert generate_content.call_count == 2


# --- 9/10: response validation ------------------------------------------


def test_malformed_json_response_raises_unavailable_not_retried(monkeypatch):
    """A response body that isn't even valid JSON must not be retried in
    this V1 - it converts straight to GeminiUnavailableError."""
    sleep_calls = []
    monkeypatch.setattr("app.services.gemini_service.sleep", lambda seconds: sleep_calls.append(seconds))

    with patch(
        "app.services.gemini_service._client.models.generate_content",
        return_value=_mock_response(text="not valid json{{{"),
    ) as generate_content:
        with pytest.raises(GeminiUnavailableError):
            generate_investigation_explanation({"analysis_id": 1})

    generate_content.assert_called_once()
    assert sleep_calls == []


def test_schema_invalid_json_response_raises_unavailable_not_retried(monkeypatch):
    """Valid JSON that does not satisfy GeminiInvestigationResponse's
    schema (e.g. missing required fields) must also convert to
    GeminiUnavailableError without being retried, and without loosening
    the schema or accepting a partial result."""
    sleep_calls = []
    monkeypatch.setattr("app.services.gemini_service.sleep", lambda seconds: sleep_calls.append(seconds))

    with patch(
        "app.services.gemini_service._client.models.generate_content",
        return_value=_mock_response(text='{"title": "t"}'),
    ) as generate_content:
        with pytest.raises(GeminiUnavailableError):
            generate_investigation_explanation({"analysis_id": 1})

    generate_content.assert_called_once()
    assert sleep_calls == []


def test_lazy_client_initialization_failure_raises_unavailable(monkeypatch):
    monkeypatch.setattr(gemini_service._client, "_resolved_client", None)

    def fail_client_initialization(**_kwargs):
        raise RuntimeError("SDK initialization failed")

    monkeypatch.setattr(gemini_service.genai, "Client", fail_client_initialization)

    with pytest.raises(GeminiUnavailableError, match="SDK initialization failed"):
        generate_investigation_explanation({"analysis_id": 1})


def test_throwing_response_text_property_raises_unavailable_without_retry(monkeypatch):
    sleep_calls = []
    monkeypatch.setattr(
        "app.services.gemini_service.sleep",
        lambda seconds: sleep_calls.append(seconds),
    )
    response = MagicMock()
    type(response).text = PropertyMock(
        side_effect=RuntimeError("response object could not decode text")
    )

    with patch(
        "app.services.gemini_service._client.models.generate_content",
        return_value=response,
    ) as generate_content:
        with pytest.raises(GeminiUnavailableError, match="response object"):
            generate_investigation_explanation({"analysis_id": 1})

    generate_content.assert_called_once()
    assert sleep_calls == []


# --- 11: successful valid response remains unchanged --------------------


def test_successful_valid_response_returns_parsed_result_unchanged():
    with patch(
        "app.services.gemini_service._client.models.generate_content",
        return_value=_mock_response(),
    ) as generate_content:
        result = generate_investigation_explanation({"analysis_id": 1})

    assert isinstance(result, GeminiInvestigationResponse)
    assert result.title == "t"
    assert result.summary == "s"
    generate_content.assert_called_once()


# --- 12: automatic function calling remains disabled (also covered above) -


def test_automatic_function_calling_disabled_even_after_a_retry(monkeypatch):
    monkeypatch.setattr("app.services.gemini_service.sleep", lambda seconds: None)

    with patch(
        "app.services.gemini_service._client.models.generate_content",
        side_effect=[_server_error(), _mock_response()],
    ) as generate_content:
        generate_investigation_explanation({"analysis_id": 1})

    _, kwargs = generate_content.call_args
    assert kwargs["config"].automatic_function_calling.disable is True


def test_unconfigured_gemini_is_optional_and_never_constructs_sdk_client(monkeypatch):
    monkeypatch.setattr(gemini_service.Settings, "GEMINI_API_KEY", None)
    monkeypatch.setattr(gemini_service._client, "_resolved_client", None)
    client_constructor = MagicMock()
    monkeypatch.setattr(gemini_service.genai, "Client", client_constructor)

    with pytest.raises(GeminiUnavailableError, match="not configured"):
        generate_investigation_explanation({"analysis_id": 1})

    client_constructor.assert_not_called()


def test_gemini_request_redacts_obvious_secrets_without_mutating_context():
    context = {
        "analysis_id": 1,
        "evidence": [
            {
                "representative_line": (
                    "Authorization: Bearer abcdefghijklmnopqrstuvwxyz"
                ),
                "attributes": {
                    "api_key": "super-secret-key",
                    "password": "database-password",
                    "safe_field": "database timeout",
                },
            }
        ],
        "source": {
            "snippet": (
                "url=mysql://alice:s3cr3t@example.com/db?"
                "token=query-secret&mode=rw\n"
                "client_secret='source-secret'\n"
                "AKIAABCDEFGHIJKLMNOP "
                "ghp_ABCDEFGHIJKLMNOPQRSTUVWXYZ123456 "
                "sk-abcdefghijklmnopqrstuvwxyz123456\n"
                "raise RuntimeError('boom')"
            ),
        },
        # Deliberately NOT PII redaction.
        "contact": "alice@example.com",
    }
    original = deepcopy(context)

    with patch(
        "app.services.gemini_service._client.models.generate_content",
        return_value=_mock_response(),
    ) as generate_content:
        generate_investigation_explanation(context)

    _, kwargs = generate_content.call_args
    sent = kwargs["contents"]
    parsed = json.loads(sent)

    for secret in (
        "abcdefghijklmnopqrstuvwxyz",
        "super-secret-key",
        "s3cr3t",
        "query-secret",
        "database-password",
        "source-secret",
        "AKIAABCDEFGHIJKLMNOP",
        "ghp_ABCDEFGHIJKLMNOPQRSTUVWXYZ123456",
        "sk-abcdefghijklmnopqrstuvwxyz123456",
    ):
        assert secret not in sent

    assert "[REDACTED]" in sent
    assert parsed["evidence"][0]["attributes"]["safe_field"] == "database timeout"
    assert "mode=rw" in parsed["source"]["snippet"]
    assert "raise RuntimeError('boom')" in parsed["source"]["snippet"]
    # We explicitly are NOT building a general PII scrubber in this project.
    assert parsed["contact"] == "alice@example.com"
    # Gemini gets a redacted COPY. Deterministic context remains intact.
    assert context == original


def test_gemini_redaction_is_narrow_and_does_not_scrub_benign_security_words():
    context = {
        "evidence": [
            {
                "representative_line": (
                    "token bucket exhausted; "
                    "password authentication failed; "
                    "secret rotation job completed"
                )
            }
        ]
    }

    with patch(
        "app.services.gemini_service._client.models.generate_content",
        return_value=_mock_response(),
    ) as generate_content:
        generate_investigation_explanation(context)

    _, kwargs = generate_content.call_args
    sent = json.loads(kwargs["contents"])

    assert sent["evidence"][0]["representative_line"] == (
        "token bucket exhausted; "
        "password authentication failed; "
        "secret rotation job completed"
    )
