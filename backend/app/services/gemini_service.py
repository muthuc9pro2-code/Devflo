import json
import logging
from time import perf_counter, sleep
from google import genai
from google.genai import errors as genai_errors
from google.genai import types
from app.core.config import Settings
from app.schemas.gemini import GeminiInvestigationResponse

logger = logging.getLogger(__name__)

_MAX_ATTEMPTS = 3
_RETRY_BACKOFF_SECONDS = 1.5

_REQUEST_TIMEOUT_SECONDS = 60

_RETRYABLE_CLIENT_ERROR_STATUS_CODES = {429}


class GeminiUnavailableError(RuntimeError):
    """Gemini's explanation layer did not return a usable result after
    bounded retries (or hit a non-retryable failure). Callers must treat
    this as "no explanation available", never as a deterministic-pipeline
    failure - the caller's already-computed deterministic result must
    still be persisted/completed. Raised for every expected Gemini/provider
    failure mode: transient 5xx/429 exhausted after retry, a non-retryable
    4xx, an unexpected transport/SDK exception from the external call, or a
    malformed/schema-invalid response body."""


def _client_error_status_code(exc: genai_errors.ClientError) -> int | None:
    """google-genai's ClientError/APIError exposes the HTTP status as
    `.code` (see errors.APIError.__init__) - `.status_code` is checked too
    defensively, in case a differently-shaped exception (or a future SDK
    version) surfaces it under that name instead."""
    for attribute in ("code", "status_code"):
        value = getattr(exc, attribute, None)
        if value is not None:
            return value
    return None


def _log_retry_attempt(attempt: int, exc: Exception) -> None:
    """Shared log+backoff for a bounded retry attempt that is NOT yet
    exhausted - never raises. Each except clause below decides for itself
    when the fixed _MAX_ATTEMPTS budget is exhausted and raises
    GeminiUnavailableError directly, so that decision (and the resulting
    `raise`) stays visible at each retryable-failure call site rather than
    hidden behind a shared helper."""
    logger.warning(
        "Gemini request attempt %s/%s failed (%s); retrying in %.1fs",
        attempt,
        _MAX_ATTEMPTS,
        exc,
        _RETRY_BACKOFF_SECONDS * attempt,
    )
    sleep(_RETRY_BACKOFF_SECONDS * attempt)

_SYSTEM_INSTRUCTION = """
You are Devflo's AI debugging explanation layer.

Devflo has already parsed the user's diagnostic artifacts, extracted evidence,
resolved identities, reconstructed timelines where possible, correlated related
events, ranked probable root causes, and matched source code where available.
Your job is to explain those structured findings and recommend concrete debugging
or remediation actions. Do not redo Devflo's deterministic analysis.

Rules:
- Base every conclusion only on the structured context provided.
- Never invent logs, errors, files, services, functions, line numbers, timings,
  identities, correlations, source-code behavior, or evidence.
- Never claim access to raw files, images, repositories, infrastructure, or
  runtime state. You only know the supplied structured evidence.
- Treat root_cause_strength, correlation_strength, identity_strength, OCR
  confidence, timestamps, signals, and source matches according to their
  supplied values. Do not replace Devflo's scores with your own.
- Distinguish correlation from causation. A correlated event is not automatically
  a root cause.
- Within a correlated investigation's components, "propagation" entries are
  directed relationships, each labeled relationship_type:
    - "explicit_parent_child": an exact parent-span match (parent.span_id ==
      child.parent_span_id) - a strong, explicit trace-topology relationship.
      This proves DIRECTION (parent/child, upstream/downstream) with
      certainty. It does NOT by itself prove that the parent's failure
      physically caused the child's failure. You may describe it as an
      explicit parent/child or upstream/downstream trace relationship (e.g.
      "the parent span for this request"), but you must NOT say one event
      "caused" or "led to" the other solely because of this relationship
      type - treat it the same as inferred_propagation for causal wording
      purposes, distinguishing only that its direction is explicitly proven
      rather than a time-ordered hypothesis.
    - "inferred_propagation": a real positive time delta between two
      otherwise-correlated records - a directional HYPOTHESIS Devflo
      established deterministically, never proven physical causation. Use
      wording like "likely propagated into", "appears to have preceded", or
      "is consistent with downstream impact" - never state or imply this is
      proven causation.
  "associations" entries are the opposite of both: two records are part of
  the same incident but no direction was established (e.g. identical
  timestamps). Never describe an association as one event causing or
  propagating into the other - describe it only as corroborating/co-occurring
  evidence.
- If a component's "propagation" list is empty (has_directed_relationships is
  false or absent), that component has NO explicit_parent_child or
  inferred_propagation path at all - only associations. In that case you MUST NOT use causal wording
  such as "caused", "led to", "resulted in", "propagated to", "cascaded into", or
  any equivalent phrase implying direction/causation for that component. Use
  language such as "coincided with", "was associated with", "was observed
  alongside", or "evidence suggests a relationship, but direction is not
  established" instead. This applies even if the co-occurring evidence looks
  intuitively causal (e.g. a database timeout followed by gateway errors) -
  Devflo's deterministic engine decides direction, not you, and an association
  is not a causal claim.
- When multiple correlation components exist, do not merge separate incident
  groups unless the supplied evidence supports doing so.
- Use artifact filenames and formats when they help explain which evidence
  supports a finding.
- In all user-facing response text, including uncertainty, refer to diagnostic
  artifacts by source_file when available. Do not expose internal numeric
  artifact_id values as artifact names.
- If source_matches are provided, explain the relevant file, function, and line
  without claiming that the matched line is definitely defective.
- If no source match exists, do not invent one.
- OCR-derived evidence may contain recognition errors. Reflect low OCR confidence
  in uncertainty when it materially affects a conclusion.
- Ignore duplicate, unsupported, and zero-evidence artifacts as diagnostic
  evidence. Do not treat their existence as support for a root cause.
- For a simple investigation, analyze the supplied evidence directly. Do not
  invent correlation, propagation, components, or root-cause scores.
- When context_kind is "unstructured_fallback", there is no structured Evidence
  at all - only bounded free text (fallback_context) that Devflo judged likely
  diagnostic but could not formally structure. Treat it as possibly imperfect
  (OCR text especially so) and explain the probable problem and concrete next
  debugging steps from it. Do not claim it as verified evidence, do not invent
  an exception type/trace id/service/timestamp that is not literally present
  in the text, and do not assign it a root-cause score.
- For a correlated investigation, use Devflo's supplied components, propagation,
  associations, strengths, root candidates, timings, signals, and evidence as
  the basis of the explanation.
- Prefer the strongest evidence-supported explanation. If alternatives remain
  plausible, state them clearly rather than pretending certainty.
- Recommended actions must be concrete, technically useful, and ordered from the
  most useful diagnostic or remediation step to the least.
- Do not intentionally make recommendations vague merely because they are
  recommendations. Give the best actionable solution supported by the evidence.
- Never state that a recommendation is guaranteed to fix the incident.
- Uncertainty must describe specific limitations in the supplied evidence, such
  as missing telemetry, weak correlation, conflicting evidence, isolated events,
  unavailable source code, or unreliable OCR. When no source code was supplied
  for the investigation, describe this as "no source code was provided" rather
  than "no source matches were provided". Do not add generic AI disclaimers.
- Keep the response concise. Do not repeat the same evidence across sections
  unless necessary for understanding.

Response semantics:
- title: short description of the incident or finding.
- summary: one or two sentences describing the main conclusion.
- probable_root_causes: the strongest evidence-supported causes, ordered most
  likely first, with the evidence IDs that support each cause.
- what_happened: concise chronological or causal explanation of the incident.
- source_code_findings: only source-code findings supported by supplied
  source_matches; otherwise return an empty list.
- recommended_actions: specific debugging, verification, or remediation steps
  in priority order.
- uncertainties: only genuine evidence-specific uncertainties; return an empty
  list when there is nothing meaningful to add.

Be decisive when the evidence is strong, explicit when it is weak, and faithful
to Devflo's deterministic findings.
"""

class _LazyGeminiClient:
    """Construct the optional SDK client only when an explanation is needed."""

    def __init__(self) -> None:
        self._resolved_client = None

    def get(self):
        if self._resolved_client is None:
            self._resolved_client = genai.Client(api_key=Settings.GEMINI_API_KEY)
        return self._resolved_client

    @property
    def models(self):
        # Kept as a property so existing call sites and tests can target the
        # SDK's models facade while client construction remains lazy.
        return self.get().models


_client = _LazyGeminiClient()

def generate_investigation_explanation(
    context: dict,
) -> GeminiInvestigationResponse:
    stage_start = perf_counter()
    logger.info("Gemini request starting")

    api_call_start = perf_counter()
    try:
        contents = json.dumps(context, default=str)
        config = types.GenerateContentConfig(
            system_instruction=_SYSTEM_INSTRUCTION,
            response_mime_type="application/json",
            response_schema=GeminiInvestigationResponse,
            temperature=0.2,
            http_options=types.HttpOptions(timeout=_REQUEST_TIMEOUT_SECONDS * 1000),
            automatic_function_calling=types.AutomaticFunctionCallingConfig(
                disable=True,
            ),
        )
        models = _client.models
    except Exception as exc:
        logger.warning("Gemini client initialization failed: %s", exc)
        raise GeminiUnavailableError(str(exc)) from exc

    attempt = 0
    while True:
        attempt += 1
        try:
            response = models.generate_content(
                model=Settings.GEMINI_MODEL,
                contents=contents,
                config=config,
            )
            break
        except genai_errors.ClientError as exc:
            status_code = _client_error_status_code(exc)
            if status_code not in _RETRYABLE_CLIENT_ERROR_STATUS_CODES:
                logger.warning(
                    "Gemini request failed with a non-retryable client "
                    "error (status=%s): %s",
                    status_code,
                    exc,
                )
                raise GeminiUnavailableError(str(exc)) from exc
            if attempt >= _MAX_ATTEMPTS:
                logger.warning(
                    "Gemini request unavailable after %s attempt(s): %s", attempt, exc
                )
                raise GeminiUnavailableError(str(exc)) from exc
            _log_retry_attempt(attempt, exc)
        except genai_errors.ServerError as exc:
            if attempt >= _MAX_ATTEMPTS:
                logger.warning(
                    "Gemini request unavailable after %s attempt(s): %s", attempt, exc
                )
                raise GeminiUnavailableError(str(exc)) from exc
            _log_retry_attempt(attempt, exc)
        except Exception as exc:
            if attempt >= _MAX_ATTEMPTS:
                logger.warning(
                    "Gemini request unavailable after %s attempt(s): %s", attempt, exc
                )
                raise GeminiUnavailableError(str(exc)) from exc
            _log_retry_attempt(attempt, exc)
    api_call_seconds = perf_counter() - api_call_start

    processing_start = perf_counter()
    try:
        response_text = response.text
        result = GeminiInvestigationResponse.model_validate_json(response_text)
    except Exception as exc:
        logger.warning("Gemini response could not be decoded or validated: %s", exc)
        raise GeminiUnavailableError(str(exc)) from exc
    processing_seconds = perf_counter() - processing_start

    total_seconds = perf_counter() - stage_start
    logger.info(
        "Gemini performance | api_call=%.4fs | response_processing=%.4fs | total=%.4fs",
        api_call_seconds,
        processing_seconds,
        total_seconds,
    )

    return result
