import json
import logging
from time import perf_counter, sleep
from pydantic import ValidationError
from google import genai
from google.genai import errors as genai_errors
from google.genai import types
from app.core.config import Settings
from app.schemas.gemini import GeminiInvestigationResponse

logger = logging.getLogger(__name__)

# Bounded retry for transient Gemini/provider failures: 5xx (ServerError),
# 429 (ClientError, rate limiting), and unexpected transport/SDK exceptions
# raised by the external generate_content(...) call itself. An ordinary
# non-429 4xx ClientError (bad request, auth, quota) is never retried since
# retrying it cannot succeed - it is converted to GeminiUnavailableError
# immediately instead. 3 total attempts with a short linear backoff is
# enough to ride out a brief outage without materially delaying finalize on
# a real one.
_MAX_ATTEMPTS = 3
_RETRY_BACKOFF_SECONDS = 1.5

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
  incomplete source matches, or unreliable OCR. Do not add generic AI disclaimers.
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

_client = genai.Client(api_key=Settings.GEMINI_API_KEY)

def generate_investigation_explanation(
    context: dict,
) -> GeminiInvestigationResponse:
    stage_start = perf_counter()
    logger.info("Gemini request starting")

    api_call_start = perf_counter()
    contents = json.dumps(context, default=str)
    config = types.GenerateContentConfig(
        system_instruction=_SYSTEM_INSTRUCTION,
        response_mime_type="application/json",
        response_schema=GeminiInvestigationResponse,
        temperature=0.2,
        # Devflo never registers callable tools/functions on this
        # request (no `tools=` above), so the model can never emit a
        # function call for the SDK to automatically dispatch -
        # Automatic Function Calling has nothing to do here. Left
        # enabled (the SDK's default), it still runs its AFC bookkeeping
        # on every call and logs "AFC is enabled with max remote calls"
        # + a recommendation to use Chat.send_message instead, neither
        # of which apply to this tool-less, single-shot request.
        # Disabling it is a config-only change; generate_content() and
        # the response schema/prompt above are unchanged.
        automatic_function_calling=types.AutomaticFunctionCallingConfig(
            disable=True,
        ),
    )

    attempt = 0
    while True:
        attempt += 1
        try:
            response = _client.models.generate_content(
                model=Settings.GEMINI_MODEL,
                contents=contents,
                config=config,
            )
            break
        except genai_errors.ClientError as exc:
            status_code = _client_error_status_code(exc)
            if status_code not in _RETRYABLE_CLIENT_ERROR_STATUS_CODES:
                # An ordinary non-429 4xx (bad request, auth, quota) is not
                # transient - retrying it cannot succeed. Converted
                # immediately so it can never escape into the finalizer as
                # a raw SDK exception.
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
            # Any other exception raised specifically by this external
            # generate_content(...) call (network/timeout/SDK-internal) is
            # contained here too: bounded retry, then GeminiUnavailableError
            # if it still fails. Deliberately not BaseException -
            # KeyboardInterrupt/SystemExit must still propagate normally.
            if attempt >= _MAX_ATTEMPTS:
                logger.warning(
                    "Gemini request unavailable after %s attempt(s): %s", attempt, exc
                )
                raise GeminiUnavailableError(str(exc)) from exc
            _log_retry_attempt(attempt, exc)
    api_call_seconds = perf_counter() - api_call_start

    processing_start = perf_counter()
    try:
        result = GeminiInvestigationResponse.model_validate_json(response.text)
    except ValidationError as exc:
        # Malformed JSON and schema-invalid JSON both raise pydantic's
        # ValidationError here (pydantic v2 wraps JSON decode failures into
        # it too) - never retried in this V1, and never loosened into a
        # partial/best-effort result: malformed AI output must not fail an
        # otherwise-complete deterministic investigation.
        logger.warning("Gemini response failed schema validation: %s", exc)
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