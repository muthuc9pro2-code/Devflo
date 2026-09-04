import json
import logging
import re
from pathlib import Path
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

_REDACTED = "[REDACTED]"

_IDENTIFIER_CAMEL_BOUNDARY = re.compile(r"(?<=[a-z0-9])(?=[A-Z])")
_IDENTIFIER_ACRONYM_BOUNDARY = re.compile(r"(?<=[A-Z])(?=[A-Z][a-z])")
_IDENTIFIER_SEPARATOR = re.compile(r"[^A-Za-z0-9]+")

_SENSITIVE_METADATA_TOKENS = {
    "algorithm", "alg", "available", "bucket", "configured", "count",
    "day", "days", "duration", "enabled", "endpoint", "expire", "expires",
    "expiry", "field", "file", "filename", "header", "hour", "hours",
    "kind", "length", "minute", "minutes", "name", "path", "policy",
    "prefix", "present", "rotated", "rotation", "second", "seconds",
    "secure", "status", "suffix", "timeout", "ttl", "type", "uri", "url",
}
_SENSITIVE_KEY_QUALIFIERS = {
    "access", "api", "auth", "credential", "encryption", "hmac", "jwt",
    "master", "private", "secret", "signing", "webhook",
}
_CONFIG_LIKE_SOURCE_SUFFIXES = {
    ".cfg", ".conf", ".env", ".ini", ".properties", ".toml", ".yaml", ".yml",
}

_QUOTED_KEY_VALUE = re.compile(
    r'(?P<prefix>["\'](?P<key>[A-Za-z_][A-Za-z0-9_.-]*)["\']\s*:\s*)'
    r'(?P<value>"(?:\\.|[^"\\])*"|\'(?:\\.|[^\'\\])*\'|`(?:\\.|[^`\\])*`)'
)
_SENSITIVE_HEADER = re.compile(
    r"(?im)^"
    r"(?P<prefix>\s*(?:authorization|proxy[-_]?authorization|"
    r"cookie|set[-_]?cookie)\s*:\s*)"
    r"(?P<value>[^\r\n]+)$"
)
_ASSIGNMENT_VALUE = re.compile(
    r"(?i)"
    r"(?P<prefix>\b(?P<key>[A-Za-z_][A-Za-z0-9_.-]*)\s*[:=]\s*)"
    r'(?P<value>"(?:\\.|[^"\\])*"|\'(?:\\.|[^\'\\])*\'|`(?:\\.|[^`\\])*`|[^\s,;&#]+)'
)
_SOURCE_LITERAL_ASSIGNMENT = re.compile(
    r"(?i)"
    r"(?P<prefix>\b(?P<key>[A-Za-z_][A-Za-z0-9_.-]*)[ \t]*[:=][ \t]*)"
    r'(?P<value>"(?!\"\")(?:\\.|[^"\\])*"|'
    r"'(?!'')(?:\\.|[^'\\])*'|"
    r"`(?:\\.|[^`\\])*`)"
)
_SOURCE_CONFIG_ASSIGNMENT_START = re.compile(
    r"^(?P<prefix>[ \t]*(?:export[ \t]+)?"
    r"(?P<key>[A-Za-z_][A-Za-z0-9_.-]*)[ \t]*[:=][ \t]*)"
    r"(?P<value>.*?)(?P<newline>\r?\n?)$"
)
_SOURCE_TRIPLE_LITERAL_ASSIGNMENT = re.compile(
    r"(?is)(?P<prefix>\b(?P<key>[A-Za-z_][A-Za-z0-9_.-]*)"
    r"[ \t]*[:=][ \t]*)(?P<quote>\"{3}|'{3})(?P<value>.*?)(?P=quote)"
)
_YAML_BLOCK_SCALAR = re.compile(
    r"^[|>](?:[1-9][+-]?|[+-][1-9]?)?$"
)
_BEARER_TOKEN = re.compile(
    r"(?i)\b(bearer\s+)([A-Za-z0-9._~+/=-]{8,})"
)
_URL_USERINFO = re.compile(
    r"(?i)\b([a-z][a-z0-9+.-]*://)([^/\s:@]*):([^@\s/]+)@"
)
_URL_QUERY_PARAMETER = re.compile(
    r"(?i)(?P<prefix>[?&](?P<key>[A-Za-z0-9_.~-]+)=)(?P<value>[^&#\s]+)"
)
_PRIVATE_KEY_BLOCK = re.compile(
    r"-----BEGIN (?P<label>[A-Z0-9 ]*PRIVATE KEY)-----.*?"
    r"-----END (?P=label)-----",
    re.DOTALL,
)
_STANDALONE_SECRET_PATTERNS = (
    re.compile(r"\bAKIA[0-9A-Z]{16}\b"),
    re.compile(r"\bAIza[0-9A-Za-z_-]{20,}\b"),
    re.compile(r"\bgh[pousr]_[0-9A-Za-z]{20,}\b"),
    re.compile(r"\bsk-[0-9A-Za-z_-]{20,}\b"),
    re.compile(r"\b(?:sk|rk)_(?:live|test)_[0-9A-Za-z]{12,}\b"),
    re.compile(r"\bxox[baprs]-[0-9A-Za-z-]{16,}\b"),
    re.compile(r"\bglpat-[0-9A-Za-z_-]{16,}\b"),
    re.compile(
        r"\beyJ[0-9A-Za-z_-]{5,}\.[0-9A-Za-z_-]{5,}\.[0-9A-Za-z_-]{5,}\b"
    ),
)

def _identifier_tokens(name: str) -> tuple[str, ...]:
    separated = _IDENTIFIER_ACRONYM_BOUNDARY.sub(" ", str(name))
    separated = _IDENTIFIER_CAMEL_BOUNDARY.sub(" ", separated)
    separated = _IDENTIFIER_SEPARATOR.sub(" ", separated)
    return tuple(token.lower() for token in separated.split() if token)

def _is_sensitive_name(name: str) -> bool:
    tokens = _identifier_tokens(name)
    if not tokens:
        return False
    token_set = set(tokens)
    if token_set & _SENSITIVE_METADATA_TOKENS:
        return False
    collapsed = "".join(tokens)
    if "authorization" in token_set or "cookie" in token_set:
        return True
    if "password" in token_set or "passwd" in token_set:
        return True
    if "token" in token_set or "secret" in token_set:
        return True
    if "key" in token_set and token_set & _SENSITIVE_KEY_QUALIFIERS:
        return True
    if collapsed.endswith(
        (
            "apikey", "password", "passwd", "accesstoken", "refreshtoken",
            "authtoken", "idtoken", "sessiontoken", "clientsecret",
            "secretkey", "privatekey", "signingkey",
        )
    ):
        return True
    if token_set & {"pass", "pwd"} and token_set & {
        "admin", "database", "db", "login", "mysql", "postgres",
        "postgresql", "redis", "smtp", "user",
    }:
        return True
    return False

def _is_sensitive_query_name(name: str) -> bool:
    tokens = _identifier_tokens(name)
    return (
        _is_sensitive_name(name)
        or "credential" in tokens
        or "signature" in tokens
        or tokens == ("sig",)
    )

def _redacted_value(value: str) -> str:
    if len(value) >= 2 and value[0] in {'"', "'", "`"} and value[-1] == value[0]:
        return f"{value[0]}{_REDACTED}{value[0]}"
    return _REDACTED

def _redact_keyed_value(match: re.Match) -> str:
    if not _is_sensitive_name(match.group("key")):
        return match.group(0)
    return f'{match.group("prefix")}{_redacted_value(match.group("value"))}'

def _redact_query_parameter(match: re.Match) -> str:
    if not _is_sensitive_query_name(match.group("key")):
        return match.group(0)
    return f'{match.group("prefix")}{_REDACTED}'

def _redact_private_key_block(match: re.Match) -> str:
    label = match.group("label")
    return f"-----BEGIN {label}-----\n{_REDACTED}\n-----END {label}-----"

def _redact_high_confidence_secret_shapes(text: str) -> str:
    redacted = _PRIVATE_KEY_BLOCK.sub(_redact_private_key_block, text)
    redacted = _URL_USERINFO.sub(r"\1\2:[REDACTED]@", redacted)
    redacted = _URL_QUERY_PARAMETER.sub(_redact_query_parameter, redacted)
    redacted = _BEARER_TOKEN.sub(r"\1[REDACTED]", redacted)
    for pattern in _STANDALONE_SECRET_PATTERNS:
        redacted = pattern.sub(_REDACTED, redacted)
    return redacted

def _redact_text_for_gemini(text: str) -> str:
    redacted = _QUOTED_KEY_VALUE.sub(_redact_keyed_value, text)
    redacted = _SENSITIVE_HEADER.sub(
        lambda match: f'{match.group("prefix")}{_REDACTED}',
        redacted,
    )
    redacted = _ASSIGNMENT_VALUE.sub(_redact_keyed_value, redacted)
    return _redact_high_confidence_secret_shapes(redacted)

def _is_config_like_source_path(source_path: str | None) -> bool:
    if not source_path:
        return False
    name = Path(source_path).name.lower()
    if name == ".env" or name.startswith(".env."):
        return True
    return Path(name).suffix.lower() in _CONFIG_LIKE_SOURCE_SUFFIXES

def _source_config_rhs_is_reference(value: str) -> bool:
    candidate = value.strip()
    if not candidate:
        return True
    if len(candidate) >= 2 and candidate[0] in {'"', "'", "`"} and candidate[-1] == candidate[0]:
        candidate = candidate[1:-1].strip()
    if candidate.startswith(("${", "$", "%")):
        return True
    return candidate.lower() in {"none", "null", "true", "false"}

def _redact_source_triple_literal(match: re.Match) -> str:
    if not _is_sensitive_name(match.group("key")):
        return match.group(0)
    quote = match.group("quote")
    return f'{match.group("prefix")}{quote}{_REDACTED}{quote}'

def _line_ending(line: str) -> str:
    if line.endswith("\r\n"):
        return "\r\n"
    if line.endswith("\n"):
        return "\n"
    return ""

def _leading_indent(line: str) -> int:
    return len(line) - len(line.lstrip(" \t"))

def _has_unescaped_closing_quote(text: str, quote: str, *, start: int) -> bool:
    escaped = False
    for character in text[start:]:
        if escaped:
            escaped = False
            continue
        if character == "\\":
            escaped = True
            continue
        if character == quote:
            return True
    return False

def _config_value_end(
    lines: list[str], start_index: int, value: str, base_indent: int
) -> int:
    stripped = value.strip()

    for marker in ('"""', "'''"):
        if not stripped.startswith(marker):
            continue
        if marker in stripped[len(marker):]:
            return start_index
        for index in range(start_index + 1, len(lines)):
            if marker in lines[index]:
                return index
        return len(lines) - 1

    if stripped[:1] in {'"', "'", "`"}:
        quote = stripped[0]
        if not _has_unescaped_closing_quote(stripped, quote, start=1):
            for index in range(start_index + 1, len(lines)):
                if _has_unescaped_closing_quote(lines[index], quote, start=0):
                    return index
            return len(lines) - 1

    yaml_marker = stripped.split("#", 1)[0].strip()
    if _YAML_BLOCK_SCALAR.fullmatch(yaml_marker):
        end = start_index
        for index in range(start_index + 1, len(lines)):
            candidate = lines[index].rstrip("\r\n")
            if not candidate.strip():
                end = index
                continue
            if _leading_indent(candidate) <= base_indent:
                break
            end = index
        return end

    if stripped.endswith("\\"):
        end = start_index
        for index in range(start_index + 1, len(lines)):
            end = index
            if not lines[index].rstrip("\r\n").rstrip().endswith("\\"):
                break
        return end

    end = start_index
    for index in range(start_index + 1, len(lines)):
        candidate = lines[index].rstrip("\r\n")
        if not candidate.strip():
            end = index
            continue
        if _leading_indent(candidate) <= base_indent:
            break
        end = index
    return end

def _redact_config_like_source(text: str) -> str:
    lines = text.splitlines(keepends=True)
    output: list[str] = []
    index = 0
    while index < len(lines):
        line = lines[index]
        match = _SOURCE_CONFIG_ASSIGNMENT_START.match(line)
        if match is None or not _is_sensitive_name(match.group("key")):
            output.append(line)
            index += 1
            continue
        value = match.group("value")
        end_index = _config_value_end(lines, index, value, _leading_indent(line))
        if not value.strip():
            if end_index == index:
                output.append(line)
                index += 1
                continue
        elif _source_config_rhs_is_reference(value):
            output.append(line)
            index += 1
            continue
        output.append(f'{match.group("prefix")}{_REDACTED}{match.group("newline")}')
        for skipped in lines[index + 1: end_index + 1]:
            output.append(_line_ending(skipped))
        index = end_index + 1
    return "".join(output)

def _redact_source_text_for_gemini(text: str, *, source_path: str | None = None) -> str:
    redacted = _QUOTED_KEY_VALUE.sub(_redact_keyed_value, text)
    redacted = _SOURCE_TRIPLE_LITERAL_ASSIGNMENT.sub(
        _redact_source_triple_literal, redacted
    )
    if _is_config_like_source_path(source_path):
        redacted = _redact_config_like_source(redacted)
    else:
        redacted = _SOURCE_LITERAL_ASSIGNMENT.sub(_redact_keyed_value, redacted)
    return _redact_high_confidence_secret_shapes(redacted)

def _redact_context_for_gemini(
    value,
    *,
    key: str | None = None,
    in_source_matches: bool = False,
    source_path: str | None = None,
):
    if key is not None and _is_sensitive_name(str(key)):
        if value is None:
            return None
        return _REDACTED
    current_source_matches = in_source_matches or key == "source_matches"
    if isinstance(value, dict):
        current_source_path = source_path
        if current_source_matches:
            relative_path = value.get("relative_path")
            if isinstance(relative_path, str):
                current_source_path = relative_path
        return {
            child_key: _redact_context_for_gemini(
                child_value,
                key=str(child_key),
                in_source_matches=current_source_matches,
                source_path=current_source_path,
            )
            for child_key, child_value in value.items()
        }
    if isinstance(value, list):
        return [
            _redact_context_for_gemini(
                item, in_source_matches=current_source_matches, source_path=source_path
            )
            for item in value
        ]
    if isinstance(value, tuple):
        return tuple(
            _redact_context_for_gemini(
                item, in_source_matches=current_source_matches, source_path=source_path
            )
            for item in value
        )
    if isinstance(value, str):
        if current_source_matches and key == "snippet":
            return _redact_source_text_for_gemini(value, source_path=source_path)
        return _redact_text_for_gemini(value)
    return value

def _redacted_json_default(value) -> str:
    return _redact_text_for_gemini(str(value))

class GeminiUnavailableError(RuntimeError):
    pass

def _client_error_status_code(exc: genai_errors.ClientError) -> int | None:
    for attribute in ("code", "status_code"):
        value = getattr(exc, attribute, None)
        if value is not None:
            return value
    return None

def _log_retry_attempt(attempt: int, exc: Exception) -> None:
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
- source_context, when present, is Devflo's authoritative marker that source
  code was supplied and prepared successfully but no diagnostic evidence matched
  it (status="ready", match_count=0). Do not describe this state as "no source
  code was provided". source_context alone is not a source match and must not
  produce a source_code_findings entry.
- Discuss specific source code only from actual source_matches supplied in the
  context. When source_matches are present, explain the relevant file, function,
  and line without claiming that the matched line is definitely defective. If
  neither source_matches nor source_context is present, do not infer or mention
  source-code availability. Never invent a source match.
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
- Uncertainty must describe only limitations explicitly represented in the
  supplied context, such as weak correlation, conflicting evidence, isolated
  events, context truncation, resource-limited artifacts, or unreliable OCR. Do
  not invent missing telemetry, source-code state, exception details, or other
  absent information merely because it was not supplied in the bounded context.
  Do not add generic AI disclaimers.
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

    def __init__(self) -> None:
        self._resolved_client = None

    def get(self):
        api_key = Settings.GEMINI_API_KEY
        if api_key is None:
            raise GeminiUnavailableError(
                "Gemini is not configured; "
                "deterministic analysis remains available"
            )
        if self._resolved_client is None:
            self._resolved_client = genai.Client(api_key=api_key)
        return self._resolved_client

    @property
    def models(self):
        return self.get().models

_client = _LazyGeminiClient()

def generate_investigation_explanation(
    context: dict,
) -> GeminiInvestigationResponse:
    stage_start = perf_counter()
    logger.info("Gemini request starting")

    api_call_start = perf_counter()
    try:
        contents = json.dumps(
            _redact_context_for_gemini(context),
            default=_redacted_json_default,
        )
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
    except GeminiUnavailableError:
        raise
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
