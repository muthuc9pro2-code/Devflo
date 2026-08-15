"""Interleaved A/B: OLD (duplicate regex in gate+normalize) vs NEW (match
once, reuse) for web_server, syslog, serverless - alternating within one
process so system load drift cancels out. Uses the real current fixtures
and the real current normalize_text_event()/level helpers; only the
gate+normalize entry points are reconstructed to the pre-change shape.
"""
import statistics
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import app.services.diagnostic_adapters as da  # noqa: E402
from app.services.artifact_detector import detect_artifact  # noqa: E402
from app.services.diagnostic_parser import level_from_http_status, normalize_level, normalize_text_event, parse_timestamp  # noqa: E402
from app.services.artifact_detector import ArtifactFormat  # noqa: E402

FIXTURE_DIR = Path(__file__).resolve().parents[1] / "tests/fixtures/bench"


# ---- OLD reconstructions (duplicate regex, no match reuse) ----

def old_web_server_may_be_important(raw_text: str) -> bool:
    access_match = da.WEB_ACCESS_RE.search(raw_text)
    if access_match:
        return level_from_http_status(int(access_match.group('status'))) in da.IMPORTANT_LEVELS
    error_match = da.WEB_ERROR_RE.search(raw_text) or da.APACHE_ERROR_RE.search(raw_text)
    if error_match:
        return normalize_level(error_match.group('level')) in da.IMPORTANT_LEVELS
    return da._generic_text_may_be_important(raw_text)


def old_normalize_web_event(raw_text, line_number, source_file):
    access_match = da.WEB_ACCESS_RE.search(raw_text)
    defaults = {}
    if access_match:
        status = int(access_match.group('status'))
        defaults.update(timestamp=access_match.group('time'), host=access_match.group('host'), endpoint=access_match.group('endpoint'), http_status=status, level=level_from_http_status(status))
    else:
        error_match = da.WEB_ERROR_RE.search(raw_text) or da.APACHE_ERROR_RE.search(raw_text)
        if error_match:
            defaults['timestamp'] = parse_timestamp(error_match.group('time').replace('/', '-', 2))
            defaults['level'] = normalize_level(error_match.group('level'))
    return normalize_text_event(raw_text, line_number, source_file=source_file, source_format=ArtifactFormat.WEB_SERVER.value, defaults=defaults)


def old_syslog_may_be_important(raw_text: str) -> bool:
    match = da.SYSLOG_5424_RE.search(raw_text) or da.SYSLOG_3164_RE.search(raw_text)
    if match:
        return da._syslog_level(int(match.group('pri')) % 8) in da.IMPORTANT_LEVELS
    return da._generic_text_may_be_important(raw_text)


def old_normalize_syslog_event(raw_text, line_number, source_file):
    match = da.SYSLOG_5424_RE.search(raw_text) or da.SYSLOG_3164_RE.search(raw_text)
    defaults = {}
    if match:
        defaults.update(timestamp=match.group('time'), level=da._syslog_level(int(match.group('pri')) % 8), host=match.group('host'), service=match.group('app'))
    return normalize_text_event(raw_text, line_number, source_file=source_file, source_format=ArtifactFormat.SYSLOG.value, defaults=defaults)


def old_serverless_may_be_important(raw_text: str) -> bool:
    if da.LAMBDA_LIFECYCLE_RE.search(raw_text):
        return False
    application = da.LAMBDA_APPLICATION_RE.search(raw_text)
    if application:
        return normalize_level(application.group('level')) in da.IMPORTANT_LEVELS
    return da._generic_text_may_be_important(raw_text)


def old_normalize_serverless_event(raw_text, line_number, source_file):
    defaults = {}
    lifecycle = da.LAMBDA_LIFECYCLE_RE.search(raw_text)
    application = da.LAMBDA_APPLICATION_RE.search(raw_text)
    if lifecycle:
        defaults.update(request_id=lifecycle.group('request_id'), level='INFO')
    elif application:
        defaults.update(timestamp=application.group('time'), request_id=application.group('request_id'), level=application.group('level'))
    if da.EXPLICIT_SERVICE_RE.search(raw_text) is None:
        defaults['service'] = 'aws-lambda'
    return normalize_text_event(raw_text, line_number, source_file=source_file, source_format=ArtifactFormat.SERVERLESS.value, defaults=defaults)


def run_old(fmt_name, records_text):
    if fmt_name == 'web_server':
        gate, norm = old_web_server_may_be_important, old_normalize_web_event
    elif fmt_name == 'syslog':
        gate, norm = old_syslog_may_be_important, old_normalize_syslog_event
    else:
        gate, norm = old_serverless_may_be_important, old_normalize_serverless_event
    count = 0
    for text in records_text:
        if gate(text):
            norm(text, 1, 'f')
            count += 1
    return count


def run_new(fmt_name, records_text):
    if fmt_name == 'web_server':
        match_fn, gate, norm = da._match_web_server_line, da._web_server_may_be_important, da._normalize_web_event
    elif fmt_name == 'syslog':
        match_fn, gate, norm = da._match_syslog_line, da._syslog_may_be_important, da._normalize_syslog_event
    else:
        match_fn, gate, norm = da._match_serverless_line, da._serverless_may_be_important, da._normalize_serverless_text_event
    count = 0
    for text in records_text:
        match = match_fn(text)
        if gate(text, match=match):
            norm(text, 1, 'f', match=match)
            count += 1
    return count


FIXTURES = {
    'web_server': 'web_server_10mib.log',
    'syslog': 'syslog_10mib.log',
    'serverless': 'serverless_10mib.log',
}

for fmt_name, fixture_name in FIXTURES.items():
    path = FIXTURE_DIR / fixture_name
    fmt = detect_artifact(path, filename=path.name)
    lines = path.read_text(errors='replace').splitlines()
    # cap to keep the A/B loop fast while still representative
    lines = lines[:60000]

    results = {"old": [], "new": []}
    order = ["old", "new"] * 5
    for label in order:
        t0 = time.perf_counter()
        if label == "old":
            run_old(fmt_name, lines)
        else:
            run_new(fmt_name, lines)
        results[label].append(time.perf_counter() - t0)

    old_med = statistics.median(results["old"])
    new_med = statistics.median(results["new"])
    print(f"{fmt_name}: old_median={old_med:.4f}s new_median={new_med:.4f}s delta={(old_med - new_med) / old_med * 100:+.1f}% old_all={[f'{v:.3f}' for v in results['old']]} new_all={[f'{v:.3f}' for v in results['new']]}")
