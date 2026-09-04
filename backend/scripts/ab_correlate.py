import posixpath
import statistics
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.core.processing_config import (
    MAX_SOURCE_CONTEXT_FILE_BYTES,
    SOURCE_CONTEXT_LINES,
)
from app.services.log_praser import ParsedEvent, StackFrame
from app.services.source_index import SourceIndex, build_index

SCRATCH = Path(tempfile.gettempdir()) / "devflo-bench-scratch"
ROOT = SCRATCH / "synthetic_extracted"

N_DIRS = 40
FILES_PER_DIR = 50
LINES_PER_FILE = 120

_MISSING = object()

def _legacy_suffix_map(index: SourceIndex) -> dict[str, list[str]]:
    by_suffix: dict[str, list[str]] = {}
    for relative_path in index.by_path:
        parts = relative_path.split("/")
        for start in range(len(parts) - 1):
            by_suffix.setdefault("/".join(parts[start + 1:]), []).append(relative_path)
    return by_suffix

def old_context_lines(index: SourceIndex, path: Path):
    cached = index._context_cache.get(path, _MISSING)
    if cached is not _MISSING:
        return cached
    lines = None
    try:
        size = path.stat().st_size
        if size > MAX_SOURCE_CONTEXT_FILE_BYTES:
            lines = None
        else:
            lines = path.read_text(errors="replace").splitlines()
    except OSError:
        lines = None
    index._context_cache[path] = lines
    return lines

def old_read_context(index: SourceIndex, relative_path: str, line_number):
    if not line_number or line_number < 1:
        return None, None, None
    lines = old_context_lines(index, index.root / relative_path)
    if lines is None:
        return None, None, None
    start = max(line_number - SOURCE_CONTEXT_LINES, 1)
    end = min(line_number + SOURCE_CONTEXT_LINES, len(lines))
    if start > end:
        return None, None, None
    return "\n".join(lines[start - 1: end]), start, end

def new_read_context(index: SourceIndex, relative_path: str, line_number):
    if not line_number or line_number < 1:
        return None, None, None
    lines = index.context_lines(relative_path)
    if lines is None:
        return None, None, None
    start = max(line_number - SOURCE_CONTEXT_LINES, 1)
    end = min(line_number + SOURCE_CONTEXT_LINES, len(lines))
    if start > end:
        return None, None, None
    return "\n".join(lines[start - 1: end]), start, end

def _build_match(index, relative_path, requested_path, frame, method, read_context):
    line_number = getattr(frame, "line", None)
    snippet, start, end = read_context(index, relative_path, line_number)
    return {
        "relative_path": relative_path,
        "requested_path": requested_path,
        "line_number": line_number,
        "function": getattr(frame, "function", None),
        "context_start": start,
        "context_end": end,
        "snippet": snippet,
        "match_method": method,
        "confidence": "high" if method == "exact" else "medium",
    }

def _match_frame(frame, index: SourceIndex, module, by_suffix: dict[str, list[str]], read_context):
    normalized = posixpath.normpath(frame.file.replace("\\", "/")).lstrip("./") if frame.file else None
    if normalized:
        if normalized in index.by_path:
            return _build_match(index, normalized, normalized, frame, "exact", read_context)
        parts = normalized.split("/")
        for start in range(1, len(parts)):
            candidates = by_suffix.get("/".join(parts[start:]))
            if candidates:
                if len(candidates) != 1:
                    return None
                method = "basename" if start == len(parts) - 1 else "suffix"
                return _build_match(index, candidates[0], normalized, frame, method, read_context)
    stem = (module or "").rsplit(".", 1)[-1] or None
    candidates = index.by_stem.get(stem) if stem else None
    if candidates and len(candidates) == 1:
        return _build_match(index, candidates[0], normalized or stem, frame, "module", read_context)
    return None

def _correlate_event(event, index, by_suffix, read_context):
    if index is None:
        return []
    module = getattr(event, "module", None)
    frames = getattr(event, "stack_frames", None) or []
    return [match for frame in frames if (match := _match_frame(frame, index, module, by_suffix, read_context))]

def old_correlate_event(event, index, by_suffix):
    return _correlate_event(event, index, by_suffix, old_read_context)

def new_correlate_event(event, index, by_suffix):
    return _correlate_event(event, index, by_suffix, new_read_context)

unique_events = [
    ParsedEvent(
        line_number=index,
        raw_line=f"error {index}",
        stack_frames=[
            StackFrame(
                file=f"module_{index % N_DIRS}/file_{index % FILES_PER_DIR}.py",
                line=(index % LINES_PER_FILE) + 1,
                function="fn_x",
            )
        ],
    )
    for index in range(20000)
]
hot_events = [
    ParsedEvent(
        line_number=index,
        raw_line=f"error {index}",
        stack_frames=[
            StackFrame(file="module_0/worker.py", line=(index % 480) + 1, function="run_x")
        ],
    )
    for index in range(20000)
]

def _run(events, correlate):
    index = build_index(ROOT)
    by_suffix = _legacy_suffix_map(index)
    for event in events:
        correlate(event, index, by_suffix)

def run_old_unique():
    _run(unique_events, old_correlate_event)

def run_new_unique():
    _run(unique_events, new_correlate_event)

def run_old_hot():
    _run(hot_events, old_correlate_event)

def run_new_hot():
    _run(hot_events, new_correlate_event)

def main() -> None:
    for scenario, old_fn, new_fn in (
        ("unique", run_old_unique, run_new_unique),
        ("hot", run_old_hot, run_new_hot),
    ):
        results = {"old": [], "new": []}
        order = ["old", "new"] * 6
        for label in order:
            fn = old_fn if label == "old" else new_fn
            t0 = time.perf_counter()
            fn()
            dt = time.perf_counter() - t0
            results[label].append(dt)
            print(f"{scenario}/{label}: {dt:.4f}s")
        old_median = statistics.median(results["old"])
        new_median = statistics.median(results["new"])
        delta = old_median - new_median
        percent = delta / old_median * 100 if old_median else 0.0
        print(
            f"{scenario}: old_median={old_median:.4f}s new_median={new_median:.4f}s "
            f"delta={delta:+.4f}s ({percent:+.1f}%)\n"
        )

if __name__ == "__main__":
    main()
