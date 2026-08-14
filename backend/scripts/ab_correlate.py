"""Interleaved A/B: old Path-keyed context cache vs new str-keyed context
cache in SourceIndex.context_lines / correlate_event, alternating within one
process so system load drift cancels out.
"""
import statistics
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.core.processing_config import MAX_SOURCE_CONTEXT_FILE_BYTES, SOURCE_CONTEXT_LINES  # noqa: E402
from app.services.log_praser import ParsedEvent, StackFrame  # noqa: E402
from app.services.source_index import build_index  # noqa: E402
import app.services.source_index as si  # noqa: E402

SCRATCH = Path("/tmp/claude-1000/-home-muthu-code-Devflo/3abd434c-c866-4d80-9ffe-ce320c8e1ffa/scratchpad")
ROOT = SCRATCH / "synthetic_extracted"

N_DIRS = 40
FILES_PER_DIR = 50
LINES_PER_FILE = 120

_MISSING = object()


def old_context_lines(index, path: Path):
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


def old_read_context(index, path: Path, line_number):
    if not line_number or line_number < 1:
        return None, None, None
    lines = old_context_lines(index, path)
    if lines is None:
        return None, None, None
    start = max(line_number - SOURCE_CONTEXT_LINES, 1)
    end = min(line_number + SOURCE_CONTEXT_LINES, len(lines))
    if start > end:
        return None, None, None
    return "\n".join(lines[start - 1: end]), start, end


def old_build_match(index, relative_path, requested_path, frame, method):
    line_number = getattr(frame, "line", None)
    snippet, start, end = old_read_context(index, index.root / relative_path, line_number)
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


def old_match_frame(frame, index, module):
    import posixpath
    normalized = posixpath.normpath(frame.file.replace("\\", "/")).lstrip("./") if frame.file else None
    if normalized:
        if normalized in index.by_path:
            return old_build_match(index, normalized, normalized, frame, "exact")
        parts = normalized.split("/")
        for start in range(1, len(parts)):
            candidates = index.by_suffix.get("/".join(parts[start:]))
            if candidates:
                if len(candidates) != 1:
                    return None
                method = "basename" if start == len(parts) - 1 else "suffix"
                return old_build_match(index, candidates[0], normalized, frame, method)
    stem = (module or "").rsplit(".", 1)[-1] or None
    candidates = index.by_stem.get(stem) if stem else None
    if candidates and len(candidates) == 1:
        return old_build_match(index, candidates[0], normalized or stem, frame, "module")
    return None


def old_correlate_event(event, index):
    if index is None:
        return []
    module = getattr(event, "module", None)
    frames = getattr(event, "stack_frames", None) or []
    return [m for frame in frames if (m := old_match_frame(frame, index, module))]


unique_events = [
    ParsedEvent(
        line_number=i,
        raw_line=f"error {i}",
        stack_frames=[
            StackFrame(file=f"module_{i % N_DIRS}/file_{i % FILES_PER_DIR}.py", line=(i % LINES_PER_FILE) + 1, function="fn_x")
        ],
    )
    for i in range(20000)
]
hot_events = [
    ParsedEvent(
        line_number=i,
        raw_line=f"error {i}",
        stack_frames=[StackFrame(file="module_0/worker.py", line=(i % 480) + 1, function="run_x")],
    )
    for i in range(20000)
]


def run_old_unique():
    index = build_index(ROOT)
    for e in unique_events:
        old_correlate_event(e, index)


def run_new_unique():
    index = build_index(ROOT)
    for e in unique_events:
        si.correlate_event(e, index)


def run_old_hot():
    index = build_index(ROOT)
    for e in hot_events:
        old_correlate_event(e, index)


def run_new_hot():
    index = build_index(ROOT)
    for e in hot_events:
        si.correlate_event(e, index)


for scenario, old_fn, new_fn in (("unique", run_old_unique, run_new_unique), ("hot", run_old_hot, run_new_hot)):
    results = {"old": [], "new": []}
    order = ["old", "new"] * 6
    for label in order:
        fn = old_fn if label == "old" else new_fn
        t0 = time.perf_counter()
        fn()
        dt = time.perf_counter() - t0
        results[label].append(dt)
        print(f"{scenario}/{label}: {dt:.4f}s")
    old_med = statistics.median(results["old"])
    new_med = statistics.median(results["new"])
    print(f"{scenario}: old_median={old_med:.4f}s new_median={new_med:.4f}s delta={old_med - new_med:+.4f}s ({(old_med - new_med) / old_med * 100:+.1f}%)\n")
