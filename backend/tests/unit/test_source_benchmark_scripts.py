from pathlib import Path
from app.services.log_praser import ParsedEvent, StackFrame
from app.services.source_index import build_index
from scripts import ab_build_index, ab_correlate

def test_historical_build_index_ab_script_is_decoupled_from_production_source_index(
    tmp_path,
):
    for relative_path in (Path("x") / "a" / "main.py", Path("y") / "b" / "worker.py"):
        path = tmp_path / relative_path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("line one\nline two\n")

    old = ab_build_index.old_build_index(tmp_path)
    new = ab_build_index.new_build_index(tmp_path)

    ab_build_index._assert_equivalent(old, new)
    assert set(old.by_path) == {"x/a/main.py", "y/b/worker.py"}
    assert old.by_suffix == new.by_suffix
    assert old.by_stem == new.by_stem

def test_historical_correlate_ab_script_uses_same_legacy_matcher_on_both_sides(
    tmp_path,
):
    source = tmp_path / "repo" / "app" / "main.py"
    source.parent.mkdir(parents=True)
    source.write_text("first\nsecond\nthird\n")

    event = ParsedEvent(
        line_number=1,
        raw_line="ERROR failure",
        stack_frames=[
            StackFrame(file="/build/app/main.py", line=2, function="run")
        ],
    )

    old_index = build_index(tmp_path)
    old_suffixes = ab_correlate._legacy_suffix_map(old_index)
    old_matches = ab_correlate.old_correlate_event(event, old_index, old_suffixes)

    new_index = build_index(tmp_path)
    new_suffixes = ab_correlate._legacy_suffix_map(new_index)
    new_matches = ab_correlate.new_correlate_event(event, new_index, new_suffixes)

    assert old_matches == new_matches
    assert old_matches[0]["relative_path"] == "repo/app/main.py"
    assert old_matches[0]["match_method"] == "suffix"
