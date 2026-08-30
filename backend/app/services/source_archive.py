"""Safe acquisition of optional investigation source code (GitHub URL or ZIP).

Never executes cloned/extracted content; only copies bytes into
investigation-scoped storage and hands off to source_index.build_index.
"""

import os
import posixpath
import re
import shutil
import stat
import subprocess
import uuid
import zipfile
import zlib
from pathlib import Path

from app.core.processing_config import (
    GITHUB_CLONE_TIMEOUT_SECONDS,
    MAX_SOURCE_FILES,
    MAX_SOURCE_TOTAL_BYTES,
    SOURCE_STORAGE_ROOT,
)
from app.services.source_index import (
    build_index,
    index_manifest_path,
    load_index_manifest,
    save_index_manifest,
)

_GITHUB_URL = re.compile(r"^https://github\.com/([\w.-]+)/([\w.-]+?)(?:\.git)?/?$")

class SourceInputError(ValueError):
    pass


class SourceSubsystemError(RuntimeError):
    """A failure contained within optional source acquisition/indexing."""

def validate_github_url(url: str) -> str:
    match = _GITHUB_URL.match((url or "").strip())
    if not match:
        raise SourceInputError("Only HTTPS github.com repository URLs are supported")
    owner, repo = match.groups()
    return f"https://github.com/{owner}/{repo}"

def _safe_members(zf: zipfile.ZipFile):
    infos = zf.infolist()
    if len(infos) > MAX_SOURCE_FILES:
        raise SourceInputError("Source ZIP contains too many entries")
    total = 0
    for member in infos:
        relative = posixpath.normpath(member.filename.replace("\\", "/"))
        if relative == ".." or relative.startswith(("/", "../")) or ":" in relative:
            raise SourceInputError(f"Unsafe path in source ZIP: {member.filename}")
        if stat.S_ISLNK(member.external_attr >> 16):
            raise SourceInputError(f"Symlink entries are not supported: {member.filename}")
        total += member.file_size
        if total > MAX_SOURCE_TOTAL_BYTES:
            raise SourceInputError("Source ZIP exceeds the extracted size limit")
        yield member, relative

def validate_source_zip(path: Path) -> None:
    try:
        with zipfile.ZipFile(path) as zf:
            for _ in _safe_members(zf):
                pass
    except zipfile.BadZipFile as error:
        raise SourceInputError("Uploaded file is not a valid ZIP archive") from error

def _extract_zip(path: Path, dest: Path) -> None:
    dest.mkdir(parents=True, exist_ok=True)
    root = dest.resolve()
    try:
        with zipfile.ZipFile(path) as zf:
            for member, relative in _safe_members(zf):
                if member.is_dir() or not relative or relative == ".":
                    continue
                target = (dest / relative).resolve()
                if root not in target.parents:
                    raise SourceInputError(f"Unsafe path in source ZIP: {member.filename}")
                target.parent.mkdir(parents=True, exist_ok=True)
                with zf.open(member) as source, open(target, "wb") as destination:
                    shutil.copyfileobj(source, destination)
    except (zipfile.BadZipFile, RuntimeError, NotImplementedError, EOFError, zlib.error) as error:
        raise SourceInputError(
            "Uploaded source ZIP is corrupt, encrypted, or uses unsupported compression"
        ) from error

def _validate_cloned_source_tree(root: Path) -> None:
    """Applies the same MAX_SOURCE_FILES / MAX_SOURCE_TOTAL_BYTES contract
    ZIP source already gets per-entry during extraction (_safe_members) to
    a cloned GitHub working tree instead - clone acquisition itself has no
    equivalent per-entry bound, so without this a repository could still
    expand into an oversized (file-count or byte-total) working tree after
    a successful clone. Symlinks (file or directory) and any non-regular
    entry are rejected outright, matching ZIP source's existing symlink
    rejection. Streaming/O(1) in file content: only lstat() metadata is
    ever read here, never file bytes, and symlinks are never followed
    (os.walk(..., followlinks=False) plus lstat rather than stat)."""
    file_count = 0
    total_bytes = 0
    for dirpath, dirnames, filenames in os.walk(root, followlinks=False):
        for name in dirnames:
            entry = Path(dirpath) / name
            if stat.S_ISLNK(entry.lstat().st_mode):
                raise SourceInputError(
                    f"Symlink entries are not supported: {entry.relative_to(root)}"
                )

        for filename in filenames:
            entry = Path(dirpath) / filename
            info = entry.lstat()
            if stat.S_ISLNK(info.st_mode):
                raise SourceInputError(
                    f"Symlink entries are not supported: {entry.relative_to(root)}"
                )
            if not stat.S_ISREG(info.st_mode):
                raise SourceInputError(
                    f"Unsupported filesystem entry: {entry.relative_to(root)}"
                )

            file_count += 1
            if file_count > MAX_SOURCE_FILES:
                raise SourceInputError("Cloned repository contains too many files")

            total_bytes += info.st_size
            if total_bytes > MAX_SOURCE_TOTAL_BYTES:
                raise SourceInputError("Cloned repository exceeds the extracted size limit")


def _clone_github(url: str, dest: Path) -> None:
    dest.mkdir(parents=True, exist_ok=True)
    env = {
        **os.environ,
        "GIT_TERMINAL_PROMPT": "0",
        "GIT_LFS_SKIP_SMUDGE": "1",
    }
    try:
        subprocess.run(
            ["git", "clone", "--depth", "1", "--single-branch", "--no-tags", url, str(dest)],
            check=True,
            capture_output=True,
            timeout=GITHUB_CLONE_TIMEOUT_SECONDS,
            env=env,
        )
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as error:
        raise SourceInputError(f"Could not clone repository: {url}") from error
    
    git_dir = dest / ".git"
    if git_dir.exists():
        shutil.rmtree(git_dir)

    _validate_cloned_source_tree(dest)

def _ready_marker(dest: Path) -> Path:
    return dest.parent / f"{dest.name}.ready"


def _analysis_source_dir(analysis_id: int) -> Path:
    """The one, non-caller-controllable location cleanup_prepared_source()
    is ever allowed to recursively delete: analysis_id is always an int
    (never a caller-supplied path), and this additionally verifies the
    resulting directory resolves to a direct child of SOURCE_STORAGE_ROOT
    before any deletion is attempted."""
    root = Path(SOURCE_STORAGE_ROOT).resolve()
    dest = (root / str(analysis_id)).resolve()
    if dest.parent != root:
        raise ValueError(f"Refusing to clean up outside {root}: {dest}")
    return dest


def cleanup_prepared_source(analysis_id: int) -> None:
    """Idempotently removes every on-disk artifact prepare_source() may
    have produced for one analysis: the prepared source directory itself,
    its ready marker, its index manifest, any in-flight temp manifest left
    by a crashed save_index_manifest(), and any generation-owned temporary
    staging directory (<id>.tmp-<generation>-<uuid>) an acquisition that
    never reached publication may have left behind. Safe to call when none
    of these exist. Only ever called at a genuine terminal point (this
    analysis is durably completed/cancelled/failed - see callers), so it
    is always safe to remove every generation's staging leftovers here,
    not just the current one's: no generation will ever run for this
    analysis again. Genuine filesystem errors (e.g. a permission problem)
    propagate as OSError rather than being swallowed here - callers decide
    whether a cleanup failure is fatal (prepare_source's own failure path)
    or best-effort (finalize's post-completion cleanup)."""
    dest = _analysis_source_dir(analysis_id)
    marker = _ready_marker(dest)
    manifest_path = index_manifest_path(dest)
    tmp_manifest_path = manifest_path.with_name(manifest_path.name + ".tmp")

    if dest.exists():
        shutil.rmtree(dest)
    marker.unlink(missing_ok=True)
    manifest_path.unlink(missing_ok=True)
    tmp_manifest_path.unlink(missing_ok=True)

    for stray_temp_dir in dest.parent.glob(f"{dest.name}.tmp-*"):
        shutil.rmtree(stray_temp_dir, ignore_errors=True)


def _load_or_rebuild_index(dest: Path, manifest_path: Path):
    """Reconstruct a SourceIndex for an already-published, immutable ready
    tree - reads the small JSON manifest cache when possible, otherwise
    rebuilds by walking `dest` itself (still read-only: os.walk() never
    modifies the tree) and refreshes the cache for the next reader. Safe
    for ANY caller, including artifact workers that must never acquire
    source themselves (see load_ready_source_index) - this never clones,
    extracts, or writes anything under `dest` itself, only the sibling
    manifest cache file (already-atomic: temp file + os.replace)."""
    cached = load_index_manifest(manifest_path, dest)
    if cached is not None:
        return cached
    index = build_index(dest)
    save_index_manifest(index, manifest_path)
    return index


def load_ready_source_index(analysis_id: int):
    """READ-ONLY: for artifact workers (see app.tasks.analysis's
    _process_artifact_task) - never clones, extracts, or publishes
    anything. Only ever loads a canonical tree some _prepare_source_task
    invocation has ALREADY durably published (the .ready marker exists,
    which is only ever written after the tree + index + manifest are all
    complete - see prepare_source). Returns None if no ready marker
    exists: the source has not been prepared yet, is still being prepared,
    or was marked unavailable - in every one of those cases the caller
    must proceed with source_index=None rather than attempting to acquire
    source itself. This is the ONLY sanctioned way an artifact worker may
    ever obtain a SourceIndex."""
    dest = Path(SOURCE_STORAGE_ROOT) / str(analysis_id)
    marker = _ready_marker(dest)
    if not marker.exists():
        return None
    manifest_path = index_manifest_path(dest)
    return _load_or_rebuild_index(dest, manifest_path)


def cleanup_generation_source_temp(analysis_id: int, generation: int) -> None:
    """Removes ONLY the generation-owned temporary staging directory
    (<id>.tmp-<generation>-<uuid>) a since-abandoned generation's
    acquisition may have left behind - never the canonical `dest`, ready
    marker, or manifest, which this generation (by definition, since it
    never reached "ready" - see the source_status "preparing" ownership
    model) cannot have published. Used when stale-processing recovery
    demotes a generation that was still "preparing" source, so a new
    generation is not forced to wait for - or, worse, adopt - a stale,
    never-fully-acquired directory left by the one it just fenced.
    Best-effort: filesystem errors are swallowed here, since this is pure
    housekeeping, never a correctness requirement (a left-behind stray
    temp directory is eventually swept by cleanup_prepared_source's own
    broader glob at genuine terminal cleanup)."""
    dest = _analysis_source_dir(analysis_id)
    for stray_temp_dir in dest.parent.glob(f"{dest.name}.tmp-{generation}-*"):
        shutil.rmtree(stray_temp_dir, ignore_errors=True)


def prepare_source(source_kind: str, source_reference: str, analysis_id: int, generation: int):
    """ACQUISITION-CAPABLE: may clone/extract/publish. Reserved for the
    single source-preparation owner (app.tasks.analysis's
    _prepare_source_task, via its own durable "preparing" ownership claim)
    - never called by an artifact worker, which must use
    load_ready_source_index() instead (see item 24 of the source-ownership
    hardening pass this docstring documents).

    Idempotent across a resumed process_analysis run: if this analysis's
    source was already fully acquired by ANY prior invocation (ready
    marker present), skip re-cloning/re-extracting entirely and just
    (re)load its index - without this, resuming a GitHub-sourced analysis
    always fails outright (`git clone` refuses a non-empty destination),
    and resuming a ZIP-sourced one fails once the staged upload has been
    deleted after its first successful use. The marker lives beside
    `dest`, not inside it, so it is never picked up by build_index as a
    source file.

    Generation-owned temporary staging: acquisition (clone/extract) always
    happens into a private, this-call-only temporary directory embedding
    `generation` in its name - never directly into the canonical `dest`.
    Only once acquisition fully succeeds is the temp directory published
    into `dest` with one atomic os.replace() (a rename, not a copy - POSIX
    guarantees this is atomic when both paths are on the same filesystem/
    mount, which they always are here since both live directly under
    SOURCE_STORAGE_ROOT). This is what makes "an old, superseded
    generation's still-running acquisition clobbers a newer generation's
    already-published source" impossible: an old generation can only ever
    delete/replace its OWN temp directory (see its except-block below,
    and the marker re-check immediately before publishing), never the
    canonical `dest` a different, newer execution already finished
    publishing - the marker re-check closes that window at the filesystem
    level, and the caller's own generation-conditional DB claim/transition
    (source_status "preparing" -> "ready", only for the exact generation
    that is still current) closes it at the durable-state level.
    """
    dest = Path(SOURCE_STORAGE_ROOT) / str(analysis_id)
    marker = _ready_marker(dest)
    manifest_path = index_manifest_path(dest)

    if marker.exists():
        return _load_or_rebuild_index(dest, manifest_path)

    temp_dest = dest.parent / f"{dest.name}.tmp-{generation}-{uuid.uuid4().hex}"
    published_by_this_call = False
    try:
        if source_kind == "github":
            _clone_github(source_reference, temp_dest)
        elif source_kind == "zip":
            _extract_zip(Path(source_reference), temp_dest)
        else:
            raise SourceInputError(f"Unsupported source kind: {source_kind}")

        # A different (necessarily newer, since ids/generations only ever
        # advance) execution may have finished publishing while this one
        # was still cloning/extracting - never overwrite an
        # already-published canonical tree with this now-superseded copy.
        # This is a defense-in-depth filesystem-level check alongside the
        # caller's own DB-level generation-conditional claim/transition;
        # either alone already makes the double-publish race exceedingly
        # unlikely, together they close it.
        if marker.exists():
            shutil.rmtree(temp_dest, ignore_errors=True)
            return _load_or_rebuild_index(dest, manifest_path)

        if dest.exists():
            # Not marked ready - a partial tree left by a crashed prior
            # attempt (or by a since-abandoned generation that never
            # reached publication). Never proven complete, so never
            # trusted; always safe to discard and replace.
            shutil.rmtree(dest)
        os.replace(temp_dest, dest)
        published_by_this_call = True

        index = build_index(dest)
        save_index_manifest(index, manifest_path)
        # The ready marker is published LAST, only once the source tree,
        # its index, and the on-disk manifest are all durably complete - a
        # reader that observes the marker can trust the whole prepared
        # state is loadable via the fast path above, with no dependency on
        # this same process ever finishing. Publishing it any earlier would
        # let a crash between the touch and the manifest write leave a
        # marker whose promised state is not actually there yet.
        marker.parent.mkdir(parents=True, exist_ok=True)
        marker.touch()
    except Exception:
        # Always removes THIS call's own private temp directory - a no-op
        # if it was already renamed away. If THIS call is also the one
        # that published `dest` (the os.replace above already succeeded)
        # and index-build/manifest-save then failed, `dest` is this call's
        # own incomplete work, not a canonical tree any newer execution
        # could yet be relying on - safe to remove, but only after one
        # more marker re-check: if some other, newer execution has somehow
        # already published in the meantime, its canonical tree must never
        # be touched.
        shutil.rmtree(temp_dest, ignore_errors=True)
        if published_by_this_call and not marker.exists():
            shutil.rmtree(dest, ignore_errors=True)
        raise
    return index
