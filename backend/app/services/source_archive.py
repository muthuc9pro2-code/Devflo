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
import zipfile
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
        # A symlinked directory is still listed in dirnames even though
        # os.walk (with followlinks=False) will not descend into it - it
        # must be rejected here, not silently skipped, exactly like a ZIP
        # symlink entry is rejected rather than ignored.
        for name in dirnames:
            entry = Path(dirpath) / name
            if stat.S_ISLNK(entry.lstat().st_mode):
                # Report the path relative to the repository root, never the
                # full server-side path (which would leak SOURCE_STORAGE_ROOT's
                # internal on-disk layout into a user-facing failure reason).
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
        # A repository containing Git LFS pointer files must not cause the
        # clone itself to download arbitrarily large LFS objects before
        # Devflo's own source limits ever get a chance to inspect the
        # working tree. Devflo does not support LFS content - this only
        # prevents an unbounded download, it never fetches LFS objects
        # some other way.
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

    # The 500 MiB / 20,000-file source limits apply to the checked-out
    # working tree, not Git's own .git metadata (packfiles, refs, etc.) -
    # removed immediately, before validation, so it can never inflate
    # either count.
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
    its ready marker, its index manifest, and any in-flight temp manifest
    left by a crashed save_index_manifest(). Safe to call when none of
    these exist. Genuine filesystem errors (e.g. a permission problem)
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


def prepare_source(source_kind: str, source_reference: str, analysis_id: int):
    """Acquire source into investigation-scoped storage and reuse/build its
    index.

    Idempotent across a resumed process_analysis run: if this analysis's
    source was already fully acquired by a prior invocation (ready marker
    present), skip re-cloning/re-extracting. Without this, resuming a
    GitHub-sourced analysis always fails outright (`git clone` refuses a
    non-empty destination), and resuming a ZIP-sourced one fails once the
    staged upload has been deleted after its first successful use. The
    marker lives beside `dest`, not inside it, so it is never picked up by
    build_index as a source file.

    Index reuse (final hardening pass, Section 6): every artifact task in
    an analysis previously called this function and got a full
    os.walk()-based build_index() EVERY time, even though the tree never
    changes after the first successful acquisition. A small JSON manifest
    (index_manifest_path) persisted beside `dest` after the first real
    build lets every subsequent call in this - or a LATER, separate worker
    process, since Celery workers are not guaranteed to share memory -
    reconstruct the same index by reading and parsing that manifest
    instead of re-walking the whole tree. Never sent through a Celery
    message (each task calls this locally); never pickled (JSON only).
    """
    dest = Path(SOURCE_STORAGE_ROOT) / str(analysis_id)
    marker = _ready_marker(dest)
    manifest_path = index_manifest_path(dest)

    if marker.exists():
        cached = load_index_manifest(manifest_path, dest)
        if cached is not None:
            return cached
        index = build_index(dest)
        save_index_manifest(index, manifest_path)
        return index

    if dest.exists():
        shutil.rmtree(dest)  # stale/partial remnant of a crashed prior attempt

    try:
        if source_kind == "github":
            _clone_github(source_reference, dest)
        elif source_kind == "zip":
            _extract_zip(Path(source_reference), dest)
        else:
            raise SourceInputError(f"Unsupported source kind: {source_kind}")

        marker.parent.mkdir(parents=True, exist_ok=True)
        marker.touch()
        index = build_index(dest)
        save_index_manifest(index, manifest_path)
    except Exception:
        # Acquisition, validation, index-building, or manifest-writing
        # failed before this analysis's source ever became "ready" (the
        # marker.exists() reuse branch above only ever returns for source
        # that WAS already fully, successfully prepared) - leave no
        # partial prepared tree behind for a later call to stumble over.
        # The staged ZIP upload itself (source_reference for
        # source_kind == "zip") is deliberately left untouched here: it
        # remains useful for a legitimate retry/resume, and is only ever
        # removed by the caller after a successful prepare_source() (see
        # _remove_staged_source_archive in tasks/analysis.py).
        cleanup_prepared_source(analysis_id)
        raise
    return index
