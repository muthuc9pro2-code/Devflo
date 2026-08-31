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
    MAX_SOURCE_PATH_DEPTH,
    MAX_SOURCE_RELATIVE_PATH_BYTES,
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


def _validate_source_path_shape(relative: str) -> None:
    parts = [part for part in relative.split("/") if part and part != "."]
    if len(parts) > MAX_SOURCE_PATH_DEPTH:
        raise SourceInputError(
            "Source path depth exceeds the supported index limit"
        )
    if (
        len(relative.encode("utf-8", errors="surrogatepass"))
        > MAX_SOURCE_RELATIVE_PATH_BYTES
    ):
        raise SourceInputError(
            "Source path length exceeds the supported index limit"
        )

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
        _validate_source_path_shape(relative)
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
            relative = entry.relative_to(root).as_posix()
            _validate_source_path_shape(relative)
            if stat.S_ISLNK(entry.lstat().st_mode):
                raise SourceInputError(
                    f"Symlink entries are not supported: {entry.relative_to(root)}"
                )

        for filename in filenames:
            entry = Path(dirpath) / filename
            relative = entry.relative_to(root).as_posix()
            _validate_source_path_shape(relative)
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
    """Idempotently remove every on-disk source artifact for one Analysis.

    This includes the canonical tree/marker/manifest, the old fixed manifest
    temp name, new unique-writer manifest temps, and every generation-owned
    temporary source directory. Called only at a genuine terminal lifecycle
    point, so removing every generation's leftovers is safe.
    """
    dest = _analysis_source_dir(analysis_id)
    marker = _ready_marker(dest)
    manifest_path = index_manifest_path(dest)
    legacy_tmp_manifest_path = manifest_path.with_name(manifest_path.name + ".tmp")

    if dest.exists():
        shutil.rmtree(dest)
    marker.unlink(missing_ok=True)
    manifest_path.unlink(missing_ok=True)
    legacy_tmp_manifest_path.unlink(missing_ok=True)

    for tmp_manifest_path in manifest_path.parent.glob(f"{manifest_path.name}.tmp-*"):
        if tmp_manifest_path.is_dir():
            shutil.rmtree(tmp_manifest_path, ignore_errors=True)
        else:
            tmp_manifest_path.unlink(missing_ok=True)

    for stray_temp in dest.parent.glob(f"{dest.name}.tmp-*"):
        if stray_temp.is_dir():
            shutil.rmtree(stray_temp, ignore_errors=True)
        else:
            stray_temp.unlink(missing_ok=True)


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
    for stray_temp in dest.parent.glob(f"{dest.name}.tmp-{generation}-*"):
        if stray_temp.is_dir():
            shutil.rmtree(stray_temp, ignore_errors=True)
        else:
            stray_temp.unlink(missing_ok=True)


def prepare_source(
    source_kind: str,
    source_reference: str,
    analysis_id: int,
    generation: int,
    publish_callback=None,
):
    """Prepare optional source in generation-private storage, then publish it.

    Clone/extract, index construction, and manifest construction all happen
    under a private ``<analysis>.tmp-<generation>-<uuid>`` directory. The
    canonical analysis source directory is not touched until every expensive
    preparation step has succeeded.

    Production passes ``publish_callback`` from the source-preparation task.
    That callback acquires the Analysis row lock, re-verifies that this exact
    processing generation still owns ``source_status='preparing'``, invokes
    the short filesystem publisher while that lock is held, and durably
    commits ``source_status='ready'`` before releasing the lock. This makes a
    stale generation unable to delete or replace a newer generation's
    canonical source while avoiding any DB lock across clone, ZIP extraction,
    or index construction.

    A ready marker is still the crash-safe filesystem publication boundary.
    If it already exists, the prepared source is adopted without requiring
    the original ZIP or another Git clone.
    """
    dest = Path(SOURCE_STORAGE_ROOT) / str(analysis_id)
    marker = _ready_marker(dest)
    manifest_path = index_manifest_path(dest)

    def _run_publisher(publisher):
        if publish_callback is None:
            return publisher()
        return publish_callback(publisher)

    if marker.exists():
        # Read/build the index before the DB publication guard, but do not
        # mutate canonical filesystem state yet. A stale generation may do
        # harmless read-only work; only an authorized current generation may
        # refresh a missing/corrupt manifest.
        ready_index = load_index_manifest(manifest_path, dest)
        manifest_needs_refresh = ready_index is None
        if ready_index is None:
            ready_index = build_index(dest)

        def _adopt_ready():
            if not marker.exists():
                return None
            if manifest_needs_refresh:
                save_index_manifest(ready_index, manifest_path)
            return ready_index

        return _run_publisher(_adopt_ready)

    temp_dest = dest.parent / f"{dest.name}.tmp-{generation}-{uuid.uuid4().hex}"
    retired_dest = None

    try:
        if source_kind == "github":
            _clone_github(source_reference, temp_dest)
        elif source_kind == "zip":
            _extract_zip(Path(source_reference), temp_dest)
        else:
            raise SourceInputError(f"Unsupported source kind: {source_kind}")

        # All expensive preparation is still generation-private here.
        # Keep the private manifest BESIDE the user-controlled tree so a
        # source file can never collide with or be overwritten by it.
        index = build_index(temp_dest)
        private_manifest = temp_dest.parent / f"{temp_dest.name}.index.json"
        save_index_manifest(index, private_manifest)

        def _publish_prepared():
            # Another complete source may have appeared while this generation
            # was doing its private preparation. Production calls this only
            # while holding the Analysis publication lock.
            if marker.exists():
                return _load_or_rebuild_index(dest, manifest_path)

            # A canonical directory with no ready marker is incomplete state
            # left by a crashed publication. Never recursively delete it while
            # holding the Analysis row lock. Rename it atomically to private
            # retirement storage, publish the complete candidate, and delete
            # the retired tree later after the DB lock has been released.
            nonlocal retired_dest
            if dest.exists():
                retired_dest = dest.parent / (
                    f"{dest.name}.tmp-{generation}-retired-{uuid.uuid4().hex}"
                )
                os.replace(dest, retired_dest)

            os.replace(temp_dest, dest)
            os.replace(private_manifest, manifest_path)
            index.root = dest

            # Marker LAST. If a worker dies after this line but before the DB
            # ready commit, a redelivery can safely adopt this exact tree.
            marker.parent.mkdir(parents=True, exist_ok=True)
            marker.touch()

            return index

        return _run_publisher(_publish_prepared)

    finally:
        # Only private state owned by this invocation is reclaimed here.
        # Never rmtree canonical `dest` from this exception/finally path:
        # after publication ownership is lost, a newer generation may own it.
        shutil.rmtree(temp_dest, ignore_errors=True)

        private_manifest = locals().get("private_manifest")
        if private_manifest is not None:
            private_manifest.unlink(missing_ok=True)

        if retired_dest is not None:
            shutil.rmtree(retired_dest, ignore_errors=True)
