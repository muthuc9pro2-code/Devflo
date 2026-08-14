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
from app.services.source_index import build_index

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

def _clone_github(url: str, dest: Path) -> None:
    dest.mkdir(parents=True, exist_ok=True)
    env = {**os.environ, "GIT_TERMINAL_PROMPT": "0"}
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

def _ready_marker(dest: Path) -> Path:
    return dest.parent / f"{dest.name}.ready"


def prepare_source(source_kind: str, source_reference: str, analysis_id: int):
    """Acquire source into investigation-scoped storage and build its index.

    Idempotent across a resumed process_analysis run: if this analysis's
    source was already fully acquired by a prior invocation (ready marker
    present), skip re-cloning/re-extracting and just rebuild the index.
    Without this, resuming a GitHub-sourced analysis always fails outright
    (`git clone` refuses a non-empty destination), and resuming a
    ZIP-sourced one fails once the staged upload has been deleted after its
    first successful use. The marker lives beside `dest`, not inside it, so
    it is never picked up by build_index as a source file.
    """
    dest = Path(SOURCE_STORAGE_ROOT) / str(analysis_id)
    marker = _ready_marker(dest)
    if marker.exists():
        return build_index(dest)

    if dest.exists():
        shutil.rmtree(dest)  # stale/partial remnant of a crashed prior attempt

    if source_kind == "github":
        _clone_github(source_reference, dest)
    elif source_kind == "zip":
        _extract_zip(Path(source_reference), dest)
    else:
        raise SourceInputError(f"Unsupported source kind: {source_kind}")

    marker.parent.mkdir(parents=True, exist_ok=True)
    marker.touch()
    return build_index(dest)
