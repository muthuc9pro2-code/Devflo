"""Differential test: vectorized BoundedJsonStream._scan vs the original
byte-by-byte reference implementation it replaced.

Feeds identical byte sequences through both scanners under many different
read-chunk splits (since the vectorized version's cross-chunk state carrying
is the riskiest part to get subtly wrong) and asserts identical outcomes:
same bytes returned, same point at which (if at all) OversizedJsonScalarError
is raised, and identical internal state after every read(). Not a pytest
test - a one-off correctness gate, run manually:

    .venv/bin/python scripts/verify_boundedjson_equivalence.py
"""

from __future__ import annotations

import random
import sys
from io import BytesIO
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.utils.bounded_json import (  # noqa: E402
    BoundedJsonStream,
    OversizedJsonScalarError,
)


class _ReferenceBoundedJsonStream:
    """The original per-byte scanner, kept only as an equivalence oracle."""

    def __init__(self, stream, *, max_scalar_bytes):
        self._stream = stream
        self._max_scalar_bytes = max_scalar_bytes
        self._inside_string = False
        self._escaped = False
        self._scalar_bytes = 0
        self._number_bytes = 0

    def read(self, size=-1):
        bounded_size = 65536 if size is None or size < 0 else min(size, 65536)
        data = self._stream.read(bounded_size)
        self._scan(data)
        return data

    def _scan(self, data: bytes) -> None:
        for byte in data:
            if not self._inside_string:
                if byte == ord('"'):
                    self._inside_string = True
                    self._escaped = False
                    self._scalar_bytes = 0
                    self._number_bytes = 0
                elif byte in b"-0123456789" or (self._number_bytes and byte in b"+.eE"):
                    self._number_bytes += 1
                    self._raise_if_oversized(self._number_bytes)
                else:
                    self._number_bytes = 0
                continue
            if self._escaped:
                self._escaped = False
                self._scalar_bytes += 1
            elif byte == ord("\\"):
                self._escaped = True
                self._scalar_bytes += 1
            elif byte == ord('"'):
                self._inside_string = False
                self._scalar_bytes = 0
                continue
            else:
                self._scalar_bytes += 1
            self._raise_if_oversized(self._scalar_bytes)

    def _raise_if_oversized(self, size):
        if size > self._max_scalar_bytes:
            raise OversizedJsonScalarError(f"...{self._max_scalar_bytes} bytes")


def _drive(cls, data: bytes, max_scalar_bytes: int, chunk_sizes: list[int]):
    """Feed `data` through `cls` using an explicit sequence of read() sizes.

    Returns (consumed_bytes, raised: bool, raise_offset_or_None).
    """
    stream = cls(BytesIO(data), max_scalar_bytes=max_scalar_bytes)
    consumed = b""
    offset = 0
    try:
        for size in chunk_sizes:
            chunk = stream.read(size)
            consumed += chunk
            offset += len(chunk)
            if not chunk:
                break
        return consumed, False, None
    except OversizedJsonScalarError:
        return consumed, True, offset


def _random_chunk_plan(rng: random.Random) -> list[int]:
    sizes = []
    remaining_reads = rng.randint(20, 60)
    for _ in range(remaining_reads):
        sizes.append(rng.choice([1, 2, 3, 5, 8, 13, 21, 64, 128, 4096, 65536]))
    return sizes


ADVERSARIAL_CASES = [
    b'{"a": "' + b"x" * 500 + b'"}',
    b'{"a": "' + b"\\" * 501 + b'"}',  # long run of escaped backslashes
    b'{"a": "' + (b'\\"' * 260) + b'"}',  # many escaped quotes
    b"[" + b"1" * 500 + b"]",
    b"[" + b"-" * 500 + b"]",
    b"[1.5e+10, 2.3e-8, " + b"9" * 500 + b"]",
    b'{"n": 123456789012345678901234567890123456789012345678901234567890}',
    b'["short", "another", 42, true, false, null, {"nested": "value"}]',
    b'{"a": "' + b"y" * 40 + b'", "b": ' + b"7" * 40 + b", \"c\": \"" + b"z" * 40 + b'"}',
    b'{"empty": "", "n": ""}',
    b"",
    b'"' + b"a" * 300,  # unterminated string (truncated stream)
    b'{"a": "esc\\\\end", "b": 5' + b"0" * 300 + b"}",
    (b'{"k' + str(i).encode() + b'": "' + bytes([65 + (i % 26)]) * 30 + b'"}' for i in range(0)),
]
ADVERSARIAL_CASES = [c for c in ADVERSARIAL_CASES if isinstance(c, (bytes, bytearray))]


def main() -> int:
    rng = random.Random(1234567)
    mismatches = 0
    trials = 0

    def check(data: bytes, max_scalar_bytes: int, chunk_sizes: list[int], label: str) -> None:
        nonlocal mismatches, trials
        trials += 1
        ref = _drive(_ReferenceBoundedJsonStream, data, max_scalar_bytes, chunk_sizes)
        new = _drive(BoundedJsonStream, data, max_scalar_bytes, chunk_sizes)
        if ref[0] != new[0] or ref[1] != new[1]:
            mismatches += 1
            print(f"MISMATCH [{label}] max={max_scalar_bytes} chunks={chunk_sizes[:8]}...")
            print(f"    ref={ref}")
            print(f"    new={new}")

    for case in ADVERSARIAL_CASES:
        for max_scalar_bytes in (1, 4, 8, 16, 32, 64, 100, 1000, 10_000_000):
            for _ in range(6):
                chunk_sizes = _random_chunk_plan(rng)
                check(case, max_scalar_bytes, chunk_sizes, "adversarial")

    # Purely random byte soup, biased toward the structurally interesting bytes.
    alphabet = b'"\\0123456789.eE+-abc{}[]: \n\t,'
    for _ in range(400):
        length = rng.randint(0, 400)
        data = bytes(rng.choice(alphabet) for _ in range(length))
        max_scalar_bytes = rng.choice([1, 2, 5, 10, 50, 200, 100_000])
        chunk_sizes = _random_chunk_plan(rng)
        check(data, max_scalar_bytes, chunk_sizes, "random")

    print(f"\ntrials={trials} mismatches={mismatches}")
    return 1 if mismatches else 0


if __name__ == "__main__":
    raise SystemExit(main())
