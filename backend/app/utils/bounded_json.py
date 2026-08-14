import re
from typing import BinaryIO

from app.core.processing_config import (
    JSON_STREAM_BUFFER_BYTES,
    MAX_DIAGNOSTIC_RECORD_BYTES,
)


class OversizedJsonScalarError(ValueError):
    """Raised before a structured scalar can exceed the ingestion memory bound."""


# A run of number-shaped bytes: digits/'-' can start or continue a run,
# '+'/'.'/'e'/'E' only ever continue one already in progress. Plain character
# classes with a single trailing '*' - no nested/alternated quantifiers, so
# this can't backtrack catastrophically; it's a single linear pass per match.
_NUMBER_RUN_PATTERN = re.compile(rb"[0-9-][0-9.eE+-]*")
_NUMBER_CONTINUATION_PATTERN = re.compile(rb"[0-9.eE+-]*")


class BoundedJsonStream:
    """Guard a binary JSON stream against unbounded scalar token allocation.

    Incremental JSON parsers still materialize an individual JSON string as one
    Python object. Scanning the raw chunks before the parser sees them lets us
    reject a pathological scalar while memory remains bounded.
    """

    def __init__(
        self,
        stream: BinaryIO,
        *,
        max_scalar_bytes: int = MAX_DIAGNOSTIC_RECORD_BYTES,
    ) -> None:
        if max_scalar_bytes <= 0:
            raise ValueError("max_scalar_bytes must be positive")
        self._stream = stream
        self._max_scalar_bytes = max_scalar_bytes
        self._inside_string = False
        self._escaped = False
        self._scalar_bytes = 0
        self._number_bytes = 0

    def read(self, size: int = -1) -> bytes:
        # Never honor an unbounded read from a parser/backend.
        bounded_size = (
            JSON_STREAM_BUFFER_BYTES
            if size is None or size < 0
            else min(size, JSON_STREAM_BUFFER_BYTES)
        )
        data = self._stream.read(bounded_size)
        self._scan(data)
        return data

    def _scan(self, data: bytes) -> None:
        # Same state machine as a byte-by-byte scan (string vs. bare-value
        # mode, backslash-escapes, digit runs), just advanced in bulk spans
        # via bytes.find()/regex instead of one Python-level step per byte.
        pos = 0
        length = len(data)
        while pos < length:
            if self._inside_string:
                pos = self._scan_string(data, pos, length)
            else:
                pos = self._scan_outside_string(data, pos, length)

    def _scan_string(self, data: bytes, pos: int, length: int) -> int:
        if self._escaped:
            # Whatever byte follows a backslash is escaped content, verbatim.
            self._escaped = False
            self._scalar_bytes += 1
            self._raise_if_oversized(self._scalar_bytes)
            return pos + 1

        quote_idx = data.find(b'"', pos)
        backslash_idx = data.find(b"\\", pos)
        if backslash_idx != -1 and (quote_idx == -1 or backslash_idx <= quote_idx):
            self._scalar_bytes += (backslash_idx - pos) + 1
            self._raise_if_oversized(self._scalar_bytes)
            self._escaped = True
            return backslash_idx + 1

        if quote_idx != -1:
            self._scalar_bytes += quote_idx - pos
            self._raise_if_oversized(self._scalar_bytes)
            self._inside_string = False
            self._scalar_bytes = 0
            self._number_bytes = 0
            return quote_idx + 1

        self._scalar_bytes += length - pos
        self._raise_if_oversized(self._scalar_bytes)
        return length

    def _scan_outside_string(self, data: bytes, pos: int, length: int) -> int:
        quote_idx = data.find(b'"', pos)
        hit_quote = quote_idx != -1
        segment_end = quote_idx if hit_quote else length

        cursor = pos
        if self._number_bytes and cursor < segment_end:
            # A number run was still open at the end of the previous chunk.
            # Its continuation bytes ('+'/'.'/'e'/'E' included, not just
            # digits) won't necessarily match _NUMBER_RUN_PATTERN's required
            # [0-9-] lead character, so this has to be checked separately
            # before falling through to the fresh-run scan below.
            continuation = _NUMBER_CONTINUATION_PATTERN.match(data, cursor, segment_end)
            run_len = continuation.end() - cursor
            if run_len:
                self._number_bytes += run_len
                self._raise_if_oversized(self._number_bytes)
                cursor = continuation.end()
                if cursor == segment_end:
                    # The carried run occupies the rest of this segment. It's
                    # still open unless what ended the segment was a quote.
                    if hit_quote:
                        self._number_bytes = 0
                        self._inside_string = True
                        self._escaped = False
                        self._scalar_bytes = 0
                        return segment_end + 1
                    return segment_end
            # Either nothing continued it (run_len == 0) or it broke off
            # mid-segment: either way the carried run is now fully resolved.
            self._number_bytes = 0
        else:
            self._number_bytes = 0

        last_match = None
        for match in _NUMBER_RUN_PATTERN.finditer(data, cursor, segment_end):
            self._raise_if_oversized(match.end() - match.start())
            last_match = match
        if last_match is not None and last_match.end() == segment_end and not hit_quote:
            # This run reaches the end of the buffered chunk (not cut short
            # by a quote), so it may continue into the next read().
            self._number_bytes = last_match.end() - last_match.start()

        if hit_quote:
            self._inside_string = True
            self._escaped = False
            self._scalar_bytes = 0
            self._number_bytes = 0
            return segment_end + 1
        return segment_end

    def _raise_if_oversized(self, size: int) -> None:
        if size > self._max_scalar_bytes:
            raise OversizedJsonScalarError(
                "JSON scalar exceeds the configured diagnostic record limit "
                f"of {self._max_scalar_bytes} bytes"
            )
