import re
from typing import BinaryIO
from app.core.processing_config import (
    JSON_STREAM_BUFFER_BYTES,
    MAX_DIAGNOSTIC_RECORD_BYTES,
)

class OversizedJsonScalarError(ValueError):
    pass

_NUMBER_RUN_PATTERN = re.compile(rb"[0-9-][0-9.eE+-]*")
_NUMBER_CONTINUATION_PATTERN = re.compile(rb"[0-9.eE+-]*")

class BoundedJsonStream:

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
        bounded_size = (
            JSON_STREAM_BUFFER_BYTES
            if size is None or size < 0
            else min(size, JSON_STREAM_BUFFER_BYTES)
        )
        data = self._stream.read(bounded_size)
        self._scan(data)
        return data

    def _scan(self, data: bytes) -> None:
        pos = 0
        length = len(data)
        while pos < length:
            if self._inside_string:
                pos = self._scan_string(data, pos, length)
            else:
                pos = self._scan_outside_string(data, pos, length)

    def _scan_string(self, data: bytes, pos: int, length: int) -> int:
        if self._escaped:
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
            continuation = _NUMBER_CONTINUATION_PATTERN.match(data, cursor, segment_end)
            run_len = continuation.end() - cursor
            if run_len:
                self._number_bytes += run_len
                self._raise_if_oversized(self._number_bytes)
                cursor = continuation.end()
                if cursor == segment_end:
                    if hit_quote:
                        self._number_bytes = 0
                        self._inside_string = True
                        self._escaped = False
                        self._scalar_bytes = 0
                        return segment_end + 1
                    return segment_end
            self._number_bytes = 0
        else:
            self._number_bytes = 0

        last_match = None
        for match in _NUMBER_RUN_PATTERN.finditer(data, cursor, segment_end):
            self._raise_if_oversized(match.end() - match.start())
            last_match = match
        if last_match is not None and last_match.end() == segment_end and not hit_quote:
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
