from typing import BinaryIO

from app.core.processing_config import (
    JSON_STREAM_BUFFER_BYTES,
    MAX_DIAGNOSTIC_RECORD_BYTES,
)


class OversizedJsonScalarError(ValueError):
    """Raised before a structured scalar can exceed the ingestion memory bound."""


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

    def _raise_if_oversized(self, size: int) -> None:
        if size > self._max_scalar_bytes:
            raise OversizedJsonScalarError(
                "JSON scalar exceeds the configured diagnostic record limit "
                f"of {self._max_scalar_bytes} bytes"
            )
