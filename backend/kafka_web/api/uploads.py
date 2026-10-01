"""Streaming `multipart/form-data` reader for the bulk-publish upload.

FastAPI parses a form body *before* it runs the route's dependencies, and spools the whole file
to disk. For bulk publish that is the wrong order: the write guard (read-only cluster, typed
confirmation) must be able to refuse a request without the file having been read, and the size
limit must stop the read, not be checked afterwards. So the route takes the raw `Request` and
this module reads its body chunk by chunk, with the limit applied as the bytes arrive.
"""

from collections.abc import AsyncIterable, Callable
from dataclasses import dataclass, field
from typing import Any

from python_multipart.exceptions import MultipartParseError
from python_multipart.multipart import MultipartParser, parse_options_header

from kafka_web.errors import PayloadTooLarge, ValidationFailed

FILE_FIELD = "file"
MAX_FIELD_BYTES = 64 * 1024  # a text field (confirm, column names) is never anywhere near this
MAX_FORM_OVERHEAD = 1024 * 1024  # boundaries, headers and text fields around the file


@dataclass
class Upload:
    fields: dict[str, str] = field(default_factory=dict)
    file: bytes | None = None


def _too_large(max_file_bytes: int) -> PayloadTooLarge:
    return PayloadTooLarge(
        f"The upload is larger than the {max_file_bytes // (1024 * 1024)} MB limit",
        code="file_too_large",
    )


def _not_multipart() -> ValidationFailed:
    return ValidationFailed("Expected a multipart/form-data upload with a file", field=FILE_FIELD)


class _Collector:
    """Parser callbacks: collects the text fields and the `file` part, enforcing the limits."""

    def __init__(self, max_file_bytes: int, on_file_start: Callable[[dict[str, str]], None] | None):
        self.upload = Upload()
        self.ended = False
        self._max_file_bytes = max_file_bytes
        self._max_total = max_file_bytes + MAX_FORM_OVERHEAD
        self._on_file_start = on_file_start
        self._total = 0
        self._header_name = b""
        self._header_value = b""
        self._headers: dict[bytes, bytes] = {}
        self._name: str | None = None
        self._is_file = False
        self._capture: bytearray | None = None  # where the current part's bytes go; None: drop

    # -- part framing --
    def on_part_begin(self) -> None:
        self._headers, self._name, self._capture = {}, None, None

    def on_header_field(self, data: bytes, start: int, end: int) -> None:
        self._header_name += data[start:end]

    def on_header_value(self, data: bytes, start: int, end: int) -> None:
        self._header_value += data[start:end]

    def on_header_end(self) -> None:
        self._headers[self._header_name.lower()] = self._header_value
        self._header_name = self._header_value = b""

    def on_headers_finished(self) -> None:
        _, params = parse_options_header(self._headers.get(b"content-disposition", b""))
        raw_name = params.get(b"name")
        self._name = raw_name.decode("utf-8", "replace") if raw_name is not None else None
        self._is_file = b"filename" in params
        if self._is_file:
            if self._name == FILE_FIELD and self.upload.file is None:
                if self._on_file_start is not None:
                    self._on_file_start(dict(self.upload.fields))
                self._capture = bytearray()
        elif self._name is not None:
            self._capture = bytearray()

    def on_part_data(self, data: bytes, start: int, end: int) -> None:
        self._total += end - start
        if self._total > self._max_total:
            raise _too_large(self._max_file_bytes)
        if self._capture is None:
            return
        self._capture += data[start:end]
        limit = self._max_file_bytes if self._is_file else MAX_FIELD_BYTES
        if len(self._capture) > limit:
            raise _too_large(self._max_file_bytes)

    def on_part_end(self) -> None:
        if self._capture is not None and self._name is not None:
            if self._is_file:
                self.upload.file = bytes(self._capture)
            else:
                self.upload.fields[self._name] = self._capture.decode("utf-8", "replace")
        self._capture = None

    def on_end(self) -> None:
        self.ended = True


async def read_upload(
    stream: AsyncIterable[bytes],
    content_type: str,
    *,
    max_file_bytes: int,
    on_file_start: Callable[[dict[str, str]], None] | None = None,
    content_length: int | None = None,
) -> Upload:
    """Read a multipart body: text fields and the `file` part (the first one only).

    `on_file_start(fields_so_far)` runs when the file part's headers arrive, before any of its
    bytes are read: it may raise to refuse the upload. Raises `PayloadTooLarge` (413
    `file_too_large`) as soon as the file, or the body as a whole, exceeds its limit, and
    `ValidationFailed` (field `file`) for a body that is not well-formed multipart.
    """
    mime, params = parse_options_header(content_type)
    boundary = params.get(b"boundary")
    if mime != b"multipart/form-data" or not boundary:
        raise _not_multipart()
    if content_length is not None and content_length > max_file_bytes + MAX_FORM_OVERHEAD:
        raise _too_large(max_file_bytes)

    collector = _Collector(max_file_bytes, on_file_start)
    callbacks: Any = {
        "on_part_begin": collector.on_part_begin,
        "on_header_field": collector.on_header_field,
        "on_header_value": collector.on_header_value,
        "on_header_end": collector.on_header_end,
        "on_headers_finished": collector.on_headers_finished,
        "on_part_data": collector.on_part_data,
        "on_part_end": collector.on_part_end,
        "on_end": collector.on_end,
    }
    try:
        parser = MultipartParser(boundary, callbacks)
        async for chunk in stream:
            parser.write(chunk)
        parser.finalize()
    except MultipartParseError:
        raise _not_multipart() from None
    if not collector.ended:
        raise _not_multipart()
    return collector.upload
