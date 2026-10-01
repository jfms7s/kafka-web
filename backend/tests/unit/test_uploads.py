"""The streaming multipart reader behind bulk upload: size limits and early confirm check."""

import asyncio
from collections.abc import AsyncIterator, Callable
from typing import Any

import pytest

from kafka_web.api.uploads import read_upload
from kafka_web.errors import AppError, PayloadTooLarge, ValidationFailed

BOUNDARY = "xBOUNDARYx"
CONTENT_TYPE = f"multipart/form-data; boundary={BOUNDARY}"


def field(name: str, value: str) -> bytes:
    return (
        f'--{BOUNDARY}\r\nContent-Disposition: form-data; name="{name}"\r\n\r\n{value}\r\n'
    ).encode()


def file_part(content: bytes, name: str = "file") -> bytes:
    head = (
        f'--{BOUNDARY}\r\nContent-Disposition: form-data; name="{name}"; filename="f.csv"\r\n'
        "Content-Type: text/csv\r\n\r\n"
    ).encode()
    return head + content + b"\r\n"


END = f"--{BOUNDARY}--\r\n".encode()


class Chunks:
    """A request body in small chunks that remembers how far the reader got."""

    def __init__(self, body: bytes, size: int = 7):
        self.chunks = [body[i : i + size] for i in range(0, len(body), size)]
        self.consumed = 0

    async def __aiter__(self) -> AsyncIterator[bytes]:
        for chunk in self.chunks:
            self.consumed += 1
            yield chunk


def run(body: bytes, **kwargs: Any):
    stream = Chunks(body, kwargs.pop("chunk_size", 7))
    kwargs.setdefault("max_file_bytes", 1_000_000)
    upload = asyncio.run(read_upload(stream, CONTENT_TYPE, **kwargs))
    return upload, stream


def test_reads_fields_and_the_file() -> None:
    body = field("confirm", "orders") + field("key_column", "id") + file_part(b"a,b\n1,2\n") + END

    upload, _ = run(body)

    assert upload.fields == {"confirm": "orders", "key_column": "id"}
    assert upload.file == b"a,b\n1,2\n"


def test_file_with_binary_and_boundary_like_bytes_survives_chunking() -> None:
    content = bytes(range(256)) * 20 + b"\r\n--not-the-boundary\r\n"

    upload, _ = run(field("confirm", "t") + file_part(content) + END, chunk_size=13)

    assert upload.file == content


def test_no_file_part_gives_none() -> None:
    upload, _ = run(field("confirm", "t") + END)

    assert upload.file is None


def test_an_oversized_file_stops_the_read_early() -> None:
    body = field("confirm", "t") + file_part(b"x" * 10_000) + END
    stream = Chunks(body)

    with pytest.raises(PayloadTooLarge) as info:
        asyncio.run(read_upload(stream, CONTENT_TYPE, max_file_bytes=100))

    assert info.value.code == "file_too_large" and info.value.status == 413
    assert stream.consumed < len(stream.chunks) / 10  # nowhere near the whole body was read


def test_a_file_of_exactly_the_limit_is_accepted() -> None:
    upload, _ = run(file_part(b"x" * 100) + END, max_file_bytes=100)

    assert upload.file == b"x" * 100


def test_a_huge_non_file_field_is_refused_too() -> None:
    with pytest.raises(PayloadTooLarge):
        run(field("confirm", "y" * 200_000) + END, max_file_bytes=100)


def test_the_file_start_hook_sees_the_fields_before_the_file_body_is_read() -> None:
    seen: list[dict[str, str]] = []
    body = field("confirm", "wrong") + file_part(b"x" * 10_000) + END
    stream = Chunks(body)

    def refuse(fields: dict[str, str]) -> None:
        seen.append(dict(fields))
        raise ValidationFailed("nope", code="confirmation_mismatch", field="confirm")

    with pytest.raises(AppError) as info:
        asyncio.run(
            read_upload(stream, CONTENT_TYPE, max_file_bytes=1_000_000, on_file_start=refuse)
        )

    assert info.value.code == "confirmation_mismatch"
    assert seen == [{"confirm": "wrong"}]
    assert stream.consumed < len(stream.chunks) / 10


def test_the_hook_sees_no_fields_when_the_file_comes_first() -> None:
    seen: list[dict[str, str]] = []
    hook: Callable[[dict[str, str]], None] = lambda fields: seen.append(dict(fields))  # noqa: E731

    upload, _ = run(file_part(b"abc") + field("confirm", "t") + END, on_file_start=hook)

    assert seen == [{}]
    assert upload.fields == {"confirm": "t"}  # still collected, to be checked afterwards


def test_a_second_file_part_is_ignored() -> None:
    upload, _ = run(file_part(b"first") + file_part(b"second", name="other") + END)

    assert upload.file == b"first"


@pytest.mark.parametrize("content_type", ["application/json", "multipart/form-data", ""])
def test_not_multipart_or_no_boundary_is_422_on_file(content_type: str) -> None:
    with pytest.raises(ValidationFailed) as info:
        asyncio.run(read_upload(Chunks(b"{}"), content_type, max_file_bytes=100))

    assert info.value.field == "file"


def test_a_malformed_body_is_422_on_file() -> None:
    with pytest.raises(ValidationFailed) as info:
        run(b"this is not multipart at all\r\n" * 3)

    assert info.value.field == "file"
