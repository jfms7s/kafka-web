"""Message publishing (spec §5): header and bulk-file parsing, and delivery over a producer."""

import csv
import io
import json
import re
from dataclasses import dataclass
from typing import Any, Literal

from kafka_web.errors import ValidationFailed

_BOM = b"\xef\xbb\xbf"
_FORMAT_PREFIX = re.compile(rb"(?:\xef\xbb\xbf)?\s*(.?)", re.DOTALL)


@dataclass(frozen=True)
class OutMessage:
    key: bytes | None
    value: bytes
    headers: list[tuple[str, bytes]]
    partition: int | None = None


@dataclass(frozen=True)
class RowError:
    """A row that could not be turned into a message; `row` is 1-based."""

    row: int
    error: str


Item = OutMessage | RowError


def _header_value(value: Any) -> bytes:
    """A header value as sent: strings as-is, anything else as its JSON text."""
    return (value if isinstance(value, str) else json.dumps(value, ensure_ascii=False)).encode()


# --- headers (single publish) ------------------------------------------------------------------


def parse_headers(text: str | None) -> list[tuple[str, bytes]]:
    """Headers from a JSON object (`{"a": "b"}`) or from `key=value` lines; blank means none."""
    if text is None or not text.strip():
        return []
    if text.lstrip().startswith("{"):
        return _headers_from_json(text)
    return _headers_from_lines(text)


def _headers_from_json(text: str) -> list[tuple[str, bytes]]:
    try:
        parsed = json.loads(text)
    except (ValueError, RecursionError) as exc:
        raise ValidationFailed(
            f"headers: not a valid JSON object ({exc})", field="headers"
        ) from None
    if not isinstance(parsed, dict):
        raise ValidationFailed("headers: expected a JSON object", field="headers")
    return [(name, _header_value(value)) for name, value in parsed.items()]


def _headers_from_lines(text: str) -> list[tuple[str, bytes]]:
    headers = []
    for line in text.splitlines():
        if not line.strip():
            continue
        name, separator, value = line.partition("=")
        if not separator or not name.strip():
            raise ValidationFailed(
                f"headers: expected key=value, got {line.strip()!r}", field="headers"
            )
        headers.append((name.strip(), value.encode()))
    return headers


# --- bulk files --------------------------------------------------------------------------------


def detect_format(content: bytes) -> Literal["json", "csv"]:
    """`json` when the first non-whitespace character (after an optional BOM) is `[`."""
    match = _FORMAT_PREFIX.match(content)
    return "json" if match is not None and match.group(1) == b"[" else "csv"


def parse_bulk(content: bytes, key_column: str | None, value_column: str | None) -> list[Item]:
    if detect_format(content) == "json":
        return parse_json(content)
    return parse_csv(content, key_column, value_column)


def _decode(content: bytes) -> str:
    try:
        return content.decode("utf-8-sig")
    except UnicodeDecodeError:
        raise ValidationFailed("The file is not valid UTF-8 text", field="file") from None


def parse_csv(content: bytes, key_column: str | None, value_column: str | None) -> list[Item]:
    """One message per data row. The key column (if any) is the key; the value column (if any)
    is the value, else the whole row minus the key is a JSON object; the other columns are
    headers. Rows of the wrong width are `RowError`s."""
    try:
        return _csv_items(_decode(content), key_column, value_column)
    except csv.Error as exc:
        raise ValidationFailed(f"The file is not valid CSV: {exc}", field="file") from None


def _csv_items(text: str, key_column: str | None, value_column: str | None) -> list[Item]:
    rows = (row for row in csv.reader(io.StringIO(text, newline="")) if row)  # skip blank lines
    columns = next(rows, [])
    if len(set(columns)) != len(columns):
        raise ValidationFailed("The CSV header repeats a column name", field="file")
    for column, field in ((key_column, "key_column"), (value_column, "value_column")):
        if column is not None and column not in columns:
            raise ValidationFailed(f"Column {column!r} is not in the CSV header", field=field)

    items: list[Item] = []
    for number, row in enumerate(rows, start=1):
        if len(row) != len(columns):
            items.append(RowError(number, f"Expected {len(columns)} columns, found {len(row)}"))
            continue
        cells = dict(zip(columns, row, strict=True))
        key = cells.pop(key_column) if key_column is not None else ""
        if value_column is None:
            value, headers = json.dumps(cells, ensure_ascii=False), {}
        else:
            value, headers = cells.pop(value_column), cells
        items.append(
            OutMessage(
                key=key.encode() if key else None,
                value=value.encode(),
                headers=[(name, text.encode()) for name, text in headers.items()],
            )
        )
    return items


def parse_json(content: bytes) -> list[Item]:
    """An array of `{key?, value, headers?}` objects. A bad item is a `RowError`."""
    text = _decode(content)
    try:
        parsed = json.loads(text)
    except (ValueError, RecursionError) as exc:
        raise ValidationFailed(f"The file is not valid JSON ({exc})", field="file") from None
    if not isinstance(parsed, list):
        raise ValidationFailed("The JSON file must be an array of messages", field="file")
    return [_json_item(number, item) for number, item in enumerate(parsed, start=1)]


def _json_item(number: int, item: Any) -> Item:
    try:
        return OutMessage(*_json_fields(item))
    except _BadItem as bad:
        return RowError(number, str(bad))
    except UnicodeEncodeError:
        return RowError(number, "Text is not valid UTF-8")


class _BadItem(Exception):
    pass


def _json_fields(item: Any) -> tuple[bytes | None, bytes, list[tuple[str, bytes]]]:
    if not isinstance(item, dict):
        raise _BadItem("Item must be an object")
    if "value" not in item:
        raise _BadItem('Missing required field "value"')
    key = item.get("key")
    if key is not None and not isinstance(key, str):
        raise _BadItem("key must be a string or null")
    return (
        None if key is None else key.encode(),
        _header_value(item["value"]),
        _json_headers(item.get("headers")),
    )


def _json_headers(headers: Any) -> list[tuple[str, bytes]]:
    if headers is None:
        return []
    if isinstance(headers, dict):
        return [(name, _header_value(value)) for name, value in headers.items()]
    pairs = headers if isinstance(headers, list) else None
    if pairs is not None and all(
        isinstance(p, list) and len(p) == 2 and isinstance(p[0], str) for p in pairs
    ):
        return [(name, _header_value(value)) for name, value in pairs]
    raise _BadItem("headers must be an object or a list of [name, value] pairs")
