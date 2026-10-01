"""Decode raw Kafka bytes (keys, values, header values) into a JSON-safe view for the browser."""

import base64
import json
from typing import Any, Literal, Protocol

from confluent_kafka import TIMESTAMP_CREATE_TIME, TIMESTAMP_LOG_APPEND_TIME
from pydantic import BaseModel
from pydantic_core import to_json

Encoding = Literal["utf-8", "base64", "null"]
TimestampType = Literal["create", "log_append", "none"]

# Well below pydantic's 255-level serialisation limit, which the response envelope also eats into.
MAX_JSON_DEPTH = 100

_TIMESTAMP_TYPES: dict[int, TimestampType] = {
    TIMESTAMP_CREATE_TIME: "create",
    TIMESTAMP_LOG_APPEND_TIME: "log_append",
}


class Decoded(BaseModel):
    encoding: Encoding
    data: str | None  # text for utf-8, base64 text for base64, None for null
    is_json: bool
    json_value: Any = None  # parsed JSON when is_json (None is legitimate for the text "null")


class HeaderView(BaseModel):
    key: str
    value: Decoded


class MessageView(BaseModel):
    partition: int
    offset: int
    timestamp: int | None
    timestamp_type: TimestampType
    key: Decoded
    value: Decoded
    headers: list[HeaderView]


class _Message(Protocol):
    """The slice of `confluent_kafka.Message` that `to_message_view` reads."""

    def partition(self) -> int | None: ...
    def offset(self) -> int | None: ...
    def timestamp(self) -> tuple[int, int]: ...
    def key(self) -> bytes | str | None: ...
    def value(self) -> bytes | str | None: ...
    def headers(self) -> list[tuple[str, bytes | str | None]] | None: ...


def _reject_constant(name: str) -> Any:
    raise ValueError(f"{name} is not valid JSON")


def _finite_float(text: str) -> float:
    number = float(text)
    if number in (float("inf"), float("-inf")):
        raise ValueError("float out of range")
    return number


def _nesting_depth(value: Any) -> int:
    """Levels of arrays/objects in a parsed JSON value (a scalar is 0, `[]` is 1)."""
    deepest = 0
    pending = [(value, 1)]
    while pending:
        node, depth = pending.pop()
        if isinstance(node, dict):
            children = list(node.values())
        elif isinstance(node, list):
            children = node
        else:
            continue
        deepest = max(deepest, depth)
        if deepest > MAX_JSON_DEPTH:
            break
        pending.extend((child, depth + 1) for child in children)
    return deepest


def _can_serialise(value: Any) -> bool:
    """Whether the response can carry `value`: pydantic must be able to write it out."""
    if _nesting_depth(value) > MAX_JSON_DEPTH:
        return False
    try:
        to_json(value)
    except ValueError:  # PydanticSerializationError, e.g. a lone surrogate escape (\ud800)
        return False
    return True


def _parse_json(text: str) -> tuple[bool, Any]:
    """`(True, value)` if `text` is strict JSON a browser can round-trip, else `(False, None)`.

    `json.loads` accepts more than the response can carry: lone surrogate escapes and nesting
    beyond pydantic's serialisation depth limit (255 levels, shared with the response envelope).
    Such text is not treated as JSON; it is still returned verbatim as `data`.
    """
    try:
        value = json.loads(text, parse_constant=_reject_constant, parse_float=_finite_float)
    except (ValueError, RecursionError):  # JSONDecodeError is a ValueError
        return False, None
    return (True, value) if _can_serialise(value) else (False, None)


def decode_bytes(raw: bytes | str | None) -> Decoded:
    if raw is None:
        return Decoded(encoding="null", data=None, is_json=False)
    if isinstance(raw, str):  # confluent-kafka hands back str only for odd, non-bytes payloads
        raw = raw.encode()
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError:
        return Decoded(encoding="base64", data=base64.b64encode(raw).decode("ascii"), is_json=False)
    is_json, value = _parse_json(text)
    return Decoded(encoding="utf-8", data=text, is_json=is_json, json_value=value)


def to_message_view(msg: _Message) -> MessageView:
    ts_type, ts = msg.timestamp()
    timestamp_type = _TIMESTAMP_TYPES.get(ts_type, "none")
    partition, offset = msg.partition(), msg.offset()
    assert partition is not None and offset is not None  # a delivered message always has both
    return MessageView(
        partition=partition,
        offset=offset,
        timestamp=ts if timestamp_type != "none" else None,
        timestamp_type=timestamp_type,
        key=decode_bytes(msg.key()),
        value=decode_bytes(msg.value()),
        headers=[HeaderView(key=k, value=decode_bytes(v)) for k, v in msg.headers() or []],
    )
