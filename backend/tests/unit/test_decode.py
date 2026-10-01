import json
from typing import Any

import pytest
from confluent_kafka import TIMESTAMP_LOG_APPEND_TIME, TIMESTAMP_NOT_AVAILABLE

from kafka_web.services.decode import MAX_JSON_DEPTH, decode_bytes, to_message_view
from tests.fakes import FakeMessage


def test_none_decodes_to_null() -> None:
    d = decode_bytes(None)

    assert (d.encoding, d.data, d.is_json) == ("null", None, False)


def test_json_object_is_parsed() -> None:
    d = decode_bytes(b'{"a":1}')

    assert d.encoding == "utf-8"
    assert d.data == '{"a":1}'
    assert d.is_json is True
    assert d.json_value == {"a": 1}


def test_json_null_scalar_is_json_with_none_value() -> None:
    d = decode_bytes(b"null")

    assert d.encoding == "utf-8"
    assert d.is_json is True
    assert d.json_value is None


def test_json_number_scalar() -> None:
    d = decode_bytes(b"42")

    assert d.is_json is True
    assert d.json_value == 42


def test_plain_text_is_not_json() -> None:
    d = decode_bytes(b"hello")

    assert (d.encoding, d.data, d.is_json) == ("utf-8", "hello", False)


def test_malformed_json_text_is_not_json() -> None:
    d = decode_bytes(b'{"a":')

    assert (d.encoding, d.is_json) == ("utf-8", False)


def test_non_finite_json_literals_are_not_json() -> None:
    # NaN / Infinity parse in Python but are not JSON and cannot be serialised to a browser.
    assert decode_bytes(b"NaN").is_json is False
    assert decode_bytes(b"[Infinity]").is_json is False
    assert decode_bytes(b"1e999").is_json is False  # overflows to inf


def test_invalid_utf8_is_base64() -> None:
    d = decode_bytes(b"\xff\xfe\x00")

    assert (d.encoding, d.data, d.is_json) == ("base64", "//4A", False)


def test_empty_bytes_is_empty_utf8() -> None:
    d = decode_bytes(b"")

    assert (d.encoding, d.data, d.is_json) == ("utf-8", "", False)


def test_pathologically_nested_json_does_not_raise() -> None:
    d = decode_bytes(b"[" * 100_000 + b"]" * 100_000)

    assert d.encoding == "utf-8"
    assert d.is_json is False


def test_message_view_maps_all_fields() -> None:
    msg = FakeMessage(
        partition=2,
        offset=7,
        key=b"k1",
        value=b'{"x": [1, 2]}',
        headers=[("trace", b"abc"), ("bin", b"\xff")],
    )

    view = to_message_view(msg)

    assert (view.partition, view.offset) == (2, 7)
    assert (view.timestamp, view.timestamp_type) == (1_700_000_000_000, "create")
    assert view.key.data == "k1"
    assert view.value.json_value == {"x": [1, 2]}
    assert [(h.key, h.value.encoding, h.value.data) for h in view.headers] == [
        ("trace", "utf-8", "abc"),
        ("bin", "base64", "/w=="),
    ]


def test_message_view_none_headers_become_empty_list() -> None:
    assert to_message_view(FakeMessage(headers=None)).headers == []


def test_message_view_header_with_none_value_is_null() -> None:
    view = to_message_view(FakeMessage(headers=[("h", None)]))

    assert view.headers[0].value.encoding == "null"


def test_message_view_tombstone_and_null_key() -> None:
    view = to_message_view(FakeMessage(key=None, value=None))

    assert view.key.encoding == "null"
    assert view.value.encoding == "null"


def test_message_view_timestamp_not_available() -> None:
    view = to_message_view(FakeMessage(timestamp=(TIMESTAMP_NOT_AVAILABLE, -1)))

    assert (view.timestamp, view.timestamp_type) == (None, "none")


def test_message_view_log_append_time() -> None:
    view = to_message_view(FakeMessage(timestamp=(TIMESTAMP_LOG_APPEND_TIME, 5)))

    assert (view.timestamp, view.timestamp_type) == (5, "log_append")


def test_message_view_serialises_to_plain_json() -> None:
    dumped: dict[str, Any] = to_message_view(FakeMessage(value=b"7")).model_dump(mode="json")

    assert dumped["value"] == {"encoding": "utf-8", "data": "7", "is_json": True, "json_value": 7}


def _nested(depth: int) -> bytes:
    return b"[" * depth + b"]" * depth


# json.loads accepts all of these, but they cannot be serialised back to the browser.
UNSERIALISABLE_JSON = {
    "lone_surrogate": b'{"a": "\\ud800"}',
    "255_deep": _nested(255),
    "5000_deep": _nested(5000),
    "deep_in_object": b'{"a":' * 300 + b"1" + b"}" * 300,
}


@pytest.mark.parametrize("raw", UNSERIALISABLE_JSON.values(), ids=UNSERIALISABLE_JSON.keys())
def test_json_that_cannot_be_serialised_is_kept_as_text(raw: bytes) -> None:
    d = decode_bytes(raw)

    assert (d.encoding, d.is_json, d.json_value) == ("utf-8", False, None)
    assert d.data == raw.decode()


@pytest.mark.parametrize("raw", UNSERIALISABLE_JSON.values(), ids=UNSERIALISABLE_JSON.keys())
def test_whole_message_view_serialises_for_any_json_payload(raw: bytes) -> None:
    view = to_message_view(FakeMessage(key=raw, value=raw, headers=[("h", raw)]))

    body = json.loads(view.model_dump_json())

    assert body["value"]["is_json"] is False
    assert body["headers"][0]["value"]["data"] == raw.decode()


def test_deeply_nested_json_within_the_limit_is_still_json() -> None:
    d = decode_bytes(_nested(MAX_JSON_DEPTH))

    assert d.is_json is True
    json.loads(to_message_view(FakeMessage(value=_nested(MAX_JSON_DEPTH))).model_dump_json())


def test_json_one_level_past_the_limit_is_text() -> None:
    assert decode_bytes(_nested(MAX_JSON_DEPTH + 1)).is_json is False
