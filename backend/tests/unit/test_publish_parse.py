import csv
import json

import pytest

from kafka_web.errors import ValidationFailed
from kafka_web.services.publish import (
    MAX_BULK_ROWS,
    OutMessage,
    RowError,
    detect_format,
    parse_csv,
    parse_headers,
    parse_json,
)

BOM = b"\xef\xbb\xbf"


# --- parse_headers ---------------------------------------------------------------------------


@pytest.mark.parametrize("text", [None, "", "   ", "\n \n"])
def test_blank_headers_are_empty(text: str | None) -> None:
    assert parse_headers(text) == []


def test_headers_json_object_stringifies_non_string_values() -> None:
    text = ' {"trace": "abc", "n": 3, "flag": true, "obj": {"a": 1}, "none": null}'

    assert parse_headers(text) == [
        ("trace", b"abc"),
        ("n", b"3"),
        ("flag", b"true"),
        ("obj", b'{"a": 1}'),
        ("none", b"null"),
    ]


def test_headers_key_value_lines() -> None:
    text = "trace=abc\n\n  source = web \nexpr=a=b\r\nempty=\n"

    assert parse_headers(text) == [
        ("trace", b"abc"),
        ("source", b"web"),
        ("expr", b"a=b"),
        ("empty", b""),
    ]


def test_headers_line_without_equals_names_the_line() -> None:
    with pytest.raises(ValidationFailed) as info:
        parse_headers("ok=1\nbroken line")

    assert info.value.field == "headers"
    assert "broken line" in info.value.message


def test_headers_line_with_empty_key_is_rejected() -> None:
    with pytest.raises(ValidationFailed) as info:
        parse_headers("=value")

    assert info.value.field == "headers"


def test_headers_invalid_json_object_is_rejected() -> None:
    with pytest.raises(ValidationFailed) as info:
        parse_headers('{"a": ')

    assert info.value.field == "headers"


def test_headers_json_that_is_not_an_object_is_rejected() -> None:
    with pytest.raises(ValidationFailed) as info:
        parse_headers("[1, 2]")  # not "{": parsed as lines, and has no "="

    assert info.value.field == "headers"


# --- detect_format ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("content", "expected"),
    [
        (b'[{"value": "x"}]', "json"),
        (b'  \n\t[{"value": "x"}]', "json"),
        (BOM + b'[{"value": "x"}]', "json"),
        (BOM + b'  \r\n[{"value": "x"}]', "json"),
        (b"a,b\n1,2\n", "csv"),
        (BOM + b"a,b\n", "csv"),
        (b'{"value": "x"}', "csv"),  # only "[" selects JSON
        (b"", "csv"),
        (b"   ", "csv"),
    ],
)
def test_detect_format(content: bytes, expected: str) -> None:
    assert detect_format(content) == expected


# --- parse_csv -------------------------------------------------------------------------------


def test_csv_key_and_value_columns() -> None:
    content = b"id,body\n1,hello\n2,world\n"

    assert parse_csv(content, "id", "body") == [
        OutMessage(key=b"1", value=b"hello", headers=[]),
        OutMessage(key=b"2", value=b"world", headers=[]),
    ]


def test_csv_without_value_column_serialises_the_row_without_the_key() -> None:
    content = b"id,name,city\n7,Ann,Lisbon\n"

    [message] = parse_csv(content, "id", None)

    assert isinstance(message, OutMessage)
    assert message.key == b"7"
    assert json.loads(message.value) == {"name": "Ann", "city": "Lisbon"}
    assert message.headers == []


def test_csv_without_key_column_has_no_key_and_keeps_every_column() -> None:
    [message] = parse_csv(b"a,b\n1,2\n", None, None)

    assert isinstance(message, OutMessage)
    assert message.key is None
    assert json.loads(message.value) == {"a": "1", "b": "2"}


def test_csv_extra_columns_become_headers() -> None:
    content = b"id,body,trace,source\n1,hi,t-1,web\n"

    [message] = parse_csv(content, "id", "body")

    assert isinstance(message, OutMessage)
    assert message.headers == [("trace", b"t-1"), ("source", b"web")]


def test_csv_empty_key_cell_is_a_null_key() -> None:
    [message] = parse_csv(b"id,body\n,hi\n", "id", "body")

    assert isinstance(message, OutMessage)
    assert message.key is None


def test_csv_ragged_row_is_a_row_error_and_does_not_stop_the_rest() -> None:
    content = b"id,body\n1,ok\n2\n3,fine,extra\n4,last\n"

    items = parse_csv(content, "id", "body")

    assert [type(i) for i in items] == [OutMessage, RowError, RowError, OutMessage]
    assert items[1] == RowError(row=2, error="Expected 2 columns, found 1")
    assert items[2] == RowError(row=3, error="Expected 2 columns, found 3")


def test_csv_blank_lines_are_skipped_and_not_counted() -> None:
    items = parse_csv(b"id,body\n1,a\n\n2\n", "id", "body")

    assert items[1] == RowError(row=2, error="Expected 2 columns, found 1")


def test_csv_decodes_a_utf8_bom_and_non_ascii() -> None:
    content = BOM + "id,body\n1,olá\n".encode()

    assert parse_csv(content, "id", "body") == [
        OutMessage(key=b"1", value="olá".encode(), headers=[])
    ]


def test_csv_quoted_fields_with_commas_and_newlines() -> None:
    [message] = parse_csv(b'id,body\n1,"a,b\nc ""q"""\n', "id", "body")

    assert isinstance(message, OutMessage)
    assert message.value == b'a,b\nc "q"'


def test_csv_header_only_is_empty() -> None:
    assert parse_csv(b"id,body\n", "id", "body") == []
    assert parse_csv(b"", None, None) == []


@pytest.mark.parametrize(
    ("key_column", "value_column", "field"),
    [("nope", None, "key_column"), (None, "nope", "value_column"), ("id", "nope", "value_column")],
)
def test_csv_unknown_column_fails_the_whole_request(
    key_column: str | None, value_column: str | None, field: str
) -> None:
    with pytest.raises(ValidationFailed) as info:
        parse_csv(b"id,body\n1,a\n", key_column, value_column)

    assert info.value.field == field
    assert "nope" in info.value.message


def test_csv_unknown_column_is_checked_even_without_rows() -> None:
    with pytest.raises(ValidationFailed):
        parse_csv(b"id,body\n", "nope", None)


def test_csv_duplicate_header_names_are_rejected() -> None:
    with pytest.raises(ValidationFailed) as info:
        parse_csv(b"id,id\n1,2\n", None, None)

    assert info.value.field == "file"


def test_csv_that_is_not_utf8_is_rejected() -> None:
    with pytest.raises(ValidationFailed) as info:
        parse_csv(b"id,body\n1,\xff\xfe\n", "id", "body")

    assert info.value.field == "file"


def test_csv_cell_over_128_kb_is_accepted() -> None:
    content = b"id,body\n1," + b"x" * 200_000 + b"\n"

    [message] = parse_csv(content, "id", "body")

    assert isinstance(message, OutMessage)
    assert len(message.value) == 200_000


def test_csv_cell_over_the_upload_cap_is_still_rejected() -> None:
    content = b"id,body\n1," + b"x" * (10 * 1024 * 1024 + 1) + b"\n"

    with pytest.raises(ValidationFailed) as info:
        parse_csv(content, "id", "body")

    assert info.value.field == "file"


def test_csv_key_column_may_also_be_the_value_column() -> None:
    [message] = parse_csv(b"id,body,t\n7,hello,x\n", "id", "id")

    assert message == OutMessage(key=b"7", value=b"7", headers=[("body", b"hello"), ("t", b"x")])


# --- row cap ---------------------------------------------------------------------------------


def csv_rows(count: int) -> bytes:
    return b"a\n" + b"1\n" * count


def json_rows(count: int) -> bytes:
    return b"[" + b",".join([b'{"value":"v"}'] * count) + b"]"


def test_csv_with_exactly_the_maximum_rows_is_accepted() -> None:
    assert len(parse_csv(csv_rows(MAX_BULK_ROWS), None, None)) == MAX_BULK_ROWS


def test_csv_with_one_row_too_many_is_rejected() -> None:
    with pytest.raises(ValidationFailed) as info:
        parse_csv(csv_rows(MAX_BULK_ROWS + 1), None, None)

    assert info.value.code == "too_many_rows"
    assert info.value.field == "file"
    assert "100,000" in info.value.message


def test_csv_parsing_stops_at_the_first_row_over_the_cap(monkeypatch: pytest.MonkeyPatch) -> None:
    pulled = 0
    real_reader = csv.reader

    def counting_reader(*args, **kwargs):
        nonlocal pulled
        for row in real_reader(*args, **kwargs):
            pulled += 1
            yield row

    monkeypatch.setattr(csv, "reader", counting_reader)

    with pytest.raises(ValidationFailed):
        parse_csv(csv_rows(MAX_BULK_ROWS + 5_000), None, None)

    assert pulled == 1 + MAX_BULK_ROWS + 1  # header, the allowed rows, and the one over


def test_ragged_and_blank_rows_count_toward_the_cap() -> None:
    content = b"a,b\n" + b"1\n" * (MAX_BULK_ROWS + 1)

    with pytest.raises(ValidationFailed) as info:
        parse_csv(content, None, None)

    assert info.value.code == "too_many_rows"


# --- parse_json ------------------------------------------------------------------------------


def test_json_array_happy_path() -> None:
    content = json.dumps(
        [
            {"key": "k1", "value": "v1"},
            {"value": "v2", "headers": {"a": "1", "n": 2}},
            {"key": None, "value": "v3", "headers": [["x", "y"], ["z", 3]]},
        ]
    ).encode()

    assert parse_json(content) == [
        OutMessage(key=b"k1", value=b"v1", headers=[]),
        OutMessage(key=None, value=b"v2", headers=[("a", b"1"), ("n", b"2")]),
        OutMessage(key=None, value=b"v3", headers=[("x", b"y"), ("z", b"3")]),
    ]


def test_json_object_and_array_values_are_serialised() -> None:
    items = parse_json(b'[{"value": {"a": [1, 2]}}, {"value": [1, "x"]}, {"value": 42}]')

    assert [i.value for i in items if isinstance(i, OutMessage)] == [
        b'{"a": [1, 2]}',
        b'[1, "x"]',
        b"42",
    ]


def test_json_non_ascii_text_value_is_utf8() -> None:
    [message] = parse_json('[{"value": "olá"}]'.encode())

    assert isinstance(message, OutMessage)
    assert message.value == "olá".encode()


def test_json_decodes_a_utf8_bom() -> None:
    assert parse_json(BOM + b'[{"value": "x"}]') == [OutMessage(key=None, value=b"x", headers=[])]


def test_json_empty_array_is_empty() -> None:
    assert parse_json(b"[]") == []


def test_json_top_level_object_fails_the_request() -> None:
    with pytest.raises(ValidationFailed) as info:
        parse_json(b'{"value": "x"}')

    assert info.value.field == "file"


@pytest.mark.parametrize(
    "content",
    [b"[{", b"\xff\xfe", b"[1,]", b"[" * 200_000],
    ids=["truncated", "not_utf8", "trailing_comma", "too_deep"],
)
def test_json_that_cannot_be_parsed_fails_the_request(content: bytes) -> None:
    with pytest.raises(ValidationFailed) as info:
        parse_json(content)

    assert info.value.field == "file"


@pytest.mark.parametrize(
    ("item", "fragment"),
    [
        ('{"key": "k"}', "value"),
        ('"just a string"', "object"),
        ("42", "object"),
        ("null", "object"),
        ('{"value": "v", "key": 5}', "key"),
        ('{"value": "v", "headers": "a=b"}', "headers"),
        ('{"value": "v", "headers": [["only-one"]]}', "headers"),
        ('{"value": "\\ud800"}', "UTF-8"),
    ],
)
def test_json_bad_items_are_row_errors_and_do_not_stop_the_rest(item: str, fragment: str) -> None:
    items = parse_json(f'[{{"value": "first"}}, {item}, {{"value": "last"}}]'.encode())

    assert [type(i) for i in items] == [OutMessage, RowError, OutMessage]
    error = items[1]
    assert isinstance(error, RowError)
    assert error.row == 2
    assert fragment in error.error


def test_json_with_exactly_the_maximum_rows_is_accepted() -> None:
    assert len(parse_json(json_rows(MAX_BULK_ROWS))) == MAX_BULK_ROWS


def test_json_with_one_item_too_many_is_rejected() -> None:
    with pytest.raises(ValidationFailed) as info:
        parse_json(json_rows(MAX_BULK_ROWS + 1))

    assert info.value.code == "too_many_rows"
    assert info.value.field == "file"


def test_json_parsing_stops_before_decoding_the_item_over_the_cap() -> None:
    # Everything after the allowed items is garbage: only a parser that has not read on can
    # report the cap instead of a syntax error.
    content = json_rows(MAX_BULK_ROWS)[:-1] + b",{{{{ not json at all"

    with pytest.raises(ValidationFailed) as info:
        parse_json(content)

    assert info.value.code == "too_many_rows"


@pytest.mark.parametrize(
    "content",
    [b"[1 2]", b"[1,]", b"[,1]", b"[1", b"[]]", b"[] x", b"[", b'[{"value": 1}, ]'],
    ids=[
        "no_comma",
        "trailing_comma",
        "leading_comma",
        "unterminated",
        "extra_bracket",
        "extra",
        "open",
        "trailing_comma_object",
    ],
)
def test_json_syntax_errors_are_still_rejected_by_the_incremental_parser(content: bytes) -> None:
    with pytest.raises(ValidationFailed) as info:
        parse_json(content)

    assert info.value.field == "file"


def test_json_array_surrounded_by_whitespace_is_fine() -> None:
    assert parse_json(b' \n[ {"value": "a"} ,\n {"value": "b"} ]\n ') == [
        OutMessage(key=None, value=b"a", headers=[]),
        OutMessage(key=None, value=b"b", headers=[]),
    ]


# --- lone surrogates -------------------------------------------------------------------------


def test_json_row_with_a_lone_surrogate_in_a_header_is_a_row_error() -> None:
    [first, second] = parse_json(
        b'[{"value": "v", "headers": {"h": "\\ud800"}},'
        b' {"value": "v", "headers": {"\\ud800": "x"}}]'
    )

    assert isinstance(first, RowError) and "UTF-8" in first.error
    assert isinstance(second, RowError) and "UTF-8" in second.error


@pytest.mark.parametrize("text", ["h=\ud800", '{"h": "\ud800"}', '{"\ud800": "x"}', "\ud800=x"])
def test_headers_with_a_lone_surrogate_are_a_validation_error(text: str) -> None:
    with pytest.raises(ValidationFailed) as info:
        parse_headers(text)

    assert info.value.field == "headers"
