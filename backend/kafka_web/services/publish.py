"""Message publishing (spec §5): header and bulk-file parsing, and delivery over a producer."""

import csv
import io
import json
import re
import time
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, Literal

from confluent_kafka import KafkaError, KafkaException, Message, Producer

from kafka_web.errors import AppError, BrokerError, KafkaTimeout, NotFound, ValidationFailed
from kafka_web.kafka.errors import call_with_timeout, map_kafka_exception, redact

DEFAULT_FLUSH_TIMEOUT_S = 30.0
_QUEUE_FULL_POLL_S = 0.5
_REPORT_GRACE_S = 0.5  # how long to wait for delivery reports another thread is still serving
_METADATA_TIMEOUT_S = 10.0

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


# --- delivery ----------------------------------------------------------------------------------


@dataclass
class RowResult:
    row: int
    ok: bool
    partition: int | None = None
    offset: int | None = None
    error: str | None = None


@dataclass
class BatchResult:
    succeeded: int
    failed: int
    results: list[RowResult]


class _Delivery:
    """What happened to one message. Written by the delivery callback, which librdkafka runs on
    whichever thread polls or flushes the (shared) producer, and read after the flush."""

    def __init__(self) -> None:
        self.queued = False
        self.reported = False
        self.partition: int | None = None
        self.offset: int | None = None
        self.cause: Exception | None = None  # a KafkaError / KafkaException / ValueError ...
        self.error: str | None = None

    def fail(self, error: str, cause: Exception | None = None) -> None:
        self.error, self.cause = error, cause

    def on_delivery(self, err: KafkaError | None, message: Message) -> None:
        if err is not None:
            self.fail(redact(err.str()) or err.name(), err)
        else:
            self.partition, self.offset = message.partition(), message.offset()
        self.reported = True

    @property
    def delivered(self) -> bool:
        return self.queued and self.reported and self.error is None

    def failure(self) -> str | None:
        if self.error is not None:
            return self.error
        return None if self.delivered else "delivery timeout"


def _kwargs(message: OutMessage, delivery: _Delivery) -> dict[str, Any]:
    kwargs: dict[str, Any] = {
        "value": message.value,
        "key": message.key,
        "headers": message.headers or None,
        "on_delivery": delivery.on_delivery,
    }
    if message.partition is not None:
        kwargs["partition"] = message.partition
    return kwargs


def _produce(
    producer: Producer, topic: str, message: OutMessage, delivery: _Delivery, deadline: float
) -> None:
    """Queue one message, waiting out a full local queue until `deadline`."""
    while True:
        try:
            producer.produce(topic, **_kwargs(message, delivery))
        except BufferError as exc:
            if time.monotonic() >= deadline:
                delivery.fail("Producer queue is full", exc)
                return
            producer.poll(_QUEUE_FULL_POLL_S)
        except (KafkaException, ValueError, TypeError) as exc:
            detail = exc.args[0].str() if isinstance(exc, KafkaException) else str(exc)
            delivery.fail(redact(str(detail)) or type(exc).__name__, exc)
            return
        else:
            delivery.queued = True
            return


def _send(
    producer: Producer, topic: str, messages: Sequence[OutMessage], flush_timeout: float
) -> list[_Delivery]:
    deliveries = [_Delivery() for _ in messages]
    deadline = time.monotonic() + flush_timeout
    for message, delivery in zip(messages, deliveries, strict=True):
        _produce(producer, topic, message, delivery, deadline)
        producer.poll(0)  # serve delivery reports as we go
    unflushed = producer.flush(flush_timeout)
    if unflushed == 0:
        # The queue is empty, but a report taken off it by another thread may still be running.
        grace = time.monotonic() + _REPORT_GRACE_S
        while time.monotonic() < grace and not all(d.reported for d in deliveries if d.queued):
            producer.poll(0.05)
    return deliveries


def publish(
    producer: Producer,
    topic: str,
    items: Sequence[Item],
    flush_timeout: float = DEFAULT_FLUSH_TIMEOUT_S,
) -> BatchResult:
    """Produce every message, then flush; report each row. A `RowError` is a failed row and
    does not stop the batch. Row numbers of messages are their 1-based position in `items`."""
    sendable = [
        (row, item) for row, item in enumerate(items, start=1) if isinstance(item, OutMessage)
    ]
    deliveries = iter(_send(producer, topic, [m for _, m in sendable], flush_timeout))
    results = []
    for row, item in enumerate(items, start=1):
        if isinstance(item, RowError):
            results.append(RowResult(item.row, ok=False, error=item.error))
            continue
        delivery = next(deliveries)
        error = delivery.failure()
        if error is None:
            results.append(
                RowResult(row, ok=True, partition=delivery.partition, offset=delivery.offset)
            )
        else:
            results.append(RowResult(row, ok=False, error=error))
    failed = sum(1 for r in results if not r.ok)
    return BatchResult(succeeded=len(results) - failed, failed=failed, results=results)


def _failure_as_app_error(delivery: _Delivery) -> AppError:
    cause = delivery.cause
    if cause is None:
        return KafkaTimeout("The message was not acknowledged in time")
    if isinstance(cause, ValueError | TypeError):
        return ValidationFailed(delivery.error or "The message was rejected")
    if isinstance(cause, BufferError):
        return BrokerError("The producer queue is full", code="producer_queue_full")
    return map_kafka_exception(cause)


def publish_one(
    producer: Producer,
    topic: str,
    message: OutMessage,
    flush_timeout: float = DEFAULT_FLUSH_TIMEOUT_S,
) -> tuple[int, int]:
    """Produce one message and return its `(partition, offset)`; a failure raises the mapped
    `AppError` (unlike `publish`, which reports it as a failed row)."""
    [delivery] = _send(producer, topic, [message], flush_timeout)
    if not delivery.delivered:
        raise _failure_as_app_error(delivery)
    assert delivery.partition is not None and delivery.offset is not None
    return delivery.partition, delivery.offset


def require_topic(admin: Any, topic: str, timeout: float = _METADATA_TIMEOUT_S) -> int:
    """The partition count of `topic`; 404 `topic_not_found` if the cluster has no such topic.

    Looks the topic up in the full listing, never by asking the brokers about that one name: a
    broker with `auto.create.topics.enable` may create a topic that is merely asked about, and
    publishing must never create one.
    """
    metadata = call_with_timeout(lambda: admin.list_topics(timeout=timeout))
    found = metadata.topics.get(topic)
    if found is None:
        raise NotFound(f"Topic {topic!r} does not exist", code="topic_not_found")
    if found.error is not None:
        raise map_kafka_exception(found.error)
    return len(found.partitions)
