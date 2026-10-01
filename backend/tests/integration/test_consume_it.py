"""Snapshot consumption against a real broker."""

import time
from collections.abc import Callable
from typing import Any

import pytest
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient
from fastapi.testclient import TestClient

pytestmark = pytest.mark.integration

BASE_TS = 1_700_000_000_000
BINARY = b"\xff\xfe\x00\x01"


@pytest.fixture
def cluster(api: TestClient, kafka_plaintext: str) -> str:
    body: dict[str, Any] = {"name": "local", "env": "dev", "bootstrap_servers": kafka_plaintext}
    assert api.post("/api/clusters", json=body).status_code == 201
    return "local"


@pytest.fixture
def filled_topic(kafka_plaintext: str, topic_factory: Callable[..., str]) -> str:
    """30 messages over 3 partitions: key k<i> on partition i % 3, timestamp BASE_TS + i s.

    Even i carry a JSON value, odd i a binary one; every message has a text and a binary header.
    """
    topic = topic_factory(partitions=3)
    producer = Producer({"bootstrap.servers": kafka_plaintext})
    for i in range(30):
        value = f'{{"n": {i}}}'.encode() if i % 2 == 0 else BINARY
        producer.produce(
            topic,
            key=f"k{i}".encode(),
            value=value,
            partition=i % 3,
            timestamp=BASE_TS + i * 1000,
            headers=[("trace", f"t{i}".encode()), ("bin", BINARY)],
        )
    assert producer.flush(10) == 0
    return topic


def snapshot(api: TestClient, cluster: str, topic: str, **params: Any) -> list[dict[str, Any]]:
    response = api.get(f"/api/clusters/{cluster}/topics/{topic}/messages", params=params)
    assert response.status_code == 200, response.text
    return response.json()["messages"]


def keys(messages: list[dict[str, Any]]) -> list[str]:
    return [m["key"]["data"] for m in messages]


def test_earliest_returns_everything_and_stops_early(
    api: TestClient, cluster: str, filled_topic: str
) -> None:
    started = time.monotonic()
    messages = snapshot(api, cluster, filled_topic, start="earliest", count=100, timeout=30)
    elapsed = time.monotonic() - started

    assert elapsed < 3, f"early stop expected, took {elapsed:.1f}s"
    assert keys(messages) == [f"k{i}" for i in range(30)]  # sorted by timestamp
    first, second = messages[0], messages[1]
    assert (first["partition"], first["offset"]) == (0, 0)
    assert first["timestamp"] == BASE_TS and first["timestamp_type"] == "create"
    assert first["value"]["is_json"] and first["value"]["json_value"] == {"n": 0}
    assert second["value"]["encoding"] == "base64" and second["value"]["data"] == "//4AAQ=="
    assert [(h["key"], h["value"]["encoding"]) for h in first["headers"]] == [
        ("trace", "utf-8"),
        ("bin", "base64"),
    ]


def test_count_limits_earliest(api: TestClient, cluster: str, filled_topic: str) -> None:
    messages = snapshot(api, cluster, filled_topic, start="earliest", count=4, timeout=30)

    assert len(messages) == 4


def test_latest_returns_the_newest_messages(
    api: TestClient, cluster: str, filled_topic: str
) -> None:
    messages = snapshot(api, cluster, filled_topic, start="latest", count=5)

    assert keys(messages) == ["k25", "k26", "k27", "k28", "k29"]


def test_offset_on_one_partition(api: TestClient, cluster: str, filled_topic: str) -> None:
    messages = snapshot(
        api, cluster, filled_topic, start="offset", partition=1, offset=3, count=100
    )

    assert {m["partition"] for m in messages} == {1}
    assert [m["offset"] for m in messages] == list(range(3, 10))
    assert keys(messages)[0] == "k10"  # partition 1 holds k1, k4, k7, k10, ...


def test_offset_past_the_end_returns_nothing(
    api: TestClient, cluster: str, filled_topic: str
) -> None:
    assert snapshot(api, cluster, filled_topic, start="offset", partition=0, offset=999) == []


def test_timestamp_starts_at_the_first_message_not_older(
    api: TestClient, cluster: str, filled_topic: str
) -> None:
    messages = snapshot(
        api, cluster, filled_topic, start="timestamp", timestamp=BASE_TS + 10_000, count=100
    )

    assert keys(messages) == [f"k{i}" for i in range(10, 30)]


def test_timestamp_after_every_message_returns_nothing(
    api: TestClient, cluster: str, filled_topic: str
) -> None:
    assert snapshot(api, cluster, filled_topic, start="timestamp", timestamp=BASE_TS + 10**9) == []


def test_null_key_and_tombstone(
    api: TestClient,
    cluster: str,
    kafka_plaintext: str,
    topic_factory: Callable[..., str],
) -> None:
    topic = topic_factory(partitions=1)
    producer = Producer({"bootstrap.servers": kafka_plaintext})
    producer.produce(topic, key=None, value=b"null")
    producer.produce(topic, key=BINARY, value=None)
    assert producer.flush(10) == 0

    first, second = snapshot(api, cluster, topic, start="earliest")

    assert first["key"]["encoding"] == "null"
    assert first["value"]["is_json"] and first["value"]["json_value"] is None
    assert second["key"]["encoding"] == "base64"
    assert second["value"]["encoding"] == "null"


@pytest.mark.parametrize("start", ["earliest", "latest"])
def test_snapshot_empty_topic_returns_fast(
    api: TestClient, cluster: str, topic_factory: Callable[..., str], start: str
) -> None:
    topic = topic_factory(partitions=3)

    started = time.monotonic()
    messages = snapshot(api, cluster, topic, start=start, timeout=30)
    elapsed = time.monotonic() - started

    assert messages == []
    assert elapsed < 2, f"empty topic took {elapsed:.1f}s"


def test_unknown_topic_is_404(api: TestClient, cluster: str) -> None:
    response = api.get(f"/api/clusters/{cluster}/topics/no-such-topic/messages")

    assert response.status_code == 404
    assert response.json()["code"] == "topic_not_found"


def test_partition_that_does_not_exist_is_422(
    api: TestClient, cluster: str, filled_topic: str
) -> None:
    response = api.get(
        f"/api/clusters/{cluster}/topics/{filled_topic}/messages", params={"partition": 9}
    )

    assert response.status_code == 422
    assert response.json()["field"] == "partition"


def test_browsing_leaves_no_consumer_group(
    api: TestClient, cluster: str, filled_topic: str, kafka_plaintext: str
) -> None:
    for start in ("earliest", "latest"):
        snapshot(api, cluster, filled_topic, start=start, count=10, timeout=5)
    snapshot(api, cluster, filled_topic, start="offset", partition=0, offset=0, timeout=5)

    admin = AdminClient({"bootstrap.servers": kafka_plaintext})
    groups = admin.list_consumer_groups().result(timeout=10)

    assert [g.group_id for g in groups.valid if g.group_id.startswith("kafka-web-")] == []
    assert groups.errors == []
