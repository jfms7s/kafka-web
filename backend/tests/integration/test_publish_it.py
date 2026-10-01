"""Publishing against a real broker; what was published is read back with the snapshot endpoint."""

import contextlib
import json
import time
from collections.abc import Callable, Iterator
from typing import Any

import pytest
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient
from fastapi.testclient import TestClient

from tests.integration.conftest import _free_ports, _plaintext_env, _start, _wait_ready

pytestmark = pytest.mark.integration


@pytest.fixture
def cluster(api: TestClient, kafka_plaintext: str) -> str:
    body: dict[str, Any] = {"name": "local", "env": "dev", "bootstrap_servers": kafka_plaintext}
    assert api.post("/api/clusters", json=body).status_code == 201
    return "local"


@pytest.fixture
def topic(topic_factory: Callable[..., str]) -> str:
    return topic_factory(partitions=3)


def read_back(api: TestClient, cluster: str, topic: str, count: int = 1000) -> list[dict[str, Any]]:
    response = api.get(
        f"/api/clusters/{cluster}/topics/{topic}/messages",
        params={"start": "earliest", "count": count, "timeout": 30},
    )
    assert response.status_code == 200, response.text
    return response.json()["messages"]


def bulk(api: TestClient, cluster: str, topic: str, content: bytes, **fields: str):
    return api.post(
        f"/api/clusters/{cluster}/topics/{topic}/messages/bulk",
        data={"confirm": topic, **fields},
        files={"file": ("upload", content)},
    )


def test_single_publish_round_trips_key_value_and_headers(
    api: TestClient, cluster: str, topic: str
) -> None:
    body = {
        "key": "order-1",
        "value": '{"total": 12.5}',
        "headers": "trace=t-1\nsource=web",
        "partition": 2,
    }

    response = api.post(f"/api/clusters/{cluster}/topics/{topic}/messages", json=body)

    assert response.status_code == 200, response.text
    assert response.json() == {"ok": True, "partition": 2, "offset": 0}
    [message] = read_back(api, cluster, topic)
    assert (message["partition"], message["offset"]) == (2, 0)
    assert message["key"]["data"] == "order-1"
    assert message["value"]["json_value"] == {"total": 12.5}
    assert [(h["key"], h["value"]["data"]) for h in message["headers"]] == [
        ("trace", "t-1"),
        ("source", "web"),
    ]


def test_single_publish_with_json_headers_and_no_key(
    api: TestClient, cluster: str, topic: str
) -> None:
    body = {"value": "plain text", "headers": '{"n": 3, "who": "me"}'}

    assert (
        api.post(f"/api/clusters/{cluster}/topics/{topic}/messages", json=body).status_code == 200
    )

    [message] = read_back(api, cluster, topic)
    assert message["key"]["data"] is None
    assert message["value"]["data"] == "plain text"
    assert {h["key"]: h["value"]["data"] for h in message["headers"]} == {"n": "3", "who": "me"}


@pytest.fixture
def auto_create_kafka() -> Iterator[str]:
    """Bootstrap of a broker of its own that *would* create a topic on first use."""
    [port] = _free_ports(1)
    env = {**_plaintext_env(port), "KAFKA_AUTO_CREATE_TOPICS_ENABLE": "true"}
    container = _start(env, [port], {})
    try:
        bootstrap = f"localhost:{port}"
        _wait_ready(container, {"bootstrap.servers": bootstrap})
        yield bootstrap
    finally:
        with contextlib.suppress(Exception):
            container.stop()


def _topic_names(bootstrap: str) -> set[str]:
    return set(AdminClient({"bootstrap.servers": bootstrap}).list_topics(timeout=10).topics)


def test_publishing_never_auto_creates_a_topic_even_if_the_broker_would(
    auto_create_kafka: str, api: TestClient
) -> None:
    # Control: this broker really does create a topic that a producer merely writes to.
    probe = Producer({"bootstrap.servers": auto_create_kafka})
    probe.produce("created-by-producer", value=b"x")
    probe.flush(10)
    deadline = time.monotonic() + 15
    while "created-by-producer" not in _topic_names(auto_create_kafka):
        assert time.monotonic() < deadline, "the broker does not auto-create topics"
        time.sleep(0.2)
    body: dict[str, Any] = {"name": "auto", "env": "dev", "bootstrap_servers": auto_create_kafka}
    assert api.post("/api/clusters", json=body).status_code == 201

    single = api.post("/api/clusters/auto/topics/ghost-single/messages", json={"value": "x"})
    bulk_response = api.post(
        "/api/clusters/auto/topics/ghost-bulk/messages/bulk",
        data={"confirm": "ghost-bulk"},
        files={"file": ("rows.csv", b"a\n1\n")},
    )

    assert single.status_code == bulk_response.status_code == 404
    assert single.json()["code"] == bulk_response.json()["code"] == "topic_not_found"
    time.sleep(1)  # a stray auto-creation would have happened by now
    names = _topic_names(auto_create_kafka)
    assert not {"ghost-single", "ghost-bulk"} & names


def test_single_publish_to_a_missing_partition_is_422(
    api: TestClient, cluster: str, topic: str
) -> None:
    response = api.post(
        f"/api/clusters/{cluster}/topics/{topic}/messages", json={"value": "x", "partition": 3}
    )

    assert response.status_code == 422
    assert response.json()["field"] == "partition"


def test_csv_upload_of_50_rows_is_published_and_readable(
    api: TestClient, cluster: str, topic: str
) -> None:
    rows = "\n".join(f"{i},payload-{i},t{i}" for i in range(50))
    content = f"id,body,trace\n{rows}\n".encode()

    response = bulk(api, cluster, topic, content, key_column="id", value_column="body")

    assert response.status_code == 200, response.text
    result = response.json()
    assert (result["succeeded"], result["failed"]) == (50, 0)
    assert all(r["ok"] and r["offset"] is not None for r in result["results"])
    messages = read_back(api, cluster, topic)
    assert sorted(int(m["key"]["data"]) for m in messages) == list(range(50))
    sample = next(m for m in messages if m["key"]["data"] == "7")
    assert sample["value"]["data"] == "payload-7"
    assert [(h["key"], h["value"]["data"]) for h in sample["headers"]] == [("trace", "t7")]


def test_csv_without_a_value_column_sends_rows_as_json(
    api: TestClient, cluster: str, topic: str
) -> None:
    assert bulk(api, cluster, topic, b"id,name\n1,Ann\n", key_column="id").status_code == 200

    [message] = read_back(api, cluster, topic)
    assert message["key"]["data"] == "1"
    assert message["value"]["json_value"] == {"name": "Ann"}


def test_json_upload_with_one_bad_row_publishes_the_rest(
    api: TestClient, cluster: str, topic: str
) -> None:
    items: list[Any] = [{"key": f"k{i}", "value": {"n": i}} for i in range(5)]
    items[2] = {"key": "k2"}  # no value
    content = json.dumps(items).encode()

    response = bulk(api, cluster, topic, content)

    assert response.status_code == 200, response.text
    result = response.json()
    assert (result["succeeded"], result["failed"]) == (4, 1)
    bad = result["results"][2]
    assert (bad["row"], bad["ok"]) == (3, False) and "value" in bad["error"]
    messages = read_back(api, cluster, topic)
    assert sorted(m["key"]["data"] for m in messages) == ["k0", "k1", "k3", "k4"]


def test_upload_is_refused_without_the_typed_confirmation(
    api: TestClient, cluster: str, topic: str
) -> None:
    response = bulk(api, cluster, topic, b"a\n1\n", confirm="wrong")

    assert response.status_code == 422
    assert response.json()["code"] == "confirmation_mismatch"
    assert read_back(api, cluster, topic) == []


def test_upload_to_a_read_only_cluster_is_403(
    api: TestClient, kafka_plaintext: str, topic: str
) -> None:
    body: dict[str, Any] = {
        "name": "ro",
        "env": "prd",
        "bootstrap_servers": kafka_plaintext,
        "read_only": True,
    }
    assert api.post("/api/clusters", json=body).status_code == 201

    response = bulk(api, "ro", topic, b"a\n1\n")

    assert response.status_code == 403
    assert response.json()["code"] == "read_only_cluster"
