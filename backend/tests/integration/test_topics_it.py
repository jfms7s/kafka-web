"""Topic endpoints against a real broker."""

from collections.abc import Callable
from typing import Any

import pytest
from fastapi.testclient import TestClient

pytestmark = pytest.mark.integration


@pytest.fixture
def cluster(api: TestClient, kafka_plaintext: str) -> str:
    body: dict[str, Any] = {"name": "local", "env": "dev", "bootstrap_servers": kafka_plaintext}
    assert api.post("/api/clusters", json=body).status_code == 201
    return "local"


def test_list_contains_created_topic_and_flags_internal(
    api: TestClient, cluster: str, topic_factory: Callable[..., str]
) -> None:
    name = topic_factory(partitions=3)

    response = api.get(f"/api/clusters/{cluster}/topics")

    assert response.status_code == 200
    topics = {t["name"]: t for t in response.json()["topics"]}
    assert topics[name] == {
        "name": name,
        "partitions": 3,
        "replication_factor": 1,
        "internal": False,
    }
    assert list(topics) == sorted(topics)


def test_config_humanizes_and_flags_non_default(
    api: TestClient, cluster: str, topic_factory: Callable[..., str]
) -> None:
    name = topic_factory(partitions=3, config={"retention.ms": "3600000"})

    response = api.get(f"/api/clusters/{cluster}/topics/{name}/config")

    assert response.status_code == 200
    body = response.json()
    assert body["name"] == name
    assert body["replication_factor"] == 1
    assert [p["id"] for p in body["partitions"]] == [0, 1, 2]
    assert all(p["leader"] >= 0 and p["isr"] and p["replicas"] for p in body["partitions"])
    entries = {e["name"]: e for e in body["entries"]}
    retention = entries["retention.ms"]
    assert retention["display_value"] == "1h"
    assert retention["value"] == "3600000"
    assert retention["is_default"] is False
    assert entries["cleanup.policy"]["is_default"] is True
    assert body["entries"][0]["name"] == "retention.ms"  # non-default entries sort first


def test_unknown_topic_is_404(api: TestClient, cluster: str) -> None:
    response = api.get(f"/api/clusters/{cluster}/topics/no-such-topic/config")

    assert response.status_code == 404
    assert response.json()["code"] == "topic_not_found"
