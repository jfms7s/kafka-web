"""Viewing a missing topic must never create it, even on a broker that auto-creates topics.

A by-name metadata request is enough to make such a broker create a topic, and GET requests are
not origin-checked, so this would otherwise let any web page create topics on a read-only cluster.
"""

import time
from typing import Any

import pytest
from confluent_kafka.admin import AdminClient, NewTopic
from fastapi.testclient import TestClient

pytestmark = pytest.mark.integration

GHOSTS = {"ghost-cfg", "ghost-reset", "ghost-snap"}


def _topic_names(bootstrap: str) -> set[str]:
    return set(AdminClient({"bootstrap.servers": bootstrap}).list_topics(timeout=10).topics)


@pytest.fixture
def auto_cluster(api: TestClient, auto_create_kafka: str) -> str:
    body: dict[str, Any] = {"name": "auto", "env": "dev", "bootstrap_servers": auto_create_kafka}
    assert api.post("/api/clusters", json=body).status_code == 201
    return "auto"


def test_viewing_a_missing_topic_never_creates_it(
    api: TestClient, auto_cluster: str, auto_create_kafka: str
) -> None:
    admin = AdminClient({"bootstrap.servers": auto_create_kafka})
    admin.create_topics([NewTopic("real", 1, 1)])["real"].result(timeout=10)
    created = api.post(
        f"/api/clusters/{auto_cluster}/groups",
        json={"group_id": "g", "topic": "real", "start": "earliest"},
    )
    assert created.status_code == 201, created.text

    config = api.get(f"/api/clusters/{auto_cluster}/topics/ghost-cfg/config")
    reset = api.post(
        f"/api/clusters/{auto_cluster}/groups/g/reset",
        json={"topic": "ghost-reset", "strategy": "latest", "confirm": "g"},
    )
    snapshot = api.get(f"/api/clusters/{auto_cluster}/topics/ghost-snap/messages")

    for response in (config, reset, snapshot):
        assert response.status_code == 404, response.text
        assert response.json()["code"] == "topic_not_found"
    time.sleep(1)  # a stray auto-creation would have happened by now
    assert not GHOSTS & _topic_names(auto_create_kafka)
