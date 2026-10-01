from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from confluent_kafka import KafkaError, KafkaException
from fastapi.testclient import TestClient

from kafka_web.api.app import create_app
from kafka_web.config.models import ClusterInput
from kafka_web.config.secrets import SecretStore
from kafka_web.config.store import ClusterStore
from kafka_web.kafka.registry import ConnectionRegistry
from tests.conftest import MemoryKeyring
from tests.fakes import Fakes, FakeTopicAdmin, config_entry, partition, topic_meta

LOCAL = "http://127.0.0.1:8000"


@pytest.fixture
def store(tmp_path: Path, memory_keyring: MemoryKeyring) -> ClusterStore:
    return ClusterStore(tmp_path / "cfg", SecretStore(memory_keyring))


@pytest.fixture
def admin() -> FakeTopicAdmin:
    return FakeTopicAdmin(
        {
            "orders.v1_x-y": topic_meta("orders.v1_x-y", [partition(0, 1, [1, 2], [1])]),
            "__consumer_offsets": topic_meta("__consumer_offsets", [partition(0, 1, [1], [1])]),
        },
        {
            "orders.v1_x-y": {
                "retention.ms": config_entry("retention.ms", "604800000"),
                "cleanup.policy": config_entry("cleanup.policy", "delete", default=True),
            }
        },
    )


@pytest.fixture
def client(store: ClusterStore, admin: FakeTopicAdmin) -> Iterator[TestClient]:
    store.create(
        ClusterInput.model_validate({"name": "dev", "env": "dev", "bootstrap_servers": "b1:9092"})
    )
    registry = ConnectionRegistry(
        store, admin_factory=lambda conf: admin, producer_factory=Fakes().producer
    )
    with TestClient(create_app(store=store, registry=registry), base_url=LOCAL) as client:
        yield client


def test_list_topics(client: TestClient) -> None:
    response = client.get("/api/clusters/dev/topics")

    assert response.status_code == 200
    assert response.json() == {
        "topics": [
            {
                "name": "__consumer_offsets",
                "partitions": 1,
                "replication_factor": 1,
                "internal": True,
            },
            {"name": "orders.v1_x-y", "partitions": 1, "replication_factor": 2, "internal": False},
        ]
    }


def test_topic_config_with_encoded_name(client: TestClient) -> None:
    response = client.get("/api/clusters/dev/topics/orders.v1_x-y/config")

    assert response.status_code == 200
    body: dict[str, Any] = response.json()
    assert body["name"] == "orders.v1_x-y"
    assert body["replication_factor"] == 2
    assert body["partitions"] == [{"id": 0, "leader": 1, "replicas": [1, 2], "isr": [1]}]
    assert [(e["name"], e["display_value"], e["is_default"]) for e in body["entries"]] == [
        ("retention.ms", "7d", False),
        ("cleanup.policy", "delete", True),
    ]
    assert body["entries"][0]["value"] == "604800000"


def test_unknown_topic_is_404(client: TestClient) -> None:
    response = client.get("/api/clusters/dev/topics/ghost/config")

    assert response.status_code == 404
    assert response.json()["code"] == "topic_not_found"


def test_unknown_cluster_is_404(client: TestClient) -> None:
    for path in ("/api/clusters/nope/topics", "/api/clusters/nope/topics/t/config"):
        response = client.get(path)
        assert response.status_code == 404
        assert response.json()["code"] == "cluster_not_found"


def test_broker_failure_uses_the_standard_error_body(
    client: TestClient, admin: FakeTopicAdmin
) -> None:
    client.get("/api/clusters/dev/topics")  # connect while the broker is "up"

    def down(topic=None, timeout=None):
        raise KafkaException(KafkaError(KafkaError._ALL_BROKERS_DOWN, "all brokers down"))

    admin.list_topics = down  # type: ignore[method-assign]

    response = client.get("/api/clusters/dev/topics")

    assert response.status_code == 502
    assert response.json() == {"code": "broker_unreachable", "message": "all brokers down"}
