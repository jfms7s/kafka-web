from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient

from kafka_web.api import messages as messages_api
from kafka_web.api.app import create_app
from kafka_web.config.models import ClusterInput
from kafka_web.config.secrets import SecretStore
from kafka_web.config.store import ClusterStore
from kafka_web.kafka.registry import ConnectionRegistry
from kafka_web.services.consume import SnapshotParams
from kafka_web.services.decode import to_message_view
from tests.conftest import MemoryKeyring
from tests.fakes import FakeMessage, Fakes

LOCAL = "http://127.0.0.1:8000"


@pytest.fixture
def calls(monkeypatch: pytest.MonkeyPatch) -> list[dict[str, Any]]:
    recorded: list[dict[str, Any]] = []

    def fake_consume(client_config: dict[str, str], topic: str, params: SnapshotParams):
        recorded.append({"conf": client_config, "topic": topic, "params": params})
        return [to_message_view(FakeMessage(partition=1, offset=4, key=b"k", value=b'{"a": 1}'))]

    monkeypatch.setattr(messages_api, "consume_snapshot", fake_consume)
    return recorded


@pytest.fixture
def client(tmp_path: Path, memory_keyring: MemoryKeyring) -> Iterator[TestClient]:
    store = ClusterStore(tmp_path / "cfg", SecretStore(memory_keyring))
    store.create(
        ClusterInput.model_validate({"name": "dev", "env": "dev", "bootstrap_servers": "b1:9092"})
    )
    fakes = Fakes()
    registry = ConnectionRegistry(store, admin_factory=fakes.admin, producer_factory=fakes.producer)
    with TestClient(create_app(store=store, registry=registry), base_url=LOCAL) as client:
        yield client


def test_returns_messages_wrapped_in_an_object(client: TestClient, calls: list) -> None:
    response = client.get("/api/clusters/dev/topics/orders/messages")

    assert response.status_code == 200
    [message] = response.json()["messages"]
    assert (message["partition"], message["offset"], message["timestamp_type"]) == (1, 4, "create")
    assert message["key"]["data"] == "k"
    assert message["value"]["json_value"] == {"a": 1}
    assert message["headers"] == []


def test_defaults_and_cluster_config_reach_the_service(client: TestClient, calls: list) -> None:
    client.get("/api/clusters/dev/topics/orders/messages")

    [call] = calls
    assert call["topic"] == "orders"
    assert call["conf"]["bootstrap.servers"] == "b1:9092"
    assert call["params"] == SnapshotParams()


def test_query_parameters_are_parsed(client: TestClient, calls: list) -> None:
    query = "count=7&timeout=3.5&start=offset&offset=12&partition=2"
    client.get(f"/api/clusters/dev/topics/orders/messages?{query}")

    assert calls[0]["params"] == SnapshotParams(
        count=7, timeout=3.5, start="offset", offset=12, partition=2
    )


def test_timestamp_query(client: TestClient, calls: list) -> None:
    client.get("/api/clusters/dev/topics/orders/messages?start=timestamp&timestamp=1700000000000")

    assert calls[0]["params"].timestamp == 1_700_000_000_000


def test_unknown_cluster_is_404(client: TestClient, calls: list) -> None:
    response = client.get("/api/clusters/nope/topics/orders/messages")

    assert response.status_code == 404
    assert response.json()["code"] == "cluster_not_found"
    assert calls == []


@pytest.mark.parametrize(
    ("query", "field"),
    [
        ("count=0", "count"),
        ("count=10001", "count"),
        ("timeout=0", "timeout"),
        ("timeout=61", "timeout"),
        ("start=middle", "start"),
        ("count=abc", "count"),
    ],
)
def test_out_of_range_parameters_are_422(
    client: TestClient, calls: list, query: str, field: str
) -> None:
    response = client.get(f"/api/clusters/dev/topics/orders/messages?{query}")

    assert response.status_code == 422
    assert response.json()["field"] == field
    assert calls == []


@pytest.mark.parametrize(
    ("query", "field"),
    [
        ("start=offset&offset=3", "partition"),
        ("start=offset&partition=0", "offset"),
        ("start=timestamp", "timestamp"),
    ],
)
def test_incomplete_start_modes_are_422_with_field(
    client: TestClient, query: str, field: str
) -> None:
    # No monkeypatch: validation runs in the real service, before any consumer is created.
    response = client.get(f"/api/clusters/dev/topics/orders/messages?{query}")

    assert response.status_code == 422
    assert response.json()["code"] == "validation_failed"
    assert response.json()["field"] == field


@pytest.mark.parametrize(
    "payload",
    [b'{"a": "\\ud800"}', b"[" * 255 + b"]" * 255, b"[" * 5000 + b"]" * 5000],
    ids=["lone_surrogate", "255_deep", "5000_deep"],
)
def test_json_the_response_cannot_carry_does_not_fail_the_snapshot(
    client: TestClient, monkeypatch: pytest.MonkeyPatch, payload: bytes
) -> None:
    view = to_message_view(FakeMessage(key=payload, value=payload, headers=[("h", payload)]))
    monkeypatch.setattr(messages_api, "consume_snapshot", lambda *args: [view])

    response = client.get("/api/clusters/dev/topics/orders/messages")

    assert response.status_code == 200
    [message] = response.json()["messages"]
    assert message["value"]["is_json"] is False
    assert message["value"]["data"] == payload.decode()
