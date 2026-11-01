from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import pytest
from confluent_kafka import KafkaError
from fastapi.testclient import TestClient

from kafka_web.api.app import create_app
from kafka_web.config.models import ClusterInput
from kafka_web.config.secrets import SecretStore
from kafka_web.config.store import ClusterStore
from kafka_web.kafka.registry import ConnectionRegistry
from tests.conftest import MemoryKeyring
from tests.fakes import (
    FakeGroupAdmin,
    Fakes,
    group_description,
    group_listing,
    kafka_exc,
    member,
)

LOCAL = "http://127.0.0.1:8000"
GROUPS = "/api/clusters/dev/groups"


class Harness:
    def __init__(self, client: TestClient, registry: ConnectionRegistry, admin: FakeGroupAdmin):
        self.client = client
        self.registry = registry
        self.admin = admin


@pytest.fixture
def harness(tmp_path: Path, memory_keyring: MemoryKeyring) -> Iterator[Harness]:
    store = ClusterStore(tmp_path / "cfg", SecretStore(memory_keyring))
    store.create(ClusterInput(name="dev", env="dev", bootstrap_servers="b:9092"))
    store.create(ClusterInput(name="prd", env="prd", bootstrap_servers="b:9092", read_only=True))
    admin = FakeGroupAdmin(
        listings=[
            group_listing("billing", "STABLE", "CONSUMER"),
            group_listing("audit", "EMPTY", "CLASSIC"),
            group_listing("Billing-batch", "EMPTY", "CLASSIC"),
        ],
        descriptions={
            "billing": group_description(
                "billing",
                "STABLE",
                type="CONSUMER",
                members=[member("m1", "svc", "/10.1.1.1", [("orders", 0)])],
            ),
            "audit": group_description("audit", "EMPTY"),
        },
        committed={"billing": {("orders", 0): 3}, "audit": {("orders", 0): 1, ("orders", 1): 1}},
        topics={"orders": 2},
        latest={("orders", 0): 10, ("orders", 1): 6},
        earliest={("orders", 0): 0, ("orders", 1): 0},
    )
    registry = ConnectionRegistry(
        store, admin_factory=lambda conf: admin, producer_factory=Fakes().producer
    )
    with TestClient(create_app(store=store, registry=registry), base_url=LOCAL) as client:
        yield Harness(client, registry, admin)


# --- reads -------------------------------------------------------------------------------------


def test_list_groups_sorted_and_filtered(harness: Harness) -> None:
    everything = harness.client.get(GROUPS)
    filtered = harness.client.get(GROUPS, params={"filter": "BILLING"})

    assert everything.status_code == 200
    assert [g["group_id"] for g in everything.json()["groups"]] == [
        "Billing-batch",
        "audit",
        "billing",
    ]
    assert filtered.json()["groups"] == [
        {"group_id": "Billing-batch", "state": "empty", "type": "classic", "is_simple": False},
        {"group_id": "billing", "state": "stable", "type": "consumer", "is_simple": False},
    ]


def test_describe_group_with_members_and_lag(harness: Harness) -> None:
    response = harness.client.get(f"{GROUPS}/billing")

    assert response.status_code == 200
    assert response.json() == {
        "group_id": "billing",
        "state": "stable",
        "type": "consumer",
        "members": [
            {
                "member_id": "m1",
                "client_id": "svc",
                "host": "/10.1.1.1",
                "assignments": [{"topic": "orders", "partition": 0}],
            }
        ],
        "offsets": [{"topic": "orders", "partition": 0, "committed": 3, "end": 10, "lag": 7}],
    }


def test_unknown_group_is_404(harness: Harness) -> None:
    response = harness.client.get(f"{GROUPS}/ghost")

    assert response.status_code == 404
    assert response.json()["code"] == "group_not_found"


def test_unknown_cluster_is_404(harness: Harness) -> None:
    for method, path in (
        ("GET", "/api/clusters/nope/groups"),
        ("GET", "/api/clusters/nope/groups/g"),
        ("POST", "/api/clusters/nope/groups"),
    ):
        response = harness.client.request(method, path, json={})
        assert response.status_code == 404
        assert response.json()["code"] == "cluster_not_found"


# --- create ------------------------------------------------------------------------------------


def create(harness: Harness, **body: Any):
    payload = {"group_id": "fresh", "topic": "orders", "start": "earliest", **body}
    return harness.client.post(GROUPS, json=payload)


def test_create_group_is_201_and_commits_the_start(harness: Harness) -> None:
    response = create(harness, start="latest")

    assert response.status_code == 201
    assert response.json() == {"group_id": "fresh"}
    assert harness.admin.alter_calls == [("fresh", [("orders", 0, 10), ("orders", 1, 6)])]


def test_create_existing_group_is_409(harness: Harness) -> None:
    response = create(harness, group_id="audit")

    assert response.status_code == 409
    assert response.json()["code"] == "group_exists"


def test_create_on_unknown_topic_is_404(harness: Harness) -> None:
    response = create(harness, topic="ghost")

    assert response.status_code == 404
    assert response.json()["code"] == "topic_not_found"


@pytest.mark.parametrize(
    "body",
    [
        {"group_id": ""},
        {"start": "timestamp"},
        {"topic": ""},
        {"unexpected": 1},
    ],
)
def test_create_validates_the_body(harness: Harness, body: dict[str, Any]) -> None:
    response = create(harness, **body)

    assert response.status_code == 422
    assert response.json()["code"] == "validation_failed"
    assert harness.admin.alter_calls == []


# --- reset -------------------------------------------------------------------------------------


def reset(harness: Harness, group: str = "audit", **body: Any):
    payload = {"topic": "orders", "strategy": "latest", "confirm": group, **body}
    return harness.client.post(f"{GROUPS}/{group}/reset", json=payload)


def test_reset_returns_the_new_offsets(harness: Harness) -> None:
    response = reset(harness)

    assert response.status_code == 200
    assert response.json() == {
        "offsets": [
            {"topic": "orders", "partition": 0, "committed": 10, "end": 10, "lag": 0},
            {"topic": "orders", "partition": 1, "committed": 6, "end": 6, "lag": 0},
        ]
    }
    assert harness.admin.alter_calls == [("audit", [("orders", 0, 10), ("orders", 1, 6)])]


def test_reset_to_a_timestamp(harness: Harness) -> None:
    harness.admin.by_time = {("orders", 0): 4}  # partition 1: nothing after → latest

    response = reset(harness, strategy="timestamp", timestamp=1_700_000_000_000)

    assert response.status_code == 200
    assert harness.admin.alter_calls == [("audit", [("orders", 0, 4), ("orders", 1, 6)])]


def test_reset_timestamp_strategy_needs_a_timestamp(harness: Harness) -> None:
    response = reset(harness, strategy="timestamp")

    assert response.status_code == 422
    assert response.json()["field"] == "timestamp"
    assert harness.admin.alter_calls == []


def test_reset_of_a_stable_group_is_409(harness: Harness) -> None:
    response = reset(harness, group="billing")

    assert response.status_code == 409
    assert response.json()["code"] == "group_not_empty"
    assert harness.admin.alter_calls == []


def test_reset_race_with_a_joining_consumer_is_409_not_502(harness: Harness) -> None:
    harness.admin.alter_partition_error = KafkaError(KafkaError.UNKNOWN_MEMBER_ID, "active")

    response = reset(harness)

    assert response.status_code == 409
    assert response.json()["code"] == "group_not_empty"


def test_reset_of_an_unknown_group_is_404(harness: Harness) -> None:
    response = reset(harness, group="ghost")

    assert response.status_code == 404
    assert response.json()["code"] == "group_not_found"


@pytest.mark.parametrize("confirm", ["wrong", "AUDIT", "", None])
def test_reset_needs_the_group_id_as_confirmation(harness: Harness, confirm: str | None) -> None:
    payload: dict[str, Any] = {"topic": "orders", "strategy": "latest"}
    if confirm is not None:
        payload["confirm"] = confirm

    response = harness.client.post(f"{GROUPS}/audit/reset", json=payload)

    assert response.status_code == 422
    assert response.json() == {
        "code": "confirmation_mismatch",
        "message": "confirm: type 'audit' to confirm this action",
        "field": "confirm",
    }
    assert harness.admin.alter_calls == []


def test_reset_to_offset_on_one_partition(harness: Harness) -> None:
    response = reset(harness, strategy="to_offset", partitions=[{"partition": 1, "offset": 4}])

    assert response.status_code == 200
    assert response.json() == {
        "offsets": [{"topic": "orders", "partition": 1, "committed": 4, "end": 6, "lag": 2}]
    }
    assert harness.admin.alter_calls == [("audit", [("orders", 1, 4)])]


def test_reset_shift_by(harness: Harness) -> None:
    response = reset(harness, strategy="shift_by", shift=2, partitions=[{"partition": 0}])

    assert response.status_code == 200
    assert harness.admin.alter_calls == [("audit", [("orders", 0, 3)])]


def test_preview_returns_resolved_targets_and_commits_nothing(harness: Harness) -> None:
    body = {
        "topic": "orders",
        "strategy": "to_offset",
        "partitions": [{"partition": 0, "offset": 99}, {"partition": 1, "offset": 5}],
    }

    response = harness.client.post(f"{GROUPS}/audit/reset/preview", json=body)

    assert response.status_code == 200
    assert response.json() == {
        "targets": [
            {"topic": "orders", "partition": 0, "current": 1, "target": 10, "clamped": True},
            {"topic": "orders", "partition": 1, "current": 1, "target": 5, "clamped": False},
        ]
    }
    assert harness.admin.alter_calls == []


def test_preview_of_a_stable_group_is_409(harness: Harness) -> None:
    response = harness.client.post(
        f"{GROUPS}/billing/reset/preview", json={"topic": "orders", "strategy": "latest"}
    )

    assert response.status_code == 409
    assert response.json()["code"] == "group_not_empty"


def test_reset_unknown_partition_is_422_on_partitions(harness: Harness) -> None:
    response = reset(harness, strategy="to_offset", partitions=[{"partition": 9, "offset": 1}])

    assert response.status_code == 422
    assert response.json()["field"] == "partitions"
    assert "9" in response.json()["message"]
    assert harness.admin.alter_calls == []


@pytest.mark.parametrize(
    ("body", "field"),
    [
        ({"strategy": "to_offset"}, "partitions"),
        ({"strategy": "to_offset", "partitions": [{"partition": 0}]}, "partitions"),
        ({"strategy": "latest", "partitions": [{"partition": 0, "offset": 1}]}, "partitions"),
        ({"strategy": "latest", "partitions": [{"partition": 0}, {"partition": 0}]}, "partitions"),
        ({"strategy": "latest", "partitions": []}, "partitions"),
        ({"strategy": "shift_by"}, "shift"),
        ({"strategy": "latest", "shift": 1}, "shift"),
        ({"strategy": "shift_by", "shift": 2**63}, "shift"),
        ({"strategy": "latest", "partitions": [{"partition": 2**31}]}, "partitions.0.partition"),
        ({"strategy": "latest", "partitions": [{"partition": -1}]}, "partitions.0.partition"),
        (
            {"strategy": "to_offset", "partitions": [{"partition": 0, "offset": 2**63}]},
            "partitions.0.offset",
        ),
    ],
)
def test_reset_body_shape_errors_name_the_field(
    harness: Harness, body: dict[str, Any], field: str
) -> None:
    response = reset(harness, **body)

    assert response.status_code == 422
    assert response.json()["field"] == field
    assert harness.admin.alter_calls == []


def test_reset_shift_by_without_committed_offset_is_422(harness: Harness) -> None:
    harness.admin.committed["audit"] = {("orders", 0): 1}

    response = reset(harness, strategy="shift_by", shift=-1)

    assert response.status_code == 422
    assert response.json()["field"] == "partitions"
    assert harness.admin.alter_calls == []


def test_precise_reset_still_needs_the_confirmation(harness: Harness) -> None:
    response = reset(
        harness, strategy="to_offset", partitions=[{"partition": 1, "offset": 4}], confirm="nope"
    )

    assert response.status_code == 422
    assert response.json()["code"] == "confirmation_mismatch"
    assert harness.admin.alter_calls == []


# --- delete ------------------------------------------------------------------------------------


def test_delete_an_empty_group_is_204(harness: Harness) -> None:
    response = harness.client.delete(f"{GROUPS}/audit", params={"confirm": "audit"})

    assert response.status_code == 204
    assert response.content == b""
    assert harness.admin.delete_calls == [["audit"]]


def test_delete_of_a_stable_group_is_409(harness: Harness) -> None:
    response = harness.client.delete(f"{GROUPS}/billing", params={"confirm": "billing"})

    assert response.status_code == 409
    assert response.json()["code"] == "group_not_empty"
    assert harness.admin.delete_calls == []


def test_delete_race_with_a_joining_consumer_is_409_not_502(harness: Harness) -> None:
    harness.admin.delete_error = kafka_exc(KafkaError.NON_EMPTY_GROUP)

    response = harness.client.delete(f"{GROUPS}/audit", params={"confirm": "audit"})

    assert response.status_code == 409
    assert response.json()["code"] == "group_not_empty"


def test_delete_of_an_unknown_group_is_404(harness: Harness) -> None:
    response = harness.client.delete(f"{GROUPS}/ghost", params={"confirm": "ghost"})

    assert response.status_code == 404


@pytest.mark.parametrize("params", [{"confirm": "nope"}, {}])
def test_delete_needs_the_group_id_as_confirmation(
    harness: Harness, params: dict[str, str]
) -> None:
    response = harness.client.delete(f"{GROUPS}/audit", params=params)

    assert response.status_code == 422
    assert response.json()["code"] == "confirmation_mismatch"
    assert harness.admin.delete_calls == []


# --- write guard -------------------------------------------------------------------------------


def test_read_only_cluster_refuses_every_write(harness: Harness) -> None:
    base = "/api/clusters/prd/groups"
    responses = [
        harness.client.post(base, json={"group_id": "g", "topic": "orders", "start": "latest"}),
        harness.client.post(
            f"{base}/audit/reset",
            json={"topic": "orders", "strategy": "latest", "confirm": "audit"},
        ),
        harness.client.post(
            f"{base}/audit/reset",
            json={
                "topic": "orders",
                "strategy": "to_offset",
                "partitions": [{"partition": 1, "offset": 3}],
                "confirm": "audit",
            },
        ),
        harness.client.post(
            f"{base}/audit/reset/preview", json={"topic": "orders", "strategy": "latest"}
        ),
        harness.client.delete(f"{base}/audit", params={"confirm": "audit"}),
    ]

    for response in responses:
        assert response.status_code == 403
        assert response.json()["code"] == "read_only_cluster"
    assert harness.admin.alter_calls == [] and harness.admin.delete_calls == []


def test_read_only_cluster_still_reads(harness: Harness) -> None:
    assert harness.client.get("/api/clusters/prd/groups").status_code == 200
    assert harness.client.get("/api/clusters/prd/groups/audit").status_code == 200


# --- broker failures and connection races ------------------------------------------------------


def test_broker_failure_uses_the_standard_error_body(harness: Harness) -> None:
    harness.admin.descriptions["audit"] = kafka_exc(KafkaError._ALL_BROKERS_DOWN, "down")

    response = harness.client.get(f"{GROUPS}/audit")

    assert response.status_code == 502
    assert response.json() == {"code": "broker_unreachable", "message": "down"}


def test_timeouts_are_504(harness: Harness) -> None:
    harness.admin.descriptions["audit"] = TimeoutError()

    response = harness.client.get(f"{GROUPS}/audit")

    assert response.status_code == 504
    assert response.json()["code"] == "kafka_timeout"


@pytest.fixture
def racing(harness: Harness, monkeypatch: pytest.MonkeyPatch) -> Callable[[int], list[Any]]:
    def install(closed_gets: int) -> list[Any]:
        real_get = harness.registry.get
        calls: list[Any] = []

        def get(name: str):
            conn = real_get(name)
            calls.append(conn)
            if len(calls) <= closed_gets:
                harness.registry.disconnect(name)
            return conn

        monkeypatch.setattr(harness.registry, "get", get)
        return calls

    return install


def test_a_connection_closed_by_a_race_is_fetched_again_once(
    harness: Harness, racing: Callable[[int], list[Any]]
) -> None:
    calls = racing(1)

    response = harness.client.get(GROUPS)

    assert response.status_code == 200
    assert len(calls) == 2 and calls[1].closed is False


def test_a_connection_closed_twice_is_409_cluster_changed_for_every_route(
    harness: Harness, racing: Callable[[int], list[Any]]
) -> None:
    racing(10)

    responses = [
        harness.client.get(GROUPS),
        harness.client.get(f"{GROUPS}/audit"),
        create(harness),
        reset(harness),
        harness.client.delete(f"{GROUPS}/audit", params={"confirm": "audit"}),
    ]

    for response in responses:
        assert response.status_code == 409
        assert response.json()["code"] == "cluster_changed"
    assert harness.admin.alter_calls == [] and harness.admin.delete_calls == []
