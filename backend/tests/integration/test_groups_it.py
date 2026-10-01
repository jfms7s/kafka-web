"""Consumer group management against a real broker."""

import contextlib
import threading
import time
import uuid
from collections.abc import Callable, Iterator
from typing import Any

import pytest
from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient
from fastapi.testclient import TestClient

from kafka_web.errors import Conflict
from kafka_web.services import groups as group_service

pytestmark = pytest.mark.integration

MESSAGES = 10
FIRST_TIMESTAMP_MS = 1_700_000_000_000
STABLE_TIMEOUT_S = 45


@pytest.fixture
def cluster(api: TestClient, kafka_plaintext: str) -> str:
    body: dict[str, Any] = {"name": "local", "env": "dev", "bootstrap_servers": kafka_plaintext}
    assert api.post("/api/clusters", json=body).status_code == 201
    return "local"


@pytest.fixture
def topic(topic_factory: Callable[..., str], kafka_plaintext: str) -> str:
    """A single-partition topic holding `MESSAGES` messages one second apart."""
    name = topic_factory(partitions=1)
    producer = Producer({"bootstrap.servers": kafka_plaintext})
    for i in range(MESSAGES):
        producer.produce(name, value=f"m{i}".encode(), timestamp=FIRST_TIMESTAMP_MS + i * 1000)
    assert producer.flush(30) == 0
    return name


@pytest.fixture
def group_id(kafka_plaintext: str) -> Iterator[str]:
    name = f"it-group-{uuid.uuid4().hex[:10]}"
    yield name
    admin = AdminClient({"bootstrap.servers": kafka_plaintext})
    with contextlib.suppress(Exception):  # the test may already have deleted it
        admin.delete_consumer_groups([name])[name].result(timeout=10)


class ActiveMember:
    """A real subscribing consumer polling in a thread; `stop()` is idempotent."""

    def __init__(self, bootstrap: str, group: str, topic: str):
        self._stop = threading.Event()
        self._consumer = Consumer(
            {
                "bootstrap.servers": bootstrap,
                "group.id": group,
                "enable.auto.commit": False,
                "auto.offset.reset": "earliest",
            }
        )
        self._consumer.subscribe([topic])
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def _run(self) -> None:
        try:
            while not self._stop.is_set():
                self._consumer.poll(0.2)
        finally:
            self._consumer.close()  # leaves the group

    def stop(self) -> None:
        self._stop.set()
        self._thread.join(timeout=30)


@pytest.fixture
def active_member(kafka_plaintext: str) -> Iterator[Callable[[str, str], ActiveMember]]:
    started: list[ActiveMember] = []

    def start(group: str, topic: str) -> ActiveMember:
        member = ActiveMember(kafka_plaintext, group, topic)
        started.append(member)
        return member

    yield start
    for member in started:
        member.stop()


def wait_for_state(api: TestClient, cluster: str, group: str, state: str) -> dict[str, Any]:
    deadline = time.monotonic() + STABLE_TIMEOUT_S
    while True:
        response = api.get(f"/api/clusters/{cluster}/groups/{group}")
        if response.status_code == 200 and response.json()["state"] == state:
            return response.json()
        assert time.monotonic() < deadline, f"group never became {state}: {response.text}"
        time.sleep(0.3)


def total_lag(detail: dict[str, Any]) -> int:
    return sum(o["lag"] for o in detail["offsets"])


def create(api: TestClient, cluster: str, group: str, topic: str, start: str = "earliest"):
    return api.post(
        f"/api/clusters/{cluster}/groups", json={"group_id": group, "topic": topic, "start": start}
    )


def reset(api: TestClient, cluster: str, group: str, topic: str, strategy: str, **extra: Any):
    return api.post(
        f"/api/clusters/{cluster}/groups/{group}/reset",
        json={"topic": topic, "strategy": strategy, "confirm": group, **extra},
    )


def test_create_at_earliest_shows_lag_and_listing(
    api: TestClient, cluster: str, topic: str, group_id: str
) -> None:
    created = create(api, cluster, group_id, topic)
    assert created.status_code == 201, created.text

    detail = api.get(f"/api/clusters/{cluster}/groups/{group_id}").json()
    listed = api.get(f"/api/clusters/{cluster}/groups", params={"filter": group_id.upper()})

    assert detail["state"] == "empty"
    assert detail["members"] == []
    assert detail["offsets"] == [
        {"topic": topic, "partition": 0, "committed": 0, "end": MESSAGES, "lag": MESSAGES}
    ]
    assert [(g["group_id"], g["state"]) for g in listed.json()["groups"]] == [(group_id, "empty")]
    assert create(api, cluster, group_id, topic).status_code == 409


def test_create_at_latest_has_no_lag(
    api: TestClient, cluster: str, topic: str, group_id: str
) -> None:
    assert create(api, cluster, group_id, topic, "latest").status_code == 201

    detail = api.get(f"/api/clusters/{cluster}/groups/{group_id}").json()

    assert total_lag(detail) == 0


def test_reset_to_latest_earliest_and_timestamp(
    api: TestClient, cluster: str, topic: str, group_id: str
) -> None:
    create(api, cluster, group_id, topic)

    latest = reset(api, cluster, group_id, topic, "latest")
    assert latest.status_code == 200, latest.text
    assert latest.json()["offsets"] == [
        {"topic": topic, "partition": 0, "committed": MESSAGES, "end": MESSAGES, "lag": 0}
    ]

    halfway = reset(api, cluster, group_id, topic, "timestamp", timestamp=FIRST_TIMESTAMP_MS + 5000)
    assert halfway.status_code == 200, halfway.text
    assert halfway.json()["offsets"][0]["committed"] == 5
    assert total_lag(api.get(f"/api/clusters/{cluster}/groups/{group_id}").json()) == 5

    past_the_end = reset(
        api, cluster, group_id, topic, "timestamp", timestamp=FIRST_TIMESTAMP_MS * 2
    )
    assert past_the_end.json()["offsets"][0]["committed"] == MESSAGES  # falls back to latest

    earliest = reset(api, cluster, group_id, topic, "earliest")
    assert earliest.json()["offsets"][0]["lag"] == MESSAGES


def test_reset_and_create_on_an_unknown_topic_are_404(
    api: TestClient, cluster: str, topic: str, group_id: str
) -> None:
    create(api, cluster, group_id, topic)

    assert reset(api, cluster, group_id, "no-such-topic", "latest").status_code == 404
    assert create(api, cluster, f"{group_id}-b", "no-such-topic").status_code == 404


def test_active_group_refuses_reset_and_delete_until_stopped(
    api: TestClient,
    cluster: str,
    topic: str,
    group_id: str,
    active_member: Callable[[str, str], ActiveMember],
) -> None:
    member = active_member(group_id, topic)
    stable = wait_for_state(api, cluster, group_id, "stable")
    assert len(stable["members"]) == 1
    assert stable["members"][0]["assignments"] == [[topic, 0]]

    blocked_reset = reset(api, cluster, group_id, topic, "latest")
    blocked_delete = api.delete(
        f"/api/clusters/{cluster}/groups/{group_id}", params={"confirm": group_id}
    )

    assert blocked_reset.status_code == 409
    assert blocked_reset.json()["code"] == "group_not_empty"
    assert blocked_delete.status_code == 409
    assert blocked_delete.json()["code"] == "group_not_empty"

    member.stop()
    wait_for_state(api, cluster, group_id, "empty")
    deleted = api.delete(f"/api/clusters/{cluster}/groups/{group_id}", params={"confirm": group_id})
    assert deleted.status_code == 204
    assert api.get(f"/api/clusters/{cluster}/groups/{group_id}").status_code == 404


def test_broker_rejection_of_a_group_that_just_became_active_is_409(
    api: TestClient,
    cluster: str,
    topic: str,
    group_id: str,
    kafka_plaintext: str,
    active_member: Callable[[str, str], ActiveMember],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The race: the group looked EMPTY, a consumer joined, then the write reached the broker."""
    active_member(group_id, topic)
    wait_for_state(api, cluster, group_id, "stable")
    monkeypatch.setattr(group_service, "_require_empty", lambda *args: None)
    admin = AdminClient({"bootstrap.servers": kafka_plaintext})

    with pytest.raises(Conflict) as reset_failure:
        group_service.reset_offsets(admin, group_id, topic, "latest", None)
    with pytest.raises(Conflict) as delete_failure:
        group_service.delete_group(admin, group_id)
    with pytest.raises(Conflict) as create_failure:
        # `create` refuses an existing group by itself; skip that check to reach the broker too
        monkeypatch.setattr(group_service, "_exists", lambda *args: False)
        group_service.create_group(admin, group_id, topic, "latest")

    assert reset_failure.value.code == "group_not_empty"
    assert delete_failure.value.code == "group_not_empty"
    assert create_failure.value.code == "group_not_empty"


def test_unknown_group_is_404_everywhere(api: TestClient, cluster: str, topic: str) -> None:
    ghost = f"ghost-{uuid.uuid4().hex[:8]}"

    assert api.get(f"/api/clusters/{cluster}/groups/{ghost}").status_code == 404
    assert reset(api, cluster, ghost, topic, "latest").status_code == 404
    delete = api.delete(f"/api/clusters/{cluster}/groups/{ghost}", params={"confirm": ghost})
    assert delete.status_code == 404
