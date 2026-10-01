import json
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import pytest
from confluent_kafka import KafkaError
from confluent_kafka.admin import ClusterMetadata
from fastapi.testclient import TestClient

from kafka_web.api import messages as messages_api
from kafka_web.api.app import create_app
from kafka_web.config.models import ClusterInput
from kafka_web.config.secrets import SecretStore
from kafka_web.config.store import ClusterStore
from kafka_web.kafka.registry import ConnectionRegistry
from tests.conftest import MemoryKeyring
from tests.fakes import DeliveringProducer, Fakes, partition, topic_meta

LOCAL = "http://127.0.0.1:8000"
SINGLE = "/api/clusters/dev/topics/orders/messages"
BULK = "/api/clusters/dev/topics/orders/messages/bulk"
CSV = b"id,body\n1,one\n2,two\n3,three\n"


class Harness:
    def __init__(self, client: TestClient, registry: ConnectionRegistry, fakes: Fakes):
        self.client = client
        self.registry = registry
        self.fakes = fakes
        self.producer = DeliveringProducer()

    @property
    def produced(self) -> list[dict[str, Any]]:
        return self.producer.produced

    def bulk(
        self,
        content: bytes = CSV,
        *,
        confirm: str | None = "orders",
        path: str = BULK,
        filename: str = "rows.csv",
        **fields: str,
    ):
        data = dict(fields)
        if confirm is not None:
            data["confirm"] = confirm
        return self.client.post(path, data=data, files={"file": (filename, content)})


@pytest.fixture
def harness(tmp_path: Path, memory_keyring: MemoryKeyring) -> Iterator[Harness]:
    store = ClusterStore(tmp_path / "cfg", SecretStore(memory_keyring))
    store.create(ClusterInput(name="dev", env="dev", bootstrap_servers="b:9092"))
    store.create(ClusterInput(name="prd", env="prd", bootstrap_servers="b:9092", read_only=True))
    fakes = Fakes()

    def metadata(admin: Any) -> ClusterMetadata:
        meta = ClusterMetadata()
        parts = [partition(i, 1, [1], [1]) for i in range(3)]
        meta.topics = {"orders": topic_meta("orders", parts)}
        return meta

    fakes.on_list_topics = metadata
    holder: list[Harness] = []
    registry = ConnectionRegistry(
        store, admin_factory=fakes.admin, producer_factory=lambda conf: holder[0].producer
    )
    with TestClient(create_app(store=store, registry=registry), base_url=LOCAL) as client:
        holder.append(Harness(client, registry, fakes))
        yield holder[0]


# --- single ----------------------------------------------------------------------------------


def test_single_publish_returns_partition_and_offset(harness: Harness) -> None:
    body = {"key": "k1", "value": '{"a": 1}', "headers": "trace=t1\nsource=web", "partition": 1}

    response = harness.client.post(SINGLE, json=body)

    assert response.status_code == 200
    assert response.json() == {"ok": True, "partition": 1, "offset": 100}
    [sent] = harness.produced
    assert (sent["topic"], sent["key"], sent["value"]) == ("orders", b"k1", b'{"a": 1}')
    assert sent["headers"] == [("trace", b"t1"), ("source", b"web")]
    assert sent["partition"] == 1


def test_single_publish_minimal_body_has_no_key_headers_or_partition(harness: Harness) -> None:
    response = harness.client.post(SINGLE, json={"value": "x"})

    assert response.status_code == 200
    [sent] = harness.produced
    assert sent["key"] is None and sent["headers"] is None and "partition" not in sent


def test_single_publish_accepts_json_headers(harness: Harness) -> None:
    harness.client.post(SINGLE, json={"value": "x", "headers": '{"n": 2}'})

    assert harness.produced[0]["headers"] == [("n", b"2")]


def test_single_publish_on_a_read_only_cluster_is_403(harness: Harness) -> None:
    response = harness.client.post("/api/clusters/prd/topics/orders/messages", json={"value": "x"})

    assert response.status_code == 403
    assert response.json()["code"] == "read_only_cluster"
    assert harness.produced == []
    assert harness.fakes.producers == []  # not even connected


def test_single_publish_unknown_cluster_is_404(harness: Harness) -> None:
    response = harness.client.post("/api/clusters/nope/topics/orders/messages", json={"value": "x"})

    assert response.status_code == 404
    assert response.json()["code"] == "cluster_not_found"


def test_single_publish_unknown_topic_is_404_and_nothing_is_produced(harness: Harness) -> None:
    response = harness.client.post("/api/clusters/dev/topics/ghost/messages", json={"value": "x"})

    assert response.status_code == 404
    assert response.json()["code"] == "topic_not_found"
    assert harness.produced == []


def test_topic_existence_is_checked_in_the_full_listing_not_by_name(harness: Harness) -> None:
    # Asking a broker about one topic name can auto-create it; only an unfiltered listing is safe.
    # FakeAdmin.list_topics takes no `topic` argument, so a by-name lookup would be a TypeError.
    response = harness.client.post(SINGLE, json={"value": "x"})

    assert response.status_code == 200
    assert harness.fakes.admins[0].list_topics_timeouts[-1] == 10.0


def test_single_publish_partition_out_of_range_is_422(harness: Harness) -> None:
    response = harness.client.post(SINGLE, json={"value": "x", "partition": 3})

    assert response.status_code == 422
    assert response.json()["field"] == "partition"
    assert harness.produced == []


@pytest.mark.parametrize(
    ("body", "field"),
    [
        ({"key": "k"}, "value"),
        ({"value": "x", "partition": -1}, "partition"),
        ({"value": "x", "headers": "no equals sign"}, "headers"),
        ({"value": "x", "headers": '{"a": '}, "headers"),
    ],
)
def test_single_publish_invalid_body_is_422(
    harness: Harness, body: dict[str, Any], field: str
) -> None:
    response = harness.client.post(SINGLE, json=body)

    assert response.status_code == 422
    assert response.json()["field"] == field
    assert harness.produced == []


def test_single_publish_delivery_failure_is_mapped(harness: Harness) -> None:
    harness.producer.fail = {0: KafkaError(KafkaError.TOPIC_AUTHORIZATION_FAILED)}

    response = harness.client.post(SINGLE, json={"value": "x"})

    assert response.status_code == 403
    assert response.json()["code"] == "authorization_failed"


def test_single_publish_undelivered_is_504(harness: Harness, monkeypatch) -> None:
    harness.producer.silent = {0}
    monkeypatch.setattr(messages_api, "FLUSH_TIMEOUT_S", 0.05)

    response = harness.client.post(SINGLE, json={"value": "x"})

    assert response.status_code == 504
    assert response.json()["code"] == "kafka_timeout"


def test_publish_uses_a_flush_timeout_of_30_seconds(harness: Harness) -> None:
    harness.client.post(SINGLE, json={"value": "x"})

    assert harness.producer.flush_timeouts == [30.0]


# --- connection raced by edit/delete/disconnect ----------------------------------------------


@pytest.fixture
def racing(harness: Harness, monkeypatch: pytest.MonkeyPatch) -> Callable[[int], list[Any]]:
    """Make the first `closed_gets` calls of `registry.get` return an already-closed connection."""

    def install(closed_gets: int) -> list[Any]:
        real_get = harness.registry.get
        calls: list[Any] = []

        def get(name: str):
            conn = real_get(name)
            calls.append(conn)
            if len(calls) <= closed_gets:
                harness.registry.disconnect(name)  # what an edit/delete does: close and drop it
            return conn

        monkeypatch.setattr(harness.registry, "get", get)
        return calls

    return install


def test_a_connection_closed_by_a_race_is_fetched_again_once(
    harness: Harness, racing: Callable[[int], list[Any]]
) -> None:
    calls = racing(1)

    # the first get() hands back a connection that was closed under us; the retry reconnects
    response = harness.client.post(SINGLE, json={"value": "x"})

    assert response.status_code == 200
    assert len(calls) == 2 and calls[1].closed is False


def test_a_connection_closed_twice_is_409_cluster_changed(
    harness: Harness, racing: Callable[[int], list[Any]]
) -> None:
    racing(2)

    response = harness.client.post(SINGLE, json={"value": "x"})

    assert response.status_code == 409
    assert response.json()["code"] == "cluster_changed"
    assert harness.produced == []


def test_bulk_also_retries_then_gives_up_with_cluster_changed(
    harness: Harness, racing: Callable[[int], list[Any]]
) -> None:
    racing(2)

    response = harness.bulk()

    assert response.status_code == 409
    assert response.json()["code"] == "cluster_changed"
    assert harness.produced == []


# --- bulk: guard order -----------------------------------------------------------------------


def test_bulk_on_a_read_only_cluster_is_403_before_the_file_is_looked_at(
    harness: Harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(messages_api, "MAX_UPLOAD_BYTES", 10)  # the file is way over the limit

    response = harness.bulk(path="/api/clusters/prd/topics/orders/messages/bulk")

    assert response.status_code == 403
    assert response.json()["code"] == "read_only_cluster"
    assert harness.produced == []


@pytest.mark.parametrize("confirm", ["order", "ORDERS", "", None])
def test_bulk_with_the_wrong_confirmation_is_422(harness: Harness, confirm: str | None) -> None:
    response = harness.bulk(confirm=confirm)

    assert response.status_code == 422
    assert response.json() == {
        "code": "confirmation_mismatch",
        "message": response.json()["message"],
        "field": "confirm",
    }
    assert harness.produced == []


def test_bulk_wrong_confirmation_wins_over_an_oversized_file_that_follows_it(
    harness: Harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    # `confirm` is sent first by data=/files= ordering in httpx: it is judged before the file.
    monkeypatch.setattr(messages_api, "MAX_UPLOAD_BYTES", 10)

    response = harness.bulk(confirm="nope")

    assert response.status_code == 422
    assert response.json()["code"] == "confirmation_mismatch"


# --- bulk: upload limits and shape -----------------------------------------------------------


def test_bulk_file_over_10_mb_is_413(harness: Harness) -> None:
    assert messages_api.MAX_UPLOAD_BYTES == 10 * 1024 * 1024
    content = b"id,body\n" + b"x" * (10 * 1024 * 1024)

    response = harness.bulk(content)

    assert response.status_code == 413
    assert response.json()["code"] == "file_too_large"
    assert harness.produced == []


def test_bulk_file_of_exactly_the_limit_is_accepted(
    harness: Harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(messages_api, "MAX_UPLOAD_BYTES", len(CSV))

    response = harness.bulk()

    assert response.status_code == 200
    assert response.json()["succeeded"] == 3


def test_bulk_without_a_file_part_is_422(harness: Harness) -> None:
    response = harness.client.post(BULK, data={"confirm": "orders"})

    assert response.status_code == 422
    assert response.json()["field"] == "file"


def test_bulk_with_a_non_multipart_body_is_422(harness: Harness) -> None:
    response = harness.client.post(BULK, json={"confirm": "orders"})

    assert response.status_code == 422
    assert response.json()["field"] == "file"


@pytest.mark.parametrize("content", [b"", b"  \n "])
def test_bulk_empty_file_is_422(harness: Harness, content: bytes) -> None:
    response = harness.bulk(content)

    assert response.status_code == 422
    assert response.json()["field"] == "file"


# --- bulk: publishing ------------------------------------------------------------------------


def test_bulk_csv_publishes_every_row(harness: Harness) -> None:
    response = harness.bulk(key_column="id", value_column="body")

    assert response.status_code == 200
    body = response.json()
    assert (body["succeeded"], body["failed"]) == (3, 0)
    assert [r["row"] for r in body["results"]] == [1, 2, 3]
    assert all(r["ok"] and r["offset"] is not None for r in body["results"])
    assert [(p["key"], p["value"]) for p in harness.produced] == [
        (b"1", b"one"),
        (b"2", b"two"),
        (b"3", b"three"),
    ]


def test_bulk_csv_without_columns_sends_each_row_as_json(harness: Harness) -> None:
    harness.bulk(b"a,b\n1,2\n")

    assert json.loads(harness.produced[0]["value"]) == {"a": "1", "b": "2"}


def test_bulk_json_with_a_bad_row_reports_it_and_publishes_the_rest(harness: Harness) -> None:
    content = json.dumps([{"value": "a"}, {"key": "no-value"}, {"value": {"n": 1}}]).encode()

    response = harness.bulk(content, filename="rows.json")

    body = response.json()
    assert (body["succeeded"], body["failed"]) == (2, 1)
    assert body["results"][1]["ok"] is False
    assert "value" in body["results"][1]["error"]
    assert [p["value"] for p in harness.produced] == [b"a", b'{"n": 1}']


def test_bulk_row_results_omit_unset_fields_for_failures(harness: Harness) -> None:
    harness.producer.fail = {0: KafkaError(KafkaError.MSG_SIZE_TOO_LARGE)}

    [row] = harness.bulk(b"a\n1\n").json()["results"]

    assert row["ok"] is False and row["error"]
    assert row["partition"] is None and row["offset"] is None


def test_bulk_unknown_column_is_422_and_nothing_is_produced(harness: Harness) -> None:
    response = harness.bulk(key_column="nope")

    assert response.status_code == 422
    assert response.json()["field"] == "key_column"
    assert harness.produced == []


def test_bulk_unparsable_json_is_422(harness: Harness) -> None:
    response = harness.bulk(b'[{"value": 1}', filename="x.json")

    assert response.status_code == 422
    assert response.json()["field"] == "file"


def test_bulk_unknown_topic_is_404_and_nothing_is_produced(harness: Harness) -> None:
    response = harness.bulk(confirm="ghost", path="/api/clusters/dev/topics/ghost/messages/bulk")

    assert response.status_code == 404
    assert response.json()["code"] == "topic_not_found"
    assert harness.produced == []


def test_bulk_header_only_csv_publishes_nothing(harness: Harness) -> None:
    response = harness.bulk(b"id,body\n")

    assert response.json() == {"succeeded": 0, "failed": 0, "results": []}
