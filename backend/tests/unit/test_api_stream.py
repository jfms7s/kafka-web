import asyncio
import json
import threading
import time
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import pytest
from confluent_kafka import KafkaError
from fastapi.testclient import TestClient
from starlette.testclient import WebSocketDenialResponse
from starlette.websockets import WebSocketDisconnect

from kafka_web.api import stream as stream_api
from kafka_web.api.app import create_app
from kafka_web.config.models import ClusterInput
from kafka_web.config.secrets import SecretStore
from kafka_web.config.store import ClusterStore
from kafka_web.kafka.registry import ConnectionRegistry
from kafka_web.services.stream import BoundedDropQueue
from tests.conftest import MemoryKeyring
from tests.fakes import FakeMessage, Fakes, LiveConsumers, LiveFakeConsumer

LOCAL = "http://127.0.0.1:8000"
# websocket_connect ignores base_url (it defaults to ws://testserver, a Host the app refuses).
WS = "ws://127.0.0.1:8000"
URL = f"{WS}/api/clusters/dev/topics/orders/stream"
DEV = {"name": "dev", "env": "dev", "bootstrap_servers": "b1:9092"}


@pytest.fixture
def consumers() -> LiveConsumers:
    return LiveConsumers(marks={0: (0, 5), 1: (0, 5)})


@pytest.fixture
def client(
    tmp_path: Path, memory_keyring: MemoryKeyring, consumers: LiveConsumers
) -> Iterator[TestClient]:
    store = ClusterStore(tmp_path / "cfg", SecretStore(memory_keyring))
    store.create(ClusterInput.model_validate(DEV))
    fakes = Fakes()
    registry = ConnectionRegistry(store, admin_factory=fakes.admin, producer_factory=fakes.producer)
    app = create_app(store=store, registry=registry, consumer_factory=consumers)
    with TestClient(app, base_url=LOCAL) as client:
        yield client
    assert no_stream_threads_within(2), "a stream worker outlived the app"


def stream_threads() -> list[threading.Thread]:
    return [t for t in threading.enumerate() if t.name.startswith("kafka-web-stream-")]


def within(seconds: float, condition: Callable[[], bool]) -> bool:
    deadline = time.monotonic() + seconds
    while not condition():
        if time.monotonic() > deadline:
            return False
        time.sleep(0.01)
    return True


def no_stream_threads_within(seconds: float) -> bool:
    return within(seconds, lambda: stream_threads() == [])


def streaming(consumers: LiveConsumers) -> LiveFakeConsumer:
    consumer = consumers.wait_for_consumer()
    assert consumer.assigned_event.wait(2), "the stream never assigned partitions"
    return consumer


def receive_items(ws: Any, count: int) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    while len(items) < count:
        frame = ws.receive_json()
        assert frame["type"] == "messages", frame
        items += frame["items"]
    return items


def close_code(ws: Any) -> int:
    with pytest.raises(WebSocketDisconnect) as closed:
        ws.receive_json()
    return closed.value.code


def active_streams(client: TestClient) -> int:
    [conn] = client.get("/api/status").json()["connections"]
    return conn["active_streams"]


# --- streaming --------------------------------------------------------------------------------


def test_receives_messages_frames(client: TestClient, consumers: LiveConsumers) -> None:
    with client.websocket_connect(URL) as ws:
        consumer = streaming(consumers)
        consumer.feed(
            FakeMessage(partition=1, offset=5, key=b"k", value=b'{"a": 1}'),
            FakeMessage(partition=0, offset=9, value=b"\xff"),
        )
        items = receive_items(ws, 2)

    assert [(m["partition"], m["offset"]) for m in items] == [(1, 5), (0, 9)]
    assert items[0]["key"]["data"] == "k"
    assert items[0]["value"]["json_value"] == {"a": 1}
    assert items[1]["value"]["encoding"] == "base64"
    assert consumer.assigned == [(0, -1), (1, -1)]  # latest: OFFSET_END


def test_frames_carry_at_most_100_messages(client: TestClient, consumers: LiveConsumers) -> None:
    with client.websocket_connect(URL) as ws:
        consumer = streaming(consumers)
        consumer.feed(*(FakeMessage(offset=i) for i in range(250)))
        sizes = []
        while sum(sizes) < 250:
            frame = ws.receive_json()
            sizes.append(len(frame["items"]))

    assert max(sizes) <= 100
    assert sum(sizes) == 250


def test_overflow_sends_a_dropped_frame(
    client: TestClient, consumers: LiveConsumers, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(stream_api, "BoundedDropQueue", lambda: BoundedDropQueue(maxsize=1))
    with client.websocket_connect(URL) as ws:
        consumer = streaming(consumers)
        consumer.feed(*(FakeMessage(offset=i) for i in range(50)))
        kept = dropped = 0
        while kept + dropped < 50:
            frame = ws.receive_json()
            if frame["type"] == "dropped":
                dropped += frame["count"]
            else:
                kept += len(frame["items"])

    assert dropped > 0
    assert kept + dropped == 50


def test_query_parameters_reach_the_consumer(client: TestClient, consumers: LiveConsumers) -> None:
    with client.websocket_connect(f"{URL}?start=offset&offset=3&partition=1") as ws:
        consumer = streaming(consumers)
        ws.send_text("stop")
        close_code(ws)

    assert consumer.assigned == [(1, 3)]
    assert consumer.conf["group.id"].startswith("kafka-web-")


# --- stop conditions --------------------------------------------------------------------------


def test_stop_closes_normally_and_ends_the_worker(
    client: TestClient, consumers: LiveConsumers
) -> None:
    with client.websocket_connect(URL) as ws:
        consumer = streaming(consumers)
        ws.send_text("stop")
        assert close_code(ws) == 1000

        assert consumer.closed
        assert stream_threads() == []


def test_other_client_text_is_ignored(client: TestClient, consumers: LiveConsumers) -> None:
    with client.websocket_connect(URL) as ws:
        consumer = streaming(consumers)
        ws.send_text("hello")
        consumer.feed(FakeMessage(offset=1))
        assert [m["offset"] for m in receive_items(ws, 1)] == [1]


def test_stream_stops_on_client_disconnect(client: TestClient, consumers: LiveConsumers) -> None:
    with client.websocket_connect(URL):
        consumer = streaming(consumers)

    assert consumer.closed_event.wait(2), "consumer not closed after the client went away"
    assert no_stream_threads_within(2)
    assert within(2, lambda: active_streams(client) == 0), "stream still registered"


@pytest.mark.parametrize(
    "change",
    [
        lambda c: c.delete("/api/clusters/dev"),
        lambda c: c.post("/api/clusters/dev/disconnect"),
        lambda c: c.put("/api/clusters/dev", json={**DEV, "region": "eu"}),
    ],
    ids=["delete", "disconnect", "edit"],
)
def test_stream_stops_when_cluster_deleted(
    client: TestClient, consumers: LiveConsumers, change: Any
) -> None:
    with client.websocket_connect(URL) as ws:
        consumer = streaming(consumers)
        assert change(client).status_code in (200, 204)

        assert ws.receive_json() == {"type": "closed", "reason": "cluster_changed"}
        assert close_code(ws) == 1001

    assert consumer.closed_event.wait(2)
    assert no_stream_threads_within(2)


def test_active_streams_counts_open_streams(client: TestClient, consumers: LiveConsumers) -> None:
    with client.websocket_connect(URL) as ws:
        streaming(consumers)
        assert active_streams(client) == 1
        ws.send_text("stop")
        close_code(ws)
        assert active_streams(client) == 0


# --- errors -----------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("query", "field"),
    [
        ("start=earliest", "start"),
        ("start=middle", "start"),
        ("start=timestamp&timestamp=-1", "timestamp"),
        ("start=timestamp", "timestamp"),
        ("start=offset&offset=3", "partition"),
        ("start=offset&partition=0", "offset"),
    ],
)
def test_invalid_params_send_an_error_frame_then_close_1008(
    client: TestClient, consumers: LiveConsumers, query: str, field: str
) -> None:
    with client.websocket_connect(f"{URL}?{query}") as ws:
        frame = ws.receive_json()
        assert close_code(ws) == 1008

    assert frame["type"] == "error"
    assert frame["code"] == "validation_failed"
    assert frame["field"] == field
    assert frame["message"]
    assert consumers.created == []


@pytest.mark.parametrize(
    ("query", "field"),
    [
        ("start=offset&offset=abc&partition=0", "offset"),
        ("start=offset&offset=3&partition=x", "partition"),
        ("start=timestamp&timestamp=1.5", "timestamp"),
        ("partition=", "partition"),
    ],
)
def test_non_integer_params_send_an_error_frame_then_close_1008(
    client: TestClient, consumers: LiveConsumers, query: str, field: str
) -> None:
    with client.websocket_connect(f"{URL}?{query}") as ws:
        frame = ws.receive_json()
        assert close_code(ws) == 1008

    assert (frame["type"], frame["code"], frame["field"]) == ("error", "validation_failed", field)
    assert "abc" not in frame["message"]  # the input is never echoed
    assert consumers.created == []


def test_unknown_cluster_sends_error_frame(client: TestClient, consumers: LiveConsumers) -> None:
    with client.websocket_connect(f"{WS}/api/clusters/nope/topics/orders/stream") as ws:
        frame = ws.receive_json()
        assert close_code(ws) == 1008

    assert (frame["type"], frame["code"]) == ("error", "cluster_not_found")
    assert consumers.created == []


def test_worker_failure_sends_error_frame_and_closes_1011(
    client: TestClient, consumers: LiveConsumers
) -> None:
    with client.websocket_connect(f"{WS}/api/clusters/dev/topics/missing/stream") as ws:
        frame = ws.receive_json()
        assert close_code(ws) == 1011

    assert (frame["type"], frame["code"]) == ("error", "topic_not_found")
    assert consumers.created[0].closed
    assert no_stream_threads_within(2)


# --- local-only guards ------------------------------------------------------------------------


@pytest.mark.parametrize(
    "origin",
    [
        "http://evil.example",
        "http://127.0.0.1.evil.example",
        "null",
        "file://",
        "http://127.0.0.1:5173",  # another local process on another port
        "http://localhost:8000",  # a different origin from 127.0.0.1:8000
    ],
)
def test_foreign_origin_is_refused_before_accept(
    client: TestClient, consumers: LiveConsumers, origin: str
) -> None:
    headers = {"origin": origin}
    with (
        pytest.raises(WebSocketDisconnect) as refused,
        client.websocket_connect(URL, headers=headers),
    ):
        pass
    assert refused.value.code == 1008
    assert consumers.created == []


def test_same_origin_is_accepted(client: TestClient, consumers: LiveConsumers) -> None:
    with client.websocket_connect(URL, headers={"origin": "http://127.0.0.1:8000"}) as ws:
        streaming(consumers)
        ws.send_text("stop")
        assert close_code(ws) == 1000


def test_vite_dev_proxy_origin_is_accepted(client: TestClient, consumers: LiveConsumers) -> None:
    """The Vite proxy (ws: true, changeOrigin false) forwards Host 127.0.0.1:5173 unchanged."""
    url = "ws://127.0.0.1:5173/api/clusters/dev/topics/orders/stream"
    with client.websocket_connect(url, headers={"origin": "http://127.0.0.1:5173"}) as ws:
        streaming(consumers)
        ws.send_text("stop")
        assert close_code(ws) == 1000


def test_foreign_host_is_refused(client: TestClient, consumers: LiveConsumers) -> None:
    refused = pytest.raises((WebSocketDenialResponse, WebSocketDisconnect))
    with refused, client.websocket_connect(URL, headers={"host": "evil.example"}):
        pass
    assert consumers.created == []


def test_messages_before_a_failure_are_delivered_before_the_error_frame(
    client: TestClient, consumers: LiveConsumers
) -> None:
    with client.websocket_connect(URL) as ws:
        consumer = streaming(consumers)
        consumer.feed(
            FakeMessage(offset=1),
            FakeMessage(offset=2),
            FakeMessage(error=KafkaError(KafkaError.TOPIC_AUTHORIZATION_FAILED)),
        )
        items = receive_items(ws, 2)
        frame = ws.receive_json()
        assert close_code(ws) == 1011

    assert [m["offset"] for m in items] == [1, 2]
    assert (frame["type"], frame["code"]) == ("error", "authorization_failed")


def test_cluster_closed_before_registration_sends_closed_without_consuming(
    client: TestClient, consumers: LiveConsumers, monkeypatch: pytest.MonkeyPatch
) -> None:
    registry = client.app.state.registry  # type: ignore[attr-defined]
    register = registry.register_stream

    def disconnect_first(name: str, stop: threading.Event) -> None:
        registry.disconnect(name)  # an edit/delete landing between `get` and registration
        register(name, stop)

    monkeypatch.setattr(registry, "register_stream", disconnect_first)
    with client.websocket_connect(URL) as ws:
        assert ws.receive_json() == {"type": "closed", "reason": "cluster_changed"}
        assert close_code(ws) == 1001

    assert consumers.created == []


# --- frame building ---------------------------------------------------------------------------


def test_encode_batch_decodes_only_the_messages_it_keeps(monkeypatch: pytest.MonkeyPatch) -> None:
    decoded: list[int] = []
    real = stream_api.to_message_view

    def counting(msg: Any) -> Any:
        decoded.append(msg.offset())
        return real(msg)

    monkeypatch.setattr(stream_api, "to_message_view", counting)
    queue: BoundedDropQueue[Any] = BoundedDropQueue(maxsize=2)
    for i in range(10):
        queue.put(FakeMessage(offset=i, value=b'{"n": 1}'))

    batch = stream_api.encode_batch(queue)

    assert decoded == [8, 9]  # the 8 dropped messages were never decoded
    assert batch.count == 2
    dropped, messages = (json.loads(text) for text in batch.frames)
    assert dropped == {"type": "dropped", "count": 8}
    assert messages["type"] == "messages"
    assert [m["offset"] for m in messages["items"]] == [8, 9]
    assert messages["items"][0]["value"]["json_value"] == {"n": 1}


def test_encode_batch_of_an_empty_queue_has_no_frames() -> None:
    batch = stream_api.encode_batch(BoundedDropQueue())
    assert (batch.frames, batch.count) == ([], 0)


def test_frames_are_built_off_the_event_loop(
    client: TestClient, consumers: LiveConsumers, monkeypatch: pytest.MonkeyPatch
) -> None:
    on_loop: list[bool] = []
    real = stream_api.encode_batch

    def spy(*args: Any, **kwargs: Any) -> Any:
        try:
            asyncio.get_running_loop()
            on_loop.append(True)
        except RuntimeError:
            on_loop.append(False)
        return real(*args, **kwargs)

    monkeypatch.setattr(stream_api, "encode_batch", spy)
    with client.websocket_connect(URL) as ws:
        streaming(consumers).feed(FakeMessage(offset=1))
        receive_items(ws, 1)

    assert on_loop
    assert not any(on_loop)
