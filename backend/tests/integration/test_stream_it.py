"""Live streaming over the WebSocket against a real broker."""

import queue
import threading
import time
from collections.abc import Callable, Iterator
from typing import Any

import pytest
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient
from fastapi.testclient import TestClient
from starlette.websockets import WebSocketDisconnect

pytestmark = pytest.mark.integration

WS = "ws://127.0.0.1:8000"
BASE_TS = 1_700_000_000_000


@pytest.fixture
def cluster(api: TestClient, kafka_plaintext: str) -> str:
    body: dict[str, Any] = {"name": "local", "env": "dev", "bootstrap_servers": kafka_plaintext}
    assert api.post("/api/clusters", json=body).status_code == 201
    return "local"


@pytest.fixture
def producer(kafka_plaintext: str) -> Producer:
    return Producer({"bootstrap.servers": kafka_plaintext})


class Frames:
    """Reads frames on a background thread, so a test can wait with a timeout instead of
    blocking forever in `receive_json`. `closed` holds the close code once the server closed."""

    def __init__(self, ws: Any):
        self._ws = ws
        self._frames: queue.Queue[dict[str, Any]] = queue.Queue()
        self.closed: int | None = None
        self._thread = threading.Thread(target=self._read, daemon=True)
        self._thread.start()

    def _read(self) -> None:
        try:
            while True:
                self._frames.put(self._ws.receive_json())
        except WebSocketDisconnect as exc:
            self.closed = exc.code

    def next(self, timeout: float) -> dict[str, Any] | None:
        try:
            return self._frames.get(timeout=timeout)
        except queue.Empty:
            return None

    def keys_until(self, wanted: set[str], timeout: float) -> list[str]:
        """Message keys received until every `wanted` key arrived or `timeout` passed."""
        seen: list[str] = []
        deadline = time.monotonic() + timeout
        while not wanted <= set(seen) and (left := deadline - time.monotonic()) > 0:
            frame = self.next(left)
            if frame is not None and frame["type"] == "messages":
                seen += [m["key"]["data"] for m in frame["items"]]
        return seen

    def send(self, text: str) -> None:
        self._ws.send_text(text)

    def wait_closed(self, timeout: float) -> int | None:
        self._thread.join(timeout)
        return self.closed


@pytest.fixture
def open_stream(api: TestClient, cluster: str) -> Iterator[Callable[..., Frames]]:
    sessions: list[Any] = []

    def open_(topic: str, **params: Any) -> Frames:
        query = "&".join(f"{k}={v}" for k, v in params.items())
        session = api.websocket_connect(
            f"{WS}/api/clusters/{cluster}/topics/{topic}/stream?{query}"
        )
        ws = session.__enter__()
        sessions.append(session)
        return Frames(ws)

    yield open_
    for session in sessions:
        session.__exit__(None, None, None)


def produce(producer: Producer, topic: str, keys: list[str], **kwargs: Any) -> None:
    for key in keys:
        producer.produce(topic, key=key.encode(), value=f'{{"k": "{key}"}}'.encode(), **kwargs)
    assert producer.flush(10) == 0


def wait_until_live(frames: Frames, producer: Producer, topic: str) -> None:
    """`latest` only sees messages produced after the consumer's assignment, which the client
    cannot observe: produce probes until one comes through."""
    deadline = time.monotonic() + 30
    for n in range(1000):
        assert time.monotonic() < deadline, "the stream never delivered a probe"
        produce(producer, topic, [f"probe-{n}"], partition=0)
        if any(k.startswith("probe-") for k in frames.keys_until({f"probe-{n}"}, timeout=0.5)):
            return


def test_latest_streams_new_messages(
    open_stream: Callable[..., Frames], producer: Producer, topic_factory: Callable[..., str]
) -> None:
    topic = topic_factory(partitions=3)
    produce(producer, topic, ["old-1", "old-2"])  # before the stream: never shown
    frames = open_stream(topic)
    wait_until_live(frames, producer, topic)

    keys = [f"m{i}" for i in range(5)]
    produce(producer, topic, keys)
    seen = frames.keys_until(set(keys), timeout=5)

    assert set(keys) <= set(seen)
    assert not {"old-1", "old-2"} & set(seen)
    frames.send("stop")
    assert frames.wait_closed(timeout=5) == 1000


def test_timestamp_replays_older_messages_then_stop_closes(
    open_stream: Callable[..., Frames],
    producer: Producer,
    topic_factory: Callable[..., str],
    kafka_plaintext: str,
) -> None:
    topic = topic_factory(partitions=2)
    for i in range(4):
        produce(producer, topic, [f"t{i}"], partition=i % 2, timestamp=BASE_TS + i * 1000)

    frames = open_stream(topic, start="timestamp", timestamp=BASE_TS + 1000)
    seen = frames.keys_until({"t1", "t2", "t3"}, timeout=10)
    assert sorted(seen) == ["t1", "t2", "t3"]

    produce(producer, topic, ["after"])  # and it keeps following the topic
    assert "after" in frames.keys_until({"after"}, timeout=5)

    frames.send("stop")
    assert frames.wait_closed(timeout=5) == 1000

    admin = AdminClient({"bootstrap.servers": kafka_plaintext})
    groups = admin.list_consumer_groups().result(timeout=10)
    assert [g.group_id for g in groups.valid if g.group_id.startswith("kafka-web-")] == []


def test_unknown_topic_sends_an_error_frame(open_stream: Callable[..., Frames]) -> None:
    frames = open_stream("it-does-not-exist")

    frame = frames.next(timeout=15)

    assert frame is not None
    assert (frame["type"], frame["code"]) == ("error", "topic_not_found")
    assert frames.wait_closed(timeout=5) == 1011
