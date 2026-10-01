"""Recording fakes for confluent-kafka clients, injected through ConnectionRegistry factories."""

import queue
import threading
import time
from typing import Any


class FakeAdmin:
    def __init__(self, conf: dict[str, Any], fakes: "Fakes"):
        self.conf = conf
        self._fakes = fakes
        self.list_topics_timeouts: list[float | None] = []

    def list_topics(self, timeout: float | None = None):
        self.list_topics_timeouts.append(timeout)
        return self._fakes.on_list_topics(self)

    def poll(self, timeout: float | None = None) -> int:
        return 0


class FakeProducer:
    def __init__(self, conf: dict[str, Any]):
        self.conf = conf
        self.flush_timeouts: list[float | None] = []

    def flush(self, timeout: float | None = None) -> int:
        self.flush_timeouts.append(timeout)
        return 0


class Fakes:
    """Factories that record every client created; `list_topics` behaviour is swappable."""

    def __init__(self):
        self.admins: list[FakeAdmin] = []
        self.producers: list[FakeProducer] = []
        self._lock = threading.Lock()
        self.on_list_topics = lambda admin: object()

    def admin(self, conf: dict[str, Any]) -> FakeAdmin:
        admin = FakeAdmin(conf, self)
        with self._lock:
            self.admins.append(admin)
        return admin

    def producer(self, conf: dict[str, Any]) -> FakeProducer:
        producer = FakeProducer(conf)
        with self._lock:
            self.producers.append(producer)
        return producer


def partition(pid: int, leader: int, replicas: list[int], isrs: list[int]):
    from confluent_kafka.admin import PartitionMetadata

    meta = PartitionMetadata()
    meta.id, meta.leader, meta.replicas, meta.isrs = pid, leader, replicas, isrs
    return meta


def topic_meta(name: str, partitions: list | None = None, error=None):
    from confluent_kafka.admin import TopicMetadata

    meta = TopicMetadata()
    meta.topic, meta.error = name, error
    meta.partitions = {p.id: p for p in partitions or []}
    return meta


def config_entry(name: str, value: str | None, *, default: bool = False, sensitive: bool = False):
    from confluent_kafka.admin import ConfigEntry, ConfigSource

    source = ConfigSource.DEFAULT_CONFIG if default else ConfigSource.DYNAMIC_TOPIC_CONFIG
    return ConfigEntry(name, value, source=source, is_default=default, is_sensitive=sensitive)


NEVER = object()  # a `configs` value whose describe_configs future never resolves


class FakeTopicAdmin:
    """Admin double for the topic service: canned cluster metadata and topic configs."""

    def __init__(self, topics: dict | None = None, configs: dict | None = None):
        self.topics = topics or {}
        self.configs = configs or {}
        self.list_topics_calls: list[dict[str, Any]] = []

    def list_topics(self, topic: str | None = None, timeout: float | None = None):
        from confluent_kafka.admin import ClusterMetadata

        self.list_topics_calls.append({"topic": topic, "timeout": timeout})
        meta = ClusterMetadata()
        meta.topics = {n: t for n, t in self.topics.items() if topic is None or n == topic}
        return meta

    def poll(self, timeout: float | None = None) -> int:
        return 0

    def describe_configs(self, resources):
        from concurrent.futures import Future

        out = {}
        for resource in resources:
            future: Future = Future()
            if self.configs.get(resource.name) is not NEVER:
                future.set_result(self.configs.get(resource.name, {}))
            out[resource] = future
        return out


class FakeMessage:
    """A `confluent_kafka.Message` stand-in; `error` makes it a per-message error event."""

    def __init__(
        self,
        *,
        partition: int = 0,
        offset: int = 0,
        timestamp: tuple[int, int] | None = None,
        key: bytes | None = None,
        value: bytes | None = None,
        headers: list[tuple[str, bytes | None]] | None = None,
        error=None,
    ):
        from confluent_kafka import TIMESTAMP_CREATE_TIME

        self._p, self._o, self._k, self._v, self._h = partition, offset, key, value, headers
        self._ts = (
            timestamp if timestamp is not None else (TIMESTAMP_CREATE_TIME, 1_700_000_000_000)
        )
        self._error = error

    def partition(self) -> int:
        return self._p

    def offset(self) -> int:
        return self._o

    def timestamp(self) -> tuple[int, int]:
        return self._ts

    def key(self) -> bytes | None:
        return self._k

    def value(self) -> bytes | None:
        return self._v

    def headers(self) -> list[tuple[str, bytes | None]] | None:
        return self._h

    def error(self):
        return self._error


def eof(partition: int, offset: int = 0) -> FakeMessage:
    from confluent_kafka import KafkaError

    return FakeMessage(
        partition=partition, offset=offset, error=KafkaError(KafkaError._PARTITION_EOF)
    )


class FakeClock:
    """A monotonic clock that only moves when told to (the fake consumer's polls move it)."""

    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


class FakeConsumer:
    """Consumer double: scripted `poll` results, canned metadata and watermarks.

    `script` items are returned one per `poll`: a message (or error event), `None` for an idle
    poll, or an exception instance, which is raised. An exhausted script keeps idling. Every poll
    advances `clock` by `tick` (messages) or by the poll timeout (idle), like a real wait would.
    """

    def __init__(
        self,
        conf: dict[str, Any],
        *,
        clock: FakeClock,
        topic: str = "orders",
        marks: dict[int, tuple[int, int]] | None = None,
        script: list[Any] | None = None,
        times: dict[int, int] | None = None,
        tick: float = 0.01,
        topic_error=None,
        watermark_error: Exception | None = None,
        time_errors: dict[int, Any] | None = None,
        call_cost: float = 0.0,
    ):
        self.conf = conf
        self.clock = clock
        self.topic = topic
        self.marks = marks if marks is not None else {0: (0, 0)}
        self.script = list(script or [])
        self.times = times or {}
        self.tick = tick
        self.assigned: list[tuple[int, int]] | None = None
        self.poll_timeouts: list[float] = []
        self.metadata_timeouts: list[float | None] = []
        self.watermark_timeouts: list[float | None] = []
        self.closed = False
        self.calls: list[str] = []
        self.watermark_error = watermark_error
        self.topic_error = topic_error
        self.time_errors = time_errors or {}
        self.call_cost = call_cost  # clock time each metadata/watermark/times call "takes"
        self.times_timeouts: list[float | None] = []

    def list_topics(self, topic: str | None = None, timeout: float | None = None):
        from confluent_kafka.admin import ClusterMetadata

        self.metadata_timeouts.append(timeout)
        self.clock.now += self.call_cost
        meta = ClusterMetadata()
        if topic == self.topic:
            parts = [partition(p, 1, [1], [1]) for p in sorted(self.marks)]
            meta.topics = {topic: topic_meta(topic, parts, error=self.topic_error)}
        else:
            meta.topics = {}
        return meta

    def get_watermark_offsets(self, tp, timeout: float | None = None, cached: bool = False):
        self.watermark_timeouts.append(timeout)
        self.clock.now += self.call_cost
        if self.watermark_error is not None:
            raise self.watermark_error
        return self.marks[tp.partition]

    def offsets_for_times(self, tps, timeout: float | None = None):
        from types import SimpleNamespace

        self.times_timeouts.append(timeout)
        self.clock.now += self.call_cost
        return [
            SimpleNamespace(
                topic=tp.topic,
                partition=tp.partition,
                offset=self.times.get(tp.partition, -1),
                error=self.time_errors.get(tp.partition),
            )
            for tp in tps
        ]

    def assign(self, tps) -> None:
        self.calls.append("assign")
        self.assigned = [(tp.partition, tp.offset) for tp in tps]

    def poll(self, timeout: float | None = None):
        self.calls.append("poll")
        assert timeout is not None
        self.poll_timeouts.append(timeout)
        item = self.script.pop(0) if self.script else None
        if isinstance(item, BaseException):
            raise item
        self.clock.now += timeout if item is None else self.tick
        return item

    def subscribe(self, *args: Any, **kwargs: Any) -> None:
        raise AssertionError("a browsing consumer must never subscribe")

    def commit(self, *args: Any, **kwargs: Any) -> None:
        raise AssertionError("a browsing consumer must never commit")

    def close(self) -> None:
        self.calls.append("close")
        self.closed = True


class ErrorEvent:
    """A client-level error (broker connection, authentication): librdkafka hands these to the
    `error_cb` while serving `poll()`, never as a polled message."""

    def __init__(self, err: Any):
        self.err = err


class LiveFakeConsumer(FakeConsumer):
    """FakeConsumer for a consumer thread: `poll` really waits, so an idle loop does not spin.

    `feed(...)` hands items to the next polls: messages, per-message error events, exceptions to
    raise, or `ErrorEvent`s, which are passed to the configured `error_cb` (the poll returns None).
    `assigned_event` / `closed_event` let a test wait for the worker instead of sleeping.
    """

    def __init__(
        self,
        conf: dict[str, Any],
        *,
        hanging_metadata_calls: int = 0,
        failing_metadata_calls: int = 0,
        **kwargs: Any,
    ):
        super().__init__(conf, clock=FakeClock(), **kwargs)
        # A paused broker: these many metadata calls block for their whole timeout, then time out.
        self.hanging_metadata_calls = hanging_metadata_calls
        # An unreachable one: these many metadata calls fail at once.
        self.failing_metadata_calls = failing_metadata_calls
        self.metadata_call_times: list[float] = []
        self._inbox: queue.Queue[Any] = queue.Queue()
        self.assigned_event = threading.Event()
        self.closed_event = threading.Event()

    def feed(self, *items: Any) -> None:
        for item in items:
            self._inbox.put(item)

    def list_topics(self, topic: str | None = None, timeout: float | None = None):
        from confluent_kafka import KafkaError, KafkaException

        self.metadata_call_times.append(time.monotonic())
        if self.hanging_metadata_calls > 0:
            self.hanging_metadata_calls -= 1
            self.metadata_timeouts.append(timeout)
            threading.Event().wait(timeout)
            raise KafkaException(KafkaError(KafkaError._TIMED_OUT))
        if self.failing_metadata_calls > 0:
            self.failing_metadata_calls -= 1
            self.metadata_timeouts.append(timeout)
            raise KafkaException(KafkaError(KafkaError._TRANSPORT))
        return super().list_topics(topic, timeout)

    def assign(self, tps) -> None:
        super().assign(tps)
        self.assigned_event.set()

    def poll(self, timeout: float | None = None):
        self.calls.append("poll")
        assert timeout is not None
        self.poll_timeouts.append(timeout)
        try:
            item = self._inbox.get(timeout=timeout)
        except queue.Empty:
            return None
        if isinstance(item, BaseException):
            raise item
        if isinstance(item, ErrorEvent):
            self.conf["error_cb"](item.err)
            return None
        return item

    def close(self) -> None:
        super().close()
        self.closed_event.set()


class LiveConsumers:
    """A consumer factory recording every `LiveFakeConsumer` it creates."""

    def __init__(self, **consumer_kwargs: Any):
        self.created: list[LiveFakeConsumer] = []
        self.consumer_kwargs = consumer_kwargs
        self.error: Exception | None = None  # raised by the factory instead of creating one

    def __call__(self, conf: dict[str, Any]) -> LiveFakeConsumer:
        if self.error is not None:
            raise self.error
        consumer = LiveFakeConsumer(conf, **self.consumer_kwargs)
        self.created.append(consumer)
        return consumer

    def wait_for_consumer(self, timeout: float = 2.0) -> LiveFakeConsumer:
        deadline = time.monotonic() + timeout
        while not self.created:
            assert time.monotonic() < deadline, "no consumer was created"
            time.sleep(0.01)
        return self.created[-1]
