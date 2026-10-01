"""Recording fakes for confluent-kafka clients, injected through ConnectionRegistry factories."""

import threading
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
