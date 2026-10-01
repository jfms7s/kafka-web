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
