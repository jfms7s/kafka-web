"""One Kafka connection (admin client + producer) per cluster, opened lazily and cached by name."""

from __future__ import annotations

import os
import tempfile
import threading
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from confluent_kafka import KafkaError, Producer
from confluent_kafka.admin import AdminClient

from kafka_web.config.models import ClusterConfig, ClusterInput
from kafka_web.config.store import ClusterStore
from kafka_web.errors import AppError, Conflict
from kafka_web.kafka.client_config import build_client_config
from kafka_web.kafka.errors import call_with_timeout, explain_connect_failure

ADMIN_TIMEOUT_S = 10.0
PRODUCER_FLUSH_TIMEOUT_S = 5
# Defaults under the user's `extra`: a TCP/TLS connect never waits longer than the admin calls.
_CLIENT_DEFAULTS = {"socket.connection.setup.timeout.ms": "10000"}
_PRODUCER_DEFAULTS = {"message.timeout.ms": "30000"}


def check_connectivity(admin: AdminClient, timeout: float = ADMIN_TIMEOUT_S) -> None:
    call_with_timeout(lambda: admin.list_topics(timeout=timeout))


@dataclass(eq=False)
class ClusterConnection:
    name: str
    client_config: dict[str, str]
    admin: AdminClient
    producer: Producer
    connected_at: datetime  # UTC
    streams: set[threading.Event] = field(default_factory=set)  # stop events of live streams

    def close(self) -> None:
        for stop in list(self.streams):
            stop.set()
        self.streams.clear()
        self.producer.flush(PRODUCER_FLUSH_TIMEOUT_S)


class _ReportedErrors:
    """`error_cb` sink. librdkafka reports *why* a broker connection failed (bad SASL password,
    untrusted certificate, refused connection) only as error events, served by `poll()`.

    Keeps the latest event per error code: retries repeat `_ALL_BROKERS_DOWN` many times, and a
    plain ring buffer would evict the one event that names the cause.
    """

    def __init__(self) -> None:
        self._latest: dict[int, KafkaError] = {}
        self._lock = threading.Lock()

    def __call__(self, err: KafkaError) -> None:
        with self._lock:
            self._latest.pop(err.code(), None)  # re-insert: dict order = order of last report
            self._latest[err.code()] = err

    def snapshot(self) -> list[KafkaError]:
        with self._lock:
            return list(self._latest.values())


def _write_temp_pem(pem: str) -> Path:
    fd, name = tempfile.mkstemp(prefix="kafka-web-test-", suffix=".pem")  # mode 0600
    path = Path(name)
    try:
        with os.fdopen(fd, "w", encoding="ascii") as handle:
            handle.write(pem)
    except BaseException:
        path.unlink(missing_ok=True)
        raise
    return path


class ConnectionRegistry:
    def __init__(
        self,
        store: ClusterStore,
        *,
        admin_factory: Callable[[dict[str, Any]], AdminClient] = AdminClient,
        producer_factory: Callable[[dict[str, Any]], Producer] = Producer,
        check: Callable[[AdminClient], None] = check_connectivity,
    ):
        self._store = store
        self._admin_factory = admin_factory
        self._producer_factory = producer_factory
        self._check = check
        self._lock = threading.Lock()  # guards the two dicts below, never held while connecting
        self._connections: dict[str, ClusterConnection] = {}
        self._cluster_locks: dict[str, threading.Lock] = {}  # serialize connect/disconnect per name

    # --- connections --------------------------------------------------------------------------

    def get(self, name: str) -> ClusterConnection:
        conn = self._cached(name)
        if conn is None:
            self._store.get(name)  # NotFound before a lock is allocated for an arbitrary name
            with self._cluster_lock(name):
                conn = self._cached(name)
                if conn is None:
                    conn = self._open(name)
                    with self._lock:
                        self._connections[name] = conn
                    return conn
        conn.admin.poll(0)  # drain queued error events so they cannot pile up on idle connections
        return conn

    def connect(self, name: str) -> ClusterConnection:
        return self.get(name)

    def disconnect(self, name: str) -> None:
        """Close the connection and stop its streams; waits for an in-flight connect first."""
        with self._lock:
            lock = self._cluster_locks.get(name)
        if lock is None:
            return
        with lock, self._lock:
            conn = self._connections.pop(name, None)
        if conn is not None:
            conn.close()

    def is_connected(self, name: str) -> bool:
        with self._lock:
            return name in self._connections

    def active(self) -> list[ClusterConnection]:
        with self._lock:
            return list(self._connections.values())

    def close_all(self) -> None:
        with self._lock:
            names = list(self._connections)
        for name in names:
            self.disconnect(name)

    # --- configuration ------------------------------------------------------------------------

    def client_config(self, name: str) -> dict[str, str]:
        """Resolved librdkafka config of a saved cluster; does not open a connection."""
        cfg = self._store.get(name)
        usability = self._store.usability(cfg)
        if not usability.usable:
            raise Conflict(usability.reason or "Cluster is unusable", code="cluster_unusable")
        return _with_defaults(
            build_client_config(
                cfg,
                sasl_password=self._store.sasl_password(cfg),
                ca_location=self._store.truststore_path(cfg),
            )
        )

    def test(self, inp: ClusterInput, existing: ClusterConfig | None) -> None:
        """Run the connectivity check from unsaved form values with a throwaway admin client.

        Blank secrets / truststore fall back to those stored for `existing` (the edit form).
        """
        cfg, pem, password = self._store.materialize(inp, existing=existing)
        if cfg.uses_sasl and password is None:
            password = self._stored_password(existing)
        temp_pem: Path | None = None
        try:
            ca_location: Path | None = None
            if cfg.uses_tls:
                if pem is not None:
                    ca_location = temp_pem = _write_temp_pem(pem)
                else:
                    ca_location = self._stored_truststore(existing)
            conf = build_client_config(cfg, sasl_password=password, ca_location=ca_location)
            self._connected_admin(_with_defaults(conf))
        finally:
            if temp_pem is not None:
                temp_pem.unlink(missing_ok=True)

    # --- internals ----------------------------------------------------------------------------

    def _cached(self, name: str) -> ClusterConnection | None:
        with self._lock:
            return self._connections.get(name)

    def _cluster_lock(self, name: str) -> threading.Lock:
        with self._lock:
            return self._cluster_locks.setdefault(name, threading.Lock())

    def _open(self, name: str) -> ClusterConnection:
        conf = self.client_config(name)
        admin = self._connected_admin(conf)
        producer = call_with_timeout(lambda: self._producer_factory({**_PRODUCER_DEFAULTS, **conf}))
        return ClusterConnection(
            name=name,
            client_config=conf,
            admin=admin,
            producer=producer,
            connected_at=datetime.now(UTC),
        )

    def _connected_admin(self, conf: dict[str, str]) -> AdminClient:
        """Create an admin client and run the connectivity check; nothing is kept on failure."""
        reported = _ReportedErrors()
        admin = call_with_timeout(lambda: self._admin_factory({**conf, "error_cb": reported}))
        try:
            call_with_timeout(lambda: self._check(admin))
        except AppError as exc:
            admin.poll(0)  # serve the queued error events into `reported`
            raise explain_connect_failure(exc, reported.snapshot()) from None
        return admin

    def _stored_password(self, existing: ClusterConfig | None) -> str:
        password = self._store.sasl_password(existing) if existing is not None else None
        if password is None:
            raise Conflict("SASL password missing from keyring", code="cluster_unusable")
        return password

    def _stored_truststore(self, existing: ClusterConfig | None) -> Path:
        path = self._store.truststore_path(existing) if existing is not None else None
        if path is None or self._store.truststore_summary(existing) is None:
            raise Conflict("Stored truststore is missing or unreadable", code="cluster_unusable")
        return path


def _with_defaults(conf: dict[str, str]) -> dict[str, str]:
    return {**_CLIENT_DEFAULTS, **conf}
