import base64
import datetime as dt
import stat
import threading
import time
from pathlib import Path
from typing import Any

import pytest
from confluent_kafka import KafkaError, KafkaException

from kafka_web.config.models import ClusterInput
from kafka_web.config.secrets import SecretStore
from kafka_web.config.store import ClusterStore
from kafka_web.errors import BrokerError, Conflict, KafkaTimeout, NotFound, Unauthorized
from kafka_web.kafka.registry import ClusterConnection, ConnectionRegistry, check_connectivity
from tests.conftest import CertBundle, MemoryKeyring, make_jks

SASL_SECRET = "sasl-s3cret-value"


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


def raise_kafka(code: int, reason: str = "boom"):
    def _raise(admin: FakeAdmin):
        raise KafkaException(KafkaError(code, reason))

    return _raise


@pytest.fixture
def store(tmp_path: Path, memory_keyring: MemoryKeyring) -> ClusterStore:
    return ClusterStore(tmp_path / "cfg", SecretStore(memory_keyring))


@pytest.fixture
def fakes() -> Fakes:
    return Fakes()


@pytest.fixture
def registry(store: ClusterStore, fakes: Fakes) -> ConnectionRegistry:
    return ConnectionRegistry(store, admin_factory=fakes.admin, producer_factory=fakes.producer)


def plaintext(name: str = "dev", **kw) -> ClusterInput:
    return ClusterInput(**{"name": name, "env": "dev", "bootstrap_servers": "b1:9092", **kw})


def sasl_ssl(certs: CertBundle, name: str = "stg", **kw) -> ClusterInput:
    fields = {
        "name": name,
        "env": "stg",
        "bootstrap_servers": "b1:9094",
        "security_protocol": "SASL_SSL",
        "sasl_mechanism": "PLAIN",
        "sasl_username": "app",
        "sasl_password": SASL_SECRET,
        "truststore_base64": base64.b64encode(make_jks([certs.ca_cert], "changeit")).decode(),
        "truststore_password": "changeit",
        **kw,
    }
    return ClusterInput(**fields)


# --- check_connectivity ------------------------------------------------------------------------


def test_check_connectivity_lists_topics_with_timeout(fakes: Fakes):
    admin = fakes.admin({})
    check_connectivity(admin)  # type: ignore[arg-type]
    assert admin.list_topics_timeouts == [10.0]


def test_check_connectivity_maps_errors(fakes: Fakes):
    fakes.on_list_topics = raise_kafka(KafkaError._TIMED_OUT)
    with pytest.raises(KafkaTimeout):
        check_connectivity(fakes.admin({}))  # type: ignore[arg-type]


# --- get / connect -----------------------------------------------------------------------------


def test_get_connects_lazily(store: ClusterStore, registry: ConnectionRegistry, fakes: Fakes):
    store.create(plaintext())
    assert fakes.admins == []
    assert not registry.is_connected("dev")

    before = dt.datetime.now(dt.UTC)
    conn = registry.get("dev")

    assert isinstance(conn, ClusterConnection)
    assert conn.name == "dev"
    assert registry.is_connected("dev")
    assert before <= conn.connected_at <= dt.datetime.now(dt.UTC)
    assert conn.connected_at.tzinfo is not None
    assert conn.streams == set()
    [admin] = fakes.admins
    [producer] = fakes.producers
    assert conn.admin is admin
    assert conn.producer is producer
    assert admin.list_topics_timeouts == [10.0]
    assert admin.conf["bootstrap.servers"] == "b1:9092"
    assert admin.conf["socket.connection.setup.timeout.ms"] == "10000"
    assert producer.conf["socket.connection.setup.timeout.ms"] == "10000"
    assert producer.conf["message.timeout.ms"] == "30000"
    assert conn.client_config["bootstrap.servers"] == "b1:9092"
    assert all(isinstance(v, str) for v in conn.client_config.values())


def test_second_get_reuses_connection(
    store: ClusterStore, registry: ConnectionRegistry, fakes: Fakes
):
    store.create(plaintext())
    first = registry.get("dev")
    assert registry.get("dev") is first
    assert registry.connect("dev") is first
    assert len(fakes.admins) == 1
    assert len(fakes.producers) == 1


def test_concurrent_gets_create_exactly_one_admin(
    store: ClusterStore, registry: ConnectionRegistry, fakes: Fakes
):
    store.create(plaintext())

    def slow_check(admin):
        time.sleep(0.05)
        return object()

    fakes.on_list_topics = slow_check
    barrier = threading.Barrier(10)
    results: list[ClusterConnection] = []
    errors: list[BaseException] = []

    def worker():
        barrier.wait()
        try:
            results.append(registry.get("dev"))
        except BaseException as exc:
            errors.append(exc)

    threads = [threading.Thread(target=worker) for _ in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=5)

    assert errors == []
    assert len(results) == 10
    assert len(fakes.admins) == 1
    assert len(fakes.producers) == 1
    assert all(conn is results[0] for conn in results)


def test_slow_connect_does_not_block_other_clusters(
    store: ClusterStore, registry: ConnectionRegistry, fakes: Fakes
):
    store.create(plaintext("slow", bootstrap_servers="slow:9092"))
    store.create(plaintext("fast", bootstrap_servers="fast:9092"))
    release = threading.Event()

    def check(admin):
        if admin.conf["bootstrap.servers"] == "slow:9092":
            release.wait(timeout=5)
        return object()

    fakes.on_list_topics = check
    slow = threading.Thread(target=registry.get, args=("slow",))
    slow.start()
    while not fakes.admins:
        time.sleep(0.005)
    try:
        assert registry.get("fast").name == "fast"
        assert not registry.is_connected("slow")
    finally:
        release.set()
        slow.join(timeout=5)
    assert registry.is_connected("slow")


def test_failed_check_is_mapped_and_not_cached(
    store: ClusterStore, registry: ConnectionRegistry, fakes: Fakes
):
    store.create(plaintext())
    fakes.on_list_topics = raise_kafka(KafkaError._TIMED_OUT)

    with pytest.raises(KafkaTimeout):
        registry.get("dev")
    assert not registry.is_connected("dev")
    assert registry.active() == []
    assert fakes.producers == []

    fakes.on_list_topics = lambda admin: object()
    registry.get("dev")
    assert len(fakes.admins) == 2  # retried, not served from a cache


def test_failed_check_reports_the_cause_from_error_callbacks(
    store: ClusterStore, registry: ConnectionRegistry, fakes: Fakes
):
    store.create(plaintext())

    def auth_failure(admin):
        admin.conf["error_cb"](KafkaError(KafkaError._AUTHENTICATION, "SASL authentication error"))
        raise KafkaException(KafkaError(KafkaError._TRANSPORT, "Failed to get metadata"))

    fakes.on_list_topics = auth_failure
    with pytest.raises(Unauthorized) as info:
        registry.get("dev")
    assert "SASL authentication error" in info.value.message


def test_client_constructor_failure_is_mapped(store: ClusterStore, fakes: Fakes):
    store.create(plaintext())

    def broken_factory(conf):
        raise KafkaException(KafkaError(KafkaError._INVALID_ARG, "No such property"))

    registry = ConnectionRegistry(
        store, admin_factory=broken_factory, producer_factory=fakes.producer
    )
    with pytest.raises(BrokerError):
        registry.get("dev")


def test_unusable_cluster_is_a_conflict(
    store: ClusterStore,
    registry: ConnectionRegistry,
    fakes: Fakes,
    memory_keyring: MemoryKeyring,
    certs: CertBundle,
):
    store.create(sasl_ssl(certs))
    memory_keyring.data.clear()

    with pytest.raises(Conflict) as info:
        registry.get("stg")
    assert info.value.code == "cluster_unusable"
    assert info.value.message == "SASL password missing from keyring"
    assert fakes.admins == []


def test_unknown_cluster_is_not_found(registry: ConnectionRegistry):
    with pytest.raises(NotFound) as info:
        registry.get("nope")
    assert info.value.code == "cluster_not_found"


def test_sasl_ssl_connection_uses_secret_and_truststore(
    store: ClusterStore, registry: ConnectionRegistry, fakes: Fakes, certs: CertBundle
):
    cfg = store.create(sasl_ssl(certs))
    registry.get("stg")
    [admin] = fakes.admins
    assert admin.conf["sasl.password"] == SASL_SECRET
    assert admin.conf["ssl.ca.location"] == str(store.truststore_path(cfg))


# --- disconnect / close ------------------------------------------------------------------------


def test_disconnect_closes_connection_and_stops_streams(
    store: ClusterStore, registry: ConnectionRegistry, fakes: Fakes
):
    store.create(plaintext())
    conn = registry.get("dev")
    stream_a, stream_b = threading.Event(), threading.Event()
    conn.streams.update({stream_a, stream_b})

    registry.disconnect("dev")

    assert not registry.is_connected("dev")
    assert stream_a.is_set()
    assert stream_b.is_set()
    assert fakes.producers[0].flush_timeouts == [5]
    registry.disconnect("dev")  # no-op when not connected
    registry.disconnect("never-existed")


def test_reconnect_after_disconnect_creates_new_clients(
    store: ClusterStore, registry: ConnectionRegistry, fakes: Fakes
):
    store.create(plaintext())
    first = registry.get("dev")
    registry.disconnect("dev")
    assert registry.get("dev") is not first
    assert len(fakes.admins) == 2


def test_active_lists_open_connections(store: ClusterStore, registry: ConnectionRegistry):
    store.create(plaintext("a"))
    store.create(plaintext("b"))
    store.create(plaintext("c"))
    registry.get("a")
    registry.get("c")
    assert sorted(conn.name for conn in registry.active()) == ["a", "c"]


def test_close_all(store: ClusterStore, registry: ConnectionRegistry, fakes: Fakes):
    store.create(plaintext("a"))
    store.create(plaintext("b"))
    registry.get("a")
    registry.get("b")
    registry.close_all()
    assert registry.active() == []
    assert all(p.flush_timeouts == [5] for p in fakes.producers)


def test_connection_close_sets_events_and_flushes(fakes: Fakes):
    producer = fakes.producer({})
    event = threading.Event()
    conn = ClusterConnection(
        name="x",
        client_config={},
        admin=fakes.admin({}),  # type: ignore[arg-type]
        producer=producer,  # type: ignore[arg-type]
        connected_at=dt.datetime.now(dt.UTC),
        streams={event},
    )
    conn.close()
    assert event.is_set()
    assert producer.flush_timeouts == [5]
    assert conn.streams == set()


# --- client_config -----------------------------------------------------------------------------


def test_client_config_does_not_open_a_connection(
    store: ClusterStore, registry: ConnectionRegistry, fakes: Fakes, certs: CertBundle
):
    store.create(sasl_ssl(certs))
    conf = registry.client_config("stg")
    assert conf["sasl.password"] == SASL_SECRET
    assert conf["socket.connection.setup.timeout.ms"] == "10000"
    assert "error_cb" not in conf
    assert all(isinstance(v, str) for v in conf.values())
    assert fakes.admins == []
    assert not registry.is_connected("stg")


def test_client_config_of_unusable_cluster_is_a_conflict(
    store: ClusterStore, registry: ConnectionRegistry, memory_keyring: MemoryKeyring, certs
):
    store.create(sasl_ssl(certs))
    memory_keyring.data.clear()
    with pytest.raises(Conflict) as info:
        registry.client_config("stg")
    assert info.value.code == "cluster_unusable"


# --- test() ------------------------------------------------------------------------------------


def test_test_uses_temp_pem_and_deletes_it(
    store: ClusterStore, registry: ConnectionRegistry, fakes: Fakes, certs: CertBundle
):
    seen: dict[str, Any] = {}

    def check(admin):
        path = Path(admin.conf["ssl.ca.location"])
        seen["path"] = path
        seen["mode"] = stat.S_IMODE(path.stat().st_mode)
        seen["pem"] = path.read_text()
        return object()

    fakes.on_list_topics = check
    registry.test(sasl_ssl(certs), None)

    assert seen["mode"] == 0o600
    assert "BEGIN CERTIFICATE" in seen["pem"]
    assert not seen["path"].exists()
    [admin] = fakes.admins
    assert admin.conf["sasl.password"] == SASL_SECRET
    assert admin.conf["socket.connection.setup.timeout.ms"] == "10000"
    assert fakes.producers == []
    assert store.list() == []  # nothing persisted
    assert not registry.is_connected("stg")


def test_test_deletes_temp_pem_when_check_fails(
    registry: ConnectionRegistry, fakes: Fakes, certs: CertBundle
):
    seen: list[Path] = []

    def failing(admin):
        seen.append(Path(admin.conf["ssl.ca.location"]))
        assert seen[0].exists()
        raise KafkaException(KafkaError(KafkaError._TRANSPORT, "down"))

    fakes.on_list_topics = failing
    with pytest.raises(BrokerError) as info:
        registry.test(sasl_ssl(certs), None)
    assert info.value.code == "broker_unreachable"
    assert not seen[0].exists()


def test_test_with_existing_falls_back_to_stored_secret_and_truststore(
    store: ClusterStore, registry: ConnectionRegistry, fakes: Fakes, certs: CertBundle
):
    existing = store.create(sasl_ssl(certs))
    edited = sasl_ssl(certs, sasl_password=None, truststore_base64=None, truststore_password=None)

    registry.test(edited, existing)

    [admin] = fakes.admins
    assert admin.conf["sasl.password"] == SASL_SECRET
    assert admin.conf["ssl.ca.location"] == str(store.truststore_path(existing))
    assert store.truststore_path(existing).exists()  # the stored PEM is not a temp file


def test_test_with_existing_but_missing_secret_is_a_conflict(
    store: ClusterStore,
    registry: ConnectionRegistry,
    fakes: Fakes,
    memory_keyring: MemoryKeyring,
    certs: CertBundle,
):
    existing = store.create(sasl_ssl(certs))
    memory_keyring.data.clear()
    edited = sasl_ssl(certs, sasl_password=None)

    with pytest.raises(Conflict) as info:
        registry.test(edited, existing)
    assert info.value.code == "cluster_unusable"
    assert fakes.admins == []


def test_test_with_existing_but_missing_truststore_is_a_conflict(
    store: ClusterStore, registry: ConnectionRegistry, fakes: Fakes, certs: CertBundle
):
    existing = store.create(sasl_ssl(certs))
    store.truststore_path(existing).unlink()
    edited = sasl_ssl(certs, truststore_base64=None, truststore_password=None)

    with pytest.raises(Conflict) as info:
        registry.test(edited, existing)
    assert info.value.code == "cluster_unusable"
    assert fakes.admins == []


def test_test_plaintext(registry: ConnectionRegistry, fakes: Fakes):
    registry.test(plaintext(), None)
    [admin] = fakes.admins
    assert admin.conf["security.protocol"] == "PLAINTEXT"
    assert admin.list_topics_timeouts == [10.0]
