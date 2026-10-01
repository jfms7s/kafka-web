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
from tests.fakes import FakeAdmin, Fakes

SASL_SECRET = "sasl-s3cret-value"


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
    # A topic deleted between the existence check and produce() must not be re-created.
    assert producer.conf["allow.auto.create.topics"] == "false"
    assert conn.client_config["bootstrap.servers"] == "b1:9092"
    assert all(isinstance(v, str) for v in conn.client_config.values())


def test_extra_cannot_turn_topic_auto_creation_back_on(
    store: ClusterStore, registry: ConnectionRegistry, fakes: Fakes
):
    # Viewing a missing topic must never create it, whatever the user's `extra` says.
    store.create(plaintext(extra={"allow.auto.create.topics": "true"}))
    registry.get("dev")
    [admin] = fakes.admins
    [producer] = fakes.producers
    assert admin.conf["allow.auto.create.topics"] == "false"
    assert producer.conf["allow.auto.create.topics"] == "false"


def test_test_connection_admin_never_auto_creates_topics(
    registry: ConnectionRegistry, fakes: Fakes
):
    registry.test(plaintext(extra={"allow.auto.create.topics": "true"}), None)
    [admin] = fakes.admins
    assert admin.conf["allow.auto.create.topics"] == "false"


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
    entered, release = threading.Event(), threading.Event()

    def check(admin):
        if admin.conf["bootstrap.servers"] == "slow:9092":
            entered.set()
            release.wait(timeout=5)
        return object()

    fakes.on_list_topics = check
    slow = threading.Thread(target=registry.get, args=("slow",))
    slow.start()
    assert entered.wait(timeout=5)
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


def test_cause_survives_a_flood_of_repeated_error_events(
    store: ClusterStore, registry: ConnectionRegistry, fakes: Fakes
):
    store.create(plaintext())

    def tls_failure_then_retries(admin):
        admin.conf["error_cb"](KafkaError(KafkaError._SSL, "SSL handshake failed"))
        for _ in range(200):  # librdkafka keeps retrying until list_topics times out
            admin.conf["error_cb"](KafkaError(KafkaError._ALL_BROKERS_DOWN, "1/1 brokers are down"))
        raise KafkaException(KafkaError(KafkaError._TRANSPORT, "Failed to get metadata"))

    fakes.on_list_topics = tls_failure_then_retries
    with pytest.raises(BrokerError) as info:
        registry.get("dev")
    assert info.value.code == "tls_error"


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


# --- fix round 1 -------------------------------------------------------------------------------


def run_threads(n: int, target) -> list[Any]:
    """Start `n` threads at once on `target()`; return each result or exception (in order)."""
    barrier = threading.Barrier(n)
    results: list[Any] = [None] * n

    def worker(i: int) -> None:
        barrier.wait()
        try:
            results[i] = target()
        except BaseException as exc:
            results[i] = exc

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=10)
    return results


def test_concurrent_gets_on_a_dead_cluster_share_one_attempt(
    store: ClusterStore, registry: ConnectionRegistry, fakes: Fakes
):
    store.create(plaintext())
    duration = 0.3

    def slow_failure(admin):
        time.sleep(duration)
        raise KafkaException(KafkaError(KafkaError._TIMED_OUT, "Failed to get metadata"))

    fakes.on_list_topics = slow_failure
    started = time.monotonic()
    results = run_threads(5, lambda: registry.get("dev"))
    elapsed = time.monotonic() - started

    assert all(isinstance(r, KafkaTimeout) for r in results), results
    assert elapsed < 2 * duration  # one shared attempt, not five serialized ones
    assert len(fakes.admins) == 1
    assert len({id(r) for r in results}) == 5  # each waiter raises its own exception object
    assert all(r.code == "kafka_timeout" and "metadata" in r.message for r in results)

    # a request arriving after the failure completes retries
    with pytest.raises(KafkaTimeout):
        registry.get("dev")
    assert len(fakes.admins) == 2


def test_waiters_receive_the_connection_of_the_shared_attempt(
    store: ClusterStore, registry: ConnectionRegistry, fakes: Fakes
):
    store.create(plaintext())

    def slow(admin):
        time.sleep(0.2)
        return object()

    fakes.on_list_topics = slow
    results = run_threads(5, lambda: registry.get("dev"))
    assert all(isinstance(r, ClusterConnection) for r in results), results
    assert all(r is results[0] for r in results)
    assert len(fakes.admins) == 1


def test_disconnect_waits_for_an_in_flight_connect_and_closes_it(
    store: ClusterStore, registry: ConnectionRegistry, fakes: Fakes
):
    store.create(plaintext())
    entered, release = threading.Event(), threading.Event()

    def gated(admin):
        entered.set()
        release.wait(timeout=5)
        return object()

    fakes.on_list_topics = gated
    results: list[Any] = []
    connector = threading.Thread(target=lambda: results.append(registry.get("dev")))
    connector.start()
    assert entered.wait(timeout=5)

    disconnector = threading.Thread(target=registry.disconnect, args=("dev",))
    disconnector.start()
    time.sleep(0.05)
    assert disconnector.is_alive()  # waiting for the in-flight connect
    release.set()
    connector.join(timeout=5)
    disconnector.join(timeout=5)

    assert not disconnector.is_alive()
    assert not registry.is_connected("dev")
    [conn] = results
    assert conn.closed
    assert fakes.producers[0].flush_timeouts == [5]


def test_update_racing_a_connect_leaves_no_stale_connection(
    store: ClusterStore, registry: ConnectionRegistry, fakes: Fakes
):
    """The PUT handler's sequence (disconnect, update, disconnect) against a slow connect."""
    store.create(plaintext())
    entered, release = threading.Event(), threading.Event()

    def gated(admin):
        if admin.conf["bootstrap.servers"] == "b1:9092":
            entered.set()
            release.wait(timeout=5)
        return object()

    fakes.on_list_topics = gated
    connector = threading.Thread(target=registry.get, args=("dev",))
    connector.start()
    assert entered.wait(timeout=5)  # connect with the old settings is in flight

    def put():
        registry.disconnect("dev")
        store.update("dev", plaintext(bootstrap_servers="b2:9092"))
        registry.disconnect("dev")

    updater = threading.Thread(target=put)
    updater.start()
    time.sleep(0.05)
    release.set()
    connector.join(timeout=5)
    updater.join(timeout=5)

    assert not registry.is_connected("dev")
    assert registry.get("dev").client_config["bootstrap.servers"] == "b2:9092"


def test_connection_repr_has_no_secrets(
    store: ClusterStore, registry: ConnectionRegistry, certs: CertBundle
):
    store.create(sasl_ssl(certs))
    conn = registry.get("stg")
    assert SASL_SECRET not in repr(conn)
    assert SASL_SECRET not in str(conn)
    assert "stg" in repr(conn)


def test_close_marks_connection_closed(store: ClusterStore, registry: ConnectionRegistry):
    store.create(plaintext())
    conn = registry.get("dev")
    assert not conn.closed
    registry.disconnect("dev")
    assert conn.closed


def test_register_stream_on_live_connection(store: ClusterStore, registry: ConnectionRegistry):
    store.create(plaintext())
    conn = registry.get("dev")
    stop = threading.Event()

    registry.register_stream("dev", stop)
    assert stop in conn.streams
    assert not stop.is_set()

    registry.unregister_stream("dev", stop)
    assert stop not in conn.streams
    registry.unregister_stream("dev", stop)  # idempotent


def test_register_stream_without_connection_stops_immediately(
    store: ClusterStore, registry: ConnectionRegistry
):
    store.create(plaintext())
    stop = threading.Event()
    registry.register_stream("dev", stop)  # never connected
    assert stop.is_set()

    registry.get("dev")
    registry.disconnect("dev")
    late = threading.Event()
    registry.register_stream("dev", late)  # connection closed by DELETE/PUT
    assert late.is_set()
    registry.unregister_stream("dev", late)
    registry.unregister_stream("nope", late)


def test_register_stream_on_closed_connection_stops_immediately(
    store: ClusterStore, registry: ConnectionRegistry
):
    store.create(plaintext())
    conn = registry.get("dev")
    conn.close()  # closed but (artificially) still cached
    stop = threading.Event()
    registry.register_stream("dev", stop)
    assert stop.is_set()
    assert stop not in conn.streams


def test_disconnect_stops_registered_streams(store: ClusterStore, registry: ConnectionRegistry):
    store.create(plaintext())
    registry.get("dev")
    stop = threading.Event()
    registry.register_stream("dev", stop)
    registry.disconnect("dev")
    assert stop.is_set()


def test_close_all_closes_every_connection_despite_failures(
    store: ClusterStore, registry: ConnectionRegistry, fakes: Fakes, caplog
):
    for name in ("a", "b", "c"):
        store.create(plaintext(name))
        registry.get(name)

    def broken_flush(timeout=None):
        raise RuntimeError("flush failed: sasl.password=hunter2")

    fakes.producers[0].flush = broken_flush  # type: ignore[method-assign]
    registry.close_all()

    assert registry.active() == []
    assert [p.flush_timeouts for p in fakes.producers[1:]] == [[5], [5]]
    assert "RuntimeError" in caplog.text
    assert "hunter2" not in caplog.text


def test_waiters_get_a_clone_of_errors_with_keyword_only_init(store: ClusterStore, fakes: Fakes):
    from kafka_web.config.truststore import TruststoreError

    store.create(plaintext())

    def check(admin):
        time.sleep(0.2)
        raise TruststoreError("bad truststore", code="truststore_invalid")

    registry = ConnectionRegistry(
        store, admin_factory=fakes.admin, producer_factory=fakes.producer, check=check
    )
    results = run_threads(3, lambda: registry.get("dev"))
    assert all(isinstance(r, TruststoreError) for r in results), results
    assert {r.code for r in results} == {"truststore_invalid"}
    assert len(fakes.admins) == 1
