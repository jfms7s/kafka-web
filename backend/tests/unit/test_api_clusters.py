import base64
import datetime as dt
import threading
from pathlib import Path
from typing import Any

import pytest
from confluent_kafka import KafkaError, KafkaException
from fastapi import FastAPI
from fastapi.testclient import TestClient

from kafka_web.api.app import create_app
from kafka_web.config.secrets import SecretStore
from kafka_web.config.store import ClusterStore
from kafka_web.kafka.registry import ConnectionRegistry
from tests.conftest import CertBundle, MemoryKeyring, make_jks
from tests.fakes import Fakes

SASL_SECRET = "sasl-s3cret-value"
TRUSTSTORE_PASSWORD = "truststore-pw-value"
LOCAL = "http://127.0.0.1:8000"


@pytest.fixture
def store(tmp_path: Path, memory_keyring: MemoryKeyring) -> ClusterStore:
    return ClusterStore(tmp_path / "cfg", SecretStore(memory_keyring))


@pytest.fixture
def fakes() -> Fakes:
    return Fakes()


@pytest.fixture
def registry(store: ClusterStore, fakes: Fakes) -> ConnectionRegistry:
    return ConnectionRegistry(store, admin_factory=fakes.admin, producer_factory=fakes.producer)


@pytest.fixture
def client(store: ClusterStore, registry: ConnectionRegistry):
    with TestClient(create_app(store=store, registry=registry), base_url=LOCAL) as client:
        yield client


@pytest.fixture
def jks_b64(certs: CertBundle) -> str:
    return base64.b64encode(make_jks([certs.ca_cert], TRUSTSTORE_PASSWORD)).decode()


def plaintext_body(name: str = "dev", **kw) -> dict[str, Any]:
    return {"name": name, "env": "dev", "bootstrap_servers": "b1:9092", **kw}


def sasl_ssl_body(jks_b64: str, name: str = "stg-eu", **kw) -> dict[str, Any]:
    return {
        "name": name,
        "env": "stg",
        "region": "eu-west-1",
        "bootstrap_servers": "b1:9094,b2:9094",
        "security_protocol": "SASL_SSL",
        "sasl_mechanism": "SCRAM-SHA-512",
        "sasl_username": "svc",
        "sasl_password": SASL_SECRET,
        "truststore_base64": jks_b64,
        "truststore_password": TRUSTSTORE_PASSWORD,
        **kw,
    }


def assert_no_secrets(text: str, jks_b64: str = "") -> None:
    for needle in (
        SASL_SECRET,
        TRUSTSTORE_PASSWORD,
        '"sasl_password"',
        '"truststore_base64"',
        '"truststore_password"',
        "BEGIN CERTIFICATE",
    ):
        assert needle not in text
    if jks_b64:
        assert jks_b64[:40] not in text


def assert_error(response, status: int, code: str, field: str | None = None) -> dict:
    assert response.status_code == status, response.text
    body = response.json()
    assert body["code"] == code
    assert isinstance(body["message"], str)
    assert body["message"]
    if field is not None:
        assert body["field"] == field
    assert set(body) <= {"code", "message", "field"}
    return body


# --- CRUD --------------------------------------------------------------------------------------


def test_list_empty(client: TestClient):
    response = client.get("/api/clusters")
    assert response.status_code == 200
    assert response.json() == []


def test_create_plaintext(client: TestClient):
    response = client.post("/api/clusters", json=plaintext_body())
    assert response.status_code == 201
    assert response.json() == {
        "name": "dev",
        "env": "dev",
        "region": None,
        "bootstrap_servers": "b1:9092",
        "security_protocol": "PLAINTEXT",
        "sasl_mechanism": None,
        "sasl_username": None,
        "has_sasl_password": False,
        "read_only": False,
        "extra": {},
        "truststore": None,
        "usable": True,
        "unusable_reason": None,
        "connected": False,
    }


def test_create_sasl_ssl_view_has_no_secrets(client: TestClient, jks_b64: str, certs):
    response = client.post("/api/clusters", json=sasl_ssl_body(jks_b64))
    assert response.status_code == 201
    assert_no_secrets(response.text, jks_b64)
    body = response.json()
    assert body["has_sasl_password"] is True
    assert body["sasl_username"] == "svc"
    assert body["usable"] is True
    [cert] = body["truststore"]
    assert cert["subject"] == "CN=Test CA"
    assert dt.datetime.fromisoformat(cert["not_after"]) == certs.ca_cert.not_valid_after_utc

    for url in ("/api/clusters", "/api/clusters/stg-eu"):
        response = client.get(url)
        assert response.status_code == 200
        assert_no_secrets(response.text, jks_b64)


def test_get_one(client: TestClient):
    client.post("/api/clusters", json=plaintext_body(region="eu", read_only=True))
    body = client.get("/api/clusters/dev").json()
    assert body["region"] == "eu"
    assert body["read_only"] is True


def test_list_reports_unusable_cluster(
    client: TestClient, jks_b64: str, memory_keyring: MemoryKeyring
):
    client.post("/api/clusters", json=sasl_ssl_body(jks_b64))
    memory_keyring.data.clear()
    [body] = client.get("/api/clusters").json()
    assert body["usable"] is False
    assert body["unusable_reason"] == "SASL password missing from keyring"
    assert body["has_sasl_password"] is False


def test_get_unknown_is_404(client: TestClient):
    assert_error(client.get("/api/clusters/nope"), 404, "cluster_not_found")


def test_duplicate_is_409(client: TestClient):
    client.post("/api/clusters", json=plaintext_body())
    assert_error(client.post("/api/clusters", json=plaintext_body()), 409, "cluster_exists")


def test_tls_without_truststore_is_422(client: TestClient, jks_b64: str):
    body = sasl_ssl_body(jks_b64, truststore_base64=None, truststore_password=None)
    assert_error(client.post("/api/clusters", json=body), 422, "truststore_required", "truststore")


def test_model_field_error_is_422(client: TestClient):
    response = client.post("/api/clusters", json=plaintext_body(name="Bad Name"))
    assert_error(response, 422, "validation_failed", "name")


def test_unknown_body_key_is_422(client: TestClient):
    response = client.post("/api/clusters", json=plaintext_body(bogus=1))
    assert_error(response, 422, "validation_failed", "bogus")


def test_wrong_truststore_password_is_422_without_echoing_it(client: TestClient, jks_b64: str):
    body = sasl_ssl_body(jks_b64, truststore_password="not-the-password")
    response = client.post("/api/clusters", json=body)
    assert response.status_code == 422
    assert "not-the-password" not in response.text
    assert SASL_SECRET not in response.text


def test_malformed_json_is_422(client: TestClient):
    response = client.post(
        "/api/clusters", content=b"{not json", headers={"content-type": "application/json"}
    )
    body = assert_error(response, 422, "validation_failed")
    assert "field" not in body  # the decode position is not a field name
    assert "JSON" in body["message"]


def test_missing_body_is_422(client: TestClient):
    assert_error(client.post("/api/clusters"), 422, "validation_failed")


def test_non_object_body_is_422(client: TestClient):
    response = client.post("/api/clusters", json=["not", "an", "object"])
    assert_error(response, 422, "validation_failed")


def test_query_param_error_names_the_field(client: TestClient):
    app = client.app

    @app.get("/api/probe")  # type: ignore[union-attr]
    def probe(limit: int) -> int:
        return limit

    body = assert_error(client.get("/api/probe", params={"limit": "x"}), 422, "validation_failed")
    assert body["field"] == "limit"
    assert body["message"].startswith("limit: ")


def test_update_keeps_blank_secrets(client: TestClient, jks_b64: str, memory_keyring):
    client.post("/api/clusters", json=sasl_ssl_body(jks_b64))
    edited = sasl_ssl_body(
        jks_b64,
        env="stg2",
        sasl_password="",
        truststore_base64="",
        truststore_password=None,
    )
    response = client.put("/api/clusters/stg-eu", json=edited)
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["env"] == "stg2"
    assert body["has_sasl_password"] is True
    assert body["truststore"][0]["subject"] == "CN=Test CA"
    assert_no_secrets(response.text, jks_b64)


def test_update_unknown_is_404(client: TestClient):
    assert_error(
        client.put("/api/clusters/nope", json=plaintext_body("nope")), 404, "cluster_not_found"
    )


def test_update_cannot_rename(client: TestClient):
    client.post("/api/clusters", json=plaintext_body())
    response = client.put("/api/clusters/dev", json=plaintext_body("other"))
    assert_error(response, 422, "name_immutable", "name")


def test_update_disconnects_open_connection(
    client: TestClient, registry: ConnectionRegistry, fakes: Fakes
):
    client.post("/api/clusters", json=plaintext_body())
    conn = registry.get("dev")
    stop = threading.Event()
    conn.streams.add(stop)

    response = client.put("/api/clusters/dev", json=plaintext_body(bootstrap_servers="b2:9092"))

    assert response.status_code == 200
    assert response.json()["connected"] is False
    assert not registry.is_connected("dev")
    assert stop.is_set()
    assert registry.get("dev").client_config["bootstrap.servers"] == "b2:9092"


def test_delete(client: TestClient, registry: ConnectionRegistry):
    client.post("/api/clusters", json=plaintext_body())
    conn = registry.get("dev")
    stop = threading.Event()
    conn.streams.add(stop)

    response = client.delete("/api/clusters/dev")

    assert response.status_code == 204
    assert response.content == b""
    assert not registry.is_connected("dev")
    assert stop.is_set()
    assert_error(client.get("/api/clusters/dev"), 404, "cluster_not_found")


def test_delete_unknown_is_404(client: TestClient):
    assert_error(client.delete("/api/clusters/nope"), 404, "cluster_not_found")


# --- connect / disconnect / status -------------------------------------------------------------


def test_connect_and_status(client: TestClient, registry: ConnectionRegistry):
    client.post("/api/clusters", json=plaintext_body(region="eu", read_only=True))

    response = client.post("/api/clusters/dev/connect")
    assert response.status_code == 200
    assert response.json()["connected"] is True
    registry.get("dev").streams.add(threading.Event())

    response = client.get("/api/status")
    assert response.status_code == 200
    [conn] = response.json()["connections"]
    assert conn["name"] == "dev"
    assert conn["env"] == "dev"
    assert conn["region"] == "eu"
    assert conn["bootstrap_servers"] == "b1:9092"
    assert conn["read_only"] is True
    assert conn["active_streams"] == 1
    connected_at = dt.datetime.fromisoformat(conn["connected_at"])
    assert connected_at.tzinfo is not None
    assert abs(dt.datetime.now(dt.UTC) - connected_at) < dt.timedelta(minutes=1)
    assert client.get("/api/clusters/dev").json()["connected"] is True


def test_status_has_no_secrets(client: TestClient, registry: ConnectionRegistry, jks_b64: str):
    client.post("/api/clusters", json=sasl_ssl_body(jks_b64))
    client.post("/api/clusters/stg-eu/connect")
    response = client.get("/api/status")
    assert len(response.json()["connections"]) == 1
    assert_no_secrets(response.text, jks_b64)


def test_status_empty(client: TestClient):
    assert client.get("/api/status").json() == {"connections": []}


def test_connect_failure_is_mapped_and_not_cached(
    client: TestClient, registry: ConnectionRegistry, fakes: Fakes
):
    client.post("/api/clusters", json=plaintext_body())

    def timeout(admin):
        raise KafkaException(KafkaError(KafkaError._TIMED_OUT, "Failed to get metadata"))

    fakes.on_list_topics = timeout
    assert_error(client.post("/api/clusters/dev/connect"), 504, "kafka_timeout")
    assert not registry.is_connected("dev")
    assert client.get("/api/status").json() == {"connections": []}


def test_connect_unusable_is_409(client: TestClient, jks_b64: str, memory_keyring):
    client.post("/api/clusters", json=sasl_ssl_body(jks_b64))
    memory_keyring.data.clear()
    assert_error(client.post("/api/clusters/stg-eu/connect"), 409, "cluster_unusable")


def test_connect_unknown_is_404(client: TestClient):
    assert_error(client.post("/api/clusters/nope/connect"), 404, "cluster_not_found")


def test_disconnect(client: TestClient, registry: ConnectionRegistry):
    client.post("/api/clusters", json=plaintext_body())
    client.post("/api/clusters/dev/connect")
    response = client.post("/api/clusters/dev/disconnect")
    assert response.status_code == 204
    assert not registry.is_connected("dev")
    assert client.post("/api/clusters/dev/disconnect").status_code == 204  # idempotent


# --- test connection ---------------------------------------------------------------------------


def test_test_connection_ok(client: TestClient, store: ClusterStore, fakes: Fakes, jks_b64):
    response = client.post("/api/clusters/test", json=sasl_ssl_body(jks_b64))
    assert response.status_code == 200
    assert response.json() == {"ok": True}
    assert len(fakes.admins) == 1
    assert store.list() == []


def test_test_connection_failure(client: TestClient, fakes: Fakes):
    def down(admin):
        raise KafkaException(KafkaError(KafkaError._TRANSPORT, "Failed to get metadata"))

    fakes.on_list_topics = down
    response = client.post("/api/clusters/test", json=plaintext_body())
    assert_error(response, 502, "broker_unreachable")


def test_test_connection_validates_input(client: TestClient, jks_b64: str):
    body = sasl_ssl_body(jks_b64, sasl_password=None)
    response = client.post("/api/clusters/test", json=body)
    assert_error(response, 422, "sasl_password_required", "sasl_password")


def test_test_connection_with_existing_uses_stored_secrets(
    client: TestClient, fakes: Fakes, jks_b64: str
):
    client.post("/api/clusters", json=sasl_ssl_body(jks_b64))
    edited = sasl_ssl_body(jks_b64, sasl_password="", truststore_base64="")
    response = client.post("/api/clusters/test", params={"existing": "stg-eu"}, json=edited)
    assert response.status_code == 200, response.text
    [admin] = fakes.admins
    assert admin.conf["sasl.password"] == SASL_SECRET


def test_test_connection_with_unknown_existing_is_404(client: TestClient):
    response = client.post("/api/clusters/test", params={"existing": "nope"}, json=plaintext_body())
    assert_error(response, 404, "cluster_not_found")


# --- app plumbing ------------------------------------------------------------------------------


def test_unknown_route_has_error_body(client: TestClient):
    assert_error(client.get("/api/nope"), 404, "not_found")


def test_method_not_allowed_has_error_body(client: TestClient):
    assert_error(client.patch("/api/clusters"), 405, "method_not_allowed")


def test_unexpected_exception_is_500_with_error_body(store: ClusterStore):
    class Exploding(ConnectionRegistry):
        def active(self):
            raise RuntimeError("secret internals")

    app = create_app(store=store, registry=Exploding(store))
    with TestClient(app, base_url=LOCAL, raise_server_exceptions=False) as client:
        response = client.get("/api/status")
    body = assert_error(response, 500, "internal_error")
    assert "secret internals" not in body["message"]


def test_shutdown_closes_all_connections(store: ClusterStore, registry: ConnectionRegistry):
    with TestClient(create_app(store=store, registry=registry), base_url=LOCAL) as client:
        client.post("/api/clusters", json=plaintext_body())
        client.post("/api/clusters/dev/connect")
        assert registry.is_connected("dev")
    assert not registry.is_connected("dev")


def test_static_dir_is_served_without_shadowing_api(tmp_path: Path, store, registry):
    static = tmp_path / "static"
    static.mkdir()
    (static / "index.html").write_text("<h1>kafka-web</h1>")
    app = create_app(store=store, registry=registry, static_dir=static)
    with TestClient(app, base_url=LOCAL) as client:
        assert "kafka-web" in client.get("/").text
        assert client.get("/api/clusters").json() == []


def test_default_app_uses_config_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    root = tmp_path / "cfg"
    root.mkdir()
    (root / "clusters.yaml").write_text(
        "clusters:\n  - {name: local, env: dev, bootstrap_servers: 'localhost:9092'}\n"
    )
    monkeypatch.setenv("KAFKA_WEB_CONFIG_DIR", str(root))
    app = create_app()
    assert isinstance(app, FastAPI)
    with TestClient(app, base_url=LOCAL) as client:
        [cluster] = client.get("/api/clusters").json()
    assert cluster["name"] == "local"


@pytest.mark.parametrize(
    ("change", "code"),
    [
        ({"name": "other"}, "name_immutable"),
        (
            {
                "security_protocol": "SASL_PLAINTEXT",
                "sasl_mechanism": "PLAIN",
                "sasl_username": "u",
            },
            "sasl_password_required",
        ),
    ],
)
def test_invalid_update_keeps_connection_and_streams(
    client: TestClient, registry: ConnectionRegistry, change: dict[str, Any], code: str
):
    client.post("/api/clusters", json=plaintext_body())
    conn = registry.get("dev")
    stop = threading.Event()
    conn.streams.add(stop)

    response = client.put("/api/clusters/dev", json={**plaintext_body(), **change})

    assert_error(response, 422, code)
    assert registry.get("dev") is conn
    assert not stop.is_set()
    assert not conn.closed
