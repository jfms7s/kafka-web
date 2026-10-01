"""Cluster API against real brokers: PLAINTEXT, SASL_SSL (JKS truststore), failure modes."""

import base64
import datetime as dt
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID
from fastapi.testclient import TestClient

from kafka_web.config.store import ClusterStore
from tests.conftest import CertBundle, make_jks

pytestmark = pytest.mark.integration

MAX_FAILURE_S = 15  # Review Focus 1: a dead or wrong broker answers within ~12 s, never hangs


def plaintext_body(bootstrap: str, name: str = "local") -> dict[str, Any]:
    return {"name": name, "env": "dev", "bootstrap_servers": bootstrap}


def sasl_ssl_body(
    bootstrap: str,
    ca_cert: x509.Certificate,
    *,
    name: str = "secure",
    password: str = "app-secret",
) -> dict[str, Any]:
    return {
        "name": name,
        "env": "stg",
        "bootstrap_servers": bootstrap,
        "security_protocol": "SASL_SSL",
        "sasl_mechanism": "PLAIN",
        "sasl_username": "app",
        "sasl_password": password,
        "truststore_base64": base64.b64encode(make_jks([ca_cert], "changeit")).decode(),
        "truststore_password": "changeit",
    }


def other_ca() -> x509.Certificate:
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "Untrusted CA")])
    now = dt.datetime.now(dt.UTC)
    return (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - dt.timedelta(days=1))
        .not_valid_after(now + dt.timedelta(days=30))
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .sign(key, hashes.SHA256())
    )


def timed(call: Callable[[], Any]) -> tuple[Any, float]:
    started = time.monotonic()
    result = call()
    return result, time.monotonic() - started


def test_create_plaintext_and_connect(
    api: TestClient, kafka_plaintext: str, topic_factory: Callable[..., str]
):
    topic = topic_factory()
    assert api.post("/api/clusters", json=plaintext_body(kafka_plaintext)).status_code == 201

    response = api.post("/api/clusters/local/connect")
    assert response.status_code == 200, response.text
    assert response.json()["connected"] is True

    [conn] = api.get("/api/status").json()["connections"]
    assert conn["name"] == "local"
    assert conn["bootstrap_servers"] == kafka_plaintext
    registry = api.app.state.registry  # type: ignore[attr-defined]
    assert topic in registry.get("local").admin.list_topics(timeout=10).topics

    assert api.post("/api/clusters/local/disconnect").status_code == 204
    assert api.get("/api/status").json() == {"connections": []}


def test_sasl_ssl_with_jks_truststore_end_to_end(
    api: TestClient, store: ClusterStore, kafka_sasl_ssl: tuple[str, CertBundle]
):
    bootstrap, certs = kafka_sasl_ssl
    body = sasl_ssl_body(bootstrap, certs.ca_cert)

    assert api.post("/api/clusters/test", json=body).json() == {"ok": True}
    created = api.post("/api/clusters", json=body)
    assert created.status_code == 201, created.text
    assert created.json()["usable"] is True

    response = api.post("/api/clusters/secure/connect")
    assert response.status_code == 200, response.text
    assert response.json()["connected"] is True
    assert [c["name"] for c in api.get("/api/status").json()["connections"]] == ["secure"]
    assert "app-secret" not in api.get("/api/clusters").text
    assert store.list()[0].truststore == "truststores/secure.pem"


def test_sasl_ssl_wrong_password_401(api: TestClient, kafka_sasl_ssl: tuple[str, CertBundle]):
    bootstrap, certs = kafka_sasl_ssl
    body = sasl_ssl_body(bootstrap, certs.ca_cert, password="wrong-secret")
    api.post("/api/clusters", json=body)

    response, elapsed = timed(lambda: api.post("/api/clusters/secure/connect"))

    assert response.status_code == 401, response.text
    assert response.json()["code"] == "authentication_failed"
    assert "wrong-secret" not in response.text
    assert elapsed < MAX_FAILURE_S
    assert api.get("/api/status").json() == {"connections": []}


def test_sasl_ssl_untrusted_ca_fails(api: TestClient, kafka_sasl_ssl: tuple[str, CertBundle]):
    bootstrap, _ = kafka_sasl_ssl
    body = sasl_ssl_body(bootstrap, other_ca())

    response, elapsed = timed(lambda: api.post("/api/clusters/test", json=body))

    assert response.status_code in (502, 504), response.text
    assert response.json()["code"] == "tls_error"
    assert elapsed < MAX_FAILURE_S


def test_connect_unreachable_broker_times_out(api: TestClient):
    body = plaintext_body("127.0.0.1:1", name="dead")

    response, elapsed = timed(lambda: api.post("/api/clusters/test", json=body))
    assert response.status_code in (502, 504), response.text
    assert response.json()["code"] in ("broker_unreachable", "kafka_timeout")
    assert elapsed < MAX_FAILURE_S

    api.post("/api/clusters", json=body)
    response, elapsed = timed(lambda: api.post("/api/clusters/dead/connect"))
    assert response.status_code in (502, 504), response.text
    assert elapsed < MAX_FAILURE_S
    assert api.get("/api/status").json() == {"connections": []}


def test_parallel_connects_to_unreachable_broker_share_one_attempt(api: TestClient):
    """Several page requests at once must not queue up one 10 s attempt each."""
    api.post("/api/clusters", json=plaintext_body("127.0.0.1:1", name="dead"))

    def connect(_: int) -> int:
        return api.post("/api/clusters/dead/connect").status_code

    started = time.monotonic()
    with ThreadPoolExecutor(max_workers=5) as pool:
        statuses = list(pool.map(connect, range(5)))
    elapsed = time.monotonic() - started

    assert all(status in (502, 504) for status in statuses), statuses
    assert elapsed < MAX_FAILURE_S


def test_test_connection_does_not_persist(
    api: TestClient, store: ClusterStore, kafka_plaintext: str
):
    response = api.post("/api/clusters/test", json=plaintext_body(kafka_plaintext))
    assert response.status_code == 200, response.text
    assert response.json() == {"ok": True}
    assert api.get("/api/clusters").json() == []
    assert api.get("/api/status").json() == {"connections": []}
    assert store.list() == []
