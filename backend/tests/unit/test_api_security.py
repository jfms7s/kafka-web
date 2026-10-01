"""DNS-rebinding / cross-site defences: only local Host headers, only local Origins may write."""

from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from kafka_web.api.app import create_app
from kafka_web.api.security import is_same_origin
from kafka_web.config.secrets import SecretStore
from kafka_web.config.store import ClusterStore
from kafka_web.kafka.registry import ConnectionRegistry
from tests.conftest import MemoryKeyring
from tests.fakes import Fakes

BODY = {"name": "dev", "env": "dev", "bootstrap_servers": "b1:9092"}


@pytest.fixture
def store(tmp_path: Path, memory_keyring: MemoryKeyring) -> ClusterStore:
    return ClusterStore(tmp_path / "cfg", SecretStore(memory_keyring))


@pytest.fixture
def fakes() -> Fakes:
    return Fakes()


@pytest.fixture
def app(store: ClusterStore, fakes: Fakes):
    registry = ConnectionRegistry(store, admin_factory=fakes.admin, producer_factory=fakes.producer)
    return create_app(store=store, registry=registry)


@pytest.mark.parametrize(
    ("origin", "host", "scheme"),
    [
        (None, "127.0.0.1:8000", "http"),  # curl and other non-browser clients send none
        (None, None, "http"),
        ("http://127.0.0.1:8000", "127.0.0.1:8000", "http"),  # the built app
        ("http://127.0.0.1:5173", "127.0.0.1:5173", "http"),  # Vite proxy keeps the Host
        ("http://localhost:5173", "localhost:5173", "http"),
        ("HTTP://LOCALHOST:5173", "LocalHost:5173", "http"),
        ("http://127.0.0.1", "127.0.0.1", "http"),
        ("http://127.0.0.1:80", "127.0.0.1", "http"),
        ("http://127.0.0.1", "127.0.0.1:80", "http"),
        ("https://localhost", "localhost:443", "https"),
        ("http://127.0.0.1:8000", "127.0.0.1:8000", "ws"),  # WebSocket handshake
        ("https://127.0.0.1:8443", "127.0.0.1:8443", "wss"),
    ],
)
def test_same_origins(origin: str | None, host: str | None, scheme: str):
    assert is_same_origin(origin, host, scheme)


@pytest.mark.parametrize(
    ("origin", "host", "scheme"),
    [
        # another local process (dev server, notebook) on another port is another origin
        ("http://127.0.0.1:5173", "127.0.0.1:8000", "http"),
        ("http://localhost:8888", "localhost:8000", "http"),
        ("http://127.0.0.1:8000", "127.0.0.1:8001", "ws"),
        ("http://127.0.0.1", "127.0.0.1:8000", "http"),
        # localhost and 127.0.0.1 are different origins
        ("http://localhost:8000", "127.0.0.1:8000", "http"),
        ("http://127.0.0.1:8000", "localhost:8000", "ws"),
        # scheme mismatch
        ("https://127.0.0.1:8000", "127.0.0.1:8000", "http"),
        ("http://127.0.0.1:8000", "127.0.0.1:8000", "wss"),
        ("http://localhost", "localhost", "https"),
        # same origin, but not a local host
        ("http://evil.example:8000", "evil.example:8000", "http"),
        # no Host to compare with
        ("http://127.0.0.1:8000", None, "http"),
        ("http://127.0.0.1:8000", "127.0.0.1:notaport", "http"),
        # malformed or opaque Origins
        ("null", "127.0.0.1:8000", "http"),
        ("", "127.0.0.1:8000", "http"),
        ("http://evil.example", "127.0.0.1:8000", "http"),
        ("http://localhost.evil.example", "localhost", "http"),
        ("http://127.0.0.1.evil.example:8000", "127.0.0.1:8000", "http"),
        ("ftp://localhost", "localhost", "http"),
        ("http://user@localhost:5173", "localhost:5173", "http"),
        ("http://localhost:notaport", "localhost", "http"),
        ("http://localhost:99999", "localhost", "http"),
        ("http://[::1]:8000", "[::1]:8000", "http"),
        ("localhost:5173", "localhost:5173", "http"),
        ("http://localhost:5173/path", "localhost:5173", "http"),
        ("file://", "127.0.0.1:8000", "http"),
    ],
)
def test_cross_origins(origin: str, host: str | None, scheme: str):
    assert not is_same_origin(origin, host, scheme)


@pytest.mark.parametrize("host", ["localhost", "127.0.0.1", "localhost:8000", "127.0.0.1:5173"])
def test_local_host_header_is_served(app, host: str):
    with TestClient(app, base_url=f"http://{host}") as client:
        assert client.get("/api/clusters").status_code == 200


@pytest.mark.parametrize("host", ["evil.example", "evil.example:8000", "attacker.localhost"])
def test_foreign_host_header_is_rejected(app, host: str, fakes: Fakes, store: ClusterStore):
    with TestClient(app, base_url=f"http://{host}") as client:
        assert client.get("/api/clusters").status_code in (400, 403)
        response = client.post("/api/clusters/test", json=BODY)
        assert response.status_code in (400, 403)
        assert client.post("/api/clusters", json=BODY).status_code in (400, 403)
    assert fakes.admins == []
    assert store.list() == []


@pytest.mark.parametrize("method", ["post", "put", "delete", "patch"])
def test_unsafe_request_from_foreign_origin_is_403(app, method: str, store: ClusterStore):
    with TestClient(app, base_url="http://127.0.0.1:8000") as client:
        response = client.request(
            method.upper(),
            "/api/clusters" if method in ("post", "patch") else "/api/clusters/dev",
            json=BODY,
            headers={"Origin": "http://evil.example"},
        )
    assert response.status_code == 403
    assert response.json()["code"] == "forbidden_origin"
    assert response.json()["message"]
    assert store.list() == []


def test_rebinding_test_connection_cannot_reach_stored_secret(app, fakes: Fakes):
    """The attack from the review: POST /test?existing=<name> with an attacker bootstrap."""
    with TestClient(app, base_url="http://127.0.0.1:8000") as client:
        response = client.post(
            "/api/clusters/test",
            params={"existing": "dev"},
            json={**BODY, "bootstrap_servers": "attacker.example:9092"},
            headers={"Origin": "http://attacker.example:8000"},
        )
    assert response.status_code == 403
    assert fakes.admins == []


def test_null_origin_is_403(app):
    with TestClient(app, base_url="http://127.0.0.1:8000") as client:
        response = client.post("/api/clusters", json=BODY, headers={"Origin": "null"})
    assert response.status_code == 403


def test_same_origin_write_is_allowed(app):
    with TestClient(app, base_url="http://127.0.0.1:8000") as client:
        response = client.post(
            "/api/clusters", json=BODY, headers={"Origin": "http://127.0.0.1:8000"}
        )
    assert response.status_code == 201


def test_vite_dev_proxy_write_is_allowed(app):
    """vite.config.ts proxies /api with changeOrigin=false: the Host stays 127.0.0.1:5173."""
    with TestClient(app, base_url="http://127.0.0.1:5173") as client:
        response = client.post(
            "/api/clusters", json=BODY, headers={"Origin": "http://127.0.0.1:5173"}
        )
    assert response.status_code == 201


@pytest.mark.parametrize(
    "origin", ["http://127.0.0.1:5173", "http://localhost:8000", "http://127.0.0.1:8888"]
)
def test_write_from_another_local_origin_is_403(app, origin: str, store: ClusterStore):
    with TestClient(app, base_url="http://127.0.0.1:8000") as client:
        response = client.post("/api/clusters", json=BODY, headers={"Origin": origin})
    assert response.status_code == 403
    assert response.json()["code"] == "forbidden_origin"
    assert store.list() == []


def test_no_origin_is_allowed(app):
    with TestClient(app, base_url="http://127.0.0.1:8000") as client:
        assert client.post("/api/clusters", json=BODY).status_code == 201


def test_safe_methods_ignore_origin(app):
    with TestClient(app, base_url="http://127.0.0.1:8000") as client:
        response = client.get("/api/clusters", headers={"Origin": "http://evil.example"})
    assert response.status_code == 200
