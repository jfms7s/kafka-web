"""Serving the built frontend: SPA fallback that never shadows or masks the API."""

from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from kafka_web.api.app import create_app
from kafka_web.config.secrets import SecretStore
from kafka_web.config.store import ClusterStore
from kafka_web.kafka.registry import ConnectionRegistry
from tests.conftest import MemoryKeyring

INDEX = '<!doctype html><div id="root"></div>'
LOCAL = "http://127.0.0.1:8000"


@pytest.fixture
def static_dir(tmp_path: Path) -> Path:
    root = tmp_path / "dist"
    (root / "assets").mkdir(parents=True)
    (root / "index.html").write_text(INDEX)
    (root / "assets" / "a.js").write_text("console.log(1)")
    return root


@pytest.fixture
def client(tmp_path: Path, static_dir: Path, memory_keyring: MemoryKeyring):
    store = ClusterStore(tmp_path / "cfg", SecretStore(memory_keyring))
    app = create_app(store=store, registry=ConnectionRegistry(store), static_dir=static_dir)
    with TestClient(app, base_url=LOCAL) as client:
        yield client


def test_root_serves_index(client: TestClient) -> None:
    response = client.get("/")
    assert response.status_code == 200
    assert response.text == INDEX


@pytest.mark.parametrize("path", ["/c/x/topics", "/clusters/new", "/c/x/topics/some.topic"])
def test_deep_links_fall_back_to_index(client: TestClient, path: str) -> None:
    response = client.get(path)
    assert response.status_code == 200
    assert response.text == INDEX
    assert response.headers["content-type"].startswith("text/html")


def test_asset_is_served_as_file(client: TestClient) -> None:
    response = client.get("/assets/a.js")
    assert response.status_code == 200
    assert response.text == "console.log(1)"


def test_missing_asset_is_404_not_index(client: TestClient) -> None:
    assert client.get("/assets/missing.js").status_code == 404


@pytest.mark.parametrize("path", ["/api/nope", "/api", "/api/clusters/x/nope"])
def test_unknown_api_path_is_json_404(client: TestClient, path: str) -> None:
    response = client.get(path)
    assert response.status_code == 404
    assert response.json()["code"] == "not_found"


def test_api_routes_are_not_shadowed(client: TestClient) -> None:
    response = client.get("/api/clusters")
    assert response.status_code == 200
    assert response.json() == []


def test_security_middleware_still_covers_static(client: TestClient) -> None:
    assert client.get("/", headers={"host": "evil.example"}).status_code == 400
    forbidden = client.post("/c/x", headers={"origin": "https://evil.example"})
    assert forbidden.status_code == 403
    assert forbidden.json()["code"] == "forbidden_origin"
