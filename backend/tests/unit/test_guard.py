from pathlib import Path
from typing import Annotated

import pytest
from fastapi import Depends
from fastapi.testclient import TestClient

from kafka_web.api.app import create_app
from kafka_web.api.deps import require_confirm, writable_cluster
from kafka_web.config.models import ClusterConfig, ClusterInput
from kafka_web.config.secrets import SecretStore
from kafka_web.config.store import ClusterStore
from kafka_web.errors import Forbidden, ValidationFailed
from kafka_web.kafka.registry import ConnectionRegistry
from tests.conftest import MemoryKeyring


@pytest.fixture
def store(tmp_path: Path, memory_keyring: MemoryKeyring) -> ClusterStore:
    store = ClusterStore(tmp_path / "cfg", SecretStore(memory_keyring))
    store.create(ClusterInput(name="prd", env="prd", bootstrap_servers="b:9092", read_only=True))
    store.create(ClusterInput(name="dev", env="dev", bootstrap_servers="b:9092"))
    return store


@pytest.fixture
def client(store: ClusterStore):
    app = create_app(store=store, registry=ConnectionRegistry(store))

    @app.post("/api/clusters/{name}/write-probe")
    def write_probe(cfg: Annotated[ClusterConfig, Depends(writable_cluster)]) -> dict[str, str]:
        return {"cluster": cfg.name}

    with TestClient(app) as client:
        yield client


def test_writable_cluster_rejects_read_only(store: ClusterStore):
    with pytest.raises(Forbidden) as info:
        writable_cluster("prd", store)
    assert info.value.code == "read_only_cluster"
    assert info.value.status == 403


def test_writable_cluster_returns_config(store: ClusterStore):
    assert writable_cluster("dev", store).name == "dev"


def test_writable_cluster_over_http(client: TestClient):
    response = client.post("/api/clusters/prd/write-probe")
    assert response.status_code == 403
    assert response.json()["code"] == "read_only_cluster"
    assert client.post("/api/clusters/dev/write-probe").json() == {"cluster": "dev"}
    assert client.post("/api/clusters/nope/write-probe").json()["code"] == "cluster_not_found"


def test_require_confirm_matches():
    require_confirm("g1", "g1")


@pytest.mark.parametrize("given", ["g2", None, "", " g1", "G1"])
def test_require_confirm_mismatch(given: str | None):
    with pytest.raises(ValidationFailed) as info:
        require_confirm("g1", given)
    assert info.value.code == "confirmation_mismatch"
    assert info.value.field == "confirm"
    assert info.value.status == 422
