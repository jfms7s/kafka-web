"""Cluster management endpoints (spec §1.6)."""

from fastapi import APIRouter, Response, status

from kafka_web.api.deps import RegistryDep, StoreDep
from kafka_web.api.schemas import (
    CertView,
    ClusterView,
    ConnectionTestResult,
    ConnectionView,
    StatusView,
)
from kafka_web.config.models import ClusterBase, ClusterConfig, ClusterInput
from kafka_web.config.secrets import KeyringUnavailable
from kafka_web.config.store import ClusterStore
from kafka_web.kafka.registry import ConnectionRegistry

router = APIRouter()

_BASE_FIELDS = set(ClusterBase.model_fields)


def _has_sasl_password(store: ClusterStore, cfg: ClusterConfig) -> bool:
    if not cfg.uses_sasl:
        return False
    try:
        return store.sasl_password(cfg) is not None
    except KeyringUnavailable:
        return False


def _view(cfg: ClusterConfig, store: ClusterStore, registry: ConnectionRegistry) -> ClusterView:
    usability = store.usability(cfg)
    certs = store.truststore_summary(cfg) if cfg.uses_tls else None
    return ClusterView(
        **cfg.model_dump(include=_BASE_FIELDS),
        has_sasl_password=_has_sasl_password(store, cfg),
        truststore=[CertView(subject=c.subject, not_after=c.not_after) for c in certs]
        if certs
        else None,
        usable=usability.usable,
        unusable_reason=usability.reason,
        connected=registry.is_connected(cfg.name),
    )


@router.get("/clusters")
def list_clusters(store: StoreDep, registry: RegistryDep) -> list[ClusterView]:
    return [_view(cfg, store, registry) for cfg in store.list()]


@router.post("/clusters", status_code=status.HTTP_201_CREATED)
def create_cluster(inp: ClusterInput, store: StoreDep, registry: RegistryDep) -> ClusterView:
    return _view(store.create(inp), store, registry)


@router.post("/clusters/test")
def test_connection(
    inp: ClusterInput, store: StoreDep, registry: RegistryDep, existing: str | None = None
) -> ConnectionTestResult:
    """Connectivity check from unsaved form values; `existing` supplies blank secrets (edit)."""
    registry.test(inp, store.get(existing) if existing else None)
    return ConnectionTestResult(ok=True)


@router.get("/clusters/{name}")
def get_cluster(name: str, store: StoreDep, registry: RegistryDep) -> ClusterView:
    return _view(store.get(name), store, registry)


@router.put("/clusters/{name}")
def update_cluster(
    name: str, inp: ClusterInput, store: StoreDep, registry: RegistryDep
) -> ClusterView:
    store.materialize(inp, existing=store.get(name))  # reject invalid edits before disturbing
    registry.disconnect(name)  # closes the connection and stops its live streams
    cfg = store.update(name, inp)
    registry.disconnect(name)  # a connect racing the update may have cached the old settings
    return _view(cfg, store, registry)


@router.delete("/clusters/{name}", status_code=status.HTTP_204_NO_CONTENT)
def delete_cluster(name: str, store: StoreDep, registry: RegistryDep) -> Response:
    registry.disconnect(name)
    store.delete(name)
    registry.disconnect(name)  # see update_cluster
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.post("/clusters/{name}/connect")
def connect_cluster(name: str, store: StoreDep, registry: RegistryDep) -> ClusterView:
    registry.connect(name)
    return _view(store.get(name), store, registry)


@router.post("/clusters/{name}/disconnect", status_code=status.HTTP_204_NO_CONTENT)
def disconnect_cluster(name: str, registry: RegistryDep) -> Response:
    registry.disconnect(name)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.get("/status")
def get_status(store: StoreDep, registry: RegistryDep) -> StatusView:
    connections = registry.active()
    configs = {cfg.name: cfg for cfg in store.list()} if connections else {}
    views = [
        ConnectionView(
            name=conn.name,
            env=cfg.env,
            region=cfg.region,
            bootstrap_servers=cfg.bootstrap_servers,
            read_only=cfg.read_only,
            connected_at=conn.connected_at,
            active_streams=len(conn.streams),
        )
        for conn in connections
        if (cfg := configs.get(conn.name)) is not None
    ]
    return StatusView(connections=sorted(views, key=lambda v: v.name))
