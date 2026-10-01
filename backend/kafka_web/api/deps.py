"""Request-scoped dependencies shared by every router: app singletons and the write guard."""

from typing import Annotated

from fastapi import Depends, Request

from kafka_web.config.models import ClusterConfig
from kafka_web.config.store import ClusterStore
from kafka_web.errors import Conflict, Forbidden, ValidationFailed
from kafka_web.kafka.registry import ClusterConnection, ConnectionRegistry


def get_store(request: Request) -> ClusterStore:
    return request.app.state.store


def get_registry(request: Request) -> ConnectionRegistry:
    return request.app.state.registry


StoreDep = Annotated[ClusterStore, Depends(get_store)]
RegistryDep = Annotated[ConnectionRegistry, Depends(get_registry)]


def writable_cluster(name: str, store: StoreDep) -> ClusterConfig:
    """The cluster named in the path, provided it accepts write actions (spec §1.5)."""
    cfg = store.get(name)
    if cfg.read_only:
        raise Forbidden(f"Cluster {name!r} is read-only", code="read_only_cluster")
    return cfg


def require_confirm(expected: str, given: str | None) -> None:
    """Typed confirmation: `given` must equal the target name exactly."""
    if given != expected:
        raise ValidationFailed(
            f"confirm: type {expected!r} to confirm this action",
            code="confirmation_mismatch",
            field="confirm",
        )


def open_connection(registry: ConnectionRegistry, name: str) -> ClusterConnection:
    """The cluster's connection for a request. One that was closed by a concurrent edit, delete or
    disconnect after we got it is fetched again (reconnecting); closed again → 409."""
    connection = registry.get(name)
    if connection.closed:
        connection = registry.get(name)
        if connection.closed:
            raise Conflict(
                f"Cluster {name!r} was changed during the request; try again",
                code="cluster_changed",
            )
    return connection
