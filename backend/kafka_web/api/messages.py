"""Message endpoints (spec §3, §5): bounded snapshot reads and publishing (single and bulk)."""

from typing import Annotated

from fastapi import APIRouter, Depends, Query, Request
from pydantic import BaseModel, ConfigDict, Field
from starlette.concurrency import run_in_threadpool

from kafka_web.api.deps import RegistryDep, require_confirm, writable_cluster
from kafka_web.api.uploads import read_upload
from kafka_web.config.models import ClusterConfig
from kafka_web.errors import Conflict, ValidationFailed
from kafka_web.kafka.registry import ClusterConnection, ConnectionRegistry
from kafka_web.services import publish as publishing
from kafka_web.services.consume import SnapshotParams, consume_snapshot
from kafka_web.services.decode import MessageView

router = APIRouter()

MAX_UPLOAD_BYTES = 10 * 1024 * 1024  # spec §5: bulk files are at most 10 MB
FLUSH_TIMEOUT_S = publishing.DEFAULT_FLUSH_TIMEOUT_S  # spec §5: flush(timeout=30)

WritableCluster = Annotated[ClusterConfig, Depends(writable_cluster)]


class MessageListView(BaseModel):
    messages: list[MessageView]


@router.get("/clusters/{name}/topics/{topic}/messages")
def snapshot_messages(
    name: str, topic: str, params: Annotated[SnapshotParams, Query()], registry: RegistryDep
) -> MessageListView:
    # A per-request consumer from the saved config: the shared admin/producer are not involved.
    client_config = registry.client_config(name)
    return MessageListView(messages=consume_snapshot(client_config, topic, params))


# --- publishing ------------------------------------------------------------------------------


class PublishRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    key: str | None = None
    value: str
    headers: str | None = Field(default=None, description="A JSON object or key=value lines")
    partition: int | None = Field(default=None, ge=0)


class PublishedView(BaseModel):
    ok: bool
    partition: int
    offset: int


class RowResultView(BaseModel):
    row: int
    ok: bool
    partition: int | None = None
    offset: int | None = None
    error: str | None = None


class BatchResultView(BaseModel):
    succeeded: int
    failed: int
    results: list[RowResultView]


def _open_connection(registry: ConnectionRegistry, name: str) -> ClusterConnection:
    """The cluster's connection for a write. One that was closed by a concurrent edit, delete or
    disconnect after we got it is fetched again (reconnecting); closed again → 409."""
    connection = registry.get(name)
    if connection.closed:
        connection = registry.get(name)
        if connection.closed:
            raise Conflict(
                f"Cluster {name!r} was changed while publishing; try again", code="cluster_changed"
            )
    return connection


@router.post("/clusters/{name}/topics/{topic}/messages")
def publish_message(
    name: str,
    topic: str,
    body: PublishRequest,
    registry: RegistryDep,
    _cluster: WritableCluster,
) -> PublishedView:
    headers = publishing.parse_headers(body.headers)  # before connecting: cheap 422s first
    connection = _open_connection(registry, name)
    partitions = publishing.require_topic(connection.admin, topic)
    if body.partition is not None and body.partition >= partitions:
        raise ValidationFailed(
            f"partition: topic {topic!r} has {partitions} partitions", field="partition"
        )
    message = publishing.OutMessage(
        key=None if body.key is None else body.key.encode(),
        value=body.value.encode(),
        headers=headers,
        partition=body.partition,
    )
    partition, offset = publishing.publish_one(connection.producer, topic, message, FLUSH_TIMEOUT_S)
    return PublishedView(ok=True, partition=partition, offset=offset)


_BULK_BODY = {
    "required": True,
    "content": {
        "multipart/form-data": {
            "schema": {
                "type": "object",
                "required": ["file", "confirm"],
                "properties": {
                    "file": {"type": "string", "format": "binary"},
                    "confirm": {"type": "string"},
                    "key_column": {"type": "string"},
                    "value_column": {"type": "string"},
                },
            }
        }
    },
}


def _content_length(request: Request) -> int | None:
    try:
        return int(request.headers["content-length"])
    except (KeyError, ValueError):
        return None


def _publish_bulk(
    registry: ConnectionRegistry,
    name: str,
    topic: str,
    content: bytes,
    key_column: str | None,
    value_column: str | None,
) -> BatchResultView:
    items = publishing.parse_bulk(content, key_column, value_column)
    connection = _open_connection(registry, name)
    publishing.require_topic(connection.admin, topic)
    result = publishing.publish(connection.producer, topic, items, FLUSH_TIMEOUT_S)
    return BatchResultView.model_validate(result, from_attributes=True)


# The body is read by hand (an `async` route over `request.stream()`): FastAPI would parse and
# spool the whole upload before running the write guard, and before any size check.
@router.post(
    "/clusters/{name}/topics/{topic}/messages/bulk",
    openapi_extra={"requestBody": _BULK_BODY},
)
async def publish_bulk(
    name: str,
    topic: str,
    request: Request,
    registry: RegistryDep,
    _cluster: WritableCluster,
) -> BatchResultView:
    def check_confirm_if_given(fields: dict[str, str]) -> None:
        if "confirm" in fields:  # sent ahead of the file: refuse before reading the file
            require_confirm(topic, fields["confirm"])

    upload = await read_upload(
        request.stream(),
        request.headers.get("content-type", ""),
        max_file_bytes=MAX_UPLOAD_BYTES,
        on_file_start=check_confirm_if_given,
        content_length=_content_length(request),
    )
    require_confirm(topic, upload.fields.get("confirm"))
    if upload.file is None or not upload.file.strip():
        raise ValidationFailed("file: choose a non-empty CSV or JSON file", field="file")
    return await run_in_threadpool(
        _publish_bulk,
        registry,
        name,
        topic,
        upload.file,
        upload.fields.get("key_column") or None,
        upload.fields.get("value_column") or None,
    )
