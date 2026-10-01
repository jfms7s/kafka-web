"""Message endpoints (spec §3): bounded snapshot reads of a topic."""

from typing import Annotated

from fastapi import APIRouter, Query
from pydantic import BaseModel

from kafka_web.api.deps import RegistryDep
from kafka_web.services.consume import SnapshotParams, consume_snapshot
from kafka_web.services.decode import MessageView

router = APIRouter()


class MessageListView(BaseModel):
    messages: list[MessageView]


@router.get("/clusters/{name}/topics/{topic}/messages")
def snapshot_messages(
    name: str, topic: str, params: Annotated[SnapshotParams, Query()], registry: RegistryDep
) -> MessageListView:
    # A per-request consumer from the saved config: the shared admin/producer are not involved.
    client_config = registry.client_config(name)
    return MessageListView(messages=consume_snapshot(client_config, topic, params))
