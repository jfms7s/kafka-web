"""Consumer group endpoints (spec §6)."""

from dataclasses import asdict
from typing import Annotated, Literal

from fastapi import APIRouter, Depends, Query, Response
from pydantic import BaseModel, ConfigDict, Field

from kafka_web.api.deps import RegistryDep, open_connection, require_confirm, writable_cluster
from kafka_web.config.models import ClusterConfig
from kafka_web.services import groups as group_service

router = APIRouter()

WritableCluster = Annotated[ClusterConfig, Depends(writable_cluster)]


class GroupSummaryView(BaseModel):
    group_id: str
    state: str
    type: str
    is_simple: bool


class GroupListView(BaseModel):
    groups: list[GroupSummaryView]


class AssignmentItem(BaseModel):
    topic: str
    partition: int


class MemberItem(BaseModel):
    member_id: str
    client_id: str
    host: str
    assignments: list[AssignmentItem]


class OffsetItem(BaseModel):
    topic: str
    partition: int
    committed: int | None
    end: int | None
    lag: int | None


class GroupDetailView(BaseModel):
    group_id: str
    state: str
    type: str
    members: list[MemberItem]
    offsets: list[OffsetItem]


class OffsetListView(BaseModel):
    offsets: list[OffsetItem]


class CreateGroupRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    group_id: str = Field(min_length=1, max_length=255)
    topic: str = Field(min_length=1)
    start: Literal["earliest", "latest"]


class CreatedGroupView(BaseModel):
    group_id: str


class ResetRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    topic: str = Field(min_length=1)
    strategy: Literal["earliest", "latest", "timestamp"]
    timestamp: int | None = Field(default=None, ge=0, description="Epoch milliseconds")
    confirm: str | None = Field(default=None, description="Must equal the group id")


@router.get("/clusters/{name}/groups")
def list_groups(
    name: str, registry: RegistryDep, filter: Annotated[str | None, Query()] = None
) -> GroupListView:
    admin = open_connection(registry, name).admin
    return GroupListView.model_validate(
        {"groups": [asdict(g) for g in group_service.list_groups(admin, filter)]}
    )


@router.get("/clusters/{name}/groups/{group}")
def describe_group(name: str, group: str, registry: RegistryDep) -> GroupDetailView:
    admin = open_connection(registry, name).admin
    detail = asdict(group_service.describe_group(admin, group))
    for member in detail["members"]:  # (topic, partition) pairs → objects, friendlier for clients
        member["assignments"] = [{"topic": t, "partition": p} for t, p in member["assignments"]]
    return GroupDetailView.model_validate(detail)


@router.post("/clusters/{name}/groups", status_code=201)
def create_group(
    name: str, body: CreateGroupRequest, registry: RegistryDep, _cluster: WritableCluster
) -> CreatedGroupView:
    admin = open_connection(registry, name).admin
    group_service.create_group(admin, body.group_id, body.topic, body.start)
    return CreatedGroupView(group_id=body.group_id)


@router.post("/clusters/{name}/groups/{group}/reset")
def reset_offsets(
    name: str, group: str, body: ResetRequest, registry: RegistryDep, _cluster: WritableCluster
) -> OffsetListView:
    require_confirm(group, body.confirm)
    admin = open_connection(registry, name).admin
    offsets = group_service.reset_offsets(admin, group, body.topic, body.strategy, body.timestamp)
    return OffsetListView.model_validate({"offsets": [asdict(o) for o in offsets]})


@router.delete("/clusters/{name}/groups/{group}", status_code=204)
def delete_group(
    name: str,
    group: str,
    registry: RegistryDep,
    _cluster: WritableCluster,
    confirm: Annotated[str | None, Query()] = None,
) -> Response:
    require_confirm(group, confirm)
    admin = open_connection(registry, name).admin
    group_service.delete_group(admin, group)
    return Response(status_code=204)
