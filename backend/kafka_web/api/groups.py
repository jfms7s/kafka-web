"""Consumer group endpoints (spec §6)."""

from dataclasses import asdict
from typing import Annotated, Literal, Self

from fastapi import APIRouter, Depends, Query, Response
from pydantic import BaseModel, ConfigDict, Field, model_validator

from kafka_web.api.deps import RegistryDep, open_connection, require_confirm, writable_cluster
from kafka_web.config.models import ClusterConfig
from kafka_web.errors import ValidationFailed
from kafka_web.services import groups as group_service

router = APIRouter()

INT32_MAX = 2**31 - 1
INT64_MAX = 2**63 - 1

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


class PartitionTarget(BaseModel):
    model_config = ConfigDict(extra="forbid")

    partition: int = Field(ge=0, le=INT32_MAX)
    offset: int | None = Field(
        default=None, ge=0, le=INT64_MAX, description="Required for to_offset, else omitted"
    )


class ResetRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    topic: str = Field(min_length=1)
    strategy: Literal["earliest", "latest", "timestamp", "to_offset", "shift_by"]
    timestamp: int | None = Field(default=None, ge=0, description="Epoch milliseconds")
    partitions: list[PartitionTarget] | None = Field(
        default=None,
        min_length=1,
        description="Partitions to reset (omitted: every partition); to_offset needs it",
    )
    shift: int | None = Field(
        default=None, ge=-INT64_MAX, le=INT64_MAX, description="shift_by only; negative rewinds"
    )
    confirm: str | None = Field(default=None, description="Must equal the group id")

    @model_validator(mode="after")
    def _fields_follow_strategy(self) -> Self:
        entries = self.partitions or []
        ids = [entry.partition for entry in entries]
        if len(set(ids)) != len(ids):
            raise ValidationFailed("partitions: a partition is listed twice", field="partitions")
        if self.strategy == "to_offset":
            if not entries or any(entry.offset is None for entry in entries):
                raise ValidationFailed(
                    "partitions: to_offset needs an offset for each partition", field="partitions"
                )
        elif any(entry.offset is not None for entry in entries):
            raise ValidationFailed(
                "partitions: an offset is only allowed with to_offset", field="partitions"
            )
        if (self.strategy == "shift_by") != (self.shift is not None):
            raise ValidationFailed(
                "shift: required for shift_by and only allowed there", field="shift"
            )
        return self

    def service_args(self) -> dict:
        """Keyword arguments for `reset_offsets` / `preview_reset`."""
        entries = self.partitions
        return {
            "partitions": None if entries is None else [e.partition for e in entries],
            "offsets": {e.partition: e.offset for e in entries or [] if e.offset is not None}
            or None,
            "shift": self.shift,
        }


class ResetTargetItem(BaseModel):
    topic: str
    partition: int
    current: int | None
    target: int
    clamped: bool


class ResetPreviewView(BaseModel):
    targets: list[ResetTargetItem]


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


@router.post("/clusters/{name}/groups/{group}/reset/preview")
def preview_reset(
    name: str, group: str, body: ResetRequest, registry: RegistryDep, _cluster: WritableCluster
) -> ResetPreviewView:
    """The targets a reset with this body would commit; commits nothing, ignores `confirm`."""
    admin = open_connection(registry, name).admin
    targets = group_service.preview_reset(
        admin, group, body.topic, body.strategy, body.timestamp, **body.service_args()
    )
    return ResetPreviewView.model_validate({"targets": [asdict(t) for t in targets]})


@router.post("/clusters/{name}/groups/{group}/reset")
def reset_offsets(
    name: str, group: str, body: ResetRequest, registry: RegistryDep, _cluster: WritableCluster
) -> OffsetListView:
    require_confirm(group, body.confirm)
    admin = open_connection(registry, name).admin
    offsets = group_service.reset_offsets(
        admin, group, body.topic, body.strategy, body.timestamp, **body.service_args()
    )
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
