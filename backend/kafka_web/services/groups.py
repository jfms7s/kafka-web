"""Consumer group listing, lag, offset resets, creation and deletion (spec §6), over an admin.

Every admin future is resolved with an explicit timeout through the shared error mapping. Writes
(reset, create, delete) check the group's state first for a precise 409, but the broker has the
final say: a consumer can join between the check and the write, and the broker's rejection
(NON_EMPTY_GROUP, UNKNOWN_MEMBER_ID, ...) is surfaced as the same 409 `group_not_empty`.
"""

from collections.abc import Callable
from concurrent.futures import Future
from dataclasses import dataclass
from typing import Any, Literal

from confluent_kafka import (
    ConsumerGroupState,
    ConsumerGroupTopicPartitions,
    KafkaError,
    KafkaException,
    TopicPartition,
)
from confluent_kafka.admin import AdminClient, ConsumerGroupDescription, OffsetSpec

from kafka_web.errors import AppError, BrokerError, Conflict, NotFound, ValidationFailed
from kafka_web.kafka.errors import call_with_timeout, map_kafka_exception

DEFAULT_TIMEOUT_S = 10.0

ResetStrategy = Literal["earliest", "latest", "timestamp"]
StartPosition = Literal["earliest", "latest"]

# What a broker answers to an offset write or a delete aimed at a group that has live members.
# Depending on the group protocol and on how far the join had got it is any of these.
_ACTIVE_GROUP_CODES = {
    KafkaError.NON_EMPTY_GROUP,
    KafkaError.UNKNOWN_MEMBER_ID,
    KafkaError.ILLEGAL_GENERATION,
    KafkaError.REBALANCE_IN_PROGRESS,
    KafkaError.GROUP_SUBSCRIBED_TO_TOPIC,
    KafkaError.FENCED_MEMBER_EPOCH,
    KafkaError.STALE_MEMBER_EPOCH,
}
_NO_COMMITTED_OFFSET = -1001  # librdkafka OFFSET_INVALID


@dataclass(frozen=True)
class GroupSummary:
    group_id: str
    state: str
    type: str
    is_simple: bool


@dataclass(frozen=True)
class MemberView:
    member_id: str
    client_id: str
    host: str
    assignments: list[tuple[str, int]]


@dataclass(frozen=True)
class OffsetView:
    topic: str
    partition: int
    committed: int | None
    end: int | None
    lag: int | None


@dataclass(frozen=True)
class GroupDetailView:
    group_id: str
    state: str
    type: str
    members: list[MemberView]
    offsets: list[OffsetView]


def _enum_name(obj: Any, attribute: str) -> str:
    """Lowercase enum name of `obj.<attribute>` (unset when the broker omitted it)."""
    value = getattr(obj, attribute, None)
    return "unknown" if value is None else value.name.lower()


def _kafka_error(failure: BaseException | KafkaError) -> KafkaError | None:
    if isinstance(failure, KafkaError):
        return failure
    if isinstance(failure, KafkaException) and failure.args:
        first = failure.args[0]
        return first if isinstance(first, KafkaError) else None
    return None


def _as_app_error(failure: BaseException | KafkaError, *, write: bool = False) -> AppError:
    """Map a failure; for writes, "the group has live members" rejections become 409."""
    error = _kafka_error(failure)
    if write and error is not None and error.code() in _ACTIVE_GROUP_CODES:
        return Conflict(
            "The group has active members; stop its consumers first", code="group_not_empty"
        )
    if isinstance(failure, KafkaError):
        return map_kafka_exception(KafkaException(failure))
    return map_kafka_exception(failure)


def _wait[T](future: Future[T], timeout: float, *, write: bool = False) -> T:
    try:
        return future.result(timeout=timeout)
    except Exception as exc:
        raise _as_app_error(exc, write=write) from None


def _guard[T](fn: Callable[[], T]) -> T:
    return call_with_timeout(fn)


def _describe(admin: AdminClient, group_id: str, timeout: float) -> ConsumerGroupDescription:
    """The group's description; a group that does not exist (DEAD, GROUP_ID_NOT_FOUND) is a 404."""
    futures = _guard(lambda: admin.describe_consumer_groups([group_id], request_timeout=timeout))
    description = _wait(futures[group_id], timeout)
    if description.state == ConsumerGroupState.DEAD:
        raise NotFound(f"Consumer group {group_id!r} does not exist", code="group_not_found")
    return description


def _require_empty(description: ConsumerGroupDescription, group_id: str) -> None:
    if description.state != ConsumerGroupState.EMPTY:
        raise Conflict(
            f"Consumer group {group_id!r} is {_enum_name(description, 'state')}; "
            "stop its consumers first",
            code="group_not_empty",
        )


def list_groups(
    admin: AdminClient, filter: str | None = None, timeout: float = DEFAULT_TIMEOUT_S
) -> list[GroupSummary]:
    future = _guard(lambda: admin.list_consumer_groups(request_timeout=timeout))
    result = _wait(future, timeout)
    if result.errors:  # a partial list would silently hide groups
        raise _as_app_error(result.errors[0])
    needle = (filter or "").lower()
    groups = [
        GroupSummary(
            group_id=listing.group_id,
            state=_enum_name(listing, "state"),
            type=_enum_name(listing, "type"),
            is_simple=bool(listing.is_simple_consumer_group),
        )
        for listing in result.valid
        if needle in listing.group_id.lower()
    ]
    return sorted(groups, key=lambda g: g.group_id)


# --- offsets -----------------------------------------------------------------------------------


def _committed_offsets(
    admin: AdminClient, group_id: str, timeout: float
) -> dict[tuple[str, int], int]:
    request = ConsumerGroupTopicPartitions(group_id)
    futures = _guard(lambda: admin.list_consumer_group_offsets([request], request_timeout=timeout))
    result = _wait(futures[group_id], timeout)
    committed: dict[tuple[str, int], int] = {}
    for tp in result.topic_partitions or []:
        if tp.error is not None:
            raise _as_app_error(tp.error)
        committed[(tp.topic, tp.partition)] = tp.offset
    return committed


def _list_offsets(
    admin: AdminClient, specs: dict[tuple[str, int], OffsetSpec], timeout: float
) -> dict[tuple[str, int], int | AppError]:
    """Partition → resolved offset, or the (mapped) error for that partition."""
    if not specs:
        return {}
    request = {TopicPartition(t, p): spec for (t, p), spec in specs.items()}
    futures = _guard(lambda: admin.list_offsets(request, request_timeout=timeout))
    out: dict[tuple[str, int], int | AppError] = {}
    for tp, future in futures.items():
        try:
            out[(tp.topic, tp.partition)] = _wait(future, timeout).offset
        except AppError as exc:
            out[(tp.topic, tp.partition)] = exc
    return out


def _offset_views(
    admin: AdminClient, committed: dict[tuple[str, int], int], timeout: float
) -> list[OffsetView]:
    """Committed offsets with the partitions' end offsets and the resulting lag."""
    ends = _list_offsets(admin, {key: OffsetSpec.latest() for key in committed}, timeout)
    views = []
    for (topic, partition), offset in sorted(committed.items()):
        end = ends[(topic, partition)]
        if isinstance(end, AppError):
            if end.code != "topic_not_found":  # a deleted topic just leaves its lag unknown
                raise end
            end = None
        known = offset if offset >= 0 else None
        end = end if end is not None and end >= 0 else None
        lag = None if known is None or end is None else max(0, end - known)
        views.append(OffsetView(topic, partition, known, end, lag))
    return views


def describe_group(
    admin: AdminClient, group_id: str, timeout: float = DEFAULT_TIMEOUT_S
) -> GroupDetailView:
    description = _describe(admin, group_id, timeout)
    committed = _committed_offsets(admin, group_id, timeout)
    members = [
        MemberView(
            member_id=m.member_id,
            client_id=m.client_id,
            host=m.host,
            assignments=[(tp.topic, tp.partition) for tp in (m.assignment.topic_partitions or [])],
        )
        for m in description.members
    ]
    return GroupDetailView(
        group_id=group_id,
        state=_enum_name(description, "state"),
        type=_enum_name(description, "type"),
        members=members,
        offsets=_offset_views(admin, committed, timeout),
    )


# --- writes ------------------------------------------------------------------------------------


def _topic_partitions(admin: AdminClient, topic: str, timeout: float) -> list[int]:
    metadata = _guard(lambda: admin.list_topics(topic=topic, timeout=timeout))
    found = metadata.topics.get(topic)
    if found is not None and found.error is not None:
        raise map_kafka_exception(found.error)
    if found is None or not found.partitions:
        raise NotFound(f"Topic {topic!r} does not exist", code="topic_not_found")
    return sorted(found.partitions)


def _resolved(
    admin: AdminClient, topic: str, spec_by_partition: dict[int, OffsetSpec], timeout: float
) -> dict[int, int]:
    results = _list_offsets(
        admin, {(topic, p): spec for p, spec in spec_by_partition.items()}, timeout
    )
    offsets: dict[int, int] = {}
    for (_, partition), offset in results.items():
        if isinstance(offset, AppError):
            raise offset
        offsets[partition] = offset
    return offsets


def resolve_targets(
    admin: AdminClient,
    topic: str,
    strategy: ResetStrategy,
    timestamp: int | None,
    timeout: float = DEFAULT_TIMEOUT_S,
) -> list[TopicPartition]:
    """Every partition of `topic` with the offset `strategy` selects."""
    if strategy == "timestamp" and (timestamp is None or timestamp < 0):
        raise ValidationFailed(
            "timestamp: a non-negative epoch-milliseconds value is required", field="timestamp"
        )
    partitions = _topic_partitions(admin, topic, timeout)
    match strategy:
        case "earliest":
            offsets = _resolved(
                admin, topic, {p: OffsetSpec.earliest() for p in partitions}, timeout
            )
        case "latest":
            offsets = _resolved(admin, topic, {p: OffsetSpec.latest() for p in partitions}, timeout)
        case "timestamp":
            assert timestamp is not None
            offsets = _resolved(
                admin, topic, {p: OffsetSpec.for_timestamp(timestamp) for p in partitions}, timeout
            )
            past_the_end = [p for p, offset in offsets.items() if offset < 0]
            offsets.update(
                _resolved(admin, topic, {p: OffsetSpec.latest() for p in past_the_end}, timeout)
            )
    unresolved = [p for p in partitions if offsets.get(p, -1) < 0]
    if unresolved:
        raise BrokerError(f"Could not resolve the {strategy} offset of partitions {unresolved}")
    return [TopicPartition(topic, p, offsets[p]) for p in partitions]


def _alter(
    admin: AdminClient, group_id: str, targets: list[TopicPartition], timeout: float
) -> None:
    request = ConsumerGroupTopicPartitions(group_id, targets)
    try:
        futures = admin.alter_consumer_group_offsets([request], request_timeout=timeout)
    except Exception as exc:
        raise _as_app_error(exc, write=True) from None
    result = _wait(futures[group_id], timeout, write=True)
    for tp in result.topic_partitions or []:
        if tp.error is not None:
            raise _as_app_error(tp.error, write=True)


def reset_offsets(
    admin: AdminClient,
    group_id: str,
    topic: str,
    strategy: ResetStrategy,
    timestamp: int | None,
    timeout: float = DEFAULT_TIMEOUT_S,
) -> list[OffsetView]:
    _require_empty(_describe(admin, group_id, timeout), group_id)
    targets = resolve_targets(admin, topic, strategy, timestamp, timeout)
    _alter(admin, group_id, targets, timeout)
    committed = {(tp.topic, tp.partition): tp.offset for tp in targets}
    return _offset_views(admin, committed, timeout)


def _exists(admin: AdminClient, group_id: str, timeout: float) -> bool:
    try:
        _describe(admin, group_id, timeout)
        return True
    except NotFound as exc:
        if exc.code != "group_not_found":
            raise
    return bool(_committed_offsets(admin, group_id, timeout))


def create_group(
    admin: AdminClient,
    group_id: str,
    topic: str,
    start: StartPosition,
    timeout: float = DEFAULT_TIMEOUT_S,
) -> None:
    """Create a group by committing its initial offsets (there is no create-group request)."""
    if _exists(admin, group_id, timeout):
        raise Conflict(f"Consumer group {group_id!r} already exists", code="group_exists")
    targets = resolve_targets(admin, topic, start, None, timeout)
    _alter(admin, group_id, targets, timeout)


def delete_group(admin: AdminClient, group_id: str, timeout: float = DEFAULT_TIMEOUT_S) -> None:
    _require_empty(_describe(admin, group_id, timeout), group_id)
    try:
        futures = admin.delete_consumer_groups([group_id], request_timeout=timeout)
    except Exception as exc:
        raise _as_app_error(exc, write=True) from None
    _wait(futures[group_id], timeout, write=True)
