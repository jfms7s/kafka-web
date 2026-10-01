"""Snapshot consumption (spec §3): read a bounded batch of messages from a topic and return it.

Each call owns a short-lived consumer that uses `assign()` only: it never subscribes, never
commits and never joins a group, so browsing leaves no consumer group behind.
"""

import contextlib
import logging
import time
import uuid
from collections.abc import Callable
from typing import Any

from confluent_kafka import Consumer, KafkaError, TopicPartition
from pydantic import BaseModel, Field

from kafka_web.errors import KafkaTimeout, NotFound, ValidationFailed
from kafka_web.kafka.errors import call_with_timeout, map_kafka_exception
from kafka_web.services.decode import MessageView, to_message_view
from kafka_web.services.offsets import StartMode, end_offsets, is_done, plan_start_offsets

CALL_TIMEOUT_S = 10.0  # one metadata, watermark or offsets-for-times call
# All setup calls together (metadata + watermarks + offsets-for-times) share this budget; the
# collection window (`params.timeout`) starts afterwards, so worst case is about 10 s + timeout.
SETUP_BUDGET_S = 10.0
POLL_SLICE_S = 0.5  # longest single poll: keeps the deadline check responsive

logger = logging.getLogger(__name__)


class SnapshotParams(BaseModel):
    count: int = Field(100, ge=1, le=10_000)
    timeout: float = Field(10, ge=1, le=60)
    start: StartMode = "latest"
    offset: int | None = None  # start == "offset": required, and so is `partition`
    # start == "timestamp": required (epoch ms). Negative values are ListOffsets sentinels
    # (-1 latest, -2 earliest, -3 max timestamp), not times.
    timestamp: int | None = Field(None, ge=0)
    partition: int | None = None


def _validate(params: SnapshotParams) -> None:
    if params.start == "offset":
        if params.partition is None:
            raise ValidationFailed("start=offset requires a partition", field="partition")
        if params.offset is None:
            raise ValidationFailed("start=offset requires an offset", field="offset")
    if params.start == "timestamp" and params.timestamp is None:
        raise ValidationFailed("start=timestamp requires a timestamp", field="timestamp")


class _Budget:
    """A deadline that several blocking calls draw from."""

    def __init__(self, duration: float, clock: Callable[[], float]):
        self._clock = clock
        self.deadline = clock() + duration

    def remaining(self) -> float:
        return self.deadline - self._clock()

    def call_timeout(self) -> float:
        """Timeout for one blocking call: its own cap, or what is left if that is less."""
        remaining = self.remaining()
        if remaining <= 0:
            raise KafkaTimeout("Kafka did not answer the snapshot setup calls in time")
        return min(CALL_TIMEOUT_S, remaining)


def _consumer_config(client_config: dict[str, str]) -> dict[str, Any]:
    return {
        **client_config,
        "group.id": f"kafka-web-{uuid.uuid4()}",
        "enable.auto.commit": "false",
        "enable.partition.eof": "true",
        "auto.offset.reset": "earliest",
        "allow.auto.create.topics": "false",
    }


def _partition_ids(
    consumer: Consumer, topic: str, wanted: int | None, budget: _Budget
) -> list[int]:
    wait = budget.call_timeout()
    metadata = call_with_timeout(lambda: consumer.list_topics(topic, timeout=wait))
    found = metadata.topics.get(topic)
    if found is not None and found.error is not None:
        raise map_kafka_exception(found.error)
    if found is None or not found.partitions:
        raise NotFound(f"Topic {topic!r} does not exist", code="topic_not_found")
    if wanted is None:
        return sorted(found.partitions)
    if wanted not in found.partitions:
        raise ValidationFailed(f"Topic {topic!r} has no partition {wanted}", field="partition")
    return [wanted]


def _watermarks(
    consumer: Consumer, topic: str, partitions: list[int], budget: _Budget
) -> dict[int, tuple[int, int]]:
    marks = {}
    for p in partitions:
        wait = budget.call_timeout()
        marks[p] = call_with_timeout(
            lambda p=p, wait=wait: consumer.get_watermark_offsets(
                TopicPartition(topic, p), timeout=wait
            )
        )
    return marks


def _timestamp_offsets(
    consumer: Consumer, topic: str, partitions: list[int], timestamp: int, budget: _Budget
) -> dict[int, int]:
    """Earliest offset at/after `timestamp` per partition (-1: none).

    A partition the broker could not answer for fails the request: skipping it would silently
    return partial results.
    """
    asked = [TopicPartition(topic, p, timestamp) for p in partitions]
    wait = budget.call_timeout()
    answered = call_with_timeout(lambda: consumer.offsets_for_times(asked, timeout=wait))
    for tp in answered:
        if tp.error is not None:
            raise map_kafka_exception(tp.error)
    return {tp.partition: tp.offset for tp in answered}


def _poll_until_done(
    consumer: Consumer,
    start_offsets: dict[int, int],
    ends: dict[int, int],
    *,
    limit: int | None,
    budget: _Budget,
) -> list[MessageView]:
    """Poll until `limit` messages, the deadline, or every planned partition reached its end."""
    positions = dict(start_offsets)  # partition -> next offset to be read
    collected: list[MessageView] = []
    while (limit is None or len(collected) < limit) and not is_done(positions, ends):
        remaining = budget.remaining()
        if remaining <= 0:
            break
        wait = min(POLL_SLICE_S, remaining)
        msg = call_with_timeout(lambda wait=wait: consumer.poll(wait))
        if msg is None:
            continue
        partition = msg.partition()
        err = msg.error()
        if err is not None:
            if err.code() != KafkaError._PARTITION_EOF:
                raise map_kafka_exception(err)
            # EOF: the partition's log is fully read. Offsets that are never delivered as
            # messages (transaction markers, compaction gaps) would otherwise stall us.
            if partition in ends:
                positions[partition] = max(positions[partition], ends[partition])
            continue
        offset = msg.offset()
        if partition not in ends:
            continue
        positions[partition] = max(positions[partition], offset + 1)
        if offset < ends[partition]:
            collected.append(to_message_view(msg))
    return collected


def consume_snapshot(
    client_config: dict[str, str],
    topic: str,
    params: SnapshotParams,
    consumer_factory: Callable[[dict[str, Any]], Consumer] = Consumer,
    clock: Callable[[], float] = time.monotonic,
) -> list[MessageView]:
    _validate(params)
    consumer = call_with_timeout(lambda: consumer_factory(_consumer_config(client_config)))
    try:
        return _snapshot(consumer, topic, params, clock)
    finally:
        with contextlib.suppress(Exception):  # a failing close must not mask the real outcome
            consumer.close()


def _snapshot(
    consumer: Consumer, topic: str, params: SnapshotParams, clock: Callable[[], float]
) -> list[MessageView]:
    setup = _Budget(SETUP_BUDGET_S, clock)
    partitions = _partition_ids(consumer, topic, params.partition, setup)
    watermarks = _watermarks(consumer, topic, partitions, setup)
    lookup = None
    if params.start == "timestamp":
        assert params.timestamp is not None  # _validate
        lookup = _timestamp_offsets(consumer, topic, partitions, params.timestamp, setup)
    plan = plan_start_offsets(
        params.start,
        watermarks,
        params.count,
        offset=params.offset,
        timestamp_offsets=lookup,
    )
    if not plan:
        return []

    ends = {p: end for p, end in end_offsets(watermarks).items() if p in plan}
    call_with_timeout(
        lambda: consumer.assign([TopicPartition(topic, p, start) for p, start in plan.items()])
    )
    # `latest` reads every planned range completely (about `count` messages in all) and trims
    # afterwards: stopping at the first `count` arrivals could drop the newest of another partition.
    limit = None if params.start == "latest" else params.count
    messages = _poll_until_done(
        consumer, plan, ends, limit=limit, budget=_Budget(params.timeout, clock)
    )
    messages.sort(key=lambda m: (m.timestamp or 0, m.partition, m.offset))
    return messages[-params.count :] if params.start == "latest" else messages
