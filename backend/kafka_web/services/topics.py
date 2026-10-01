"""Topic listing and topic configuration (spec §2), over a confluent-kafka admin client."""

from dataclasses import dataclass
from typing import Any

from confluent_kafka.admin import AdminClient, ConfigEntry, ConfigResource, ResourceType

from kafka_web.errors import NotFound
from kafka_web.kafka.errors import call_with_timeout, map_kafka_exception
from kafka_web.services.humanize import humanize_config_value

DEFAULT_TIMEOUT_S = 10.0


@dataclass(frozen=True)
class TopicSummary:
    name: str
    partitions: int
    replication_factor: int
    internal: bool


@dataclass(frozen=True)
class PartitionDetail:
    id: int
    leader: int
    replicas: list[int]
    isr: list[int]


@dataclass(frozen=True)
class ConfigEntryView:
    name: str
    value: str | None
    display_value: str | None
    is_default: bool
    source: str
    sensitive: bool


@dataclass(frozen=True)
class TopicConfigView:
    name: str
    partitions: list[PartitionDetail]
    replication_factor: int
    entries: list[ConfigEntryView]


def _replication_factor(partitions: dict[int, Any]) -> int:
    """Replica count of the lowest-numbered partition (0 for a topic without partitions)."""
    if not partitions:
        return 0
    return len(partitions[min(partitions)].replicas)


def list_topics(admin: AdminClient, timeout: float = DEFAULT_TIMEOUT_S) -> list[TopicSummary]:
    metadata = call_with_timeout(lambda: admin.list_topics(timeout=timeout))
    return [
        TopicSummary(
            name=name,
            partitions=len(topic.partitions),
            replication_factor=_replication_factor(topic.partitions),
            internal=name.startswith("__"),
        )
        for name, topic in sorted(metadata.topics.items())
    ]


def _partition_details(admin: AdminClient, topic: str, timeout: float) -> list[PartitionDetail]:
    metadata = call_with_timeout(lambda: admin.list_topics(topic=topic, timeout=timeout))
    found = metadata.topics.get(topic)
    if found is not None and found.error is not None:
        raise map_kafka_exception(found.error)
    if found is None or not found.partitions:
        raise NotFound(f"Topic {topic!r} does not exist", code="topic_not_found")
    return [
        PartitionDetail(id=p.id, leader=p.leader, replicas=list(p.replicas), isr=list(p.isrs))
        for _, p in sorted(found.partitions.items())
    ]


def _entry_view(entry: ConfigEntry) -> ConfigEntryView:
    value = None if entry.is_sensitive else entry.value
    return ConfigEntryView(
        name=entry.name,
        value=value,
        display_value=humanize_config_value(entry.name, value),
        is_default=bool(entry.is_default),
        source=entry.source.name,
        sensitive=bool(entry.is_sensitive),
    )


def _config_entries(admin: AdminClient, topic: str, timeout: float) -> list[ConfigEntryView]:
    resource = ConfigResource(ResourceType.TOPIC, topic)

    def fetch() -> dict[str, ConfigEntry]:
        return admin.describe_configs([resource])[resource].result(timeout=timeout)

    entries = [_entry_view(entry) for entry in call_with_timeout(fetch).values()]
    return sorted(entries, key=lambda e: (e.is_default, e.name))


def describe_topic(
    admin: AdminClient, topic: str, timeout: float = DEFAULT_TIMEOUT_S
) -> TopicConfigView:
    partitions = _partition_details(admin, topic, timeout)
    return TopicConfigView(
        name=topic,
        partitions=partitions,
        replication_factor=len(partitions[0].replicas),
        entries=_config_entries(admin, topic, timeout),
    )
