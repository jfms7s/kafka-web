"""Existence checks for a topic that never ask the brokers about one topic by name."""

from typing import Any

from confluent_kafka.admin import TopicMetadata

from kafka_web.errors import NotFound
from kafka_web.kafka.errors import call_with_timeout, map_kafka_exception

DEFAULT_TIMEOUT_S = 10.0


def find_topic(admin: Any, topic: str, timeout: float = DEFAULT_TIMEOUT_S) -> TopicMetadata:
    """Metadata of `topic`; 404 `topic_not_found` if the cluster has no such topic.

    Looks the topic up in the full listing, never by asking the brokers about that one name: a
    broker with `auto.create.topics.enable` may create a topic that is merely asked about, and
    viewing, publishing or resetting must never create one (a read-only cluster included).
    """
    metadata = call_with_timeout(lambda: admin.list_topics(timeout=timeout))
    found = metadata.topics.get(topic)
    if found is None:
        raise NotFound(f"Topic {topic!r} does not exist", code="topic_not_found")
    if found.error is not None:
        raise map_kafka_exception(found.error)
    return found


def topic_partition_ids(admin: Any, topic: str, timeout: float = DEFAULT_TIMEOUT_S) -> list[int]:
    """Sorted partition ids of `topic`; 404 `topic_not_found` if it is missing or has none."""
    found = find_topic(admin, topic, timeout)
    if not found.partitions:
        raise NotFound(f"Topic {topic!r} does not exist", code="topic_not_found")
    return sorted(found.partitions)
