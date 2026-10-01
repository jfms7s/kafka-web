from concurrent.futures import Future

import pytest
from confluent_kafka import KafkaError, KafkaException
from confluent_kafka.admin import ConfigResource, ResourceType

from kafka_web.errors import BrokerError, KafkaTimeout, NotFound
from kafka_web.services.topics import (
    ConfigEntryView,
    PartitionDetail,
    TopicSummary,
    describe_topic,
    list_topics,
)
from tests.fakes import NEVER, FakeTopicAdmin, config_entry, partition, topic_meta


def orders_meta():
    return topic_meta(
        "orders",
        [partition(1, 2, [2, 3, 1], [2, 3]), partition(0, 1, [1, 2, 3], [1, 2, 3])],
    )


def test_list_topics_sorted_with_internal_flag_and_rf_from_partition_zero() -> None:
    admin = FakeTopicAdmin(
        {
            "orders": orders_meta(),
            "__consumer_offsets": topic_meta("__consumer_offsets", [partition(0, 1, [1], [1])]),
            "alpha": topic_meta("alpha", [partition(0, 1, [1, 2], [1])]),
        }
    )

    assert list_topics(admin) == [
        TopicSummary("__consumer_offsets", 1, 1, True),
        TopicSummary("alpha", 1, 2, False),
        TopicSummary("orders", 2, 3, False),
    ]


def test_list_topics_passes_timeout_and_handles_partitionless_topic() -> None:
    admin = FakeTopicAdmin({"empty": topic_meta("empty", [])})

    assert list_topics(admin, timeout=3.0) == [TopicSummary("empty", 0, 0, False)]
    assert admin.list_topics_calls == [{"topic": None, "timeout": 3.0}]


def test_list_topics_maps_broker_failures() -> None:
    class Down(FakeTopicAdmin):
        def list_topics(self, topic=None, timeout=None):
            raise KafkaException(KafkaError(KafkaError._ALL_BROKERS_DOWN, "all down"))

    with pytest.raises(BrokerError) as caught:
        list_topics(Down())
    assert caught.value.code == "broker_unreachable"


def test_describe_topic_partitions_sorted_and_rf_from_first_partition() -> None:
    admin = FakeTopicAdmin({"orders": orders_meta()}, {"orders": {}})

    view = describe_topic(admin, "orders")

    assert view.name == "orders"
    assert view.replication_factor == 3
    assert view.partitions == [
        PartitionDetail(0, 1, [1, 2, 3], [1, 2, 3]),
        PartitionDetail(1, 2, [2, 3, 1], [2, 3]),
    ]
    assert admin.list_topics_calls == [{"topic": "orders", "timeout": 10.0}]


def test_describe_topic_entries_non_default_first_then_by_name() -> None:
    admin = FakeTopicAdmin(
        {"orders": orders_meta()},
        {
            "orders": {
                "segment.bytes": config_entry("segment.bytes", "1073741824", default=True),
                "retention.ms": config_entry("retention.ms", "3600000"),
                "cleanup.policy": config_entry("cleanup.policy", "delete", default=True),
                "max.message.bytes": config_entry("max.message.bytes", "2097152"),
            }
        },
    )

    entries = describe_topic(admin, "orders").entries

    assert [e.name for e in entries] == [
        "max.message.bytes",
        "retention.ms",
        "cleanup.policy",
        "segment.bytes",
    ]
    assert entries[1] == ConfigEntryView(
        name="retention.ms",
        value="3600000",
        display_value="1h",
        is_default=False,
        source="DYNAMIC_TOPIC_CONFIG",
        sensitive=False,
    )
    assert entries[3].display_value == "1 GiB"
    assert entries[3].is_default is True
    assert entries[3].source == "DEFAULT_CONFIG"


def test_describe_topic_requests_the_topic_resource_with_timeout() -> None:
    admin = FakeTopicAdmin({"orders": orders_meta()}, {"orders": {}})
    seen: list[ConfigResource] = []
    original = admin.describe_configs

    def spy(resources):
        seen.extend(resources)
        return original(resources)

    admin.describe_configs = spy  # type: ignore[method-assign]
    describe_topic(admin, "orders")

    assert [(r.restype, r.name) for r in seen] == [(ResourceType.TOPIC, "orders")]


def test_sensitive_entries_never_expose_a_value() -> None:
    admin = FakeTopicAdmin(
        {"orders": orders_meta()},
        {"orders": {"secret.thing": config_entry("secret.thing", "leaked", sensitive=True)}},
    )

    [entry] = describe_topic(admin, "orders").entries

    assert entry.sensitive is True
    assert entry.value is None
    assert entry.display_value is None


def test_unknown_topic_missing_from_metadata_is_not_found() -> None:
    with pytest.raises(NotFound) as caught:
        describe_topic(FakeTopicAdmin(), "ghost")
    assert caught.value.code == "topic_not_found"


def test_unknown_topic_reported_via_metadata_error_is_not_found() -> None:
    error = KafkaError(KafkaError.UNKNOWN_TOPIC_OR_PART)
    admin = FakeTopicAdmin({"ghost": topic_meta("ghost", [], error=error)})

    with pytest.raises(NotFound) as caught:
        describe_topic(admin, "ghost")
    assert caught.value.code == "topic_not_found"


def test_topic_without_partitions_and_no_error_is_not_found() -> None:
    with pytest.raises(NotFound):
        describe_topic(FakeTopicAdmin({"ghost": topic_meta("ghost", [])}), "ghost")


def test_other_metadata_errors_are_mapped_not_masked_as_not_found() -> None:
    error = KafkaError(KafkaError._TIMED_OUT)
    admin = FakeTopicAdmin({"orders": topic_meta("orders", [], error=error)})

    with pytest.raises(KafkaTimeout):
        describe_topic(admin, "orders")


def test_describe_configs_failure_is_mapped() -> None:
    admin = FakeTopicAdmin({"orders": orders_meta()})

    def failing(resources):
        future: Future = Future()
        future.set_exception(KafkaException(KafkaError(KafkaError.UNKNOWN_TOPIC_OR_PART)))
        return {resources[0]: future}

    admin.describe_configs = failing  # type: ignore[method-assign]

    with pytest.raises(NotFound):
        describe_topic(admin, "orders")


def test_describe_configs_future_is_resolved_with_the_timeout() -> None:
    admin = FakeTopicAdmin({"orders": orders_meta()}, {"orders": NEVER})

    with pytest.raises(KafkaTimeout):
        describe_topic(admin, "orders", timeout=0.01)


def test_source_given_as_a_raw_int_is_named() -> None:
    """The real client reports `ConfigEntry.source` as a plain int."""
    entry = config_entry("retention.ms", "1")
    entry.source = 1  # DYNAMIC_TOPIC_CONFIG
    admin = FakeTopicAdmin({"orders": orders_meta()}, {"orders": {"retention.ms": entry}})

    [view] = describe_topic(admin, "orders").entries

    assert view.source == "DYNAMIC_TOPIC_CONFIG"
