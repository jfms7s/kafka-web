from typing import Any

import pytest
from confluent_kafka import KafkaError, KafkaException

from kafka_web.errors import BrokerError, KafkaTimeout, NotFound, ValidationFailed
from kafka_web.services.consume import SnapshotParams, consume_snapshot
from tests.fakes import FakeClock, FakeConsumer, FakeMessage, eof

CONF = {"bootstrap.servers": "b1:9092", "sasl.password": "s3cret"}


class Harness:
    """Runs `consume_snapshot` against one FakeConsumer on a fake clock."""

    def __init__(self, **consumer_kwargs: Any) -> None:
        self.clock = FakeClock()
        self.consumers: list[FakeConsumer] = []
        self._kwargs = consumer_kwargs

    def factory(self, conf: dict[str, Any]) -> FakeConsumer:
        consumer = FakeConsumer(conf, clock=self.clock, **self._kwargs)
        self.consumers.append(consumer)
        return consumer

    def run(self, **params: Any):
        return consume_snapshot(
            CONF,
            "orders",
            SnapshotParams(**params),
            consumer_factory=self.factory,
            clock=self.clock,
        )

    @property
    def consumer(self) -> FakeConsumer:
        assert len(self.consumers) == 1
        return self.consumers[0]


def msgs(partition: int, start: int, stop: int, ts: int = 1000) -> list[FakeMessage]:
    return [
        FakeMessage(partition=partition, offset=o, key=f"k{o}".encode(), timestamp=(1, ts + o))
        for o in range(start, stop)
    ]


def test_stops_at_count() -> None:
    h = Harness(marks={0: (0, 10)}, script=msgs(0, 0, 10))

    result = h.run(start="earliest", count=3)

    assert [m.offset for m in result] == [0, 1, 2]
    assert h.consumer.poll_timeouts.__len__() == 3


def test_stops_at_deadline_on_an_idle_topic() -> None:
    h = Harness(marks={0: (0, 5)}, script=[])

    result = h.run(start="earliest", timeout=2)

    assert result == []
    assert h.clock.now == pytest.approx(1002.0)  # exactly the 2 s budget was waited
    assert max(h.consumer.poll_timeouts) <= 0.5
    assert h.consumer.closed


def test_poll_timeout_never_overshoots_the_deadline() -> None:
    h = Harness(marks={0: (0, 5)}, script=[])

    h.run(start="earliest", timeout=1.2)

    assert sum(h.consumer.poll_timeouts) == pytest.approx(1.2)


def test_returns_early_when_every_partition_reached_its_end() -> None:
    h = Harness(marks={0: (0, 2), 1: (0, 1)}, script=[*msgs(0, 0, 2), *msgs(1, 0, 1)])

    result = h.run(start="earliest", count=100, timeout=60)

    assert len(result) == 3
    assert len(h.consumer.poll_timeouts) == 3  # no idle polling after the last message
    assert h.clock.now < 1001


def test_partition_eof_marks_a_partition_done_even_if_end_offset_is_never_delivered() -> None:
    # Transaction markers / compaction leave offsets that are never delivered as messages.
    h = Harness(marks={0: (0, 10)}, script=[*msgs(0, 0, 2), eof(0, 2)])

    result = h.run(start="earliest", count=100, timeout=60)

    assert [m.offset for m in result] == [0, 1]
    assert h.clock.now < 1001


def test_ignores_messages_at_or_after_the_captured_end() -> None:
    h = Harness(marks={0: (0, 2)}, script=msgs(0, 0, 5))

    result = h.run(start="earliest", count=100)

    assert [m.offset for m in result] == [0, 1]


def test_partition_eof_events_are_skipped() -> None:
    h = Harness(marks={0: (0, 3), 1: (0, 1)}, script=[*msgs(0, 0, 3), eof(0, 3), *msgs(1, 0, 1)])

    result = h.run(start="earliest")

    assert len(result) == 4


def test_empty_plan_never_assigns_or_polls() -> None:
    h = Harness(marks={0: (0, 0), 1: (4, 4)})

    result = h.run(start="latest")

    assert result == []
    assert h.consumer.assigned is None
    assert "poll" not in h.consumer.calls
    assert h.consumer.closed


def test_consumer_is_closed_when_poll_raises() -> None:
    boom = KafkaException(KafkaError(KafkaError._ALL_BROKERS_DOWN, "all down"))
    h = Harness(marks={0: (0, 5)}, script=[boom])

    with pytest.raises(BrokerError) as exc:
        h.run(start="earliest")

    assert exc.value.code == "broker_unreachable"
    assert h.consumer.closed


def test_message_errors_other_than_eof_are_mapped_and_close_the_consumer() -> None:
    err = FakeMessage(error=KafkaError(KafkaError.UNKNOWN_TOPIC_OR_PART))
    h = Harness(marks={0: (0, 5)}, script=[err])

    with pytest.raises(NotFound):
        h.run(start="earliest")

    assert h.consumer.closed


def test_consumer_config_has_no_group_footprint() -> None:
    h = Harness(marks={0: (0, 1)}, script=msgs(0, 0, 1))

    h.run(start="earliest")
    conf = h.consumer.conf

    assert conf["group.id"].startswith("kafka-web-")
    assert conf["enable.auto.commit"] == "false"
    assert conf["enable.partition.eof"] == "true"
    assert conf["auto.offset.reset"] == "earliest"
    assert conf["bootstrap.servers"] == "b1:9092"
    assert conf["sasl.password"] == "s3cret"
    assert "assign" in h.consumer.calls  # FakeConsumer raises if subscribe/commit are called


def test_every_request_gets_its_own_group_id() -> None:
    h = Harness(marks={0: (0, 0)})

    h.run()
    h.run()

    assert h.consumers[0].conf["group.id"] != h.consumers[1].conf["group.id"]


def test_the_callers_config_is_not_mutated() -> None:
    h = Harness(marks={0: (0, 0)})

    h.run()

    assert "group.id" not in CONF


def test_every_blocking_call_has_an_explicit_timeout() -> None:
    h = Harness(marks={0: (0, 1), 1: (0, 1)}, script=[*msgs(0, 0, 1), *msgs(1, 0, 1)])

    h.run(start="earliest")

    assert h.consumer.metadata_timeouts == [10]
    assert h.consumer.watermark_timeouts == [10, 10]


def test_unknown_topic_is_404() -> None:
    h = Harness(topic="other")

    with pytest.raises(NotFound) as exc:
        h.run()

    assert exc.value.code == "topic_not_found"
    assert h.consumer.closed


def test_topic_metadata_error_unknown_topic_is_404() -> None:
    h = Harness(marks={0: (0, 1)}, topic_error=KafkaError(KafkaError.UNKNOWN_TOPIC_OR_PART))

    with pytest.raises(NotFound):
        h.run()

    assert h.consumer.closed


def test_partition_not_in_topic_is_a_validation_error_on_partition() -> None:
    h = Harness(marks={0: (0, 1), 1: (0, 1)})

    with pytest.raises(ValidationFailed) as exc:
        h.run(partition=7)

    assert exc.value.field == "partition"
    assert h.consumer.closed


def test_watermark_timeout_is_a_504_and_closes() -> None:
    timed_out = KafkaException(KafkaError(KafkaError._TIMED_OUT, "timed out"))
    h = Harness(marks={0: (0, 1)}, watermark_error=timed_out)

    with pytest.raises(KafkaTimeout):
        h.run()

    assert h.consumer.closed


@pytest.mark.parametrize(
    ("params", "field"),
    [
        ({"start": "offset", "partition": 0}, "offset"),
        ({"start": "offset", "offset": 3}, "partition"),
        ({"start": "timestamp"}, "timestamp"),
    ],
)
def test_incomplete_start_params_fail_before_any_consumer_is_created(
    params: dict[str, Any], field: str
) -> None:
    h = Harness()

    with pytest.raises(ValidationFailed) as exc:
        h.run(**params)

    assert exc.value.field == field
    assert h.consumers == []


def test_latest_keeps_the_newest_count_across_partitions() -> None:
    # P=2, count=3 -> per=2: 4 messages are read, the last 3 by timestamp are returned.
    script = [
        FakeMessage(partition=0, offset=8, timestamp=(1, 10)),
        FakeMessage(partition=0, offset=9, timestamp=(1, 30)),
        FakeMessage(partition=1, offset=3, timestamp=(1, 20)),
        FakeMessage(partition=1, offset=4, timestamp=(1, 40)),
    ]
    h = Harness(marks={0: (0, 10), 1: (0, 5)}, script=script)

    result = h.run(start="latest", count=3)

    assert h.consumer.assigned == [(0, 8), (1, 3)]
    assert [(m.partition, m.offset) for m in result] == [(1, 3), (0, 9), (1, 4)]


def test_results_are_sorted_by_timestamp_then_partition_then_offset() -> None:
    script = [
        FakeMessage(partition=1, offset=0, timestamp=(1, 5)),
        FakeMessage(partition=0, offset=1, timestamp=(1, 5)),
        FakeMessage(partition=0, offset=0, timestamp=(1, 5)),
        FakeMessage(partition=1, offset=1, timestamp=(1, 1)),
    ]
    h = Harness(marks={0: (0, 2), 1: (0, 2)}, script=script)

    result = h.run(start="earliest")

    assert [(m.partition, m.offset) for m in result] == [(1, 1), (0, 0), (0, 1), (1, 0)]


def test_messages_without_a_timestamp_sort_first() -> None:
    script = [
        FakeMessage(partition=0, offset=0, timestamp=(1, 9)),
        FakeMessage(partition=0, offset=1, timestamp=(0, -1)),
    ]
    h = Harness(marks={0: (0, 2)}, script=script)

    result = h.run(start="earliest")

    assert [m.offset for m in result] == [1, 0]


def test_offset_mode_assigns_the_clamped_offset_of_one_partition() -> None:
    h = Harness(marks={0: (0, 5), 1: (10, 20)}, script=msgs(1, 12, 20))

    result = h.run(start="offset", partition=1, offset=12, count=3)

    assert h.consumer.assigned == [(1, 12)]
    assert [m.offset for m in result] == [12, 13, 14]
    assert h.consumer.watermark_timeouts == [10]  # only the chosen partition is queried


def test_timestamp_mode_skips_partitions_without_a_message_at_or_after_it() -> None:
    h = Harness(marks={0: (0, 5), 1: (0, 5)}, times={0: 2, 1: -1}, script=msgs(0, 2, 5))

    result = h.run(start="timestamp", timestamp=1234)

    assert h.consumer.assigned == [(0, 2)]
    assert [m.offset for m in result] == [2, 3, 4]


def test_timestamp_with_no_match_returns_empty_without_assigning() -> None:
    h = Harness(marks={0: (0, 5)}, times={})

    assert h.run(start="timestamp", timestamp=1234) == []
    assert h.consumer.assigned is None


def test_messages_are_decoded_views() -> None:
    h = Harness(
        marks={0: (0, 1)},
        script=[FakeMessage(offset=0, key=b"k", value=b'{"a": 1}', headers=[("h", b"\xff")])],
    )

    [view] = h.run(start="earliest")

    assert view.key.data == "k"
    assert view.value.json_value == {"a": 1}
    assert view.headers[0].value.encoding == "base64"


def test_snapshot_params_defaults_and_bounds() -> None:
    p = SnapshotParams()
    assert (p.count, p.timeout, p.start) == (100, 10, "latest")
    for bad in ({"count": 0}, {"count": 10_001}, {"timeout": 0.5}, {"timeout": 61}):
        with pytest.raises(ValueError):
            SnapshotParams(**bad)
