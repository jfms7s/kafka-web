import threading
import time

import pytest
from confluent_kafka import OFFSET_END, KafkaError, KafkaException
from pydantic import ValidationError

from kafka_web.errors import BrokerError, NotFound, ValidationFailed
from kafka_web.services.consume import validate_start
from kafka_web.services.decode import MessageView, to_message_view
from kafka_web.services.stream import BoundedDropQueue, StreamParams, StreamWorker
from tests.fakes import FakeMessage, LiveConsumers, LiveFakeConsumer, eof

CONF = {"bootstrap.servers": "b1:9092"}
POLL_S = 0.1


def view(offset: int) -> MessageView:
    return to_message_view(FakeMessage(offset=offset, value=f"v{offset}".encode()))


# --- StreamParams -----------------------------------------------------------------------------


def test_params_default_to_latest() -> None:
    assert StreamParams() == StreamParams(start="latest")


@pytest.mark.parametrize("bad", [{"start": "earliest"}, {"start": "middle"}, {"timestamp": -1}])
def test_params_reject_unknown_start_and_negative_timestamp(bad: dict) -> None:
    with pytest.raises(ValidationError):
        StreamParams.model_validate(bad)


@pytest.mark.parametrize(
    ("params", "field"),
    [
        (StreamParams(start="offset", offset=3), "partition"),
        (StreamParams(start="offset", partition=0), "offset"),
        (StreamParams(start="timestamp"), "timestamp"),
    ],
)
def test_params_cross_field_rules_match_snapshot(params: StreamParams, field: str) -> None:
    with pytest.raises(ValidationFailed) as caught:
        validate_start(params)
    assert caught.value.field == field


# --- BoundedDropQueue -------------------------------------------------------------------------


def test_queue_drops_oldest_when_full_and_counts_drops() -> None:
    queue = BoundedDropQueue(maxsize=3)
    for i in range(5):
        queue.put(view(i))

    items, dropped = queue.drain()

    assert [m.offset for m in items] == [2, 3, 4]
    assert dropped == 2


def test_queue_drop_counter_resets_per_drain() -> None:
    queue = BoundedDropQueue(maxsize=1)
    queue.put(view(0))
    queue.put(view(1))
    assert queue.drain()[1] == 1

    assert queue.drain() == ([], 0)
    queue.put(view(2))
    assert queue.drain() == ([view(2)], 0)


def test_queue_drain_takes_at_most_max_items_in_order() -> None:
    queue = BoundedDropQueue(maxsize=10)
    for i in range(5):
        queue.put(view(i))

    first, _ = queue.drain(max_items=2)
    rest, _ = queue.drain(max_items=100)

    assert [m.offset for m in first] == [0, 1]
    assert [m.offset for m in rest] == [2, 3, 4]


def test_queue_default_capacity_is_1000() -> None:
    queue = BoundedDropQueue()
    message = view(0)
    for _ in range(1001):
        queue.put(message)

    items, dropped = queue.drain(max_items=5000)
    assert (len(items), dropped) == (1000, 1)


def test_queue_is_thread_safe_under_concurrent_puts_and_drains() -> None:
    queue = BoundedDropQueue(maxsize=1000)
    message = view(0)
    producers_done = threading.Event()
    kept = dropped = 0

    def produce() -> None:
        for _ in range(5000):
            queue.put(message)

    def consume() -> None:
        nonlocal kept, dropped
        while not producers_done.is_set():
            items, lost = queue.drain(100)
            kept, dropped = kept + len(items), dropped + lost

    producers = [threading.Thread(target=produce) for _ in range(2)]
    drainer = threading.Thread(target=consume)
    drainer.start()
    for thread in producers:
        thread.start()
    for thread in producers:
        thread.join()
    producers_done.set()
    drainer.join()
    items, lost = queue.drain(max_items=10_000)

    assert kept + len(items) + dropped + lost == 10_000


# --- StreamWorker -----------------------------------------------------------------------------


def start_worker(
    consumers: LiveConsumers, params: StreamParams | None = None, topic: str = "orders"
) -> tuple[StreamWorker, BoundedDropQueue, threading.Event]:
    queue, stop = BoundedDropQueue(), threading.Event()
    worker = StreamWorker(
        CONF,
        topic,
        params or StreamParams(),
        queue,
        stop,
        consumer_factory=consumers,
        poll_interval=POLL_S,
    )
    worker.start()
    return worker, queue, stop


def started(consumers: LiveConsumers) -> LiveFakeConsumer:
    consumer = consumers.wait_for_consumer()
    assert consumer.assigned_event.wait(2), "the worker never assigned partitions"
    return consumer


def wait_for(queue: BoundedDropQueue, count: int, timeout: float = 2.0) -> list[MessageView]:
    collected: list[MessageView] = []
    deadline = time.monotonic() + timeout
    while len(collected) < count and time.monotonic() < deadline:
        collected += queue.drain()[0]
        time.sleep(0.01)
    return collected


def stopped(worker: StreamWorker, stop: threading.Event) -> None:
    stop.set()
    worker.join(timeout=2)
    assert not worker.is_alive()


def test_worker_is_a_daemon_thread() -> None:
    worker = StreamWorker(CONF, "orders", StreamParams(), BoundedDropQueue(), threading.Event())
    assert worker.daemon


def test_latest_assigns_offset_end_on_every_partition() -> None:
    consumers = LiveConsumers(marks={0: (0, 5), 1: (2, 9), 2: (0, 0)})
    worker, _, stop = start_worker(consumers)

    consumer = started(consumers)
    stopped(worker, stop)

    assert consumer.assigned == [(0, OFFSET_END), (1, OFFSET_END), (2, OFFSET_END)]
    assert worker.error is None


def test_latest_with_partition_assigns_only_that_partition() -> None:
    consumers = LiveConsumers(marks={0: (0, 5), 1: (2, 9)})
    worker, _, stop = start_worker(consumers, StreamParams(partition=1))

    consumer = started(consumers)
    stopped(worker, stop)

    assert consumer.assigned == [(1, OFFSET_END)]


def test_offset_start_is_clamped_to_the_watermarks() -> None:
    for offset, expected in [(20, 20), (3, 10), (100, 50)]:
        consumers = LiveConsumers(marks={0: (0, 0), 2: (10, 50)})
        worker, _, stop = start_worker(
            consumers, StreamParams(start="offset", offset=offset, partition=2)
        )
        consumer = started(consumers)
        stopped(worker, stop)

        assert consumer.assigned == [(2, expected)], offset


def test_timestamp_start_waits_at_the_end_of_partitions_without_newer_messages() -> None:
    consumers = LiveConsumers(marks={0: (0, 10), 1: (0, 7)}, times={0: 4, 1: -1})
    worker, _, stop = start_worker(consumers, StreamParams(start="timestamp", timestamp=1234))

    consumer = started(consumers)
    stopped(worker, stop)

    # Partition 1 has nothing at/after the timestamp yet: it follows from its high watermark.
    assert consumer.assigned == [(0, 4), (1, 7)]


def test_consumer_has_no_group_footprint_and_no_eof_events() -> None:
    consumers = LiveConsumers()
    worker, _, stop = start_worker(consumers)

    consumer = started(consumers)
    stopped(worker, stop)

    assert consumer.conf["bootstrap.servers"] == "b1:9092"
    assert consumer.conf["group.id"].startswith("kafka-web-")
    assert consumer.conf["enable.auto.commit"] == "false"
    assert consumer.conf["enable.partition.eof"] == "false"
    assert all(t is not None for t in consumer.metadata_timeouts)


def test_polled_messages_are_decoded_into_the_queue() -> None:
    consumers = LiveConsumers()
    worker, queue, stop = start_worker(consumers)
    consumer = started(consumers)

    consumer.feed(FakeMessage(offset=7, key=b"k", value=b'{"a": 1}'), eof(0), FakeMessage(offset=8))
    messages = wait_for(queue, 2)
    stopped(worker, stop)

    assert [m.offset for m in messages] == [7, 8]
    assert messages[0].value.json_value == {"a": 1}
    assert worker.error is None


def test_polls_use_the_poll_interval() -> None:
    consumers = LiveConsumers()
    worker, _, stop = start_worker(consumers)
    consumer = started(consumers)
    time.sleep(POLL_S * 2.5)
    stopped(worker, stop)

    assert consumer.poll_timeouts
    assert set(consumer.poll_timeouts) == {POLL_S}


def test_stop_event_ends_the_loop_within_one_poll_interval_and_closes_consumer() -> None:
    consumers = LiveConsumers()
    worker, _, stop = start_worker(consumers)
    consumer = started(consumers)

    began = time.monotonic()
    stop.set()
    worker.join(timeout=2)
    elapsed = time.monotonic() - began

    assert not worker.is_alive()
    assert elapsed < POLL_S + 0.1
    assert consumer.closed
    assert consumer.calls[-1] == "close"
    assert worker.error is None


def test_stop_set_before_start_never_assigns() -> None:
    consumers = LiveConsumers()
    queue, stop = BoundedDropQueue(), threading.Event()
    stop.set()
    worker = StreamWorker(CONF, "orders", StreamParams(), queue, stop, consumer_factory=consumers)

    worker.start()
    worker.join(timeout=2)

    assert not worker.is_alive()
    [consumer] = consumers.created
    assert consumer.assigned is None
    assert consumer.closed


def test_poll_exception_is_mapped_and_closes_the_consumer() -> None:
    consumers = LiveConsumers()
    worker, _, _ = start_worker(consumers)
    consumer = started(consumers)

    consumer.feed(KafkaException(KafkaError(KafkaError.UNKNOWN_TOPIC_OR_PART)))
    worker.join(timeout=2)

    assert not worker.is_alive()
    assert isinstance(worker.error, NotFound)
    assert worker.error.code == "topic_not_found"
    assert consumer.closed


def test_unexpected_exception_becomes_a_broker_error() -> None:
    consumers = LiveConsumers()
    worker, _, _ = start_worker(consumers)
    consumer = started(consumers)

    consumer.feed(RuntimeError("boom"))
    worker.join(timeout=2)

    assert isinstance(worker.error, BrokerError)
    assert consumer.closed


def test_error_event_ends_the_stream_with_the_mapped_error() -> None:
    consumers = LiveConsumers()
    worker, _, _ = start_worker(consumers)
    consumer = started(consumers)

    consumer.feed(FakeMessage(error=KafkaError(KafkaError.TOPIC_AUTHORIZATION_FAILED)))
    worker.join(timeout=2)

    assert worker.error is not None
    assert worker.error.code == "authorization_failed"
    assert consumer.closed


def test_transient_disconnect_event_does_not_end_the_stream() -> None:
    consumers = LiveConsumers()
    worker, queue, stop = start_worker(consumers)
    consumer = started(consumers)

    consumer.feed(FakeMessage(error=KafkaError(KafkaError._TRANSPORT)), FakeMessage(offset=3))
    messages = wait_for(queue, 1)

    assert worker.is_alive()
    stopped(worker, stop)
    assert [m.offset for m in messages] == [3]
    assert worker.error is None


def test_unknown_topic_fails_setup_without_assigning() -> None:
    consumers = LiveConsumers()
    worker, _, _ = start_worker(consumers, topic="missing")
    worker.join(timeout=2)

    [consumer] = consumers.created
    assert worker.error is not None
    assert worker.error.code == "topic_not_found"
    assert consumer.assigned is None
    assert consumer.closed


def test_unknown_partition_fails_setup() -> None:
    consumers = LiveConsumers(marks={0: (0, 1)})
    worker, _, _ = start_worker(consumers, StreamParams(partition=4))
    worker.join(timeout=2)

    assert isinstance(worker.error, ValidationFailed)
    assert worker.error.field == "partition"


def test_consumer_creation_failure_is_reported() -> None:
    consumers = LiveConsumers()
    consumers.error = KafkaException(KafkaError(KafkaError._INVALID_ARG, "bad config"))
    worker, _, _ = start_worker(consumers)
    worker.join(timeout=2)

    assert not worker.is_alive()
    assert isinstance(worker.error, BrokerError)
