import pytest
from confluent_kafka import KafkaError, KafkaException

from kafka_web.errors import BrokerError, KafkaTimeout, ValidationFailed
from kafka_web.services.publish import (
    BatchResult,
    OutMessage,
    RowError,
    RowResult,
    publish,
    publish_one,
)
from tests.fakes import DeliveringProducer


def msg(value: str, **kwargs) -> OutMessage:
    return OutMessage(key=kwargs.pop("key", None), value=value.encode(), headers=[], **kwargs)


def test_every_message_is_produced_with_key_value_headers_and_partition() -> None:
    producer = DeliveringProducer()
    message = OutMessage(key=b"k", value=b"v", headers=[("h", b"1")], partition=2)

    publish(producer, "orders", [message, msg("plain")])

    first, second = producer.produced
    assert (first["topic"], first["key"], first["value"]) == ("orders", b"k", b"v")
    assert first["headers"] == [("h", b"1")]
    assert first["partition"] == 2
    assert second["key"] is None
    assert "partition" not in second  # unset: left to the partitioner
    assert second["headers"] is None  # no headers


def test_all_delivered_reports_partition_and_offset_per_row() -> None:
    producer = DeliveringProducer()

    result = publish(producer, "orders", [msg("a"), msg("b"), msg("c")])

    assert result == BatchResult(
        succeeded=3,
        failed=0,
        results=[
            RowResult(row=1, ok=True, partition=0, offset=100),
            RowResult(row=2, ok=True, partition=1, offset=100),
            RowResult(row=3, ok=True, partition=0, offset=101),
        ],
    )
    assert producer.flush_timeouts == [30.0]


def test_delivery_errors_fail_only_their_rows() -> None:
    err = KafkaError(KafkaError.MSG_SIZE_TOO_LARGE)
    producer = DeliveringProducer(fail={1: err})

    result = publish(producer, "orders", [msg("a"), msg("b"), msg("c")])

    assert (result.succeeded, result.failed) == (2, 1)
    assert [r.ok for r in result.results] == [True, False, True]
    failed = result.results[1]
    assert failed.row == 2
    assert failed.partition is None and failed.offset is None
    assert failed.error and "large" in failed.error.lower()


def test_row_errors_are_reported_failed_in_order_without_being_produced() -> None:
    producer = DeliveringProducer()
    items = [msg("a"), RowError(row=2, error="Item must be an object"), msg("c")]

    result = publish(producer, "orders", items)

    assert len(producer.produced) == 2
    assert [(r.row, r.ok, r.error) for r in result.results] == [
        (1, True, None),
        (2, False, "Item must be an object"),
        (3, True, None),
    ]
    assert (result.succeeded, result.failed) == (2, 1)


def test_undelivered_rows_fail_with_delivery_timeout() -> None:
    producer = DeliveringProducer(silent={1})

    result = publish(producer, "orders", [msg("a"), msg("b"), msg("c")], flush_timeout=0.1)

    assert [r.ok for r in result.results] == [True, False, True]
    assert result.results[1].error == "delivery timeout"
    assert producer.flush_timeouts == [0.1]


def test_a_full_queue_is_polled_and_retried() -> None:
    producer = DeliveringProducer(buffer_errors=1)

    result = publish(producer, "orders", [msg("a")])

    assert result.succeeded == 1
    assert producer.attempts == 2
    assert 0.5 in producer.poll_timeouts


def test_a_queue_that_never_drains_fails_the_row_instead_of_hanging() -> None:
    producer = DeliveringProducer(buffer_errors=10**9)

    result = publish(producer, "orders", [msg("a"), msg("b")], flush_timeout=0.05)

    assert [r.ok for r in result.results] == [False, False]
    assert "queue" in (result.results[0].error or "").lower()


def test_a_produce_time_failure_fails_only_that_row() -> None:
    boom = KafkaException(KafkaError(KafkaError.MSG_SIZE_TOO_LARGE))
    producer = DeliveringProducer(produce_errors={0: boom})

    result = publish(producer, "orders", [msg("a"), msg("b")])

    assert [r.ok for r in result.results] == [False, True]
    assert "large" in (result.results[0].error or "").lower()


def test_a_produce_time_exception_without_a_kafka_error_still_fails_just_that_row() -> None:
    producer = DeliveringProducer(produce_errors={0: KafkaException("plain text")})

    result = publish(producer, "orders", [msg("a"), msg("b")])

    assert [r.ok for r in result.results] == [False, True]
    assert "plain text" in (result.results[0].error or "")


def test_a_report_served_by_another_thread_after_flush_still_counts() -> None:
    # flush() returned 0 (queue empty) but this message's callback has not run yet.
    producer = DeliveringProducer(late={0})

    result = publish(producer, "orders", [msg("a")])

    assert result.succeeded == 1


def test_an_empty_batch_produces_nothing() -> None:
    producer = DeliveringProducer()

    assert publish(producer, "orders", []) == BatchResult(succeeded=0, failed=0, results=[])


def test_error_text_never_carries_a_password() -> None:
    err = KafkaError(KafkaError._TRANSPORT, 'sasl.password="hunter2" refused')
    producer = DeliveringProducer(fail={0: err})

    result = publish(producer, "orders", [msg("a")])

    assert "hunter2" not in (result.results[0].error or "")


# --- publish_one -----------------------------------------------------------------------------


def test_publish_one_returns_partition_and_offset() -> None:
    producer = DeliveringProducer()

    assert publish_one(producer, "orders", msg("a", partition=1)) == (1, 100)


def test_publish_one_maps_a_delivery_error() -> None:
    producer = DeliveringProducer(fail={0: KafkaError(KafkaError._MSG_TIMED_OUT, "gone")})

    with pytest.raises(BrokerError):
        publish_one(producer, "orders", msg("a"))


def test_publish_one_maps_authorization_failure() -> None:
    from kafka_web.errors import Forbidden

    producer = DeliveringProducer(fail={0: KafkaError(KafkaError.TOPIC_AUTHORIZATION_FAILED)})

    with pytest.raises(Forbidden) as info:
        publish_one(producer, "orders", msg("a"))

    assert info.value.code == "authorization_failed"


def test_publish_one_undelivered_is_a_kafka_timeout() -> None:
    producer = DeliveringProducer(silent={0})

    with pytest.raises(KafkaTimeout):
        publish_one(producer, "orders", msg("a"), flush_timeout=0.1)


def test_publish_one_maps_a_produce_time_kafka_exception() -> None:
    boom = KafkaException(KafkaError(KafkaError._UNKNOWN_PARTITION, "no such partition"))
    producer = DeliveringProducer(produce_errors={0: boom})

    with pytest.raises(BrokerError):
        publish_one(producer, "orders", msg("a"))


def test_publish_one_rejects_a_value_the_client_refuses_as_validation() -> None:
    producer = DeliveringProducer(produce_errors={0: ValueError("bad header")})

    with pytest.raises(ValidationFailed):
        publish_one(producer, "orders", msg("a"))
