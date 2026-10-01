import concurrent.futures

import pytest
from confluent_kafka import KafkaError, KafkaException

from kafka_web.errors import (
    AppError,
    BrokerError,
    Conflict,
    Forbidden,
    KafkaTimeout,
    NotFound,
    Unauthorized,
)
from kafka_web.kafka.errors import call_with_timeout, explain_connect_failure, map_kafka_exception


def kafka_exc(code: int, reason: str | None = None) -> KafkaException:
    err = KafkaError(code, reason) if reason is not None else KafkaError(code)
    return KafkaException(err)


@pytest.mark.parametrize(
    ("code", "cls", "app_code"),
    [
        (KafkaError._TIMED_OUT, KafkaTimeout, "kafka_timeout"),
        (KafkaError.REQUEST_TIMED_OUT, KafkaTimeout, "kafka_timeout"),
        (KafkaError._TRANSPORT, BrokerError, "broker_unreachable"),
        (KafkaError._ALL_BROKERS_DOWN, BrokerError, "broker_unreachable"),
        (KafkaError._RESOLVE, BrokerError, "broker_unreachable"),
        (KafkaError.SASL_AUTHENTICATION_FAILED, Unauthorized, "authentication_failed"),
        (KafkaError._AUTHENTICATION, Unauthorized, "authentication_failed"),
        (KafkaError.TOPIC_AUTHORIZATION_FAILED, Forbidden, "authorization_failed"),
        (KafkaError.GROUP_AUTHORIZATION_FAILED, Forbidden, "authorization_failed"),
        (KafkaError.CLUSTER_AUTHORIZATION_FAILED, Forbidden, "authorization_failed"),
        (KafkaError.UNKNOWN_TOPIC_OR_PART, NotFound, "topic_not_found"),
        (KafkaError._UNKNOWN_TOPIC, NotFound, "topic_not_found"),
        (KafkaError.GROUP_ID_NOT_FOUND, NotFound, "group_not_found"),
        (KafkaError.NON_EMPTY_GROUP, Conflict, "group_not_empty"),
        (KafkaError._SSL, BrokerError, "tls_error"),
        (KafkaError.INVALID_PARTITIONS, BrokerError, "broker_error"),
    ],
)
def test_kafka_exception_mapping(code: int, cls: type[AppError], app_code: str):
    mapped = map_kafka_exception(kafka_exc(code))
    assert type(mapped) is cls
    assert mapped.code == app_code
    assert mapped.status == cls.status


def test_status_codes_follow_ruling_d():
    assert map_kafka_exception(kafka_exc(KafkaError._TIMED_OUT)).status == 504
    assert map_kafka_exception(kafka_exc(KafkaError._TRANSPORT)).status == 502


def test_bare_kafka_error_is_mapped_like_its_exception():
    mapped = map_kafka_exception(KafkaError(KafkaError.GROUP_ID_NOT_FOUND))  # type: ignore[arg-type]
    assert isinstance(mapped, NotFound)
    assert mapped.code == "group_not_found"


def test_message_comes_from_the_kafka_error():
    mapped = map_kafka_exception(kafka_exc(KafkaError._TRANSPORT, "Broker transport failure"))
    assert "Broker transport failure" in mapped.message


def test_futures_timeout_maps_to_kafka_timeout():
    mapped = map_kafka_exception(concurrent.futures.TimeoutError())
    assert isinstance(mapped, KafkaTimeout)
    assert mapped.code == "kafka_timeout"
    assert mapped.message


def test_arbitrary_exception_maps_to_broker_error():
    mapped = map_kafka_exception(RuntimeError("librdkafka exploded"))
    assert type(mapped) is BrokerError
    assert mapped.code == "broker_error"
    assert "librdkafka exploded" in mapped.message


def test_exception_with_empty_text_still_has_a_message():
    mapped = map_kafka_exception(RuntimeError())
    assert mapped.message


def test_app_error_passes_through_unchanged():
    original = NotFound("nope", code="cluster_not_found")
    assert map_kafka_exception(original) is original


@pytest.mark.parametrize(
    "text",
    [
        "config sasl.password=hunter2 rejected",
        "Invalid value password=hunter2, try again",
        'PlainLoginModule required username="app" password="hunter2";',
        "PASSWORD: hunter2",
    ],
)
def test_message_never_contains_password_values(text: str):
    for exc in (RuntimeError(text), kafka_exc(KafkaError._INVALID_ARG, text)):
        mapped = map_kafka_exception(exc)
        assert "hunter2" not in mapped.message
        assert "hunter2" not in str(mapped)


def test_call_with_timeout_returns_value():
    assert call_with_timeout(lambda: 42) == 42


def test_call_with_timeout_maps_exceptions():
    def boom():
        raise kafka_exc(KafkaError._TIMED_OUT)

    with pytest.raises(KafkaTimeout) as info:
        call_with_timeout(boom)
    assert info.value.__cause__ is None  # the original may embed config values


def test_call_with_timeout_maps_future_timeouts():
    future: concurrent.futures.Future[int] = concurrent.futures.Future()
    with pytest.raises(KafkaTimeout):
        call_with_timeout(lambda: future.result(timeout=0.01))


# --- connect-failure refinement ----------------------------------------------------------------
# librdkafka's list_topics only says "_TRANSPORT"; the real cause is reported via error_cb.


def transport_failure() -> AppError:
    return map_kafka_exception(kafka_exc(KafkaError._TRANSPORT, "Failed to get metadata"))


def test_explain_prefers_authentication_cause():
    reported = [
        KafkaError(KafkaError._ALL_BROKERS_DOWN, "1/1 brokers are down"),
        KafkaError(KafkaError._AUTHENTICATION, "SASL authentication error: password=hunter2"),
    ]
    explained = explain_connect_failure(transport_failure(), reported)
    assert isinstance(explained, Unauthorized)
    assert "SASL authentication error" in explained.message
    assert "hunter2" not in explained.message


def test_explain_prefers_tls_cause():
    reported = [KafkaError(KafkaError._SSL, "SSL handshake failed: certificate verify failed")]
    explained = explain_connect_failure(transport_failure(), reported)
    assert type(explained) is BrokerError
    assert explained.code == "tls_error"
    assert "certificate verify failed" in explained.message


def test_explain_adds_transport_detail_to_message():
    reported = [KafkaError(KafkaError._TRANSPORT, "Connect to 127.0.0.1:1 failed: refused")]
    explained = explain_connect_failure(transport_failure(), reported)
    assert explained.code == "broker_unreachable"
    assert "Connect to 127.0.0.1:1 failed: refused" in explained.message


def test_explain_keeps_timeout_status_with_detail():
    failure = map_kafka_exception(kafka_exc(KafkaError._TIMED_OUT))
    reported = [KafkaError(KafkaError._RESOLVE, "Failed to resolve 'nope:9092'")]
    explained = explain_connect_failure(failure, reported)
    assert isinstance(explained, KafkaTimeout)
    assert "Failed to resolve" in explained.message


def test_explain_without_reports_returns_original():
    failure = transport_failure()
    assert explain_connect_failure(failure, []) is failure


def test_explain_does_not_override_specific_failures():
    failure = NotFound("gone", code="topic_not_found")
    reported = [KafkaError(KafkaError._AUTHENTICATION, "auth")]
    assert explain_connect_failure(failure, reported) is failure
