"""Map librdkafka / confluent-kafka failures onto the application's HTTP error model."""

import re
from collections.abc import Callable, Iterable

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

# `password=x`, `password: x`, `password="x"` (JAAS) — the value is replaced, the key kept.
_PASSWORD_VALUE = re.compile(r"""(password\s*[=:]\s*)("[^"]*"|'[^']*'|[^\s,;]+)""", re.IGNORECASE)

_TIMEOUT_CODES = {KafkaError._TIMED_OUT, KafkaError.REQUEST_TIMED_OUT}
_UNREACHABLE_CODES = {KafkaError._TRANSPORT, KafkaError._ALL_BROKERS_DOWN, KafkaError._RESOLVE}
_AUTHENTICATION_CODES = {KafkaError.SASL_AUTHENTICATION_FAILED, KafkaError._AUTHENTICATION}
_AUTHORIZATION_CODES = {
    KafkaError.TOPIC_AUTHORIZATION_FAILED,
    KafkaError.GROUP_AUTHORIZATION_FAILED,
    KafkaError.CLUSTER_AUTHORIZATION_FAILED,
}
_TOPIC_NOT_FOUND_CODES = {KafkaError.UNKNOWN_TOPIC_OR_PART, KafkaError._UNKNOWN_TOPIC}

# Codes after which the connect failure's root cause is worth looking up in error_cb reports.
_VAGUE_FAILURE_CODES = {"broker_unreachable", "kafka_timeout"}


def redact(text: str) -> str:
    return _PASSWORD_VALUE.sub(r"\1***", text)


def _from_kafka_error(err: KafkaError) -> AppError:
    code = err.code()
    message = redact(err.str()) or err.name()
    if code in _TIMEOUT_CODES:
        return KafkaTimeout(message)
    if code in _UNREACHABLE_CODES:
        return BrokerError(message, code="broker_unreachable")
    if code in _AUTHENTICATION_CODES:
        return Unauthorized(message, code="authentication_failed")
    if code in _AUTHORIZATION_CODES:
        return Forbidden(message, code="authorization_failed")
    if code in _TOPIC_NOT_FOUND_CODES:
        return NotFound(message, code="topic_not_found")
    if code == KafkaError.GROUP_ID_NOT_FOUND:
        return NotFound(message, code="group_not_found")
    if code == KafkaError.NON_EMPTY_GROUP:
        return Conflict(message, code="group_not_empty")
    if code == KafkaError._SSL:
        return BrokerError(message, code="tls_error")
    return BrokerError(message)


def map_kafka_exception(exc: BaseException) -> AppError:
    """Translate any exception raised by a Kafka call into an `AppError`.

    Messages are redacted of `password=...` values; callers must not chain the original exception
    (its text or args may embed client configuration).
    """
    if isinstance(exc, AppError):
        return exc
    if isinstance(exc, KafkaError):
        return _from_kafka_error(exc)
    if isinstance(exc, KafkaException) and exc.args and isinstance(exc.args[0], KafkaError):
        return _from_kafka_error(exc.args[0])
    if isinstance(exc, TimeoutError):  # concurrent.futures.TimeoutError is TimeoutError (3.11+)
        return KafkaTimeout("The Kafka operation timed out")
    return BrokerError(redact(str(exc)) or f"Kafka client error ({type(exc).__name__})")


def call_with_timeout[T](fn: Callable[[], T]) -> T:
    """Run a (bounded) Kafka call, re-raising any failure as a mapped `AppError`.

    The timeout itself is the caller's: pass `timeout=` to the client call or to `future.result`.
    """
    try:
        return fn()
    except Exception as exc:
        raise map_kafka_exception(exc) from None


def explain_connect_failure(failure: AppError, reported: Iterable[KafkaError]) -> AppError:
    """Replace a vague connect failure with the cause librdkafka reported via `error_cb`.

    `list_topics` against a broker that rejects the SASL password or presents an untrusted
    certificate only fails with `_TRANSPORT`; the real cause arrives as an error event.
    """
    if failure.code not in _VAGUE_FAILURE_CODES:
        return failure
    reported = list(reported)
    for codes in (_AUTHENTICATION_CODES, {KafkaError._SSL}):
        for err in reported:
            if err.code() in codes:
                return _from_kafka_error(err)
    for err in reversed(reported):
        if err.code() in (KafkaError._TRANSPORT, KafkaError._RESOLVE):
            detail = redact(err.str())
            return type(failure)(f"{failure.message}: {detail}", code=failure.code)
    return failure
