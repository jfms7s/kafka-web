"""Live streaming (spec §4): a consumer thread feeding a bounded, drop-oldest queue.

The worker owns a throwaway-group consumer (same footprint rules as snapshot reads: `assign()`
only, never subscribe or commit) and polls until its stop event is set, queueing raw messages.
The WebSocket bridge (`api/stream.py`) drains, decodes and serialises them in a worker thread;
it never touches the consumer, so no librdkafka call or bulk decoding runs on the event loop.
"""

import contextlib
import logging
import threading
import time
from collections import deque
from collections.abc import Callable
from typing import Any, Literal

from confluent_kafka import (
    OFFSET_END,
    Consumer,
    KafkaError,
    KafkaException,
    Message,
    TopicPartition,
)
from pydantic import BaseModel, Field

from kafka_web.errors import AppError
from kafka_web.kafka.errors import call_with_timeout, map_kafka_exception
from kafka_web.services.consume import (
    SETUP_BUDGET_S,
    Budget,
    consumer_config,
    fetch_watermarks,
    lookup_timestamp_offsets,
    partition_ids,
)
from kafka_web.services.offsets import plan_start_offsets

QUEUE_SIZE = 1000
POLL_INTERVAL_S = 0.5
# Setup calls (metadata, watermarks, offsets-for-times) share SETUP_BUDGET_S, but each attempt
# waits at most SETUP_SLICE_S and timed-out attempts are retried: between slices the stop event
# is checked, so a client leaving during a slow setup is honoured within about a second.
SETUP_SLICE_S = 1.0
PROBE_TIMEOUT_S = 2.0  # how long an _ALL_BROKERS_DOWN report is double-checked with a metadata call
_RETRYABLE_SETUP_CODES = {"kafka_timeout", "broker_unreachable"}
# Client-level errors (broker connections, authentication) never come out of `poll()`: librdkafka
# hands them to the `error_cb`, which it calls while serving `poll()` on the worker thread. These
# end the stream; anything else (e.g. one `_TRANSPORT` disconnect, which librdkafka retries by
# itself) is only logged. A dead cluster thus surfaces as an error frame instead of a stream
# that silently stays "live".
#
# `_ALL_BROKERS_DOWN` is the exception: librdkafka also raises it while it is still working through
# a bootstrap server's addresses (`localhost` resolving to ::1, refused, before 127.0.0.1 connects
# over TLS), so it is only believed when a metadata probe fails as well.
_FATAL_CLIENT_ERRORS = {
    KafkaError._ALL_BROKERS_DOWN,
    KafkaError.SASL_AUTHENTICATION_FAILED,
    KafkaError._AUTHENTICATION,
    KafkaError.TOPIC_AUTHORIZATION_FAILED,
    KafkaError.GROUP_AUTHORIZATION_FAILED,
    KafkaError.CLUSTER_AUTHORIZATION_FAILED,
}

logger = logging.getLogger(__name__)


class StreamParams(BaseModel):
    start: Literal["latest", "offset", "timestamp"] = "latest"  # latest = new messages only
    offset: int | None = None  # start == "offset": required, and so is `partition`
    timestamp: int | None = Field(None, ge=0)  # start == "timestamp": required (epoch ms)
    partition: int | None = None


class _Stopped(Exception):
    """The stop event was set during setup: not a failure."""


class _SlicedBudget(Budget):
    """The setup budget, handed out in slices of at most SETUP_SLICE_S; stops when told to."""

    def __init__(self, stop: threading.Event):
        super().__init__(SETUP_BUDGET_S, time.monotonic)
        self._stop = stop

    def call_timeout(self) -> float:
        if self._stop.is_set():
            raise _Stopped
        return min(SETUP_SLICE_S, super().call_timeout())


class BoundedDropQueue[T]:
    """Thread-safe FIFO of at most `maxsize` items; when full, `put` drops the oldest.

    The worker queues raw consumer messages: decoding is left to whoever drains, so the messages
    dropped on overflow (most of them, when replaying a backlog) are never decoded.
    """

    def __init__(self, maxsize: int = QUEUE_SIZE):
        self._items: deque[T] = deque()
        self._maxsize = maxsize
        self._dropped = 0
        self._lock = threading.Lock()

    def put(self, item: T) -> None:
        with self._lock:
            if len(self._items) >= self._maxsize:
                self._items.popleft()
                self._dropped += 1
            self._items.append(item)

    def drain(self, max_items: int = 100) -> tuple[list[T], int]:
        """Up to `max_items` oldest items, and how many were dropped since the last drain."""
        with self._lock:
            count = min(max_items, len(self._items))
            items = [self._items.popleft() for _ in range(count)]
            dropped, self._dropped = self._dropped, 0
        return items, dropped


class StreamWorker(threading.Thread):
    """Polls one topic until `stop` is set; `error` holds the mapped failure if it died."""

    def __init__(
        self,
        client_config: dict[str, str],
        topic: str,
        params: StreamParams,
        queue: BoundedDropQueue[Message],
        stop: threading.Event,
        consumer_factory: Callable[[dict[str, Any]], Consumer] = Consumer,
        poll_interval: float = POLL_INTERVAL_S,
    ):
        super().__init__(name=f"kafka-web-stream-{topic}", daemon=True)
        self._client_config = client_config
        self._topic = topic
        self._params = params
        self.queue = queue
        self.stop_event = stop
        self._consumer_factory = consumer_factory
        self._poll_interval = poll_interval
        self.error: AppError | None = None
        self._client_error: KafkaError | None = None  # first fatal error_cb report

    def run(self) -> None:
        try:
            consumer = call_with_timeout(lambda: self._consumer_factory(self._config()))
        except AppError as exc:
            self.error = exc
            return
        try:
            if self._assign(consumer):
                self._poll(consumer)
        except _Stopped:
            pass
        except Exception as exc:
            self.error = map_kafka_exception(exc)
        finally:
            with contextlib.suppress(Exception):  # a failing close must not mask the outcome
                consumer.close()

    def _config(self) -> dict[str, Any]:
        # No EOF events: a live stream sits at the end of the log most of the time.
        return {
            **consumer_config(self._client_config),
            "enable.partition.eof": "false",
            "error_cb": self._on_client_error,
        }

    def _on_client_error(self, err: KafkaError) -> None:
        if err.fatal() or err.code() in _FATAL_CLIENT_ERRORS:
            if self._client_error is None:
                self._client_error = err
        else:
            logger.info("Live stream on %r: client error %s", self._topic, err.name())

    def _assign(self, consumer: Consumer) -> bool:
        """Assign the start positions; False (or `_Stopped`) when stopped during setup."""
        budget = _SlicedBudget(self.stop_event)
        params, topic = self._params, self._topic
        partitions = self._retrying(
            lambda b: partition_ids(consumer, topic, params.partition, b), budget
        )
        if params.start == "latest":
            starts = dict.fromkeys(partitions, OFFSET_END)
        else:
            marks = self._retrying(
                lambda b: fetch_watermarks(consumer, topic, partitions, b), budget
            )
            lookup = None
            if params.start == "timestamp":
                timestamp = params.timestamp
                assert timestamp is not None  # validate_start
                lookup = self._retrying(
                    lambda b: lookup_timestamp_offsets(consumer, topic, partitions, timestamp, b),
                    budget,
                )
            plan = plan_start_offsets(
                params.start, marks, 0, offset=params.offset, timestamp_offsets=lookup
            )
            # The plan omits partitions with nothing to read yet; a live stream still follows
            # them, from the high watermark captured above (not OFFSET_END: that could skip
            # messages produced since).
            starts = {p: plan.get(p, high) for p, (_, high) in marks.items()}
        if self.stop_event.is_set():
            return False
        tps = [TopicPartition(topic, p, offset) for p, offset in starts.items()]
        call_with_timeout(lambda: consumer.assign(tps))
        return True

    def _retrying[T](self, call: Callable[[Budget], T], budget: _SlicedBudget) -> T:
        """Run one setup step, retrying timeouts / unreachable brokers until the budget is spent.

        Raises `_Stopped` as soon as a stop is noticed (between slices, at most about a slice).
        """
        while True:
            began = time.monotonic()
            try:
                return call(budget)
            except AppError as exc:
                if exc.code not in _RETRYABLE_SETUP_CODES:
                    raise
                failure = exc
            # Sit out the rest of the slice, so an instantly failing call does not spin; a stop
            # ends the wait at once.
            if self.stop_event.wait(max(0.0, SETUP_SLICE_S - (time.monotonic() - began))):
                raise _Stopped
            if budget.remaining() <= 0:
                raise failure  # the last real cause, not just "out of time"

    def _confirm_client_error(self, consumer: Consumer) -> None:
        """Raise the reported client error, unless it was a transient all-brokers-down."""
        error = self._client_error
        assert error is not None
        if error.code() == KafkaError._ALL_BROKERS_DOWN:
            try:
                consumer.list_topics(self._topic, timeout=PROBE_TIMEOUT_S)
            except KafkaException:
                pass  # the brokers really are unreachable
            else:
                self._client_error = None
                return
        raise map_kafka_exception(error)

    def _poll(self, consumer: Consumer) -> None:
        while not self.stop_event.is_set():
            msg = call_with_timeout(lambda: consumer.poll(self._poll_interval))
            if self._client_error is not None:  # reported to the error_cb during that poll
                self._confirm_client_error(consumer)
            if msg is None:
                continue
            err = msg.error()  # a per-partition consumer error (unknown topic, ACL, ...)
            if err is None:
                self.queue.put(msg)
            elif err.code() != KafkaError._PARTITION_EOF:
                raise map_kafka_exception(err)
