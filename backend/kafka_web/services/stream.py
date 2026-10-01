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

from confluent_kafka import OFFSET_END, Consumer, KafkaError, Message, TopicPartition
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
# Error events librdkafka recovers from by itself (it reconnects): a live stream keeps going.
# Everything else, including _ALL_BROKERS_DOWN, ends the stream with an error frame.
_TRANSIENT_ERRORS = {KafkaError._TRANSPORT}

logger = logging.getLogger(__name__)


class StreamParams(BaseModel):
    start: Literal["latest", "offset", "timestamp"] = "latest"  # latest = new messages only
    offset: int | None = None  # start == "offset": required, and so is `partition`
    timestamp: int | None = Field(None, ge=0)  # start == "timestamp": required (epoch ms)
    partition: int | None = None


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

    def run(self) -> None:
        try:
            consumer = call_with_timeout(lambda: self._consumer_factory(self._config()))
        except AppError as exc:
            self.error = exc
            return
        try:
            if self._assign(consumer):
                self._poll(consumer)
        except Exception as exc:
            self.error = map_kafka_exception(exc)
        finally:
            with contextlib.suppress(Exception):  # a failing close must not mask the outcome
                consumer.close()

    def _config(self) -> dict[str, Any]:
        # No EOF events: a live stream sits at the end of the log most of the time.
        return {**consumer_config(self._client_config), "enable.partition.eof": "false"}

    def _assign(self, consumer: Consumer) -> bool:
        """Assign the start positions; False when stopped during setup."""
        budget = Budget(SETUP_BUDGET_S, time.monotonic)
        params = self._params
        partitions = partition_ids(consumer, self._topic, params.partition, budget)
        if params.start == "latest":
            starts = dict.fromkeys(partitions, OFFSET_END)
        else:
            marks = fetch_watermarks(consumer, self._topic, partitions, budget)
            lookup = None
            if params.start == "timestamp":
                assert params.timestamp is not None  # validate_start
                lookup = lookup_timestamp_offsets(
                    consumer, self._topic, partitions, params.timestamp, budget
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
        tps = [TopicPartition(self._topic, p, offset) for p, offset in starts.items()]
        call_with_timeout(lambda: consumer.assign(tps))
        return True

    def _poll(self, consumer: Consumer) -> None:
        while not self.stop_event.is_set():
            msg = call_with_timeout(lambda: consumer.poll(self._poll_interval))
            if msg is None:
                continue
            err = msg.error()
            if err is None:
                self.queue.put(msg)
            elif err.code() in _TRANSIENT_ERRORS:
                logger.info("Live stream on %r: transient error %s", self._topic, err.name())
            elif err.code() != KafkaError._PARTITION_EOF:
                raise map_kafka_exception(err)
