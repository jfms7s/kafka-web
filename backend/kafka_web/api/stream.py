"""Live stream WebSocket (spec §4): bridges a `StreamWorker` thread to the browser.

Frames sent: `{"type": "messages", "items": [...]}` (at most 100 per frame, flushed at least every
100 ms), `{"type": "dropped", "count": n}` (the bounded queue overflowed), `{"type": "error",
"code", "message"[, "field"]}` then close, and `{"type": "closed", "reason": "cluster_changed"}`
then close 1001 when the cluster is edited, deleted or disconnected. The client may send the
text `"stop"`; any other text is ignored.

The event loop never makes a librdkafka call: the worker thread owns the consumer, the bridge only
drains the queue, and the blocking registry/join calls run in worker threads (`to_thread`).
"""

import asyncio
import contextlib
import logging
import threading
from dataclasses import dataclass
from typing import Any

from fastapi import APIRouter, WebSocket, WebSocketDisconnect, status
from pydantic import ValidationError

from kafka_web.api.security import is_local_origin
from kafka_web.errors import AppError, BrokerError, ValidationFailed
from kafka_web.kafka.registry import ConnectionRegistry
from kafka_web.services.consume import validate_start
from kafka_web.services.stream import BoundedDropQueue, StreamParams, StreamWorker

router = APIRouter()
logger = logging.getLogger(__name__)

BATCH_SIZE = 100
FLUSH_INTERVAL_S = 0.1
JOIN_TIMEOUT_S = 2.0

_CLOSE_NORMAL = status.WS_1000_NORMAL_CLOSURE
_CLOSE_GOING_AWAY = status.WS_1001_GOING_AWAY
_CLOSE_POLICY = status.WS_1008_POLICY_VIOLATION
_CLOSE_ERROR = status.WS_1011_INTERNAL_ERROR


@dataclass(frozen=True)
class _End:
    """How a stream ends: an optional last frame, then an optional close code (None: the
    client is already gone)."""

    frame: dict[str, Any] | None = None
    code: int | None = None


_CLIENT_STOPPED = _End(code=_CLOSE_NORMAL)
_CLIENT_GONE = _End()
_CLUSTER_CHANGED = _End({"type": "closed", "reason": "cluster_changed"}, _CLOSE_GOING_AWAY)


def _error_end(exc: AppError, code: int) -> _End:
    return _End({"type": "error", **exc.to_body()}, code)


def _parse(
    start: str, offset: int | None, timestamp: int | None, partition: int | None
) -> StreamParams:
    raw = {"start": start, "offset": offset, "timestamp": timestamp, "partition": partition}
    try:
        params = StreamParams.model_validate(raw)
    except ValidationError as exc:  # the message never echoes the input
        first = exc.errors()[0]
        field = ".".join(str(part) for part in first["loc"]) or None
        raise ValidationFailed(f"{field}: {first['msg']}", field=field) from None
    validate_start(params)
    return params


@router.websocket("/clusters/{name}/topics/{topic}/stream")
async def stream(
    ws: WebSocket,
    name: str,
    topic: str,
    start: str = "latest",
    offset: int | None = None,
    timestamp: int | None = None,
    partition: int | None = None,
) -> None:
    # The HTTP Origin middleware does not see WebSockets, and browsers let any page open one:
    # refuse a foreign page before accepting (the handshake then fails with 403).
    if not is_local_origin(ws.headers.get("origin")):
        await ws.close(code=_CLOSE_POLICY)
        return
    await ws.accept()
    registry: ConnectionRegistry = ws.app.state.registry
    try:
        params = _parse(start, offset, timestamp, partition)
        await asyncio.to_thread(registry.get, name)  # connects if needed: 404/409/502/504
        client_config = await asyncio.to_thread(registry.client_config, name)
    except AppError as exc:
        await _finish(ws, _error_end(exc, _CLOSE_POLICY if exc.status < 500 else _CLOSE_ERROR))
        return
    worker = StreamWorker(
        client_config,
        topic,
        params,
        BoundedDropQueue(),
        threading.Event(),
        consumer_factory=ws.app.state.consumer_factory,
    )
    end = await _run(ws, registry, name, worker)
    await _finish(ws, end)


async def _run(
    ws: WebSocket, registry: ConnectionRegistry, name: str, worker: StreamWorker
) -> _End:
    """Run the worker for as long as the stream lives; always stops, joins and unregisters it
    before returning, so the final frame/close reaches the client after the cleanup."""
    stop, queue = worker.stop_event, worker.queue
    # Brief lock acquisitions only, safe on the loop. Sets `stop` at once when the connection
    # was closed (edit/delete/disconnect) since `registry.get` above.
    registry.register_stream(name, stop)
    try:
        if stop.is_set():
            return _CLUSTER_CHANGED
        worker.start()
        return await _pump(ws, queue, worker, stop)
    finally:
        stop.set()  # the worker exits within one poll interval and closes its consumer
        try:
            if worker.ident is not None:  # started
                # Cancelled handlers (server shutdown) skip the wait; the daemon still exits.
                await asyncio.to_thread(worker.join, JOIN_TIMEOUT_S)
                if worker.is_alive():
                    logger.warning("Live stream worker %s did not stop in time", worker.name)
        finally:
            registry.unregister_stream(name, stop)


async def _pump(
    ws: WebSocket, queue: BoundedDropQueue, worker: StreamWorker, stop: threading.Event
) -> _End:
    receiver = asyncio.create_task(_receive(ws, stop))
    try:
        while True:
            alive = worker.is_alive()  # before draining: a dead worker's queue is final
            items, dropped = queue.drain(BATCH_SIZE)
            if dropped:
                await ws.send_json({"type": "dropped", "count": dropped})
            if items:
                frame = {"type": "messages", "items": [m.model_dump(mode="json") for m in items]}
                await ws.send_json(frame)
            if receiver.done():
                return receiver.result()
            if stop.is_set():  # set by the registry: the cluster was edited/deleted/disconnected
                return _CLUSTER_CHANGED
            if not alive and len(items) < BATCH_SIZE:
                error = worker.error or BrokerError("The live stream ended unexpectedly")
                return _error_end(error, _CLOSE_ERROR)
            if len(items) < BATCH_SIZE:  # a full batch is flushed again right away
                await asyncio.wait([receiver], timeout=FLUSH_INTERVAL_S)
    except WebSocketDisconnect:
        stop.set()
        return _CLIENT_GONE
    finally:
        receiver.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await receiver


async def _receive(ws: WebSocket, stop: threading.Event) -> _End:
    """Wait for the client's `"stop"` or its disconnect; either stops the worker at once."""
    while True:
        message = await ws.receive()
        if message["type"] == "websocket.disconnect":
            stop.set()
            return _CLIENT_GONE
        if message.get("text") == "stop":
            stop.set()
            return _CLIENT_STOPPED


async def _finish(ws: WebSocket, end: _End) -> None:
    """Send the last frame and close; the client may already be gone, which is fine."""
    with contextlib.suppress(WebSocketDisconnect, RuntimeError):
        if end.frame is not None:
            await ws.send_json(end.frame)
        if end.code is not None:
            await ws.close(code=end.code)
