"""FastAPI application factory: wiring, error handlers and lifecycle."""

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from starlette.concurrency import run_in_threadpool
from starlette.exceptions import HTTPException as StarletteHTTPException
from starlette.middleware.trustedhost import TrustedHostMiddleware

from kafka_web.api import clusters
from kafka_web.api.security import LOCAL_HOSTS, LocalOriginMiddleware
from kafka_web.config.paths import config_dir
from kafka_web.config.secrets import SecretStore
from kafka_web.config.store import ClusterStore
from kafka_web.errors import AppError
from kafka_web.kafka.registry import ConnectionRegistry

logger = logging.getLogger(__name__)

_HTTP_CODES = {404: "not_found", 405: "method_not_allowed"}


def _error(status: int, code: str, message: str, field: str | None = None) -> JSONResponse:
    body = {"code": code, "message": message}
    if field is not None:
        body["field"] = field
    return JSONResponse(status_code=status, content=body)


async def _app_error(request: Request, exc: Exception) -> JSONResponse:
    assert isinstance(exc, AppError)
    return JSONResponse(status_code=exc.status, content=exc.to_body())


async def _request_validation_error(request: Request, exc: Exception) -> JSONResponse:
    """FastAPI's own parsing errors (malformed JSON, wrong param types) in the app's error shape.

    The offending input is never echoed: it may be a secret.
    """
    assert isinstance(exc, RequestValidationError)
    errors = exc.errors()
    if not errors:
        return _error(422, "validation_failed", "Invalid request")
    first = errors[0]
    if first.get("type") == "json_invalid":
        return _error(422, "validation_failed", "Request body is not valid JSON")
    path = [str(part) for part in first.get("loc", ())[1:]]  # drop "body" / "query" / "path"
    field = ".".join(path) or None
    message = f"{field}: {first['msg']}" if field else str(first["msg"])
    return _error(422, "validation_failed", message, field)


async def _http_error(request: Request, exc: Exception) -> JSONResponse:
    assert isinstance(exc, StarletteHTTPException)
    response = _error(exc.status_code, _HTTP_CODES.get(exc.status_code, "http_error"), exc.detail)
    response.headers.update(exc.headers or {})
    return response


async def _unexpected_error(request: Request, exc: Exception) -> JSONResponse:
    logger.error("Unhandled %s on %s %s", type(exc).__name__, request.method, request.url.path)
    return _error(500, "internal_error", "Internal server error")


def create_app(
    *,
    store: ClusterStore | None = None,
    registry: ConnectionRegistry | None = None,
    static_dir: Path | None = None,
) -> FastAPI:
    store = store if store is not None else ClusterStore(config_dir(), SecretStore())
    registry = registry if registry is not None else ConnectionRegistry(store)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        yield
        await run_in_threadpool(registry.close_all)  # flushes producers: blocking

    app = FastAPI(title="kafka-web", lifespan=lifespan)
    app.state.store = store
    app.state.registry = registry

    # Outermost first: a foreign Host is refused before the Origin check or any route runs.
    app.add_middleware(LocalOriginMiddleware)
    app.add_middleware(TrustedHostMiddleware, allowed_hosts=list(LOCAL_HOSTS))

    app.add_exception_handler(AppError, _app_error)
    app.add_exception_handler(RequestValidationError, _request_validation_error)
    app.add_exception_handler(StarletteHTTPException, _http_error)
    app.add_exception_handler(Exception, _unexpected_error)

    app.include_router(clusters.router, prefix="/api")
    if static_dir is not None:
        app.mount("/", StaticFiles(directory=static_dir, html=True), name="static")
    return app
