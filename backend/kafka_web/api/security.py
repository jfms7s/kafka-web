"""Local-only request guards against DNS rebinding and cross-site requests.

The app binds 127.0.0.1, but a browser can still be steered at it: a rebinding page reaches it
under an attacker-controlled Host, and any page can send a cross-origin POST. Both would let a
page drive the API, e.g. `POST /api/clusters/test?existing=<name>` with an attacker bootstrap,
which sends the stored SASL password to the attacker. So: only local Host headers are served
(TrustedHostMiddleware), and unsafe methods with a non-local Origin are refused. Requests without
an Origin (curl, same-origin GETs) are allowed. Task 7's WebSocket applies `is_local_origin` too.
"""

from urllib.parse import urlsplit

from starlette.datastructures import Headers
from starlette.responses import JSONResponse
from starlette.types import ASGIApp, Receive, Scope, Send

LOCAL_HOSTS = ("127.0.0.1", "localhost")
UNSAFE_METHODS = frozenset({"POST", "PUT", "DELETE", "PATCH"})


def is_local_origin(origin: str | None) -> bool:
    """True for no Origin, or exactly `http(s)://127.0.0.1|localhost[:port]`."""
    if origin is None:
        return True
    try:
        parts = urlsplit(origin)
        parts.port  # noqa: B018 - raises ValueError for a malformed or out-of-range port
    except ValueError:
        return False
    return (
        parts.scheme in ("http", "https")
        and parts.hostname in LOCAL_HOSTS
        and parts.username is None
        and parts.password is None
        and not (parts.path or parts.query or parts.fragment)
    )


class LocalOriginMiddleware:
    """Refuse POST/PUT/DELETE/PATCH whose Origin header is present and not local (403)."""

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if (
            scope["type"] == "http"
            and scope["method"] in UNSAFE_METHODS
            and not is_local_origin(Headers(scope=scope).get("origin"))
        ):
            response = JSONResponse(
                status_code=403,
                content={
                    "code": "forbidden_origin",
                    "message": "Write requests from other origins are not allowed",
                },
            )
            await response(scope, receive, send)
            return
        await self.app(scope, receive, send)
