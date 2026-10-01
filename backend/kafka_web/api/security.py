"""Local-only request guards against DNS rebinding and cross-site requests.

The app binds 127.0.0.1, but a browser can still be steered at it: a rebinding page reaches it
under an attacker-controlled Host, and any page can send a cross-origin POST or open a WebSocket.
Both would let a page drive the API, e.g. `POST /api/clusters/test?existing=<name>` with an
attacker bootstrap, which sends the stored SASL password to the attacker, or read a live stream.
So: only local Host headers are served (TrustedHostMiddleware), and unsafe methods and WebSocket
handshakes are refused unless their Origin is the request's own origin. "Local" is not enough:
another local process (a dev server, a notebook) on another port is another origin. Requests
without an Origin (curl, same-origin GETs) are allowed.
"""

from urllib.parse import SplitResult, urlsplit

from starlette.datastructures import Headers
from starlette.responses import JSONResponse
from starlette.types import ASGIApp, Receive, Scope, Send

LOCAL_HOSTS = ("127.0.0.1", "localhost")
UNSAFE_METHODS = frozenset({"POST", "PUT", "DELETE", "PATCH"})

# ASGI scope scheme -> the web origin scheme a same-origin page has
_ORIGIN_SCHEMES = {"http": "http", "ws": "http", "https": "https", "wss": "https"}
_DEFAULT_PORTS = {"http": 80, "https": 443}


def _host_port(parts: SplitResult, scheme: str) -> tuple[str, int] | None:
    """`(lowercased host, port with the scheme's default filled in)`; None if malformed."""
    try:
        port = parts.port  # raises ValueError for a malformed or out-of-range port
    except ValueError:
        return None
    if parts.hostname is None:
        return None
    return parts.hostname, port if port is not None else _DEFAULT_PORTS[scheme]


def is_same_origin(origin: str | None, host: str | None, scheme: str) -> bool:
    """True for no Origin, or an Origin equal to the request's own `scheme://host:port`.

    `host` is the request's Host header and `scheme` its ASGI scope scheme (`http`, `https`,
    `ws`, `wss`). Default ports are normalised; `localhost` and `127.0.0.1` stay distinct, as
    they are for the browser. The origin must also be a local host (defence in depth).
    """
    if origin is None:
        return True
    expected_scheme = _ORIGIN_SCHEMES.get(scheme)
    if expected_scheme is None or host is None:
        return False
    parts = urlsplit(origin)
    if (
        parts.scheme.lower() != expected_scheme
        or parts.username is not None
        or parts.password is not None
        or parts.path
        or parts.query
        or parts.fragment
    ):
        return False
    theirs = _host_port(parts, expected_scheme)
    ours = _host_port(urlsplit(f"//{host}"), expected_scheme)
    return theirs is not None and theirs == ours and theirs[0] in LOCAL_HOSTS


def request_is_same_origin(scope: Scope) -> bool:
    headers = Headers(scope=scope)
    return is_same_origin(headers.get("origin"), headers.get("host"), scope["scheme"])


class LocalOriginMiddleware:
    """Refuse POST/PUT/DELETE/PATCH whose Origin header is present and not this origin (403)."""

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if (
            scope["type"] == "http"
            and scope["method"] in UNSAFE_METHODS
            and not request_is_same_origin(scope)
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
