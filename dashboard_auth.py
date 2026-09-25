from __future__ import annotations

import base64
import hmac
import os

from fastapi import Request
from fastapi.responses import JSONResponse, Response

_REMOTE_AUTH_ENABLED = os.getenv("DASHBOARD_REMOTE_AUTH_ENABLED", "true").strip().lower() in {"1","true","yes","on"}
_USER = os.getenv("DASHBOARD_BASIC_USER", "").strip()
_PASSWORD = os.getenv("DASHBOARD_BASIC_PASSWORD", "").strip()


def _is_remote_dashboard_request(request: Request) -> bool:
    raw_host = (request.headers.get("host") or "").strip().lower()
    host = raw_host
    if raw_host.startswith("[") and "]" in raw_host:
        host = raw_host[1:raw_host.index("]")]
    elif ":" in raw_host:
        host = raw_host.split(":", 1)[0]

    # Every non-loopback request is remote and must pass dashboard auth.
    return host not in {"127.0.0.1", "localhost", "::1"}

def _authorized(request: Request) -> bool:
    auth = request.headers.get("authorization") or ""
    if not auth.lower().startswith("basic "):
        return False
    try:
        raw = base64.b64decode(auth.split(" ", 1)[1]).decode("utf-8")
        username, password = raw.split(":", 1)
    except Exception:
        return False
    return hmac.compare_digest(username, _USER) and hmac.compare_digest(password, _PASSWORD)


async def dashboard_remote_auth_middleware(request: Request, call_next):
    if not _REMOTE_AUTH_ENABLED or not _is_remote_dashboard_request(request):
        return await call_next(request)

    # The ChatGPT bridge carries its own strong bearer-token authentication.
    if request.url.path.startswith("/api/chatgpt/"):
        return await call_next(request)

    if len(_USER) < 3 or len(_PASSWORD) < 16:
        return JSONResponse(
            {"ok": False, "error": "Remote dashboard authentication is not configured safely"},
            status_code=503,
        )

    if not _authorized(request):
        return Response(
            status_code=401,
            headers={"WWW-Authenticate": 'Basic realm="Stock Alert Dashboard", charset="UTF-8"'},
        )

    return await call_next(request)
