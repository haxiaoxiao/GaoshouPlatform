from __future__ import annotations

import os
from collections.abc import Callable
from ipaddress import ip_address
from secrets import compare_digest

from fastapi import FastAPI, HTTPException, Request, status

_PROCESS_ID_HEADER = "X-Gaoshou-Process-ID"


def _is_loopback(host: str | None) -> bool:
    if not host:
        return False
    try:
        return ip_address(host).is_loopback
    except ValueError:
        return False


def install_shutdown_endpoint(
    app: FastAPI,
    *,
    request_shutdown: Callable[[], None],
    process_id: int | None = None,
) -> None:
    """Install the local process-control endpoint used by the Windows stop script."""

    expected_process_id = str(os.getpid() if process_id is None else process_id)

    @app.post("/internal/shutdown", status_code=status.HTTP_202_ACCEPTED, include_in_schema=False)
    async def shutdown_service(request: Request) -> dict[str, str]:
        client_host = request.client.host if request.client is not None else None
        claimed_process_id = request.headers.get(_PROCESS_ID_HEADER, "")
        if not _is_loopback(client_host) or not compare_digest(
            claimed_process_id,
            expected_process_id,
        ):
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="forbidden")
        request_shutdown()
        return {"status": "shutdown_requested"}
