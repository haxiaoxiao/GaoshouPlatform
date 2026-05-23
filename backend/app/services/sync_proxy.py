from __future__ import annotations

from typing import Any

import httpx
from fastapi import HTTPException

from app.core.config import settings


def _sync_url(path: str) -> str:
    base = settings.sync_service_url.rstrip("/")
    suffix = path if path.startswith("/") else f"/{path}"
    return f"{base}{suffix}"


async def proxy_sync_request(
    method: str,
    path: str,
    *,
    json_body: Any | None = None,
    params: dict[str, Any] | None = None,
) -> dict[str, Any]:
    cleaned_params = (
        {key: value for key, value in params.items() if value is not None}
        if params
        else None
    )
    timeout = httpx.Timeout(connect=2.0, read=5.0, write=5.0, pool=2.0)
    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            response = await client.request(
                method,
                _sync_url(path),
                json=json_body,
                params=cleaned_params,
            )
    except httpx.RequestError as exc:
        raise HTTPException(status_code=503, detail="数据同步服务未启动") from exc

    if response.status_code >= 400:
        detail: Any
        try:
            detail = response.json().get("detail", response.text)
        except Exception:
            detail = response.text
        raise HTTPException(status_code=response.status_code, detail=detail)

    return response.json()


async def sync_service_health() -> dict[str, Any]:
    try:
        async with httpx.AsyncClient(timeout=2.0) as client:
            response = await client.get(_sync_url("/health"))
        if response.status_code >= 400:
            return {"healthy": False, "error": response.text}
        return {"healthy": True, "status": response.json()}
    except Exception as exc:
        return {"healthy": False, "error": str(exc)}
