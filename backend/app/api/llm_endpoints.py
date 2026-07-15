from __future__ import annotations

import logging
from datetime import datetime
from time import perf_counter
from typing import Any, Literal

from fastapi import APIRouter, Depends, HTTPException, Response, status
from pydantic import BaseModel, ConfigDict, Field, model_validator
from sqlalchemy.ext.asyncio import AsyncSession

from app.ai.gateway import GatewayCandidate, complete_candidate, sanitize_error
from app.db.models.llm_endpoint import LlmEndpoint
from app.db.sqlite import get_async_session
from app.services.llm_endpoints import LlmEndpointService

router = APIRouter()
logger = logging.getLogger(__name__)


class LlmEndpointCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    config: dict[str, Any] | None = None
    name: str | None = Field(default=None, min_length=1, max_length=100)
    api_base: str | None = Field(default=None, min_length=8, max_length=500)
    api_key: str | None = Field(default=None, min_length=1, max_length=2000)
    model: str | None = Field(default=None, min_length=1, max_length=200)
    enabled: bool = True

    @model_validator(mode="after")
    def validate_request_shape(self) -> LlmEndpointCreate:
        legacy = (self.name, self.api_base, self.api_key, self.model)
        if self.config is not None:
            if any(value is not None for value in legacy):
                raise ValueError("provide config or all legacy fields, not both")
            return self
        if any(value is None for value in legacy):
            raise ValueError("provide config or all legacy fields")
        return self


class LlmEndpointUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    config: dict[str, Any] | None = None
    name: str | None = Field(default=None, min_length=1, max_length=100)
    api_base: str | None = Field(default=None, min_length=8, max_length=500)
    api_key: str | None = Field(default=None, max_length=2000)
    model: str | None = Field(default=None, min_length=1, max_length=200)
    enabled: bool | None = None

    @model_validator(mode="after")
    def validate_request_shape(self) -> LlmEndpointUpdate:
        discrete_fields = {"name", "api_base", "api_key", "model"}
        supplied_discrete_fields = self.model_fields_set & discrete_fields
        if self.config is not None:
            if supplied_discrete_fields:
                raise ValueError("config cannot be mixed with legacy fields")
            return self
        if not (self.model_fields_set - {"config"}):
            raise ValueError("update must not be empty")
        return self


class LlmEndpointRead(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str
    name: str
    api_base: str
    api_key_hint: str
    model: str
    provider: str
    review_model: str | None
    wire_api: Literal["responses", "chat_completions"]
    reasoning_effort: Literal["minimal", "low", "medium", "high", "xhigh"] | None
    disable_response_storage: bool
    requires_openai_auth: bool
    config: dict[str, Any]
    preserved_fields: list[str]
    priority: int
    enabled: bool
    consecutive_failures: int
    cooldown_until: datetime | None
    last_success_at: datetime | None
    last_failure_at: datetime | None
    last_error: str | None
    created_at: datetime
    updated_at: datetime


class LlmEndpointReorder(BaseModel):
    endpoint_ids: list[str]


class LlmEndpointTestResult(BaseModel):
    status: Literal["ok", "error"]
    latency_ms: int = Field(ge=0)
    model: str | None = None
    error: str | None = None


def _read(endpoint: LlmEndpoint) -> LlmEndpointRead:
    return LlmEndpointRead.model_validate(LlmEndpointService.serialize(endpoint))


def _service_error(error: ValueError, *, conflict: bool = False) -> HTTPException:
    detail = str(error)
    if "not found" in detail:
        return HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=detail)
    return HTTPException(
        status_code=status.HTTP_409_CONFLICT if conflict else status.HTTP_422_UNPROCESSABLE_CONTENT,
        detail=detail,
    )


async def _rollback_failed_health_write(
    service: LlmEndpointService,
    endpoint_id: str,
    *,
    api_key: str,
    operation: str,
    health_error: Exception,
) -> None:
    try:
        await service.session.rollback()
    except Exception as rollback_error:
        logger.warning(
            "LLM endpoint test health rollback failed endpoint_id=%s operation=%s error=%s",
            endpoint_id,
            operation,
            sanitize_error(rollback_error, api_key),
        )
    logger.warning(
        "LLM endpoint test health write failed endpoint_id=%s operation=%s error=%s",
        endpoint_id,
        operation,
        sanitize_error(health_error, api_key),
    )


async def _try_mark_failure(
    service: LlmEndpointService,
    endpoint_id: str,
    safe_error: str,
    *,
    api_key: str,
) -> None:
    try:
        await service.mark_failure(endpoint_id, safe_error)
    except Exception as health_error:
        await _rollback_failed_health_write(
            service,
            endpoint_id,
            api_key=api_key,
            operation="failure",
            health_error=health_error,
        )


async def _try_mark_success(
    service: LlmEndpointService,
    endpoint_id: str,
    *,
    api_key: str,
) -> None:
    try:
        await service.mark_success(endpoint_id)
    except Exception as health_error:
        await _rollback_failed_health_write(
            service,
            endpoint_id,
            api_key=api_key,
            operation="success",
            health_error=health_error,
        )


@router.get("", response_model=list[LlmEndpointRead])
async def list_llm_endpoints(session: AsyncSession = Depends(get_async_session)):
    return [_read(endpoint) for endpoint in await LlmEndpointService(session).list()]


@router.post("", response_model=LlmEndpointRead, status_code=status.HTTP_201_CREATED)
async def create_llm_endpoint(
    payload: LlmEndpointCreate,
    session: AsyncSession = Depends(get_async_session),
):
    service = LlmEndpointService(session)
    try:
        if payload.config is not None:
            endpoint = await service.create_from_config(payload.config, enabled=payload.enabled)
        else:
            endpoint = await service.create(**payload.model_dump(exclude={"config"}))
    except ValueError as error:
        raise _service_error(error) from None
    return _read(endpoint)


@router.post("/reorder", response_model=list[LlmEndpointRead])
async def reorder_llm_endpoints(
    payload: LlmEndpointReorder,
    session: AsyncSession = Depends(get_async_session),
):
    try:
        endpoints = await LlmEndpointService(session).reorder(payload.endpoint_ids)
    except ValueError as error:
        raise _service_error(error, conflict=True) from None
    return [_read(endpoint) for endpoint in endpoints]


@router.patch("/{endpoint_id}", response_model=LlmEndpointRead)
async def update_llm_endpoint(
    endpoint_id: str,
    payload: LlmEndpointUpdate,
    session: AsyncSession = Depends(get_async_session),
):
    service = LlmEndpointService(session)
    try:
        if payload.config is not None:
            endpoint = await service.update_from_config(
                endpoint_id,
                payload.config,
                enabled=payload.enabled,
            )
        else:
            endpoint = await service.update(
                endpoint_id,
                **payload.model_dump(exclude={"config"}, exclude_unset=True),
            )
    except ValueError as error:
        raise _service_error(error) from None
    return _read(endpoint)


@router.delete("/{endpoint_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_llm_endpoint(
    endpoint_id: str,
    session: AsyncSession = Depends(get_async_session),
) -> Response:
    try:
        await LlmEndpointService(session).delete(endpoint_id)
    except ValueError as error:
        raise _service_error(error) from None
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.post("/{endpoint_id}/test", response_model=LlmEndpointTestResult)
async def test_llm_endpoint(
    endpoint_id: str,
    session: AsyncSession = Depends(get_async_session),
):
    service = LlmEndpointService(session)
    try:
        endpoint = await session.get(LlmEndpoint, endpoint_id)
        if endpoint is None:
            raise ValueError(f"LLM endpoint {endpoint_id} not found")
        api_key = await service.decrypt_api_key(endpoint_id)
    except ValueError as error:
        raise _service_error(error) from None

    candidate = GatewayCandidate(
        endpoint_id=endpoint.id,
        name=endpoint.name,
        api_base=endpoint.api_base,
        api_key=api_key,
        model=endpoint.model,
        source="database",
        review_model=endpoint.review_model,
        provider=endpoint.provider,
        wire_api=endpoint.wire_api,
        reasoning_effort=endpoint.reasoning_effort,
        disable_response_storage=endpoint.disable_response_storage,
        requires_openai_auth=endpoint.requires_openai_auth,
    )
    started = perf_counter()
    result = None
    safe_error = None
    try:
        result = await complete_candidate(
            candidate,
            [{"role": "user", "content": "Reply with OK."}],
            temperature=0,
            max_tokens=1,
        )
    except Exception as error:
        safe_error = sanitize_error(error, api_key)

    if safe_error is not None:
        await _try_mark_failure(service, endpoint_id, safe_error, api_key=api_key)
        return LlmEndpointTestResult(
            status="error",
            latency_ms=max(0, round((perf_counter() - started) * 1000)),
            error=safe_error,
        )
    await _try_mark_success(service, endpoint_id, api_key=api_key)
    assert result is not None
    return LlmEndpointTestResult(
        status="ok",
        latency_ms=max(0, round((perf_counter() - started) * 1000)),
        model=result.model or endpoint.model,
    )
