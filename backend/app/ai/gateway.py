from __future__ import annotations

import importlib.util
import os
from typing import Any

from loguru import logger

from app.ai.schemas import AIChatMessage, LLMGatewayStatus
from app.core.config import settings


class LLMGatewayError(RuntimeError):
    """Raised when the configured LLM provider cannot be used."""


def _env_key_configured(name: str) -> bool:
    return bool(name and os.getenv(name, "").strip())


def _model_supports_temperature(model: str) -> bool:
    normalized = model.split("/", 1)[-1].lower()
    return not normalized.startswith("gpt-5")


def _format_gateway_error(exc: Exception) -> str:
    message = str(exc)
    if "Invalid API key" in message:
        return "Invalid API key"
    if "<!doctype html" in message.lower() or "<html" in message.lower():
        return "LLM endpoint returned HTML instead of an OpenAI-compatible JSON response"
    return message[:1000]


class LLMGateway:
    """Small LiteLLM-backed gateway used by Copilot and legacy LLM strategy flows."""

    def __init__(self) -> None:
        self.provider = settings.ai_provider
        self.model = settings.ai_model
        self.api_key_env = settings.ai_api_key_env
        self.api_key = os.getenv(self.api_key_env, "").strip()
        self.base_url = settings.ai_base_url.strip()
        self.timeout_seconds = float(settings.ai_timeout_seconds)
        self.default_max_tokens = int(settings.ai_max_tokens)

    def status(self) -> LLMGatewayStatus:
        available = importlib.util.find_spec("litellm") is not None
        configured = _env_key_configured(self.api_key_env)
        error = None
        if not available:
            error = "litellm is not installed"
        elif not configured:
            error = f"{self.api_key_env} is not configured"
        return LLMGatewayStatus(
            available=available,
            configured=configured,
            provider=self.provider,
            model=self.model,
            api_key_env=self.api_key_env,
            api_key_configured=configured,
            timeout_seconds=self.timeout_seconds,
            max_tokens=self.default_max_tokens,
            error=error,
        )

    def is_ready(self) -> bool:
        status = self.status()
        return status.available and status.configured

    def chat(
        self,
        *,
        messages: list[AIChatMessage | dict[str, str]],
        system: str | None = None,
        model: str | None = None,
        temperature: float | None = None,
        max_tokens: int | None = None,
        extra_params: dict[str, Any] | None = None,
    ) -> str:
        status = self.status()
        if not status.available:
            raise LLMGatewayError(status.error or "litellm is not installed")
        if not status.configured:
            raise LLMGatewayError(status.error or f"{self.api_key_env} is not configured")

        try:
            from litellm import completion
        except Exception as exc:  # pragma: no cover - defensive import guard
            raise LLMGatewayError(f"Unable to import litellm: {exc}") from exc

        payload_messages: list[dict[str, str]] = []
        if system:
            payload_messages.append({"role": "system", "content": system})
        for message in messages:
            if isinstance(message, AIChatMessage):
                payload_messages.append({"role": message.role, "content": message.content})
            else:
                payload_messages.append(
                    {
                        "role": str(message.get("role") or "user"),
                        "content": str(message.get("content") or ""),
                    }
                )

        selected_model = model or self.model
        params: dict[str, Any] = {
            "model": selected_model,
            "messages": payload_messages,
            "max_tokens": max_tokens or self.default_max_tokens,
            "timeout": self.timeout_seconds,
        }
        if _model_supports_temperature(selected_model):
            params["temperature"] = settings.ai_temperature if temperature is None else temperature
        if self.base_url:
            params["base_url"] = self.base_url
        if self.api_key:
            params["api_key"] = self.api_key
        if extra_params:
            params.update(extra_params)

        logger.debug("Calling LLM gateway model={} messages={}", params["model"], len(payload_messages))
        try:
            response = completion(**params)
        except Exception as exc:
            message = _format_gateway_error(exc)
            logger.warning("LLM gateway call failed for model={}: {}", params["model"], message)
            raise LLMGatewayError(message) from exc
        return self._extract_text(response)

    @staticmethod
    def _extract_text(response: Any) -> str:
        if isinstance(response, dict):
            choices = response.get("choices") or []
            if choices:
                message = choices[0].get("message") or {}
                return str(message.get("content") or "")
        choices = getattr(response, "choices", None)
        if choices:
            first = choices[0]
            message = getattr(first, "message", None)
            if isinstance(message, dict):
                return str(message.get("content") or "")
            content = getattr(message, "content", None)
            if content is not None:
                return str(content)
        content = getattr(response, "content", None)
        if isinstance(content, str):
            return content
        return str(response)


_gateway: LLMGateway | None = None


def get_llm_gateway() -> LLMGateway:
    global _gateway
    if _gateway is None:
        _gateway = LLMGateway()
    return _gateway


def reset_llm_gateway() -> None:
    global _gateway
    _gateway = None
