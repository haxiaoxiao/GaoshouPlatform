from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator

AIToolRiskLevel = Literal["read", "write", "danger"]
AIToolStatus = Literal["ok", "error", "needs_confirmation"]
AIWorkflowStatus = Literal["planned", "completed", "needs_confirmation", "error"]


class AIChatMessage(BaseModel):
    role: Literal["system", "user", "assistant", "tool"]
    content: str


class AIActionCard(BaseModel):
    tool_name: str
    title: str
    description: str
    arguments: dict[str, Any] = Field(default_factory=dict)
    risk_level: AIToolRiskLevel = "read"
    requires_confirmation: bool = False
    route: str | None = None


class AIChatRequest(BaseModel):
    messages: list[AIChatMessage]
    page_context: dict[str, Any] | None = None
    auto_execute: bool = True


class AIChatResponse(BaseModel):
    message: AIChatMessage
    actions: list[AIActionCard] = Field(default_factory=list)
    executed_tools: list[dict[str, Any]] = Field(default_factory=list)
    trace: dict[str, Any] = Field(default_factory=dict)
    artifact_id: str | None = None
    model: str | None = None
    offline: bool = False


class LLMGatewayStatus(BaseModel):
    available: bool
    configured: bool
    provider: str
    model: str
    api_key_env: str
    api_key_configured: bool
    timeout_seconds: float
    max_tokens: int
    error: str | None = None


class AIToolDefinitionPublic(BaseModel):
    name: str
    title: str
    description: str
    category: str
    risk_level: AIToolRiskLevel = "read"
    requires_confirmation: bool = False
    input_schema: dict[str, Any]
    output_schema: dict[str, Any]


class AIToolExecutionRequest(BaseModel):
    arguments: dict[str, Any] = Field(default_factory=dict)
    confirmed: bool = False


class AIToolExecutionResponse(BaseModel):
    tool_name: str
    status: AIToolStatus
    summary: str
    result: dict[str, Any] | list[Any] | str | int | float | bool | None = None
    artifact_id: str | None = None
    task_id: str | None = None
    result_ref: str | None = None
    error: str | None = None


class AIWorkflowNodeTrace(BaseModel):
    name: str
    title: str
    status: str
    detail: str | None = None
    tool_name: str | None = None
    arguments: dict[str, Any] = Field(default_factory=dict)


class AIWorkflowDefinitionPublic(BaseModel):
    name: str
    title: str
    description: str
    category: str
    nodes: list[AIWorkflowNodeTrace]
    input_schema: dict[str, Any]


class AIWorkflowRunRequest(BaseModel):
    command: str | None = None
    messages: list[AIChatMessage] = Field(default_factory=list)
    page_context: dict[str, Any] | None = None
    arguments: dict[str, Any] = Field(default_factory=dict)
    auto_execute: bool = True
    dry_run: bool = False
    confirmed: bool = False


class AIWorkflowRunResponse(BaseModel):
    workflow_name: str
    status: AIWorkflowStatus
    summary: str
    nodes: list[AIWorkflowNodeTrace] = Field(default_factory=list)
    tool_results: list[dict[str, Any]] = Field(default_factory=list)
    pending_tools: list[dict[str, Any]] = Field(default_factory=list)
    result: dict[str, Any] = Field(default_factory=dict)
    artifact_id: str | None = None
    error: str | None = None


class AIStatusResponse(BaseModel):
    enabled: bool
    gateway: LLMGatewayStatus
    tool_count: int
    artifact_store: dict[str, Any]
    decisions: list[str]


class AIConfigResponse(BaseModel):
    enabled: bool
    provider: str
    model: str
    base_url: str | None = None
    api_key_env: str
    api_key_configured: bool
    api_key_masked: str | None = None
    api_key_source: str | None = None
    api_key_warning: str | None = None
    env_file: str
    requires_restart: bool = False
    updated_at: str
    gateway: LLMGatewayStatus


class AIConfigUpdate(BaseModel):
    enabled: bool | None = None
    provider: str | None = Field(default=None, min_length=1, max_length=50)
    model: str | None = Field(default=None, min_length=1, max_length=200)
    base_url: str | None = Field(default=None, max_length=1000)
    api_key_env: str | None = Field(default=None, min_length=2, max_length=100)
    api_key: str | None = Field(default=None, max_length=8192)
    clear_api_key: bool = False

    @field_validator("provider", "model", "base_url", "api_key_env", "api_key")
    @classmethod
    def _strip_optional_text(cls, value: str | None) -> str | None:
        if value is None:
            return None
        return value.strip()

    @field_validator("base_url")
    @classmethod
    def _validate_base_url(cls, value: str | None) -> str | None:
        if not value:
            return ""
        if not (value.startswith("http://") or value.startswith("https://")):
            raise ValueError("base_url must start with http:// or https://")
        if "\n" in value or "\r" in value:
            raise ValueError("base_url must be a single line")
        return value.rstrip("/")

    @field_validator("api_key_env")
    @classmethod
    def _validate_api_key_env(cls, value: str | None) -> str | None:
        if value is None:
            return None
        if not value.replace("_", "").isalnum() or not (value[0].isalpha() or value[0] == "_"):
            raise ValueError("api_key_env must be an environment variable name, e.g. OPENAI_API_KEY")
        return value

    @field_validator("api_key")
    @classmethod
    def _validate_api_key(cls, value: str | None) -> str | None:
        if value is None:
            return None
        if "\n" in value or "\r" in value:
            raise ValueError("api_key must be a single line")
        return value
