from __future__ import annotations

from datetime import datetime
from typing import Any

from app.ai.tools import get_ai_tool_registry
from app.core.config import settings


def _tool_payload(tool: Any) -> dict[str, Any]:
    public = tool.public().model_dump()
    return {
        "name": public["name"],
        "title": public["title"],
        "description": public["description"],
        "category": public["category"],
        "risk_level": public["risk_level"],
        "requires_confirmation": public["requires_confirmation"],
        "input_schema": public["input_schema"],
        "http": {
            "execute": f"{settings.api_prefix}/ai/tools/{public['name']}/execute",
            "method": "POST",
            "body_shape": {"arguments": "object", "confirmed": "boolean"},
        },
        "mcp": {
            "name": public["name"],
            "call_shape": {"arguments": "object", "confirmed": "boolean"},
        },
    }


def build_ai_tool_manifest() -> dict[str, Any]:
    """Return the stable contract shared by Copilot, HTTP tools and MCP."""
    from app.ai.workflows import list_ai_workflows

    tools = get_ai_tool_registry().list()
    workflows = [workflow.model_dump() for workflow in list_ai_workflows()]
    categories: dict[str, int] = {}
    risk_levels: dict[str, int] = {}
    confirmation_required = 0
    for tool in tools:
        categories[tool.category] = categories.get(tool.category, 0) + 1
        risk_levels[tool.risk_level] = risk_levels.get(tool.risk_level, 0) + 1
        if tool.requires_confirmation:
            confirmation_required += 1

    return {
        "schema_version": "gaoshou-ai-native-tool-manifest/v1",
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "server": {
            "name": "GaoshouPlatform AI Native",
            "phase": "v2",
            "tool_registry": "app.ai.tools.get_ai_tool_registry",
        },
        "counts": {
            "tools": len(tools),
            "categories": len(categories),
            "confirmation_required": confirmation_required,
            "risk_levels": risk_levels,
        },
        "categories": categories,
        "transports": {
            "http": {
                "base_prefix": settings.api_prefix,
                "status": f"{settings.api_prefix}/ai/status",
                "manifest": f"{settings.api_prefix}/ai/manifest",
                "tools": f"{settings.api_prefix}/ai/tools",
                "chat": f"{settings.api_prefix}/ai/chat",
                "workflows": f"{settings.api_prefix}/ai/workflows",
                "workflow_run_template": f"{settings.api_prefix}/ai/workflows/{{workflow_name}}/run",
                "execute_template": f"{settings.api_prefix}/ai/tools/{{tool_name}}/execute",
            },
            "mcp_stdio": {
                "command": "python",
                "args": ["-m", "app.ai.mcp_server"],
                "description": "Exposes every AI tool registry entry as a stdio MCP tool.",
            },
        },
        "policies": {
            "read_tools_auto_executable": True,
            "write_or_danger_tools_require_confirmation": True,
            "live_trading_phase_1": "read_only_status_and_account_tools",
            "secrets": "API keys are configured via env/.env.local and never returned by manifest.",
        },
        "workflows": workflows,
        "tools": [_tool_payload(tool) for tool in tools],
    }
