from __future__ import annotations

import asyncio
from typing import Any

from app.ai.tools import get_ai_tool_registry


def create_mcp_server():
    """Create a stdio MCP server backed by the AI tool registry."""
    try:
        from mcp.server.fastmcp import FastMCP
    except Exception as exc:  # pragma: no cover - depends on optional package
        raise RuntimeError("Install the mcp package to run app.ai.mcp_server") from exc

    server = FastMCP("GaoshouPlatform AI Native")
    registry = get_ai_tool_registry()

    def make_runner(tool_name: str):
        async def run_tool(
            arguments: dict[str, Any] | None = None,
            confirmed: bool = False,
        ) -> dict[str, Any]:
            result = await registry.execute(
                tool_name,
                arguments or {},
                confirmed=confirmed,
                session=None,
            )
            return result.model_dump()

        return run_tool

    for tool in registry.list():
        runner = make_runner(tool.name)
        runner.__name__ = tool.name.replace(".", "_")
        runner.__doc__ = tool.description
        server.tool(name=tool.name, description=tool.description)(runner)

    return server


def main() -> None:
    server = create_mcp_server()
    result = server.run()
    if asyncio.iscoroutine(result):  # pragma: no cover - SDK version compatibility
        asyncio.run(result)


if __name__ == "__main__":
    main()
