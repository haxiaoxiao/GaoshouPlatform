from __future__ import annotations

import inspect
from typing import Any

from mcp.server.fastmcp import FastMCP

from app.ai.tools import execute_tool, list_tools
from app.db.sqlite import async_session_factory

mcp = FastMCP("GaoshouPlatform Read-Only Research Tools")


def _schema_annotation(schema: dict[str, Any], *, required: bool) -> Any:
    annotation = {
        "array": list,
        "boolean": bool,
        "integer": int,
        "number": float,
        "object": dict,
        "string": str,
    }.get(str(schema.get("type")), Any)
    return annotation if required else annotation | None


def _make_handler(tool_name: str, input_schema: dict[str, Any]):
    async def handler(**arguments: Any) -> Any:
        async with async_session_factory() as session:
            return await execute_tool(session, tool_name, arguments)

    required = set(input_schema.get("required") or [])
    parameters = []
    for name, schema in dict(input_schema.get("properties") or {}).items():
        parameters.append(inspect.Parameter(
            name,
            inspect.Parameter.KEYWORD_ONLY,
            default=inspect.Parameter.empty if name in required else None,
            annotation=_schema_annotation(dict(schema), required=name in required),
        ))
    handler.__signature__ = inspect.Signature(parameters, return_annotation=Any)  # type: ignore[attr-defined]
    return handler


def _register_tools() -> None:
    for definition in list_tools(read_only=True):
        handler = _make_handler(definition.name, definition.input_schema)
        handler.__name__ = definition.name
        handler.__doc__ = definition.description
        mcp.add_tool(handler, name=definition.name, description=definition.description)
        registered = mcp._tool_manager.get_tool(definition.name)
        if registered is not None:
            registered.parameters = definition.input_schema


_register_tools()


if __name__ == "__main__":
    mcp.run(transport="stdio")
