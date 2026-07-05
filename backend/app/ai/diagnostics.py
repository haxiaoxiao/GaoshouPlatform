from __future__ import annotations

from collections import Counter
from typing import Any

from app.ai.schemas import LLMGatewayStatus


def _as_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if hasattr(value, "model_dump"):
        return value.model_dump()
    return {}


def _counter_payload(counter: Counter[str]) -> dict[str, int]:
    return {key: counter[key] for key in sorted(counter)}


def _preview(text: Any, limit: int = 160) -> str:
    value = str(text or "").strip()
    if len(value) <= limit:
        return value
    return value[: limit - 1].rstrip() + "..."


def _trace_from_artifact(artifact: dict[str, Any]) -> dict[str, Any]:
    outputs = artifact.get("key_outputs")
    if not isinstance(outputs, dict):
        return {}
    trace = outputs.get("trace")
    return trace if isinstance(trace, dict) else {}


def _answer_mode(trace: dict[str, Any]) -> str:
    answer = trace.get("answer")
    if isinstance(answer, dict):
        return str(answer.get("mode") or "unknown")
    return "unknown"


def _executed_tools(artifact: dict[str, Any]) -> list[dict[str, Any]]:
    outputs = artifact.get("key_outputs")
    if not isinstance(outputs, dict):
        return []
    tools = outputs.get("executed_tools")
    if isinstance(tools, list):
        return [item for item in tools if isinstance(item, dict)]
    workflow_tools = outputs.get("tool_results")
    if isinstance(workflow_tools, list):
        return [item for item in workflow_tools if isinstance(item, dict)]
    return []


def _tool_calls(artifact: dict[str, Any], trace: dict[str, Any]) -> list[dict[str, Any]]:
    calls = trace.get("tool_calls")
    if isinstance(calls, list):
        return [item for item in calls if isinstance(item, dict)]
    artifact_calls = artifact.get("tool_calls")
    return [item for item in artifact_calls if isinstance(item, dict)] if isinstance(artifact_calls, list) else []


def _recent_artifact_payload(artifact: dict[str, Any]) -> dict[str, Any]:
    trace = _trace_from_artifact(artifact)
    outputs = artifact.get("key_outputs") if isinstance(artifact.get("key_outputs"), dict) else {}
    executed = _executed_tools(artifact)
    calls = _tool_calls(artifact, trace)
    reply_preview = ""
    if isinstance(outputs, dict):
        reply_preview = _preview(outputs.get("reply") or outputs.get("summary") or "", 180)
    return {
        "artifact_id": artifact.get("artifact_id"),
        "kind": artifact.get("kind"),
        "status": artifact.get("status"),
        "created_at": artifact.get("created_at"),
        "input_summary": _preview(artifact.get("input_summary"), 180),
        "reply_preview": reply_preview,
        "route_source": trace.get("source") or "unknown",
        "answer_mode": _answer_mode(trace),
        "tool_call_count": len(calls),
        "executed_count": len(executed),
        "error": artifact.get("error") or trace.get("error"),
    }


def build_ai_diagnostics(
    *,
    enabled: bool,
    gateway: LLMGatewayStatus | dict[str, Any],
    manifest: dict[str, Any],
    artifacts: list[dict[str, Any]],
    sample_limit: int,
) -> dict[str, Any]:
    """Summarize AI Native routing/tool/artifact health without exposing secrets."""
    gateway_payload = _as_dict(gateway)
    ready = bool(enabled and gateway_payload.get("available") and gateway_payload.get("configured"))
    status_counts: Counter[str] = Counter()
    kind_counts: Counter[str] = Counter()
    route_sources: Counter[str] = Counter()
    answer_modes: Counter[str] = Counter()
    tool_statuses: Counter[str] = Counter()
    top_tools: Counter[str] = Counter()
    recent_failures: list[dict[str, Any]] = []
    answer_errors = 0
    pending_confirmations = 0

    for artifact in artifacts:
        status_counts[str(artifact.get("status") or "unknown")] += 1
        kind_counts[str(artifact.get("kind") or "unknown")] += 1
        trace = _trace_from_artifact(artifact)
        if trace:
            source = str(trace.get("source") or "unknown")
            route_sources[source] += 1
            mode = _answer_mode(trace)
            answer_modes[mode] += 1
            answer = trace.get("answer")
            if isinstance(answer, dict) and answer.get("error"):
                answer_errors += 1
        for call in _tool_calls(artifact, trace):
            tool_name = str(call.get("tool_name") or "unknown")
            top_tools[tool_name] += 1
            if call.get("status") == "pending_confirmation":
                pending_confirmations += 1
        for tool in _executed_tools(artifact):
            tool_name = str(tool.get("tool_name") or "unknown")
            status = str(tool.get("status") or "unknown")
            tool_statuses[status] += 1
            top_tools[tool_name] += 1
            if status != "ok":
                recent_failures.append(
                    {
                        "artifact_id": artifact.get("artifact_id"),
                        "created_at": artifact.get("created_at"),
                        "tool_name": tool_name,
                        "status": status,
                        "summary": _preview(tool.get("summary"), 160),
                        "error": _preview(tool.get("error"), 160),
                    }
                )
        if artifact.get("status") not in {None, "completed", "ok"} and artifact.get("error"):
            recent_failures.append(
                {
                    "artifact_id": artifact.get("artifact_id"),
                    "created_at": artifact.get("created_at"),
                    "tool_name": "artifact",
                    "status": str(artifact.get("status") or "unknown"),
                    "summary": _preview(artifact.get("input_summary"), 160),
                    "error": _preview(artifact.get("error"), 160),
                }
            )

    warnings: list[str] = []
    if not enabled:
        warnings.append("AI Gateway is disabled.")
    if not gateway_payload.get("available"):
        warnings.append(str(gateway_payload.get("error") or "LiteLLM is not available."))
    elif not gateway_payload.get("configured"):
        warnings.append(str(gateway_payload.get("error") or "API key is not configured."))
    if recent_failures:
        warnings.append(f"{len(recent_failures)} recent tool/artifact failures in sampled artifacts.")
    if answer_errors:
        warnings.append(f"{answer_errors} answer synthesis errors in sampled artifacts.")

    recent_artifacts = [_recent_artifact_payload(artifact) for artifact in artifacts[:8]]
    counts = manifest.get("counts") if isinstance(manifest.get("counts"), dict) else {}
    transports = manifest.get("transports") if isinstance(manifest.get("transports"), dict) else {}
    http_transport = transports.get("http") if isinstance(transports.get("http"), dict) else {}
    mcp_transport = transports.get("mcp_stdio") if isinstance(transports.get("mcp_stdio"), dict) else {}
    workflows = manifest.get("workflows") if isinstance(manifest.get("workflows"), list) else []

    return {
        "health": {
            "status": "ready" if ready and not recent_failures else "degraded" if ready else "offline",
            "ready": ready,
            "warnings": warnings,
        },
        "gateway": {
            "enabled": enabled,
            "available": bool(gateway_payload.get("available")),
            "configured": bool(gateway_payload.get("configured")),
            "provider": gateway_payload.get("provider"),
            "model": gateway_payload.get("model"),
            "api_key_env": gateway_payload.get("api_key_env"),
            "api_key_configured": bool(gateway_payload.get("api_key_configured")),
            "timeout_seconds": gateway_payload.get("timeout_seconds"),
            "max_tokens": gateway_payload.get("max_tokens"),
            "error": gateway_payload.get("error"),
        },
        "manifest": {
            "schema_version": manifest.get("schema_version"),
            "generated_at": manifest.get("generated_at"),
            "tool_count": counts.get("tools", 0),
            "workflow_count": len(workflows),
            "categories": manifest.get("categories") or {},
            "risk_levels": counts.get("risk_levels") or {},
            "confirmation_required": counts.get("confirmation_required", 0),
            "workflows": [
                {
                    "name": workflow.get("name"),
                    "title": workflow.get("title"),
                    "category": workflow.get("category"),
                    "node_count": len(workflow.get("nodes") or []),
                }
                for workflow in workflows
                if isinstance(workflow, dict)
            ],
            "http": {
                "chat": http_transport.get("chat"),
                "manifest": http_transport.get("manifest"),
                "workflows": http_transport.get("workflows"),
                "workflow_run_template": http_transport.get("workflow_run_template"),
                "execute_template": http_transport.get("execute_template"),
            },
            "mcp_stdio": {
                "command": mcp_transport.get("command"),
                "args": mcp_transport.get("args") or [],
            },
        },
        "artifacts": {
            "sample_limit": sample_limit,
            "sampled": len(artifacts),
            "status_counts": _counter_payload(status_counts),
            "kind_counts": _counter_payload(kind_counts),
            "recent": recent_artifacts,
            "latest": recent_artifacts[0] if recent_artifacts else None,
        },
        "routing": {
            "source_counts": _counter_payload(route_sources),
            "pending_confirmation_count": pending_confirmations,
        },
        "answers": {
            "mode_counts": _counter_payload(answer_modes),
            "error_count": answer_errors,
        },
        "tools": {
            "status_counts": _counter_payload(tool_statuses),
            "top_tools": [
                {"tool_name": name, "count": count}
                for name, count in top_tools.most_common(10)
            ],
            "recent_failures": recent_failures[:8],
        },
    }
