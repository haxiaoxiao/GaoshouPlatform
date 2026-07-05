from __future__ import annotations

import os
from datetime import datetime, timedelta

import pytest
from httpx import ASGITransport, AsyncClient

from app.ai.gateway import LLMGatewayError, reset_llm_gateway
from app.ai.workflows import get_compiled_langgraph_workflow
from app.api.ai import (
    _prefer_workflow_action,
    _stock_symbol_from_text_or_name,
    _suggest_actions,
    _workflow_action_from_intent,
)
from app.core.config import settings
from app.db.sqlite import get_async_session
from app.main import app


async def _fake_session():
    yield object()


class _FakeArtifact:
    artifact_id = "ai-test-artifact"


async def _fake_create_artifact(*_args, **_kwargs):
    return _FakeArtifact()


async def _fake_update_artifact(*_args, **_kwargs):
    return _FakeArtifact()


def _patch_artifacts(monkeypatch):
    monkeypatch.setattr("app.api.ai.create_artifact", _fake_create_artifact)
    monkeypatch.setattr("app.api.ai.update_artifact", _fake_update_artifact)


@pytest.mark.asyncio
async def test_ai_status_and_tools_routes_are_registered():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        status_resp = await client.get("/api/ai/status")
        tools_resp = await client.get("/api/ai/tools")

    assert status_resp.status_code == 200
    assert status_resp.json()["code"] == 0
    assert status_resp.json()["data"]["tool_count"] >= 193
    assert tools_resp.status_code == 200
    tool_names = {tool["name"] for tool in tools_resp.json()["data"]}
    assert {
        "system.status",
        "system.health",
        "system.cache",
        "system.dev_data_mode",
        "system.update_dev_data_mode",
        "system.live_trading_guardrails",
        "system.update_live_trading_guardrails",
        "system.tool_catalog",
        "system.mcp_manifest",
        "system.ai_diagnostics",
        "system.ai_artifacts",
        "system.ai_artifact_detail",
        "workflow.catalog",
        "workflow.run",
        "workflow.command_graph",
        "workflow.report_strategy_graph",
        "workflow.quant_research_graph",
        "akshare.stock_daily",
        "akshare.stock_daily_batch",
        "akshare.stock_spot",
        "akshare.stock_list",
        "akshare.stock_info",
        "akshare.stock_hist",
        "data.stock_snapshot",
        "data.stock_detail",
        "data.review_context",
        "data.stock_list",
        "data.kline_daily",
        "data.kline_minute",
        "data.klines_query",
        "data.market_snapshot",
        "data.realtime_quote",
        "data.sync_status",
        "data.sync_cancel",
        "data.sync_cancel_all",
        "data.watchlist_groups",
        "data.watchlist_group_create",
        "data.watchlist_group_delete",
        "data.watchlist_stock_add",
        "data.watchlist_stock_remove",
        "data.financial",
        "data.indicator_batch",
        "data.indicator_timeseries_batch",
        "data.index_pool",
        "explorer.tables",
        "explorer.table_schema",
        "explorer.table_search",
        "explorer.distinct_values",
        "parquet.datasets",
        "parquet.dataset_schema",
        "indicator.categories",
        "indicator.description",
        "indicator.query",
        "indicator.compute",
        "indicator.screen",
        "indicator.financial",
        "compute.validate",
        "compute.evaluate",
        "compute.screen",
        "compute.precompute",
        "compute.batch",
        "evaluation.ic_analysis",
        "evaluation.quantile_backtest",
        "evaluation.full_report",
        "evaluation.report",
        "evaluation.board",
        "factor.templates_v2",
        "factor.validate_python",
        "factor.validate_v2",
        "factor.create_legacy",
        "factor.update_legacy",
        "factor.delete_legacy",
        "factor.analysis_list",
        "factor.analysis_detail",
        "factor.create_v2",
        "factor.update_v2",
        "factor.delete_v2",
        "factor.preview_saved",
        "factor.run_python_saved",
        "factor.precompute_saved",
        "factor.coverage_saved",
        "factor.analyze_saved",
        "factor.evaluate_saved",
        "factor_value.definitions",
        "factor_value.param_hashes",
        "factor_value.query",
        "factor_value.paper_manifest",
        "factor_value.paper_experiments",
        "factor_value.paper_feature_snapshot",
        "factor_value.precompute_prepare",
        "factor_value.precompute",
        "factor_value.group_precompute",
        "sentiment.summary",
        "sentiment.threads",
        "sentiment.ingest_run",
        "factor_research.prepare",
        "factor_research.submit",
        "factor_research.batch",
        "factor_research.combinations",
        "backtest.capabilities",
        "backtest.engines",
        "backtest.preset_dual_stock_grid",
        "backtest.index_pool_detail",
        "backtest.create_preset_dual_stock_grid_strategy",
        "backtest.create_preset_multi_factor_strategy",
        "backtest.create_preset_tech_small_cap_strategy",
        "backtest.pool_symbols",
        "backtest.records",
        "backtest.create_record",
        "backtest.run_record",
        "backtest.delete_record",
        "backtest.delete_records_batch",
        "backtest.timer_coverage",
        "backtest.data_coverage",
        "backtest.stock_names",
        "backtest.optimize_grid",
        "backtest.optimize_walk_forward",
        "backtest.strategy_params_schema",
        "backtest.strategy_params_validate",
        "backtest.task_cancel",
        "backtest.task_report",
        "backtest.factor",
        "backtest.submit",
        "strategy.list",
        "report.chat_session_create",
        "report.chat_session_send",
        "strategy.create",
        "strategy.update",
        "strategy.delete",
        "strategy.trend_signals_daily",
        "strategy.trend_signals_summary",
        "strategy.trend_backtest",
        "strategy.deep_value_backtest",
        "live_trading.status",
        "live_trading.account",
        "live_trading.strategy_profiles",
        "live_trading.weekly_trades",
        "live_trading.account_initialize",
        "live_trading.profile_create",
        "live_trading.profile_update",
        "live_trading.preflight",
        "live_trading.signals",
        "live_trading.runner_start",
        "live_trading.runner_stop",
        "live_trading.runner_takeover",
        "live_trading.orders_sync",
        "live_trading.orders_submit",
        "live_trading.orders_cancel",
        "live_trading.orders_cancel_resubmit",
        "live_trading.orders_close_local",
    } <= tool_names


@pytest.mark.asyncio
async def test_ai_manifest_exposes_http_and_mcp_contract():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        resp = await client.get("/api/ai/manifest")

    body = resp.json()
    assert resp.status_code == 200
    assert body["code"] == 0
    manifest = body["data"]
    assert manifest["schema_version"] == "gaoshou-ai-native-tool-manifest/v1"
    assert manifest["counts"]["tools"] >= 193
    assert manifest["transports"]["mcp_stdio"]["args"] == ["-m", "app.ai.mcp_server"]
    assert manifest["transports"]["http"]["execute_template"] == "/api/ai/tools/{tool_name}/execute"
    assert manifest["transports"]["http"]["workflow_run_template"] == "/api/ai/workflows/{workflow_name}/run"
    workflow_names = {workflow["name"] for workflow in manifest["workflows"]}
    assert {"CommandGraph", "ReportStrategyGraph", "QuantResearchGraph"} <= workflow_names
    by_name = {tool["name"]: tool for tool in manifest["tools"]}
    assert by_name["data.kline_daily"]["mcp"]["call_shape"] == {
        "arguments": "object",
        "confirmed": "boolean",
    }
    assert by_name["data.stock_list"]["requires_confirmation"] is False
    assert by_name["data.stock_detail"]["requires_confirmation"] is False
    assert by_name["data.klines_query"]["requires_confirmation"] is False
    assert by_name["data.indicator_timeseries_batch"]["requires_confirmation"] is False
    assert by_name["backtest.index_pool_detail"]["requires_confirmation"] is False
    assert by_name["factor.run_python_saved"]["requires_confirmation"] is False
    assert by_name["data.sync_submit"]["requires_confirmation"] is True
    assert by_name["data.watchlist_stock_add"]["requires_confirmation"] is True
    assert by_name["indicator.compute"]["requires_confirmation"] is True
    assert by_name["backtest.create_preset_multi_factor_strategy"]["requires_confirmation"] is True
    assert by_name["factor.create_v2"]["requires_confirmation"] is True
    assert by_name["factor.evaluate_saved"]["requires_confirmation"] is True


@pytest.mark.asyncio
async def test_ai_diagnostics_summarizes_artifact_trace(monkeypatch):
    async def fake_list_artifacts(*_args, **_kwargs):
        return [
            {
                "artifact_id": "ai-a1",
                "kind": "ai_chat",
                "status": "completed",
                "input_summary": "看一下运行任务进度",
                "tool_calls": [],
                "key_outputs": {
                    "reply": "当前运行任务已检查。",
                    "executed_tools": [
                        {
                            "tool_name": "runtime.tasks",
                            "status": "ok",
                            "summary": "找到 0 个运行任务。",
                            "error": None,
                        }
                    ],
                    "trace": {
                        "source": "llm",
                        "answer": {"mode": "react_answer", "error": None},
                        "tool_calls": [
                            {
                                "tool_name": "runtime.tasks",
                                "status": "ok",
                            }
                        ],
                    },
                },
                "error": None,
                "created_at": "2026-07-04T10:00:00",
                "updated_at": "2026-07-04T10:00:01",
            },
            {
                "artifact_id": "ai-a2",
                "kind": "ai_chat",
                "status": "completed",
                "input_summary": "查行情",
                "tool_calls": [],
                "key_outputs": {
                    "reply": "行情不可用。",
                    "executed_tools": [
                        {
                            "tool_name": "data.kline_daily",
                            "status": "error",
                            "summary": "QMT 不可用",
                            "error": "xtquant unavailable",
                        }
                    ],
                    "trace": {
                        "source": "fallback",
                        "answer": {"mode": "template_observation", "error": None},
                        "tool_calls": [
                            {
                                "tool_name": "data.kline_daily",
                                "status": "error",
                            }
                        ],
                    },
                },
                "error": None,
                "created_at": "2026-07-04T09:00:00",
                "updated_at": "2026-07-04T09:00:01",
            },
            {
                "artifact_id": "ai-w1",
                "kind": "workflow:QuantResearchGraph",
                "status": "completed",
                "input_summary": "603629.SH 走势解读",
                "tool_calls": [{"tool_name": "data.stock_snapshot", "arguments": {"symbol": "603629.SH"}}],
                "key_outputs": {
                    "workflow_name": "QuantResearchGraph",
                    "status": "completed",
                    "summary": "QuantResearchGraph 已处理：1 个工具完成，0 个异常，0 个待确认/待执行。",
                    "tool_results": [
                        {
                            "tool_name": "data.stock_snapshot",
                            "status": "ok",
                            "summary": "603629.SH 利通电子 快照已读取。",
                            "error": None,
                        }
                    ],
                },
                "error": None,
                "created_at": "2026-07-04T08:00:00",
                "updated_at": "2026-07-04T08:00:01",
            },
        ]

    monkeypatch.setattr("app.api.ai.list_artifacts", fake_list_artifacts)
    app.dependency_overrides[get_async_session] = _fake_session
    try:
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            resp = await client.get("/api/ai/diagnostics?limit=20")
    finally:
        app.dependency_overrides.clear()

    body = resp.json()
    assert resp.status_code == 200
    assert body["code"] == 0
    diagnostics = body["data"]
    assert diagnostics["manifest"]["tool_count"] >= 60
    assert diagnostics["artifacts"]["sampled"] == 3
    assert diagnostics["artifacts"]["kind_counts"]["workflow:QuantResearchGraph"] == 1
    assert diagnostics["artifacts"]["latest"]["artifact_id"] == "ai-a1"
    assert diagnostics["routing"]["source_counts"] == {"fallback": 1, "llm": 1}
    assert diagnostics["answers"]["mode_counts"]["react_answer"] == 1
    assert diagnostics["tools"]["status_counts"] == {"error": 1, "ok": 2}
    assert diagnostics["tools"]["recent_failures"][0]["tool_name"] == "data.kline_daily"


@pytest.mark.asyncio
async def test_ai_workflows_catalog_and_dry_run(monkeypatch):
    _patch_artifacts(monkeypatch)
    app.dependency_overrides[get_async_session] = _fake_session
    try:
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            catalog_resp = await client.get("/api/ai/workflows")
            run_resp = await client.post(
                "/api/ai/workflows/CommandGraph/run",
                json={
                    "command": "看一下系统状态",
                    "dry_run": True,
                    "arguments": {
                        "tool_calls": [
                            {"tool_name": "workflow.catalog", "arguments": {}},
                            {"tool_name": "system.status", "arguments": {}},
                        ]
                    },
                },
            )
    finally:
        app.dependency_overrides.clear()

    catalog_body = catalog_resp.json()
    assert catalog_resp.status_code == 200
    workflow_names = {workflow["name"] for workflow in catalog_body["data"]}
    assert {"CommandGraph", "ReportStrategyGraph", "QuantResearchGraph"} <= workflow_names

    run_body = run_resp.json()
    assert run_resp.status_code == 200
    assert run_body["code"] == 0
    data = run_body["data"]
    assert data["workflow_name"] == "CommandGraph"
    assert data["status"] == "planned"
    assert data["artifact_id"] == "ai-test-artifact"
    assert data["pending_tools"][0]["tool_name"] == "system.status"
    assert all(item["tool_name"] != "workflow.catalog" for item in data["pending_tools"])
    assert data["result"]["tool_calls"][0]["tool_name"] == "system.status"
    assert data["result"]["graph_runtime"] == "langgraph"
    assert data["result"]["compiled_graph"] == "CompiledStateGraph"


def test_ai_workflows_are_compiled_langgraph_stategraphs():
    compiled = get_compiled_langgraph_workflow("CommandGraph")
    assert type(compiled).__name__ == "CompiledStateGraph"
    assert hasattr(compiled, "ainvoke")


def test_ai_workflow_intent_promotes_complex_requests():
    research_action = _workflow_action_from_intent(
        "给我利通过去一个月的走势，给出你的解读",
        resolved_symbol="603629.SH",
    )
    assert research_action is not None
    assert research_action.tool_name == "workflow.quant_research_graph"
    assert research_action.arguments["arguments"]["symbol"] == "603629.SH"
    assert research_action.arguments["arguments"]["daily_limit"] == 30

    report_action = _workflow_action_from_intent(
        "基于这份研报生成 AKQuant 策略：低估值高成长，月度调仓，控制回撤。",
        resolved_symbol=None,
    )
    assert report_action is not None
    assert report_action.tool_name == "workflow.report_strategy_graph"
    assert report_action.arguments["arguments"]["convert_to_akquant"] is True


def test_ai_workflow_promotion_prunes_redundant_read_tools():
    text = "给我603629.SH过去一个月的走势，给出你的解读"
    actions = _suggest_actions(text, None, resolved_symbol="603629.SH")
    promoted, workflow_action = _prefer_workflow_action(
        actions=actions,
        text=text,
        resolved_symbol="603629.SH",
    )
    assert workflow_action is not None
    assert [action.tool_name for action in promoted] == ["workflow.quant_research_graph"]


@pytest.mark.asyncio
async def test_ai_config_update_masks_secret_and_updates_runtime(monkeypatch, tmp_path):
    env_file = tmp_path / ".env.local"
    secret = "sk-test-secret"
    monkeypatch.setattr("app.api.ai._ai_config_env_path", lambda: env_file)
    monkeypatch.setattr(settings, "ai_enabled", True)
    monkeypatch.setattr(settings, "ai_provider", "litellm")
    monkeypatch.setattr(settings, "ai_model", "deepseek/deepseek-chat")
    monkeypatch.setattr(settings, "ai_base_url", "")
    monkeypatch.setattr(settings, "ai_api_key_env", "DEEPSEEK_API_KEY")
    monkeypatch.delenv("DEEPSEEK_API_KEY", raising=False)
    reset_llm_gateway()

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        resp = await client.put(
            "/api/ai/config",
            json={
                "provider": "litellm",
                "model": "deepseek/deepseek-chat",
                "base_url": "https://api.deepseek.com",
                "api_key_env": "DEEPSEEK_API_KEY",
                "api_key": secret,
            },
        )
        status_resp = await client.get("/api/ai/status")

    body = resp.json()
    assert resp.status_code == 200
    assert body["code"] == 0
    assert body["data"]["api_key_configured"] is True
    assert body["data"]["base_url"] == "https://api.deepseek.com"
    assert body["data"]["api_key_masked"] == "sk-t...cret"
    assert secret not in str(body)
    assert "AI_BASE_URL=https://api.deepseek.com" in env_file.read_text(encoding="utf-8")
    assert "DEEPSEEK_API_KEY=sk-test-secret" in env_file.read_text(encoding="utf-8")
    assert os.getenv("DEEPSEEK_API_KEY") == secret
    assert status_resp.json()["data"]["gateway"]["configured"] is True

    reset_llm_gateway()


@pytest.mark.asyncio
async def test_ai_config_clear_removes_secret(monkeypatch, tmp_path):
    env_file = tmp_path / ".env.local"
    env_file.write_text(
        "AI_PROVIDER=litellm\nAI_MODEL=deepseek/deepseek-chat\nAI_API_KEY_ENV=DEEPSEEK_API_KEY\nDEEPSEEK_API_KEY=sk-test-secret\n",
        encoding="utf-8",
    )
    monkeypatch.setattr("app.api.ai._ai_config_env_path", lambda: env_file)
    monkeypatch.setenv("DEEPSEEK_API_KEY", "sk-test-secret")
    monkeypatch.setattr(settings, "ai_provider", "litellm")
    monkeypatch.setattr(settings, "ai_model", "deepseek/deepseek-chat")
    monkeypatch.setattr(settings, "ai_base_url", "")
    monkeypatch.setattr(settings, "ai_api_key_env", "DEEPSEEK_API_KEY")
    reset_llm_gateway()

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        resp = await client.put(
            "/api/ai/config",
            json={
                "provider": "litellm",
                "model": "deepseek/deepseek-chat",
                "api_key_env": "DEEPSEEK_API_KEY",
                "clear_api_key": True,
            },
        )

    body = resp.json()
    assert resp.status_code == 200
    assert body["data"]["api_key_configured"] is False
    assert "DEEPSEEK_API_KEY=sk-test-secret" not in env_file.read_text(encoding="utf-8")
    assert os.getenv("DEEPSEEK_API_KEY") is None
    reset_llm_gateway()


@pytest.mark.asyncio
async def test_ai_config_accepts_openai_shaped_key_and_non_upper_env(monkeypatch, tmp_path):
    env_file = tmp_path / ".env.local"
    secret = "sk-proj-abc_123.def-456"
    monkeypatch.setattr("app.api.ai._ai_config_env_path", lambda: env_file)
    monkeypatch.setattr(settings, "ai_enabled", True)
    monkeypatch.setattr(settings, "ai_provider", "litellm")
    monkeypatch.setattr(settings, "ai_model", "gpt-4o-mini")
    monkeypatch.setattr(settings, "ai_base_url", "")
    monkeypatch.setattr(settings, "ai_api_key_env", "OPENAI_API_KEY")
    monkeypatch.delenv("openai_api_key", raising=False)
    reset_llm_gateway()

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        resp = await client.put(
            "/api/ai/config",
            json={
                "provider": "litellm",
                "model": "gpt-4o-mini",
                "base_url": "https://api.openai.com/v1",
                "api_key_env": "openai_api_key",
                "api_key": secret,
            },
        )

    body = resp.json()
    assert resp.status_code == 200
    assert body["data"]["api_key_configured"] is True
    assert body["data"]["api_key_env"] == "openai_api_key"
    assert body["data"]["api_key_masked"] == "sk-p...-456"
    assert body["data"]["api_key_warning"] is None
    assert secret not in str(body)
    assert "openai_api_key=sk-proj-abc_123.def-456" in env_file.read_text(encoding="utf-8")
    assert os.getenv("openai_api_key") == secret
    reset_llm_gateway()


@pytest.mark.asyncio
async def test_ai_config_splits_env_assignment_pasted_as_api_key(monkeypatch, tmp_path):
    env_file = tmp_path / ".env.local"
    secret = "sk-proj-pasted-secret"
    monkeypatch.setattr("app.api.ai._ai_config_env_path", lambda: env_file)
    monkeypatch.setattr(settings, "ai_enabled", True)
    monkeypatch.setattr(settings, "ai_provider", "litellm")
    monkeypatch.setattr(settings, "ai_model", "gpt-5")
    monkeypatch.setattr(settings, "ai_base_url", "https://api.openai.com/v1")
    monkeypatch.setattr(settings, "ai_api_key_env", "OPENAI_API_KEY")
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    reset_llm_gateway()

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        resp = await client.put(
            "/api/ai/config",
            json={
                "provider": "litellm",
                "model": "gpt-5",
                "base_url": "https://api.openai.com/v1",
                "api_key_env": "OPENAI_API_KEY",
                "api_key": f"OPENAI_API_KEY={secret}",
            },
        )

    body = resp.json()
    env_text = env_file.read_text(encoding="utf-8")
    assert resp.status_code == 200
    assert body["data"]["api_key_configured"] is True
    assert body["data"]["api_key_env"] == "OPENAI_API_KEY"
    assert body["data"]["api_key_masked"] == "sk-p...cret"
    assert "OPENAI_API_KEY=OPENAI_API_KEY=" not in env_text
    assert f"OPENAI_API_KEY={secret}" in env_text
    assert os.getenv("OPENAI_API_KEY") == secret
    reset_llm_gateway()


@pytest.mark.asyncio
async def test_ai_config_warns_when_openai_compatible_key_shape_is_suspicious(monkeypatch, tmp_path):
    env_file = tmp_path / ".env.local"
    monkeypatch.setattr("app.api.ai._ai_config_env_path", lambda: env_file)
    monkeypatch.setattr(settings, "ai_enabled", True)
    monkeypatch.setattr(settings, "ai_provider", "litellm")
    monkeypatch.setattr(settings, "ai_model", "gpt-5")
    monkeypatch.setattr(settings, "ai_base_url", "https://api.openai.com/v1")
    monkeypatch.setattr(settings, "ai_api_key_env", "OPENAI_API_KEY")
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    reset_llm_gateway()

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        resp = await client.put(
            "/api/ai/config",
            json={
                "provider": "litellm",
                "model": "gpt-5",
                "base_url": "https://api.openai.com/v1",
                "api_key_env": "OPENAI_API_KEY",
                "api_key": "OPENAI_API_KEY_value_that_is_not_a_secret",
            },
        )

    body = resp.json()
    assert resp.status_code == 200
    assert body["data"]["api_key_configured"] is True
    assert "不是 sk- 开头" in body["data"]["api_key_warning"]
    reset_llm_gateway()


@pytest.mark.asyncio
async def test_ai_write_tool_requires_confirmation(monkeypatch):
    _patch_artifacts(monkeypatch)
    app.dependency_overrides[get_async_session] = _fake_session
    try:
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            resp = await client.post(
                "/api/ai/tools/data.sync_submit/execute",
                json={"arguments": {"sync_type": "stock_info"}, "confirmed": False},
            )
            optimize_resp = await client.post(
                "/api/ai/tools/backtest.optimize_grid/execute",
                json={
                    "arguments": {
                        "engine": "akquant",
                        "start_date": "2024-01-01",
                        "end_date": "2024-02-01",
                        "symbols": ["603629.SH"],
                        "param_grid": {"x": [1, 2]},
                    },
                    "confirmed": False,
                },
            )
            factor_resp = await client.post(
                "/api/ai/tools/factor_value.precompute/execute",
                json={
                    "arguments": {
                        "factor_names": ["high_volume_signal"],
                        "start_date": "2024-01-01",
                        "end_date": "2024-01-31",
                        "symbols": ["603629.SH"],
                        "async_task": True,
                    },
                    "confirmed": False,
                },
            )
            sentiment_resp = await client.post(
                "/api/ai/tools/sentiment.ingest_run/execute",
                json={"arguments": {"sources": ["eastmoney_guba"], "max_pages": 1}, "confirmed": False},
            )
            compute_precompute_resp = await client.post(
                "/api/ai/tools/compute.precompute/execute",
                json={
                    "arguments": {
                        "expressions": ["$close"],
                        "symbols": ["603629.SH"],
                        "start_date": "2024-01-01",
                        "end_date": "2024-01-31",
                    },
                    "confirmed": False,
                },
            )
            saved_factor_precompute_resp = await client.post(
                "/api/ai/tools/factor.precompute_saved/execute",
                json={
                    "arguments": {
                        "factor_id": 1,
                        "start_date": "2024-01-01",
                        "end_date": "2024-01-31",
                    },
                    "confirmed": False,
                },
            )
            watchlist_group_create_resp = await client.post(
                "/api/ai/tools/data.watchlist_group_create/execute",
                json={"arguments": {"name": "AI test group"}, "confirmed": False},
            )
            watchlist_stock_add_resp = await client.post(
                "/api/ai/tools/data.watchlist_stock_add/execute",
                json={"arguments": {"group_id": 1, "symbol": "603629.SH"}, "confirmed": False},
            )
            indicator_compute_resp = await client.post(
                "/api/ai/tools/indicator.compute/execute",
                json={"arguments": {"indicator_names": ["roe"], "symbols": ["603629.SH"]}, "confirmed": False},
            )
            factor_create_v2_resp = await client.post(
                "/api/ai/tools/factor.create_v2/execute",
                json={
                    "arguments": {
                        "name": "ai_test_factor",
                        "expression": "$close",
                        "stock_pool": "hs300",
                        "source_type": "dsl",
                        "engine": "builtin",
                    },
                    "confirmed": False,
                },
            )
            factor_evaluate_saved_resp = await client.post(
                "/api/ai/tools/factor.evaluate_saved/execute",
                json={
                    "arguments": {
                        "factor_id": 1,
                        "start_date": "2024-01-01",
                        "end_date": "2024-01-31",
                    },
                    "confirmed": False,
                },
            )
            preset_strategy_resp = await client.post(
                "/api/ai/tools/backtest.create_preset_multi_factor_strategy/execute",
                json={"arguments": {"name": "multi_factor"}, "confirmed": False},
            )
            factor_research_resp = await client.post(
                "/api/ai/tools/factor_research.submit/execute",
                json={
                    "arguments": {
                        "factor_name": "small_cap",
                        "stock_pool_value": "zz500",
                        "start_date": "2024-01-01",
                        "end_date": "2024-01-31",
                    },
                    "confirmed": False,
                },
            )
            strategy_create_resp = await client.post(
                "/api/ai/tools/strategy.create/execute",
                json={"arguments": {"name": "AI test strategy", "code": "def init(context): pass"}, "confirmed": False},
            )
            backtest_create_resp = await client.post(
                "/api/ai/tools/backtest.create_record/execute",
                json={
                    "arguments": {
                        "strategy_id": 1,
                        "start_date": "2024-01-01",
                        "end_date": "2024-01-31",
                        "initial_capital": "1000000",
                    },
                    "confirmed": False,
                },
            )
            live_runner_resp = await client.post(
                "/api/ai/tools/live_trading.runner_start/execute",
                json={"arguments": {"mode": "paper", "interval_seconds": 60}, "confirmed": False},
            )
            live_order_resp = await client.post(
                "/api/ai/tools/live_trading.orders_submit/execute",
                json={"arguments": {"mode": "live", "orders": [], "confirm": False}, "confirmed": False},
            )
    finally:
        app.dependency_overrides.clear()

    body = resp.json()
    assert resp.status_code == 200
    assert body["code"] == 0
    assert body["data"]["status"] == "needs_confirmation"
    assert body["data"]["artifact_id"] == "ai-test-artifact"
    assert optimize_resp.json()["data"]["status"] == "needs_confirmation"
    assert factor_resp.json()["data"]["status"] == "needs_confirmation"
    assert sentiment_resp.json()["data"]["status"] == "needs_confirmation"
    assert compute_precompute_resp.json()["data"]["status"] == "needs_confirmation"
    assert saved_factor_precompute_resp.json()["data"]["status"] == "needs_confirmation"
    assert watchlist_group_create_resp.json()["data"]["status"] == "needs_confirmation"
    assert watchlist_stock_add_resp.json()["data"]["status"] == "needs_confirmation"
    assert indicator_compute_resp.json()["data"]["status"] == "needs_confirmation"
    assert factor_create_v2_resp.json()["data"]["status"] == "needs_confirmation"
    assert factor_evaluate_saved_resp.json()["data"]["status"] == "needs_confirmation"
    assert preset_strategy_resp.json()["data"]["status"] == "needs_confirmation"
    assert factor_research_resp.json()["data"]["status"] == "needs_confirmation"
    assert strategy_create_resp.json()["data"]["status"] == "needs_confirmation"
    assert backtest_create_resp.json()["data"]["status"] == "needs_confirmation"
    assert live_runner_resp.json()["data"]["status"] == "needs_confirmation"
    assert live_order_resp.json()["data"]["status"] == "needs_confirmation"


@pytest.mark.asyncio
async def test_ai_read_tool_executes_without_confirmation(monkeypatch):
    _patch_artifacts(monkeypatch)
    app.dependency_overrides[get_async_session] = _fake_session
    try:
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            resp = await client.post(
                "/api/ai/tools/runtime.tasks/execute",
                json={"arguments": {"include_finished": True}},
            )
    finally:
        app.dependency_overrides.clear()

    body = resp.json()
    assert resp.status_code == 200
    assert body["code"] == 0
    assert body["data"]["status"] == "ok"
    assert body["data"]["tool_name"] == "runtime.tasks"


@pytest.mark.asyncio
async def test_ai_tool_wraps_internal_api_errors(monkeypatch):
    _patch_artifacts(monkeypatch)
    app.dependency_overrides[get_async_session] = _fake_session
    try:
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            resp = await client.post(
                "/api/ai/tools/explorer.table_schema/execute",
                json={"arguments": {"table_name": "definitely_missing_table"}},
            )
    finally:
        app.dependency_overrides.clear()

    body = resp.json()
    assert resp.status_code == 200
    assert body["code"] == 0
    assert body["data"]["status"] == "error"
    assert "not found" in body["data"]["error"]


@pytest.mark.asyncio
async def test_ai_chat_offline_returns_suggested_actions(monkeypatch):
    class FakeGateway:
        model = "fake-model"

        def is_ready(self):
            return False

        def status(self):
            return {
                "available": True,
                "configured": False,
                "provider": "litellm",
                "model": self.model,
                "api_key_env": "OPENAI_API_KEY",
                "api_key_configured": False,
                "timeout_seconds": 30.0,
                "max_tokens": 1000,
                "error": "not configured",
            }

    _patch_artifacts(monkeypatch)
    monkeypatch.setattr("app.api.ai.get_llm_gateway", lambda: FakeGateway())
    app.dependency_overrides[get_async_session] = _fake_session
    try:
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            resp = await client.post(
                "/api/ai/chat",
                json={
                    "messages": [
                        {"role": "user", "content": "看一下 600519.SH 的股票快照和数据状态"}
                    ],
                    "page_context": {"path": "/data"},
                    "auto_execute": False,
                },
            )
    finally:
        app.dependency_overrides.clear()

    body = resp.json()
    assert resp.status_code == 200
    assert body["code"] == 0
    assert body["data"]["offline"] is True
    action_names = {action["tool_name"] for action in body["data"]["actions"]}
    assert {"system.data_summary", "data.stock_snapshot"} <= action_names


@pytest.mark.asyncio
async def test_ai_chat_suggests_market_tools_for_cn_symbol_without_suffix(monkeypatch):
    class FakeGateway:
        model = "fake-model"

        def is_ready(self):
            return False

        def status(self):
            return {
                "available": True,
                "configured": False,
                "provider": "litellm",
                "model": self.model,
                "api_key_env": "OPENAI_API_KEY",
                "api_key_configured": False,
                "timeout_seconds": 30.0,
                "max_tokens": 1000,
                "error": "not configured",
            }

    _patch_artifacts(monkeypatch)
    monkeypatch.setattr("app.api.ai.get_llm_gateway", lambda: FakeGateway())
    app.dependency_overrides[get_async_session] = _fake_session
    try:
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            resp = await client.post(
                "/api/ai/chat",
                json={
                    "messages": [{"role": "user", "content": "帮我看一下600519的行情和K线"}],
                    "auto_execute": False,
                },
            )
    finally:
        app.dependency_overrides.clear()

    body = resp.json()
    action_map = {action["tool_name"]: action["arguments"] for action in body["data"]["actions"]}
    assert resp.status_code == 200
    assert "data.market_snapshot" in action_map
    assert "data.realtime_quote" in action_map
    assert "data.kline_daily" in action_map
    assert action_map["data.realtime_quote"]["symbol"] == "600519.SH"


@pytest.mark.asyncio
async def test_ai_action_resolution_supports_stock_name_and_relative_date():
    class FakeRows:
        def all(self):
            return [("603629.SH", "利通电子")]

    class FakeSession:
        async def execute(self, _stmt):
            return FakeRows()

    symbol = await _stock_symbol_from_text_or_name("给我利通电子昨天的行情", FakeSession())
    assert symbol == "603629.SH"
    short_symbol = await _stock_symbol_from_text_or_name("给我利通过去一个月的走势", FakeSession())
    assert short_symbol == "603629.SH"

    actions = _suggest_actions("给我利通电子昨天的行情", None, resolved_symbol=symbol)
    action_map = {action.tool_name: action.arguments for action in actions}
    yesterday = (datetime.now().date() - timedelta(days=1)).isoformat()
    assert action_map["data.market_snapshot"]["symbol"] == "603629.SH"
    assert action_map["data.kline_daily"]["symbol"] == "603629.SH"
    assert action_map["data.kline_daily"]["start_date"] == yesterday
    assert action_map["data.kline_daily"]["end_date"] == yesterday
    assert "data.symbols" not in action_map


@pytest.mark.asyncio
async def test_ai_chat_auto_executes_read_tools(monkeypatch):
    class FakeGateway:
        model = "fake-model"

        def is_ready(self):
            return True

        def status(self):
            return {
                "available": True,
                "configured": True,
                "provider": "litellm",
                "model": self.model,
                "api_key_env": "OPENAI_API_KEY",
                "api_key_configured": True,
                "timeout_seconds": 30.0,
                "max_tokens": 1000,
                "error": None,
            }

        def chat(self, **_kwargs):
            raise AssertionError("chat should not be called when read tools were auto-executed")

    _patch_artifacts(monkeypatch)
    monkeypatch.setattr("app.api.ai.get_llm_gateway", lambda: FakeGateway())
    app.dependency_overrides[get_async_session] = _fake_session
    try:
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            resp = await client.post(
                "/api/ai/chat",
                json={"messages": [{"role": "user", "content": "看一下运行任务进度"}]},
            )
    finally:
        app.dependency_overrides.clear()

    body = resp.json()
    assert resp.status_code == 200
    assert body["code"] == 0
    assert body["data"]["executed_tools"][0]["tool_name"] == "runtime.tasks"
    assert "已直接查询" in body["data"]["message"]["content"]
    assert "runtime.tasks" not in {action["tool_name"] for action in body["data"]["actions"]}


@pytest.mark.asyncio
async def test_ai_chat_gateway_error_returns_planned_actions_without_second_model_call(monkeypatch):
    calls = 0

    class FakeGateway:
        model = "gpt-5"

        def is_ready(self):
            return True

        def status(self):
            return {
                "available": True,
                "configured": True,
                "provider": "litellm",
                "model": self.model,
                "api_key_env": "OPENAI_API_KEY",
                "api_key_configured": True,
                "timeout_seconds": 30.0,
                "max_tokens": 1000,
                "error": None,
            }

        def chat(self, **_kwargs):
            nonlocal calls
            calls += 1
            raise LLMGatewayError("unsupported model parameter")

    _patch_artifacts(monkeypatch)
    monkeypatch.setattr("app.api.ai.get_llm_gateway", lambda: FakeGateway())
    app.dependency_overrides[get_async_session] = _fake_session
    try:
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            resp = await client.post(
                "/api/ai/chat",
                json={"messages": [{"role": "user", "content": "看一下系统状态"}], "auto_execute": False},
            )
    finally:
        app.dependency_overrides.clear()

    body = resp.json()
    assert resp.status_code == 200
    assert body["code"] == 0
    assert body["data"]["offline"] is False
    assert calls == 1
    assert body["data"]["actions"][0]["tool_name"] == "system.status"
    assert body["data"]["trace"]["answer"]["mode"] == "planned_actions"
    assert "system.status" in body["data"]["message"]["content"]
    assert "unsupported model parameter" in body["data"]["message"]["content"]


def test_llm_gateway_omits_temperature_for_gpt5(monkeypatch):
    captured: dict[str, object] = {}

    def fake_completion(**kwargs):
        captured.update(kwargs)
        return {"choices": [{"message": {"content": "ok"}}]}

    monkeypatch.setattr("litellm.completion", fake_completion)
    monkeypatch.setattr(settings, "ai_provider", "litellm")
    monkeypatch.setattr(settings, "ai_model", "gpt-5")
    monkeypatch.setattr(settings, "ai_base_url", "https://api.openai.com/v1")
    monkeypatch.setattr(settings, "ai_api_key_env", "OPENAI_API_KEY")
    monkeypatch.setenv("OPENAI_API_KEY", "sk-test")
    reset_llm_gateway()

    from app.ai.gateway import get_llm_gateway

    reply = get_llm_gateway().chat(messages=[{"role": "user", "content": "hi"}], temperature=0.3)

    assert reply == "ok"
    assert captured["model"] == "gpt-5"
    assert "temperature" not in captured
    reset_llm_gateway()


@pytest.mark.asyncio
async def test_ai_chat_llm_router_uses_conversation_context_and_trace(monkeypatch):
    class FakeGateway:
        model = "gpt-5"

        def is_ready(self):
            return True

        def status(self):
            return {
                "available": True,
                "configured": True,
                "provider": "litellm",
                "model": self.model,
                "api_key_env": "OPENAI_API_KEY",
                "api_key_configured": True,
                "timeout_seconds": 30.0,
                "max_tokens": 1000,
                "error": None,
            }

        def chat(self, **_kwargs):
            return (
                '{"tool_calls":[{"tool_name":"data.kline_daily",'
                '"arguments":{"symbol":"601318.SH","limit":7},'
                '"reason":"上一轮询问中国平安平均收盘价，本轮确认按7个交易日统计。"}],'
                '"clarification":null,"confidence":0.92}'
            )

    _patch_artifacts(monkeypatch)
    monkeypatch.setattr("app.api.ai.get_llm_gateway", lambda: FakeGateway())
    app.dependency_overrides[get_async_session] = _fake_session
    try:
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            resp = await client.post(
                "/api/ai/chat",
                json={
                    "messages": [
                        {"role": "user", "content": "给我中国平安的过去7天的平均收盘价"},
                        {"role": "assistant", "content": "按交易日还是自然日？"},
                        {"role": "user", "content": "7个交易日"},
                    ],
                    "auto_execute": False,
                },
            )
    finally:
        app.dependency_overrides.clear()

    body = resp.json()
    assert resp.status_code == 200
    action_map = {action["tool_name"]: action["arguments"] for action in body["data"]["actions"]}
    assert action_map["data.kline_daily"]["symbol"] == "601318.SH"
    assert action_map["data.kline_daily"]["limit"] == 7
    assert "live_trading.status" not in action_map

    month_actions = _suggest_actions(
        "给我利通过去一个月的走势，给出你的解读",
        None,
        resolved_symbol="603629.SH",
    )
    month_action_map = {action.tool_name: action.arguments for action in month_actions}
    assert month_action_map["data.kline_daily"]["limit"] == 30
    assert body["data"]["trace"]["source"] == "llm"
    assert body["data"]["trace"]["context"]["routing_text"] == "给我中国平安的过去7天的平均收盘价\n7个交易日"
    assert body["data"]["trace"]["tool_calls"][0]["reason"].startswith("上一轮询问")


@pytest.mark.asyncio
async def test_ai_chat_uses_react_answer_node_after_tool_execution(monkeypatch):
    calls: list[str | None] = []

    class FakeGateway:
        model = "gpt-5"

        def is_ready(self):
            return True

        def status(self):
            return {
                "available": True,
                "configured": True,
                "provider": "litellm",
                "model": self.model,
                "api_key_env": "OPENAI_API_KEY",
                "api_key_configured": True,
                "timeout_seconds": 30.0,
                "max_tokens": 1000,
                "error": None,
            }

        def chat(self, **kwargs):
            calls.append(kwargs.get("system"))
            if len(calls) == 1:
                return (
                    '{"tool_calls":[{"tool_name":"runtime.tasks",'
                    '"arguments":{"include_finished":true},'
                    '"reason":"客户询问运行任务进度，需要读取 runtime task 列表。"}],'
                    '"clarification":null,"confidence":0.9}'
                )
            return "当前运行任务已检查，未发现需要你立即处理的阻塞项。"

    _patch_artifacts(monkeypatch)
    monkeypatch.setattr("app.api.ai.get_llm_gateway", lambda: FakeGateway())
    app.dependency_overrides[get_async_session] = _fake_session
    try:
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            resp = await client.post(
                "/api/ai/chat",
                json={"messages": [{"role": "user", "content": "看一下运行任务进度"}]},
            )
    finally:
        app.dependency_overrides.clear()

    body = resp.json()
    assert resp.status_code == 200
    assert body["data"]["executed_tools"][0]["tool_name"] == "runtime.tasks"
    assert body["data"]["message"]["content"] == "当前运行任务已检查，未发现需要你立即处理的阻塞项。"
    assert body["data"]["trace"]["answer"]["mode"] == "react_answer"
    assert len(calls) == 2


def test_fallback_router_treats_trading_day_as_market_data_not_live_trading():
    actions = _suggest_actions(
        "给我中国平安的过去7天的平均收盘价\n7个交易日",
        None,
        resolved_symbol="601318.SH",
    )
    action_map = {action.tool_name: action.arguments for action in actions}
    assert action_map["data.kline_daily"]["symbol"] == "601318.SH"
    assert action_map["data.kline_daily"]["limit"] == 7
    assert "live_trading.status" not in action_map


def test_fallback_router_covers_expanded_platform_tools():
    system_actions = _suggest_actions("看一下系统健康和缓存状态，任务编号 task_id:abc123456", None)
    system_map = {action.tool_name: action.arguments for action in system_actions}
    assert "system.health" in system_map
    assert "system.cache" in system_map
    assert system_map["runtime.task_detail"]["task_id"] == "abc123456"

    artifact_actions = _suggest_actions("查看对话记录 artifacts，artifact_id:ai-abc123 记录详情", None)
    artifact_map = {action.tool_name: action.arguments for action in artifact_actions}
    assert artifact_map["system.ai_artifacts"]["limit"] == 50
    assert artifact_map["system.ai_artifact_detail"]["artifact_id"] == "ai-abc123"

    explorer_actions = _suggest_actions("查看表 klines_daily 的 schema 字段，distinct 去重值", None)
    explorer_map = {action.tool_name: action.arguments for action in explorer_actions}
    assert explorer_map["explorer.table_schema"]["table_name"] == "klines_daily"
    assert explorer_map["explorer.distinct_values"]["table_name"] == "klines_daily"

    compute_actions = _suggest_actions(
        "表达式校验 表达式: Mean($close, 5)，表达式计算",
        None,
        resolved_symbol="603629.SH",
    )
    compute_map = {action.tool_name: action.arguments for action in compute_actions}
    assert compute_map["compute.validate"]["expression"] == "Mean($close, 5)"
    assert compute_map["compute.evaluate"]["symbols"] == ["603629.SH"]
    assert "start_date" in compute_map["compute.evaluate"]

    compute_batch_actions = _suggest_actions(
        "表达式预计算 表达式: Mean($close, 5)，批量因子计算",
        None,
        resolved_symbol="603629.SH",
    )
    compute_batch_map = {action.tool_name: action.arguments for action in compute_batch_actions}
    assert compute_batch_map["compute.precompute"]["expressions"] == ["Mean($close, 5)"]
    assert compute_batch_map["compute.batch"]["configs"][0]["expression"] == "Mean($close, 5)"

    akshare_actions = _suggest_actions(
        "用 AKShare 查 603629.SH 的日线、个股信息、回测历史和实时快照",
        None,
        resolved_symbol="603629.SH",
    )
    akshare_map = {action.tool_name: action.arguments for action in akshare_actions}
    assert akshare_map["akshare.stock_daily"]["symbol"] == "603629.SH"
    assert akshare_map["akshare.stock_info"]["symbol"] == "603629.SH"
    assert akshare_map["akshare.stock_hist"]["symbols"] == ["603629.SH"]
    assert akshare_map["akshare.stock_spot"]["limit"] == 200

    normal_market_actions = _suggest_actions("查 603629.SH 的日线和行情", None, resolved_symbol="603629.SH")
    normal_market_names = {action.tool_name for action in normal_market_actions}
    assert "data.kline_daily" in normal_market_names
    assert all(not name.startswith("akshare.") for name in normal_market_names)

    stock_detail_actions = _suggest_actions("查 603629.SH 的股票详情和前端详情", None, resolved_symbol="603629.SH")
    stock_detail_map = {action.tool_name: action.arguments for action in stock_detail_actions}
    assert stock_detail_map["data.stock_detail"]["symbol"] == "603629.SH"

    stock_list_actions = _suggest_actions("股票列表 行业:电子 搜索:利通", None)
    stock_list_map = {action.tool_name: action.arguments for action in stock_list_actions}
    assert stock_list_map["data.stock_list"]["page_size"] == 50
    assert stock_list_map["data.stock_list"]["industry"] == "电子"
    assert stock_list_map["data.stock_list"]["search"] == "利通"

    klines_query_actions = _suggest_actions(
        "data.klines_query 查询 603629.SH 分钟K线，最近 30 天",
        None,
        resolved_symbol="603629.SH",
    )
    klines_query_map = {action.tool_name: action.arguments for action in klines_query_actions}
    assert klines_query_map["data.klines_query"]["symbol"] == "603629.SH"
    assert klines_query_map["data.klines_query"]["period"] == "minute"
    assert klines_query_map["data.klines_query"]["page_size"] == 30

    evaluation_actions = _suggest_actions(
        "做 IC分析、分层回测、完整评估和因子看板，表达式: Mean($close, 5)",
        None,
        resolved_symbol="603629.SH",
    )
    evaluation_map = {action.tool_name: action.arguments for action in evaluation_actions}
    assert evaluation_map["evaluation.ic_analysis"]["symbols"] == ["603629.SH"]
    assert evaluation_map["evaluation.quantile_backtest"]["expression"] == "Mean($close, 5)"
    assert "evaluation.full_report" in evaluation_map
    assert "evaluation.board" in evaluation_map

    factor_v2_actions = _suggest_actions(
        "V2 因子模板，Python 因子校验 code: def compute(data, context): return 1，V2 因子校验",
        None,
    )
    factor_v2_map = {action.tool_name: action.arguments for action in factor_v2_actions}
    assert "factor.templates_v2" in factor_v2_map
    assert factor_v2_map["factor.validate_python"]["code"].startswith("def compute")
    assert factor_v2_map["factor.validate_v2"]["expression"] == "$close"

    watchlist_actions = _suggest_actions("group_id:3 603629.SH 加入自选，移除自选，创建自选分组，删除自选分组", None)
    watchlist_map = {action.tool_name: action.arguments for action in watchlist_actions}
    assert watchlist_map["data.watchlist_group_create"]["name"] == "AI 自选分组"
    assert watchlist_map["data.watchlist_group_delete"]["group_id"] == 3
    assert watchlist_map["data.watchlist_stock_add"]["symbol"] == "603629.SH"
    assert watchlist_map["data.watchlist_stock_add"]["group_id"] == 3
    assert watchlist_map["data.watchlist_stock_remove"]["symbol"] == "603629.SH"

    indicator_actions = _suggest_actions(
        "indicator_name:roe 指标分类 指标详情 查询指标 指标选股",
        None,
        resolved_symbol="603629.SH",
    )
    indicator_map = {action.tool_name: action.arguments for action in indicator_actions}
    assert "indicator.categories" in indicator_map
    assert indicator_map["indicator.description"]["name"] == "roe"
    assert indicator_map["indicator.query"]["symbols"] == ["603629.SH"]
    assert indicator_map["indicator.query"]["indicator_names"] == ["roe"]
    assert indicator_map["indicator.screen"]["filters"][0]["indicator_name"] == "roe"

    indicator_compute_actions = _suggest_actions(
        "indicator_name:roe 计算指标 指标财务",
        None,
        resolved_symbol="603629.SH",
    )
    indicator_compute_map = {action.tool_name: action.arguments for action in indicator_compute_actions}
    assert indicator_compute_map["indicator.compute"]["indicator_names"] == ["roe"]
    assert indicator_compute_map["indicator.financial"]["symbol"] == "603629.SH"

    indicator_timeseries_actions = _suggest_actions(
        "indicator_name:roe 批量指标时序",
        None,
        resolved_symbol="603629.SH",
    )
    indicator_timeseries_map = {action.tool_name: action.arguments for action in indicator_timeseries_actions}
    assert indicator_timeseries_map["data.indicator_timeseries_batch"]["symbols"] == ["603629.SH"]
    assert indicator_timeseries_map["data.indicator_timeseries_batch"]["names"] == ["roe"]

    legacy_factor_actions = _suggest_actions(
        "factor_id:7 创建 legacy 因子，更新 legacy 因子，删除 legacy 因子，因子分析记录",
        None,
    )
    legacy_factor_map = {action.tool_name: action.arguments for action in legacy_factor_actions}
    assert legacy_factor_map["factor.create_legacy"]["name"] == "ai_factor_draft"
    assert legacy_factor_map["factor.update_legacy"]["id"] == 7
    assert legacy_factor_map["factor.delete_legacy"]["id"] == 7
    assert legacy_factor_map["factor.analysis_list"]["factor_id"] == 7

    factor_v2_crud_actions = _suggest_actions(
        "factor_id:42 创建 v2 因子，更新 v2 因子，删除 v2 因子，保存因子评估",
        None,
    )
    factor_v2_crud_map = {action.tool_name: action.arguments for action in factor_v2_crud_actions}
    assert factor_v2_crud_map["factor.create_v2"]["name"] == "ai_factor_draft"
    assert factor_v2_crud_map["factor.update_v2"]["factor_id"] == 42
    assert factor_v2_crud_map["factor.delete_v2"]["id"] == 42
    assert factor_v2_crud_map["factor.evaluate_saved"]["factor_id"] == 42

    analysis_detail_actions = _suggest_actions("analysis_id:5 因子分析详情", None)
    analysis_detail_map = {action.tool_name: action.arguments for action in analysis_detail_actions}
    assert analysis_detail_map["factor.analysis_detail"]["id"] == 5

    saved_factor_actions = _suggest_actions(
        "factor_id:42 保存因子预览、保存因子覆盖、保存因子预计算、保存因子分析",
        None,
    )
    saved_factor_map = {action.tool_name: action.arguments for action in saved_factor_actions}
    assert saved_factor_map["factor.preview_saved"]["factor_id"] == 42
    assert saved_factor_map["factor.coverage_saved"]["factor_id"] == 42
    assert saved_factor_map["factor.precompute_saved"]["factor_id"] == 42
    assert saved_factor_map["factor.analyze_saved"]["factor_id"] == 42

    saved_python_factor_actions = _suggest_actions("factor_id:42 运行 Python 因子 run-python", None)
    saved_python_factor_map = {action.tool_name: action.arguments for action in saved_python_factor_actions}
    assert saved_python_factor_map["factor.run_python_saved"]["factor_id"] == 42
    assert saved_python_factor_map["factor.run_python_saved"]["params"] == {}

    factor_actions = _suggest_actions("查询论文因子实验规格、参数哈希 factor_name:small_cap 和因子值查询", None)
    factor_map = {action.tool_name: action.arguments for action in factor_actions}
    assert "factor_value.paper_manifest" in factor_map
    assert "factor_value.paper_experiments" in factor_map
    assert factor_map["factor_value.param_hashes"]["factor_names"] == ["small_cap"]
    assert factor_map["factor_value.query"]["factor_name"] == "small_cap"

    backtest_actions = _suggest_actions("检查 603629.SH 的回测数据覆盖和股票名称映射", None, resolved_symbol="603629.SH")
    backtest_map = {action.tool_name: action.arguments for action in backtest_actions}
    assert backtest_map["backtest.data_coverage"]["symbols"] == ["603629.SH"]
    assert backtest_map["backtest.stock_names"]["symbols"] == ["603629.SH"]

    factor_backtest_actions = _suggest_actions("因子回测 表达式: Mean($close, 5)", None)
    factor_backtest_map = {action.tool_name: action.arguments for action in factor_backtest_actions}
    assert factor_backtest_map["backtest.factor"]["config"]["expression"] == "Mean($close, 5)"
    assert factor_backtest_map["backtest.factor"]["config"]["stock_pool"] == "hs300"

    report_actions = _suggest_actions("task_id:bt123456 回测报告 quantstats", None)
    report_map = {action.tool_name: action.arguments for action in report_actions}
    assert report_map["backtest.task_report"]["task_id"] == "bt123456"

    review_actions = _suggest_actions("给 603629.SH 做个复盘，读取投研上下文", None, resolved_symbol="603629.SH")
    review_map = {action.tool_name: action.arguments for action in review_actions}
    assert review_map["data.review_context"]["symbol"] == "603629.SH"
    assert review_map["data.review_context"]["lookback_days"] == 60

    parquet_actions = _suggest_actions("查看 dataset:klines_daily 的 parquet schema 和数据集字段", None)
    parquet_map = {action.tool_name: action.arguments for action in parquet_actions}
    assert parquet_map["parquet.dataset_schema"]["dataset"] == "klines_daily"

    preset_actions = _suggest_actions("读取双标的网格预设，并写入通用多因子策略和创建科技小市值策略", None)
    preset_map = {action.tool_name: action.arguments for action in preset_actions}
    assert "backtest.preset_dual_stock_grid" in preset_map
    assert preset_map["backtest.create_preset_multi_factor_strategy"]["name"] == "multi_factor"
    assert preset_map["backtest.create_preset_tech_small_cap_strategy"]["name"] == "tech_small_cap"

    index_pool_detail_actions = _suggest_actions("查看 399101.SZ 回测指数池详情和成分覆盖", None)
    index_pool_detail_map = {action.tool_name: action.arguments for action in index_pool_detail_actions}
    assert index_pool_detail_map["backtest.index_pool_detail"]["index_symbol"] == "399101.SZ"

    report_chat_actions = _suggest_actions("创建研报对话会话：低估值高成长，月度调仓", None)
    report_chat_map = {action.tool_name: action.arguments for action in report_chat_actions}
    assert report_chat_map["report.chat_session_create"]["report_filename"] == "copilot-report.txt"
    assert "低估值高成长" in report_chat_map["report.chat_session_create"]["report_text"]

    report_chat_send_actions = _suggest_actions("session_id:sess123 继续研报对话，请生成 AKQuant 代码", None)
    report_chat_send_map = {action.tool_name: action.arguments for action in report_chat_send_actions}
    assert report_chat_send_map["report.chat_session_send"]["session_id"] == "sess123"

    live_actions = _suggest_actions("查看实盘策略配置和周度成交", None)
    live_map = {action.tool_name: action.arguments for action in live_actions}
    assert "live_trading.strategy_profiles" in live_map
    assert "live_trading.weekly_trades" in live_map

    live_detail_actions = _suggest_actions("profile_key:paper_default 查看待处理委托、订单审计和成交记录", None)
    live_detail_map = {action.tool_name: action.arguments for action in live_detail_actions}
    assert live_detail_map["live_trading.pending_orders"]["profile_key"] == "paper_default"
    assert live_detail_map["live_trading.order_audit"]["profile_key"] == "paper_default"
    assert live_detail_map["live_trading.trades"]["profile_key"] == "paper_default"

    ops_actions = _suggest_actions("查看实盘防护和开发数据真实数据目录，取消同步", None)
    ops_map = {action.tool_name: action.arguments for action in ops_actions}
    assert "system.live_trading_guardrails" in ops_map
    assert "system.dev_data_mode" in ops_map
    assert "data.sync_cancel" in ops_map

    precompute_actions = _suggest_actions("因子预计算准备，然后因子集合预计算 small_cap_v4_core", None)
    precompute_map = {action.tool_name: action.arguments for action in precompute_actions}
    assert "factor_value.precompute_prepare" in precompute_map
    assert "factor_value.group_precompute" in precompute_map

    research_actions = _suggest_actions(
        "factor_name:small_cap 因子研究准备，提交因子研究，批量因子研究，因子组合候选",
        None,
    )
    research_map = {action.tool_name: action.arguments for action in research_actions}
    assert research_map["factor_research.prepare"]["factor_name"] == "small_cap"
    assert research_map["factor_research.submit"]["factor_name"] == "small_cap"
    assert research_map["factor_research.batch"]["factor_names"] == ["small_cap"]
    assert research_map["factor_research.combinations"]["factor_names"] == ["small_cap"]

    optimize_actions = _suggest_actions("做一次 grid search 参数优化和 walk-forward 滚动验证，并校验策略参数 schema", None)
    optimize_map = {action.tool_name: action.arguments for action in optimize_actions}
    assert "backtest.optimize_grid" in optimize_map
    assert "backtest.optimize_walk_forward" in optimize_map
    assert "backtest.strategy_params_schema" in optimize_map

    strategy_ops_actions = _suggest_actions(
        "strategy_id:7 趋势资金日度信号、趋势资金信号汇总、趋势资金回测、深度价值回测",
        None,
        resolved_symbol="603629.SH",
    )
    strategy_ops_map = {action.tool_name: action.arguments for action in strategy_ops_actions}
    assert strategy_ops_map["strategy.trend_signals_daily"]["symbols"] == ["603629.SH"]
    assert strategy_ops_map["strategy.trend_signals_summary"]["symbols"] == ["603629.SH"]
    assert strategy_ops_map["strategy.trend_backtest"]["symbols"] == ["603629.SH"]
    assert "strategy.deep_value_backtest" in strategy_ops_map

    backtest_crud_actions = _suggest_actions(
        "strategy_id:7 backtest_id:9 创建传统回测，运行传统回测，删除传统回测，批量删除回测",
        None,
    )
    backtest_crud_map = {action.tool_name: action.arguments for action in backtest_crud_actions}
    assert backtest_crud_map["backtest.create_record"]["strategy_id"] == 7
    assert backtest_crud_map["backtest.run_record"]["id"] == 9
    assert backtest_crud_map["backtest.delete_record"]["id"] == 9
    assert backtest_crud_map["backtest.delete_records_batch"]["ids"] == [9]

    live_control_actions = _suggest_actions(
        "profile_key:smallcap_live 实盘预检，生成实盘信号，启动 runner，停止 runner，提交委托，撤单重报",
        None,
    )
    live_control_map = {action.tool_name: action.arguments for action in live_control_actions}
    assert live_control_map["live_trading.preflight"]["profile_key"] == "smallcap_live"
    assert live_control_map["live_trading.signals"]["profile_key"] == "smallcap_live"
    assert live_control_map["live_trading.runner_start"]["profile_key"] == "smallcap_live"
    assert "live_trading.runner_stop" in live_control_map
    assert live_control_map["live_trading.orders_submit"]["confirm"] is False
    assert "live_trading.orders_cancel_resubmit" in live_control_map

    sentiment_actions = _suggest_actions("查看舆情线程并提交舆情抓取", None)
    sentiment_map = {action.tool_name: action.arguments for action in sentiment_actions}
    assert "sentiment.threads" in sentiment_map
    assert "sentiment.ingest_run" in sentiment_map
