from __future__ import annotations

import os
from datetime import datetime, timedelta

import pytest
from httpx import ASGITransport, AsyncClient

from app.ai.gateway import LLMGatewayError, reset_llm_gateway
from app.api.ai import _stock_symbol_from_text_or_name, _suggest_actions
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
    assert status_resp.json()["data"]["tool_count"] >= 60
    assert tools_resp.status_code == 200
    tool_names = {tool["name"] for tool in tools_resp.json()["data"]}
    assert {
        "system.status",
        "system.tool_catalog",
        "data.stock_snapshot",
        "data.kline_daily",
        "data.kline_minute",
        "data.market_snapshot",
        "data.realtime_quote",
        "data.sync_status",
        "data.watchlist_groups",
        "data.financial",
        "data.indicator_batch",
        "data.index_pool",
        "explorer.tables",
        "parquet.datasets",
        "factor_value.definitions",
        "sentiment.summary",
        "backtest.capabilities",
        "backtest.records",
        "backtest.timer_coverage",
        "backtest.submit",
        "strategy.list",
        "live_trading.status",
        "live_trading.account",
    } <= tool_names


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
    finally:
        app.dependency_overrides.clear()

    body = resp.json()
    assert resp.status_code == 200
    assert body["code"] == 0
    assert body["data"]["status"] == "needs_confirmation"
    assert body["data"]["artifact_id"] == "ai-test-artifact"


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
async def test_ai_chat_gateway_error_returns_offline_response(monkeypatch):
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
    assert body["data"]["offline"] is True
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
