from __future__ import annotations

import asyncio
import os
import re
from datetime import datetime, timedelta
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Path, Query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.ai.answer import synthesize_answer_with_llm
from app.ai.artifacts import create_artifact, get_artifact, list_artifacts, update_artifact
from app.ai.diagnostics import build_ai_diagnostics
from app.ai.gateway import LLMGatewayError, get_llm_gateway, reset_llm_gateway
from app.ai.manifest import build_ai_tool_manifest
from app.ai.router import AIRoutePlan, AIRouterNodeTrace, route_actions_with_llm
from app.ai.schemas import (
    AIActionCard,
    AIChatMessage,
    AIChatRequest,
    AIChatResponse,
    AIConfigResponse,
    AIConfigUpdate,
    AIStatusResponse,
    AIToolExecutionRequest,
    AIToolExecutionResponse,
    AIWorkflowRunRequest,
)
from app.ai.tools import get_ai_tool_registry
from app.ai.workflows import get_ai_workflow, list_ai_workflows, run_ai_workflow
from app.core.config import settings
from app.db.models.stock import Stock
from app.db.sqlite import get_async_session

router = APIRouter()

AI_CONFIG_KEYS = ("AI_ENABLED", "AI_PROVIDER", "AI_MODEL", "AI_BASE_URL", "AI_API_KEY_ENV")
ENV_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _ok(data: Any) -> dict[str, Any]:
    return {"code": 0, "message": "success", "data": data}


def _ai_config_env_path():
    return settings.base_dir / ".env.local"


def _read_env_value(path: Any, key: str) -> str | None:
    if not path.exists():
        return None
    value: str | None = None
    for line in path.read_text(encoding="utf-8").splitlines():
        text = line.strip()
        if not text or text.startswith("#") or "=" not in text:
            continue
        current_key, raw_value = text.split("=", 1)
        if current_key.strip() == key:
            value = raw_value.strip().strip('"').strip("'")
    return value


def _parse_bool_text(value: str | None, default: bool) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _env_bool_text(value: bool) -> str:
    return "true" if value else "false"


def _split_secret_assignment(value: str | None) -> tuple[str | None, str | None]:
    if not value or "=" not in value:
        return None, value
    maybe_name, maybe_secret = value.split("=", 1)
    maybe_name = maybe_name.strip()
    maybe_secret = maybe_secret.strip().strip('"').strip("'")
    if ENV_NAME_RE.fullmatch(maybe_name) and maybe_secret:
        return maybe_name, maybe_secret
    return None, value


def _write_env_values(path: Any, updates: dict[str, str], remove_keys: set[str] | None = None) -> None:
    remove_keys = remove_keys or set()
    path.parent.mkdir(parents=True, exist_ok=True)
    existing_lines = path.read_text(encoding="utf-8").splitlines() if path.exists() else []
    remaining = set(updates)
    output: list[str] = []

    for line in existing_lines:
        text = line.strip()
        if text and not text.startswith("#") and "=" in text:
            key = text.split("=", 1)[0].strip()
            if key in updates:
                if key in remaining:
                    output.append(f"{key}={updates[key]}")
                    remaining.discard(key)
                continue
            if key in remove_keys:
                continue
        output.append(line)

    if remaining:
        if output and output[-1].strip():
            output.append("")
        output.append("# AI Native gateway configuration.")
        for key in AI_CONFIG_KEYS:
            if key in remaining:
                output.append(f"{key}={updates[key]}")
                remaining.discard(key)
        for key in sorted(remaining):
            output.append(f"{key}={updates[key]}")

    path.write_text("\n".join(output).rstrip() + "\n", encoding="utf-8")


def _mask_secret(value: str | None) -> str | None:
    if not value:
        return None
    if len(value) <= 8:
        return "*" * len(value)
    return f"{value[:4]}...{value[-4:]}"


def _api_key_warning(value: str, base_url: str, api_key_env: str) -> str | None:
    if not value:
        return None
    lower_context = f"{base_url} {api_key_env}".lower()
    openai_compatible = "openai" in lower_context or "/v1" in lower_context or "0029" in lower_context
    if openai_compatible and not value.startswith("sk-"):
        return "当前 Key 不是 sk- 开头，OpenAI-compatible 网关可能会返回 Invalid API key"
    return None


def _apply_ai_runtime_config(
    *,
    enabled: bool,
    provider: str,
    model: str,
    base_url: str,
    api_key_env: str,
) -> None:
    changed = (
        settings.ai_enabled != enabled
        or settings.ai_provider != provider
        or settings.ai_model != model
        or settings.ai_base_url != base_url
        or settings.ai_api_key_env != api_key_env
    )
    settings.ai_enabled = enabled
    settings.ai_provider = provider
    settings.ai_model = model
    settings.ai_base_url = base_url
    settings.ai_api_key_env = api_key_env
    if changed:
        reset_llm_gateway()


def _ai_config_payload() -> AIConfigResponse:
    env_path = _ai_config_env_path()
    provider = _read_env_value(env_path, "AI_PROVIDER") or settings.ai_provider
    model = _read_env_value(env_path, "AI_MODEL") or settings.ai_model
    base_url = (_read_env_value(env_path, "AI_BASE_URL") or settings.ai_base_url or "").rstrip("/")
    api_key_env = _read_env_value(env_path, "AI_API_KEY_ENV") or settings.ai_api_key_env
    enabled = _parse_bool_text(_read_env_value(env_path, "AI_ENABLED"), bool(settings.ai_enabled))
    _apply_ai_runtime_config(
        enabled=enabled,
        provider=provider,
        model=model,
        base_url=base_url,
        api_key_env=api_key_env,
    )

    file_key = _read_env_value(env_path, api_key_env)
    process_key = os.getenv(api_key_env, "").strip()
    if not process_key and file_key:
        os.environ[api_key_env] = file_key
        process_key = file_key
        reset_llm_gateway()

    active_key = process_key or file_key or ""
    source = "process" if process_key else ".env.local" if file_key else None
    return AIConfigResponse(
        enabled=enabled,
        provider=provider,
        model=model,
        base_url=base_url or None,
        api_key_env=api_key_env,
        api_key_configured=bool(active_key.strip()),
        api_key_masked=_mask_secret(active_key.strip()),
        api_key_source=source,
        api_key_warning=_api_key_warning(active_key.strip(), base_url, api_key_env),
        env_file=str(env_path),
        requires_restart=False,
        updated_at=datetime.now().isoformat(timespec="seconds"),
        gateway=get_llm_gateway().status(),
    )


def _last_user_text(messages: list[AIChatMessage]) -> str:
    for message in reversed(messages):
        if message.role == "user":
            return message.content.strip()
    return ""


def _routing_text(messages: list[AIChatMessage]) -> str:
    user_texts = [message.content.strip() for message in messages if message.role == "user" and message.content.strip()]
    if not user_texts:
        return ""
    latest = user_texts[-1]
    if len(latest) <= 24 and len(user_texts) >= 2:
        return "\n".join(user_texts[-2:])
    return latest


def _conversation_text(messages: list[AIChatMessage]) -> str:
    rows = [f"{message.role}: {message.content.strip()}" for message in messages[-10:] if message.content.strip()]
    return "\n".join(rows)


def _tool_action(name: str, arguments: dict[str, Any] | None = None) -> AIActionCard:
    return get_ai_tool_registry().get(name).action(arguments)


def _stock_symbol_from_text(text: str) -> str | None:
    match = re.search(r"(?<!\d)([036]\d{5})\.(SH|SZ)(?![A-Z0-9])", text.upper())
    if match:
        return f"{match.group(1)}.{match.group(2)}"
    match = re.search(r"(?<!\d)([036]\d{5})(?!\d)", text)
    if not match:
        return None
    code = match.group(1)
    suffix = "SH" if code.startswith("6") else "SZ"
    return f"{code}.{suffix}"


def _text_needs_symbol_resolution(text: str) -> bool:
    lower = text.lower()
    return any(
        word in lower
        for word in [
            "行情",
            "价格",
            "quote",
            "k线",
            "kline",
            "走势",
            "股票",
            "收盘",
            "均价",
            "平均",
            "交易日",
            "财务",
            "业绩",
            "指标",
            "因子",
            "舆情",
            "情绪",
        ]
    )


async def _stock_symbol_from_text_or_name(text: str, session: AsyncSession) -> str | None:
    symbol = _stock_symbol_from_text(text)
    if symbol or not _text_needs_symbol_resolution(text):
        return symbol
    compact_text = re.sub(r"\s+", "", text)
    try:
        rows = (await session.execute(select(Stock.symbol, Stock.name).where(Stock.name.is_not(None)))).all()
    except Exception:
        return None
    for row in sorted(rows, key=lambda item: len(str(item[1] or "")), reverse=True):
        name = str(row[1] or "").strip()
        if name and name in compact_text:
            return str(row[0])
    prefix_matches: dict[str, list[str]] = {}
    for row in rows:
        symbol_value = str(row[0])
        name = str(row[1] or "").strip()
        for prefix_len in range(min(len(name), 4), 1, -1):
            prefix = name[:prefix_len]
            if prefix and prefix in compact_text:
                prefix_matches.setdefault(prefix, []).append(symbol_value)
                break
    for _prefix, symbols in sorted(prefix_matches.items(), key=lambda item: len(item[0]), reverse=True):
        unique_symbols = sorted(set(symbols))
        if len(unique_symbols) == 1:
            return unique_symbols[0]
    return None


def _date_window_from_text(text: str) -> tuple[str, str] | None:
    today = datetime.now().date()
    if "前天" in text:
        day = today - timedelta(days=2)
        return day.isoformat(), day.isoformat()
    if "昨天" in text or "昨日" in text:
        day = today - timedelta(days=1)
        return day.isoformat(), day.isoformat()
    if "今天" in text or "今日" in text:
        return today.isoformat(), today.isoformat()
    match = re.search(r"(20\d{2})[-/.年](\d{1,2})[-/.月](\d{1,2})日?", text)
    if match:
        day = datetime(int(match.group(1)), int(match.group(2)), int(match.group(3))).date()
        return day.isoformat(), day.isoformat()
    return None


def _bar_limit_from_text(text: str) -> int | None:
    if re.search(r"(过去|最近)?\s*(一|1)\s*个?\s*月", text):
        return 30
    match = re.search(r"(?:过去|最近)?\s*(\d{1,4})\s*个?\s*(?:交易日|天|日)", text)
    if not match:
        return None
    return max(1, min(int(match.group(1)), 5000))


def _suggest_actions(
    text: str,
    page_context: dict[str, Any] | None,
    *,
    resolved_symbol: str | None = None,
) -> list[AIActionCard]:
    lower = text.lower()
    actions: list[AIActionCard] = []
    today = datetime.now().date()
    date_window = _date_window_from_text(text)
    requested_bar_limit = _bar_limit_from_text(text)
    if any(word in lower for word in ["状态", "health", "系统", "后端"]):
        actions.append(_tool_action("system.status"))
    if any(word in lower for word in ["健康", "health", "heartbeat", "存活"]):
        actions.append(_tool_action("system.health"))
    if any(word in lower for word in ["缓存", "cache", "redis"]):
        actions.append(_tool_action("system.cache"))
    if any(word in lower for word in ["dev data", "开发数据", "真实数据目录", "生产数据目录"]):
        actions.append(_tool_action("system.dev_data_mode"))
    if any(word in lower for word in ["实盘防护", "下单护栏", "guardrails", "kill switch", "风控开关"]):
        actions.append(_tool_action("system.live_trading_guardrails"))
    if any(word in lower for word in ["诊断", "diagnostics", "路由", "trace", "artifact", "工具调用"]):
        actions.append(_tool_action("system.ai_diagnostics"))
    artifact_id_match = re.search(r"(?:artifact_id|artifact|记录(?:id|编号))[：:\s]+([A-Za-z0-9_.:-]+)", text, re.IGNORECASE)
    artifact_id = artifact_id_match.group(1) if artifact_id_match else None
    if any(word in lower for word in ["对话记录", "聊天记录", "历史记录", "artifact 列表", "artifacts"]):
        actions.append(_tool_action("system.ai_artifacts", {"limit": 50}))
    if artifact_id and any(word in lower for word in ["artifact", "记录详情", "对话详情"]):
        actions.append(_tool_action("system.ai_artifact_detail", {"artifact_id": artifact_id}))
    if any(word in lower for word in ["工作流", "workflow", "graph", "langgraph", "节点"]):
        actions.append(_tool_action("workflow.catalog"))
    if any(word in lower for word in ["数据", "新鲜", "覆盖", "summary", "总览"]):
        actions.append(_tool_action("system.data_summary"))
    if any(word in lower for word in ["任务", "进度", "running", "runtime", "task"]):
        actions.append(_tool_action("runtime.tasks", {"include_finished": True}))
    task_id = None
    task_match = re.search(r"(?:task[_\s-]?id|任务(?:id|编号)?)[:：\s]*([A-Za-z0-9_.:-]{6,})", text, re.IGNORECASE)
    if task_match:
        task_id = re.sub(
            r"^(?:task[_\s-]?id|任务(?:id|编号)?)[：:\s-]*",
            "",
            task_match.group(1),
            flags=re.IGNORECASE,
        )
        actions.append(_tool_action("runtime.task_detail", {"task_id": task_id}))
    symbol = resolved_symbol or _stock_symbol_from_text(text)
    if symbol:
        actions.append(_tool_action("data.stock_snapshot", {"symbol": symbol}))
        if any(word in lower for word in ["股票详情", "个股详情", "前端详情", "stock detail", "data.stock_detail"]):
            actions.append(_tool_action("data.stock_detail", {"symbol": symbol}))
        wants_quote = any(word in lower for word in ["行情", "价格", "quote", "实时", "盘口"])
        wants_kline = date_window or any(
            word in lower
            for word in ["k线", "kline", "日线", "走势", "ohlc", "历史行情", "收盘", "均价", "平均", "交易日"]
        )
        if wants_kline and date_window:
            start_date, end_date = date_window
            actions.append(
                _tool_action(
                    "data.kline_daily",
                    {
                        "symbol": symbol,
                        "start_date": start_date,
                        "end_date": end_date,
                        "limit": 10,
                    },
                )
            )
        if wants_quote:
            actions.append(_tool_action("data.market_snapshot", {"symbol": symbol, "daily_limit": 5, "include_realtime": True}))
            actions.append(_tool_action("data.realtime_quote", {"symbol": symbol}))
        if wants_kline and not date_window:
            if not any(action.tool_name == "data.market_snapshot" for action in actions):
                actions.append(_tool_action("data.market_snapshot", {"symbol": symbol, "daily_limit": 20, "include_realtime": False}))
            limit = requested_bar_limit or 120
            start_date = today - timedelta(days=max(limit * 3, 30))
            end_date = today.isoformat()
            actions.append(
                _tool_action(
                    "data.kline_daily",
                    {
                        "symbol": symbol,
                        "start_date": start_date.isoformat(),
                        "end_date": end_date,
                        "limit": limit,
                    },
                )
            )
        if any(word in lower for word in ["分钟", "minute", "分时", "timer"]):
            actions.append(_tool_action("data.kline_minute", {"symbol": symbol, "limit": 240}))
        if any(word in lower for word in ["财务", "业绩", "roe", "pe", "pb", "营收", "利润"]):
            actions.append(_tool_action("data.financial", {"symbol": symbol, "report_count": 8}))
        if any(word in lower for word in ["复盘", "投研上下文", "review context", "review_context"]):
            actions.append(_tool_action("data.review_context", {"symbol": symbol, "lookback_days": requested_bar_limit or 60}))
        if any(word in lower for word in ["指标", "因子", "indicator", "factor"]):
            actions.append(_tool_action("data.indicator_batch", {"symbols": [symbol]}))
        if any(word in lower for word in ["舆情", "情绪", "帖子", "雪球", "股吧", "sentiment"]):
            actions.append(_tool_action("sentiment.summary", {"symbol": symbol}))
        if any(word in lower for word in ["k线查询", "k 线查询", "klines query", "data.klines_query", "通用k线", "通用 k 线"]):
            period = "minute" if any(word in lower for word in ["分钟", "minute", "分时"]) else "daily"
            query_args: dict[str, Any] = {
                "symbol": symbol,
                "period": period,
                "page": 1,
                "page_size": min(requested_bar_limit or (240 if period == "minute" else 100), 1000),
            }
            if date_window:
                start_date, end_date = date_window
                query_args["start_date"] = start_date
                query_args["end_date"] = end_date
            elif requested_bar_limit:
                query_args["start_date"] = (today - timedelta(days=max(requested_bar_limit * 3, 30))).isoformat()
                query_args["end_date"] = today.isoformat()
            actions.append(_tool_action("data.klines_query", query_args))
    wants_akshare = any(word in lower for word in ["akshare", "ak share", "ak 数据", "ak外部"])
    ak_start = (today - timedelta(days=90)).strftime("%Y%m%d")
    ak_end = today.strftime("%Y%m%d")
    if wants_akshare:
        ak_symbols = [symbol] if symbol else ["000001.SZ"]
        if any(word in lower for word in ["股票列表", "代码列表", "stock list"]):
            actions.append(_tool_action("akshare.stock_list", {"limit": 500}))
        if any(word in lower for word in ["实时", "快照", "spot"]):
            actions.append(_tool_action("akshare.stock_spot", {"limit": 200}))
        if symbol and any(word in lower for word in ["个股信息", "基础信息", "stock info"]):
            actions.append(_tool_action("akshare.stock_info", {"symbol": symbol}))
        if any(word in lower for word in ["批量日线", "daily batch"]):
            actions.append(
                _tool_action(
                    "akshare.stock_daily_batch",
                    {"symbols": ak_symbols, "start_date": ak_start, "end_date": ak_end, "adjust": "qfq"},
                )
            )
        elif symbol and any(word in lower for word in ["日线", "daily", "k线", "历史行情"]):
            actions.append(
                _tool_action(
                    "akshare.stock_daily",
                    {"symbol": symbol, "start_date": ak_start, "end_date": ak_end, "adjust": "qfq"},
                )
            )
        if any(word in lower for word in ["回测历史", "hist", "akquant 兼容"]):
            actions.append(
                _tool_action(
                    "akshare.stock_hist",
                    {"symbols": ak_symbols, "start_date": ak_start, "end_date": ak_end, "adjust": "qfq"},
                )
            )
    if not wants_akshare and any(word in lower for word in ["股票列表", "股票清单", "分页股票", "股票分页", "stock list"]):
        search_match = re.search(r"(?:search|搜索|关键词)[:：\s]*([^，,。;\s]+)", text, re.IGNORECASE)
        industry_match = re.search(r"(?:industry|行业)[:：\s]*([^，,。;\s]+)", text, re.IGNORECASE)
        stock_list_args: dict[str, Any] = {"page": 1, "page_size": 50}
        if search_match:
            stock_list_args["search"] = search_match.group(1)
        if industry_match:
            stock_list_args["industry"] = industry_match.group(1)
        actions.append(_tool_action("data.stock_list", stock_list_args))
    if any(word in lower for word in ["行情", "k线", "kline", "价格"]) and not symbol:
        actions.append(_tool_action("data.symbols", {"limit": 200}))
    if any(word in lower for word in ["选股", "筛选", "股票池", "screen"]):
        actions.append(_tool_action("data.stock_screen", {"limit": 100}))
    if any(word in lower for word in ["自选", "watchlist"]):
        actions.append(_tool_action("data.watchlist_groups"))
    group_id_match = re.search(r"(?:group_id|分组(?:id|编号)?)[:：\s]*([0-9]+)", text, re.IGNORECASE)
    group_id = int(group_id_match.group(1)) if group_id_match else None
    if any(word in lower for word in ["创建自选", "新建自选", "创建分组", "watchlist group create"]):
        actions.append(_tool_action("data.watchlist_group_create", {"name": "AI 自选分组"}))
    if group_id is not None and any(word in lower for word in ["删除自选分组", "删除分组", "watchlist group delete"]):
        actions.append(_tool_action("data.watchlist_group_delete", {"group_id": group_id}))
    if symbol and group_id is not None and any(word in lower for word in ["加入自选", "添加自选", "加入分组", "watchlist add"]):
        actions.append(_tool_action("data.watchlist_stock_add", {"group_id": group_id, "symbol": symbol}))
    if symbol and group_id is not None and any(word in lower for word in ["移除自选", "删除自选股", "移出分组", "watchlist remove"]):
        actions.append(_tool_action("data.watchlist_stock_remove", {"group_id": group_id, "symbol": symbol}))
    if any(word in lower for word in ["行业", "申万"]):
        actions.append(_tool_action("data.industries"))
    if any(word in lower for word in ["指数", "成分", "399101", "中小综指", "小市值"]):
        index_symbol = _stock_symbol_from_text(text) or "399101.SZ"
        actions.append(_tool_action("data.index_catalog", {"pool_only": True}))
        actions.append(_tool_action("data.index_pool", {"index_symbol": index_symbol}))
        if any(word in lower for word in ["回测指数池详情", "指数池详情", "index pool detail", "backtest.index_pool_detail"]):
            actions.append(_tool_action("backtest.index_pool_detail", {"index_symbol": index_symbol}))
    expression_match = re.search(r"(?:表达式|expression|expr)[:：]\s*([^\n]+)", text, re.IGNORECASE)
    expression_text = expression_match.group(1).strip() if expression_match else None
    if expression_text:
        expression_text = re.split(r"[，；;。]", expression_text, maxsplit=1)[0].strip()
    default_start_date = (today - timedelta(days=90)).isoformat()
    default_end_date = today.isoformat()
    factor_name_match = re.search(r"(?:factor_name|因子名)[:：\s]*([A-Za-z0-9_.:-]+)", text, re.IGNORECASE)
    factor_id_match = re.search(r"(?:factor_id|因子(?:id|编号)?)[:：\s]*([0-9]+)", text, re.IGNORECASE)
    factor_id = int(factor_id_match.group(1)) if factor_id_match else None
    analysis_id_match = re.search(r"(?:analysis_id|分析(?:id|编号)?)[:：\s]*([0-9]+)", text, re.IGNORECASE)
    analysis_id = int(analysis_id_match.group(1)) if analysis_id_match else None
    if any(word in lower for word in ["因子", "factor", "算子", "指标"]):
        actions.append(_tool_action("indicator.catalog"))
        actions.append(_tool_action("compute.operators"))
        actions.append(_tool_action("factor_value.definitions"))
    indicator_name_match = re.search(r"(?:indicator_name|指标名|指标)[:：\s]*([A-Za-z0-9_.:-]+)", text, re.IGNORECASE)
    indicator_name = indicator_name_match.group(1) if indicator_name_match else "roe"
    if any(word in lower for word in ["指标分类", "indicator categories"]):
        actions.append(_tool_action("indicator.categories"))
    if any(word in lower for word in ["指标详情", "指标描述", "indicator description"]):
        actions.append(_tool_action("indicator.description", {"name": indicator_name}))
    if any(word in lower for word in ["查询指标", "指标值查询", "indicator.query"]):
        actions.append(
            _tool_action(
                "indicator.query",
                {
                    "symbols": [symbol] if symbol else ["000001.SZ"],
                    "indicator_names": [indicator_name],
                    "trade_date": default_end_date,
                },
            )
        )
    if any(word in lower for word in ["计算指标", "指标计算", "indicator.compute"]):
        actions.append(
            _tool_action(
                "indicator.compute",
                {
                    "indicator_names": [indicator_name] if indicator_name else None,
                    "symbols": [symbol] if symbol else None,
                    "full_compute": False,
                },
            )
        )
    if any(word in lower for word in ["指标选股", "indicator screen"]):
        actions.append(
            _tool_action(
                "indicator.screen",
                {
                    "filters": [{"indicator_name": indicator_name, "op": ">=", "value": 0}],
                    "trade_date": default_end_date,
                    "limit": 50,
                },
            )
        )
    if symbol and any(word in lower for word in ["指标财务", "indicator financial"]):
        actions.append(_tool_action("indicator.financial", {"symbol": symbol, "report_count": 8}))
    if any(
        word in lower
        for word in [
            "批量指标时序",
            "指标时序批量",
            "indicator timeseries batch",
            "data.indicator_timeseries_batch",
        ]
    ):
        actions.append(
            _tool_action(
                "data.indicator_timeseries_batch",
                {
                    "symbols": [symbol] if symbol else ["000001.SZ"],
                    "names": [indicator_name],
                    "start_date": default_start_date,
                    "end_date": default_end_date,
                    "limit": 200000,
                },
            )
        )
    if any(word in lower for word in ["表达式校验", "校验表达式", "validate expression", "compute.validate"]):
        actions.append(_tool_action("compute.validate", {"expression": expression_text or "$close"}))
    if any(word in lower for word in ["表达式计算", "计算表达式", "evaluate expression", "compute.evaluate"]):
        arguments: dict[str, Any] = {
            "expression": expression_text or "$close",
            "symbols": [symbol] if symbol else ["000001.SZ"],
            "start_date": default_start_date,
            "end_date": default_end_date,
        }
        actions.append(_tool_action("compute.evaluate", arguments))
    if any(word in lower for word in ["表达式选股", "条件表达式", "compute.screen"]):
        actions.append(
            _tool_action(
                "compute.screen",
                {
                    "condition": expression_text or "$close > 0",
                    "trade_date": default_end_date,
                    "limit": 50,
                },
            )
        )
    if any(word in lower for word in ["表达式预计算", "compute.precompute", "precompute expression"]):
        actions.append(
            _tool_action(
                "compute.precompute",
                {
                    "expressions": [expression_text or "$close"],
                    "symbols": [symbol] if symbol else ["000001.SZ"],
                    "start_date": default_start_date,
                    "end_date": default_end_date,
                    "engine": "builtin",
                },
            )
        )
    if any(word in lower for word in ["批量因子计算", "compute.batch", "batch compute"]):
        actions.append(
            _tool_action(
                "compute.batch",
                {
                    "configs": [
                        {
                            "expression": expression_text or "$close",
                            "stock_pool": "hs300",
                            "start_date": default_start_date,
                            "end_date": default_end_date,
                            "benchmark": "000300.SH",
                            "direction": "desc",
                        }
                    ]
                },
            )
        )
    eval_args = {
        "expression": expression_text or "$close",
        "symbols": [symbol] if symbol else ["000001.SZ"],
        "start_date": default_start_date,
        "end_date": default_end_date,
    }
    if any(word in lower for word in ["ic分析", "ic 分析", "ic analysis", "evaluation.ic_analysis"]):
        actions.append(_tool_action("evaluation.ic_analysis", eval_args))
    if any(word in lower for word in ["分层回测", "quantile backtest", "evaluation.quantile_backtest"]):
        actions.append(_tool_action("evaluation.quantile_backtest", {**eval_args, "n_groups": 5, "rebalance_freq": "monthly"}))
    if any(word in lower for word in ["完整评估", "完整报告", "full report", "evaluation.full_report"]):
        actions.append(_tool_action("evaluation.full_report", {**eval_args, "n_groups": 5, "rebalance_freq": "monthly"}))
    if any(word in lower for word in ["六模块", "因子评估报告", "evaluation.report"]):
        actions.append(
            _tool_action(
                "evaluation.report",
                {
                    "config": {
                        "expression": expression_text or "$close",
                        "stock_pool": "hs300",
                        "start_date": default_start_date,
                        "end_date": default_end_date,
                        "benchmark": "000300.SH",
                        "direction": "desc",
                    },
                    "eval_config": {"group_count": 5},
                },
            )
        )
    if any(word in lower for word in ["因子看板", "factor board", "evaluation.board"]):
        actions.append(_tool_action("evaluation.board", {"query": {}}))
    if any(word in lower for word in ["v2因子模板", "v2 因子模板", "factor.templates_v2"]):
        actions.append(_tool_action("factor.templates_v2"))
    code_match = re.search(r"(?:代码|code)[:：]\s*(.+)", text, re.IGNORECASE | re.DOTALL)
    if any(word in lower for word in ["python因子校验", "python 因子校验", "validate-python", "factor.validate_python"]):
        actions.append(_tool_action("factor.validate_python", {"code": code_match.group(1).strip() if code_match else ""}))
    if any(word in lower for word in ["v2因子校验", "v2 因子校验", "保存因子校验", "factor.validate_v2"]):
        actions.append(_tool_action("factor.validate_v2", {"expression": expression_text or "$close", "stock_pool": "hs300"}))
    if any(word in lower for word in ["创建 legacy 因子", "legacy 因子创建", "factor.create_legacy"]):
        actions.append(
            _tool_action(
                "factor.create_legacy",
                {
                    "name": "ai_factor_draft",
                    "category": "custom",
                    "source": "custom",
                    "code": expression_text or "$close",
                    "parameters": {"expression": expression_text or "$close"},
                },
            )
        )
    if factor_id is not None and any(word in lower for word in ["更新 legacy 因子", "legacy 因子更新", "factor.update_legacy"]):
        actions.append(_tool_action("factor.update_legacy", {"id": factor_id}))
    if factor_id is not None and any(word in lower for word in ["删除 legacy 因子", "legacy 因子删除", "factor.delete_legacy"]):
        actions.append(_tool_action("factor.delete_legacy", {"id": factor_id}))
    if any(word in lower for word in ["因子分析记录", "analysis list", "factor.analysis_list"]):
        actions.append(_tool_action("factor.analysis_list", {"factor_id": factor_id, "limit": 20}))
    if analysis_id is not None and any(word in lower for word in ["因子分析详情", "analysis detail", "factor.analysis_detail"]):
        actions.append(_tool_action("factor.analysis_detail", {"id": analysis_id}))
    if any(word in lower for word in ["创建 v2 因子", "v2 因子创建", "factor.create_v2"]):
        actions.append(
            _tool_action(
                "factor.create_v2",
                {
                    "name": "ai_factor_draft",
                    "expression": expression_text or "$close",
                    "stock_pool": "hs300",
                    "source_type": "dsl",
                    "engine": "builtin",
                },
            )
        )
    if factor_id is not None and any(word in lower for word in ["更新 v2 因子", "v2 因子更新", "factor.update_v2"]):
        actions.append(_tool_action("factor.update_v2", {"factor_id": factor_id}))
    if factor_id is not None and any(word in lower for word in ["删除 v2 因子", "v2 因子删除", "factor.delete_v2"]):
        actions.append(_tool_action("factor.delete_v2", {"id": factor_id}))
    if any(word in lower for word in ["因子值", "factor value", "覆盖率", "缓存"]):
        actions.append(_tool_action("factor_value.definitions"))
        actions.append(_tool_action("factor_value.groups"))
    if factor_id is not None and any(word in lower for word in ["保存因子预览", "因子预览", "preview saved", "factor.preview_saved"]):
        actions.append(
            _tool_action(
                "factor.preview_saved",
                {
                    "factor_id": factor_id,
                    "start_date": default_start_date,
                    "end_date": default_end_date,
                    "limit": 200,
                },
            )
        )
    if factor_id is not None and any(
        word in lower
        for word in ["运行保存的 python 因子", "运行 python 因子", "python 因子运行", "run-python", "factor.run_python_saved"]
    ):
        actions.append(
            _tool_action(
                "factor.run_python_saved",
                {
                    "factor_id": factor_id,
                    "symbols": [symbol] if symbol else None,
                    "start_date": default_start_date,
                    "end_date": default_end_date,
                    "limit": 200,
                    "params": {},
                },
            )
        )
    if factor_id is not None and any(word in lower for word in ["保存因子评估", "因子评估", "evaluate saved", "factor.evaluate_saved"]):
        actions.append(
            _tool_action(
                "factor.evaluate_saved",
                {
                    "factor_id": factor_id,
                    "start_date": default_start_date,
                    "end_date": default_end_date,
                    "n_groups": 5,
                },
            )
        )
    if factor_id is not None and any(word in lower for word in ["保存因子预计算", "factor.precompute_saved"]):
        actions.append(
            _tool_action(
                "factor.precompute_saved",
                {
                    "factor_id": factor_id,
                    "start_date": default_start_date,
                    "end_date": default_end_date,
                },
            )
        )
    if factor_id is not None and any(word in lower for word in ["保存因子覆盖", "因子覆盖", "factor.coverage_saved"]):
        actions.append(
            _tool_action(
                "factor.coverage_saved",
                {
                    "factor_id": factor_id,
                    "start_date": default_start_date,
                    "end_date": default_end_date,
                },
            )
        )
    if factor_id is not None and any(word in lower for word in ["保存因子分析", "因子分析", "factor.analyze_saved"]):
        actions.append(
            _tool_action(
                "factor.analyze_saved",
                {
                    "factor_id": factor_id,
                    "start_date": default_start_date,
                    "end_date": default_end_date,
                    "n_groups": 5,
                },
            )
        )
    if any(word in lower for word in ["参数哈希", "param hash", "param_hash"]):
        actions.append(_tool_action("factor_value.param_hashes", {"factor_names": [factor_name_match.group(1) if factor_name_match else "small_cap"]}))
    if any(word in lower for word in ["因子值查询", "查询因子值", "factor_value.query"]):
        actions.append(
            _tool_action(
                "factor_value.query",
                {"factor_name": factor_name_match.group(1) if factor_name_match else "small_cap", "trade_date": default_end_date},
            )
        )
    if any(word in lower for word in ["论文因子", "paper factor", "paper_feature", "实验规格"]):
        actions.append(_tool_action("factor_value.paper_manifest"))
        actions.append(_tool_action("factor_value.paper_experiments"))
    if any(word in lower for word in ["特征快照", "feature snapshot"]):
        actions.append(_tool_action("factor_value.paper_feature_snapshot"))
    factor_names_match = re.search(r"(?:factor_names|因子列表)[:：\s]*([A-Za-z0-9_.,:; -]+)", text, re.IGNORECASE)
    if factor_names_match:
        factor_names = [
            name.strip()
            for name in re.split(r"[,，;；\s]+", factor_names_match.group(1))
            if name.strip()
        ]
    else:
        factor_names = [factor_name_match.group(1)] if factor_name_match else ["small_cap"]
    research_args = {
        "factor_name": factor_names[0],
        "stock_pool_value": "zz500",
        "start_date": default_start_date,
        "end_date": default_end_date,
    }
    if any(word in lower for word in ["因子研究准备", "research prepare", "factor_research.prepare"]):
        actions.append(_tool_action("factor_research.prepare", research_args))
    if any(word in lower for word in ["提交因子研究", "运行因子研究", "research submit", "factor_research.submit"]):
        actions.append(_tool_action("factor_research.submit", research_args))
    if any(word in lower for word in ["批量因子研究", "research batch", "factor_research.batch"]):
        actions.append(
            _tool_action(
                "factor_research.batch",
                {
                    "factor_names": factor_names,
                    "stock_pool_value": "zz500",
                    "start_date": default_start_date,
                    "end_date": default_end_date,
                },
            )
        )
    if any(word in lower for word in ["因子组合候选", "因子组合", "research combinations", "factor_research.combinations"]):
        actions.append(_tool_action("factor_research.combinations", {"factor_names": factor_names, "limit": 200}))
    if any(word in lower for word in ["数据覆盖", "覆盖检查"]):
        actions.append(
            _tool_action(
                "backtest.data_coverage",
                {
                    "engine": "builtin",
                    "symbols": [symbol] if symbol else [],
                    "start_date": (today - timedelta(days=365)).isoformat(),
                    "end_date": default_end_date,
                    "bar_type": "daily",
                },
            )
        )
    if any(word in lower for word in ["股票名称", "名称映射", "stock names"]):
        actions.append(_tool_action("backtest.stock_names", {"symbols": [symbol] if symbol else []}))
    if any(word in lower for word in ["数据浏览", "explorer", "表", "sql", "schema", "字段", "列名"]):
        actions.append(_tool_action("explorer.tables"))
    table_match = re.search(r"(?:表|table)[:：\s]*([A-Za-z0-9_./-]+)", text, re.IGNORECASE)
    if any(word in lower for word in ["schema", "字段", "列名", "表结构"]):
        actions.append(_tool_action("explorer.table_schema", {"table_name": table_match.group(1) if table_match else "klines_daily"}))
    if any(word in lower for word in ["distinct", "去重值", "枚举值"]):
        actions.append(
            _tool_action(
                "explorer.distinct_values",
                {"table_name": table_match.group(1) if table_match else "stocks", "column": "symbol"},
            )
        )
    if any(word in lower for word in ["表搜索", "搜索表", "quick_search", "table search"]):
        actions.append(_tool_action("explorer.table_search", {"table_name": table_match.group(1) if table_match else "stocks", "page_size": 20}))
    if any(word in lower for word in ["parquet", "数据湖", "数据集"]):
        actions.append(_tool_action("parquet.datasets"))
    dataset_match = re.search(r"(?:dataset|数据集)[:：\s]*([A-Za-z0-9_./-]+)", text, re.IGNORECASE)
    if any(word in lower for word in ["parquet schema", "数据集 schema", "数据集字段", "数据集列名"]):
        actions.append(_tool_action("parquet.dataset_schema", {"dataset": dataset_match.group(1) if dataset_match else "klines_daily"}))
    if any(word in lower for word in ["同步", "sync"]):
        actions.append(_tool_action("data.sync_status"))
        actions.append(_tool_action("data.sync_catalog"))
        actions.append(_tool_action("data.sync_logs", {"limit": 50}))
        actions.append(_tool_action("data.sync_submit", {"sync_type": "stock_info", "sync_mode": "range"}))
    if any(word in lower for word in ["取消同步", "停止同步", "cancel sync", "sync cancel"]):
        actions.append(_tool_action("data.sync_cancel"))
    if any(word in lower for word in ["取消全部同步", "停止所有同步", "cancel all sync", "sync cancel all"]):
        actions.append(_tool_action("data.sync_cancel_all"))
    if any(word in lower for word in ["回测", "backtest"]):
        actions.append(_tool_action("backtest.capabilities"))
        actions.append(_tool_action("backtest.engines"))
        actions.append(_tool_action("backtest.records"))
        actions.append(_tool_action("backtest.submit"))
    if any(word in lower for word in ["因子回测", "factor backtest", "backtest.factor"]):
        actions.append(
            _tool_action(
                "backtest.factor",
                {
                    "config": {
                        "expression": expression_text or "$close",
                        "stock_pool": "hs300",
                        "start_date": default_start_date,
                        "end_date": default_end_date,
                        "benchmark": "000300.SH",
                        "direction": "desc",
                    },
                    "bt_config": {"rebalance_period": "monthly"},
                },
            )
        )
    if task_id and any(word in lower for word in ["回测报告", "html report", "quantstats", "backtest report"]):
        actions.append(_tool_action("backtest.task_report", {"task_id": task_id}))
    if any(word in lower for word in ["双标的网格预设", "底仓网格预设", "dual stock grid preset"]):
        actions.append(_tool_action("backtest.preset_dual_stock_grid"))
    if any(word in lower for word in ["写入双标的网格", "创建双标的网格策略", "dual stock grid strategy"]):
        actions.append(_tool_action("backtest.create_preset_dual_stock_grid_strategy", {"name": "dual_stock_grid"}))
    if any(word in lower for word in ["写入通用多因子", "创建通用多因子策略", "multi factor strategy"]):
        actions.append(_tool_action("backtest.create_preset_multi_factor_strategy", {"name": "multi_factor"}))
    if any(word in lower for word in ["写入科技小市值", "创建科技小市值策略", "tech small cap strategy"]):
        actions.append(_tool_action("backtest.create_preset_tech_small_cap_strategy", {"name": "tech_small_cap"}))
    if any(word in lower for word in ["grid search", "网格搜索", "参数优化"]):
        actions.append(_tool_action("backtest.optimize_grid"))
    if any(word in lower for word in ["walk-forward", "walk forward", "滚动验证", "走步验证"]):
        actions.append(_tool_action("backtest.optimize_walk_forward"))
    if any(word in lower for word in ["策略参数 schema", "参数 schema", "参数表单"]):
        actions.append(_tool_action("backtest.strategy_params_schema"))
    if any(word in lower for word in ["策略参数校验", "参数校验", "validate params"]):
        actions.append(_tool_action("backtest.strategy_params_validate"))
    if any(word in lower for word in ["取消回测", "停止回测", "cancel backtest"]):
        actions.append(_tool_action("backtest.task_cancel"))
    if any(word in lower for word in ["预定义股票池", "回测股票池", "pool symbols"]):
        actions.append(_tool_action("backtest.pool_symbols"))
    if any(word in lower for word in ["策略", "strategy"]):
        actions.append(_tool_action("strategy.list"))
    strategy_id_match = re.search(r"(?:strategy_id|策略(?:id|编号)?)[:：\s]*([0-9]+)", text, re.IGNORECASE)
    strategy_id = int(strategy_id_match.group(1)) if strategy_id_match else None
    backtest_id_match = re.search(r"(?:backtest_id|回测(?:id|编号)?)[:：\s]*([0-9]+)", text, re.IGNORECASE)
    backtest_id = int(backtest_id_match.group(1)) if backtest_id_match else None
    session_id_match = re.search(r"(?:session_id|会话(?:id|编号)?)[:：\s]*([A-Za-z0-9_.:-]+)", text, re.IGNORECASE)
    session_id = session_id_match.group(1) if session_id_match else None
    strategy_window = {
        "start_date": (today - timedelta(days=90)).isoformat(),
        "end_date": default_end_date,
        "symbols": [symbol] if symbol else None,
        "use_watchlist": False,
    }
    if any(word in lower for word in ["创建策略", "strategy.create"]):
        actions.append(_tool_action("strategy.create", {"name": "AI 生成策略草稿", "code": "# TODO: fill strategy code"}))
    if strategy_id is not None and any(word in lower for word in ["更新策略", "strategy.update"]):
        actions.append(_tool_action("strategy.update", {"id": strategy_id}))
    if strategy_id is not None and any(word in lower for word in ["删除策略", "strategy.delete"]):
        actions.append(_tool_action("strategy.delete", {"id": strategy_id}))
    if any(word in lower for word in ["趋势资金日度信号", "日度信号", "trend signals daily", "strategy.trend_signals_daily"]):
        actions.append(_tool_action("strategy.trend_signals_daily", strategy_window))
    if any(word in lower for word in ["趋势资金信号汇总", "信号汇总", "trend signals summary", "strategy.trend_signals_summary"]):
        actions.append(_tool_action("strategy.trend_signals_summary", strategy_window))
    if any(word in lower for word in ["趋势资金回测", "趋势资金组合回测", "trend backtest", "strategy.trend_backtest"]):
        actions.append(_tool_action("strategy.trend_backtest", strategy_window))
    if any(word in lower for word in ["深度价值回测", "deep value", "strategy.deep_value_backtest"]):
        actions.append(
            _tool_action(
                "strategy.deep_value_backtest",
                {
                    "start_date": (today - timedelta(days=365 * 3)).isoformat(),
                    "end_date": default_end_date,
                    "initial_capital": 1_000_000,
                    "pool": "all",
                },
            )
        )
    if any(word in lower for word in ["创建研报对话", "研报对话会话", "report chat session"]):
        actions.append(
            _tool_action(
                "report.chat_session_create",
                {
                    "report_text": text.strip(),
                    "report_filename": "copilot-report.txt",
                },
            )
        )
    if session_id and any(word in lower for word in ["发送研报对话", "继续研报对话", "report chat send"]):
        actions.append(
            _tool_action(
                "report.chat_session_send",
                {
                    "session_id": session_id,
                    "message": text.strip(),
                },
            )
        )
    if any(word in lower for word in ["创建传统回测", "创建回测记录", "backtest.create_record"]):
        actions.append(
            _tool_action(
                "backtest.create_record",
                {
                    "strategy_id": strategy_id or 1,
                    "start_date": (today - timedelta(days=365)).isoformat(),
                    "end_date": default_end_date,
                    "initial_capital": "1000000",
                },
            )
        )
    if backtest_id is not None and any(word in lower for word in ["运行传统回测", "运行回测记录", "backtest.run_record"]):
        actions.append(_tool_action("backtest.run_record", {"id": backtest_id}))
    if backtest_id is not None and any(word in lower for word in ["删除传统回测", "删除回测记录", "backtest.delete_record"]):
        actions.append(_tool_action("backtest.delete_record", {"id": backtest_id}))
    if any(word in lower for word in ["批量删除回测", "backtest.delete_records_batch"]):
        actions.append(_tool_action("backtest.delete_records_batch", {"ids": [backtest_id] if backtest_id else []}))
    if any(word in lower for word in ["因子预计算准备", "预计算准备", "precompute prepare"]):
        actions.append(_tool_action("factor_value.precompute_prepare"))
    if any(word in lower for word in ["因子预计算", "precompute factor", "factor precompute"]):
        actions.append(_tool_action("factor_value.precompute"))
    if any(word in lower for word in ["因子集合预计算", "分组预计算", "group precompute"]):
        actions.append(_tool_action("factor_value.group_precompute"))
    if any(word in lower for word in ["舆情线程", "主题线程", "sentiment threads"]):
        actions.append(_tool_action("sentiment.threads"))
    if any(word in lower for word in ["舆情抓取", "情绪抓取", "sentiment ingest", "抓帖子"]):
        actions.append(_tool_action("sentiment.ingest_run"))
    live_lower = lower.replace("交易日", "")
    if any(word in live_lower for word in ["实盘", "下单", "qmt", "runner", "账户", "持仓", "委托", "成交", "撤单"]):
        actions.append(_tool_action("live_trading.status"))
        actions.append(_tool_action("live_trading.account", {"mode": "paper"}))
    live_profile_intent_text = live_lower.replace("profile_key", "")
    profile_match = re.search(r"(?:profile_key|profile|策略配置)[:：\s]*([A-Za-z0-9_.:-]+)", text, re.IGNORECASE)
    profile_key = profile_match.group(1) if profile_match else None
    if any(word in live_profile_intent_text for word in ["实盘策略", "策略配置", "profile"]):
        actions.append(_tool_action("live_trading.strategy_profiles"))
    if any(word in live_lower for word in ["周度成交", "weekly trade", "weekly trades"]):
        actions.append(_tool_action("live_trading.weekly_trades"))
    if any(word in live_lower for word in ["待处理订单", "待处理委托", "pending orders"]):
        actions.append(_tool_action("live_trading.pending_orders", {"profile_key": profile_key, "mode": "live", "limit": 100}))
    if any(word in live_lower for word in ["订单审计", "委托审计", "order audit"]):
        actions.append(_tool_action("live_trading.order_audit", {"profile_key": profile_key, "mode": None, "limit": 100}))
    if any(word in live_lower for word in ["成交记录", "交易记录", "trade records", "trades"]):
        actions.append(_tool_action("live_trading.trades", {"profile_key": profile_key, "mode": None, "limit": 100}))
    if any(word in live_lower for word in ["初始化账户", "account initialize", "account_initialize"]):
        actions.append(
            _tool_action(
                "live_trading.account_initialize",
                {"profile_key": profile_key, "mode": "paper", "capital": 1_000_000, "reset_existing": False},
            )
        )
    if any(word in live_profile_intent_text for word in ["创建实盘策略配置", "创建 profile", "profile create"]):
        actions.append(
            _tool_action(
                "live_trading.profile_create",
                {"strategy_id": strategy_id or 1, "profile_key": profile_key or "ai_profile_draft"},
            )
        )
    if any(word in live_profile_intent_text for word in ["更新实盘策略配置", "更新 profile", "profile update"]) and profile_key:
        actions.append(_tool_action("live_trading.profile_update", {"profile_key": profile_key}))
    if any(word in live_lower for word in ["实盘预检", "策略预检", "preflight"]):
        actions.append(_tool_action("live_trading.preflight", {"profile_key": profile_key, "mode": "paper"}))
    if any(word in live_lower for word in ["生成实盘信号", "生成策略信号", "live signals"]):
        actions.append(_tool_action("live_trading.signals", {"profile_key": profile_key, "mode": "paper"}))
    if any(word in live_lower for word in ["启动 runner", "runner start"]):
        actions.append(_tool_action("live_trading.runner_start", {"profile_key": profile_key, "mode": "paper"}))
    if any(word in live_lower for word in ["停止 runner", "runner stop"]):
        actions.append(_tool_action("live_trading.runner_stop"))
    if any(word in live_lower for word in ["人工接管", "runner takeover", "takeover"]):
        actions.append(_tool_action("live_trading.runner_takeover", {"reason": "human takeover from Copilot"}))
    if any(word in live_lower for word in ["同步委托", "订单同步", "orders sync"]):
        actions.append(_tool_action("live_trading.orders_sync", {"profile_key": profile_key, "mode": "live"}))
    if any(word in live_lower for word in ["提交委托", "提交订单", "orders submit"]):
        actions.append(_tool_action("live_trading.orders_submit", {"mode": "live", "orders": [], "confirm": False}))
    if any(word in live_lower for word in ["撤单重报", "cancel resubmit"]):
        actions.append(_tool_action("live_trading.orders_cancel_resubmit", {"profile_key": profile_key, "mode": "live"}))
    elif any(word in live_lower for word in ["撤单", "取消委托", "orders cancel"]):
        actions.append(_tool_action("live_trading.orders_cancel", {"profile_key": profile_key, "mode": "live", "confirm": False}))
    if any(word in live_lower for word in ["本地关闭委托", "关闭本地委托", "close local"]):
        actions.append(_tool_action("live_trading.orders_close_local", {"profile_key": profile_key, "mode": "live", "confirm": False}))
    route_path = str((page_context or {}).get("path") or "")
    if route_path.startswith("/trade") and not any(action.tool_name == "live_trading.status" for action in actions):
        actions.append(_tool_action("live_trading.status"))
    if route_path.startswith("/data/sync") and not any(action.tool_name == "data.sync_status" for action in actions):
        actions.append(_tool_action("data.sync_status"))
    if route_path.startswith("/watchlist") and not any(action.tool_name == "data.watchlist_groups" for action in actions):
        actions.append(_tool_action("data.watchlist_groups"))
    if route_path.startswith("/explorer") and not any(action.tool_name == "explorer.tables" for action in actions):
        actions.append(_tool_action("explorer.tables"))
    if route_path.startswith("/backtest") and not any(action.tool_name == "backtest.records" for action in actions):
        actions.append(_tool_action("backtest.records"))
    deduped: list[AIActionCard] = []
    seen: set[str] = set()
    for action in actions:
        if action.tool_name in seen:
            continue
        seen.add(action.tool_name)
        deduped.append(action)
    priority_names: list[str] = []
    if any(word in lower for word in ["对话记录", "聊天记录", "历史记录", "artifact 列表", "artifacts"]):
        priority_names.append("system.ai_artifacts")
    if any(word in lower for word in ["artifact", "记录详情", "对话详情"]):
        priority_names.append("system.ai_artifact_detail")
    if any(word in lower for word in ["创建自选", "新建自选", "创建分组", "watchlist group create"]):
        priority_names.append("data.watchlist_group_create")
    if any(word in lower for word in ["删除自选分组", "删除分组", "watchlist group delete"]):
        priority_names.append("data.watchlist_group_delete")
    if any(word in lower for word in ["加入自选", "添加自选", "加入分组", "watchlist add"]):
        priority_names.append("data.watchlist_stock_add")
    if any(word in lower for word in ["移除自选", "删除自选股", "移出分组", "watchlist remove"]):
        priority_names.append("data.watchlist_stock_remove")
    if any(word in lower for word in ["指标分类", "indicator categories"]):
        priority_names.append("indicator.categories")
    if any(word in lower for word in ["指标详情", "指标描述", "indicator description"]):
        priority_names.append("indicator.description")
    if any(word in lower for word in ["查询指标", "指标值查询", "indicator.query"]):
        priority_names.append("indicator.query")
    if any(word in lower for word in ["计算指标", "指标计算", "indicator.compute"]):
        priority_names.append("indicator.compute")
    if any(word in lower for word in ["指标选股", "indicator screen"]):
        priority_names.append("indicator.screen")
    if any(word in lower for word in ["指标财务", "indicator financial"]):
        priority_names.append("indicator.financial")
    if any(
        word in lower
        for word in [
            "批量指标时序",
            "指标时序批量",
            "indicator timeseries batch",
            "data.indicator_timeseries_batch",
        ]
    ):
        priority_names.append("data.indicator_timeseries_batch")
    if any(word in lower for word in ["复盘", "投研上下文", "review context", "review_context"]):
        priority_names.append("data.review_context")
    if any(word in lower for word in ["股票详情", "个股详情", "前端详情", "stock detail", "data.stock_detail"]):
        priority_names.append("data.stock_detail")
    if any(word in lower for word in ["k线查询", "k 线查询", "klines query", "data.klines_query", "通用k线", "通用 k 线"]):
        priority_names.append("data.klines_query")
    wants_akshare_priority = any(word in lower for word in ["akshare", "ak share", "ak 数据", "ak外部"])
    if not wants_akshare_priority and any(word in lower for word in ["股票列表", "股票清单", "分页股票", "股票分页", "stock list"]):
        priority_names.append("data.stock_list")
    if wants_akshare_priority and any(word in lower for word in ["股票列表", "代码列表", "stock list"]):
        priority_names.append("akshare.stock_list")
    if wants_akshare_priority and any(word in lower for word in ["实时", "快照", "spot"]):
        priority_names.append("akshare.stock_spot")
    if wants_akshare_priority and any(word in lower for word in ["个股信息", "基础信息", "stock info"]):
        priority_names.append("akshare.stock_info")
    if wants_akshare_priority and any(word in lower for word in ["批量日线", "daily batch"]):
        priority_names.append("akshare.stock_daily_batch")
    if wants_akshare_priority and any(word in lower for word in ["日线", "daily", "k线", "历史行情"]):
        priority_names.append("akshare.stock_daily")
    if wants_akshare_priority and any(word in lower for word in ["回测历史", "hist", "akquant 兼容"]):
        priority_names.append("akshare.stock_hist")
    if any(word in lower for word in ["表达式校验", "校验表达式", "validate expression", "compute.validate"]):
        priority_names.append("compute.validate")
    if any(word in lower for word in ["表达式计算", "计算表达式", "evaluate expression", "compute.evaluate"]):
        priority_names.append("compute.evaluate")
    if any(word in lower for word in ["表达式选股", "条件表达式", "compute.screen"]):
        priority_names.append("compute.screen")
    if any(word in lower for word in ["表达式预计算", "compute.precompute", "precompute expression"]):
        priority_names.append("compute.precompute")
    if any(word in lower for word in ["批量因子计算", "compute.batch", "batch compute"]):
        priority_names.append("compute.batch")
    if any(word in lower for word in ["ic分析", "ic 分析", "ic analysis", "evaluation.ic_analysis"]):
        priority_names.append("evaluation.ic_analysis")
    if any(word in lower for word in ["分层回测", "quantile backtest", "evaluation.quantile_backtest"]):
        priority_names.append("evaluation.quantile_backtest")
    if any(word in lower for word in ["完整评估", "完整报告", "full report", "evaluation.full_report"]):
        priority_names.append("evaluation.full_report")
    if any(word in lower for word in ["六模块", "因子评估报告", "evaluation.report"]):
        priority_names.append("evaluation.report")
    if any(word in lower for word in ["因子看板", "factor board", "evaluation.board"]):
        priority_names.append("evaluation.board")
    if any(word in lower for word in ["v2因子模板", "v2 因子模板", "factor.templates_v2"]):
        priority_names.append("factor.templates_v2")
    if any(word in lower for word in ["python因子校验", "python 因子校验", "validate-python", "factor.validate_python"]):
        priority_names.append("factor.validate_python")
    if any(word in lower for word in ["v2因子校验", "v2 因子校验", "保存因子校验", "factor.validate_v2"]):
        priority_names.append("factor.validate_v2")
    if any(word in lower for word in ["创建 legacy 因子", "legacy 因子创建", "factor.create_legacy"]):
        priority_names.append("factor.create_legacy")
    if any(word in lower for word in ["更新 legacy 因子", "legacy 因子更新", "factor.update_legacy"]):
        priority_names.append("factor.update_legacy")
    if any(word in lower for word in ["删除 legacy 因子", "legacy 因子删除", "factor.delete_legacy"]):
        priority_names.append("factor.delete_legacy")
    if any(word in lower for word in ["因子分析记录", "analysis list", "factor.analysis_list"]):
        priority_names.append("factor.analysis_list")
    if any(word in lower for word in ["因子分析详情", "analysis detail", "factor.analysis_detail"]):
        priority_names.append("factor.analysis_detail")
    if any(word in lower for word in ["创建 v2 因子", "v2 因子创建", "factor.create_v2"]):
        priority_names.append("factor.create_v2")
    if any(word in lower for word in ["更新 v2 因子", "v2 因子更新", "factor.update_v2"]):
        priority_names.append("factor.update_v2")
    if any(word in lower for word in ["删除 v2 因子", "v2 因子删除", "factor.delete_v2"]):
        priority_names.append("factor.delete_v2")
    if any(word in lower for word in ["保存因子预览", "因子预览", "preview saved", "factor.preview_saved"]):
        priority_names.append("factor.preview_saved")
    if any(
        word in lower
        for word in ["运行保存的 python 因子", "运行 python 因子", "python 因子运行", "run-python", "factor.run_python_saved"]
    ):
        priority_names.append("factor.run_python_saved")
    if any(word in lower for word in ["保存因子预计算", "factor.precompute_saved"]):
        priority_names.append("factor.precompute_saved")
    if any(word in lower for word in ["保存因子覆盖", "因子覆盖", "factor.coverage_saved"]):
        priority_names.append("factor.coverage_saved")
    if any(word in lower for word in ["保存因子分析", "因子分析", "factor.analyze_saved"]):
        priority_names.append("factor.analyze_saved")
    if any(word in lower for word in ["保存因子评估", "因子评估", "evaluate saved", "factor.evaluate_saved"]):
        priority_names.append("factor.evaluate_saved")
    if any(word in lower for word in ["参数哈希", "param hash", "param_hash"]):
        priority_names.append("factor_value.param_hashes")
    if any(word in lower for word in ["因子值查询", "查询因子值", "factor_value.query"]):
        priority_names.append("factor_value.query")
    if any(word in lower for word in ["论文因子", "paper factor", "paper_feature", "实验规格"]):
        priority_names.extend(["factor_value.paper_manifest", "factor_value.paper_experiments"])
    if any(word in lower for word in ["特征快照", "feature snapshot"]):
        priority_names.append("factor_value.paper_feature_snapshot")
    if any(word in lower for word in ["因子研究准备", "research prepare", "factor_research.prepare"]):
        priority_names.append("factor_research.prepare")
    if any(word in lower for word in ["提交因子研究", "运行因子研究", "research submit", "factor_research.submit"]):
        priority_names.append("factor_research.submit")
    if any(word in lower for word in ["批量因子研究", "research batch", "factor_research.batch"]):
        priority_names.append("factor_research.batch")
    if any(word in lower for word in ["因子组合候选", "因子组合", "research combinations", "factor_research.combinations"]):
        priority_names.append("factor_research.combinations")
    if any(word in lower for word in ["schema", "字段", "列名", "表结构"]):
        priority_names.append("explorer.table_schema")
    if any(word in lower for word in ["distinct", "去重值", "枚举值"]):
        priority_names.append("explorer.distinct_values")
    if any(word in lower for word in ["表搜索", "搜索表", "quick_search", "table search"]):
        priority_names.append("explorer.table_search")
    if any(word in lower for word in ["parquet schema", "数据集 schema", "数据集字段", "数据集列名"]):
        priority_names.append("parquet.dataset_schema")
    if any(word in lower for word in ["数据覆盖", "覆盖检查"]):
        priority_names.append("backtest.data_coverage")
    if any(word in lower for word in ["股票名称", "名称映射", "stock names"]):
        priority_names.append("backtest.stock_names")
    if any(word in lower for word in ["取消同步", "停止同步", "cancel sync", "sync cancel"]):
        priority_names.append("data.sync_cancel")
    if any(word in lower for word in ["取消全部同步", "停止所有同步", "cancel all sync", "sync cancel all"]):
        priority_names.append("data.sync_cancel_all")
    if any(word in lower for word in ["grid search", "网格搜索", "参数优化"]):
        priority_names.append("backtest.optimize_grid")
    if any(word in lower for word in ["walk-forward", "walk forward", "滚动验证", "走步验证"]):
        priority_names.append("backtest.optimize_walk_forward")
    if any(word in lower for word in ["策略参数 schema", "参数 schema", "参数表单"]):
        priority_names.append("backtest.strategy_params_schema")
    if any(word in lower for word in ["策略参数校验", "参数校验", "validate params"]):
        priority_names.append("backtest.strategy_params_validate")
    if any(word in lower for word in ["取消回测", "停止回测", "cancel backtest"]):
        priority_names.append("backtest.task_cancel")
    if any(word in lower for word in ["回测报告", "html report", "quantstats", "backtest report"]):
        priority_names.append("backtest.task_report")
    if any(word in lower for word in ["回测指数池详情", "指数池详情", "index pool detail", "backtest.index_pool_detail"]):
        priority_names.append("backtest.index_pool_detail")
    if any(word in lower for word in ["预定义股票池", "回测股票池", "pool symbols"]):
        priority_names.append("backtest.pool_symbols")
    if any(word in lower for word in ["因子回测", "factor backtest", "backtest.factor"]):
        priority_names.append("backtest.factor")
    if any(word in lower for word in ["双标的网格预设", "底仓网格预设", "dual stock grid preset"]):
        priority_names.append("backtest.preset_dual_stock_grid")
    if any(word in lower for word in ["写入双标的网格", "创建双标的网格策略", "dual stock grid strategy"]):
        priority_names.append("backtest.create_preset_dual_stock_grid_strategy")
    if any(word in lower for word in ["写入通用多因子", "创建通用多因子策略", "multi factor strategy"]):
        priority_names.append("backtest.create_preset_multi_factor_strategy")
    if any(word in lower for word in ["写入科技小市值", "创建科技小市值策略", "tech small cap strategy"]):
        priority_names.append("backtest.create_preset_tech_small_cap_strategy")
    if any(word in lower for word in ["创建策略", "strategy.create"]):
        priority_names.append("strategy.create")
    if any(word in lower for word in ["更新策略", "strategy.update"]):
        priority_names.append("strategy.update")
    if any(word in lower for word in ["删除策略", "strategy.delete"]):
        priority_names.append("strategy.delete")
    if any(word in lower for word in ["趋势资金日度信号", "日度信号", "trend signals daily", "strategy.trend_signals_daily"]):
        priority_names.append("strategy.trend_signals_daily")
    if any(word in lower for word in ["趋势资金信号汇总", "信号汇总", "trend signals summary", "strategy.trend_signals_summary"]):
        priority_names.append("strategy.trend_signals_summary")
    if any(word in lower for word in ["趋势资金回测", "趋势资金组合回测", "trend backtest", "strategy.trend_backtest"]):
        priority_names.append("strategy.trend_backtest")
    if any(word in lower for word in ["深度价值回测", "deep value", "strategy.deep_value_backtest"]):
        priority_names.append("strategy.deep_value_backtest")
    if any(word in lower for word in ["创建研报对话", "研报对话会话", "report chat session"]):
        priority_names.append("report.chat_session_create")
    if any(word in lower for word in ["发送研报对话", "继续研报对话", "report chat send"]):
        priority_names.append("report.chat_session_send")
    if any(word in lower for word in ["创建传统回测", "创建回测记录", "backtest.create_record"]):
        priority_names.append("backtest.create_record")
    if any(word in lower for word in ["运行传统回测", "运行回测记录", "backtest.run_record"]):
        priority_names.append("backtest.run_record")
    if any(word in lower for word in ["删除传统回测", "删除回测记录", "backtest.delete_record"]):
        priority_names.append("backtest.delete_record")
    if any(word in lower for word in ["批量删除回测", "backtest.delete_records_batch"]):
        priority_names.append("backtest.delete_records_batch")
    if any(word in lower for word in ["因子预计算准备", "预计算准备", "precompute prepare"]):
        priority_names.append("factor_value.precompute_prepare")
    if any(word in lower for word in ["因子集合预计算", "分组预计算", "group precompute"]):
        priority_names.append("factor_value.group_precompute")
    if any(word in lower for word in ["因子预计算", "precompute factor", "factor precompute"]):
        priority_names.append("factor_value.precompute")
    if any(word in lower for word in ["舆情线程", "主题线程", "sentiment threads"]):
        priority_names.append("sentiment.threads")
    if any(word in lower for word in ["舆情抓取", "情绪抓取", "sentiment ingest", "抓帖子"]):
        priority_names.append("sentiment.ingest_run")
    live_profile_intent_text = live_lower.replace("profile_key", "")
    if any(word in live_profile_intent_text for word in ["实盘策略", "策略配置", "profile"]):
        priority_names.append("live_trading.strategy_profiles")
    if any(word in live_lower for word in ["周度成交", "weekly trade", "weekly trades"]):
        priority_names.append("live_trading.weekly_trades")
    if any(word in live_lower for word in ["待处理订单", "待处理委托", "pending orders"]):
        priority_names.append("live_trading.pending_orders")
    if any(word in live_lower for word in ["订单审计", "委托审计", "order audit"]):
        priority_names.append("live_trading.order_audit")
    if any(word in live_lower for word in ["成交记录", "交易记录", "trade records", "trades"]):
        priority_names.append("live_trading.trades")
    if any(word in live_lower for word in ["初始化账户", "account initialize", "account_initialize"]):
        priority_names.append("live_trading.account_initialize")
    if any(word in live_profile_intent_text for word in ["创建实盘策略配置", "创建 profile", "profile create"]):
        priority_names.append("live_trading.profile_create")
    if any(word in live_profile_intent_text for word in ["更新实盘策略配置", "更新 profile", "profile update"]):
        priority_names.append("live_trading.profile_update")
    if any(word in live_lower for word in ["实盘预检", "策略预检", "preflight"]):
        priority_names.append("live_trading.preflight")
    if any(word in live_lower for word in ["生成实盘信号", "生成策略信号", "live signals"]):
        priority_names.append("live_trading.signals")
    if any(word in live_lower for word in ["启动 runner", "runner start"]):
        priority_names.append("live_trading.runner_start")
    if any(word in live_lower for word in ["停止 runner", "runner stop"]):
        priority_names.append("live_trading.runner_stop")
    if any(word in live_lower for word in ["人工接管", "runner takeover", "takeover"]):
        priority_names.append("live_trading.runner_takeover")
    if any(word in live_lower for word in ["同步委托", "订单同步", "orders sync"]):
        priority_names.append("live_trading.orders_sync")
    if any(word in live_lower for word in ["提交委托", "提交订单", "orders submit"]):
        priority_names.append("live_trading.orders_submit")
    if any(word in live_lower for word in ["撤单重报", "cancel resubmit"]):
        priority_names.append("live_trading.orders_cancel_resubmit")
    elif any(word in live_lower for word in ["撤单", "取消委托", "orders cancel"]):
        priority_names.append("live_trading.orders_cancel")
    if any(word in live_lower for word in ["本地关闭委托", "关闭本地委托", "close local"]):
        priority_names.append("live_trading.orders_close_local")
    if priority_names:
        priority_set = set(priority_names)
        prioritized = [action for name in priority_names for action in deduped if action.tool_name == name]
        prioritized.extend(action for action in deduped if action.tool_name not in priority_set)
        deduped = prioritized
    return deduped[:6]


def _workflow_action_from_intent(text: str, *, resolved_symbol: str | None = None) -> AIActionCard | None:
    lower = text.lower()
    stripped = text.strip()
    if not stripped:
        return None

    report_intent = (
        any(word in lower for word in ["研报", "报告", "report"])
        and any(word in lower for word in ["策略", "转策略", "生成策略", "strategy", "akquant"])
    )
    if report_intent:
        return _tool_action(
            "workflow.report_strategy_graph",
            {
                "command": stripped,
                "arguments": {
                    "report_text": stripped,
                    "report_filename": "copilot-input.txt",
                    "convert_to_akquant": "akquant" in lower or "回测" in lower,
                },
                "auto_execute": True,
            },
        )

    symbol = resolved_symbol or _stock_symbol_from_text(text)
    research_intent = bool(symbol) and any(
        word in lower
        for word in [
            "解读",
            "分析",
            "研究",
            "投研",
            "走势",
            "怎么看",
            "机会",
            "风险",
            "因子",
            "舆情",
            "情绪",
        ]
    )
    if research_intent:
        return _tool_action(
            "workflow.quant_research_graph",
            {
                "command": stripped,
                "arguments": {
                    "symbol": symbol,
                    "topic": stripped,
                    "daily_limit": _bar_limit_from_text(text) or 60,
                    "include_factors": True,
                    "include_sentiment": any(word in lower for word in ["舆情", "情绪", "雪球", "股吧", "sentiment"]),
                },
                "auto_execute": True,
            },
        )

    command_graph_intent = (
        any(word in lower for word in ["工作流", "workflow", "graph", "节点", "诊断一下", "排查"])
        or sum(
            int(any(word in lower for word in bucket))
            for bucket in [
                ["状态", "health", "系统"],
                ["任务", "进度", "runtime"],
                ["数据", "覆盖", "summary"],
                ["策略", "strategy"],
                ["回测", "backtest"],
            ]
        )
        >= 3
    )
    if command_graph_intent:
        return _tool_action(
            "workflow.command_graph",
            {
                "command": stripped,
                "auto_execute": True,
            },
        )
    return None


def _prefer_workflow_action(
    *,
    actions: list[AIActionCard],
    text: str,
    resolved_symbol: str | None,
) -> tuple[list[AIActionCard], AIActionCard | None]:
    workflow_action = _workflow_action_from_intent(text, resolved_symbol=resolved_symbol)
    if workflow_action is None:
        return actions, None
    existing = next((action for action in actions if action.tool_name == workflow_action.tool_name), None)
    preserved_pending = [
        action
        for action in actions
        if not action.tool_name.startswith("workflow.")
        and (action.requires_confirmation or action.risk_level != "read")
    ]
    selected = existing or workflow_action
    return [selected, *preserved_pending][:6], selected


def _has_live_trading_intent(text: str) -> bool:
    lower = text.lower().replace("交易日", "")
    return any(word in lower for word in ["实盘", "下单", "qmt", "runner", "账户", "持仓", "委托", "成交", "撤单"])


def _filter_actions_by_intent(actions: list[AIActionCard], text: str) -> tuple[list[AIActionCard], list[str]]:
    if _has_live_trading_intent(text):
        return actions, []
    filtered = [action for action in actions if not action.tool_name.startswith("live_trading.")]
    removed = [action.tool_name for action in actions if action.tool_name.startswith("live_trading.")]
    return filtered, removed


def _fallback_route_plan(
    *,
    existing_plan: AIRoutePlan,
    actions: list[AIActionCard],
    detail: str,
) -> AIRoutePlan:
    return AIRoutePlan(
        actions=actions,
        source="fallback" if existing_plan.source == "fallback" else "llm+fallback",
        error=existing_plan.error,
        clarification=existing_plan.clarification,
        confidence=existing_plan.confidence,
        tool_reasons=[
            {"tool_name": action.tool_name, "reason": action.description, "arguments": action.arguments}
            for action in actions
        ],
        nodes=[
            *existing_plan.nodes,
            AIRouterNodeTrace(name="fallback_router", status="ok", detail=detail),
        ],
    )


def _offline_reply(text: str, actions: list[AIActionCard]) -> str:
    if actions:
        labels = "、".join(action.title for action in actions)
        return f"我可以先执行这些动作：{labels}。"
    if text:
        return "我已经收到。当前模型网关未就绪，可以先用工具卡片处理平台状态、数据、任务和回测入口。"
    return "模型网关未就绪，工具层可用。"


def _reply_from_pending_actions(
    actions: list[AIActionCard],
    *,
    auto_execute: bool,
    route_error: str | None = None,
) -> str:
    if not actions:
        return "我需要更多上下文才能选择合适的平台工具。"
    intro = "我已经选好了下一步工具，但本次请求设置为不自动执行。" if not auto_execute else "以下工具需要确认后才能执行。"
    lines = [intro, ""]
    for action in actions[:6]:
        risk = "只读" if action.risk_level == "read" else "写入" if action.risk_level == "write" else "高风险"
        confirm = "，需要确认" if action.requires_confirmation else ""
        lines.append(f"- `{action.tool_name}`：{action.title}（{risk}{confirm}）")
    if route_error:
        lines.append("")
        lines.append(f"路由已切换到本地兜底：{route_error}")
    return "\n".join(lines)


def _auto_executable(action: AIActionCard) -> bool:
    if action.requires_confirmation or action.risk_level != "read":
        return False
    return action.tool_name not in {"data.realtime_quote", "data.realtime_quotes"}


def _compact_tool_result(result: AIToolExecutionResponse) -> dict[str, Any]:
    return {
        "tool_name": result.tool_name,
        "status": result.status,
        "summary": result.summary,
        "task_id": result.task_id,
        "result_ref": result.result_ref,
        "error": result.error,
        "result": result.result,
    }


def _public_executed_tool(item: dict[str, Any]) -> dict[str, Any]:
    public = {key: value for key, value in item.items() if key != "result"}
    result = item.get("result")
    if str(item.get("tool_name") or "").startswith("workflow.") and isinstance(result, dict):
        public["workflow"] = {
            "workflow_name": result.get("workflow_name"),
            "status": result.get("status"),
            "summary": result.get("summary"),
            "nodes": result.get("nodes") if isinstance(result.get("nodes"), list) else [],
            "pending_tools": result.get("pending_tools") if isinstance(result.get("pending_tools"), list) else [],
        }
    return public


def _preview_tool_result(item: dict[str, Any]) -> list[str]:
    result = item.get("result")
    tool_name = str(item.get("tool_name") or "")
    lines: list[str] = []
    if tool_name.startswith("workflow.") and isinstance(result, dict):
        summary = result.get("summary")
        if summary:
            lines.append(str(summary))
        nested_results = result.get("tool_results")
        if isinstance(nested_results, list):
            for nested in nested_results[:5]:
                if not isinstance(nested, dict):
                    continue
                nested_name = str(nested.get("tool_name") or "")
                nested_summary = str(nested.get("summary") or "")
                if nested_name or nested_summary:
                    lines.append(f"{nested_name}：{nested_summary}".strip("："))
                lines.extend(f"  {line}" for line in _preview_tool_result(nested))
    elif tool_name == "data.stock_snapshot" and isinstance(result, dict):
        symbol = str(result.get("symbol") or "").strip()
        name = str(result.get("name") or "").strip()
        industry = str(result.get("industry") or "-").strip() or "-"
        flags = []
        if result.get("is_st"):
            flags.append("ST")
        if result.get("is_suspend"):
            flags.append("停牌")
        if result.get("is_delist"):
            flags.append("退市")
        state = "、".join(flags) if flags else "正常交易状态"
        lines.append(f"股票：{symbol} {name}，行业：{industry}，状态：{state}。")
    elif tool_name == "data.market_snapshot" and isinstance(result, dict):
        stock = result.get("stock") if isinstance(result.get("stock"), dict) else {}
        bars = result.get("daily_bars") if isinstance(result.get("daily_bars"), list) else []
        if stock:
            lines.append(f"股票：{stock.get('symbol')} {stock.get('name') or ''}，行业：{stock.get('industry') or '-'}")
        if bars:
            latest = bars[0]
            lines.append(
                "本地最新日线："
                f"{latest.get('datetime')} 开 {latest.get('open')} 高 {latest.get('high')} "
                f"低 {latest.get('low')} 收 {latest.get('close')} 额 {latest.get('amount')}"
            )
            if len(bars) > 1:
                rows = [
                    f"{bar.get('datetime')} 收 {bar.get('close')}"
                    for bar in bars[1:3]
                    if isinstance(bar, dict)
                ]
            if rows:
                lines.append("近几日：" + "；".join(rows))
            lines.extend(_kline_trend_preview(bars))
        realtime_error = result.get("realtime_error")
        if realtime_error:
            lines.append(f"实时行情：不可用（{realtime_error}）")
    elif tool_name == "data.kline_daily" and isinstance(result, list):
        if not result:
            lines.append("本地未返回该日期区间的日 K 记录。")
        lines.extend(_kline_trend_preview(result))
        for row in result[:3]:
            if isinstance(row, dict):
                lines.append(f"{row.get('datetime')} 收 {row.get('close')} 成交额 {row.get('amount')}")
    elif isinstance(result, dict):
        total = result.get("total")
        if total is not None:
            lines.append(f"total={total}")
    elif isinstance(result, list):
        lines.append(f"返回 {len(result)} 行。")
    return lines


def _date_from_bar(row: dict[str, Any]) -> str:
    return str(row.get("datetime") or row.get("trade_date") or "")[:10]


def _format_number(value: float | None, digits: int = 2) -> str:
    if value is None:
        return "-"
    return f"{value:.{digits}f}"


def _format_pct(value: float | None) -> str:
    if value is None:
        return "-"
    return f"{value:+.2f}%"


def _format_amount(value: float | None) -> str:
    if value is None:
        return "-"
    if abs(value) >= 100_000_000:
        return f"{value / 100_000_000:.2f}亿"
    if abs(value) >= 10_000:
        return f"{value / 10_000:.2f}万"
    return f"{value:.2f}"


def _kline_trend_preview(rows: list[Any]) -> list[str]:
    clean_rows = [row for row in rows if isinstance(row, dict)]
    if not clean_rows:
        return []
    sortable = [(date, row) for row in clean_rows if (date := _date_from_bar(row))]
    ordered = [row for _, row in sorted(sortable, key=lambda item: item[0])] if sortable else list(reversed(clean_rows))
    amounts = [_to_float(row.get("amount")) for row in ordered]
    valid_amounts = [value for value in amounts if value is not None]
    first = ordered[0]
    latest = ordered[-1]
    first_close = _to_float(first.get("close"))
    latest_close = _to_float(latest.get("close"))
    change_pct = (latest_close / first_close - 1) * 100 if first_close and latest_close is not None else None
    prev_close = _to_float(ordered[-2].get("close")) if len(ordered) >= 2 else None
    latest_day_pct = (latest_close / prev_close - 1) * 100 if prev_close and latest_close is not None else None

    high_pairs = [
        (_to_float(row.get("high")), _date_from_bar(row))
        for row in ordered
        if _to_float(row.get("high")) is not None
    ]
    low_pairs = [
        (_to_float(row.get("low")), _date_from_bar(row))
        for row in ordered
        if _to_float(row.get("low")) is not None
    ]
    high_value, high_date = max(high_pairs, key=lambda item: item[0] or 0) if high_pairs else (None, "-")
    low_value, low_date = min(low_pairs, key=lambda item: item[0] or 0) if low_pairs else (None, "-")
    high_drawdown = (latest_close / high_value - 1) * 100 if high_value and latest_close is not None else None
    recent_rows = ordered[-3:]
    recent = "；".join(
        f"{_date_from_bar(row)} 收 {_format_number(_to_float(row.get('close')))}"
        for row in recent_rows
    )
    judgement = "震荡"
    if high_drawdown is not None and high_drawdown <= -15:
        judgement = "冲高后回撤明显，短线偏弱"
    elif change_pct is not None and change_pct >= 8:
        judgement = "区间仍有涨幅，但需结合高点回撤观察"
    elif change_pct is not None and change_pct <= -8:
        judgement = "区间下跌明显，趋势偏弱"
    elif latest_day_pct is not None and abs(latest_day_pct) >= 5:
        judgement = "最新交易日波动较大，短线分歧升温"
    return [
        f"样本：{len(clean_rows)} 条，{_date_from_bar(first)} 至 {_date_from_bar(latest)}。",
        f"收盘：{_format_number(first_close)} -> {_format_number(latest_close)}，区间涨跌 {_format_pct(change_pct)}。",
        f"区间高低：高 {_format_number(high_value)}（{high_date}），低 {_format_number(low_value)}（{low_date}）。",
        f"高点回撤：最新收盘较区间高点 {_format_pct(high_drawdown)}；最近一日 {_format_pct(latest_day_pct)}。",
        f"平均成交额：{_format_amount(sum(valid_amounts) / len(valid_amounts) if valid_amounts else None)}。",
        f"走势判断：{judgement}；关注 {_format_number(low_value)} 附近支撑和 {_format_number(high_value)} 附近压力。",
        f"最近三日：{recent}。" if recent else "",
    ]


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _stock_label_from_tools(executed_tools: list[dict[str, Any]], fallback_symbol: str | None = None) -> str:
    for item in executed_tools:
        if item.get("tool_name") != "data.stock_snapshot":
            continue
        result = item.get("result")
        if not isinstance(result, dict):
            continue
        symbol = str(result.get("symbol") or fallback_symbol or "").strip()
        name = str(result.get("name") or "").strip()
        return f"{symbol} {name}".strip()
    return fallback_symbol or ""


def _average_close_reply(text: str, executed_tools: list[dict[str, Any]]) -> str | None:
    if not any(word in text for word in ["平均", "均价", "平均收盘", "平均收盘价"]):
        return None
    kline_item = next((item for item in executed_tools if item.get("tool_name") == "data.kline_daily"), None)
    if not kline_item or kline_item.get("status") != "ok":
        return None
    rows = kline_item.get("result")
    if not isinstance(rows, list) or not rows:
        return None
    closes: list[float] = []
    dates: list[str] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        close = _to_float(row.get("close"))
        if close is None:
            continue
        closes.append(close)
        dates.append(str(row.get("datetime") or row.get("trade_date") or "")[:10])
    if not closes:
        return None
    args = kline_item.get("arguments") if isinstance(kline_item.get("arguments"), dict) else {}
    symbol = str(args.get("symbol") or "").strip()
    label = _stock_label_from_tools(executed_tools, symbol)
    average = sum(closes) / len(closes)
    newest = dates[0] if dates else "-"
    oldest = dates[-1] if dates else "-"
    close_preview = "、".join(f"{value:.2f}" for value in closes[:10])
    return (
        f"{label or symbol} 最近 {len(closes)} 个交易日的平均收盘价是 {average:.2f}。\n"
        f"样本区间：{oldest} 至 {newest}；收盘价：{close_preview}。"
    )


async def _auto_execute_actions(
    actions: list[AIActionCard],
    session: AsyncSession,
    *,
    limit: int = 3,
) -> list[dict[str, Any]]:
    executed: list[dict[str, Any]] = []
    for action in actions:
        if len(executed) >= limit:
            break
        if not _auto_executable(action):
            continue
        result = await get_ai_tool_registry().execute(
            action.tool_name,
            action.arguments,
            confirmed=False,
            session=session,
        )
        item = _compact_tool_result(result)
        item["arguments"] = action.arguments
        executed.append(item)
    return executed


def _reply_from_executed_tools(
    text: str,
    executed_tools: list[dict[str, Any]],
    pending_actions: list[AIActionCard],
) -> str:
    metric_reply = _average_close_reply(text, executed_tools)
    lines = [metric_reply, "", "工具执行："] if metric_reply else ["已直接查询："]
    for item in executed_tools:
        if (
            item.get("tool_name") == "data.kline_daily"
            and item.get("status") == "error"
            and "xtquant" in str(item.get("summary") or "").lower()
        ):
            args = item.get("arguments") if isinstance(item.get("arguments"), dict) else {}
            start = args.get("start_date") or "-"
            end = args.get("end_date") or start
            lines.append(
                f"- 未取到 `data.kline_daily`：{args.get('symbol') or ''} {start} 至 {end} "
                "本地日 K 暂无记录，QMT/实时兜底在当前环境不可用。"
            )
            continue
        status = "完成" if item.get("status") == "ok" else "异常"
        lines.append(f"- {status} `{item.get('tool_name')}`：{item.get('summary')}")
        lines.extend(f"  {line}" for line in _preview_tool_result(item))
    if pending_actions:
        labels = "、".join(action.title for action in pending_actions[:4])
        lines.append(f"仍需确认或手动执行：{labels}。")
    return "\n".join(lines)


def _build_trace_payload(
    *,
    route_plan: AIRoutePlan,
    latest_text: str,
    routing_text: str,
    resolved_symbol: str | None,
    actions: list[AIActionCard],
    executed_public: list[dict[str, Any]],
    pending_actions: list[AIActionCard],
    answer_mode: str,
    answer_error: str | None = None,
) -> dict[str, Any]:
    executed_by_name = {str(item.get("tool_name") or ""): item for item in executed_public}
    pending_names = {action.tool_name for action in pending_actions}
    reason_by_name = {
        str(item.get("tool_name") or ""): str(item.get("reason") or "")
        for item in route_plan.tool_reasons
        if isinstance(item, dict)
    }
    tool_calls: list[dict[str, Any]] = []
    for action in actions:
        executed = executed_by_name.get(action.tool_name)
        if executed:
            status = str(executed.get("status") or "executed")
            summary = executed.get("summary")
        elif action.tool_name in pending_names:
            status = "pending_confirmation"
            summary = "需要确认后执行"
        else:
            status = "planned"
            summary = None
        tool_calls.append(
            {
                "tool_name": action.tool_name,
                "title": action.title,
                "arguments": action.arguments,
                "risk_level": action.risk_level,
                "reason": reason_by_name.get(action.tool_name) or action.description,
                "status": status,
                "summary": summary,
                "workflow": executed.get("workflow") if isinstance(executed, dict) else None,
            }
        )

    nodes = [node.model_dump() for node in route_plan.nodes]
    nodes.append(
        {
            "name": "execute",
            "status": "ok" if executed_public else "skipped",
            "detail": f"{len(executed_public)} executed, {len(pending_actions)} pending",
        }
    )
    nodes.append({"name": "answer", "status": "error" if answer_error else "ok", "detail": answer_mode})

    return {
        "note": "这里展示的是可审计路由摘要和工具依据，不是模型隐藏思维链。",
        "source": route_plan.source,
        "confidence": route_plan.confidence,
        "error": route_plan.error,
        "clarification": route_plan.clarification,
        "context": {
            "latest_user": latest_text,
            "routing_text": routing_text,
            "resolved_symbol": resolved_symbol,
        },
        "answer": {
            "mode": answer_mode,
            "error": answer_error,
        },
        "nodes": nodes,
        "tool_calls": tool_calls,
    }


def _system_prompt() -> str:
    tool_lines = [
        f"- {tool.name}: {tool.description}"
        for tool in get_ai_tool_registry().list()
    ]
    return (
        "你是 GaoshouPlatform 的 AI Native Copilot。"
        "回答要简洁。不要输出 XML/HTML 风格的工具标签。"
        "平台动作由后端工具执行；没有工具结果时，只能说明需要查询，不能编造数据。"
        "不要声称已经执行未执行的同步、回测或交易动作。"
        "实盘交易只能查询状态，不能自动下单。\n\n"
        "可用工具：\n" + "\n".join(tool_lines)
    )


async def _route_actions_with_llm_async(**kwargs: Any) -> AIRoutePlan:
    return await asyncio.to_thread(route_actions_with_llm, **kwargs)


async def _synthesize_answer_with_llm_async(**kwargs: Any):
    return await asyncio.to_thread(synthesize_answer_with_llm, **kwargs)


async def _gateway_chat_async(gateway: Any, **kwargs: Any) -> str:
    return await asyncio.to_thread(gateway.chat, **kwargs)


def _llm_step_timeout(default_seconds: float, *, cap_seconds: float) -> float:
    try:
        configured = float(settings.ai_timeout_seconds)
    except (TypeError, ValueError):
        configured = default_seconds
    return max(1.0, min(configured, cap_seconds))


@router.get("/status")
async def ai_status() -> dict[str, Any]:
    current = _ai_config_payload()
    data = AIStatusResponse(
        enabled=current.enabled,
        gateway=current.gateway,
        tool_count=len(get_ai_tool_registry().list()),
        artifact_store={
            "type": "sqlite",
            "table": "ai_artifacts",
            "enabled": True,
        },
        decisions=[
            "LiteLLM SDK is embedded in the backend process for phase 1.",
            "Tool registry is the single contract shared by Copilot, MCP and future LangGraph flows.",
            "Copilot uses an LLM RouterNode first, then falls back to local deterministic routing.",
            "The stdio MCP server exposes every registered platform tool through python -m app.ai.mcp_server.",
            "The AI tool manifest is available at /api/ai/manifest for external agents and diagnostics.",
            "CommandGraph, ReportStrategyGraph and QuantResearchGraph expose repeatable workflow nodes over the same tool registry.",
            "Live trading tools are read-only in phase 1.",
        ],
    )
    return _ok(data.model_dump())


@router.get("/config")
async def get_ai_config() -> dict[str, Any]:
    return _ok(_ai_config_payload().model_dump())


@router.put("/config")
async def update_ai_config(payload: AIConfigUpdate) -> dict[str, Any]:
    current = _ai_config_payload()
    provider = payload.provider or current.provider
    model = payload.model or current.model
    base_url = (payload.base_url if payload.base_url is not None else current.base_url) or ""
    api_key_env = payload.api_key_env or current.api_key_env
    assigned_env, api_key_value = _split_secret_assignment(payload.api_key)
    if assigned_env:
        api_key_env = assigned_env
    enabled = current.enabled if payload.enabled is None else payload.enabled
    if provider != "litellm":
        raise HTTPException(status_code=400, detail="Only provider='litellm' is supported in phase 1")
    if api_key_env in AI_CONFIG_KEYS:
        raise HTTPException(status_code=400, detail="api_key_env cannot be one of the AI config keys")

    updates = {
        "AI_ENABLED": _env_bool_text(enabled),
        "AI_PROVIDER": provider,
        "AI_MODEL": model,
        "AI_BASE_URL": base_url,
        "AI_API_KEY_ENV": api_key_env,
    }
    remove_keys: set[str] = set()
    if payload.clear_api_key:
        remove_keys.add(api_key_env)
    elif api_key_value:
        updates[api_key_env] = api_key_value

    env_path = _ai_config_env_path()
    _write_env_values(env_path, updates, remove_keys=remove_keys)
    os.environ.update(updates)
    if payload.clear_api_key:
        os.environ.pop(api_key_env, None)
    elif api_key_value:
        os.environ[api_key_env] = api_key_value
    _apply_ai_runtime_config(
        enabled=enabled,
        provider=provider,
        model=model,
        base_url=base_url,
        api_key_env=api_key_env,
    )
    reset_llm_gateway()
    return _ok(_ai_config_payload().model_dump())


@router.get("/tools")
async def ai_tools() -> dict[str, Any]:
    return _ok([tool.model_dump() for tool in get_ai_tool_registry().public_definitions()])


@router.get("/manifest")
async def ai_tool_manifest() -> dict[str, Any]:
    return _ok(build_ai_tool_manifest())


@router.get("/diagnostics")
async def ai_diagnostics(
    limit: int = Query(50, ge=1, le=200),
    session: AsyncSession = Depends(get_async_session),
) -> dict[str, Any]:
    current = _ai_config_payload()
    manifest = build_ai_tool_manifest()
    artifacts = await list_artifacts(session, limit=limit)
    return _ok(
        build_ai_diagnostics(
            enabled=current.enabled,
            gateway=current.gateway,
            manifest=manifest,
            artifacts=artifacts,
            sample_limit=limit,
        )
    )


@router.get("/workflows")
async def ai_workflows() -> dict[str, Any]:
    return _ok([workflow.model_dump() for workflow in list_ai_workflows()])


@router.post("/workflows/{workflow_name}/run")
async def run_ai_workflow_route(
    request: AIWorkflowRunRequest,
    workflow_name: str = Path(description="Workflow name, e.g. CommandGraph"),
    session: AsyncSession = Depends(get_async_session),
) -> dict[str, Any]:
    try:
        get_ai_workflow(workflow_name)
    except KeyError as exc:
        return {"code": 1, "message": str(exc), "data": None}

    artifact = await create_artifact(
        session,
        kind=f"workflow:{workflow_name}",
        status="running",
        input_summary=(request.command or str(request.arguments))[:500],
        tool_calls=[],
    )
    try:
        result = await run_ai_workflow(workflow_name, request, session=session)
    except Exception as exc:
        await update_artifact(session, artifact.artifact_id, status="error", error=str(exc))
        raise
    else:
        result.artifact_id = artifact.artifact_id
        await update_artifact(
            session,
            artifact.artifact_id,
            status=result.status,
            tool_calls=[
                {"tool_name": item.get("tool_name"), "arguments": item.get("arguments") or {}}
                for item in result.tool_results
                if item.get("tool_name")
            ],
            key_outputs=result.model_dump(),
            error=result.error,
        )
        return _ok(result.model_dump())


@router.post("/chat")
async def ai_chat(
    request: AIChatRequest,
    session: AsyncSession = Depends(get_async_session),
) -> dict[str, Any]:
    _ai_config_payload()
    latest_text = _last_user_text(request.messages)
    routing_text = _routing_text(request.messages)
    resolved_symbol = await _stock_symbol_from_text_or_name(routing_text, session)
    gateway = get_llm_gateway()
    offline = not gateway.is_ready()
    model = gateway.model
    context_hints = {
        "latest_user": latest_text,
        "routing_text": routing_text,
        "resolved_symbol": resolved_symbol,
    }
    try:
        route_plan = await asyncio.wait_for(
            _route_actions_with_llm_async(
                gateway=gateway,
                messages=request.messages,
                page_context=request.page_context,
                context_hints=context_hints,
            ),
            timeout=_llm_step_timeout(20.0, cap_seconds=30.0),
        )
    except TimeoutError:
        route_plan = AIRoutePlan(
            source="fallback",
            error="RouterNode timeout",
            nodes=[
                AIRouterNodeTrace(name="context", status="ok", detail="conversation and local hints prepared"),
                AIRouterNodeTrace(name="router", status="fallback", detail="RouterNode timeout"),
            ],
        )
    actions = route_plan.actions
    fallback_actions = _suggest_actions(routing_text, request.page_context, resolved_symbol=resolved_symbol)
    if (not actions and fallback_actions) or route_plan.source == "fallback":
        actions = fallback_actions
        route_plan = _fallback_route_plan(
            existing_plan=route_plan,
            actions=actions,
            detail="local keyword/data fallback generated tool calls",
        )
    actions, promoted_workflow = _prefer_workflow_action(
        actions=actions,
        text=routing_text,
        resolved_symbol=resolved_symbol,
    )
    if promoted_workflow:
        route_plan = AIRoutePlan(
            actions=actions,
            source=route_plan.source if route_plan.source.startswith("llm") else "fallback",
            error=route_plan.error,
            clarification=route_plan.clarification,
            confidence=route_plan.confidence,
            tool_reasons=[
                *route_plan.tool_reasons,
                {
                    "tool_name": promoted_workflow.tool_name,
                    "reason": "复杂请求命中工作流意图，优先交给 Graph 节点执行。",
                    "arguments": promoted_workflow.arguments,
                },
            ],
            nodes=[
                *route_plan.nodes,
                AIRouterNodeTrace(
                    name="workflow_intent",
                    status="promoted",
                    detail=f"preferred {promoted_workflow.tool_name}",
                ),
            ],
        )
    actions, removed_actions = _filter_actions_by_intent(actions, routing_text)
    if removed_actions:
        route_plan = AIRoutePlan(
            actions=actions,
            source=route_plan.source,
            error=route_plan.error,
            clarification=route_plan.clarification,
            confidence=route_plan.confidence,
            tool_reasons=[
                item
                for item in route_plan.tool_reasons
                if isinstance(item, dict) and item.get("tool_name") not in set(removed_actions)
            ],
            nodes=[
                *route_plan.nodes,
                AIRouterNodeTrace(
                    name="guardrail",
                    status="filtered",
                    detail=f"removed non-live-intent tools: {', '.join(removed_actions)}",
                ),
            ],
        )
    executed_tools = await _auto_execute_actions(actions, session) if request.auto_execute else []
    executed_names = {str(item.get("tool_name") or "") for item in executed_tools}
    if request.auto_execute:
        pending_actions = [
            action
            for action in actions
            if action.tool_name not in executed_names and (action.requires_confirmation or action.risk_level != "read")
        ]
    else:
        pending_actions = actions
    executed_public = [_public_executed_tool(item) for item in executed_tools]
    answer_mode = "tool_result"
    answer_error: str | None = None
    if executed_tools:
        reply = _reply_from_executed_tools(routing_text, executed_tools, pending_actions)
        preliminary_trace = _build_trace_payload(
            route_plan=route_plan,
            latest_text=latest_text,
            routing_text=routing_text,
            resolved_symbol=resolved_symbol,
            actions=actions,
            executed_public=executed_public,
            pending_actions=pending_actions,
            answer_mode="template_observation",
        )
        if route_plan.source.startswith("llm") and not offline:
            try:
                answer_result = await asyncio.wait_for(
                    _synthesize_answer_with_llm_async(
                        gateway=gateway,
                        messages=request.messages,
                        routing_text=routing_text,
                        executed_tools=executed_tools,
                        pending_actions=pending_actions,
                        trace=preliminary_trace,
                    ),
                    timeout=_llm_step_timeout(25.0, cap_seconds=25.0),
                )
                if answer_result.content:
                    reply = answer_result.content
                    answer_mode = answer_result.source
                elif answer_result.error:
                    answer_error = answer_result.error
            except TimeoutError:
                answer_error = "AnswerNode timeout"
    elif offline:
        answer_mode = "offline"
        reply = route_plan.clarification or _offline_reply(routing_text, actions)
    elif pending_actions:
        answer_mode = "planned_actions"
        reply = _reply_from_pending_actions(
            pending_actions,
            auto_execute=request.auto_execute,
            route_error=route_plan.error,
        )
    else:
        try:
            answer_mode = "llm_text"
            reply = await asyncio.wait_for(
                _gateway_chat_async(
                    gateway,
                    system=_system_prompt(),
                    messages=request.messages,
                    temperature=settings.ai_temperature,
                    max_tokens=settings.ai_max_tokens,
                ),
                timeout=_llm_step_timeout(30.0, cap_seconds=30.0),
            )
        except (LLMGatewayError, TimeoutError) as exc:
            offline = True
            answer_mode = "fallback_after_llm_error"
            reply = f"{route_plan.clarification or _offline_reply(routing_text, actions)}（模型网关：{exc}）"

    trace = _build_trace_payload(
        route_plan=route_plan,
        latest_text=latest_text,
        routing_text=routing_text,
        resolved_symbol=resolved_symbol,
        actions=actions,
        executed_public=executed_public,
        pending_actions=pending_actions,
        answer_mode=answer_mode,
        answer_error=answer_error,
    )

    artifact = await create_artifact(
        session,
        kind="ai_chat",
        status="completed",
        input_summary=latest_text[:500],
        tool_calls=[action.model_dump() for action in actions],
        key_outputs={
            "reply": reply[:2000],
            "offline": offline,
            "executed_tools": executed_public,
            "trace": trace,
        },
    )
    response = AIChatResponse(
        message=AIChatMessage(role="assistant", content=reply),
        actions=pending_actions,
        executed_tools=executed_public,
        trace=trace,
        artifact_id=artifact.artifact_id,
        model=model,
        offline=offline,
    )
    return _ok(response.model_dump())


@router.post("/tools/{tool_name}/execute")
async def execute_ai_tool(
    request: AIToolExecutionRequest,
    tool_name: str = Path(description="AI tool name, e.g. system.status"),
    session: AsyncSession = Depends(get_async_session),
) -> dict[str, Any]:
    artifact = await create_artifact(
        session,
        kind=f"tool:{tool_name}",
        status="running",
        input_summary=f"{tool_name} {request.arguments}",
        tool_calls=[{"tool_name": tool_name, "arguments": request.arguments}],
    )
    result = await get_ai_tool_registry().execute(
        tool_name,
        request.arguments,
        confirmed=request.confirmed,
        session=session,
    )
    status = "completed" if result.status == "ok" else result.status
    await update_artifact(
        session,
        artifact.artifact_id,
        status=status,
        result_ref=result.result_ref,
        key_outputs={
            "summary": result.summary,
            "result": result.result,
            "task_id": result.task_id,
        },
        error=result.error,
    )
    response = AIToolExecutionResponse(
        **result.model_dump(exclude={"artifact_id"}),
        artifact_id=artifact.artifact_id,
    )
    return _ok(response.model_dump())


@router.get("/artifacts")
async def list_ai_artifacts(
    kind: str | None = Query(None),
    limit: int = Query(50, ge=1, le=200),
    session: AsyncSession = Depends(get_async_session),
) -> dict[str, Any]:
    return _ok(await list_artifacts(session, kind=kind, limit=limit))


@router.get("/artifacts/{artifact_id}")
async def get_ai_artifact(
    artifact_id: str,
    session: AsyncSession = Depends(get_async_session),
) -> dict[str, Any]:
    artifact = await get_artifact(session, artifact_id)
    if artifact is None:
        return {"code": 1, "message": "artifact not found", "data": None}
    return _ok(artifact)
