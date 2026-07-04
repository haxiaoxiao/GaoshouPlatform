from __future__ import annotations

import os
import re
from datetime import datetime, timedelta
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Path, Query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.ai.answer import synthesize_answer_with_llm
from app.ai.artifacts import create_artifact, get_artifact, list_artifacts, update_artifact
from app.ai.gateway import LLMGatewayError, get_llm_gateway, reset_llm_gateway
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
)
from app.ai.tools import get_ai_tool_registry
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
    if any(word in lower for word in ["数据", "新鲜", "覆盖", "summary", "总览"]):
        actions.append(_tool_action("system.data_summary"))
    if any(word in lower for word in ["任务", "进度", "running", "runtime"]):
        actions.append(_tool_action("runtime.tasks", {"include_finished": True}))
    symbol = resolved_symbol or _stock_symbol_from_text(text)
    if symbol:
        actions.append(_tool_action("data.stock_snapshot", {"symbol": symbol}))
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
        if any(word in lower for word in ["指标", "因子", "indicator", "factor"]):
            actions.append(_tool_action("data.indicator_batch", {"symbols": [symbol]}))
        if any(word in lower for word in ["舆情", "情绪", "帖子", "雪球", "股吧", "sentiment"]):
            actions.append(_tool_action("sentiment.summary", {"symbol": symbol}))
    if any(word in lower for word in ["行情", "k线", "kline", "价格"]) and not symbol:
        actions.append(_tool_action("data.symbols", {"limit": 200}))
    if any(word in lower for word in ["选股", "筛选", "股票池", "screen"]):
        actions.append(_tool_action("data.stock_screen", {"limit": 100}))
    if any(word in lower for word in ["自选", "watchlist"]):
        actions.append(_tool_action("data.watchlist_groups"))
    if any(word in lower for word in ["行业", "申万"]):
        actions.append(_tool_action("data.industries"))
    if any(word in lower for word in ["指数", "成分", "399101", "中小综指", "小市值"]):
        actions.append(_tool_action("data.index_catalog", {"pool_only": True}))
        actions.append(_tool_action("data.index_pool", {"index_symbol": "399101.SZ"}))
    if any(word in lower for word in ["因子", "factor", "算子", "指标"]):
        actions.append(_tool_action("indicator.catalog"))
        actions.append(_tool_action("compute.operators"))
        actions.append(_tool_action("factor_value.definitions"))
    if any(word in lower for word in ["因子值", "factor value", "覆盖率", "缓存"]):
        actions.append(_tool_action("factor_value.definitions"))
        actions.append(_tool_action("factor_value.groups"))
    if any(word in lower for word in ["数据浏览", "explorer", "表", "sql"]):
        actions.append(_tool_action("explorer.tables"))
    if any(word in lower for word in ["parquet", "数据湖", "数据集"]):
        actions.append(_tool_action("parquet.datasets"))
    if any(word in lower for word in ["同步", "sync"]):
        actions.append(_tool_action("data.sync_status"))
        actions.append(_tool_action("data.sync_catalog"))
        actions.append(_tool_action("data.sync_logs", {"limit": 50}))
        actions.append(_tool_action("data.sync_submit", {"sync_type": "stock_info", "sync_mode": "range"}))
    if any(word in lower for word in ["回测", "backtest"]):
        actions.append(_tool_action("backtest.capabilities"))
        actions.append(_tool_action("backtest.records"))
        actions.append(_tool_action("backtest.submit"))
    if any(word in lower for word in ["策略", "strategy"]):
        actions.append(_tool_action("strategy.list"))
    live_lower = lower.replace("交易日", "")
    if any(word in live_lower for word in ["实盘", "下单", "qmt", "runner", "账户", "持仓", "委托", "成交", "撤单"]):
        actions.append(_tool_action("live_trading.status"))
        actions.append(_tool_action("live_trading.account", {"mode": "paper"}))
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
    return deduped[:6]


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


def _preview_tool_result(item: dict[str, Any]) -> list[str]:
    result = item.get("result")
    tool_name = str(item.get("tool_name") or "")
    lines: list[str] = []
    if tool_name == "data.market_snapshot" and isinstance(result, dict):
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
        realtime_error = result.get("realtime_error")
        if realtime_error:
            lines.append(f"实时行情：不可用（{realtime_error}）")
    elif tool_name == "data.kline_daily" and isinstance(result, list):
        if not result:
            lines.append("本地未返回该日期区间的日 K 记录。")
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
    route_plan = route_actions_with_llm(
        gateway=gateway,
        messages=request.messages,
        page_context=request.page_context,
        context_hints=context_hints,
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
    executed_public = [{key: value for key, value in item.items() if key != "result"} for item in executed_tools]
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
            answer_result = synthesize_answer_with_llm(
                gateway=gateway,
                messages=request.messages,
                routing_text=routing_text,
                executed_tools=executed_tools,
                pending_actions=pending_actions,
                trace=preliminary_trace,
            )
            if answer_result.content:
                reply = answer_result.content
                answer_mode = answer_result.source
            elif answer_result.error:
                answer_error = answer_result.error
    elif offline:
        answer_mode = "offline"
        reply = route_plan.clarification or _offline_reply(routing_text, actions)
    else:
        try:
            answer_mode = "llm_text"
            reply = gateway.chat(
                system=_system_prompt(),
                messages=request.messages,
                temperature=settings.ai_temperature,
                max_tokens=settings.ai_max_tokens,
            )
        except LLMGatewayError as exc:
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
