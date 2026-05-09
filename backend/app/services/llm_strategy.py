"""LLM 策略生成服务 — 代码转换 + 研报对话生成 akquant 策略

Session 存储在 Redis，TTL 1 小时自动清理。
"""

from __future__ import annotations

import json
import re
import uuid
from typing import Any

from anthropic import Anthropic
from loguru import logger

from app.cache.redis_cache import get_redis_client

SESSION_TTL = 3600  # 1 小时，秒
SESSION_PREFIX = "llm:chat:"

# ── Prompts ──

CONVERT_SYSTEM = """你是量化策略专家，精通多种回测框架之间的代码转换。

你的任务是将用户提供的策略代码转换为 AKQuant 框架的格式。

## AKQuant 策略格式要求

```python
import akquant as aq
import numpy as np

class MyStrategy(aq.Strategy):
    def on_start(self):
        # 初始化指标（可选）
        pass

    def on_bar(self, bar):
        \"\"\"每根K线触发一次\"\"\"
        pos = self.get_position(bar.symbol)
        # bar.open, bar.high, bar.low, bar.close, bar.volume
        # self.buy(symbol, quantity) / self.sell(symbol, quantity)
        # self.close_position(symbol)
        # self.get_history(count, symbol, field) -> np.ndarray
        pass
```

## 关键API对照
- RQAlpha context.portfolio → self.get_position(symbol) / self.get_positions()
- RQAlpha context.order_value(sym, val) → self.buy(sym, qty)
- RQAlpha bar_dict[sym].close → bar.close (当前symbol的bar)
- RQAlpha history(sym, 20) → self.get_history(20, sym, 'close')
- Backtrader self.datas[0].close[0] → bar.close
- VNPY on_bar(bar) → def on_bar(self, bar)

## 规则
1. 输出纯 Python 代码，不要 markdown 标记，不要解释文字
2. 保留原策略的核心逻辑和参数
3. 必须继承 aq.Strategy，必须包含 def on_bar(self, bar)
4. 如果原策略涉及多只股票，使用 self.get_position(symbol) 区分
5. 不要使用原框架特有的API（context/self.datas/on_tick等）
"""

CHAT_SYSTEM = """你是量化策略开发专家。你精通 A 股数据分析，擅长根据研报编写 AKQuant 回测策略代码。

## AKQuant 策略格式

```python
import akquant as aq
import numpy as np

class MyStrategy(aq.Strategy):
    def on_start(self):
        pass

    def on_bar(self, bar):
        pos = self.get_position(bar.symbol)
        # bar.open, bar.high, bar.low, bar.close, bar.volume
        # self.buy(symbol, quantity) → 买入
        # self.sell(symbol, quantity) → 卖出
        # self.close_position(symbol) → 清仓
        # self.get_history(n, symbol, 'close') → np.ndarray 最近n根bar收盘价
        pass
```

## A 股交易规则
- T+1 制度，当日买入次日才能卖出
- 最小交易单位 100 股（1手）
- 涨跌停 ±10%（普通股），ST股 ±5%
- 佣金默认万三（0.0003），印花税千一仅卖出

## 对话规则
1. 先理解研报中的选股逻辑、调仓频率、风控规则
2. 如有不明确的地方，向用户提问确认
3. 确认完毕后，生成完整可运行的代码
4. 代码输出格式：单独一行 ```python 后跟代码，再一行 ``` 结束
5. 代码必须能直接复制到回测平台运行

## 平台数据
日线数据字段: open, high, low, close, volume (通过 bar 对象访问)
可用指标: self.get_history() 获取历史K线自行计算均线/RSI/MACD等
"""


def _get_llm() -> Anthropic:
    return Anthropic()


def _extract_text(response) -> str:
    """从 LLM response 提取文本，跳过 ThinkingBlock"""
    parts = []
    for block in response.content:
        if hasattr(block, "text"):
            parts.append(block.text)
    return "\n".join(parts)


def _extract_code(content: str) -> str | None:
    """从 LLM 回复中提取 Python 代码块"""
    m = re.search(r'```python\s*\n([\s\S]*?)```', content)
    if m:
        return m.group(1).strip()
    # fallback: 查找 class ... Strategy 开头的代码
    m = re.search(r'(import akquant[\s\S]*?class\s+\w+[\s\S]*?(?:\n\S|\Z))', content)
    if m:
        return m.group(1).strip()
    return None


# ── 代码转换（无状态）──

def convert_to_akquant(source_code: str) -> str:
    """单次调用：将任意策略代码转为 akquant 格式"""
    client = _get_llm()
    response = client.messages.create(
        model="deepseek-v4-pro",
        max_tokens=4096,
        temperature=0.2,
        timeout=60.0,
        system=CONVERT_SYSTEM,
        messages=[{
            "role": "user",
            "content": f"请将以下策略代码转换为 AKQuant 格式:\n\n```\n{source_code[:8000]}\n```",
        }],
    )
    content = _extract_text(response)
    code = _extract_code(content)
    return code or content.strip()


# ── 研报对话（有状态，Redis 会话）──

def _session_key(session_id: str) -> str:
    return f"{SESSION_PREFIX}{session_id}"


def create_chat_session(report_text: str, report_filename: str = "") -> dict:
    """创建对话会话，返回首次 LLM 回复"""
    session_id = str(uuid.uuid4())[:12]
    redis = get_redis_client()

    # 初始化会话
    messages: list[dict] = [
        {"role": "user", "content": f"我正在阅读一份研报 ({report_filename})。请理解其中的策略逻辑。\n\n研报内容:\n{report_text[:12000]}\n\n请总结研报中的选股逻辑、调仓频率和风控规则，然后询问我是否需要调整或补充。如果逻辑已经清晰，可以直接生成策略代码。"}
    ]

    client = _get_llm()
    response = client.messages.create(
        model="deepseek-v4-pro",
        max_tokens=4096,
        temperature=0.3,
        timeout=60.0,
        system=CHAT_SYSTEM,
        messages=messages,
    )
    reply = _extract_text(response)

    # 保存到 Redis
    messages.append({"role": "assistant", "content": reply})
    session_data = {
        "report_filename": report_filename,
        "messages": messages,
    }
    redis.set(_session_key(session_id), json.dumps(session_data, ensure_ascii=False), ttl=SESSION_TTL)
    logger.info("Chat session {} created, TTL={}", session_id, SESSION_TTL)

    code = _extract_code(reply)
    return {
        "session_id": session_id,
        "reply": reply,
        "code": code,
    }


def send_chat_message(session_id: str, message: str) -> dict:
    """发送消息到已有会话，返回 LLM 回复"""
    redis = get_redis_client()
    raw = redis.get(_session_key(session_id))
    if not raw:
        raise ValueError(f"会话 {session_id} 不存在或已过期（1小时有效）")

    session_data: dict = json.loads(raw)
    messages: list[dict] = session_data["messages"]

    # 追加用户消息
    messages.append({"role": "user", "content": message})

    client = _get_llm()
    response = client.messages.create(
        model="deepseek-v4-pro",
        max_tokens=4096,
        temperature=0.3,
        timeout=60.0,
        system=CHAT_SYSTEM,
        messages=messages,
    )
    reply = _extract_text(response)

    # 更新 Redis
    messages.append({"role": "assistant", "content": reply})
    redis.set(_session_key(session_id), json.dumps(session_data, ensure_ascii=False), ttl=SESSION_TTL)
    logger.info("Chat session {} updated, {} messages", session_id, len(messages))

    code = _extract_code(reply)
    return {
        "session_id": session_id,
        "reply": reply,
        "code": code,
    }
