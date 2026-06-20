"""研报转策略 — PDF/TXT 解析 + LLM 生成策略代码"""
from __future__ import annotations

import json
import re
from pathlib import Path

from anthropic import Anthropic
from loguru import logger

# 平台知识库 — 数据字典 + 策略模板
_SYSTEM_PROMPT = """你是量化策略开发专家。你精通 A 股数据分析和 Python 回测代码编写。

## 平台数据能力

Parquet 日K线 (klines_daily): 2006~2026, 5204只, 字段: symbol/trade_date/open/high/low/close/volume/amount
Parquet 周K线 (klines_weekly): 2011~2026, 从日线聚合, 同上字段
Parquet 指标/因子缓存: symbol/indicator_name/trade_date/value 或 symbol/expr_hash/trade_date/value

可用指标: pe_ttm(2011~2026), pb, dividend_yield(2006~2026), dividend_cash(1993~2026),
return_5d/20d/60d, volatility_20d, ma5/ma10/ma20, rsi_14, roe, turnover_rate, ma250_weekly, price_to_ma250w

## 三种策略类型

### 1. 表达式 (expression) — 单因子公式
一行表达式，如: close/MA(close,20)-1, RSI(close,14)
适用: 只有一个因子公式的情况

### 2. 脚本 (builtin=script) — 事件驱动 handle_bar
Python 脚本: def init(context) + def handle_bar(context, bar_dict)
可用函数: context.get_daily_close(sym,date), context.get_weekly_ma(sym,period,date),
  context.get_indicator(sym,name,date), context.get_all_symbols(), order_shares/order_value
适用: 多条件选股+定时调仓

### 3. 内置策略 (builtin=builtin) — 独立 Python 类
批量本地数据查询, class DeepValueStrategy: def screen() + def run()
一次查询全量数据, 批量筛选, 不依赖事件引擎
适用: 全市场筛选+高频批量查询

## 输出格式
{ "strategy_type": "expression|script|builtin", "name": "策略名", "code": "完整代码",
  "summary": "一句话", "conditions": ["条件"], "frequency": "调仓频率" }

代码必须能直接运行。不要包含 markdown 标记。"""


def _build_user_prompt(report_text: str) -> str:
    safe_text = report_text[:12000]  # 截断以防超 token 限制
    return f"""根据以下研报内容生成量化策略代码。

研报内容:
{safe_text}

要求:
1. 提取选股条件、调仓频率、仓位管理规则
2. 选择最合适的策略类型 (expression/script/builtin)
3. 生成完整可运行的 Python 代码
4. 用 JSON 格式输出, 包含: strategy_type, name, code, summary, conditions, frequency

JSON:"""


def parse_report(file_path: str) -> str:
    """提取 PDF/TXT 文本"""
    path = Path(file_path)
    if path.suffix.lower() == '.pdf':
        try:
            import pdfplumber
            text = ""
            with pdfplumber.open(path) as pdf:
                for page in pdf.pages:
                    t = page.extract_text()
                    if t:
                        text += t + "\n"
            return text.strip() or "PDF 文本提取失败"
        except Exception as e:
            logger.error(f"PDF parse error: {e}")
            return f"PDF 解析失败: {e}"
    elif path.suffix.lower() in ('.txt', '.md'):
        return path.read_text(encoding='utf-8', errors='ignore')
    else:
        return path.read_text(encoding='utf-8', errors='ignore')


def generate_strategy(report_text: str) -> dict:
    """调用 LLM 生成策略"""
    client = Anthropic()
    response = client.messages.create(
        model="deepseek-v4-pro",
        max_tokens=4096,
        temperature=0.3,
        system=_SYSTEM_PROMPT,
        messages=[{"role": "user", "content": _build_user_prompt(report_text)}],
    )
    content = response.content[0].text

    # 提取 JSON
    json_match = re.search(r'\{[\s\S]*\}', content)
    if json_match:
        try:
            return json.loads(json_match.group())
        except json.JSONDecodeError:
            pass

    return {
        "strategy_type": "script",
        "name": "研报策略",
        "code": content,
        "summary": "LLM 返回非 JSON 格式, 已保存原始输出",
        "conditions": [],
        "frequency": "每日",
    }
