---
name: report-to-strategy
description: Use when the user uploads a research report (PDF/TXT) and asks to generate a trading strategy from it, or says "基于研报生成策略", "从研报提取策略", "analyze this report and create a strategy"
---

# Report-to-Strategy

把研报（PDF/TXT）转化为可执行的量化策略代码。

## Overview

读取研报 → 提取策略逻辑 → 匹配合适的策略类型 → 生成代码 → 填充到编辑器

## When to Use

- 用户上传 PDF/TXT 文件说"帮我生成策略"
- 用户说"基于这份研报写个回测"
- 用户粘贴研报文本要求提取交易逻辑

## Strategy Types

平台支持三种策略，按复杂度递增：

| 类型 | 适用场景 | 执行引擎 |
|------|----------|----------|
| **表达式** | 单因子或多因子线性组合 | Vectorized 分层回测 |
| **脚本** | 多条件选股 + 定时调仓 | Event-driven `handle_bar` |
| **内置** | 批量全市场筛选 + 高复杂度 | 独立 Python 类，直接查 ClickHouse |

**判断逻辑：**
- 只有 1 个因子公式 → 表达式
- 有明确的调仓频率 + 条件判断 → 脚本
- 需要全市场筛选 + 批量数据查询 → 内置

## Process

### Step 1: 提取研报文本

用户上传 PDF 后用 `pdftotext` 或 Python `pdfplumber` 提取文本。TXT 直接读取。

### Step 2: 解析策略要素

识别以下要素：
- **选股条件**: 指标阈值（PE<30, 股息率>3%）
- **调仓频率**: 每日/每周/每月/每年
- **仓位管理**: 等权/市值加权/固定数量
- **持有周期**: 固定天数/到期调仓
- **止损止盈**: 阈值条件
- **数据依赖**: 日线/分钟线/财务数据/周线

### Step 3: 选择策略类型

根据复杂度推荐：
- 只有 1 个因子公式 → **表达式**
- 有明确调仓频率 + 多个条件 → **脚本** (event-driven)
- 需要全市场批量筛选 → **内置策略** (ClickHouse 直查)

### Step 4: 映射到平台可用数据

参考 [data-dictionary.md](data-dictionary.md) 确认所需指标在 ClickHouse 中可用。

### Step 5: 生成代码

使用 [templates/](templates/) 中的模板生成代码。

### Step 6: 输出格式

输出 JSON 便于前端填充：
```json
{
  "strategy_type": "builtin|script|expression",
  "name": "策略名称",
  "code": "完整代码",
  "summary": "一句话描述",
  "conditions": ["条件1", "条件2"],
  "frequency": "年度调仓",
  "warnings": ["股息率依赖dividend_yield指标，需确认数据覆盖"]
}
```

## Code Templates

见 [templates/](templates/) 目录。内置策略模板为 `deep_value.py` 的结构骨架。
