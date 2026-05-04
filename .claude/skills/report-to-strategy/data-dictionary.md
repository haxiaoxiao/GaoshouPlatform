# GaoshouPlatform 数据字典

## ClickHouse 表

### klines_daily — 日K线
| 字段 | 类型 | 说明 |
|------|------|------|
| symbol | String | 股票代码 (e.g. 600000.SH) |
| trade_date | Date | 交易日期 |
| open/high/low/close | Decimal(10,4) | OHLC |
| volume | UInt64 | 成交量(股) |
| amount | Decimal(18,4) | 成交额(元) |
| turnover_rate | Decimal(8,4) | 换手率 |

数据范围: 2006-01-04 ~ 2026-04-30, 5204只股票

### klines_weekly — 周K线
同日K线结构。从日线聚合生成(resample W, open=first, close=last)。
数据范围: 2011~2026, 5178只股票

### stock_indicators — 截面指标
| 字段 | 类型 | 说明 |
|------|------|------|
| symbol | String | 股票代码 |
| indicator_name | String | 指标名 |
| trade_date | Date | 日期 |
| value | Float64 | 指标值 |

可用指标:

| 指标名 | 类别 | 说明 | 数据范围 |
|--------|------|------|----------|
| pe_ttm | 估值 | 市盈率TTM | 2011-03~2026-05, 5636股 |
| pb | 估值 | 市净率 | 同 pe_ttm |
| dividend_yield | 估值 | 股息率(近12月分红/股价) | 2006-02~2026-05, 4326股 |
| dividend_cash | 分红 | 每股现金分红(元) | 1993~2026, 4811股 |
| return_5d/20d/60d | 动量 | N日涨幅 | 依赖日线数据 |
| volatility_20d | 波动 | 20日波动率(年化) | 依赖日线数据 |
| ma5/ma10/ma20 | 技术 | N日均线 | 依赖日线数据 |
| rsi_14 | 技术 | 14日RSI | 依赖日线数据 |
| roe | 质量 | 净资产收益率 | 依赖财务数据 |
| turnover_rate | 流动性 | 换手率 | 依赖日线数据 |
| ma250_weekly | 技术 | 250周均线 | 依赖周线数据 |
| price_to_ma250w | 估值 | 价格/250周线比值 | 依赖周线数据 |

## SQLite 表

### stocks — 股票基础信息
symbol, name, exchange, industry(申万一级), list_date, is_st, total_mv(万元), circ_mv(万元), pe_ttm, pb, roe, total_shares(万股)

### financial_data — 季度财务
symbol, report_date, eps, bvps, roe, revenue(万元), net_profit(万元), revenue_yoy, profit_yoy, gross_margin, total_assets, total_liability, total_equity

数据范围: 2023-09 ~ 2026-05

## 策略中可用的函数 (脚本模式 only)

在 `handle_bar(context, bar_dict)` 中:
- `context.get_daily_close(symbol, as_of_date)` → 获取日收盘价
- `context.get_weekly_ma(symbol, period, as_of_date)` → 获取周均线
- `context.get_indicator(symbol, indicator_name, trade_date)` → 查询 stock_indicators
- `context.get_all_symbols()` → 获取全量A股列表
- `order_shares(symbol, shares)` → 下单
- `order_value(symbol, value)` → 按金额下单
- `context.portfolio.get_position(symbol)` → 获取持仓
