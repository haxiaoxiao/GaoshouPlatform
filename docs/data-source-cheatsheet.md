# 数据源小抄：miniQMT、Tushare、AKShare

本文记录 ID=43 小市值策略对齐聚宽过程中验证过的数据源经验。平台默认仍以 miniQMT/xtquant 为主数据源；Tushare 和 AKShare 用作补历史缺口、指数成分和退市股数据的兜底。

Last updated: 2026-05-25.

Implementation note (2026-05-26): 平台已新增 `sync_type="tushare_relay"`。第一批接入 `adj_factor`、`moneyflow`、`ths_index`、`ths_member`、`block_moneyflow`、`stk_auction_replay`，统一落到本地 Parquet；新闻、公告、研报保留为受限补充源，默认不做全量抓取。

当前默认运行方式是 `MARKET_DATA_BACKEND=parquet`：同步和补数仍以 miniQMT/xtquant、Tushare、Indevs/Tushare Replay 等数据源为入口，但回测、因子研究和 DataSkill 应优先读取本地 SQLite + Parquet/DuckDB。策略运行时不要反复访问外部数据源。

## 总体优先级

| 场景 | 首选 | 兜底 | 说明 |
|---|---|---|---|
| 实时行情、实盘相关 | miniQMT | 无 | 本地券商/QMT 环境最可靠，外部接口不适合实盘。 |
| 当前 A 股基础信息 | miniQMT | Tushare / AKShare | QMT 能给当前 instrument detail；Tushare/AKShare 可补名称、行业等。 |
| 普通在市股票日线 | miniQMT | Tushare, AKShare | QMT 本地缓存优先；Tushare/AKShare 可补历史区间。 |
| 退市/历史股票日线 | Tushare | AKShare | QMT 对部分退市股会返回成交量但 OHLC 为 0，不能直接入库。 |
| 指数日线 | miniQMT | Tushare `index_daily` | `000001.SH` 这类指数 QMT 可用；Tushare 指数接口也可用。 |
| 指数历史成分 | Tushare `index_weight` | 手工快照 / 现有自选池临时降级 | 小市值指数成分必须 point-in-time，不能用当前 960 只代替。 |
| 财务/股本/历史市值 | Tushare `daily_basic` | miniQMT 财务缓存 / AKShare 个股信息 | 退市股股本和市值用 Tushare 更完整。 |
| 固定时间点分钟线 | Parquet/DuckDB 或 ClickHouse 已落库分钟线 | miniQMT 本地缓存 | 回测只读本地列式库；不要在策略运行时反复打 QMT。 |
| 完整历史 1 分钟线 | 本地 JQ 分钟文件 → Parquet `klines_minute` | miniQMT/Indevs 补缺口 | 已导入 2005-01-04 至 2026-05-15 全 A 分钟线，适合回测和 timer 抽点。 |
| 因子值缓存 | Parquet `factor_values` | 重新预计算 | 当前因子研究、Alpha101、TA-Lib 和小市值因子统一写入 Factor Value Store。 |

## miniQMT / xtquant

优势：
- 平台主数据源，和本地 QMT 缓存、实时行情、后续交易链路一致。
- 适合当前股票基础信息、实时 quote、在市股票 K 线、指数日线。
- `000001.SH` 指数日线验证可正常返回有效 OHLC。

适用场景：
- 常规同步：`stock_info`、`stock_full`、`kline_daily`、`kline_minute`、`realtime_mv`。
- 回测前补当前股票池中仍在市股票的数据。
- 实盘/盘中数据，不应依赖 Tushare/AKShare。

注意事项：
- xtquant 是同步阻塞 SDK，必须通过 `asyncio.get_running_loop().run_in_executor()` 或 `asyncio.to_thread()` 包装。
- 财务下载不要用 `download_financial_data`，会卡死；只用 `download_financial_data2(callback=None)`。
- 对部分退市股，`get_market_data_ex` 会返回行，但 `open/high/low/close/amount` 为 0 或 `-0.0`，这类数据必须判无效，不能入库。
- QMT 股票详情对退市股常返回 `None`，不能作为历史回测唯一元数据来源。
- miniQMT 分钟线权限与服务端实际下发窗口需要实测确认，不能只看“最多 5 年”的口头范围。当前环境主动调用 `download_history_data2(period='1m')` 后，`2026-04/2026-05` 和 `2025-06` 可落盘并读回；`2025-05-15` 可读回 241 根，`2025-05-14` 可读回 123 根；`2025-05-07`、`2025-01`、`2024`、`2021` 服务端返回 0 根。
- `download_history_data2` 是当前验证可真实落盘的主动下载入口；旧 `download_history_data` 对同一历史区间可能返回 `None` 但 DAT 文件不增长。分钟线网关应优先使用 `download_history_data2`，再兜底旧接口。
- `get_market_data_ex(period='1m', start_time='YYYYMMDDHHMMSS', end_time='YYYYMMDDHHMMSS')` 对未缓存/服务端不下发的历史区间会返回空；`start_time='' + end_time + count` 只能回退本地已缓存切片，不能自动补齐 2021 年历史。
- 批量补 timer 分钟线要按月或更小区间分片，并设置单票超时，避免一次请求多年数据时无法判断进度。
- 对 timer 回测，先以 Parquet/ClickHouse 中 `(10:00,10:30,14:30,14:50)` 稀疏分钟点的覆盖率探测最早可跑日期。主动补数只能补 QMT 当前还能下发的缺口；完整 JQ 分钟 Parquet 已覆盖更长历史。

### miniQMT 分钟线下载和读取流程

已验证的正确链路：

1. 先用 `download_history_data2(stock_list, period='1m', start_time, end_time, callback, incrementally=True)` 主动补充历史分钟数据。
2. 下载完成后优先用 `get_local_data(..., period='1m', data_dir=...)` 或平台封装读取本地数据，避免每次回测都打 QMT 服务端。
3. 平台只抽取策略需要的固定时间点，例如 `10:00`、`10:30`、`14:30`、`14:50`，写入 Parquet `klines_minute_timer` 或 ClickHouse `klines_minute`。
4. 回测阶段从 Parquet/ClickHouse 读 `bar_type="minute_timer"`，不直接读 QMT，不加载完整分钟线。

注意：

- `data_dir` 应指向 miniQMT 配套的 `userdata_mini` 数据目录。用户本机手动下载目录可从 QMT 客户端配置或 `xtdata.data_dir` 确认。
- 手动客户端能下载的区间，不代表脚本已经正确读取到本地文件；要分别验证“下载完成”和“本地读取返回行数”。
- 对 960 只左右的中小综指动态成分池，稀疏 timer 数据量可控，适合先同步进 Parquet/ClickHouse 再回测。
- 不建议在策略运行中按股票即时调用 QMT 拉分钟线；会导致回测不可复现、速度慢，也更难定位缺口。

推荐检查：

```powershell
$env:PYTHONPATH='E:\Projects\GaoshouPlatform\backend'
.\backend\.venv\Scripts\python.exe backend/app/scripts/sync_timer_minute_points.py `
  --index-symbol 399101.SZ `
  --start 20210515 `
  --end 20260508 `
  --times 10:00,10:30,14:30,14:50
```

覆盖率检查：

```text
GET /api/backtest/timer-coverage?index_symbol=399101.SZ&start_date=2021-05-15&end_date=2026-05-08&times=10:00,10:30,14:30,14:50
```

## 本地 JQ 1 分钟数据 → Parquet

已验证本地目录：

```text
E:\Projects\QuantData\JQ_a_minute\闲鱼商品_A股1分钟数据_聚宽版\01_数据文件
```

源文件形态：

- 年度/批次 Parquet：`time, code, open, close, high, low, volume, money[, paused]`
- 2026-04 之后的 zip/tar.gz 归档：`datetime, open, high, low, close, volume, amount`
- 聚宽代码后缀会转换为平台后缀：`.XSHE -> .SZ`，`.XSHG -> .SH`，`.BJSE -> .BJ`

目标数据集：

```text
E:\Projects\GaoshouPlatform\data\parquet\klines_minute\year=YYYY\month=MM\part-*.parquet
```

当前导入覆盖：

| 指标 | 结果 |
|---|---|
| 时间范围 | `2005-01-04 09:31:00` 至 `2026-05-15 15:00:00` |
| 行数 | 约 `3,753,384,618` |
| 股票数 | `5580` |
| 状态库 | `data/parquet/import_state/jq_minute_import.sqlite` |
| 异常清理 | 已删除 `2026-05-01/04/05` 节假日碎片 |

常用命令：

```powershell
$env:PYTHONPATH='E:\Projects\GaoshouPlatform\backend'
$src='E:\Projects\QuantData\JQ_a_minute\闲鱼商品_A股1分钟数据_聚宽版\01_数据文件'

# 导入 JoinQuant 风格 parquet
.\backend\.venv\Scripts\python.exe -m app.scripts.import_jq_minute_parquet `
  --source $src `
  --file-glob 'stock_1m_*.parquet' `
  --start 2021-03-01 `
  --end 2026-03-31 `
  --duckdb-memory-limit 20GB `
  --threads 4

# 导入 zip/tar.gz 归档
.\backend\.venv\Scripts\python.exe -m app.scripts.import_jq_minute_archives `
  --source $src `
  --pattern 'a_share_mins_2026-*.zip'

# 清理异常交易日碎片，默认 dry-run，加 --apply 才会重写分区
.\backend\.venv\Scripts\python.exe -m app.scripts.clean_minute_parquet_dates `
  --start 2026-05-01 `
  --end 2026-05-15 `
  --min-symbols 1000 `
  --min-positive-volume-rows 100000

# 月分区去重压缩
.\backend\.venv\Scripts\python.exe -m app.scripts.compact_jq_minute_duckdb `
  --year 2026 `
  --month 03
```

注意：

- Parquet 导入脚本有断点状态库，重复执行会跳过已成功文件；带 `--force` 才会重跑。
- 源文件中有些日期范围外的空批次，脚本会标记为成功且 `rows=0`。
- `volume=0` 的分钟不一定是坏数据，可能是个股当分钟无成交；应按全市场截面和交易日历判断异常。
- 节假日小样本归档不能直接作为交易日使用，需用 `clean_minute_parquet_dates.py` 清洗。
- 回测和 DataSkill 读取应通过 `get_market_data_store()` / `ParquetMarketDataStore`，不要直接拼接文件路径。

## Tushare

优势：
- 对退市/历史 A 股覆盖明显优于 QMT 当前 instrument 视角。
- `pro.daily` 可获取退市股有效 OHLC；本次验证 `002071.SZ`、`002473.SZ` 等均可返回。
- `daily_basic` 可取历史总股本、流通股本、总市值、流通市值，适合小市值排序对齐。
- `stock_basic(list_status='D')` 可查退市股票名称、上市日期、退市日期。
- `index_weight` 可取 `399101.SZ` 历史成分权重快照，是还原聚宽 `get_index_stocks('399101.XSHE')` 的关键。
- `index_daily` 可补指数日线。

适用场景：
- 历史回测补缺，尤其是 2020 起包含退市股的策略。
- 指数池 point-in-time 成分同步。
- 历史股本/市值快照，用于避免小市值排序只用当前市值或估算值。

注意事项：
- 环境变量可能没有 `TUSHARE_TOKEN`/`TS_TOKEN`，但本机 `tushare.get_token()` 可能已有本地 token；脚本应按 `env -> ts.get_token()` 顺序读取。
- `pro.daily` 股票日线的 `amount` 单位是千元，落到 ClickHouse `klines_daily.amount` 时应乘以 `1000`。
- `index_daily.amount` 通常也是千元口径，和现有 QMT 数据对齐时要检查单位。
- `index_weight` 不是每天都有快照。策略应在调仓日用 `trade_date <= as_of` 的最近一次快照，而不是要求当天必须有成分。
- `stock_basic` 默认只查在市股票；退市股要显式 `list_status='D'`。

## AKShare

优势：
- 无 token，适合临时补单只股票、快速验证行情。
- `stock_zh_a_hist` 对部分退市股能返回有效 OHLC，和 Tushare 数值基本一致。
- `stock_individual_info_em` 可补当前股票名称、总股本、流通股、行业等。

适用场景：
- Tushare 不可用或被限流时，补单只/少量历史日线。
- 快速人工验证某只股票历史价格。
- 当前在市股票的个股信息兜底。

注意事项：
- 批量连续请求容易被东财接口断开，需要限速、重试。
- `stock_zh_a_daily` 对部分退市股会 JSONDecodeError；退市股优先试 `stock_zh_a_hist`。
- `stock_individual_info_em` 对退市股常只返回简称，股本/市值为 `-`，不能依赖它做历史市值。
- AKShare 可作为补充，但不要替代 miniQMT 成为平台主数据源。

## 小市值策略的关键数据原则

1. 股票池不能用当前自选池静态代替历史指数池。聚宽源码在每次调仓调用 `get_index_stocks('399101.XSHE')`，平台应使用 `399101.SZ` 的历史成分快照。
2. 指数成分表应保存 `index_symbol, jq_index_symbol, symbol, trade_date, weight, source`，查询时取 `trade_date <= 调仓日` 的最近快照。
3. 回测引擎可以预加载整个区间的成分并集，但策略调仓必须按当日快照筛选。
4. 日线交易价格应避免未来数据。当前策略按用户要求使用 open 作为交易价，不用当天 close 作为成交价。
5. 小市值排序优先使用历史股本/市值快照；没有快照时可用 `total_shares * previous_close` 估算，再兜底 SQLite 当前市值。
6. 如果策略只在固定时间执行，不需要完整分钟线；使用 `minute_timer` 稀疏分钟 bar，并通过 `on_timer` 或 AKQuant timer 机制触发。
7. 前端选择股票池时应选择指数池，例如“中小综指 / 399101.SZ”，而不是从自选股中选择固定 960 只股票。
8. 回测起始日期应由控制面板和数据覆盖情况共同决定。若 QMT 分钟权限只能覆盖 2021-05-15 之后，则全量回测以最早可用 timer 覆盖日期作为起点。
9. 行业集中度、ST、停牌、涨跌停、退市股处理都属于与聚宽对齐的关键差异点，需要在年度 debug 报告中单独输出。

## 已落地脚本和接口

- 指数成分缓存服务：`backend/app/services/index_components.py`
- 指数池接口：`GET /api/backtest/index-pools/{index_symbol}?start_date=YYYY-MM-DD&end_date=YYYY-MM-DD`
- 稀疏 timer 分钟线同步脚本：`backend/app/scripts/sync_timer_minute_points.py`
- timer 覆盖探测接口：`GET /api/backtest/timer-coverage?index_symbol=399101.SZ&start_date=2021-05-15&end_date=2026-05-08&times=10:00,10:30,14:30,14:50`

## 当前推荐工作流

1. 前端回测页选择 `engine="akquant"`。
2. 股票池选择指数池“中小综指 / 399101.SZ”。
3. `bar_type` 选择 `minute_timer`。
4. timer 时间从控制面板传入，例如 `10:00,10:30,14:30,14:50`。
5. 先跑 timer 覆盖检查，确认最早可用日期。
6. 对缺失区间先用 miniQMT 主动下载，再同步所需 timer 点到 Parquet/ClickHouse。
7. 回测只从 Parquet/ClickHouse 读取数据。
8. 若和聚宽结果差异大，按年度导出持仓、订单、日志，对比以下差异点：
   - 指数成分快照
   - 市值排序输入
   - 行业集中度过滤
   - ST/停牌/退市过滤
   - 涨跌停可成交判断
   - AKQuant 与 JQ 的成交时点语义

常用检查：

```powershell
$env:PYTHONPATH='E:\Projects\GaoshouPlatform\backend'
```

```powershell
$env:PYTHONPATH='E:\Projects\GaoshouPlatform\backend'
@'
import asyncio
from datetime import date
from app.services.index_components import load_index_symbols

async def main():
    symbols = await load_index_symbols('399101.SZ', date(2020, 1, 1), date(2020, 2, 10))
    print(len(symbols), symbols[:5], symbols[-5:])

asyncio.run(main())
'@ | .\backend\.venv\Scripts\python.exe -
```
