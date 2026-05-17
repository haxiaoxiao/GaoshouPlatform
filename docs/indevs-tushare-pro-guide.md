# Indevs Tushare Pro Replay 接口小抄

Last updated: 2026-05-16.

本文记录 `https://ai-tool.indevs.in/quant/tushare-pro-catalog/` 下的新 Tushare Pro Replay / 聚合接口能力，以及在本项目里后续接入时应采用的调用方式、适用场景和已验证结果。

## 1. 基本调用规则

### 鉴权

接口通过 HTTP Header 鉴权：

```text
X-API-Key: <api-key>
```

项目代码里不要硬编码密钥。推荐使用环境变量：

```powershell
$env:INDEVS_TUSHARE_API_KEY="<your-api-key>"
```

### Base URL

目录页模板给出了三个 base：

```text
http://127.0.0.1:8000/tushare/pro
https://ai-tool.indevs.in/tushare/pro
https://tushare.indevs.in/tushare/pro
```

其中 `127.0.0.1:8000` 是本地代理入口，只有本机代理服务启动时可用；当前本机未监听 8000 端口，因此本轮验证显示连接失败。两个公网域名都可用，但不同接口/时段的超时表现不同。生产代码应做 base url 回退：

1. 优先请求 `http://127.0.0.1:8000/tushare/pro`，适合有本地代理或站内服务时使用。
2. 本地不可用、超时、连接重置、HTTP 5xx 时回退到 `https://ai-tool.indevs.in/tushare/pro`。
3. 继续失败时回退到 `https://tushare.indevs.in/tushare/pro`。
4. 单个请求设置连接超时和读取超时，不要无界等待。

### REST 路径

普通 Pro 风格接口：

```text
GET /tushare/pro/{api_name}
```

例如：

```text
GET https://ai-tool.indevs.in/tushare/pro/daily_basic?ts_code=000001.SZ&trade_date=20250110
```

A 股历史分钟回放：

```text
GET /tushare/pro/a_share_mins
```

集合竞价回放：

```text
GET /tushare/pro/stk_auction_replay
```

### 返回结构

多数接口返回 Tushare native envelope：

```json
{
  "code": 0,
  "msg": "ok",
  "data": {
    "fields": ["ts_code", "trade_date", "close"],
    "items": [["000001.SZ", "20250110", 11.3]]
  },
  "api_name": "daily_basic",
  "count": 1
}
```

解析方式：

```python
def rows(payload: dict) -> list[dict]:
    data = payload.get("data") or {}
    if isinstance(data, dict) and "fields" in data and "items" in data:
        return [dict(zip(data["fields"], row)) for row in data["items"]]
    if isinstance(data, list):
        return data
    return []
```

## 2. 已验证接口结果

以下为 2026-05-16 在本机使用新 key 的 smoke test 结果。测试优先排除了实时/latest/盘口快照类接口。

| 接口 | 场景 | 验证结果 | 备注 |
|---|---|---|---|
| `stock_basic` | A 股基础信息 | OK | 返回 `ts_code/symbol/name/market/exchange/list_status` 等；部分行业字段为空 |
| `daily_basic` | 日频估值/市值 | OK | 可返回 `close`；若显式请求部分字段，本次 `total_mv/circ_mv/pe_ttm/pb` 返回空字符串，需要扩大字段或改用默认字段再验证 |
| `adj_factor` | 复权因子 | OK | 返回 `adj_factor` |
| `index_daily` | 指数日线 | OK | `399101.SZ` 可返回 OHLCV/amount |
| `index_weight` | 指数权重 | 异常 | 对 `399101.SZ` 返回 `invalid_params: Excel file format cannot be determined...`，后续不能直接依赖，优先试聚合接口 |
| `get_index_stocks` | 聚合指数成分 | OK | `399101.SZ` 可返回 `index_symbol/con_code/con_name/in_date` |
| `get_trade_days` | 聚合交易日 | OK | `ai-tool` 超时后 `tushare.indevs.in` 成功 |
| `income` | 利润表 | OK | 返回最新报告期字段 |
| `balancesheet` | 资产负债表 | OK | 返回资产、负债、股东权益等字段 |
| `cashflow` | 现金流量表 | OK | 返回经营/投资/筹资现金流 |
| `anns_d` | 公告 | OK | `ai-tool` 超时后 `tushare.indevs.in` 成功 |
| `stk_auction_replay` | A 股集合竞价回放 | OK | `summary` 模式可返回 09:25 集合竞价摘要 |
| `a_share_mins` | A 股历史分钟回放 | OK | 可返回 1MIN 历史分钟；本次 2026-05-15 09:30-09:35 返回 2 条 |
| `ths_index` | 同花顺指数 | 超时 | 两个域名 15s 内均超时，需独立长超时或低频异步任务验证 |
| `get_all_securities` | 聚合证券列表 | 超时 | 两个域名 15s 内均超时，可能数据量大，后续加 `limit/分页/长超时` 验证 |

### 三 base 补测结果

2026-05-16 追加补测了目录页中的三个 base。结果如下：

| 接口 | `127.0.0.1:8000` | `ai-tool.indevs.in` | `tushare.indevs.in` |
|---|---|---|---|
| `stock_basic` | 连接失败，本地未监听 | OK，约 7.89s | OK，约 2.13s |
| `index_daily` | 连接失败，本地未监听 | OK，约 6.72s | OK，约 5.38s |
| `a_share_mins` | 连接失败，本地未监听 | OK，约 3.22s | OK，约 1.70s |
| `stk_auction_replay` | 连接失败，本地未监听 | OK，约 2.24s | OK，约 2.05s |
| `anns_d` | 连接失败，本地未监听 | OK，约 2.80s | OK，约 1.64s |

这轮样本里 `tushare.indevs.in` 普遍更快，但不能据此永久固定优先级。实现上仍建议保留三 base 回退，并记录每次请求的实际 base、耗时和错误。

## 3. 对平台最有价值的能力

### A 股历史分钟回放：`a_share_mins`

用途：

- 补充 miniQMT 无法稳定读取的历史分钟片段
- 给固定 timer 策略补 10:30、14:50 这类稀疏分钟点
- 盘后复盘、分时研究、分钟级因子验证

路径：

```text
GET /tushare/pro/a_share_mins
```

参数：

| 参数 | 示例 | 说明 |
|---|---|---|
| `ts_code` | `000001.SZ` | A 股代码 |
| `freq` | `1MIN` | 支持 `1MIN/5MIN/15MIN/30MIN/60MIN` |
| `start_date` | `2026-05-15 09:30:00` | 开始时间 |
| `end_date` | `2026-05-15 10:00:00` | 结束时间 |
| `limit` | `500` | 返回条数限制 |

实测字段：

```text
ts_code, freq, time, open, high, low, close, vol, amount
```

注意：

- 目录说明该接口读取站内已经录制下来的 A 股 1 分钟本地缓存，再按请求频率聚合。
- 不属于实时 latest 口径。
- 当前返回可能有 `vol=0/amount=0` 的分钟，需要落库前做有效性标记，不要盲目当成可成交流动性。

推荐落库：

```text
E:\Projects\QuantData\indevs_tushare\parquet\a_share_mins\
  freq=1MIN\
    year=YYYY\
      month=MM\
        SYMBOL.parquet
```

字段标准化为：

```text
symbol, datetime, trade_date, minute, open, high, low, close, volume, amount, source, updated_at
```

### 集合竞价回放：`stk_auction_replay`

用途：

- 小市值策略可增加开盘集合竞价过滤
- 研究高开/低开、竞价成交额、竞价换手
- 做开盘买入可成交性判断

路径：

```text
GET /tushare/pro/stk_auction_replay
```

参数：

| 参数 | 示例 | 说明 |
|---|---|---|
| `ts_code` | `000001.SZ` | 股票代码 |
| `trade_date` | `20260327` | 交易日 |
| `mode` | `summary` | `summary` 或 `timeline` |

实测 `summary` 字段包含：

```text
ts_code, trade_date, trade_time, start_time, end_time, freq,
open, high, low, close, price, vol, amount, vwap
```

推荐单独落库，不要混入普通分钟 K：

```text
E:\Projects\QuantData\indevs_tushare\parquet\auction_replay\
  mode=summary\
    year=YYYY\
      month=MM\
        SYMBOL.parquet
```

### 指数和动态股票池

可用接口：

| 接口 | 用途 | 状态 |
|---|---|---|
| `index_daily` | 指数 OHLCV | OK |
| `get_index_stocks` | 聚合指数成分 | OK |
| `index_weight` | Tushare 原始权重 | 当前异常 |

对 ID=43 小市值策略，优先级建议：

1. 平台已有 SQLite `index_components`
2. `get_index_stocks(index_symbol=399101.SZ)` 补成分
3. `index_weight` 暂不作为主路径，除非服务端修复当前 Excel 解析错误

`get_index_stocks` 实测字段：

```text
index_symbol, con_code, con_name, in_date
```

该接口目前更像“当前/累计成员映射”，不一定等价于 point-in-time 权重快照。用于历史回测前，需要确认是否包含 `out_date` 或历史生效区间，否则不能直接替代调仓日成分快照。

### 财务三表

可用接口：

| 接口 | 用途 |
|---|---|
| `income` | 利润表 |
| `balancesheet` | 资产负债表 |
| `cashflow` | 现金流量表 |

适用：

- 补齐 SQLite `financial_data`
- 构建质量、成长、杠杆类因子
- 做财报公告日后的 point-in-time 数据切片

注意：

- 财务数据应同时保存 `ann_date` 和 `end_date`。
- 回测中只能在 `trade_date >= ann_date` 后使用该报告数据。

### 公告：`anns_d`

用途：

- 停复牌、分红、风险提示、业绩预告等事件研究
- 为策略增加公告事件过滤

实测字段：

```text
ts_code, name, title, ann_date, ann_time, url
```

建议按 `ann_date` 分区落 Parquet：

```text
parquet/announcements/year=YYYY/month=MM/part.parquet
```

### 日频估值和复权

可用接口：

| 接口 | 用途 |
|---|---|
| `daily_basic` | 市值、估值、换手率等日频截面 |
| `adj_factor` | 复权因子 |

注意：

- 本次 `daily_basic` 在显式请求 `total_mv/circ_mv/pe_ttm/pb` 时返回空字符串，但此前默认字段调用曾返回完整数值。后续实现时建议优先不传 `fields` 做完整拉取，再在本地裁剪字段。
- `daily_basic` 很适合补小市值排序输入，但要先做字段完整性抽样验证。

## 4. 推荐接入策略

### 数据源优先级

| 数据 | 第一优先级 | 新接口定位 |
|---|---|---|
| 实时行情 | miniQMT | 用户已有实时源，新接口不作为主路径 |
| 日线 | miniQMT / 当前 Parquet store | 可作为缺口补数 |
| 历史分钟 | miniQMT 本地缓存 | `a_share_mins` 可补历史缺口或交叉验证 |
| 固定 timer 分钟点 | 已落库稀疏分钟 | `a_share_mins` 可作为补点来源 |
| 指数成分 | SQLite `index_components` | `get_index_stocks` 辅助，需确认历史语义 |
| 财务三表 | SQLite `financial_data` | 可作为增强补充 |
| 公告事件 | 当前缺少稳定主源 | `anns_d` 可作为新主源 |
| 集合竞价 | 当前缺少稳定主源 | `stk_auction_replay` 可作为新主源 |

### 任务执行方式

这类接口不适合在策略回测过程中同步请求。推荐流程：

1. 先用独立同步脚本按日期/股票池拉取
2. 统一落 Parquet
3. 更新平台 `MarketDataStore` 或新建事件数据 store
4. 回测只读本地 Parquet/DuckDB

### 重试与超时

建议客户端策略：

```text
connect_timeout = 5s
read_timeout = 15-30s
max_retries = 2
backoff = 0.8s, 1.6s
base_urls = [
  http://127.0.0.1:8000/tushare/pro,
  https://ai-tool.indevs.in/tushare/pro,
  https://tushare.indevs.in/tushare/pro
]
```

对 `get_all_securities`、`ths_index` 这类大结果接口，应使用：

- `limit`
- 分页或日期切片
- 后台任务
- 进度日志
- 断点续传状态表

## 5. 最小 smoke test

```python
import os
import requests

API_KEY = os.environ["INDEVS_TUSHARE_API_KEY"]
BASE_URLS = [
    "http://127.0.0.1:8000/tushare/pro",
    "https://ai-tool.indevs.in/tushare/pro",
    "https://tushare.indevs.in/tushare/pro",
]

def get_json(path: str, params: dict) -> dict:
    last_error = None
    for base in BASE_URLS:
        session = requests.Session()
        session.trust_env = False
        try:
            response = session.get(
                f"{base}{path}",
                headers={"X-API-Key": API_KEY, "Accept": "application/json"},
                params=params,
                timeout=(5, 20),
            )
            if response.ok:
                return response.json()
            last_error = f"{base}: HTTP {response.status_code} {response.text[:200]}"
        except requests.RequestException as exc:
            last_error = f"{base}: {exc}"
    raise RuntimeError(last_error)

payload = get_json(
    "/a_share_mins",
    {
        "ts_code": "000001.SZ",
        "freq": "1MIN",
        "start_date": "2026-05-15 09:30:00",
        "end_date": "2026-05-15 10:00:00",
        "limit": 5,
    },
)

fields = payload["data"]["fields"]
rows = [dict(zip(fields, item)) for item in payload["data"]["items"]]
print(rows[:2])
```

## 6. 后续待验证

| 接口 | 待确认问题 |
|---|---|
| `a_share_mins` | 历史覆盖从哪天开始；是否覆盖全 A；停牌/无成交分钟如何表示 |
| `stk_auction_replay` | `timeline` 模式字段和覆盖率 |
| `daily_basic` | 默认字段是否稳定返回市值/估值；显式 fields 为空字符串的原因 |
| `get_index_stocks` | 是否是当前成分、累计成分，还是 point-in-time 成分 |
| `index_weight` | 当前 Excel 解析错误是否为服务端 bug |
| `ths_index` | 是否需要更长超时、分页或更精确参数 |
| `get_all_securities` | 大结果接口是否支持分页 |
