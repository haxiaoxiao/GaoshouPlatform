# TSMF 科技小市值生产化运行说明

本文记录 `entry_filter_relaxed_risk` 与 `us_entry_filter_combined` 两个生产候选在 dev 环境内的端到端入口。

## 数据

- A 股行情与分钟 timer：走现有 `kline_daily`、`kline_minute`、`factor_dependency` 同步链路。
- 因子预计算：至少准备 `small_cap_v4_core`、`cn_paper_implemented`、`cn_paper_style_rotation`。
- 美股隔夜过滤：使用 `sync_type="us_market"` 同步 QQQ、SMH、SOXX、NVDA 到 `E:\Projects\Data\external\us_market\us_market_daily.csv`。

示例请求：

```json
{
  "sync_type": "us_market",
  "sync_mode": "incremental",
  "start_date": null,
  "end_date": "2026-06-13"
}
```

## 策略变体

- `us_entry_filter_combined`：QQQ/SMH/SOXX/NVDA 前一美股交易日触发负冲击时，只阻止次日新买和加仓。
- `entry_filter_relaxed_risk`：当前默认最优候选，在 combined 入场过滤基础上放松风控到 `stop_loss_pct=0.10`、`trailing_stop_pct=0.16`、`portfolio_drawdown_stop_pct=0.20`、`high_volume_risk_max=0.95`。

两个变体均通过策略记录 `parameters` 管理，前端回测页提供“策略参数 JSON”编辑区。

## 回测与实盘

- 回测入口：前端 `/backtest`，内置策略会自动创建两个 TSMF 变体。
- 实盘/信号入口：前端 `/live` 的“科技小市值组合执行”面板。
- API 入口：
  - `GET /api/grid-trading/tech-small-cap/variants`
  - `POST /api/grid-trading/tech-small-cap/signals`
  - `POST /api/grid-trading/tech-small-cap/orders/submit`
- 真实下单必须设置 `GRID_TRADING_ENABLE_ORDER_SUBMIT=true`，且提交接口需要 `confirm=true`。
