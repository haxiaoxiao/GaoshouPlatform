# Deep Value Strategy Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a deep value strategy (price < 250-week MA by 10%+, dividend yield > 3.5%, PE < 40, 1-year hold) with weekly K-line data pipeline.

**Architecture:** Follow existing patterns — QMT → ClickHouse pipeline for weekly K-lines (modeled after kline_daily sync), strategy class directly queries ClickHouse (like TrendCapitalStrategy), indicators registered for UI/query use.

**Tech Stack:** Python 3.12+, ClickHouse, xtquant, FastAPI

**Spec:** `docs/superpowers/specs/2026-05-04-deep-value-strategy-design.md`

---

### Task 1: Add `klines_weekly` table to ClickHouse init

**Files:**
- Modify: `backend/app/db/clickhouse.py:19-64`

- [ ] **Step 1: Add klines_weekly table creation in init_clickhouse_tables()**

After the klines_minute table creation, add:

```python
    # 创建周K线表
    client.execute("""
        CREATE TABLE IF NOT EXISTS klines_weekly
        (
            symbol LowCardinality(String),
            trade_date Date,
            open Decimal(10, 4),
            high Decimal(10, 4),
            low Decimal(10, 4),
            close Decimal(10, 4),
            volume UInt64,
            amount Decimal(18, 4),
            turnover_rate Decimal(8, 4),
            created_at DateTime DEFAULT now()
        )
        ENGINE = MergeTree()
        PARTITION BY toYYYYMM(trade_date)
        ORDER BY (symbol, trade_date)
    """)
```

- [ ] **Step 2: Run syntax check**

```bash
cd backend && python -c "import ast; ast.parse(open('app/db/clickhouse.py').read()); print('OK')"
```

- [ ] **Step 3: Commit**

```bash
git add backend/app/db/clickhouse.py
git commit -m "feat: add klines_weekly table to ClickHouse init"
```

---

### Task 2: Add `get_kline_weekly` method to QMT gateway

**Files:**
- Modify: `backend/app/engines/qmt_gateway.py` — add method after `get_kline_minute()` (~line 739)

- [ ] **Step 1: Add get_kline_weekly method**

After the `get_kline_minute` method, add:

```python
    async def get_kline_weekly(
        self,
        symbol: str,
        start_date: date,
        end_date: date,
    ) -> list[KlineData]:
        """获取周K线数据"""
        xt = self._get_xt()

        start_str = start_date.strftime("%Y%m%d")
        end_str = end_date.strftime("%Y%m%d")

        loop = asyncio.get_running_loop()

        try:
            await loop.run_in_executor(
                None,
                lambda: xt.download_history_data(symbol, period="1w", start_time=start_str, end_time=end_str),
            )
        except Exception:
            pass

        data = await loop.run_in_executor(
            None,
            lambda: xt.get_market_data_ex(
                field_list=[],
                stock_list=[symbol],
                period="1w",
                start_time=start_str,
                end_time=end_str,
            ),
        )

        results = []
        if symbol in data:
            df = data[symbol]
            for idx, row in df.iterrows():
                try:
                    if isinstance(idx, str):
                        trade_date = datetime.strptime(idx, "%Y%m%d").date()
                    elif hasattr(idx, "date"):
                        trade_date = idx.date()
                    else:
                        trade_date = idx

                    kline = KlineData(
                        symbol=symbol,
                        datetime=trade_date,
                        open=float(row.get("open", 0)),
                        high=float(row.get("high", 0)),
                        low=float(row.get("low", 0)),
                        close=float(row.get("close", 0)),
                        volume=int(row.get("volume", 0)),
                        amount=float(row.get("amount", 0)),
                        turnover=float(row.get("turnover", 0)) if row.get("turnover") else None,
                    )
                    results.append(kline)
                except Exception:
                    continue

        return results
```

- [ ] **Step 2: Run syntax check**

```bash
cd backend && python -c "import ast; ast.parse(open('app/engines/qmt_gateway.py').read()); print('OK')"
```

- [ ] **Step 3: Commit**

```bash
git add backend/app/engines/qmt_gateway.py
git commit -m "feat: add get_kline_weekly method to QMT gateway"
```

---

### Task 3: Add `sync_kline_weekly` to sync service

**Files:**
- Modify: `backend/app/services/sync_service.py` — add method after `sync_kline_minute` (near line 1083)
- Modify: `backend/app/api/data.py:586-593` — add kline_weekly dispatch
- Modify: `backend/app/api/data.py:97` — add to SyncRequest description
- Modify: `backend/app/api/data.py:618` — add to valid_types

- [ ] **Step 1: Add sync_kline_weekly method to SyncService**

After the `sync_kline_minute` method, add:

```python
    async def sync_kline_weekly(
        self,
        symbols: list[str] | None = None,
        start_date: date | None = None,
        end_date: date | None = None,
        task_id: int | None = None,
        failure_strategy: str = "skip",
        full_sync: bool = False,
    ) -> SyncProgress:
        """同步周K线数据"""
        global _current_sync

        if end_date is None:
            end_date = date.today()
        if start_date is None:
            start_date = end_date - timedelta(days=365 * 7)  # ~7 years for 250-week MA

        progress = SyncProgress(
            sync_type="kline_weekly",
            status="running",
            start_time=datetime.now(),
            details={
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
                "full_sync": full_sync,
            },
        )
        _current_sync = progress

        ch_client = get_ch_client()

        try:
            if symbols is None:
                query = select(Stock.symbol)
                result = await self.session.execute(query)
                symbols = [row[0] for row in result.all()]

            progress.total = len(symbols)

            if full_sync and symbols:
                progress.details["message"] = "正在删除已有数据..."
                for symbol in symbols:
                    try:
                        ch_client.execute(
                            "DELETE FROM klines_weekly WHERE symbol = %(symbol)s "
                            "AND trade_date >= %(start_date)s AND trade_date <= %(end_date)s",
                            {"symbol": symbol, "start_date": start_date, "end_date": end_date},
                        )
                    except Exception:
                        pass

            failed_symbols: list[dict[str, str]] = []
            total_klines = 0

            for i, symbol in enumerate(symbols):
                try:
                    klines = await qmt_gateway.get_kline_weekly(
                        symbol, start_date, end_date
                    )

                    if not klines:
                        progress.current = i + 1
                        continue

                    rows = [
                        {
                            "symbol": kline.symbol,
                            "trade_date": kline.datetime if isinstance(kline.datetime, date) else date.fromisoformat(str(kline.datetime)),
                            "open": kline.open,
                            "high": kline.high,
                            "low": kline.low,
                            "close": kline.close,
                            "volume": kline.volume,
                            "amount": kline.amount,
                        }
                        for kline in klines
                    ]

                    if rows:
                        ch_client.execute(
                            "INSERT INTO klines_weekly "
                            "(symbol, trade_date, open, high, low, close, volume, amount) "
                            "VALUES",
                            rows,
                        )
                        total_klines += len(rows)

                    progress.current = i + 1
                    progress.success_count += 1

                except Exception as e:
                    progress.failed_count += 1
                    failed_symbols.append({"symbol": symbol, "error": str(e)})
                    if failure_strategy == "stop":
                        raise

            try:
                cleaned = qmt_gateway.clean_local_cache(symbols=symbols, data_type="kline")
                progress.details["cache_cleaned"] = cleaned
            except Exception:
                pass

            indicator_scheduler.run_after_sync("kline_weekly", symbols=symbols, trade_date=end_date)
            progress.status = "completed"
            progress.end_time = datetime.now()
            progress.details["total_klines"] = total_klines
            progress.details["failed_symbols"] = failed_symbols[:100]

            await self.create_sync_log(
                sync_type="kline_weekly",
                status="completed",
                total_count=progress.total,
                success_count=progress.success_count,
                failed_count=progress.failed_count,
                start_time=progress.start_time,
                end_time=progress.end_time,
                details=progress.details,
                task_id=task_id,
            )
            await self.session.commit()

        except Exception as e:
            progress.status = "failed"
            progress.end_time = datetime.now()
            progress.error_message = str(e)
            progress.details["error"] = str(e)[:500]
            progress.details["error_type"] = type(e).__name__
            await self.create_sync_log(
                sync_type="kline_weekly",
                status="failed",
                total_count=progress.total,
                success_count=progress.success_count,
                failed_count=progress.failed_count,
                start_time=progress.start_time,
                end_time=progress.end_time,
                error_message=str(e),
                details=progress.details,
                task_id=task_id,
            )
            await self.session.commit()
            raise

        finally:
            _current_sync = None
```

- [ ] **Step 2: Add kline_weekly dispatch in data.py API**

Modify the `elif` chain in `_run_sync_task` (~line 586):

```python
            elif sync_type == "kline_weekly":
                await service.sync_kline_weekly(
                    symbols=symbols,
                    start_date=start_date,
                    end_date=end_date,
                    failure_strategy=failure_strategy,
                    full_sync=full_sync,
                )
```

Insert this BEFORE the `else:  # kline_minute` branch, and change the `else` to `elif sync_type == "kline_minute":`.

Update the valid_types tuple:

```python
    valid_types = ("stock_info", "stock_full", "financial_data", "kline_daily", "kline_minute", "kline_weekly", "realtime_mv", "dividends")
```

Update SyncRequest.sync_type description:

```python
    sync_type: str = Field(description="同步类型: stock_info/stock_full/financial_data/kline_daily/kline_minute/kline_weekly/realtime_mv/dividends")
```

Update the docstring for trigger_sync to include kline_weekly.

- [ ] **Step 3: Run syntax check**

```bash
cd backend && python -c "import ast; ast.parse(open('app/services/sync_service.py').read()); print('OK')" && python -c "import ast; ast.parse(open('app/api/data.py').read()); print('OK')"
```

- [ ] **Step 4: Commit**

```bash
git add backend/app/services/sync_service.py backend/app/api/data.py
git commit -m "feat: add kline_weekly sync pipeline"
```

---

### Task 4: Fix `dividend_yield` computation in sync_dividends

**Files:**
- Modify: `backend/app/services/sync_service.py:1701-1748`

**Problem:**
1. `symbols_with_data[:1000]` limits to 1000 stocks
2. Each stock does individual DELETE + INSERT (N queries)
3. Only stores `date.today()` — not usable for historical backtest

**Fix:** Remove the 1000 limit, batch the INSERT, and compute yield at each dividend ex-date (not just today).

- [ ] **Step 1: Replace the yield computation section (lines 1701-1748)**

Replace the entire "阶段4" block:

```python
            # ========== 阶段4: 计算股息率 ==========
            progress.details["phase"] = "yield"
            progress.details["message"] = "正在计算股息率..."

            yield_rows = []
            for symbol in symbols_with_data:
                try:
                    result = ch_client.execute(
                        """SELECT trade_date, value FROM stock_indicators
                        WHERE symbol = %(symbol)s AND indicator_name = 'dividend_cash'
                        ORDER BY trade_date DESC""",
                        {"symbol": symbol}
                    )
                    if not result:
                        continue

                    price_rows = ch_client.execute(
                        """SELECT trade_date, close FROM klines_daily
                        WHERE symbol = %(symbol)s
                        ORDER BY trade_date DESC""",
                        {"symbol": symbol}
                    )
                    price_map = {str(r[0]): float(r[1]) for r in price_rows if r[1]}

                    for ex_date, cash in result:
                        cash = float(cash or 0)
                        if cash <= 0:
                            continue
                        ex_date_str = str(ex_date)
                        if ex_date_str not in price_map:
                            continue
                        price = price_map[ex_date_str]
                        if price <= 0:
                            continue
                        # Sum trailing 12-month dividends from this ex_date
                        one_year_ago = (ex_date - td(days=365)).isoformat()
                        total_cash = sum(
                            float(r[1] or 0) for r in result
                            if str(r[0]) >= one_year_ago and str(r[0]) <= ex_date_str
                        )
                        if total_cash <= 0:
                            continue
                        div_yield = (total_cash / price) * 100
                        yield_rows.append({
                            "symbol": symbol,
                            "indicator_name": "dividend_yield",
                            "trade_date": ex_date,
                            "value": round(div_yield, 4),
                        })
                except Exception:
                    continue

            if yield_rows:
                ch_client.execute(
                    "DELETE FROM stock_indicators WHERE indicator_name = 'dividend_yield'"
                )
                batch_size = 500
                for i in range(0, len(yield_rows), batch_size):
                    ch_client.execute(
                        "INSERT INTO stock_indicators (symbol, indicator_name, trade_date, value) VALUES",
                        yield_rows[i:i + batch_size]
                    )
```

- [ ] **Step 2: Run syntax check**

```bash
cd backend && python -c "import ast; ast.parse(open('app/services/sync_service.py').read()); print('OK')"
```

- [ ] **Step 3: Commit**

```bash
git add backend/app/services/sync_service.py
git commit -m "fix: remove 1000-stock limit and compute historical dividend_yield"
```

---

### Task 5: Add `ma250_weekly` indicator

**Files:**
- Modify: `backend/app/indicators/technical.py` — add at end of file

- [ ] **Step 1: Add MA250Weekly indicator class**

Append to `backend/app/indicators/technical.py`:

```python
@IndicatorRegistry.register
class MA250Weekly(IndicatorBase):
    name = "ma250_weekly"
    display_name = "250周均线"
    category = "technical"
    tags = ["技术", "行情"]
    data_type = "时序"
    is_precomputed = False
    dependencies = []
    description = "250周移动平均线"
    unit = "CNY"

    def compute(self, context: IndicatorContext) -> float | None:
        from app.db.clickhouse import get_ch_client
        ch = get_ch_client()
        try:
            result = ch.execute(
                """SELECT avg(close) FROM (
                    SELECT close FROM klines_weekly
                    WHERE symbol = %(symbol)s
                    ORDER BY trade_date DESC
                    LIMIT 250
                )""",
                {"symbol": context.symbol}
            )
            if result and result[0] and result[0][0]:
                return round(float(result[0][0]), 4)
        except Exception:
            pass
        return None
```

- [ ] **Step 2: Run syntax check**

```bash
cd backend && python -c "import ast; ast.parse(open('app/indicators/technical.py').read()); print('OK')"
```

- [ ] **Step 3: Commit**

```bash
git add backend/app/indicators/technical.py
git commit -m "feat: add ma250_weekly indicator"
```

---

### Task 6: Add `price_to_ma250w` indicator

**Files:**
- Modify: `backend/app/indicators/valuation.py` — add before DividendYield class

- [ ] **Step 1: Add PriceToMA250W indicator class**

Insert before the `DividendYield` class in `backend/app/indicators/valuation.py`:

```python
@IndicatorRegistry.register
class PriceToMA250W(IndicatorBase):
    name = "price_to_ma250w"
    display_name = "价格/250周线"
    category = "valuation"
    tags = ["估值", "行情"]
    data_type = "截面"
    is_precomputed = False
    dependencies = ["ma250_weekly"]
    description = "最新收盘价 / 250周均线，< 1 表示低于周线"
    unit = "x"

    def compute(self, context: IndicatorContext) -> float | None:
        from app.db.clickhouse import get_ch_client
        ch = get_ch_client()
        try:
            close_result = ch.execute(
                """SELECT close FROM klines_daily
                WHERE symbol = %(symbol)s
                ORDER BY trade_date DESC LIMIT 1""",
                {"symbol": context.symbol}
            )
            if not close_result or not close_result[0][0]:
                return None
            close = float(close_result[0][0])

            ma_result = ch.execute(
                """SELECT avg(close) FROM (
                    SELECT close FROM klines_weekly
                    WHERE symbol = %(symbol)s
                    ORDER BY trade_date DESC
                    LIMIT 250
                )""",
                {"symbol": context.symbol}
            )
            if not ma_result or not ma_result[0] or not ma_result[0][0]:
                return None
            ma250 = float(ma_result[0][0])
            if ma250 <= 0:
                return None
            return round(close / ma250, 4)
        except Exception:
            pass
        return None
```

- [ ] **Step 2: Run syntax check**

```bash
cd backend && python -c "import ast; ast.parse(open('app/indicators/valuation.py').read()); print('OK')"
```

- [ ] **Step 3: Commit**

```bash
git add backend/app/indicators/valuation.py
git commit -m "feat: add price_to_ma250w indicator"
```

---

### Task 7: Build `DeepValueStrategy` class

**Files:**
- Create: `backend/app/strategies/deep_value.py`

The strategy directly queries ClickHouse (like TrendCapitalStrategy), computing conditions on the fly rather than relying on pre-computed stock_indicators.

- [ ] **Step 1: Create the strategy file**

```python
"""深度价值策略 — 低估值 + 高股息 + 深度折价

入场条件:
  - 股价 < 250周均线 × 0.9  (低于周线10%+)
  - 股息率 > 3.5%
  - 0 < PE < 40
持仓: 等权, 持有52周, 每13周调仓
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any

import numpy as np

from app.db.clickhouse import get_ch_client


@dataclass
class Position:
    symbol: str
    entry_date: date
    entry_price: float
    shares: int = 0


class DeepValueStrategy:
    """深度价值策略"""

    PRICE_TO_MA_MAX = 0.9
    DIVIDEND_YIELD_MIN = 3.5
    PE_MIN = 0
    PE_MAX = 40
    HOLD_WEEKS = 52
    REBALANCE_EVERY_WEEKS = 13
    MAX_POSITIONS = 10

    def __init__(self, symbols: list[str] | None = None):
        self.ch = get_ch_client()
        self.symbols = symbols

    def _get_all_symbols(self) -> list[str]:
        if self.symbols:
            return self.symbols
        from sqlalchemy import create_engine, select
        from app.db.models.stock import Stock
        from app.core.config import settings
        url = settings.database_url.replace("+aiosqlite", "")
        engine = create_engine(url)
        with engine.connect() as conn:
            rows = conn.execute(select(Stock.symbol).where(
                Stock.is_st == 0, Stock.is_delist == 0
            )).all()
        engine.dispose()
        return [r[0] for r in rows]

    def screen_candidates(self, as_of_date: date) -> list[dict]:
        """筛选满足条件的候选标的，按 score 排序"""
        symbols = self._get_all_symbols()

        candidates = []
        for sym in symbols:
            r = self._eval_symbol(sym, as_of_date)
            if r:
                candidates.append(r)

        candidates.sort(key=lambda x: -x["score"])
        return candidates[:self.MAX_POSITIONS]

    def _eval_symbol(self, symbol: str, as_of_date: date) -> dict | None:
        """评估单只股票是否满足入场条件"""
        try:
            # ── 250周均线 ──
            ma_row = self.ch.execute(
                """SELECT avg(close) FROM (
                    SELECT close FROM klines_weekly
                    WHERE symbol = %(sym)s AND trade_date <= %(dt)s
                    ORDER BY trade_date DESC LIMIT 250
                )""",
                {"sym": symbol, "dt": as_of_date}
            )
            if not ma_row or not ma_row[0] or not ma_row[0][0]:
                return None
            ma250 = float(ma_row[0][0])

            # ── 最新收盘价 ──
            close_row = self.ch.execute(
                """SELECT close FROM klines_daily
                WHERE symbol = %(sym)s AND trade_date <= %(dt)s
                ORDER BY trade_date DESC LIMIT 1""",
                {"sym": symbol, "dt": as_of_date}
            )
            if not close_row or not close_row[0][0]:
                return None
            close = float(close_row[0][0])

            # 条件1: 股价 < 250周线 × 0.9
            price_to_ma = close / ma250
            if price_to_ma >= self.PRICE_TO_MA_MAX:
                return None

            # ── 股息率 (近12个月累计分红/当前股价) ──
            div_row = self.ch.execute(
                """SELECT sum(value) FROM stock_indicators
                WHERE symbol = %(sym)s AND indicator_name = 'dividend_cash'
                AND trade_date >= %(start)s AND trade_date <= %(dt)s""",
                {"sym": symbol, "start": as_of_date - timedelta(days=365), "dt": as_of_date}
            )
            total_cash = float(div_row[0][0] or 0) if div_row and div_row[0] else 0
            dividend_yield = (total_cash / close) * 100 if close > 0 else 0

            # 条件2: 股息率 > 3.5%
            if dividend_yield <= self.DIVIDEND_YIELD_MIN:
                return None

            # ── PE (从SQLite stocks表获取最新值) ──
            pe_row = self.ch.execute(
                """SELECT value FROM stock_indicators
                WHERE symbol = %(sym)s AND indicator_name = 'pe_ttm'
                ORDER BY trade_date DESC LIMIT 1""",
                {"sym": symbol}
            )
            pe = float(pe_row[0][0]) if pe_row and pe_row[0] and pe_row[0][0] else None

            # 条件3: 0 < PE < 40
            if pe is None or pe <= self.PE_MIN or pe >= self.PE_MAX:
                return None

            # ── 综合评分 (折价越多 + 股息越高 = 分越高) ──
            score = (1 - price_to_ma) * 50 + dividend_yield

            return {
                "symbol": symbol,
                "close": close,
                "ma250_weekly": round(ma250, 2),
                "price_to_ma": round(price_to_ma, 4),
                "dividend_yield": round(dividend_yield, 4),
                "pe_ttm": round(pe, 4) if pe else None,
                "score": round(score, 4),
            }
        except Exception:
            return None

    def run(
        self,
        start_date: date,
        end_date: date,
        initial_capital: float = 1_000_000,
    ) -> dict[str, Any]:
        """运行回测"""
        # 获取所有周三作为调仓日 (周K线以周三为锚)
        trading_weeks = self._get_weekly_dates(start_date, end_date)

        positions: dict[str, Position] = {}
        cash = initial_capital
        trades: list[dict] = []
        nav_series: list[dict] = []

        rebalance_counter = 0
        for i, week_date in enumerate(trading_weeks):
            # ── 检查持仓到期 ──
            expired = []
            for sym, pos in positions.items():
                held_weeks = (week_date - pos.entry_date).days / 7
                if held_weeks >= self.HOLD_WEEKS:
                    expired.append(sym)

            for sym in expired:
                exit_price = self._get_price(sym, week_date)
                if exit_price:
                    pnl = (exit_price / positions[sym].entry_price - 1) * 100
                    cash += positions[sym].shares * exit_price
                    trades.append({
                        "symbol": sym, "direction": "sell",
                        "entry_date": positions[sym].entry_date.isoformat(),
                        "exit_date": week_date.isoformat(),
                        "entry_price": positions[sym].entry_price,
                        "exit_price": exit_price,
                        "pnl_pct": round(pnl, 2),
                    })
                del positions[sym]

            # ── 调仓检查 ──
            rebalance_counter += 1
            if rebalance_counter < self.REBALANCE_EVERY_WEEKS or len(positions) >= self.MAX_POSITIONS:
                nav_series.append(self._record_nav(week_date, positions, cash))
                continue
            rebalance_counter = 0

            # ── 筛选候选 & 建仓 ──
            candidates = self.screen_candidates(week_date)
            slots = self.MAX_POSITIONS - len(positions)

            for c in candidates[:slots]:
                sym = c["symbol"]
                if sym in positions:
                    continue
                price = c["close"]
                invest = initial_capital * 0.10  # 单票10%
                shares = int(invest / price / 100) * 100  # 整手
                if shares < 100 or shares * price > cash:
                    continue
                cash -= shares * price
                positions[sym] = Position(sym, week_date, price, shares)
                trades.append({
                    "symbol": sym, "direction": "buy",
                    "entry_date": week_date.isoformat(),
                    "entry_price": price,
                    "price_to_ma": c["price_to_ma"],
                    "dividend_yield": c["dividend_yield"],
                    "pe_ttm": c["pe_ttm"],
                })

            nav_series.append(self._record_nav(week_date, positions, cash))

        # ── 强制平仓 ──
        final_date = trading_weeks[-1] if trading_weeks else end_date
        for sym, pos in list(positions.items()):
            exit_price = self._get_price(sym, final_date) or pos.entry_price
            pnl = (exit_price / pos.entry_price - 1) * 100
            cash += pos.shares * exit_price
            trades.append({
                "symbol": sym, "direction": "sell",
                "entry_date": pos.entry_date.isoformat(),
                "exit_date": final_date.isoformat(),
                "entry_price": pos.entry_price,
                "exit_price": exit_price,
                "pnl_pct": round(pnl, 2),
            })

        # ── 统计 ──
        final_value = cash
        if nav_series:
            final_value = nav_series[-1]["total_value"]

        pnls = [t["pnl_pct"] for t in trades if t["direction"] == "sell" and t["pnl_pct"] is not None]
        winning = [p for p in pnls if p > 0]

        return {
            "total_return": round((final_value / initial_capital - 1) * 100, 2),
            "initial_capital": initial_capital,
            "final_capital": round(final_value, 2),
            "total_trades": len(trades),
            "win_count": len(winning),
            "loss_count": len(pnls) - len(winning),
            "win_rate": round(len(winning) / len(pnls) * 100, 2) if pnls else 0,
            "avg_return": round(float(np.mean(pnls)), 2) if pnls else 0,
            "total_return_sum": round(float(np.sum(pnls)), 2) if pnls else 0,
            "trades": trades[-100:],
            "nav_series": nav_series,
        }

    def _get_price(self, symbol: str, as_of_date: date) -> float | None:
        r = self.ch.execute(
            """SELECT close FROM klines_daily
            WHERE symbol = %(sym)s AND trade_date <= %(dt)s
            ORDER BY trade_date DESC LIMIT 1""",
            {"sym": symbol, "dt": as_of_date}
        )
        return float(r[0][0]) if r and r[0] and r[0][0] else None

    def _get_weekly_dates(self, start: date, end: date) -> list[date]:
        rows = self.ch.execute(
            """SELECT DISTINCT trade_date FROM klines_weekly
            WHERE trade_date >= %(start)s AND trade_date <= %(end)s
            ORDER BY trade_date""",
            {"start": start, "end": end}
        )
        return [r[0] for r in rows]

    def _record_nav(self, d: date, positions: dict, cash: float) -> dict:
        equity = cash
        for pos in positions.values():
            price = self._get_price(pos.symbol, d)
            if price:
                equity += pos.shares * price
        return {"date": d.isoformat(), "total_value": round(equity, 2)}
```

- [ ] **Step 2: Run syntax check**

```bash
cd backend && python -c "import ast; ast.parse(open('app/strategies/deep_value.py').read()); print('OK')"
```

- [ ] **Step 3: Commit**

```bash
git add backend/app/strategies/deep_value.py
git commit -m "feat: add DeepValueStrategy — low PB, high dividend, deep discount"
```

---

### Task 8: Wire up strategy API endpoint for backtest

**Files:**
- Modify: `backend/app/api/strategy.py` — file already imports `asyncio`, uses `asyncio.to_thread()` pattern. Add endpoint after existing `/backtest` route (~line 212).

- [ ] **Step 1: Add deep value backtest endpoint**

Add after the existing `run_backtest` endpoint (after line 212):

```python

class DeepValueBacktestRequest(BaseModel):
    start_date: date = "2015-01-01"
    end_date: date = "2025-12-31"
    initial_capital: float = 1_000_000
    symbols: list[str] | None = None


@router.post("/deep-value/backtest", summary="深度价值策略回测")
async def run_deep_value_backtest(
    req: DeepValueBacktestRequest,
):
    """深度价值策略回测 — 低估值+高股息+深度折价"""
    from app.strategies.deep_value import DeepValueStrategy
    strategy = DeepValueStrategy(symbols=req.symbols)
    result = await asyncio.to_thread(
        strategy.run, req.start_date, req.end_date, req.initial_capital
    )
    return {"code": 0, "message": "success", "data": result}
```

- [ ] **Step 2: Run syntax check**

```bash
cd backend && python -c "import ast; ast.parse(open('app/api/strategy.py').read()); print('OK')"
```

- [ ] **Step 3: Commit**

```bash
git add backend/app/api/strategy.py
git commit -m "feat: add deep value strategy backtest API endpoint"
```
