# 趋势资金事件驱动策略 — 实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在GaoshouPlatform中实现趋势资金事件驱动策略，基于自选股生成日频交易信号

**Architecture:** 使用项目现有的DataSkill获取数据，创建独立的策略模块在app/strategies/trend_money/目录下通过API暴露信号

**Tech Stack:** Python 3.12, FastAPI, ClickHouse, SQLite, QMT Gateway

---

## 文件结构

```
backend/app/
├── strategies/
│   └── trend_money/
│       ├── __init__.py
│       ├── detector.py      # 趋势资金计算核心
│       ├── filter.py        # 股票过滤
│       └── signal.py        # 信号生成
├── api/
│   ├── router.py            # 添加策略路由
│   └─��� strategy.py          # 新建策略API
└── services/
    └── __init__.py          # 导出策略服务
```

---

## Task 1: 创建趋势资金检测核心模块

**Files:**
- Create: `backend/app/strategies/trend_money/detector.py`
- Test: `tests/strategies/test_trend_money.py`

- [ ] **Step 1: 创建目录结构**

```python
# backend/app/strategies/__init__.py
from .trend_money import TrendMoneyDetector, TrendMoneySignal

# backend/app/strategies/trend_money/__init__.py
from .detector import TrendMoneyDetector
from .signal import TrendMoneySignal, TrendMoneyResult

__all__ = ["TrendMoneyDetector", "TrendMoneySignal", "TrendMoneyResult"]
```

- [ ] **Step 2: 实现TrendMoneyDetector**

```python
# backend/app/strategies/trend_money/detector.py
"""趋势资金检测器 — 基于分钟K线计算趋势资金强度"""
from __future__ import annotations
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

from app.services.data_skill import DataSkill, KlineBar


@dataclass
class TrendMoneySignal:
    """单只股票的趋势资金信号"""
    symbol: str
    name: str
    trend_strength: float          # 趋势强度 (当日/均值)
    trend_continuity: float        # 连续性 (3日加权)
    momentum: float                # 当日动量 %
    composite_score: float        # 综合信号分
    daily_volume: int             # 当日成交量
    avg_volume_20d: float         # 20日平均成交量
    close_price: float            # 收盘价
    change_pct: float             # 当日涨跌幅


class TrendMoneyDetector:
    """趋势资金检测器"""
    
    def __init__(self, data_skill: DataSkill):
        self.data_skill = data_skill

    async def compute(self, symbols: list[str], trade_date: date | None = None) -> list[TrendMoneySignal]:
        """计算多只股票的趋势资金信号
        
        Args:
            symbols: 股票代码列表
            trade_date: 交易日期，None则默认今日
            
        Returns:
            TrendMoneySignal列表
        """
        if not symbols:
            return []
            
        # 获取每只股票的信号
        results: list[TrendMoneySignal] = []
        for symbol in symbols:
            signal = await self._compute_single(symbol, trade_date)
            if signal:
                results.append(signal)
                
        return results

    async def _compute_single(self, symbol: str, trade_date: date | None = None) -> TrendMoneySignal | None:
        """计算单只股票的趋势资金信号"""
        from datetime import timedelta
        
        if trade_date is None:
            from datetime import date as today
            trade_date = today.today()
            
        # 获取当日分钟K线
        minute_bars = await self.data_skill.get_kline_minute(
            symbol=symbol,
            start_date=trade_date,
            end_date=trade_date,
            limit=500
        )
        
        if not minute_bars:
            return None
            
        # 获取近20日日K线
        daily_bars = await self.data_skill.get_kline_daily(
            symbol=symbol,
            start_date=trade_date - timedelta(days=30),
            end_date=trade_date - timedelta(days=1),
            limit=20
        )
        
        # 计算趋势资金因子
        return self._calculate_trend_signals(symbol, minute_bars, daily_bars)

    def _calculate_trend_signals(
        self, 
        symbol: str, 
        minute_bars: list[KlineBar],
        daily_bars: list[KlineBar]
    ) -> TrendMoneySignal | None:
        """计算趋势资金信号"""
        if not minute_bars or not daily_bars:
            return None
            
        # 当日累计分钟成交量
        daily_volume = sum(bar.volume for bar in minute_bars)
        
        # 20日平均日成交量
        avg_volume_20d = sum(bar.volume for bar in daily_bars) / len(daily_bars) if daily_bars else 0
        
        # 趋势强度
        trend_strength = daily_volume / avg_volume_20d if avg_volume_20d > 0 else 0
        
        # 动量: 当日涨跌幅 (用分钟K线计算)
        first_bar = minute_bars[0]
        last_bar = minute_bars[-1]
        if first_bar.open and first_bar.open > 0:
            momentum = (last_bar.close - first_bar.open) / first_bar.open * 100
        else:
            momentum = 0
            
        # 连续性: 简化版 - 使用趋势强度本身作为连续性参考
        trend_continuity = trend_strength
        
        # 综合信号分
        composite_score = trend_strength * 0.4 + trend_continuity * 0.3 + abs(momentum) * 0.3
        
        return TrendMoneySignal(
            symbol=symbol,
            name="",  # 后面从Stock表填充
            trend_strength=trend_strength,
            trend_continuity=trend_continuity,
            momentum=momentum,
            composite_score=composite_score,
            daily_volume=daily_volume,
            avg_volume_20d=avg_volume_20d,
            close_price=last_bar.close,
            change_pct=momentum
        )
```

- [ ] **Step 3: 创建测试文件验证导入**

```python
# tests/strategies/test_trend_money.py
from datetime import date
from app.strategies.trend_money import TrendMoneyDetector, TrendMoneySignal

def test_import():
    """验证模块导入成功"""
    assert TrendMoneyDetector is not None
    assert TrendMoneySignal is not None
    
def test_signal_dataclass():
    """验证信号数据结构"""
    signal = TrendMoneySignal(
        symbol="600000.SH",
        name="测试",
        trend_strength=2.5,
        trend_continuity=2.0,
        momentum=3.0,
        composite_score=2.5,
        daily_volume=1000000,
        avg_volume_20d=500000,
        close_price=10.0,
        change_pct=3.0
    )
    assert signal.symbol == "600000.SH"
    assert signal.trend_strength == 2.5
```

- [ ] **Step 4: 运行测试验证**

Run: `cd backend && python -m pytest tests/strategies/test_trend_money.py -v`
Expected: PASS

- [ ] **Step 5: 提交**

```bash
git add backend/app/strategies/ tests/strategies/
git commit -m "feat: add TrendMoneyDetector core module"
```

---

## Task 2: 创建股票过滤器模块

**Files:**
- Create: `backend/app/strategies/trend_money/filter.py`
- Modify: `backend/app/strategies/trend_money/detector.py` (添加name填充)

- [ ] **Step 1: 实现过滤器**

```python
# backend/app/strategies/trend_money/filter.py
"""股票过滤器 — ST/停牌/市值/行业过滤"""
from __future__ import annotations
from dataclasses import dataclass
from typing import Any


# 科技行业列表 (申万一级行业)
TECH_INDUSTRIES = {
    "电子", "计算机", "通信", "传媒", "软件服务", "互联网服务",
    "IT设备", "半导体", "元件", "光学光电子", "通信设备",
    "软件开发", "IT服务", "游戏", "影视院线", "出版", "数字媒体"
}


@dataclass
class FilterResult:
    """过滤结果"""
    passed: bool
    reason: str | None = None


class StockFilter:
    """股票过滤器"""
    
    def __init__(
        self,
        min_market_value: float = 10_0000,  # 10亿=100000万元
        allow_st: bool = False,
        allow_suspend: bool = False,
        tech_only: bool = True
    ):
        """初始化过滤器
        
        Args:
            min_market_value: 最小市值(万元)，默认10亿
            allow_st: 是否允许ST股
            allow_suspend: 是否允许停牌股
            tech_only: 是否只允许科技行业
        """
        self.min_market_value = min_market_value
        self.allow_st = allow_st
        self.allow_suspend = allow_suspend
        self.tech_only = tech_only

    def check(self, stock_info: dict[str, Any]) -> FilterResult:
        """检查股票是否通过过滤
        
        Args:
            stock_info: 股票信息字典，包含:
                - is_st: ST状态 (0=正常, 1=ST, 2=*ST)
                - is_suspend: 停牌状态 (0=正常, 1=停牌)
                - total_mv: 总市值(万元)
                - industry: 行业名称
                
        Returns:
            FilterResult
        """
        # 检查ST
        if not self.allow_st and stock_info.get("is_st", 0) > 0:
            return FilterResult(passed=False, reason="ST股票")
            
        # 检查停牌
        if not self.allow_suspend and stock_info.get("is_suspend", 0) > 0:
            return FilterResult(passed=False, reason="停牌股票")
            
        # 检查市值
        total_mv = stock_info.get("total_mv")
        if total_mv is None or total_mv < self.min_market_value:
            return FilterResult(passed=False, reason=f"市值不足{self.min_market_value/10000}亿")
            
        # 检查行业
        if self.tech_only:
            industry = stock_info.get("industry", "")
            if industry not in TECH_INDUSTRIES:
                return FilterResult(passed=False, reason=f"非科技行业({industry})")
                
        return FilterResult(passed=True)

    def filter_batch(
        self, 
        stocks: list[dict[str, Any]]
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """批量过滤股票
        
        Args:
            stocks: 股票信息列表
            
        Returns:
            (通过列表, 拒绝列表)
        """
        passed = []
        rejected = []
        
        for stock in stocks:
            result = self.check(stock)
            if result.passed:
                passed.append(stock)
            else:
                stock["filter_reason"] = result.reason
                rejected.append(stock)
                
        return passed, rejected
```

- [ ] **Step 2: 更新detector.py填充股票名称**

修改 `TrendMoneyDetector._calculate_trend_signals` 返回时添加name字段

```python
# 在 _calculate_trend_signals 结尾处修改 return:
return TrendMoneySignal(
    symbol=symbol,
    name="",  # TODO: 后续从stock表获取
    # ... 其他字段
)
```

- [ ] **Step 3: 提交**

```bash
git add backend/app/strategies/trend_money/filter.py
git commit -m "feat: add StockFilter module"
```

---

## Task 3: 创建信号生成器和服务

**Files:**
- Create: `backend/app/strategies/trend_money/signal.py`
- Create: `backend/app/services/trend_money_service.py`

- [ ] **Step 1: 实现信号生成器**

```python
# backend/app/strategies/trend_money/signal.py
"""信号生成器 — 生成最终交易信号"""
from __future__ import annotations
from dataclasses import dataclass, field
from datetime import date
from typing import Any


@dataclass
class TrendMoneyResult:
    """策略执行结果"""
    date: date
    signal_type: str  # "日频收盘信号"
    stocks: list[dict[str, Any]] = field(default_factory=list)
    total_scanned: int = 0
    passed_filter: int = 0


class SignalGenerator:
    """信号生成器"""
    
    # 阈值配置
    TREND_STRENGTH_BUY = 1.5      # 趋势强度 > 1.5 考虑买入
    MOMENTUM_BUY = 2.0            # 动量 > 2% 看多
    COMPOSITE_SCORE_TOP = 5      # 取Top N
    
    def generate(
        self,
        signals: list[dict[str, Any]],
        top_n: int = 5
    ) -> TrendMoneyResult:
        """生成最终交易信号
        
        Args:
            signals: 趋势资金信号列表
            top_n: 取Top N
            
        Returns:
            TrendMoneyResult
        """
        if not signals:
            return TrendMoneyResult(
                date=date.today(),
                signal_type="日频收盘信号",
                stocks=[]
            )
            
        # 排序
        sorted_signals = sorted(
            signals, 
            key=lambda x: x.get("composite_score", 0), 
            reverse=True
        )
        
        # 生成信号
        result_stocks = []
        for i, s in enumerate(sorted_signals[:top_n]):
            signal_type = self._judge_signal(s)
            result_stocks.append({
                "rank": i + 1,
                "symbol": s["symbol"],
                "name": s.get("name", ""),
                "signal": signal_type,
                "confidence": min(s.get("composite_score", 0) / 3, 1.0),
                "trend_strength": s.get("trend_strength", 0),
                "trend_continuity": s.get("trend_continuity", 0),
                "momentum": s.get("momentum", 0),
                "composite_score": s.get("composite_score", 0),
                "reason": self._generate_reason(s, signal_type)
            })
            
        return TrendMoneyResult(
            date=date.today(),
            signal_type="日频收盘信号",
            stocks=result_stocks,
            total_scanned=len(signals),
            passed_filter=len(result_stocks)
        )
    
    def _judge_signal(self, s: dict[str, Any]) -> str:
        """判断信号类型"""
        trend = s.get("trend_strength", 0)
        momentum = s.get("momentum", 0)
        
        if trend > self.TREND_STRENGTH_BUY and momentum > self.MOMENTUM_BUY:
            return "强势买入"
        elif trend > self.TREND_STRENGTH_BUY and momentum < -self.MOMENTUM_BUY:
            return "预警观察"
        elif trend > 1.2:
            return "待观察"
        else:
            return "无信号"
    
    def _generate_reason(self, s: dict[str, Any], signal_type: str) -> str:
        """生成信号原因描述"""
        trend = s.get("trend_strength", 0)
        momentum = s.get("momentum", 0)
        
        if signal_type == "强势买入":
            return f"趋势强度{trend:.2f}倍，涨跌幅{momentum:.2f}%"
        elif signal_type == "预警观察":
            return f"趋势资金入场但价格下跌，趋势强度{trend:.2f}倍"
        else:
            return "信号不明显"
```

- [ ] **Step 2: 实现服务层**

```python
# backend/app/services/trend_money_service.py
"""趋势资金策略服务"""
from __future__ import annotations
from datetime import date
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import Stock
from app.db.models.watchlist import WatchlistStock
from app.services.data_skill import DataSkill
from app.strategies.trend_money import TrendMoneyDetector
from app.strategies.trend_money.filter import StockFilter
from app.strategies.trend_money.signal import SignalGenerator


class TrendMoneyService:
    """趋势资金策略服务"""
    
    def __init__(self, session: AsyncSession):
        self.session = session
        self.data_skill = DataSkill(session)
        self.detector = TrendMoneyDetector(self.data_skill)
        self.filter = StockFilter(tech_only=True)
        self.signal_gen = SignalGenerator()
    
    async def generate_signals(
        self, 
        group_id: int | None = None,
        trade_date: date | None = None
    ) -> dict[str, Any]:
        """生成趋势资金信号
        
        Args:
            group_id: 自选股分组ID，None则用全部自选股
            trade_date: 交易日期
            
        Returns:
            信号结果字典
        """
        # 1. 获取自选股列表
        symbols = await self._get_watchlist_symbols(group_id)
        if not symbols:
            return {"error": "自选股为空", "stocks": []}
            
        # 2. 获取股票信息用于过滤
        stock_infos = await self._get_stock_infos(symbols)
        
        # 3. 计算趋势资金因子
        trend_signals = await self.detector.compute(symbols, trade_date)
        
        # 4. 转换为dict并填充股票名称和行业
        signal_dicts = []
        for sig in trend_signals:
            info = stock_infos.get(sig.symbol, {})
            signal_dicts.append({
                "symbol": sig.symbol,
                "name": info.get("name", ""),
                "industry": info.get("industry", ""),
                "total_mv": info.get("total_mv", 0),
                "is_st": info.get("is_st", 0),
                "is_suspend": info.get("is_suspend", 0),
                "trend_strength": sig.trend_strength,
                "trend_continuity": sig.trend_continuity,
                "momentum": sig.momentum,
                "composite_score": sig.composite_score,
                "close_price": sig.close_price,
                "change_pct": sig.change_pct
            })
        
        # 5. 过滤
        filtered = []
        for s in signal_dicts:
            if self.filter.check(s).passed:
                filtered.append(s)
                
        # 6. 生成信号
        result = self.signal_gen.generate(filtered)
        
        return {
            "date": result.date.isoformat() if result.date else "",
            "signal_type": result.signal_type,
            "stocks": result.stocks,
            "total_scanned": result.total_scanned,
            "passed_filter": result.passed_filter
        }
    
    async def _get_watchlist_symbols(self, group_id: int | None) -> list[str]:
        """获取自选股代码列表"""
        if group_id:
            stmt = select(WatchlistStock.symbol).where(
                WatchlistStock.group_id == group_id
            )
        else:
            # 获取所有自选股
            stmt = select(WatchlistStock.symbol)
            
        rows = (await self.session.execute(stmt)).scalars().all()
        return list(rows)
    
    async def _get_stock_infos(self, symbols: list[str]) -> dict[str, dict[str, Any]]:
        """获取股票信息用于过滤"""
        if not symbols:
            return {}
            
        stmt = select(Stock).where(Stock.symbol.in_(symbols))
        rows = (await self.session.execute(stmt)).scalars().all()
        
        return {
            s.symbol: {
                "name": s.name,
                "industry": s.industry,
                "total_mv": s.total_mv or 0,
                "is_st": s.is_st or 0,
                "is_suspend": s.is_suspend or 0
            }
            for s in rows
        }
```

- [ ] **Step 3: 提交**

```bash
git add backend/app/strategies/trend_money/signal.py backend/app/services/trend_money_service.py
git commit -m "feat: add TrendMoney signal generator and service"
```

---

## Task 4: 创建API路由

**Files:**
- Create: `backend/app/api/strategy.py`
- Modify: `backend/app/api/router.py`
- Modify: `backend/app/services/__init__.py`

- [ ] **Step 1: 创建API路由**

```python
# backend/app/api/strategy.py
"""策略API — 趋势资金事件驱动策略"""
from __future__ import annotations
from datetime import date

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession
from typing import Any

from app.db.sqlite import get_db_session
from app.services.trend_money_service import TrendMoneyService


router = APIRouter()


@router.get("/trend-money/signals", summary="获取趋势资金信号")
async def get_trend_money_signals(
    group_id: int | None = Query(default=None, description="自选股分组ID"),
    trade_date: str | None = Query(default=None, description="交易日期YYYY-MM-DD"),
    db: AsyncSession = Depends(get_db_session)
) -> dict[str, Any]:
    """获取趋势资金事件驱动信号
    
    基于自选股计算趋势资金因子，返回信号股票列表
    """
    service = TrendMoneyService(db)
    
    # 解析日期
    dt = None
    if trade_date:
        dt = date.fromisoformat(trade_date)
    
    result = await service.generate_signals(group_id, dt)
    return result


@router.get("/trend-money/history", summary="获取历史信号")
async def get_trend_money_history(
    limit: int = Query(default=30, description="返回天数"),
    db: AsyncSession = Depends(get_db_session)
) -> dict[str, Any]:
    """获取历史信号记录（待实现存储后使用）"""
    return {
        "message": "历史信号存储功能待实现",
        "limit": limit
    }
```

- [ ] **Step 2: 注册路由**

```python
# backend/app/api/router.py 添加
from app.api import strategy as strategy_router
# 在 create_router() 中添加
api_router.include_router(strategy_router, prefix="/strategy", tags=["策略"])
```

- [ ] **Step 3: 更新services导出**

```python
# backend/app/services/__init__.py 添加
from .trend_money_service import TrendMoneyService

__all__ = [
    # ... 现有导出
    "TrendMoneyService",
]
```

- [ ] **Step 4: 提交**

```bash
git add backend/app/api/strategy.py backend/app/api/router.py backend/app/services/__init__.py
git commit -m "feat: add TrendMoney strategy API endpoints"
```

---

## Task 5: 前端展示（可选）

**Files:**
- Modify: `frontend/src/api/` 添加策略API
- Modify: 前端页面添加信号展示

此任务可以后续添加，先验证后端功能正常。

---

## 实施检查清单

- [ ] Task 1: TrendMoneyDetector 核心模块
- [ ] Task 2: StockFilter 过滤器
- [ ] Task 3: SignalGenerator + Service
- [ ] Task 4: API 路由
- [ ] Task 5: 测试运行

---

## 预期输出示例

```json
{
  "date": "2026-04-26",
  "signal_type": "日频收盘信号",
  "stocks": [
    {
      "rank": 1,
      "symbol": "688XXX.SH",
      "name": "XXXX科技",
      "signal": "强势买入",
      "confidence": 0.85,
      "trend_strength": 2.3,
      "trend_continuity": 2.0,
      "momentum": 5.2,
      "composite_score": 2.95,
      "reason": "趋势强度2.30倍，涨跌幅5.20%"
    }
  ],
  "total_scanned": 20,
  "passed_filter": 5
}
```

---

*计划版本: 1.0*  
*创建时间: 2026-04-26*