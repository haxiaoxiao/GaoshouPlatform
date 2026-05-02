# backend/app/services/backtest_service.py
"""回测服务"""
from datetime import date
from decimal import Decimal
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import Backtest, Strategy
from app.engines.vn_engine import BacktestConfig, vn_engine


class BacktestService:
    """回测服务"""

    def __init__(self, session: AsyncSession):
        self.session = session

    # ==================== Strategy CRUD ====================

    async def create_strategy(
        self,
        name: str,
        code: str,
        parameters: dict[str, Any] | None = None,
        description: str | None = None,
    ) -> Strategy:
        """
        创建策略

        Args:
            name: 策略名称
            code: 策略代码
            parameters: 策略参数
            description: 策略描述

        Returns:
            Strategy: 新创建的策略
        """
        strategy = Strategy(
            name=name,
            code=code,
            parameters=parameters,
            description=description,
        )
        self.session.add(strategy)
        await self.session.flush()
        return strategy

    async def get_strategies(
        self,
        page: int = 1,
        page_size: int = 20,
    ) -> tuple[list[Strategy], int]:
        """
        获取策略列表(分页)

        Args:
            page: 页码(从1开始)
            page_size: 每页数量

        Returns:
            tuple[list[Strategy], int]: 策略列表和总数
        """
        # 统计总数
        count_query = select(func.count()).select_from(Strategy)
        total_result = await self.session.execute(count_query)
        total = total_result.scalar() or 0

        # 分页查询
        offset = (page - 1) * page_size
        query = select(Strategy).order_by(Strategy.id.desc()).offset(offset).limit(page_size)
        result = await self.session.execute(query)
        strategies = list(result.scalars().all())

        return strategies, total

    async def get_strategy(self, strategy_id: int) -> Strategy | None:
        """
        获取单个策略

        Args:
            strategy_id: 策略ID

        Returns:
            Strategy | None: 策略对象,不存在返回None
        """
        query = select(Strategy).where(Strategy.id == strategy_id)
        result = await self.session.execute(query)
        return result.scalar_one_or_none()

    async def update_strategy(
        self,
        strategy_id: int,
        name: str | None = None,
        code: str | None = None,
        parameters: dict[str, Any] | None = None,
        description: str | None = None,
    ) -> Strategy | None:
        """
        更新策略

        Args:
            strategy_id: 策略ID
            name: 新名称
            code: 新代码
            parameters: 新参数
            description: 新描述

        Returns:
            Strategy | None: 更新后的策略,不存在返回None
        """
        strategy = await self.get_strategy(strategy_id)
        if strategy is None:
            return None

        if name is not None:
            strategy.name = name
        if code is not None:
            strategy.code = code
        if parameters is not None:
            strategy.parameters = parameters
        if description is not None:
            strategy.description = description

        await self.session.flush()
        return strategy

    async def delete_strategy(self, strategy_id: int) -> bool:
        """
        删除策略

        Args:
            strategy_id: 策略ID

        Returns:
            bool: 是否成功删除
        """
        strategy = await self.get_strategy(strategy_id)
        if strategy is None:
            return False

        await self.session.delete(strategy)
        await self.session.flush()
        return True

    # ==================== Backtest Management ====================

    async def create_backtest(
        self,
        strategy_id: int,
        start_date: date,
        end_date: date,
        initial_capital: Decimal | float,
        parameters: dict[str, Any] | None = None,
    ) -> Backtest:
        """
        创建回测任务

        Args:
            strategy_id: 策略ID
            start_date: 回测起始日期
            end_date: 回测结束日期
            initial_capital: 初始资金
            parameters: 回测参数

        Returns:
            Backtest: 新创建的回测任务
        """
        # 转换为 Decimal
        if isinstance(initial_capital, float):
            initial_capital = Decimal(str(initial_capital))

        backtest = Backtest(
            strategy_id=strategy_id,
            start_date=start_date,
            end_date=end_date,
            initial_capital=initial_capital,
            parameters=parameters,
            status="pending",
        )
        self.session.add(backtest)
        await self.session.flush()
        return backtest

    async def get_backtests(
        self,
        strategy_id: int | None = None,
        status: str | None = None,
        page: int = 1,
        page_size: int = 20,
    ) -> tuple[list[Backtest], int]:
        """
        获取回测列表(分页)

        Args:
            strategy_id: 策略ID筛选
            status: 状态筛选
            page: 页码(从1开始)
            page_size: 每页数量

        Returns:
            tuple[list[Backtest], int]: 回测列表和总数
        """
        # 构建查询
        query = select(Backtest)

        if strategy_id is not None:
            query = query.where(Backtest.strategy_id == strategy_id)
        if status is not None:
            query = query.where(Backtest.status == status)

        # 统计总数
        count_query = select(func.count()).select_from(query.subquery())
        total_result = await self.session.execute(count_query)
        total = total_result.scalar() or 0

        # 分页查询
        offset = (page - 1) * page_size
        query = query.order_by(Backtest.id.desc()).offset(offset).limit(page_size)
        result = await self.session.execute(query)
        backtests = list(result.scalars().all())

        return backtests, total

    async def get_backtest(self, backtest_id: int) -> Backtest | None:
        """
        获取单个回测任务

        Args:
            backtest_id: 回测ID

        Returns:
            Backtest | None: 回测对象,不存在返回None
        """
        query = select(Backtest).where(Backtest.id == backtest_id)
        result = await self.session.execute(query)
        return result.scalar_one_or_none()

    async def delete_backtest(self, backtest_id: int) -> bool:
        """删除单个回测记录"""
        backtest = await self.get_backtest(backtest_id)
        if backtest is None:
            return False
        await self.session.delete(backtest)
        await self.session.flush()
        return True

    async def delete_backtests_batch(self, ids: list[int]) -> int:
        """批量删除回测记录，返回删除数量"""
        query = select(Backtest).where(Backtest.id.in_(ids))
        result = await self.session.execute(query)
        backtests = list(result.scalars().all())
        for b in backtests:
            await self.session.delete(b)
        await self.session.flush()
        return len(backtests)

    async def run_backtest(self, backtest_id: int) -> dict[str, Any]:
        """
        运行回测

        Args:
            backtest_id: 回测ID

        Returns:
            dict[str, Any]: 回测结果
        """
        backtest = await self.get_backtest(backtest_id)
        if backtest is None:
            return {"success": False, "error": "Backtest not found"}

        # 获取关联的策略
        strategy = await self.get_strategy(backtest.strategy_id)
        if strategy is None:
            return {"success": False, "error": "Strategy not found"}

        try:
            # 更新状态为运行中
            backtest.status = "running"
            await self.session.flush()

            # 构建回测配置
            config = BacktestConfig(
                strategy_code=strategy.code,
                strategy_params=backtest.parameters or {},
                symbols=[],  # TODO: 从策略或参数中获取
                start_date=backtest.start_date,
                end_date=backtest.end_date,
                initial_capital=backtest.initial_capital or Decimal("1000000"),
            )

            # 运行回测
            result = await vn_engine.run_backtest(config)

            # 更新回测结果
            backtest.status = "completed"
            backtest.result = {
                "total_return": str(result.total_return),
                "annual_return": str(result.annual_return),
                "max_drawdown": str(result.max_drawdown),
                "sharpe_ratio": str(result.sharpe_ratio),
                "total_trades": result.total_trades,
                "win_trades": result.win_trades,
                "loss_trades": result.loss_trades,
                "win_rate": str(result.win_rate),
                "final_capital": str(result.final_capital),
            }
            await self.session.flush()

            return {
                "success": True,
                "result": backtest.result,
            }

        except Exception as e:
            # 更新状态为失败
            backtest.status = "failed"
            backtest.result = {"error": str(e)}
            await self.session.flush()

            return {"success": False, "error": str(e)}

    async def get_backtest_report(self, backtest_id: int) -> dict[str, Any]:
        """
        获取回测报告

        Args:
            backtest_id: 回测ID

        Returns:
            dict[str, Any]: 回测报告数据
        """
        backtest = await self.get_backtest(backtest_id)
        if backtest is None:
            return {"success": False, "error": "Backtest not found"}

        # 获取关联的策略
        strategy = await self.get_strategy(backtest.strategy_id)

        return {
            "success": True,
            "report": {
                "id": backtest.id,
                "strategy_id": backtest.strategy_id,
                "strategy_name": strategy.name if strategy else None,
                "status": backtest.status,
                "start_date": backtest.start_date.isoformat(),
                "end_date": backtest.end_date.isoformat(),
                "initial_capital": str(backtest.initial_capital) if backtest.initial_capital else None,
                "parameters": backtest.parameters,
                "result": backtest.result,
                "created_at": backtest.created_at.isoformat() if backtest.created_at else None,
                "updated_at": backtest.updated_at.isoformat() if backtest.updated_at else None,
            },
        }
