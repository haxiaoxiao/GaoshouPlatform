# backend/app/scripts/init_demo_strategy.py
"""初始化演示策略脚本"""
import asyncio

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.db.models import Strategy
from app.db.sqlite import async_session_factory, engine


# 双均线策略代码
DOUBLE_MA_STRATEGY_CODE = '''
"""
双均线策略示例

策略逻辑:
- 当短期均线上穿长期均线时买入
- 当短期均线下穿长期均线时卖出
"""

def init(context):
    # 设置策略参数
    context.short_period = context.params.get('short_period', 5)
    context.long_period = context.params.get('long_period', 20)


def handle_bar(context, bar):
    # 获取历史数据
    prices = context.history('close', context.long_period + 1)

    if len(prices) < context.long_period:
        return

    # 计算均线
    short_ma = prices[-context.short_period:].mean()
    long_ma = prices[-context.long_period:].mean()
    prev_short_ma = prices[-context.short_period-1:-1].mean()
    prev_long_ma = prices[-context.long_period-1:-1].mean()

    # 获取当前持仓
    position = context.get_position(bar.symbol)

    # 金叉买入
    if prev_short_ma <= prev_long_ma and short_ma > long_ma:
        if position.quantity == 0:
            order = context.buy(bar.symbol, 100)
            print(f"买入信号: {bar.symbol} @ {bar.close}")

    # 死叉卖出
    if prev_short_ma >= prev_long_ma and short_ma < long_ma:
        if position.quantity > 0:
            order = context.sell(bar.symbol, position.quantity)
            print(f"卖出信号: {bar.symbol} @ {bar.close}")
'''.strip()


async def init_demo_strategy() -> None:
    """初始化演示策略"""
    async with async_session_factory() as session:
        # 检查是否已存在
        result = await session.execute(
            select(Strategy).where(Strategy.name == "双均线策略")
        )
        existing = result.scalar_one_or_none()

        if existing:
            print(f"策略已存在: ID={existing.id}, Name={existing.name}")
            return

        # 创建新策略
        strategy = Strategy(
            name="双均线策略",
            code=DOUBLE_MA_STRATEGY_CODE,
            parameters={"short_period": 5, "long_period": 20},
            description="经典双均线策略，金叉买入，死叉卖出",
        )
        session.add(strategy)
        await session.commit()
        await session.refresh(strategy)

        print(f"策略创建成功: ID={strategy.id}, Name={strategy.name}")


async def main() -> None:
    """主函数"""
    try:
        await init_demo_strategy()
    finally:
        await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
