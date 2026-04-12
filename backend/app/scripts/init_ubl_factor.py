# backend/app/scripts/init_ubl_factor.py
"""初始化 UBL 因子"""
import asyncio
from datetime import date

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import Factor
from app.db.sqlite import async_session_factory


async def init_ubl_factors():
    """初始化 UBL 相关因子"""
    async with async_session_factory() as session:
        # 检查是否已存在
        query = select(Factor).where(Factor.name == "UBL")
        result = await session.execute(query)
        if result.scalar_one_or_none():
            print("UBL 因子已存在，跳过初始化")
            return

        # 创建 UBL 因子
        ubl_factor = Factor(
            name="UBL",
            category="技术因子",
            source="custom",
            code="""
# UBL 因子计算逻辑
# 蜡烛上影线 = High - max(Open, Close)
# 威廉下影线 = Close - Low
# 标准化：当日值 / 过去M日均值
# 均值因子：过去N日标准化影线的均值
# 标准差因子：过去N日标准化影线的标准差
# UBL = zscore(蜡烛上_std_desize) + zscore(威廉下_mean_desize)
""",
            parameters={
                "normalize_window": 5,
                "factor_window": 20,
                "forward_period": 20,
            },
            description="""
UBL 上下影线因子（东吴证券研报）

核心逻辑：
1. 蜡烛上影线因子有效，标准差版本效果最佳
2. 蜡烛下影线因子无效
3. 威廉下影线因子有效，均值版本效果最佳
4. 融合构建 UBL 综合因子

公式：
- 蜡烛上影线 = High - max(Open, Close)
- 威廉下影线 = Close - Low
- UBL = zscore(蜡烛上_std_desize) + zscore(威廉下_mean_desize)

回测表现（2009-2020）：
- 年化收益：15.86%
- 信息比率：2.29
- 月度胜率：73.53%
- 最大回撤：3.68%

来源：东吴证券《上下影线，蜡烛好还是威廉好？》(2020)
""",
        )
        session.add(ubl_factor)

        # 创建蜡烛上影线因子
        upper_shadow_factor = Factor(
            name="蜡烛上影线",
            category="技术因子",
            source="custom",
            code="# 蜡烛上影线 = High - max(Open, Close)",
            parameters={
                "normalize_window": 5,
                "factor_window": 20,
            },
            description="蜡烛图上影线，衡量卖压大小",
        )
        session.add(upper_shadow_factor)

        # 创建威廉下影线因子
        lower_shadow_factor = Factor(
            name="威廉下影线",
            category="技术因子",
            source="custom",
            code="# 威廉下影线 = Close - Low",
            parameters={
                "normalize_window": 5,
                "factor_window": 20,
            },
            description="威廉指标下影线，衡量买气强弱",
        )
        session.add(lower_shadow_factor)

        await session.commit()
        print("已创建因子: UBL, 蜡烛上影线, 威廉下影线")


async def run_ubl_analysis():
    """运行 UBL 因子分析"""
    from app.engines.factor_engine import FactorConfig, get_factor_engine
    from app.services.factor_service import FactorService

    print("开始运行 UBL 因子分析...")

    engine = get_factor_engine()
    result = engine.run_factor_analysis(
        symbols=None,  # 使用所有股票
        start_date=date(2024, 1, 1),
        end_date=date(2026, 4, 12),
        config=FactorConfig(
            normalize_window=5,
            factor_window=20,
            forward_period=20,
        ),
    )

    print("\n=== UBL 因子分析结果 ===")
    print(f"IC 均值: {result.get('ic_mean', 'N/A'):.4f}")
    print(f"IC 标准差: {result.get('ic_std', 'N/A'):.4f}")
    print(f"年化 ICIR: {result.get('annual_icir', 'N/A'):.4f}")
    print(f"年化收益: {result.get('annual_return', 'N/A'):.2%}")
    print(f"信息比率: {result.get('information_ratio', 'N/A'):.4f}")
    print(f"月度胜率: {result.get('win_rate', 'N/A'):.2%}")
    print(f"最大回撤: {result.get('max_drawdown', 'N/A'):.2%}")
    print(f"股票数量: {result.get('total_stocks', 'N/A')}")
    print(f"分析日期数: {result.get('total_dates', 'N/A')}")

    return result


if __name__ == "__main__":
    # 初始化因子
    asyncio.run(init_ubl_factors())

    # 运行分析（需要数据库有 K 线数据）
    # asyncio.run(run_ubl_analysis())
