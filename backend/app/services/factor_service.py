# backend/app/services/factor_service.py
"""因子管理服务"""
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.db.models import Factor, FactorAnalysis

CUSTOM_FACTOR_CATEGORY = "custom"


def normalize_factor_category(
    category: str | None,
    *,
    source: str | None = None,
    code: str | None = None,
) -> str | None:
    """User-authored factors should stay under one custom library."""
    if code or str(source or "").lower() in {"custom", "dsl", "python"}:
        return CUSTOM_FACTOR_CATEGORY
    return category


@dataclass
class FactorCreate:
    """创建因子请求"""

    name: str
    category: str | None = None
    source: str | None = None
    code: str | None = None
    parameters: dict[str, Any] | None = None
    description: str | None = None


@dataclass
class FactorUpdate:
    """更新因子请求"""

    name: str | None = None
    category: str | None = None
    source: str | None = None
    code: str | None = None
    parameters: dict[str, Any] | None = None
    description: str | None = None


class FactorService:
    """因子管理服务"""

    def __init__(self, session: AsyncSession):
        self.session = session

    async def create_factor(self, data: FactorCreate) -> Factor:
        """
        创建因子

        Args:
            data: 因子创建请求

        Returns:
            Factor: 创建的因子记录
        """
        factor = Factor(
            name=data.name,
            category=normalize_factor_category(data.category, source=data.source, code=data.code),
            source=data.source,
            code=data.code,
            parameters=data.parameters,
            description=data.description,
        )
        self.session.add(factor)
        await self.session.flush()
        return factor

    async def get_factor(self, factor_id: int) -> Factor | None:
        """
        获取因子详情

        Args:
            factor_id: 因子ID

        Returns:
            Factor | None: 因子记录
        """
        query = select(Factor).where(Factor.id == factor_id)
        result = await self.session.execute(query)
        return result.scalar_one_or_none()

    async def get_factor_by_name(self, name: str) -> Factor | None:
        """
        根据名称获取因子

        Args:
            name: 因子名称

        Returns:
            Factor | None: 因子记录
        """
        query = select(Factor).where(Factor.name == name)
        result = await self.session.execute(query)
        return result.scalar_one_or_none()

    async def list_factors(
        self,
        category: str | None = None,
        source: str | None = None,
    ) -> list[Factor]:
        """
        获取因子列表

        Args:
            category: 分类筛选
            source: 来源筛选

        Returns:
            list[Factor]: 因子列表
        """
        query = select(Factor)

        if category:
            query = query.where(Factor.category == category)
        if source:
            query = query.where(Factor.source == source)

        query = query.order_by(Factor.created_at.desc())
        result = await self.session.execute(query)
        return list(result.scalars().all())

    async def update_factor(self, factor_id: int, data: FactorUpdate) -> Factor | None:
        """
        更新因子

        Args:
            factor_id: 因子ID
            data: 更新数据

        Returns:
            Factor | None: 更新后的因子
        """
        factor = await self.get_factor(factor_id)
        if factor is None:
            return None

        if data.name is not None:
            factor.name = data.name
        if data.category is not None:
            factor.category = normalize_factor_category(data.category, source=data.source or factor.source, code=data.code or factor.code)
        if data.source is not None:
            factor.source = data.source
        if data.code is not None:
            factor.code = data.code
        if data.parameters is not None:
            factor.parameters = data.parameters
        if data.description is not None:
            factor.description = data.description
        factor.category = normalize_factor_category(factor.category, source=factor.source, code=factor.code)

        factor.updated_at = datetime.now()
        await self.session.flush()
        return factor

    async def delete_factor(self, factor_id: int) -> bool:
        """
        删除因子

        Args:
            factor_id: 因子ID

        Returns:
            bool: 是否删除成功
        """
        factor = await self.get_factor(factor_id)
        if factor is None:
            return False

        await self.session.delete(factor)
        await self.session.flush()
        return True

    async def get_analysis(self, analysis_id: int) -> FactorAnalysis | None:
        """
        获取分析结果

        Args:
            analysis_id: 分析ID

        Returns:
            FactorAnalysis | None: 分析结果
        """
        query = (
            select(FactorAnalysis)
            .options(selectinload(FactorAnalysis.factor))
            .where(FactorAnalysis.id == analysis_id)
        )
        result = await self.session.execute(query)
        return result.scalar_one_or_none()

    async def list_analyses(
        self,
        factor_id: int | None = None,
        limit: int = 50,
    ) -> list[FactorAnalysis]:
        """
        获取分析结果列表

        Args:
            factor_id: 因子ID筛选
            limit: 返回数量限制

        Returns:
            list[FactorAnalysis]: 分析结果列表
        """
        query = select(FactorAnalysis).options(selectinload(FactorAnalysis.factor))

        if factor_id:
            query = query.where(FactorAnalysis.factor_id == factor_id)

        query = query.order_by(FactorAnalysis.created_at.desc()).limit(limit)
        result = await self.session.execute(query)
        return list(result.scalars().all())
