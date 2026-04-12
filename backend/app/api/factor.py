# backend/app/api/factor.py
"""因子研究 API 接口"""
from datetime import date
from typing import Any

from fastapi import APIRouter, Body, Depends, HTTPException, Path, Query
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.sqlite import get_async_session
from app.engines.factor_engine import FactorConfig
from app.services.factor_service import FactorCreate, FactorService, FactorUpdate

router = APIRouter()


# ============== Pydantic Models ==============


class FactorCreateRequest(BaseModel):
    """创建因子请求"""

    name: str = Field(min_length=1, max_length=50, description="因子名称")
    category: str | None = Field(default=None, description="因子分类")
    source: str | None = Field(default=None, description="来源: qmt/custom")
    code: str | None = Field(default=None, description="因子计算代码")
    parameters: dict[str, Any] | None = Field(default=None, description="默认参数")
    description: str | None = Field(default=None, description="因子描述")


class FactorUpdateRequest(BaseModel):
    """更新因子请求"""

    name: str | None = Field(default=None, min_length=1, max_length=50)
    category: str | None = None
    source: str | None = None
    code: str | None = None
    parameters: dict[str, Any] | None = None
    description: str | None = None


class FactorResponse(BaseModel):
    """因子响应"""

    id: int
    name: str
    category: str | None
    source: str | None
    code: str | None
    parameters: dict[str, Any] | None
    description: str | None
    created_at: str | None
    updated_at: str | None


class FactorAnalysisRequest(BaseModel):
    """因子分析请求"""

    start_date: date = Field(description="分析起始日期")
    end_date: date = Field(description="分析结束日期")
    symbols: list[str] | None = Field(default=None, description="股票代码列表")
    normalize_window: int = Field(default=5, description="标准化窗口")
    factor_window: int = Field(default=20, description="因子计算窗口")
    forward_period: int = Field(default=20, description="收益前瞻期")


class FactorAnalysisResponse(BaseModel):
    """因子分析响应"""

    id: int
    factor_id: int
    factor_name: str | None
    start_date: str
    end_date: str
    ic_mean: float | None
    ic_std: float | None
    ir: float | None
    details: dict[str, Any] | None
    created_at: str


# ============== Factor Endpoints ==============


@router.get("/factors", summary="获取因子列表")
async def list_factors(
    category: str | None = Query(default=None, description="分类筛选"),
    source: str | None = Query(default=None, description="来源筛选"),
    session: AsyncSession = Depends(get_async_session),
) -> dict[str, Any]:
    """
    获取因子列表
    """
    service = FactorService(session)
    factors = await service.list_factors(category=category, source=source)

    items = [
        {
            "id": f.id,
            "name": f.name,
            "category": f.category,
            "source": f.source,
            "description": f.description,
            "created_at": f.created_at.isoformat() if f.created_at else None,
            "updated_at": f.updated_at.isoformat() if f.updated_at else None,
        }
        for f in factors
    ]

    return {"code": 0, "message": "success", "data": items}


@router.post("/factors", summary="创建因子")
async def create_factor(
    request: FactorCreateRequest = Body(description="因子信息"),
    session: AsyncSession = Depends(get_async_session),
) -> dict[str, Any]:
    """
    创建新因子
    """
    service = FactorService(session)

    # 检查名称是否重复
    existing = await service.get_factor_by_name(request.name)
    if existing:
        raise HTTPException(status_code=400, detail=f"因子名称 '{request.name}' 已存在")

    factor = await service.create_factor(
        FactorCreate(
            name=request.name,
            category=request.category,
            source=request.source,
            code=request.code,
            parameters=request.parameters,
            description=request.description,
        )
    )
    await session.commit()

    return {
        "code": 0,
        "message": "success",
        "data": {
            "id": factor.id,
            "name": factor.name,
            "category": factor.category,
            "source": factor.source,
            "description": factor.description,
            "created_at": factor.created_at.isoformat() if factor.created_at else None,
        },
    }


@router.get("/factors/{factor_id}", summary="获取因子详情")
async def get_factor(
    factor_id: int = Path(description="因子ID"),
    session: AsyncSession = Depends(get_async_session),
) -> dict[str, Any]:
    """
    获取因子详情
    """
    service = FactorService(session)
    factor = await service.get_factor(factor_id)

    if factor is None:
        raise HTTPException(status_code=404, detail=f"因子 {factor_id} 不存在")

    return {
        "code": 0,
        "message": "success",
        "data": {
            "id": factor.id,
            "name": factor.name,
            "category": factor.category,
            "source": factor.source,
            "code": factor.code,
            "parameters": factor.parameters,
            "description": factor.description,
            "created_at": factor.created_at.isoformat() if factor.created_at else None,
            "updated_at": factor.updated_at.isoformat() if factor.updated_at else None,
        },
    }


@router.put("/factors/{factor_id}", summary="更新因子")
async def update_factor(
    factor_id: int = Path(description="因子ID"),
    request: FactorUpdateRequest = Body(description="更新数据"),
    session: AsyncSession = Depends(get_async_session),
) -> dict[str, Any]:
    """
    更新因子信息
    """
    service = FactorService(session)

    # 检查名称是否重复
    if request.name:
        existing = await service.get_factor_by_name(request.name)
        if existing and existing.id != factor_id:
            raise HTTPException(status_code=400, detail=f"因子名称 '{request.name}' 已存在")

    factor = await service.update_factor(
        factor_id,
        FactorUpdate(
            name=request.name,
            category=request.category,
            source=request.source,
            code=request.code,
            parameters=request.parameters,
            description=request.description,
        ),
    )

    if factor is None:
        raise HTTPException(status_code=404, detail=f"因子 {factor_id} 不存在")

    await session.commit()

    return {
        "code": 0,
        "message": "success",
        "data": {
            "id": factor.id,
            "name": factor.name,
            "category": factor.category,
            "source": factor.source,
            "description": factor.description,
        },
    }


@router.delete("/factors/{factor_id}", summary="删除因子")
async def delete_factor(
    factor_id: int = Path(description="因子ID"),
    session: AsyncSession = Depends(get_async_session),
) -> dict[str, Any]:
    """
    删除因子
    """
    service = FactorService(session)
    success = await service.delete_factor(factor_id)

    if not success:
        raise HTTPException(status_code=404, detail=f"因子 {factor_id} 不存在")

    await session.commit()

    return {"code": 0, "message": "success", "data": {"deleted": True}}


@router.post("/factors/{factor_id}/analyze", summary="运行因子分析")
async def run_factor_analysis(
    factor_id: int = Path(description="因子ID"),
    request: FactorAnalysisRequest = Body(description="分析参数"),
    session: AsyncSession = Depends(get_async_session),
) -> dict[str, Any]:
    """
    运行因子分析

    计算 IC、分组收益等指标
    """
    service = FactorService(session)

    config = FactorConfig(
        normalize_window=request.normalize_window,
        factor_window=request.factor_window,
        forward_period=request.forward_period,
    )

    try:
        analysis = await service.run_analysis(
            factor_id=factor_id,
            start_date=request.start_date,
            end_date=request.end_date,
            symbols=request.symbols,
            config=config,
        )
        await session.commit()

        return {
            "code": 0,
            "message": "success",
            "data": {
                "id": analysis.id,
                "factor_id": analysis.factor_id,
                "start_date": analysis.start_date.isoformat(),
                "end_date": analysis.end_date.isoformat(),
                "ic_mean": float(analysis.ic_mean) if analysis.ic_mean else None,
                "ic_std": float(analysis.ic_std) if analysis.ic_std else None,
                "ir": float(analysis.ir) if analysis.ir else None,
                "details": analysis.details,
                "created_at": analysis.created_at.isoformat() if analysis.created_at else None,
            },
        }
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"分析失败: {str(e)}")


# ============== Analysis Endpoints ==============


@router.get("/analyses", summary="获取分析记录列表")
async def list_analyses(
    factor_id: int | None = Query(default=None, description="因子ID筛选"),
    limit: int = Query(default=20, ge=1, le=100, description="返回数量限制"),
    session: AsyncSession = Depends(get_async_session),
) -> dict[str, Any]:
    """
    获取因子分析记录列表
    """
    service = FactorService(session)
    analyses = await service.list_analyses(factor_id=factor_id, limit=limit)

    items = [
        {
            "id": a.id,
            "factor_id": a.factor_id,
            "factor_name": a.factor.name if a.factor else None,
            "start_date": a.start_date.isoformat(),
            "end_date": a.end_date.isoformat(),
            "ic_mean": float(a.ic_mean) if a.ic_mean else None,
            "ic_std": float(a.ic_std) if a.ic_std else None,
            "ir": float(a.ir) if a.ir else None,
            "created_at": a.created_at.isoformat() if a.created_at else None,
        }
        for a in analyses
    ]

    return {"code": 0, "message": "success", "data": items}


@router.get("/analyses/{analysis_id}", summary="获取分析结果详情")
async def get_analysis(
    analysis_id: int = Path(description="分析ID"),
    session: AsyncSession = Depends(get_async_session),
) -> dict[str, Any]:
    """
    获取因子分析结果详情
    """
    service = FactorService(session)
    analysis = await service.get_analysis(analysis_id)

    if analysis is None:
        raise HTTPException(status_code=404, detail=f"分析记录 {analysis_id} 不存在")

    return {
        "code": 0,
        "message": "success",
        "data": {
            "id": analysis.id,
            "factor_id": analysis.factor_id,
            "factor_name": analysis.factor.name if analysis.factor else None,
            "start_date": analysis.start_date.isoformat(),
            "end_date": analysis.end_date.isoformat(),
            "ic_mean": float(analysis.ic_mean) if analysis.ic_mean else None,
            "ic_std": float(analysis.ic_std) if analysis.ic_std else None,
            "ir": float(analysis.ir) if analysis.ir else None,
            "turnover_rate": float(analysis.turnover_rate) if analysis.turnover_rate else None,
            "details": analysis.details,
            "created_at": analysis.created_at.isoformat() if analysis.created_at else None,
        },
    }
