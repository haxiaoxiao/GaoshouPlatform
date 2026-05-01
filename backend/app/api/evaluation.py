"""因子评估 API — /v2/evaluation"""
from datetime import date

from fastapi import APIRouter
from loguru import logger
from pydantic import BaseModel, Field

from app.services.factor_evaluation import FactorEvaluationService

router = APIRouter(prefix="/v2/evaluation")


# ============== Request Models ==============


class ICAnalysisRequest(BaseModel):
    expression: str = Field(description="因子表达式")
    symbols: list[str] = Field(description="股票代码列表")
    start_date: str = Field(description="起始日期 (ISO 格式)")
    end_date: str = Field(description="结束日期 (ISO 格式)")


class QuantileBacktestRequest(BaseModel):
    expression: str = Field(description="因子表达式")
    symbols: list[str] = Field(description="股票代码列表")
    start_date: str = Field(description="起始日期 (ISO 格式)")
    end_date: str = Field(description="结束日期 (ISO 格式)")
    n_groups: int = Field(default=5, ge=2, le=20, description="分组数量")
    rebalance_freq: str = Field(default="monthly", description="再平衡频率")


class FullReportRequest(BaseModel):
    expression: str = Field(description="因子表达式")
    symbols: list[str] = Field(description="股票代码列表")
    start_date: str = Field(description="起始日期 (ISO 格式)")
    end_date: str = Field(description="结束日期 (ISO 格式)")
    n_groups: int = Field(default=5, ge=2, le=20, description="分组数量")
    rebalance_freq: str = Field(default="monthly", description="再平衡频率")


# ============== Endpoints ==============


@router.post("/ic-analysis")
async def ic_analysis(req: ICAnalysisRequest):
    """单因子 IC 分析"""
    try:
        start = date.fromisoformat(req.start_date)
        end = date.fromisoformat(req.end_date)
        service = FactorEvaluationService()
        data = await service.run_ic_analysis(
            expression=req.expression,
            symbols=req.symbols,
            start_date=start,
            end_date=end,
        )
        return {"code": 0, "message": "success", "data": data}
    except Exception as e:
        logger.exception("IC analysis failed")
        return {"code": 1, "message": str(e), "data": None}


@router.post("/quantile-backtest")
async def quantile_backtest(req: QuantileBacktestRequest):
    """分层回测"""
    try:
        start = date.fromisoformat(req.start_date)
        end = date.fromisoformat(req.end_date)
        service = FactorEvaluationService()
        data = await service.run_quantile_backtest(
            expression=req.expression,
            symbols=req.symbols,
            start_date=start,
            end_date=end,
            n_groups=req.n_groups,
            rebalance_freq=req.rebalance_freq,
        )
        return {"code": 0, "message": "success", "data": data}
    except Exception as e:
        logger.exception("Quantile backtest failed")
        return {"code": 1, "message": str(e), "data": None}


@router.post("/full-report")
async def full_report(req: FullReportRequest):
    """完整单因子评估报告"""
    try:
        start = date.fromisoformat(req.start_date)
        end = date.fromisoformat(req.end_date)
        service = FactorEvaluationService()
        data = await service.run_full_report(
            expression=req.expression,
            symbols=req.symbols,
            start_date=start,
            end_date=end,
            n_groups=req.n_groups,
            rebalance_freq=req.rebalance_freq,
        )
        return {"code": 0, "message": "success", "data": data}
    except Exception as e:
        logger.exception("Full report failed")
        return {"code": 1, "message": str(e), "data": None}
