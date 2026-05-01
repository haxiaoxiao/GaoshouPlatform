"""因子评估 API — /v2/evaluation"""
import logging
from datetime import date

from fastapi import APIRouter
from pydantic import BaseModel, Field

from app.models.factor import FactorConfig, EvalConfig, BoardQuery
from app.services.factor_evaluation import FactorEvaluationService, get_evaluation_service

logger = logging.getLogger(__name__)

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


@router.post("/report")
async def factor_report(config: FactorConfig, eval_config: EvalConfig | None = None):
    """Generate 6-module factor analysis report."""
    try:
        svc = get_evaluation_service()
        report = await svc.report(config, eval_config)
        return {"code": 0, "message": "success", "data": report.model_dump()}
    except Exception as e:
        logger.exception("Factor report failed")
        return {"code": 1, "message": str(e), "data": None}


@router.post("/board")
async def factor_board(query: BoardQuery):
    """Query factor board with filters, sorting, and pagination."""
    try:
        svc = get_evaluation_service()
        result = await svc.board_query(query)
        return {"code": 0, "message": "success", "data": result.model_dump()}
    except Exception as e:
        logger.exception("Factor board query failed")
        return {"code": 1, "message": str(e), "data": None}
