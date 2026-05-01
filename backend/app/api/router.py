# backend/app/api/router.py
from fastapi import APIRouter

from .backtest import router as backtest_router
from .data import router as data_router
from .data_explorer import router as data_explorer_router
from .data_skill import router as data_skill_router
from .evaluation import router as evaluation_router
from .factor import router as factor_router
from .indicator import router as indicator_router
from .strategy import router as strategy_router
from .system import router as system_router
from app.compute.api import router as compute_router
from app.backtest.api import router as backtest_v2_router
from app.api.factors import router as factors_router

api_router = APIRouter()

api_router.include_router(system_router, prefix="/system", tags=["系统"])
api_router.include_router(data_router, prefix="/data", tags=["数据"])
api_router.include_router(data_explorer_router, prefix="/explorer", tags=["数据浏览器"])
api_router.include_router(data_skill_router, prefix="/skill", tags=["数据技能"])
api_router.include_router(backtest_router, prefix="/backtest", tags=["回测"])
api_router.include_router(factor_router, prefix="/factor", tags=["因子"])
api_router.include_router(compute_router, tags=["计算引擎"])
api_router.include_router(backtest_v2_router, tags=["回测引擎"])
api_router.include_router(indicator_router, prefix="/indicators", tags=["指标"])
api_router.include_router(strategy_router, prefix="/strategy", tags=["策略"])
api_router.include_router(evaluation_router, tags=["因子评估"])
api_router.include_router(factors_router)
