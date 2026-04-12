# backend/app/api/router.py
from fastapi import APIRouter

from .backtest import router as backtest_router
from .data import router as data_router
from .factor import router as factor_router
from .system import router as system_router

api_router = APIRouter()

# 注册各模块路由
api_router.include_router(system_router, prefix="/system", tags=["系统"])
api_router.include_router(data_router, prefix="/data", tags=["数据"])
api_router.include_router(backtest_router, prefix="/backtest", tags=["回测"])
api_router.include_router(factor_router, prefix="/factor", tags=["因子"])
