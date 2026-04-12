# backend/app/api/router.py
from fastapi import APIRouter

from .data import router as data_router
from .system import router as system_router

api_router = APIRouter()

# 注册各模块路由
api_router.include_router(system_router, prefix="/system", tags=["系统"])
api_router.include_router(data_router, prefix="/data", tags=["数据"])
