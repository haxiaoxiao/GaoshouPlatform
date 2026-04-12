# backend/app/main.py
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.api import api_router
from app.db import init_db


@asynccontextmanager
async def lifespan(app: FastAPI):
    """应用生命周期管理"""
    await init_db()
    yield


app = FastAPI(
    title="GaoshouPlatform API",
    description="量化投研平台后端服务",
    version="0.1.0",
    lifespan=lifespan,
)

# 注册 API 路由
app.include_router(api_router, prefix="/api")


@app.get("/health")
async def health_check():
    """健康检查接口（根路径）"""
    return {"status": "ok", "version": "0.1.0"}


@app.get("/")
async def root():
    """根路径"""
    return {"message": "Welcome to GaoshouPlatform API"}
