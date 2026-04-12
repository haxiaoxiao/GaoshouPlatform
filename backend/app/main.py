# backend/app/main.py
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.api import api_router
from app.core.scheduler import load_enabled_tasks, start_scheduler, stop_scheduler
from app.db import init_db

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """应用生命周期管理"""
    logger.info("Starting application...")

    # 初始化数据库
    await init_db()
    logger.info("Database initialized")

    # 启动调度器
    start_scheduler()
    logger.info("Scheduler started")

    # 加载启用的定时任务
    await load_enabled_tasks()
    logger.info("Sync tasks loaded")

    yield

    # 关闭调度器
    logger.info("Stopping application...")
    stop_scheduler()
    logger.info("Application stopped")


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
