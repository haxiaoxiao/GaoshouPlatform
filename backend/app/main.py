# backend/app/main.py
from contextlib import asynccontextmanager
from uuid import uuid4

from fastapi import FastAPI, Request
from loguru import logger

from app.api import api_router
from app.cache.redis_cache import get_redis_client
from app.core.blocking import install_default_executor, shutdown_default_executor
from app.core.dev_data_mode import apply_dev_data_mode_to_settings
from app.core.logging import setup_logging
from app.db import init_db
from app.services.runtime_tasks import mark_stale_runtime_tasks_failed

# 配置日志
setup_logging(debug=True)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """应用生命周期管理"""
    logger.info("Starting application...")
    apply_dev_data_mode_to_settings()
    install_default_executor()

    # 初始化数据库
    await init_db()
    logger.info("Database initialized")
    stale_sentiment_tasks = mark_stale_runtime_tasks_failed(kinds={"sentiment_ingest"})
    if stale_sentiment_tasks:
        logger.warning("Marked {} legacy sentiment task(s) as failed", stale_sentiment_tasks)
    interrupted_ai_approvals = mark_stale_runtime_tasks_failed(
        kinds={"ai_approval"},
        older_than_seconds=0,
        message="Write outcome is indeterminate after application restart",
    )
    if interrupted_ai_approvals:
        logger.warning("Marked {} interrupted AI approval(s) as failed", interrupted_ai_approvals)
    from app.api.ai import resume_ai_workflows
    from app.db.sqlite import async_session_factory
    from app.services.ai_native import AINativeService

    async with async_session_factory() as session:
        ai_service = AINativeService(session)
        expired_ai_conversations = await ai_service.cleanup_expired()
        reconciled_ai_conversations = await ai_service.reconcile_approval_states()
    if expired_ai_conversations:
        logger.info("Removed {} expired AI conversation(s)", expired_ai_conversations)
    if reconciled_ai_conversations:
        logger.info("Reconciled approval state in {} AI conversation(s)", reconciled_ai_conversations)
    resumed_ai_workflows = resume_ai_workflows()
    if resumed_ai_workflows:
        logger.info("Resumed {} AI workflow(s)", resumed_ai_workflows)

    # 启动调度器
    logger.info("Sync scheduler is owned by the isolated sync service")

    # 加载启用的定时任务

    # 初始化 Redis 缓存
    redis_client = None
    try:
        redis_client = get_redis_client()
        if redis_client.available:
            logger.info("Redis cache initialized")
        else:
            logger.info("Redis cache not available, running without cache")
    except Exception:
        logger.info("Redis cache not available, running without cache")

    yield

    # 关闭调度器
    logger.info("Stopping application...")
    # 关闭 Redis 连接
    if redis_client is not None and redis_client.available:
        redis_client.close()
        logger.info("Redis connection closed")

    shutdown_default_executor()
    logger.info("Application stopped")


app = FastAPI(
    title="GaoshouPlatform API",
    description="量化投研平台后端服务",
    version="0.1.0",
    lifespan=lifespan,
)

# 注册 API 路由
app.include_router(api_router, prefix="/api")


@app.middleware("http")
async def add_contract_headers(request: Request, call_next):
    request_id = request.headers.get("X-Request-ID") or uuid4().hex
    request.state.request_id = request_id
    with logger.contextualize(request_id=request_id):
        response = await call_next(request)
    response.headers["X-Request-ID"] = request_id
    if (
        request.url.path.startswith("/api/")
        and not request.url.path.startswith("/api/v1")
        and not request.url.path.startswith("/api/ai")
    ):
        response.headers["Deprecation"] = "true"
        response.headers["Sunset"] = "Thu, 10 Sep 2026 00:00:00 GMT"
        response.headers["Link"] = '</api/v1>; rel="successor-version"'
    return response


@app.middleware("http")
async def apply_dev_data_mode_middleware(request, call_next):
    apply_dev_data_mode_to_settings()
    return await call_next(request)


@app.get("/health")
async def health_check():
    """健康检查接口（根路径）"""
    return {"status": "ok", "version": "0.1.0"}


@app.get("/")
async def root():
    """根路径"""
    return {"message": "Welcome to GaoshouPlatform API"}
