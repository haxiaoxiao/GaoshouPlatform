# backend/app/main.py
import asyncio
from contextlib import asynccontextmanager
from datetime import datetime
from uuid import uuid4

from fastapi import FastAPI, Request
from loguru import logger
from sqlalchemy import or_, select

from app.api import api_router
from app.cache.redis_cache import get_redis_client
from app.core.blocking import install_default_executor, shutdown_default_executor
from app.core.config import settings
from app.core.dev_data_mode import apply_dev_data_mode_to_settings
from app.core.logging import setup_logging
from app.data_stores import get_market_data_store
from app.db import init_db
from app.db.models.stock import Stock
from app.db.sqlite import async_session_factory
from app.services.market_radar import FocusUniverseResolver, MarketRadarService
from app.services.market_radar_data import MarketRadarDataService
from app.services.market_radar_intraday_context import MarketRadarIntradayContextLoader
from app.services.market_radar_store import MarketRadarStore
from app.services.qmt_realtime_feed import QmtRealtimeFeed
from app.services.runtime_tasks import mark_stale_runtime_tasks_failed

# 配置日志
setup_logging(debug=True)


def _is_a_share_symbol(symbol: str, exchange: str | None) -> bool:
    normalized = symbol.strip().upper()
    if len(normalized) != 9 or normalized[6] != ".":
        return False
    code, suffix = normalized.split(".", 1)
    if not code.isdigit() or suffix != (exchange or suffix).strip().upper():
        return False
    if suffix == "SH":
        return code.startswith(("60", "68"))
    if suffix == "SZ":
        return code.startswith(("00", "30"))
    if suffix == "BJ":
        return code.startswith(("4", "8", "9"))
    return False


async def _load_market_radar_universe() -> tuple[str, ...]:
    """Load active A-share equities with a short, isolated session."""

    async with async_session_factory() as session:
        result = await session.execute(
            select(Stock.symbol, Stock.exchange).where(
                Stock.exchange.in_(("SH", "SZ", "BJ")),
                or_(Stock.is_delist == 0, Stock.is_delist.is_(None)),
                or_(Stock.is_suspend == 0, Stock.is_suspend.is_(None)),
                or_(Stock.list_date.is_(None), Stock.list_date <= datetime.now().date()),
                or_(Stock.delist_date.is_(None), Stock.delist_date > datetime.now().date()),
            )
        )
    return tuple(
        sorted(
            symbol.strip().upper()
            for symbol, exchange in result
            if isinstance(symbol, str) and _is_a_share_symbol(symbol, exchange)
        )
    )


async def _start_market_radar_runtime(app: FastAPI) -> None:
    """Create and start the API-owned market-radar runtime exactly once."""

    if getattr(app.state, "market_radar_service", None) is not None:
        return
    session = async_session_factory()
    market_store = get_market_data_store()
    feed = QmtRealtimeFeed(
        universe_loader=_load_market_radar_universe,
        enabled=settings.market_radar_realtime_enabled,
        push_stale_seconds=settings.market_radar_push_stale_seconds,
        poll_interval_seconds=settings.market_radar_poll_interval_seconds,
        resubscribe_seconds=settings.market_radar_resubscribe_seconds,
    )
    context_loader = MarketRadarIntradayContextLoader(session, market_store=market_store)
    service = MarketRadarService(
        feed=feed,
        data_service=MarketRadarDataService(session, store=market_store),
        store=MarketRadarStore(session),
        focus_resolver=FocusUniverseResolver(session),
        eligible_universe_loader=context_loader.load_eligible_universe,
        symbol_context_loader=context_loader.load_symbol_context,
    )
    app.state.market_radar_session = session
    app.state.market_radar_feed = feed
    app.state.market_radar_service = service
    app.state.market_radar_refresh_tasks = set()
    app.state.market_radar_start_error = None
    try:
        await service.start()
    except ImportError:
        app.state.market_radar_start_error = "realtime market data unavailable"
        logger.warning("Market radar started without realtime QMT capability")
    else:
        feed_status = feed.status
        if callable(feed_status):
            feed_status = feed_status()
        mode = getattr(feed_status, "mode", None)
        if mode in {"offline", "polling_30s"}:
            logger.warning("Market radar realtime feed degraded: mode={}", mode)


async def _stop_market_radar_runtime(app: FastAPI) -> None:
    """Stop refresh work, the singleton feed service, and its owned session."""

    tasks = tuple(getattr(app.state, "market_radar_refresh_tasks", ()) or ())
    for task in tasks:
        task.cancel()
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)

    service = getattr(app.state, "market_radar_service", None)
    if service is not None:
        try:
            await service.stop()
        except Exception as exc:
            logger.warning("Market radar shutdown failed: {}", type(exc).__name__)
    session = getattr(app.state, "market_radar_session", None)
    if session is not None:
        await session.close()
    app.state.market_radar_service = None
    app.state.market_radar_feed = None
    app.state.market_radar_session = None


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

    try:
        await _start_market_radar_runtime(app)
        yield
    finally:
        logger.info("Stopping application...")
        try:
            await _stop_market_radar_runtime(app)
        finally:
            try:
                if redis_client is not None and redis_client.available:
                    redis_client.close()
                    logger.info("Redis connection closed")
            finally:
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
