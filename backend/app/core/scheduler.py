# backend/app/core/scheduler.py
"""定时任务调度器模块

使用 APScheduler 实现定时任务调度，支持 cron 表达式。
"""
from datetime import datetime
from typing import Any

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from loguru import logger
from sqlalchemy import select, update

from app.db.models import SyncTask
from app.db.sqlite import async_session_factory

# 单例调度器实例
_scheduler: AsyncIOScheduler | None = None


def get_scheduler() -> AsyncIOScheduler:
    """
    获取调度器单例实例

    Returns:
        AsyncIOScheduler: 调度器实例
    """
    global _scheduler
    if _scheduler is None:
        _scheduler = AsyncIOScheduler()
    return _scheduler


def start_scheduler() -> None:
    """启动调度器"""
    scheduler = get_scheduler()
    if not scheduler.running:
        scheduler.start()
        logger.info("Scheduler started")


def stop_scheduler() -> None:
    """停止调度器"""
    global _scheduler
    if _scheduler is not None and _scheduler.running:
        _scheduler.shutdown(wait=False)
        logger.info("Scheduler stopped")


async def _execute_sync_job(task_id: int, sync_type: str, **kwargs: Any) -> None:
    """
    执行同步任务的内部函数

    Args:
        task_id: 任务ID
        sync_type: 同步类型
        **kwargs: 传递给同步服务的额外参数
    """
    from app.services.sync_service import SyncService

    logger.info(f"Executing sync job: task_id={task_id}, sync_type={sync_type}")

    try:
        async with async_session_factory() as session:
            sync_service = SyncService(session)

            # 根据同步类型调用相应方法
            if sync_type == "stock_info":
                await sync_service.sync_stock_info(task_id=task_id)
            elif sync_type == "kline_daily":
                await sync_service.sync_kline_daily(
                    task_id=task_id,
                    symbols=kwargs.get("symbols"),
                    start_date=kwargs.get("start_date"),
                    end_date=kwargs.get("end_date"),
                )
            elif sync_type == "kline_minute":
                await sync_service.sync_kline_minute(
                    task_id=task_id,
                    symbols=kwargs.get("symbols"),
                    start_date=kwargs.get("start_date"),
                    end_date=kwargs.get("end_date"),
                )
            else:
                logger.error(f"Unknown sync type: {sync_type}")

            # 更新任务的最后执行时间
            now = datetime.now()
            await session.execute(
                update(SyncTask)
                .where(SyncTask.id == task_id)
                .values(last_run_at=now)
            )
            await session.commit()

            logger.info(f"Sync job completed: task_id={task_id}")

    except Exception as e:
        logger.exception(f"Sync job failed: task_id={task_id}, error={e}")


def add_sync_job(task: SyncTask) -> str | None:
    """
    添加同步任务到调度器

    Args:
        task: 同步任务配置对象

    Returns:
        str | None: 任务ID（job_id），失败返回 None
    """
    scheduler = get_scheduler()
    job_id = f"sync_task_{task.id}"

    try:
        # 解析 cron 表达式
        trigger = CronTrigger.from_crontab(task.cron_expression)

        # 准备任务参数
        kwargs: dict[str, Any] = {}
        if task.symbols:
            import json

            try:
                kwargs["symbols"] = json.loads(task.symbols)
            except json.JSONDecodeError:
                logger.warning(f"Invalid symbols JSON for task {task.id}")

        if task.start_date:
            kwargs["start_date"] = task.start_date
        if task.end_date:
            kwargs["end_date"] = task.end_date

        # 添加任务
        scheduler.add_job(
            _execute_sync_job,
            trigger=trigger,
            id=job_id,
            args=[task.id, task.sync_type],
            kwargs=kwargs,
            name=task.name,
            replace_existing=True,
        )

        # 更新下次执行时间
        next_run = scheduler.get_job(job_id).next_run_time
        if next_run:
            _update_task_next_run(task.id, next_run)

        logger.info(
            f"Added sync job: id={job_id}, name={task.name}, "
            f"cron={task.cron_expression}, next_run={next_run}"
        )

        return job_id

    except Exception as e:
        logger.error(f"Failed to add sync job {task.id}: {e}")
        return None


def remove_sync_job(task_id: int) -> bool:
    """
    从调度器移除同步任务

    Args:
        task_id: 任务ID

    Returns:
        bool: 是否成功移除
    """
    scheduler = get_scheduler()
    job_id = f"sync_task_{task_id}"

    try:
        scheduler.remove_job(job_id)
        logger.info(f"Removed sync job: id={job_id}")
        return True
    except Exception:
        # 任务不存在时会抛出异常
        return False


def update_sync_job(task: SyncTask) -> str | None:
    """
    更新调度器中的同步任务

    实际上是先移除再添加新任务

    Args:
        task: 更新后的同步任务配置

    Returns:
        str | None: 任务ID，失败返回 None
    """
    remove_sync_job(task.id)
    return add_sync_job(task)


def _update_task_next_run(task_id: int, next_run: datetime) -> None:
    """
    更新任务的下次执行时间（同步版本，用于在调度器中调用）

    Args:
        task_id: 任务ID
        next_run: 下次执行时间
    """
    import asyncio

    async def _do_update():
        async with async_session_factory() as session:
            await session.execute(
                update(SyncTask)
                .where(SyncTask.id == task_id)
                .values(next_run_at=next_run)
            )
            await session.commit()

    try:
        # 尝试在现有事件循环中运行
        loop = asyncio.get_running_loop()
        asyncio.ensure_future(_do_update(), loop=loop)
    except RuntimeError:
        # 没有运行中的事件循环，创建新的
        asyncio.run(_do_update())


async def load_enabled_tasks() -> None:
    """
    加载所有启用的同步任务到调度器

    在应用启动时调用，从数据库加载所有启用的任务。
    """
    async with async_session_factory() as session:
        result = await session.execute(
            select(SyncTask).where(SyncTask.enabled == True)
        )
        tasks = result.scalars().all()

        loaded_count = 0
        for task in tasks:
            job_id = add_sync_job(task)
            if job_id:
                loaded_count += 1

        logger.info(f"Loaded {loaded_count} enabled sync tasks from database")


async def reload_scheduler_tasks() -> int:
    """
    重新加载所有同步任务

    清空当前调度器中的任务，重新从数据库加载。

    Returns:
        int: 成功加载的任务数量
    """
    scheduler = get_scheduler()

    # 移除所有 sync_task_* 任务
    for job in scheduler.get_jobs():
        if job.id.startswith("sync_task_"):
            scheduler.remove_job(job.id)

    # 重新加载
    await load_enabled_tasks()

    return len([j for j in scheduler.get_jobs() if j.id.startswith("sync_task_")])
