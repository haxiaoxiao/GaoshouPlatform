"""Centralized logging configuration using loguru."""
import sys
from pathlib import Path
from loguru import logger


def setup_logging(debug: bool = False) -> None:
    """配置 loguru — 移除默认 handler，添加控制台 + 文件输出。

    Args:
        debug: 是否开启 DEBUG 级别日志
    """
    logger.remove()

    level = "DEBUG" if debug else "INFO"

    # 控制台输出
    logger.add(
        sys.stderr,
        level=level,
        format=(
            "<green>{time:HH:mm:ss}</green> | "
            "<level>{level: <8}</level> | "
            "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> | "
            "<level>{message}</level>"
        ),
        colorize=True,
    )

    # 确保日志目录存在
    log_dir = Path(__file__).parent.parent.parent / "logs"
    log_dir.mkdir(exist_ok=True)

    # 文件输出 — 按天轮转，保留 30 天
    logger.add(
        log_dir / "gaoshou_{time:YYYY-MM-DD}.log",
        level="DEBUG",
        format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level} | {name}:{function}:{line} | {message}",
        rotation="00:00",
        retention="30 days",
        compression="gz",
        encoding="utf-8",
        enqueue=True,
    )

    # 错误单独记录
    logger.add(
        log_dir / "error_{time:YYYY-MM-DD}.log",
        level="ERROR",
        format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {name}:{function}:{line} | {message}\n{exception}",
        rotation="00:00",
        retention="90 days",
        encoding="utf-8",
        enqueue=True,
    )

    logger.info(f"Logging configured (level={level}, log_dir={log_dir})")
