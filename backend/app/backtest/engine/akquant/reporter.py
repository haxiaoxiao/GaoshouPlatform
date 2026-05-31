"""quantstats HTML 报告生成"""
from __future__ import annotations

from pathlib import Path
from typing import Any

from loguru import logger

from app.backtest.engine.akquant import AKQUANT_AVAILABLE

# 报告输出目录
REPORTS_DIR = Path(__file__).resolve().parent.parent.parent.parent / "reports"


def _ensure_reports_dir():
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)


def generate_report(
    task_id: str,
    raw_result: Any,
    benchmark_returns: Any = None,
) -> str | None:
    """生成 quantstats HTML 报告，返回文件路径"""
    if not AKQUANT_AVAILABLE:
        logger.warning("akquant not installed, report generation skipped")
        return None

    _ensure_reports_dir()
    filepath = REPORTS_DIR / f"{task_id}.html"

    try:
        raw_result.report(
            filename=str(filepath),
            benchmark=benchmark_returns,
            show=False,
        )
        logger.info("Report saved: {}", filepath)
        return str(filepath)
    except Exception as e:
        logger.error("Report generation failed: {}", e)
        return None


def get_report_path(task_id: str) -> str | None:
    """获取已有报告的路径"""
    filepath = REPORTS_DIR / f"{task_id}.html"
    if filepath.exists():
        return str(filepath)
    return None


def serve_report(task_id: str) -> str | None:
    """返回报告 HTML 内容"""
    filepath = REPORTS_DIR / f"{task_id}.html"
    if not filepath.exists():
        return None
    return filepath.read_text(encoding="utf-8")
