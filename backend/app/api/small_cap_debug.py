"""Small-cap strategy debug task API."""

from __future__ import annotations

import asyncio
import time
import uuid
from datetime import date
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from fastapi import APIRouter, Body
from pydantic import BaseModel, Field

from app.scripts.run_small_cap_yearly_debug import _run_segment, _write_csv, _year_segments

router = APIRouter()

_tasks: dict[str, dict[str, Any]] = {}


class SmallCapYearlyDebugRequest(BaseModel):
    strategy_id: int = 43
    index_symbol: str = "399101.SZ"
    start_date: date = Field(default=date(2020, 1, 1))
    end_date: date = Field(default=date(2026, 5, 6))
    initial_capital: float = 1_000_000
    cash_buffer_rate: float = 0.002
    filter_st: bool = True
    v4_indicator_mode: str = "robust"
    industry_mode: str = "local"
    execution_plan_mode: str = "timer"
    output_dir: str = "backend/app/reports/small_cap_yearly_debug"


@router.post("/yearly")
async def run_yearly_debug(request: SmallCapYearlyDebugRequest = Body(...)):
    task_id = str(uuid.uuid4())[:8]
    _tasks[task_id] = {
        "status": "queued",
        "progress": 0.0,
        "created_at": time.time(),
        "result": None,
        "error": None,
    }

    async def _run() -> None:
        task = _tasks[task_id]
        task["status"] = "running"
        try:
            out_dir = Path(request.output_dir)
            if not out_dir.is_absolute():
                out_dir = Path(__file__).resolve().parents[3] / out_dir
            out_dir.mkdir(parents=True, exist_ok=True)
            args = SimpleNamespace(
                strategy_id=request.strategy_id,
                index_symbol=request.index_symbol,
                initial_capital=request.initial_capital,
                cash_buffer_rate=request.cash_buffer_rate,
                filter_st=request.filter_st,
                v4_indicator_mode=request.v4_indicator_mode,
                industry_mode=request.industry_mode,
                execution_plan_mode=request.execution_plan_mode,
            )
            segments = _year_segments(request.start_date, request.end_date)
            rows = []
            for idx, (year, seg_start, seg_end) in enumerate(segments, start=1):
                task["current_year"] = year
                rows.append(await _run_segment(args, year, seg_start, seg_end, out_dir))
                task["progress"] = idx / max(1, len(segments))
            _write_csv(out_dir / "summary.csv", rows)
            import json

            (out_dir / "summary.json").write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")
            task["status"] = "done"
            task["progress"] = 1.0
            task["result"] = {
                "rows": rows,
                "output_dir": str(out_dir),
                "summary_csv": str(out_dir / "summary.csv"),
                "summary_json": str(out_dir / "summary.json"),
            }
            task["finished_at"] = time.time()
        except Exception as exc:
            task["status"] = "failed"
            task["progress"] = 1.0
            task["error"] = f"{type(exc).__name__}: {exc}"
            task["finished_at"] = time.time()

    asyncio.create_task(_run())
    return {"code": 0, "message": "success", "data": {"task_id": task_id}}


@router.get("/yearly/{task_id}")
async def get_yearly_debug_task(task_id: str):
    task = _tasks.get(task_id)
    if task is None:
        return {"code": 1, "message": "Task not found", "data": None}
    return {"code": 0, "message": "success", "data": task}
