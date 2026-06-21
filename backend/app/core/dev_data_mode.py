"""Runtime data-mode switch for the development environment."""
from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from app.core.config import settings


@dataclass(frozen=True)
class DevDataModeState:
    enabled: bool
    environment: str
    use_prod_data: bool
    active_data_dir: str
    active_database_url: str
    active_parquet_data_dir: str
    dev_local_data_dir: str
    dev_prod_data_dir: str
    warning: str | None
    updated_at: str | None = None


_WARNING = (
    "当前 dev 环境将直接读写生产真实数据目录。数据同步会写入真实 SQLite/Parquet，"
    "策略运行会读取真实行情、财务和因子数据。请确认不是在做破坏性测试。"
)


def is_dev_environment() -> bool:
    return settings.base_dir.name.lower().endswith("-dev")


def _mode_file() -> Path:
    return Path(settings.dev_data_mode_file)


def _resolve(path_text: str) -> Path:
    return Path(path_text).expanduser().resolve()


def _default_use_prod_data() -> bool:
    if not is_dev_environment():
        return False
    prod_dir = _resolve(settings.dev_prod_data_dir)
    configured_data_dir = _resolve(settings.gaoshou_data_dir)
    configured_parquet_dir = _resolve(settings.parquet_data_dir)
    prod_parquet_dir = (prod_dir / "parquet").resolve()
    return configured_data_dir == prod_dir or configured_parquet_dir == prod_parquet_dir


def _read_payload() -> dict[str, Any]:
    path = _mode_file()
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return data if isinstance(data, dict) else {}


def _active_paths(use_prod_data: bool) -> tuple[Path, str, Path]:
    data_dir = _resolve(settings.dev_prod_data_dir if use_prod_data else settings.dev_local_data_dir)
    database_url = f"sqlite+aiosqlite:///{(data_dir / 'gaoshou.db').as_posix()}"
    parquet_dir = data_dir / "parquet"
    return data_dir, database_url, parquet_dir


def _sqlite_path_for_data_dir(data_dir: Path) -> Path:
    return data_dir / "gaoshou.db"


def get_dev_data_mode_state() -> DevDataModeState:
    enabled = is_dev_environment()
    payload = _read_payload() if enabled else {}
    use_prod_data = bool(payload.get("use_prod_data", _default_use_prod_data())) if enabled else False
    if enabled and not use_prod_data:
        local_data_dir = _resolve(settings.dev_local_data_dir)
        if not _sqlite_path_for_data_dir(local_data_dir).exists():
            use_prod_data = True
    data_dir, database_url, parquet_dir = _active_paths(use_prod_data) if enabled else (
        settings.data_dir,
        settings.database_url,
        Path(settings.parquet_data_dir),
    )
    return DevDataModeState(
        enabled=enabled,
        environment="dev" if enabled else "prod",
        use_prod_data=use_prod_data,
        active_data_dir=str(data_dir),
        active_database_url=database_url,
        active_parquet_data_dir=str(parquet_dir),
        dev_local_data_dir=str(_resolve(settings.dev_local_data_dir)),
        dev_prod_data_dir=str(_resolve(settings.dev_prod_data_dir)),
        warning=_WARNING if enabled and use_prod_data else None,
        updated_at=str(payload.get("updated_at")) if payload.get("updated_at") else None,
    )


def set_dev_data_mode(use_prod_data: bool) -> DevDataModeState:
    if not is_dev_environment():
        return get_dev_data_mode_state()
    if not use_prod_data:
        data_dir, _, _ = _active_paths(False)
        sqlite_path = _sqlite_path_for_data_dir(data_dir)
        if not sqlite_path.exists():
            raise FileNotFoundError(f"Dev local SQLite database does not exist: {sqlite_path}")
    path = _mode_file()
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "use_prod_data": bool(use_prod_data),
        "updated_at": datetime.now().isoformat(timespec="seconds"),
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return apply_dev_data_mode_to_settings()


def apply_dev_data_mode_to_settings() -> DevDataModeState:
    state = get_dev_data_mode_state()
    if not state.enabled:
        return state

    active_data_dir = Path(state.active_data_dir)
    active_parquet_dir = Path(state.active_parquet_data_dir)
    if not state.use_prod_data:
        sqlite_path = _sqlite_path_for_data_dir(active_data_dir)
        if not sqlite_path.exists():
            raise FileNotFoundError(f"Dev local SQLite database does not exist: {sqlite_path}")
    active_data_dir.mkdir(parents=True, exist_ok=True)
    active_parquet_dir.mkdir(parents=True, exist_ok=True)

    settings.gaoshou_data_dir = state.active_data_dir
    settings.database_url = state.active_database_url
    settings.parquet_data_dir = state.active_parquet_data_dir
    settings.market_data_backend = "parquet"
    return state


def dev_data_mode_payload() -> dict[str, Any]:
    return asdict(get_dev_data_mode_state())
