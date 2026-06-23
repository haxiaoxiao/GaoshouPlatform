import os
import socket
from pathlib import Path

from pydantic_settings import BaseSettings

_BACKEND_DIR = Path(__file__).resolve().parents[2]
_BASE_DIR = _BACKEND_DIR.parent
_HOSTNAME = socket.gethostname().split(".")[0]

_ENV_FILES = (
    str(_BASE_DIR / ".env"),
    str(_BASE_DIR / ".env.local"),
    str(_BASE_DIR / f".env.{_HOSTNAME}.local"),
    str(_BACKEND_DIR / ".env"),
    str(_BACKEND_DIR / ".env.local"),
    str(_BACKEND_DIR / f".env.{_HOSTNAME}.local"),
)


def _env_file_value(name: str) -> str | None:
    value: str | None = None
    for env_file in _ENV_FILES:
        path = Path(env_file)
        if not path.exists():
            continue
        for line in path.read_text(encoding="utf-8").splitlines():
            text = line.strip()
            if not text or text.startswith("#") or "=" not in text:
                continue
            key, raw = text.split("=", 1)
            if key.strip() == name:
                value = raw.strip().strip('"').strip("'")
    return value


_DEFAULT_SYNCED_DATA_DIR = _BASE_DIR.parent / "data" / "BaiduSyncdisk"
_DEFAULT_DATA_DIR = _DEFAULT_SYNCED_DATA_DIR if _DEFAULT_SYNCED_DATA_DIR.exists() else (_BASE_DIR.parent / "Data")
_DATA_DIR = Path(os.getenv("GAOSHOU_DATA_DIR") or _env_file_value("GAOSHOU_DATA_DIR") or _DEFAULT_DATA_DIR).expanduser()
_DATA_DIR.mkdir(parents=True, exist_ok=True)
_DB_PATH = _DATA_DIR / "gaoshou.db"
_LEGACY_DB_PATHS = (
    _BASE_DIR / "data" / "gaoshou.db",
    _BACKEND_DIR / "data" / "gaoshou.db",
)
_LEGACY_DB_PATH = next((path for path in _LEGACY_DB_PATHS if path.exists()), None)
if not _DB_PATH.exists() and _LEGACY_DB_PATH is not None:
    _DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    try:
        _LEGACY_DB_PATH.replace(_DB_PATH)
    except OSError:
        # If another process keeps SQLite open, keep using the legacy DB for
        # this process. The file can be moved after the process stops.
        _DB_PATH = _LEGACY_DB_PATH


class Settings(BaseSettings):
    """应用配置"""

    # 应用信息
    app_name: str = "GaoshouPlatform"
    app_version: str = "0.1.0"
    debug: bool = True
    gaoshou_data_dir: str = str(_DATA_DIR)

    # 数据库配置
    database_url: str = f"sqlite+aiosqlite:///{_DB_PATH.as_posix()}"

    # Redis 缓存配置
    redis_host: str = "localhost"
    redis_port: int = 16379
    redis_db: int = 0
    redis_password: str = ""

    # 行情数据存储配置
    market_data_backend: str = "parquet"
    parquet_data_dir: str = str(_DATA_DIR / "parquet")
    duckdb_path: str = ":memory:"
    dev_local_data_dir: str = str(_DATA_DIR)
    dev_prod_data_dir: str = str(_DATA_DIR)
    dev_data_mode_file: str = str(_BASE_DIR / ".runtime" / "dev_data_mode.json")
    parquet_minute_append_only: bool = True
    qmt_daily_clean_cache_after_sync: bool = False
    qmt_daily_compute_indicators_after_sync: bool = False
    qmt_financial_compute_indicators_after_sync: bool = False
    qmt_minute_clean_cache_after_sync: bool = False
    qmt_minute_compute_indicators_after_sync: bool = False
    sync_service_url: str = "http://127.0.0.1:18810"
    sync_service_port: int = 18810
    enable_sync_scheduler: bool = True
    indevs_tushare_api_key: str = ""
    indevs_tushare_base_urls: str = (
        "http://127.0.0.1:8000/tushare/pro,"
        "https://ai-tool.indevs.in/tushare/pro,"
        "https://tushare.indevs.in/tushare/pro"
    )
    indevs_tushare_rps: float = 1.0
    indevs_tushare_timeout_seconds: int = 30

    # 模拟 / 实盘交易配置
    live_trading_enable_order_submit: bool = False
    live_trading_auto_execute_enabled: bool = False
    live_trading_default_profile: str = "tsmf_cashaware_stable"
    live_trading_seed_strategy_ids: str = "62,63"
    qmt_account_id: str = ""
    qmt_account_type: str = "STOCK"
    qmt_trader_path: str = ""
    xueqiu_chrome_path: str = r"C:\Program Files\Google\Chrome\Application\chrome.exe"
    xueqiu_debug_port: int = 9222
    xueqiu_user_data_dir: str = str(_DATA_DIR / "sentiment" / "xueqiu-profile")
    xueqiu_spyder_dir: str = r"E:\Projects\xueqiu-spyder"
    xueqiu_cookie: str = ""
    flocktrader_dir: str = r"E:\Projects\flocktrader"
    nga_cookie: str = ""
    nga_data_dir: str = str(_DATA_DIR / "sentiment" / "NGAdata")
    nga_board_fid: int = 706
    wechat_sogou_queries: str = "开盘啦 创始人 股票,开盘啦 A股 股票,龙虎榜 A股 游资,短线 A股 股票,涨停板 A股"
    wechat_sogou_cookie: str = ""

    # API 配置
    api_prefix: str = "/api"
    backend_port: int = 18800
    frontend_port: int = 13500

    @property
    def base_dir(self) -> Path:
        return _BASE_DIR

    @property
    def data_dir(self) -> Path:
        return self.sqlite_db_path.parent

    @property
    def sqlite_db_path(self) -> Path:
        """Return the SQLite file configured by DATABASE_URL."""
        for prefix in ("sqlite+aiosqlite:///", "sqlite:///"):
            if self.database_url.startswith(prefix):
                path = Path(self.database_url[len(prefix):])
                return path if path.is_absolute() else (_BASE_DIR / path).resolve()
        return _DB_PATH

    class Config:
        env_file = _ENV_FILES
        env_file_encoding = "utf-8"
        extra = "ignore"


settings = Settings()
