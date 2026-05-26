import socket
from pathlib import Path
from pydantic_settings import BaseSettings

_BACKEND_DIR = Path(__file__).resolve().parents[2]
_BASE_DIR = _BACKEND_DIR.parent
_HOSTNAME = socket.gethostname().split(".")[0]
_DATA_DIR = _BASE_DIR / "data"
_DATA_DIR.mkdir(parents=True, exist_ok=True)
_LEGACY_DATA_DIR = _BACKEND_DIR / "data"
_DB_PATH = _DATA_DIR / "gaoshou.db"
_LEGACY_DB_PATH = _LEGACY_DATA_DIR / "gaoshou.db"
if not _DB_PATH.exists() and _LEGACY_DB_PATH.exists():
    _DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    try:
        _LEGACY_DB_PATH.replace(_DB_PATH)
    except OSError:
        # If another process keeps SQLite open, keep using the legacy DB for
        # this process. The file can be moved after the process stops.
        _DB_PATH = _LEGACY_DB_PATH

_ENV_FILES = (
    str(_BASE_DIR / ".env"),
    str(_BASE_DIR / ".env.local"),
    str(_BASE_DIR / f".env.{_HOSTNAME}.local"),
    str(_BACKEND_DIR / ".env"),
    str(_BACKEND_DIR / ".env.local"),
    str(_BACKEND_DIR / f".env.{_HOSTNAME}.local"),
)


class Settings(BaseSettings):
    """应用配置"""

    # 应用信息
    app_name: str = "GaoshouPlatform"
    app_version: str = "0.1.0"
    debug: bool = True

    # 数据库配置
    database_url: str = f"sqlite+aiosqlite:///{_DB_PATH.as_posix()}"

    # ClickHouse 配置
    clickhouse_host: str = "localhost"
    clickhouse_port: int = 19000
    clickhouse_database: str = "gaoshou"
    clickhouse_user: str = "default"
    clickhouse_password: str = ""

    # Redis 缓存配置
    redis_host: str = "localhost"
    redis_port: int = 16379
    redis_db: int = 0
    redis_password: str = ""

    # 行情数据存储配置
    market_data_backend: str = "parquet"  # parquet | clickhouse
    parquet_data_dir: str = str(_DATA_DIR / "parquet")
    duckdb_path: str = ":memory:"
    clickhouse_enabled: bool = False
    parquet_minute_append_only: bool = True
    qmt_minute_clean_cache_after_sync: bool = False
    qmt_minute_compute_indicators_after_sync: bool = False
    sync_service_url: str = "http://127.0.0.1:8810"
    sync_service_port: int = 8810
    enable_sync_scheduler: bool = True
    indevs_tushare_api_key: str = ""
    indevs_tushare_base_urls: str = (
        "http://127.0.0.1:8000/tushare/pro,"
        "https://ai-tool.indevs.in/tushare/pro,"
        "https://tushare.indevs.in/tushare/pro"
    )
    indevs_tushare_rps: float = 1.0
    indevs_tushare_timeout_seconds: int = 30

    # 网格交易信号配置
    grid_trading_enable_order_submit: bool = False
    qmt_account_id: str = ""
    qmt_account_type: str = "STOCK"
    qmt_trader_path: str = ""

    # API 配置
    api_prefix: str = "/api"

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


settings = Settings()
