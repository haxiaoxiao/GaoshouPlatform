from pathlib import Path
from pydantic_settings import BaseSettings

_BACKEND_DIR = Path(__file__).resolve().parents[2]
_BASE_DIR = _BACKEND_DIR.parent
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


class Settings(BaseSettings):
    """应用配置"""

    # 应用信息
    app_name: str = "GaoshouPlatform"
    app_version: str = "0.1.0"
    debug: bool = True

    # 数据库配置
    database_url: str = f"sqlite+aiosqlite:///{_DB_PATH}"

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
    parquet_data_dir: str = "E:/Projects/GaoshouPlatform/data/parquet"
    duckdb_path: str = ":memory:"
    clickhouse_enabled: bool = False

    # API 配置
    api_prefix: str = "/api"

    @property
    def base_dir(self) -> Path:
        return _BASE_DIR

    @property
    def data_dir(self) -> Path:
        return _DATA_DIR

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


settings = Settings()
