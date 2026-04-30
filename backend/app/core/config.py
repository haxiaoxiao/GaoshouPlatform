# backend/app/core/config.py
from pathlib import Path
from pydantic_settings import BaseSettings

_BASE_DIR = Path(__file__).resolve().parent.parent.parent
_DATA_DIR = _BASE_DIR / "data"
_DATA_DIR.mkdir(exist_ok=True)
_DB_PATH = _DATA_DIR / "gaoshou.db"


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
