# backend/app/core/config.py
from pathlib import Path
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """应用配置"""

    # 应用信息
    app_name: str = "GaoshouPlatform"
    app_version: str = "0.1.0"
    debug: bool = True

    # 数据库配置
    database_url: str = "sqlite+aiosqlite:///./data/gaoshou.db"

    # ClickHouse 配置
    clickhouse_host: str = "localhost"
    clickhouse_port: int = 9000
    clickhouse_database: str = "gaoshou"
    clickhouse_user: str = "default"
    clickhouse_password: str = ""

    # API 配置
    api_prefix: str = "/api"

    @property
    def base_dir(self) -> Path:
        return Path(__file__).parent.parent.parent

    @property
    def data_dir(self) -> Path:
        data_dir = self.base_dir / "data"
        data_dir.mkdir(exist_ok=True)
        return data_dir

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


settings = Settings()
