# Phase 1: 基础框架搭建 Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** 搭建项目骨架，后端和前端都能启动运行，数据库连接正常。

**Architecture:** 模块化分层架构。后端使用 FastAPI + SQLAlchemy + SQLite，前端使用 Vue 3 + TypeScript + Element Plus + Vite。前后端分离，通过 RESTful API 通信。

**Tech Stack:** Python 3.12, FastAPI, SQLAlchemy 2.x, Pydantic 2.x, SQLite | Node.js 24, Vue 3, TypeScript, Element Plus, Vite, Axios, Pinia

---

## Task 1: 后端项目初始化

**Files:**
- Create: `backend/pyproject.toml`
- Create: `backend/requirements.txt`
- Create: `backend/app/__init__.py`
- Create: `backend/app/main.py`

**Step 1: 创建后端目录结构**

```bash
mkdir -p backend/app backend/tests
```

**Step 2: 创建 pyproject.toml**

```toml
# backend/pyproject.toml
[project]
name = "gaoshou-platform"
version = "0.1.0"
description = "量化投研平台后端服务"
readme = "README.md"
requires-python = ">=3.10"
dependencies = [
    "fastapi>=0.110.0",
    "uvicorn[standard]>=0.27.0",
    "sqlalchemy>=2.0.0",
    "pydantic>=2.0.0",
    "pydantic-settings>=2.0.0",
    "aiosqlite>=0.19.0",
    "python-multipart>=0.0.6",
]

[project.optional-dependencies]
dev = [
    "pytest>=7.0.0",
    "pytest-asyncio>=0.21.0",
    "httpx>=0.25.0",
    "ruff>=0.1.0",
]

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[tool.ruff]
line-length = 100
target-version = "py310"

[tool.ruff.lint]
select = ["E", "F", "W", "I", "N", "UP", "B"]
ignore = ["E501"]

[tool.pytest.ini_options]
asyncio_mode = "auto"
testpaths = ["tests"]
```

**Step 3: 创建 requirements.txt（用于 pip 安装）**

```txt
# backend/requirements.txt
fastapi>=0.110.0
uvicorn[standard]>=0.27.0
sqlalchemy>=2.0.0
pydantic>=2.0.0
pydantic-settings>=2.0.0
aiosqlite>=0.19.0
python-multipart>=0.0.6

# Dev dependencies
pytest>=7.0.0
pytest-asyncio>=0.21.0
httpx>=0.25.0
ruff>=0.1.0
```

**Step 4: 创建 app/__init__.py**

```python
# backend/app/__init__.py
"""GaoshouPlatform 量化投研平台后端服务"""
```

**Step 5: 创建最小化的 main.py**

```python
# backend/app/main.py
from fastapi import FastAPI

app = FastAPI(
    title="GaoshouPlatform API",
    description="量化投研平台后端服务",
    version="0.1.0",
)


@app.get("/health")
async def health_check():
    """健康检查接口"""
    return {"status": "ok", "version": "0.1.0"}


@app.get("/")
async def root():
    """根路径"""
    return {"message": "Welcome to GaoshouPlatform API"}
```

**Step 6: 创建 Python 虚拟环境并安装依赖**

```bash
cd backend
python -m venv .venv
# Windows
.\.venv\Scripts\activate
# Linux/Mac
# source .venv/bin/activate
pip install -r requirements.txt
```

**Step 7: 启动后端服务验证**

```bash
cd backend
uvicorn app.main:app --reload --port 8000
```

访问 http://localhost:8000/docs 应该能看到 Swagger UI 文档页面。

**Step 8: 提交**

```bash
git add backend/
git commit -m "$(cat <<'EOF'
feat(backend): initialize FastAPI project

- Add pyproject.toml and requirements.txt
- Create minimal FastAPI app with health check
- Configure ruff and pytest

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>
EOF
)"
```

---

## Task 2: 数据库连接配置

**Files:**
- Create: `backend/app/core/__init__.py`
- Create: `backend/app/core/config.py`
- Create: `backend/app/db/__init__.py`
- Create: `backend/app/db/sqlite.py`
- Create: `backend/app/db/models/__init__.py`
- Create: `backend/app/db/models/base.py`
- Modify: `backend/app/main.py`

**Step 1: 创建目录结构**

```bash
mkdir -p backend/app/core backend/app/db/models
```

**Step 2: 创建 core/__init__.py**

```python
# backend/app/core/__init__.py
"""核心模块"""
```

**Step 3: 创建配置管理 config.py**

```python
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
```

**Step 4: 创建 db/__init__.py**

```python
# backend/app/db/__init__.py
"""数据库模块"""
from .sqlite import get_async_session, init_db

__all__ = ["get_async_session", "init_db"]
```

**Step 5: 创建 SQLite 数据库连接 sqlite.py**

```python
# backend/app/db/sqlite.py
from contextlib import asynccontextmanager
from typing import AsyncGenerator

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.core.config import settings

engine = create_async_engine(
    settings.database_url,
    echo=settings.debug,
    future=True,
)

async_session_factory = async_sessionmaker(
    engine,
    class_=AsyncSession,
    expire_on_commit=False,
)


async def init_db():
    """初始化数据库（创建所有表）"""
    from app.db.models.base import Base

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


@asynccontextmanager
async def get_async_session() -> AsyncGenerator[AsyncSession, None]:
    """获取数据库会话的上下文管理器"""
    async with async_session_factory() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
```

**Step 6: 创建 models/__init__.py**

```python
# backend/app/db/models/__init__.py
"""数据模型"""
from .base import Base

__all__ = ["Base"]
```

**Step 7: 创建基础模型 base.py**

```python
# backend/app/db/models/base.py
from datetime import datetime

from sqlalchemy import DateTime, func
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    """SQLAlchemy 声明式基类"""

    pass


class TimestampMixin:
    """时间戳混入类"""

    created_at: Mapped[datetime] = mapped_column(
        DateTime, default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=func.now(), onupdate=func.now(), nullable=False
    )
```

**Step 8: 更新 main.py 添加数据库初始化**

```python
# backend/app/main.py
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.db import init_db


@asynccontextmanager
async def lifespan(app: FastAPI):
    """应用生命周期管理"""
    # 启动时初始化数据库
    await init_db()
    yield
    # 关闭时清理资源


app = FastAPI(
    title="GaoshouPlatform API",
    description="量化投研平台后端服务",
    version="0.1.0",
    lifespan=lifespan,
)


@app.get("/health")
async def health_check():
    """健康检查接口"""
    return {"status": "ok", "version": "0.1.0"}


@app.get("/")
async def root():
    """根路径"""
    return {"message": "Welcome to GaoshouPlatform API"}
```

**Step 9: 创建 data 目录并添加 .gitkeep**

```bash
mkdir -p backend/data
touch backend/data/.gitkeep
```

**Step 10: 创建 .gitignore**

```gitignore
# backend/.gitignore
# Python
__pycache__/
*.py[cod]
*$py.class
.venv/
venv/
ENV/

# Database
*.db
data/*.db

# IDE
.idea/
.vscode/
*.swp
*.swo

# Testing
.pytest_cache/
.coverage
htmlcov/

# Build
dist/
build/
*.egg-info/
```

**Step 11: 启动验证数据库初始化**

```bash
cd backend
uvicorn app.main:app --reload --port 8000
```

启动后应该看到没有错误，data 目录下会自动创建 gaoshou.db 文件。

**Step 12: 提交**

```bash
git add backend/
git commit -m "$(cat <<'EOF'
feat(backend): add SQLite database connection

- Create config management with pydantic-settings
- Add async SQLite engine and session factory
- Create base model with timestamp mixin
- Integrate database init in app lifespan

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>
EOF
)"
```

---

## Task 3: 核心数据模型定义

**Files:**
- Create: `backend/app/db/models/stock.py`
- Create: `backend/app/db/models/strategy.py`
- Create: `backend/app/db/models/factor.py`
- Modify: `backend/app/db/models/__init__.py`

**Step 1: 创建股票模型 stock.py**

```python
# backend/app/db/models/stock.py
from datetime import date, datetime
from decimal import Decimal

from sqlalchemy import Date, DateTime, Index, Numeric, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base, TimestampMixin


class Stock(Base, TimestampMixin):
    """股票基础信息表"""

    __tablename__ = "stocks"

    symbol: Mapped[str] = mapped_column(String(20), primary_key=True, comment="股票代码")
    name: Mapped[str | None] = mapped_column(String(50), comment="股票名称")
    exchange: Mapped[str | None] = mapped_column(String(10), comment="交易所")
    industry: Mapped[str | None] = mapped_column(String(50), comment="所属行业")
    list_date: Mapped[date | None] = mapped_column(Date, comment="上市日期")


class KlineDaily(Base):
    """日K线数据表"""

    __tablename__ = "klines_daily"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    symbol: Mapped[str] = mapped_column(String(20), nullable=False, index=True, comment="股票代码")
    trade_date: Mapped[date] = mapped_column(Date, nullable=False, index=True, comment="交易日期")
    open: Mapped[Decimal | None] = mapped_column(Numeric(10, 3), comment="开盘价")
    high: Mapped[Decimal | None] = mapped_column(Numeric(10, 3), comment="最高价")
    low: Mapped[Decimal | None] = mapped_column(Numeric(10, 3), comment="最低价")
    close: Mapped[Decimal | None] = mapped_column(Numeric(10, 3), comment="收盘价")
    volume: Mapped[int | None] = mapped_column(comment="成交量")
    amount: Mapped[Decimal | None] = mapped_column(Numeric(18, 2), comment="成交额")
    turnover_rate: Mapped[Decimal | None] = mapped_column(Numeric(8, 4), comment="换手率")
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.now, nullable=False)

    __table_args__ = (
        Index("idx_klines_daily_symbol_date", "symbol", "trade_date", unique=True),
    )


class KlineMinute(Base):
    """分钟K线数据表"""

    __tablename__ = "klines_minute"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    symbol: Mapped[str] = mapped_column(String(20), nullable=False, index=True, comment="股票代码")
    datetime: Mapped[datetime] = mapped_column(DateTime, nullable=False, index=True, comment="分钟时间戳")
    open: Mapped[Decimal | None] = mapped_column(Numeric(10, 3), comment="开盘价")
    high: Mapped[Decimal | None] = mapped_column(Numeric(10, 3), comment="最高价")
    low: Mapped[Decimal | None] = mapped_column(Numeric(10, 3), comment="最低价")
    close: Mapped[Decimal | None] = mapped_column(Numeric(10, 3), comment="收盘价")
    volume: Mapped[int | None] = mapped_column(comment="成交量")
    amount: Mapped[Decimal | None] = mapped_column(Numeric(18, 2), comment="成交额")
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.now, nullable=False)

    __table_args__ = (
        Index("idx_klines_minute_symbol_datetime", "symbol", "datetime", unique=True),
    )
```

**Step 2: 创建策略模型 strategy.py**

```python
# backend/app/db/models/strategy.py
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import Date, DateTime, ForeignKey, Index, Numeric, String, Text
from sqlalchemy.dialects.sqlite import JSON
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .base import Base, TimestampMixin


class Strategy(Base, TimestampMixin):
    """策略表"""

    __tablename__ = "strategies"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(100), nullable=False, comment="策略名称")
    code: Mapped[str] = mapped_column(Text, nullable=False, comment="策略代码")
    parameters: Mapped[dict[str, Any] | None] = mapped_column(JSON, comment="策略参数")
    description: Mapped[str | None] = mapped_column(Text, comment="策略描述")


class Backtest(Base, TimestampMixin):
    """回测记录表"""

    __tablename__ = "backtests"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    strategy_id: Mapped[int] = mapped_column(ForeignKey("strategies.id"), nullable=False, comment="关联策略")
    status: Mapped[str] = mapped_column(String(20), default="pending", comment="状态: pending/running/completed/failed")
    start_date: Mapped[date] = mapped_column(Date, nullable=False, comment="回测起始日期")
    end_date: Mapped[date] = mapped_column(Date, nullable=False, comment="回测结束日期")
    initial_capital: Mapped[Decimal | None] = mapped_column(Numeric(15, 2), comment="初始资金")
    parameters: Mapped[dict[str, Any] | None] = mapped_column(JSON, comment="回测参数")
    result: Mapped[dict[str, Any] | None] = mapped_column(JSON, comment="回测结果摘要")
    report_path: Mapped[str | None] = mapped_column(String(255), comment="详细报告路径")

    strategy: Mapped["Strategy"] = relationship()


class Order(Base, TimestampMixin):
    """交易订单表"""

    __tablename__ = "orders"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    order_id: Mapped[str | None] = mapped_column(String(50), unique=True, comment="交易所订单号")
    symbol: Mapped[str] = mapped_column(String(20), nullable=False, comment="股票代码")
    direction: Mapped[str] = mapped_column(String(10), nullable=False, comment="方向: buy/sell")
    order_type: Mapped[str] = mapped_column(String(10), nullable=False, comment="类型: limit/market")
    price: Mapped[Decimal | None] = mapped_column(Numeric(10, 3), comment="委托价格")
    quantity: Mapped[int] = mapped_column(nullable=False, comment="委托数量")
    filled_quantity: Mapped[int] = mapped_column(default=0, comment="成交数量")
    status: Mapped[str] = mapped_column(String(20), nullable=False, comment="状态: pending/filled/cancelled")
    strategy_id: Mapped[int | None] = mapped_column(ForeignKey("strategies.id"), comment="关联策略")
    signal_time: Mapped[datetime | None] = mapped_column(DateTime, comment="信号产生时间")
    order_time: Mapped[datetime | None] = mapped_column(DateTime, comment="下单时间")


class Trade(Base, TimestampMixin):
    """成交记录表"""

    __tablename__ = "trades"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    trade_id: Mapped[str | None] = mapped_column(String(50), unique=True, comment="成交编号")
    order_id: Mapped[str] = mapped_column(String(50), nullable=False, index=True, comment="关联订单")
    symbol: Mapped[str] = mapped_column(String(20), nullable=False, comment="股票代码")
    direction: Mapped[str] = mapped_column(String(10), nullable=False, comment="方向: buy/sell")
    price: Mapped[Decimal] = mapped_column(Numeric(10, 3), nullable=False, comment="成交价格")
    quantity: Mapped[int] = mapped_column(nullable=False, comment="成交数量")
    commission: Mapped[Decimal | None] = mapped_column(Numeric(10, 2), comment="手续费")
    trade_time: Mapped[datetime] = mapped_column(DateTime, nullable=False, comment="成交时间")
```

**Step 3: 创建因子模型 factor.py**

```python
# backend/app/db/models/factor.py
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import Date, DateTime, ForeignKey, Numeric, String, Text
from sqlalchemy.dialects.sqlite import JSON
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .base import Base, TimestampMixin


class Factor(Base, TimestampMixin):
    """因子定义表"""

    __tablename__ = "factors"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(50), unique=True, nullable=False, comment="因子名称")
    category: Mapped[str | None] = mapped_column(String(50), comment="因子分类")
    source: Mapped[str | None] = mapped_column(String(20), comment="来源: qmt/custom")
    code: Mapped[str | None] = mapped_column(Text, comment="因子计算代码")
    parameters: Mapped[dict[str, Any] | None] = mapped_column(JSON, comment="默认参数")
    description: Mapped[str | None] = mapped_column(Text, comment="因子描述")


class FactorAnalysis(Base, TimestampMixin):
    """因子分析结果表"""

    __tablename__ = "factor_analysis"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    factor_id: Mapped[int] = mapped_column(ForeignKey("factors.id"), nullable=False, comment="关联因子")
    start_date: Mapped[date] = mapped_column(Date, nullable=False, comment="分析起始日期")
    end_date: Mapped[date] = mapped_column(Date, nullable=False, comment="分析结束日期")
    ic_mean: Mapped[Decimal | None] = mapped_column(Numeric(6, 4), comment="IC均值")
    ic_std: Mapped[Decimal | None] = mapped_column(Numeric(6, 4), comment="IC标准差")
    ir: Mapped[Decimal | None] = mapped_column(Numeric(6, 4), comment="信息比率")
    turnover_rate: Mapped[Decimal | None] = mapped_column(Numeric(6, 4), comment="换手率")
    details: Mapped[dict[str, Any] | None] = mapped_column(JSON, comment="详细分析结果")

    factor: Mapped["Factor"] = relationship()
```

**Step 4: 更新 models/__init__.py 导出所有模型**

```python
# backend/app/db/models/__init__.py
"""数据模型"""
from .base import Base, TimestampMixin
from .factor import Factor, FactorAnalysis
from .stock import KlineDaily, KlineMinute, Stock
from .strategy import Backtest, Order, Strategy, Trade

__all__ = [
    "Base",
    "TimestampMixin",
    "Stock",
    "KlineDaily",
    "KlineMinute",
    "Strategy",
    "Backtest",
    "Order",
    "Trade",
    "Factor",
    "FactorAnalysis",
]
```

**Step 5: 验证模型定义正确**

```bash
cd backend
python -c "from app.db.models import *; print('Models imported successfully')"
```

**Step 6: 提交**

```bash
git add backend/
git commit -m "$(cat <<'EOF'
feat(backend): define core data models

- Add Stock, KlineDaily, KlineMinute models
- Add Strategy, Backtest, Order, Trade models
- Add Factor, FactorAnalysis models
- Export all models from __init__.py

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>
EOF
)"
```

---

## Task 4: API 路由框架搭建

**Files:**
- Create: `backend/app/api/__init__.py`
- Create: `backend/app/api/router.py`
- Create: `backend/app/api/system.py`
- Modify: `backend/app/main.py`

**Step 1: 创建目录**

```bash
mkdir -p backend/app/api
```

**Step 2: 创建 api/__init__.py**

```python
# backend/app/api/__init__.py
"""API 路由模块"""
from .router import api_router

__all__ = ["api_router"]
```

**Step 3: 创建路由聚合器 router.py**

```python
# backend/app/api/router.py
from fastapi import APIRouter

from .system import router as system_router

api_router = APIRouter()

# 注册各模块路由
api_router.include_router(system_router, prefix="/system", tags=["系统"])
```

**Step 4: 创建系统状态 API system.py**

```python
# backend/app/api/system.py
from fastapi import APIRouter

router = APIRouter()


@router.get("/status")
async def get_system_status():
    """获取系统状态"""
    return {
        "status": "running",
        "database": "connected",
    }


@router.get("/health")
async def health_check():
    """健康检查"""
    return {"status": "healthy"}
```

**Step 5: 更新 main.py 集成路由**

```python
# backend/app/main.py
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.api import api_router
from app.db import init_db


@asynccontextmanager
async def lifespan(app: FastAPI):
    """应用生命周期管理"""
    await init_db()
    yield


app = FastAPI(
    title="GaoshouPlatform API",
    description="量化投研平台后端服务",
    version="0.1.0",
    lifespan=lifespan,
)

# 注册 API 路由
app.include_router(api_router, prefix="/api")


@app.get("/health")
async def health_check():
    """健康检查接口（根路径）"""
    return {"status": "ok", "version": "0.1.0"}


@app.get("/")
async def root():
    """根路径"""
    return {"message": "Welcome to GaoshouPlatform API"}
```

**Step 6: 启动验证 API**

```bash
cd backend
uvicorn app.main:app --reload --port 8000
```

访问 http://localhost:8000/api/system/status 应该返回系统状态。

**Step 7: 提交**

```bash
git add backend/
git commit -m "$(cat <<'EOF'
feat(backend): setup API router framework

- Create API router aggregator
- Add system status endpoints
- Integrate router in main app

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>
EOF
)"
```

---

## Task 5: 前端项目初始化

**Files:**
- Create: `frontend/` (Vue 3 + Vite 项目)

**Step 1: 使用 Vite 创建 Vue 3 项目**

```bash
npm create vite@latest frontend -- --template vue-ts
```

**Step 2: 进入前端目录并安装依赖**

```bash
cd frontend
npm install
```

**Step 3: 安装额外依赖**

```bash
npm install element-plus axios pinia vue-router@4
npm install -D sass @types/node
```

**Step 4: 验证前端项目启动**

```bash
cd frontend
npm run dev
```

访问 http://localhost:5173 应该能看到 Vue 初始页面。

**Step 5: 提交**

```bash
git add frontend/
git commit -m "$(cat <<'EOF'
feat(frontend): initialize Vue 3 project with Vite

- Create Vue 3 + TypeScript project
- Add Element Plus, Axios, Pinia, Vue Router
- Configure development environment

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>
EOF
)"
```

---

## Task 6: 前端路由配置

**Files:**
- Create: `frontend/src/router/index.ts`
- Modify: `frontend/src/main.ts`

**Step 1: 创建路由目录**

```bash
mkdir -p frontend/src/router frontend/src/views
```

**Step 2: 创建路由配置**

```typescript
// frontend/src/router/index.ts
import { createRouter, createWebHistory } from 'vue-router'

const router = createRouter({
  history: createWebHistory(),
  routes: [
    {
      path: '/',
      redirect: '/data',
    },
    {
      path: '/data',
      name: 'DataManage',
      component: () => import('@/views/DataManage/index.vue'),
      meta: { title: '数据管理' },
    },
    {
      path: '/factor',
      name: 'FactorResearch',
      component: () => import('@/views/FactorResearch/index.vue'),
      meta: { title: '因子研究' },
    },
    {
      path: '/backtest',
      name: 'StrategyBacktest',
      component: () => import('@/views/StrategyBacktest/index.vue'),
      meta: { title: '策略回测' },
    },
    {
      path: '/trade',
      name: 'LiveTrading',
      component: () => import('@/views/LiveTrading/index.vue'),
      meta: { title: '实盘交易' },
    },
    {
      path: '/monitor',
      name: 'SystemMonitor',
      component: () => import('@/views/SystemMonitor/index.vue'),
      meta: { title: '系统监控' },
    },
  ],
})

export default router
```

**Step 3: 更新 vite.config.ts 添加路径别名**

```typescript
// frontend/vite.config.ts
import { defineConfig } from 'vite'
import vue from '@vitejs/plugin-vue'
import { fileURLToPath, URL } from 'node:url'

export default defineConfig({
  plugins: [vue()],
  resolve: {
    alias: {
      '@': fileURLToPath(new URL('./src', import.meta.url))
    }
  },
  server: {
    port: 5173,
    proxy: {
      '/api': {
        target: 'http://localhost:8000',
        changeOrigin: true,
      },
    },
  },
})
```

**Step 4: 更新 main.ts 集成路由和 Pinia**

```typescript
// frontend/src/main.ts
import { createApp } from 'vue'
import { createPinia } from 'pinia'
import ElementPlus from 'element-plus'
import 'element-plus/dist/index.css'

import App from './App.vue'
import router from './router'

const app = createApp(App)

app.use(createPinia())
app.use(router)
app.use(ElementPlus)

app.mount('#app')
```

**Step 5: 创建占位页面组件**

```bash
mkdir -p frontend/src/views/DataManage \
         frontend/src/views/FactorResearch \
         frontend/src/views/StrategyBacktest \
         frontend/src/views/LiveTrading \
         frontend/src/views/SystemMonitor
```

创建 5 个占位页面，例如：

```vue
<!-- frontend/src/views/DataManage/index.vue -->
<template>
  <div class="page-container">
    <h2>数据管理</h2>
    <p>正在开发中...</p>
  </div>
</template>

<script setup lang="ts">
</script>

<style scoped>
.page-container {
  padding: 20px;
}
</style>
```

（其他 4 个页面同理，只改标题）

**Step 6: 验证路由**

```bash
cd frontend
npm run dev
```

访问不同路由应该显示对应页面。

**Step 7: 提交**

```bash
git add frontend/
git commit -m "$(cat <<'EOF'
feat(frontend): setup Vue Router with page structure

- Configure router with 5 main pages
- Add path alias in vite config
- Create placeholder page components
- Setup API proxy to backend

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>
EOF
)"
```

---

## Task 7: 前端 API 调用封装

**Files:**
- Create: `frontend/src/api/request.ts`
- Create: `frontend/src/api/system.ts`
- Create: `frontend/src/api/index.ts`

**Step 1: 创建 API 目录**

```bash
mkdir -p frontend/src/api
```

**Step 2: 创建 Axios 封装 request.ts**

```typescript
// frontend/src/api/request.ts
import axios, { type AxiosInstance, type AxiosResponse } from 'axios'
import { ElMessage } from 'element-plus'

const request: AxiosInstance = axios.create({
  baseURL: '/api',
  timeout: 10000,
  headers: {
    'Content-Type': 'application/json',
  },
})

// 响应拦截器
request.interceptors.response.use(
  (response: AxiosResponse) => {
    return response.data
  },
  (error) => {
    const message = error.response?.data?.detail || error.message || '请求失败'
    ElMessage.error(message)
    return Promise.reject(error)
  }
)

export default request
```

**Step 3: 创建系统 API system.ts**

```typescript
// frontend/src/api/system.ts
import request from './request'

export interface SystemStatus {
  status: string
  database: string
}

export const systemApi = {
  getStatus: () => request.get<SystemStatus>('/system/status'),

  healthCheck: () => request.get<{ status: string }>('/system/health'),
}
```

**Step 4: 创建 API 导出 index.ts**

```typescript
// frontend/src/api/index.ts
export * from './system'
export { default as request } from './request'
```

**Step 5: 提交**

```bash
git add frontend/
git commit -m "$(cat <<'EOF'
feat(frontend): add API request wrapper

- Create Axios instance with interceptors
- Add system API module
- Configure error handling

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>
EOF
)"
```

---

## Task 8: 前端基础布局

**Files:**
- Create: `frontend/src/layouts/MainLayout.vue`
- Modify: `frontend/src/App.vue`

**Step 1: 创建布局目录**

```bash
mkdir -p frontend/src/layouts
```

**Step 2: 创建主布局组件**

```vue
<!-- frontend/src/layouts/MainLayout.vue -->
<template>
  <el-container class="main-layout">
    <!-- 侧边栏 -->
    <el-aside width="200px" class="sidebar">
      <div class="logo">
        <h1>高手平台</h1>
      </div>
      <el-menu
        :default-active="activeMenu"
        router
        class="sidebar-menu"
      >
        <el-menu-item index="/data">
          <el-icon><DataLine /></el-icon>
          <span>数据管理</span>
        </el-menu-item>
        <el-menu-item index="/factor">
          <el-icon><TrendCharts /></el-icon>
          <span>因子研究</span>
        </el-menu-item>
        <el-menu-item index="/backtest">
          <el-icon><DataAnalysis /></el-icon>
          <span>策略回测</span>
        </el-menu-item>
        <el-menu-item index="/trade">
          <el-icon><ShoppingCart /></el-icon>
          <span>实盘交易</span>
        </el-menu-item>
        <el-menu-item index="/monitor">
          <el-icon><Monitor /></el-icon>
          <span>系统监控</span>
        </el-menu-item>
      </el-menu>
    </el-aside>

    <!-- 主内容区 -->
    <el-container>
      <el-header class="header">
        <div class="header-title">{{ pageTitle }}</div>
      </el-header>
      <el-main class="main-content">
        <router-view />
      </el-main>
    </el-container>
  </el-container>
</template>

<script setup lang="ts">
import { computed } from 'vue'
import { useRoute } from 'vue-router'
import { DataLine, TrendCharts, DataAnalysis, ShoppingCart, Monitor } from '@element-plus/icons-vue'

const route = useRoute()

const activeMenu = computed(() => route.path)

const pageTitle = computed(() => {
  return (route.meta?.title as string) || '首页'
})
</script>

<style scoped>
.main-layout {
  height: 100vh;
}

.sidebar {
  background-color: #304156;
  color: #fff;
}

.logo {
  height: 60px;
  display: flex;
  align-items: center;
  justify-content: center;
  background-color: #263445;
}

.logo h1 {
  font-size: 18px;
  color: #fff;
  margin: 0;
}

.sidebar-menu {
  border-right: none;
  background-color: #304156;
}

.sidebar-menu .el-menu-item {
  color: #bfcbd9;
}

.sidebar-menu .el-menu-item:hover,
.sidebar-menu .el-menu-item.is-active {
  background-color: #263445;
  color: #409eff;
}

.header {
  background-color: #fff;
  border-bottom: 1px solid #e6e6e6;
  display: flex;
  align-items: center;
  padding: 0 20px;
}

.header-title {
  font-size: 18px;
  font-weight: 500;
}

.main-content {
  background-color: #f5f7fa;
  padding: 20px;
}
</style>
```

**Step 3: 更新 App.vue 使用布局**

```vue
<!-- frontend/src/App.vue -->
<template>
  <MainLayout />
</template>

<script setup lang="ts">
import MainLayout from '@/layouts/MainLayout.vue'
</script>

<style>
* {
  margin: 0;
  padding: 0;
  box-sizing: border-box;
}

html, body, #app {
  height: 100%;
  font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
}
</style>
```

**Step 4: 验证布局**

```bash
cd frontend
npm run dev
```

应该能看到左侧导航栏和顶部标题栏。

**Step 5: 提交**

```bash
git add frontend/
git commit -m "$(cat <<'EOF'
feat(frontend): implement main layout with sidebar

- Create MainLayout component with sidebar navigation
- Add menu items for all pages
- Apply Element Plus styling
- Update App.vue to use layout

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>
EOF
)"
```

---

## Task 9: 前后端联调验证

**Step 1: 启动后端**

```bash
cd backend
uvicorn app.main:app --reload --port 8000
```

**Step 2: 启动前端（新终端）**

```bash
cd frontend
npm run dev
```

**Step 3: 验证 API 代理**

打开浏览器控制台，在前端页面执行：

```javascript
fetch('/api/system/status')
  .then(r => r.json())
  .then(console.log)
```

应该返回 `{status: "running", database: "connected"}`

**Step 4: 最终提交**

```bash
git add .
git commit -m "$(cat <<'EOF'
chore: verify frontend-backend integration

Backend running on port 8000
Frontend running on port 5173 with API proxy
All routes and API calls working correctly

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>
EOF
)"
```

---

## 完成标准

- [ ] 后端 FastAPI 服务可启动，访问 `/docs` 显示 API 文档
- [ ] 数据库 SQLite 正常连接，表结构创建成功
- [ ] 前端 Vue 3 项目可启动，显示布局和导航
- [ ] 前端可正常调用后端 API（通过代理）
- [ ] 所有代码已提交到 Git 仓库

---

*计划版本: 1.0*
*创建日期: 2026-04-12*
