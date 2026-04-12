# Phase 2: 数据模块设计文档

> 创建日期: 2026-04-12
> 状态: 已确认，准备实施

## 一、需求概述

### 1.1 功能目标

**能获取、存储、展示数据**

- miniQMT 网关封装
- 数据同步服务（手动 + 定时）
- 数据查询 API
- 股票列表页面（搜索 + 行业筛选 + 自选股分组）
- K线图表展示
- 数据同步管理

### 1.2 需求明细

| 需求项 | 选择 |
|-------|------|
| 数据源 SDK | xtquant (miniQMT) |
| 同步方式 | 手动触发 + 定时自动 |
| 同步内容 | 用户可配置选择 |
| 历史范围 | 用户可指定时间范围 |
| 进度展示 | 进度条 + 统计信息 |
| K线图表 | 先做基础K线图 |
| 股票列表 | 列表 + 搜索 + 行业筛选 + 自选股分组 |
| 定时配置 | Cron 表达式 |
| 失败处理 | 用户可配置策略 |

---

## 二、架构设计

### 2.1 分层架构

```
┌─────────────────────────────────────────────────────────┐
│                      前端页面                            │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐     │
│  │ 股票列表页面 │  │ K线图表页面  │  │ 同步管理页面 │     │
│  └─────────────┘  └─────────────┘  └─────────────┘     │
├─────────────────────────────────────────────────────────┤
│                      API 层                             │
│  GET/POST /api/data/stocks     - 股票列表              │
│  GET      /api/data/klines     - K线查询               │
│  POST     /api/data/sync       - 触发同步              │
│  GET      /api/data/sync/status - 同步状态             │
│  GET/POST /api/data/tasks      - 定时任务管理          │
├─────────────────────────────────────────────────────────┤
│                      服务层                             │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐     │
│  │ DataService │  │ SyncService │  │TaskScheduler│     │
│  │  数据查询    │  │  数据同步    │  │  定时任务    │     │
│  └─────────────┘  └─────────────┘  └─────────────┘     │
├─────────────────────────────────────────────────────────┤
│                      网关层                             │
│  ┌─────────────────────────────────────────────────┐   │
│  │              QMTGateway (xtquant 封装)           │   │
│  │  - 股票列表获取                                   │   │
│  │  - K线数据获取                                    │   │
│  │  - 连接状态管理                                   │   │
│  └─────────────────────────────────────────────────┘   │
├─────────────────────────────────────────────────────────┤
│                      存储层                             │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐     │
│  │  Stock表    │  │KlineDaily表 │  │KlineMinute表│     │
│  │  自选股分组  │  │  同步任务表  │  │  同步日志表  │     │
│  └─────────────┘  └─────────────┘  └─────────────┘     │
└─────────────────────────────────────────────────────────┘
```

---

## 三、数据表设计

### 3.1 自选股分组表

```sql
CREATE TABLE watchlist_groups (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name VARCHAR(50) NOT NULL,           -- 分组名称
    description TEXT,                    -- 分组描述
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### 3.2 自选股关联表

```sql
CREATE TABLE watchlist_stocks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    group_id INTEGER NOT NULL,           -- 分组ID
    symbol VARCHAR(20) NOT NULL,         -- 股票代码
    added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (group_id) REFERENCES watchlist_groups(id),
    UNIQUE(group_id, symbol)             -- 同一分组不重复添加
);
```

### 3.3 同步任务配置表

```sql
CREATE TABLE sync_tasks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name VARCHAR(100) NOT NULL,          -- 任务名称
    cron_expression VARCHAR(100) NOT NULL, -- Cron 表达式
    sync_type VARCHAR(50) NOT NULL,      -- stock_info / kline_daily / kline_minute
    symbols TEXT,                        -- 股票代码列表（JSON数组，空为全市场）
    start_date DATE,                     -- 历史数据起始日期
    end_date DATE,                       -- 历史数据结束日期
    failure_strategy VARCHAR(20) DEFAULT 'skip', -- skip / retry / stop
    retry_count INTEGER DEFAULT 3,       -- 重试次数
    enabled BOOLEAN DEFAULT TRUE,        -- 是否启用
    last_run_at TIMESTAMP,               -- 上次执行时间
    next_run_at TIMESTAMP,               -- 下次执行时间
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### 3.4 同步执行记录表

```sql
CREATE TABLE sync_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id INTEGER,                     -- 关联任务（手动同步为空）
    sync_type VARCHAR(50) NOT NULL,
    status VARCHAR(20) NOT NULL,         -- running / completed / failed
    total_count INTEGER,                 -- 总数量
    success_count INTEGER,               -- 成功数量
    failed_count INTEGER,                -- 失败数量
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP,
    error_message TEXT,                  -- 错误信息汇总
    details JSON,                        -- 详细结果
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

---

## 四、后端模块结构

```
backend/app/
├── api/
│   └── data.py                 # 数据相关 API
├── services/
│   ├── __init__.py
│   ├── data_service.py         # 数据查询服务
│   └── sync_service.py         # 数据同步服务
├── engines/
│   ├── __init__.py
│   └── qmt_gateway.py          # xtquant 网关封装
├── core/
│   ├── __init__.py
│   ├── config.py               # (已有)
│   └── scheduler.py            # 定时任务调度器
└── db/
    └── models/
        ├── __init__.py         # (已有)
        ├── stock.py            # (已有)
        ├── sync.py             # 新增：同步相关模型
        └── watchlist.py        # 新增：自选股模型
```

---

## 五、API 接口设计

### 5.1 股票相关

```
GET  /api/data/stocks                    # 获取股票列表
参数: search, industry, group_id, page, page_size

GET  /api/data/stocks/{symbol}           # 获取股票详情
```

### 5.2 自选股相关

```
GET  /api/data/watchlist/groups          # 获取自选股分组列表
POST /api/data/watchlist/groups          # 创建自选股分组
PUT  /api/data/watchlist/groups/{id}     # 更新分组
DELETE /api/data/watchlist/groups/{id}   # 删除分组
POST /api/data/watchlist/groups/{id}/stocks    # 添加股票到分组
DELETE /api/data/watchlist/groups/{id}/stocks/{symbol}  # 从分组移除股票
```

### 5.3 K线数据相关

```
GET  /api/data/klines                    # 获取K线数据
参数: symbol, kline_type(daily/minute), start_date, end_date, limit
```

### 5.4 数据同步相关

```
POST /api/data/sync                      # 手动触发同步
参数: sync_type, symbols, start_date, end_date, failure_strategy, retry_count

GET  /api/data/sync/status               # 获取当前同步状态
GET  /api/data/sync/logs                 # 获取同步日志列表
GET  /api/data/sync/logs/{id}            # 获取同步日志详情
```

### 5.5 定时任务相关

```
GET  /api/data/tasks                     # 获取定时任务列表
POST /api/data/tasks                     # 创建定时任务
PUT  /api/data/tasks/{id}                # 更新任务
DELETE /api/data/tasks/{id}              # 删除任务
POST /api/data/tasks/{id}/toggle         # 启用/禁用任务
```

---

## 六、前端页面设计

### 6.1 目录结构

```
frontend/src/views/DataManage/
├── index.vue                    # 数据管理主页面（Tab 切换）
├── StockList.vue                # 股票列表组件
├── WatchlistPanel.vue           # 自选股面板
├── KlineChart.vue               # K线图表组件
├── SyncPanel.vue                # 数据同步面板
└── TaskManager.vue              # 定时任务管理
```

### 6.2 股票列表页面布局

```
┌─────────────────────────────────────────────────────────────┐
│  数据管理                                                    │
├─────────────────────────────────────────────────────────────┤
│  [股票列表] [自选股] [数据同步] [定时任务]                    │
├─────────────────────────────────────────────────────────────┤
│  搜索: [________] 行业: [全部 ▼] 分组: [全部 ▼]  [查询]     │
├─────────────────────────────────────────────────────────────┤
│  代码     名称      行业      最新价    涨跌幅    操作       │
│  ─────────────────────────────────────────────────────────  │
│  000001  平安银行   金融      12.34    +1.25%   [详情][加自选]│
│  000002  万科A     房地产     8.56     -0.58%   [详情][加自选]│
├─────────────────────────────────────────────────────────────┤
│  共 5000 条  [<] 1 2 3 ... 50 [>]                           │
└─────────────────────────────────────────────────────────────┘
```

### 6.3 数据同步面板布局

```
┌─────────────────────────────────────────────────────────────┐
│  数据同步                                                    │
├─────────────────────────────────────────────────────────────┤
│  同步类型:  [✓] 股票基础信息  [✓] 日K线  [ ] 分钟K线        │
│  时间范围:  [2024-01-01] 至 [2025-04-12]                    │
│  股票范围:  [○] 全市场  [●] 自选股  [ ] 指定股票            │
│  失败处理:  [跳过 ▼]  重试次数: [3]                          │
│  [开始同步]                                                  │
├─────────────────────────────────────────────────────────────┤
│  同步状态                                                    │
│  ████████████████████░░░░░░░░░░  65%                       │
│  正在同步: 000001.SZ 平安银行  成功: 3250  失败: 12          │
├─────────────────────────────────────────────────────────────┤
│  最近同步记录                                                │
│  时间              类型        状态    成功/总数   详情      │
│  2025-04-12 18:00 日K线同步    完成    5000/5012  [查看]    │
└─────────────────────────────────────────────────────────────┘
```

---

## 七、核心组件设计

### 7.1 QMTGateway

```python
class QMTGateway:
    """miniQMT 数据网关"""

    async def get_stock_list(self) -> list[StockInfo]:
        """获取股票列表"""

    async def get_kline_daily(
        self,
        symbol: str,
        start_date: date,
        end_date: date
    ) -> list[KlineData]:
        """获取日K线数据"""

    async def get_kline_minute(
        self,
        symbol: str,
        start_date: date,
        end_date: date
    ) -> list[KlineData]:
        """获取分钟K线数据"""

    async def check_connection(self) -> bool:
        """检查连接状态"""
```

### 7.2 SyncService

```python
class SyncService:
    """数据同步服务"""

    async def sync_stock_info(
        self,
        symbols: list[str] | None = None,
        on_progress: Callable = None
    ) -> SyncResult:
        """同步股票基础信息"""

    async def sync_kline_daily(
        self,
        symbols: list[str] | None = None,
        start_date: date = None,
        end_date: date = None,
        failure_strategy: str = "skip",
        retry_count: int = 3,
        on_progress: Callable = None
    ) -> SyncResult:
        """同步日K线数据"""

    async def get_sync_status(self) -> SyncStatus:
        """获取当前同步状态"""
```

### 7.3 DataService

```python
class DataService:
    """数据查询服务"""

    async def get_stocks(
        self,
        search: str = None,
        industry: str = None,
        group_id: int = None,
        page: int = 1,
        page_size: int = 50
    ) -> PaginatedResult[Stock]:
        """查询股票列表"""

    async def get_klines(
        self,
        symbol: str,
        kline_type: str = "daily",
        start_date: date = None,
        end_date: date = None,
        limit: int = 500
    ) -> list[KlineData]:
        """查询K线数据"""

    async def get_industries(self) -> list[str]:
        """获取所有行业列表"""
```

---

## 八、技术依赖

### 8.1 后端新增依赖

```txt
apscheduler>=3.10.0      # 定时任务调度
xtquant                  # miniQMT SDK
```

### 8.2 前端依赖

```bash
npm install echarts      # K线图表
```

---

## 九、开发计划

| 任务 | 描述 |
|------|------|
| Task 1 | 新增数据表模型 (watchlist, sync) |
| Task 2 | QMTGateway 网关封装 |
| Task 3 | DataService 数据查询服务 |
| Task 4 | SyncService 数据同步服务 |
| Task 5 | 数据相关 API 接口 |
| Task 6 | 定时任务调度器 |
| Task 7 | 前端股票列表页面 |
| Task 8 | 前端K线图表组件 |
| Task 9 | 前端数据同步面板 |
| Task 10 | 前端定时任务管理 |

---

*文档版本: 1.0*
*创建日期: 2026-04-12*
