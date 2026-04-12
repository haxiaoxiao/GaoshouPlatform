# GaoshouPlatform 量化投研平台设计文档

> 创建日期: 2026-04-12
> 状态: 已确认，准备实施

## 一、项目概述

### 1.1 项目定位

全流程量化投研平台，覆盖：因子研究 → 策略编写 → 回测优化 → 实盘交易

### 1.2 目标市场

- 初期：A股市场
- 后期：扩展期货市场

### 1.3 开发优先级

**先跑通核心流程，快速验证价值**

## 二、架构设计

### 2.1 整体架构

采用**模块化分层架构**：

```
┌─────────────────────────────────────────────────────────────────┐
│                        前端层 (Vue 3)                             │
│  ┌──────────┬──────────┬──────────┬──────────┬──────────┐       │
│  │ 数据管理  │ 因子研究  │ 策略回测  │ 实盘交易  │ 系统监控  │       │
│  └──────────┴──────────┴──────────┴──────────┴──────────┘       │
├─────────────────────────────────────────────────────────────────┤
│                        API层 (FastAPI)                           │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │  RESTful API  │  WebSocket 实时推送  │  后台任务队列      │    │
│  └─────────────────────────────────────────────────────────┘    │
├─────────────────────────────────────────────────────────────────┤
│                        业务服务层                                │
│  ┌──────────┬──────────┬──────────┬──────────┬──────────┐       │
│  │ 数据服务  │ 因子服务  │ 策略服务  │ 回测服务  │ 交易服务  │       │
│  │DataService│FactorSvc │StrategySvc│BacktestSvc│TradeSvc │       │
│  └──────────┴──────────┴──────────┴──────────┴──────────┘       │
├─────────────────────────────────────────────────────────────────┤
│                        核心引擎层                                │
│  ┌────────────────────┬────────────────────┬─────────────────┐  │
│  │    VeighNa 引擎     │    miniQMT 网关     │    因子引擎      │  │
│  │  (回测/交易核心)    │  (实盘数据+交易)    │  (因子计算)      │  │
│  └────────────────────┴────────────────────┴─────────────────┘  │
├─────────────────────────────────────────────────────────────────┤
│                        数据存储层                                │
│  ┌────────────────────┬────────────────────┬─────────────────┐  │
│  │    ClickHouse      │      SQLite        │    文件存储      │  │
│  │   (K线/tick数据)   │  (配置/策略/交易)   │  (日志/回测报告)  │  │
│  │    (后期添加)       │                    │                 │  │
│  └────────────────────┴────────────────────┴─────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

### 2.2 技术栈

| 层级 | 技术选型 | 版本 |
|------|---------|------|
| **前端** | | |
| 框架 | Vue 3 + TypeScript | 3.4+ |
| UI组件库 | Element Plus | 2.4+ |
| 状态管理 | Pinia | 2.x |
| 图表库 | ECharts | 5.x |
| HTTP客户端 | Axios | 1.x |
| 构建工具 | Vite | 5.x |
| **后端** | | |
| 框架 | FastAPI | 0.110+ |
| 异步任务 | Celery + Redis（或后台线程） | 按需 |
| ORM | SQLAlchemy | 2.x |
| 数据验证 | Pydantic | 2.x |
| **核心引擎** | | |
| 回测/交易 | VeighNa | 3.x |
| 数据网关 | miniQMT | - |
| **数据存储** | | |
| 时序数据 | ClickHouse（后期） | 24.x |
| 关系数据 | SQLite | 3.x |

## 三、模块划分

### 3.1 目录结构

```
GaoshouPlatform/
├── backend/                    # 后端服务
│   ├── app/
│   │   ├── api/               # API 路由
│   │   │   ├── data.py        # 数据相关接口
│   │   │   ├── factor.py      # 因子相关接口
│   │   │   ├── strategy.py    # 策略相关接口
│   │   │   ├── backtest.py    # 回测相关接口
│   │   │   ├── trade.py       # 交易相关接口
│   │   │   └── system.py      # 系统状态接口
│   │   │
│   │   ├── services/          # 业务服务
│   │   │   ├── data_service.py
│   │   │   ├── factor_service.py
│   │   │   ├── strategy_service.py
│   │   │   ├── backtest_service.py
│   │   │   └── trade_service.py
│   │   │
│   │   ├── engines/           # 核心引擎
│   │   │   ├── vn_engine.py   # VeighNa 引擎封装
│   │   │   ├── qmt_gateway.py # miniQMT 网关
│   │   │   └── factor_engine.py
│   │   │
│   │   ├── models/            # 数据模型
│   │   │   ├── strategy.py
│   │   │   ├── backtest.py
│   │   │   ├── trade.py
│   │   │   └── factor.py
│   │   │
│   │   ├── db/                # 数据库连接
│   │   │   ├── clickhouse.py
│   │   │   └── sqlite.py
│   │   │
│   │   ├── core/              # 核心组件
│   │   │   ├── config.py      # 配置管理
│   │   │   ├── events.py      # 事件系统
│   │   │   └── scheduler.py   # 任务调度
│   │   │
│   │   └── main.py            # 应用入口
│   │
│   ├── tests/                 # 测试
│   ├── requirements.txt
│   └── pyproject.toml
│
├── frontend/                   # 前端服务
│   ├── src/
│   │   ├── views/             # 页面组件
│   │   │   ├── DataManage/
│   │   │   ├── FactorResearch/
│   │   │   ├── StrategyBacktest/
│   │   │   ├── LiveTrading/
│   │   │   └── SystemMonitor/
│   │   ├── components/        # 通用组件
│   │   ├── api/               # API 调用
│   │   ├── stores/            # 状态管理
│   │   └── router/            # 路由配置
│   ├── package.json
│   └── vite.config.ts
│
├── factors/                    # 因子库（独立目录，便于扩展）
│   ├── technical/             # 技术因子
│   │   ├── trend/            # 趋势类
│   │   ├── oscillator/       # 震荡类
│   │   └── volume/           # 量价类
│   ├── fundamental/           # 基本面因子
│   └── custom/                # 自定义因子
│
├── strategies/                 # 策略库
│   └── examples/              # 示例策略
│
├── docs/                       # 文档
│   └── plans/                 # 设计文档
│
├── scripts/                    # 工具脚本
│   ├── init_db.py             # 初始化数据库
│   └── sync_data.py           # 数据同步
│
├── config/                     # 配置文件
│   ├── config.yaml            # 主配置
│   └── logging.yaml           # 日志配置
│
└── README.md
```

## 四、数据流设计

### 4.1 数据获取与存储流程

```
miniQMT ──→ QMTGateway ──→ DataService ──→ SQLite（初期）/ ClickHouse（后期）
    │                              │
    │  (实时行情)                   │ (历史K线)
    │                              ↓
    └────────────────────→ WebSocket ──→ 前端
```

### 4.2 因子研究流程

```
SQLite ──→ DataService(读取行情) ──→ FactorEngine ──→ 计算结果
                                                   │
                    FactorService ←────────────────┘
                          │
                    ┌─────┴─────┐
                    ↓           ↓
              IC/IR分析    因子存储
                    │           │
                    └─────┬─────┘
                          ↓
                        前端展示
```

### 4.3 策略回测流程

```
前端 ──→ 回测参数 ──→ BacktestService ──→ VnEngine.initialize()
                                           │
SQLite ──→ 历史数据 ──→ VnEngine.load_data()
                                           │
                    VnEngine.run_backtest() ──→ 事件驱动回测
                                           │
                                           ↓
                    BacktestService ──→ 回测报告 ──→ 前端展示
                                           │
                                           ↓
                                      文件存储
```

### 4.4 实盘交易流程

```
miniQMT ──→ 实时行情 ──→ VnEngine.on_tick()
                              │
策略逻辑 ──→ 信号生成 ──→ TradeService
                              │
                    ┌─────────┴─────────┐
                    ↓                   ↓
              风控检查            手动确认(初期)
                    │                   │
                    └─────────┬─────────┘
                              ↓
                        QMTGateway.send_order()
                              │
                              ↓
                           交易所
                              │
                              ↓
                        成交回报 ──→ 更新持仓/资金
```

## 五、API 接口设计

### 5.1 RESTful API

```
# 数据接口
GET  /api/data/stocks              # 获取股票列表
GET  /api/data/klines              # 获取K线数据
POST /api/data/sync                # 同步历史数据

# 因子接口
GET  /api/factors                  # 获取因子列表
POST /api/factors/calculate        # 计算因子
POST /api/factors/analyze          # 因子IC/IR分析
POST /api/factors/upload           # 上传自定义因子

# 策略接口
GET  /api/strategies               # 获取策略列表
POST /api/strategies               # 创建策略
PUT  /api/strategies/{id}          # 更新策略
DELETE /api/strategies/{id}        # 删除策略

# 回测接口
POST /api/backtest/run             # 运行回测
GET  /api/backtest/{id}/status     # 获取回测状态
GET  /api/backtest/{id}/report     # 获取回测报告

# 交易接口
GET  /api/trade/account            # 账户信息
GET  /api/trade/positions          # 持仓信息
POST /api/trade/order              # 下单（手动确认后）
GET  /api/trade/orders             # 委托列表
GET  /api/trade/trades             # 成交列表

# 系统接口
GET  /api/system/status            # 系统状态
GET  /api/system/logs              # 系统日志
POST /api/system/start             # 启动交易服务
POST /api/system/stop              # 停止交易服务
```

### 5.2 WebSocket 事件

```python
# 实时行情推送
{"type": "tick", "symbol": "000001.SZ", "price": 12.34, ...}

# 订单状态推送
{"type": "order_update", "order_id": "xxx", "status": "filled", ...}

# 回测进度推送
{"type": "backtest_progress", "percent": 45, ...}

# 系统告警推送
{"type": "alert", "level": "warning", "message": "..."}
```

## 六、数据模型设计

### 6.1 SQLite 表结构（配置/交易类数据）

```sql
-- 策略表
CREATE TABLE strategies (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name VARCHAR(100) NOT NULL,
    code TEXT NOT NULL,
    parameters JSON,
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 回测记录表
CREATE TABLE backtests (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    strategy_id INTEGER NOT NULL,
    status VARCHAR(20) DEFAULT 'pending',
    start_date DATE NOT NULL,
    end_date DATE NOT NULL,
    initial_capital DECIMAL(15,2),
    parameters JSON,
    result JSON,
    report_path VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (strategy_id) REFERENCES strategies(id)
);

-- 交易订单表
CREATE TABLE orders (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id VARCHAR(50) UNIQUE,
    symbol VARCHAR(20) NOT NULL,
    direction VARCHAR(10) NOT NULL,
    order_type VARCHAR(10) NOT NULL,
    price DECIMAL(10,3),
    quantity INTEGER NOT NULL,
    filled_quantity INTEGER DEFAULT 0,
    status VARCHAR(20) NOT NULL,
    strategy_id INTEGER,
    signal_time TIMESTAMP,
    order_time TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (strategy_id) REFERENCES strategies(id)
);

-- 成交记录表
CREATE TABLE trades (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    trade_id VARCHAR(50) UNIQUE,
    order_id VARCHAR(50) NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    direction VARCHAR(10) NOT NULL,
    price DECIMAL(10,3) NOT NULL,
    quantity INTEGER NOT NULL,
    commission DECIMAL(10,2),
    trade_time TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (order_id) REFERENCES orders(order_id)
);

-- 因子定义表
CREATE TABLE factors (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name VARCHAR(50) NOT NULL UNIQUE,
    category VARCHAR(50),
    source VARCHAR(20),
    code TEXT,
    parameters JSON,
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 因子分析结果表
CREATE TABLE factor_analysis (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    factor_id INTEGER NOT NULL,
    start_date DATE NOT NULL,
    end_date DATE NOT NULL,
    ic_mean DECIMAL(6,4),
    ic_std DECIMAL(6,4),
    ir DECIMAL(6,4),
    turnover_rate DECIMAL(6,4),
    details JSON,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (factor_id) REFERENCES factors(id)
);

-- 系统配置表
CREATE TABLE config (
    key VARCHAR(100) PRIMARY KEY,
    value TEXT,
    description TEXT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 股票基础信息表
CREATE TABLE stocks (
    symbol VARCHAR(20) PRIMARY KEY,
    name VARCHAR(50),
    exchange VARCHAR(10),
    industry VARCHAR(50),
    list_date DATE,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- K线数据表（初期用SQLite，后期迁移到ClickHouse）
CREATE TABLE klines_daily (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol VARCHAR(20) NOT NULL,
    trade_date DATE NOT NULL,
    open DECIMAL(10,3),
    high DECIMAL(10,3),
    low DECIMAL(10,3),
    close DECIMAL(10,3),
    volume INTEGER,
    amount DECIMAL(18,2),
    turnover_rate DECIMAL(8,4),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(symbol, trade_date)
);

-- 分钟K线表
CREATE TABLE klines_minute (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol VARCHAR(20) NOT NULL,
    datetime TIMESTAMP NOT NULL,
    open DECIMAL(10,3),
    high DECIMAL(10,3),
    low DECIMAL(10,3),
    close DECIMAL(10,3),
    volume INTEGER,
    amount DECIMAL(18,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(symbol, datetime)
);
```

## 七、MVP 开发计划

### 阶段一：基础框架搭建

**目标：项目骨架可运行**

- [ ] 后端项目初始化
- [ ] 数据库连接（SQLite）
- [ ] API 框架搭建（FastAPI 路由）
- [ ] 基础配置管理
- [ ] 前端项目初始化
- [ ] 路由配置
- [ ] API 调用封装
- [ ] 基础布局（侧边栏 + 内容区）

### 阶段二：数据模块

**目标：能获取、存储、展示数据**

- [ ] miniQMT 网关封装
- [ ] 数据同步服务
- [ ] 数据查询 API
- [ ] 股票列表页面
- [ ] K线图表展示
- [ ] 数据同步操作

### 阶段三：回测模块

**目标：能运行简单策略回测**

- [ ] VeighNa 引擎集成
- [ ] 回测服务
- [ ] 回测 API
- [ ] 策略编辑器（代码编辑）
- [ ] 回测参数配置
- [ ] 回测报告展示

### 阶段四：因子模块

**目标：能计算和分析因子**

- [ ] 因子引擎
- [ ] 内置技术因子
- [ ] QMT因子对接
- [ ] 因子分析 API
- [ ] 因子列表
- [ ] 因子计算配置
- [ ] IC/IR 分析结果展示

### 阶段五：实盘交易（手动确认）

**目标：能手动下单交易**

- [ ] 实时行情订阅
- [ ] 交易服务
- [ ] 简单风控
- [ ] 订单/成交 API
- [ ] 账户信息展示
- [ ] 持仓展示
- [ ] 下单界面（手动）
- [ ] 委托/成交列表

## 八、多终端协同开发

### 8.1 开发模式

- **开发阶段**：Git 仓库管理，Mac 和 PC 都可以开发
- **实盘运行**：通过远程桌面操作 PC 上的平台

### 8.2 Git 工作流

```
main（稳定版本）
  └── develop（开发分支）
        ├── feature/data-module
        ├── feature/backtest-module
        └── feature/trade-module
```

## 九、因子库规划

### 9.1 因子来源

1. **QMT内置因子** — 复用 miniQMT 自带的因子，避免重复研发
2. **内置技术因子** — 常用技术指标（MA、MACD、RSI、布林带等）
3. **自定义因子** — 用户自行编写的因子

### 9.2 因子分类

- 趋势类因子
- 震荡类因子
- 量价类因子
- 基本面因子（后期）

### 9.3 因子评估

- IC（信息系数）
- IR（信息比率）
- 因子相关性分析
- 换手率分析

---

*文档版本: 1.0*
*最后更新: 2026-04-12*
