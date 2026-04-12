# GaoshouPlatform - 量化投研平台

一个基于 Vue 3 + FastAPI 的量化投研平台，支持数据管理、因子研究、策略回测和实盘交易。

## 项目结构

```
GaoshouPlatform/
├── backend/                 # FastAPI 后端服务
│   ├── app/
│   │   ├── api/            # API 路由
│   │   │   ├── data.py     # 数据相关 API
│   │   │   └── system.py   # 系统 API
│   │   ├── core/           # 核心配置
│   │   │   ├── config.py   # 配置管理
│   │   │   └── scheduler.py # 定时任务调度
│   │   ├── db/             # 数据库模型和连接
│   │   │   ├── models/     # SQLAlchemy 模型
│   │   │   └── sqlite.py   # 数据库连接
│   │   ├── engines/        # 数据引擎
│   │   │   └── qmt_gateway.py # QMT/xtquant 网关
│   │   ├── services/       # 业务服务
│   │   │   ├── data_service.py # 数据查询服务
│   │   │   └── sync_service.py # 数据同步服务
│   │   └── main.py         # 应用入口
│   ├── data/               # 数据存储目录
│   ├── tests/              # 测试文件
│   └── requirements.txt    # Python 依赖
├── frontend/               # Vue 3 前端应用
│   ├── src/
│   │   ├── api/           # API 调用封装
│   │   ├── layouts/       # 布局组件
│   │   ├── router/        # 路由配置
│   │   ├── views/         # 页面组件
│   │   │   ├── DataManage/ # 数据管理页面
│   │   │   │   ├── StockList.vue # 股票列表
│   │   │   │   └── SyncPanel.vue # 同步面板
│   │   │   └── StockDetail.vue # 股票详情(K线图)
│   │   └── main.ts        # 应用入口
│   ├── package.json       # Node.js 依赖
│   └── vite.config.ts     # Vite 配置
└── docs/                   # 文档目录
```

## 技术栈

### 后端
- **FastAPI** - 现代 Python Web 框架
- **SQLAlchemy** - ORM，支持异步
- **SQLite** - 嵌入式数据库（开发环境）
- **Pydantic** - 数据验证
- **APScheduler** - 定时任务调度
- **xtquant** - QMT 数据接口

### 前端
- **Vue 3** - 渐进式 JavaScript 框架
- **TypeScript** - 类型安全
- **Vite** - 下一代前端构建工具
- **Element Plus** - Vue 3 UI 组件库
- **Vue Router** - 路由管理
- **Pinia** - 状态管理
- **Axios** - HTTP 客户端
- **ECharts** - 图表可视化

## 功能模块

### 已实现 (Phase 1 & 2)

1. **数据管理**
   - 股票列表展示（分页、搜索、行业筛选）
   - K 线图表（日K、分钟K，蜡烛图+成交量）
   - 数据同步面板（股票信息/K线数据同步）
   - 自选股分组管理

2. **因子研究** - 开发中
3. **策略回测** - 开发中
4. **实盘交易** - 开发中
5. **系统监控** - 开发中

## 快速开始

### 环境要求
- Python 3.10+
- Node.js 18+

### 后端启动

```bash
cd backend
python -m venv .venv
.venv\Scripts\activate  # Windows
pip install -r requirements.txt
uvicorn app.main:app --reload --port 8000
```

### 前端启动

```bash
cd frontend
npm install
npm run dev
```

### 访问地址
- 前端应用: http://localhost:5173
- API 文档: http://localhost:8000/docs
- 健康检查: http://localhost:8000/health

## API 端点

### 系统 API
| 端点 | 方法 | 描述 |
|------|------|------|
| `/health` | GET | 根路径健康检查 |
| `/api/system/status` | GET | 系统状态 |
| `/api/system/health` | GET | API 健康检查 |

### 数据 API
| 端点 | 方法 | 描述 |
|------|------|------|
| `/api/data/stocks` | GET | 获取股票列表（分页、搜索、筛选） |
| `/api/data/stocks/{symbol}` | GET | 获取股票详情 |
| `/api/data/klines` | GET | 获取K线数据 |
| `/api/data/sync` | POST | 触发数据同步 |
| `/api/data/sync/status` | GET | 获取同步状态 |
| `/api/data/sync/logs` | GET | 获取同步日志 |
| `/api/data/watchlist/groups` | GET/POST | 自选股分组管理 |

## 开发状态

### Phase 1 - 基础框架 ✅
- [x] 后端项目初始化
- [x] 数据库连接配置
- [x] 核心数据模型定义
- [x] API 路由框架搭建
- [x] 前端项目初始化
- [x] 前端路由配置
- [x] 前端 API 调用封装
- [x] 前端基础布局
- [x] 前后端联调验证

### Phase 2 - 数据管理 ✅
- [x] 数据表模型（Stock、Kline、Watchlist、SyncTask）
- [x] QMTGateway 网关封装
- [x] DataService 数据查询服务
- [x] SyncService 数据同步服务
- [x] 数据相关 API 接口
- [x] 定时任务调度器
- [x] 股票列表页面
- [x] K线图表组件
- [x] 数据同步面板
- [x] 前后端联调验证

### Phase 3 - 因子研究 (计划中)
- [ ] 因子定义和管理
- [ ] 因子计算引擎
- [ ] 因子分析报告

## License

MIT
