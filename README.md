# GaoshouPlatform - 量化投研平台

一个基于 Vue 3 + FastAPI 的量化投研平台，支持数据管理、因子研究、策略回测和实盘交易。

## 项目结构

```
GaoshouPlatform/
├── backend/                 # FastAPI 后端服务
│   ├── app/
│   │   ├── api/            # API 路由
│   │   ├── core/           # 核心配置
│   │   ├── db/             # 数据库模型和连接
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

### 前端
- **Vue 3** - 渐进式 JavaScript 框架
- **TypeScript** - 类型安全
- **Vite** - 下一代前端构建工具
- **Element Plus** - Vue 3 UI 组件库
- **Vue Router** - 路由管理
- **Pinia** - 状态管理
- **Axios** - HTTP 客户端

## 功能模块

1. **数据管理** - 股票数据导入、清洗、存储
2. **因子研究** - 因子计算、分析、可视化
3. **策略回测** - 策略开发、历史回测、绩效分析
4. **实盘交易** - 实盘策略运行、订单管理
5. **系统监控** - 系统状态、日志查看

## 快速开始

### 环境要求
- Python 3.10+
- Node.js 18+

### 后端启动

```bash
cd backend
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

| 端点 | 方法 | 描述 |
|------|------|------|
| `/health` | GET | 根路径健康检查 |
| `/api/system/status` | GET | 系统状态 |
| `/api/system/health` | GET | API 健康检查 |

## 开发状态

- [x] 后端项目初始化
- [x] 数据库连接配置
- [x] 核心数据模型定义
- [x] API 路由框架搭建
- [x] 前端项目初始化
- [x] 前端路由配置
- [x] 前端 API 调用封装
- [x] 前端基础布局
- [x] 前后端联调验证

## License

MIT
