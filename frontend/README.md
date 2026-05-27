# GaoshouPlatform 前端

Last updated: 2026-05-25.

前端使用 Vue 3 + TypeScript + Vite + Element Plus，默认通过 Vite dev server 访问后端 API。

## 启动

```powershell
cd E:\Projects\GaoshouPlatform\frontend
npm install
npm run dev
```

常见本地端口：

| 服务 | 默认/常见端口 |
|---|---|
| Vite 前端 | `5173` 或桌面脚本指定的 `3500` |
| FastAPI 后端 | `8000` 或当前开发会话的 `8800` |

实际访问地址以启动脚本输出为准。

## 主要页面

| 路由 | 用途 |
|---|---|
| `/data` | 数据管理、同步、股票列表、K 线查询 |
| `/explorer` | 本地 Parquet / ClickHouse 数据浏览 |
| `/factors` | 因子研究入口 |
| `/factors/board` | 因子看板、IC/覆盖率排序 |
| `/factors/detail` | 因子详情、公式、人话解释和研究结果 |
| `/factor-values` | 因子值缓存、预计算、覆盖率和截面预览 |
| `/backtest` | 策略回测和 AKQuant 入口 |

## 因子研究注意事项

- 因子值缓存统一使用 `/api/factor-values/*`。
- Alpha101 101 个公式支持宽表批量预计算。
- 看板排序依赖已保存的研究结果；如果公式或缓存口径变更，需要重新预计算并重新跑研究。
- 覆盖率低的因子不要直接和高覆盖率因子比较 IC。

## 验证

```powershell
cd E:\Projects\GaoshouPlatform\frontend
npm run build
```

Vite chunk size warning 是当前已知的非阻塞构建警告。
