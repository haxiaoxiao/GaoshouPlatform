# 市场雷达数据同步面板设计

## 背景

市场雷达页面依赖 `tushare_limit_list_d`、`tushare_limit_step` 和 `tushare_margin`，但当前同步目录只展示核心 QMT 数据与通用 Tushare Relay 数据，用户无法从 `/data/sync` 直接补齐这些来源。

## 目标

- 在同步目录增加三类市场雷达依赖数据。
- 增加一个“市场雷达数据”预设，支持一次加入完整依赖集。
- 保留单个数据集加入队列、覆盖率展示、历史记录和失败状态。
- 同步结果继续写入现有 Parquet 数据集，市场雷达无需新增读取路径。

## 数据集

| 数据集 | 显示名 | 来源 | 存储 | 作用 |
|---|---|---|---|---|
| `tushare_limit_list_d` | 涨跌停明细 | Tushare Relay | `tushare_limit_list_d` | 涨跌停数、炸板率 |
| `tushare_limit_step` | 连板梯队 | Tushare Relay | `tushare_limit_step` | 最高板、晋级率、连板明细 |
| `tushare_margin` | 两融余额 | Tushare Relay | `tushare_margin` | 拥挤度与风险偏好 |

## 交互

- `/data/sync` 概览页展示“市场雷达数据”预设。
- 预设一次加入上述三个 Relay 数据集；未配置 Relay 凭据时沿用现有额度敏感/禁用提示。
- 目录页单项仍可加入队列，使用现有日期、失败策略和 Relay 参数。
- 市场雷达页面的“立即刷新”仍只负责刷新/重算快照，不替代数据同步。

## 后端

- 在 Relay 数据集规格中注册三项数据集、字段、日期列和目标存储名。
- 将三项数据集加入市场雷达预设，不改变已有预设语义。
- 复用现有 `tushare_relay` 批量同步和 Parquet 写入链路。
- 保持当前 market-radar freshness 检查和 `stale` 原因，不在同步层伪造日期。

## 验收

- `GET /api/data/sync/catalog` 返回三项数据集和“市场雷达数据”预设。
- 预设可被前端加入队列并生成 `tushare_relay` 请求。
- 现有同步目录、市场雷达 API、Relay 同步测试不回归。
- 前端类型检查、构建和相关 Vitest 测试通过。
