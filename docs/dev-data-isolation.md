# 开发样本数据隔离指南

本指南用于逐页重构前端时使用一小段真实生产数据，同时保证 dev 与 prod 存储互不干涉。

## 隔离原则

- 生产数据只读：脚本用 SQLite `mode=ro` 打开 `gaoshou.db`，Parquet 只由 DuckDB 扫描读取。
- 开发样本仍独立：默认写入公共数据目录里的 `E:\Projects\Data\dev_sample\gaoshou.db` 与 `E:\Projects\Data\dev_sample\parquet\`，不再写入仓库 `data/`。
- ClickHouse 关闭：开发样本默认 `MARKET_DATA_BACKEND=parquet`、`CLICKHOUSE_ENABLED=false`。
- 同步调度关闭：本地覆盖配置写入 `ENABLE_SYNC_SCHEDULER=false`，避免开发时误触生产同步。

## 生成样本

```powershell
cd E:\Projects\GaoshouPlatform-dev\backend
.\.venv\Scripts\python.exe -m app.scripts.create_dev_sample_data `
  --source-data-dir E:\Projects\Data `
  --target-data-dir E:\Projects\Data\dev_sample `
  --start-date 2026-05-01 `
  --end-date 2026-05-15 `
  --max-symbols 60 `
  --overwrite `
  --write-host-env
```

`--write-host-env` 会生成被 Git 忽略的 `E:\Projects\GaoshouPlatform-dev\.env.<hostname>.local`，覆盖 `.env.local` 中可能指向完整公共数据的路径。

## 开发环境口径

生成后后端实际使用：

```text
DATABASE_URL=sqlite+aiosqlite:///E:/Projects/Data/dev_sample/gaoshou.db
PARQUET_DATA_DIR=E:/Projects/Data/dev_sample/parquet
MARKET_DATA_BACKEND=parquet
CLICKHOUSE_ENABLED=false
```

如需回到完整公共数据，将 `.env.<hostname>.local` 改回 `E:/Projects/Data/gaoshou.db` 与 `E:/Projects/Data/parquet` 后重启后端即可。

## 验证

```powershell
cd E:\Projects\GaoshouPlatform-dev
Get-Content E:\Projects\Data\dev_sample\manifest.json
```

后端启动后检查：

```powershell
Invoke-WebRequest -UseBasicParsing http://127.0.0.1:18800/api/system/status
```

确认响应里的 `parquet_data_dir` 指向 `E:\Projects\Data\dev_sample\parquet`。

也可以直接校验 SQLite 里的配置/任务/策略/因子/自选股，以及 Parquet 缓存覆盖：

```powershell
cd E:\Projects\GaoshouPlatform-dev\backend
.\.venv\Scripts\python.exe -m app.scripts.validate_dev_sample_data `
  --json-report E:\Projects\Data\dev_sample\validation-report.json
```

校验会覆盖：

- SQLite 表是否存在、关键表行数、日期范围。
- 自选股、因子分析、财务、日频基础数据、涨跌停、舆情与 `stocks` 的关联完整性。
- 策略、回测、因子、同步日志等 JSON 配置字段是否可解析。
- 配置字段里是否仍引用生产目录或 prod 仓库路径。
- `klines_daily`、`klines_minute`、`klines_minute_cum_timer`、`factor_values` 等 Parquet 样本行数和日期范围。

接口到存储的覆盖矩阵见 `docs/dev-data-interface-coverage.md`。
