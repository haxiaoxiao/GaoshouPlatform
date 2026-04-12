# 行情数据存储改造：SQLite → ClickHouse

> 创建日期: 2026-04-12
> 状态: 设计中

## 一、改造目标

将 K线数据（日K、分钟K）从 SQLite 迁移到 ClickHouse，保持 API 接口不变。

## 二、改造范围

### 保留 SQLite 存储
- Stock（股票基础信息）
- Strategy/Backtest/Order/Trade（策略相关）
- Factor/FactorAnalysis（因子相关）
- WatchlistGroup/WatchlistStock（自选股）
- SyncTask/SyncLog（同步任务）

### 迁移到 ClickHouse
- KlineDaily（日K线）
- KlineMinute（分钟K线）

## 三、文件改动

```
backend/app/
├── db/
│   ├── clickhouse.py          # 新增：ClickHouse 连接
│   ├── sqlite.py              # 保留：SQLite 连接
│   └── models/
│       └── stock.py           # 移除 KlineDaily/KlineMinute 模型
├── services/
│   ├── data_service.py        # 修改：K线查询改用 ClickHouse
│   └── sync_service.py        # 修改：K线写入改用 ClickHouse
├── api/
│   └── data.py                # 保持不变
└── core/
    └── config.py              # 修改：添加 ClickHouse 配置
```

## 四、ClickHouse 表结构

```sql
-- 日K线表
CREATE TABLE IF NOT EXISTS klines_daily
(
    symbol LowCardinality(String),
    trade_date Date,
    open Decimal(10, 4),
    high Decimal(10, 4),
    low Decimal(10, 4),
    close Decimal(10, 4),
    volume UInt64,
    amount Decimal(18, 4),
    created_at DateTime DEFAULT now()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(trade_date)
ORDER BY (symbol, trade_date);

-- 分钟K线表
CREATE TABLE IF NOT EXISTS klines_minute
(
    symbol LowCardinality(String),
    datetime DateTime,
    open Decimal(10, 4),
    high Decimal(10, 4),
    low Decimal(10, 4),
    close Decimal(10, 4),
    volume UInt64,
    amount Decimal(18, 4),
    created_at DateTime DEFAULT now()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(datetime)
ORDER BY (symbol, datetime);
```

## 五、依赖

```txt
clickhouse-driver>=0.2.6
```

## 六、开发任务

1. 添加 ClickHouse 连接模块
2. 修改配置文件
3. 修改 DataService K线查询
4. 修改 SyncService K线写入
5. 移除 SQLite K线模型
6. 测试验证

---

*文档版本: 1.0*
