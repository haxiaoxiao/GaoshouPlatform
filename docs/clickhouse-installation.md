# ClickHouse 安装指南 (Windows)

## 一、安装方式

Windows 上安装 ClickHouse 推荐使用 Docker 或 WSL2。以下是几种方式：

### 方式一：Docker（推荐）

1. **安装 Docker Desktop**
   - 下载：https://www.docker.com/products/docker-desktop/
   - 安装并启动 Docker Desktop

2. **运行 ClickHouse 容器**
   ```powershell
   # 创建数据目录
   mkdir E:\clickhouse-data

   # 启动 ClickHouse
   docker run -d `
     --name clickhouse-server `
     --ulimit nofile=262144:262144 `
     -p 9000:9000 `
     -p 8123:8123 `
     -v E:\clickhouse-data:/var/lib/clickhouse `
     clickhouse/clickhouse-server
   ```

3. **验证安装**
   ```powershell
   docker exec -it clickhouse-server clickhouse-client
   ```
   进入客户端后执行：
   ```sql
   SELECT version();
   ```

### 方式二：WSL2 + 原生安装

1. **启用 WSL2**
   ```powershell
   wsl --install
   ```

2. **安装 Ubuntu**
   ```powershell
   wsl --install -d Ubuntu
   ```

3. **在 WSL2 Ubuntu 中安装 ClickHouse**
   ```bash
   # 进入 WSL
   wsl

   # 安装 ClickHouse
   sudo apt-get update
   sudo apt-get install -y apt-transport-https ca-certificates curl gnupg
   curl -fsSL 'https://packages.clickhouse.com/rpm/lts/repodata/repomd.xml.key' | sudo gpg --dearmor -o /usr/share/keyrings/clickhouse-keyring.gpg

   # 对于 Ubuntu，使用官方仓库
   sudo apt-get install -y clickhouse-server clickhouse-client

   # 启动服务
   sudo service clickhouse-server start

   # 连接测试
   clickhouse-client
   ```

### 方式三：使用便携版（简化）

下载预编译的 ClickHouse 二进制文件：

```powershell
# 创建目录
mkdir E:\clickhouse

# 下载（需要从官方获取 Windows 预览版）
# 注意：官方对 Windows 原生支持有限，推荐 Docker 方式
```

---

## 二、推荐方案：Docker

```powershell
# 1. 创建数据目录
mkdir E:\clickhouse-data
mkdir E:\clickhouse-logs

# 2. 启动 ClickHouse
docker run -d `
  --name clickhouse-server `
  --restart always `
  -p 9000:9000 `
  -p 8123:8123 `
  -v E:\clickhouse-data:/var/lib/clickhouse `
  -v E:\clickhouse-logs:/var/log/clickhouse-server `
  clickhouse/clickhouse-server:latest

# 3. 验证运行状态
docker ps | findstr clickhouse

# 4. 进入客户端
docker exec -it clickhouse-server clickhouse-client
```

---

## 三、创建数据库和表

启动 ClickHouse 后，运行以下 SQL：

```sql
-- 创建数据库
CREATE DATABASE IF NOT EXISTS gaoshou;

-- 切换数据库
USE gaoshou;

-- 创建日K线表
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

-- 创建分钟K线表
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

-- 验证表创建
SHOW TABLES;
```

---

## 四、常用命令

```powershell
# 查看日志
docker logs clickhouse-server

# 停止服务
docker stop clickhouse-server

# 启动服务
docker start clickhouse-server

# 重启服务
docker restart clickhouse-server

# 进入客户端
docker exec -it clickhouse-server clickhouse-client

# 备份数据
docker cp clickhouse-server:/var/lib/clickhouse E:\clickhouse-backup
```

---

## 五、连接配置

后端配置（已在 `backend/app/core/config.py` 中配置）：

```python
# 默认配置
clickhouse_host: str = "localhost"
clickhouse_port: int = 9000
clickhouse_database: str = "gaoshou"
clickhouse_user: str = "default"
clickhouse_password: str = ""
```

如需修改，可创建 `.env` 文件：

```env
CLICKHOUSE_HOST=localhost
CLICKHOUSE_PORT=9000
CLICKHOUSE_DATABASE=gaoshou
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=
```

---

## 六、验证连接

启动后端服务后，可以测试：

```bash
cd backend
uvicorn app.main:app --reload --port 8000
```

访问 http://localhost:8000/docs 查看 API 文档。

---

*创建日期: 2026-04-12*
