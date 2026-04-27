# DEX Quant Server

量化回测计算服务，为 **dex-quant-skill** 提供 HTTP API 后端。

## 技术栈

- **FastAPI** — 高性能异步 Web 框架
- **MySQL (aiomysql)** — 异步连接池持久化（策略、回测结果、K 线缓存）
- **numpy / pandas** — 数值计算与数据处理
- **httpx** — 调用 Binance / CoinGecko 等公开 API

## API 概览

| 方法 | 路径 | 说明 |
|------|------|------|
| `POST` | `/api/v1/backtest/run` | 执行回测（核心） |
| `GET`  | `/api/v1/backtest/{id}` | 查询回测结果 |
| `GET`  | `/api/v1/backtest/{id}/trades` | 获取交易记录 |
| `GET`  | `/api/v1/backtest/{id}/equity` | 获取权益曲线 |
| `POST` | `/api/v1/strategies/` | 创建策略 |
| `GET`  | `/api/v1/strategies/` | 列出所有策略 |
| `GET`  | `/api/v1/strategies/{id}` | 获取策略详情 |
| `PUT`  | `/api/v1/strategies/{id}` | 更新策略 |
| `POST` | `/api/v1/data/klines` | 获取 K 线数据 |
| `GET`  | `/api/v1/data/symbols` | 列出可用交易对 |
| `GET`  | `/health` | 健康检查 |

## 本地运行

```bash
pip install -r requirements.txt
uvicorn app.main:app --reload
```

服务启动后访问 http://localhost:8000/docs 查看交互式 API 文档。

## 阿里云 ECS 部署建议

推荐部署形态：

- `dex-quant-server` 部署在阿里云 ECS
- MySQL 使用阿里云 RDS MySQL，或先在同机自建 MySQL
- Nginx 反向代理到 `127.0.0.1:8000`
- 域名通过 HTTPS 暴露，例如 `https://quant.example.com`
- `dex-skill` 侧通过环境变量 `DEX_QUANT_SERVER_URL=https://quant.example.com` 指向新后端

最小上线步骤：

```bash
# 1. 上传代码到 ECS，例如 /opt/dex-quant-server
cp .env.example .env

# 2. 编辑环境变量
vim .env

# 3. 启动服务
bash deploy.sh
```

Nginx 示例：

```nginx
server {
    listen 80;
    server_name quant.example.com;

    location / {
        proxy_pass http://127.0.0.1:8000;
        proxy_set_header Host $host;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_set_header X-Forwarded-Host $host;
    }
}
```

建议再用 Certbot 或阿里云证书把 80 升级到 HTTPS。

## Docker 运行

```bash
docker build -t dex-quant-server .
docker run -p 8000:8000 \
  -e MYSQL_HOST=your-mysql-host \
  -e MYSQL_PORT=3306 \
  -e MYSQL_USER=root \
  -e MYSQL_PASSWORD=your-password \
  -e MYSQL_DB=dex_quant \
  dex-quant-server
```

## 环境变量

> **安全提示**：密码等敏感信息必须通过环境变量传入，禁止硬编码在代码中。

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `MYSQL_HOST` | *(必填)* | MySQL 主机地址 |
| `MYSQL_PORT` | `3306` | MySQL 端口 |
| `MYSQL_USER` | `root` | MySQL 用户名 |
| `MYSQL_PASSWORD` | *(必填)* | MySQL 密码 |
| `MYSQL_DB` | `dex_quant` | MySQL 数据库名 |
| `mysql_db` | *(可选)* | 旧格式兼容：JSON 字符串 `{"host":"..","port":..,"user":"..","password":"..","db":".."}` |
| `DB_POOL_SIZE` | `1` | 连接池大小 |
| `PROXY_URL` | *(空)* | HTTP 代理地址，用于访问 Binance API |
| `API_PREFIX` | `/api/v1` | API 路由前缀 |
| `PUBLIC_BASE_URL` | *(空)* | 对外访问的 HTTPS 域名，用于生成 Vault 安全链接等绝对地址 |
| `DEFAULT_FEE_RATE` | `0.0005` | 默认手续费率 |
| `DEFAULT_SLIPPAGE_BPS` | `2.0` | 默认滑点（基点） |
| `MAX_BACKTEST_BARS` | `500000` | 单次回测最大 K 线数 |
| `SANDBOX_MODE` | `docker` | 脚本执行模式：`docker`（容器隔离，默认）或 `process`（进程内沙箱，无 Docker 时回退） |
| `SANDBOX_IMAGE` | `dex-sandbox:latest` | Docker 沙箱镜像名（仅 docker 模式） |
| `SANDBOX_TIMEOUT` | `120` | 沙箱执行超时秒数 |
| `SANDBOX_MEMORY` | `512m` | 沙箱容器内存限制（仅 docker 模式） |
| `SANDBOX_CPUS` | `1` | 沙箱容器 CPU 限制（仅 docker 模式） |

## Docker 沙箱（推荐自托管使用）

自托管服务器建议开启 Docker 沙箱，每个策略脚本在独立容器中执行：

```bash
# 1. 构建沙箱镜像
cd sandbox && bash build.sh

# 2. 启用 docker 模式
export SANDBOX_MODE=docker
```

安全措施：`--network=none` 断网 / `--memory=512m` / `--cpus=1` / `--read-only` / `--user=65534` 非root / `--cap-drop=ALL`

## skill 端切换到阿里云后端

`dex-skill` 现在支持通过环境变量切换服务地址：

```bash
export DEX_QUANT_SERVER_URL=https://quant.example.com
```

如果你是用长期运行环境，建议把它写进 shell profile、Bot 进程环境变量，或 skill 运行容器的启动配置中。

## 架构关系

```
┌─────────────────────┐
│   dex-quant-skill   │  ← Codex/Cursor Agent Skill
│  (自然语言 → 策略)   │
└─────────┬───────────┘
          │ HTTP API
          ▼
┌─────────────────────┐
│  dex-quant-server   │  ← 本项目
│                     │
│  ┌───────────────┐  │
│  │   Routers     │  │  FastAPI 路由层
│  │  (backtest/   │  │
│  │   strategy/   │  │
│  │   data)       │  │
│  └───────┬───────┘  │
│          ▼          │
│  ┌───────────────┐  │
│  │   Services    │  │  编排层（BacktestService / DataService）
│  └───────┬───────┘  │
│          ▼          │
│  ┌───────────────┐  │
│  │   Core        │  │  引擎（backtest_engine / indicators / data_client）
│  └───────┬───────┘  │
│          ▼          │
│  ┌───────────────┐  │
│  │   MySQL DB    │  │  持久化（策略 / 回测结果 / K 线缓存）
│  └───────────────┘  │
└─────────────────────┘
```
