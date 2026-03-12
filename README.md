# DEX Quant Server

量化回测计算服务，为 **dex-quant-skill** 提供 HTTP API 后端。

## 技术栈

- **FastAPI** — 高性能异步 Web 框架
- **SQLite (aiosqlite)** — 轻量持久化（策略、回测结果、K 线缓存）
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

## Docker 运行

```bash
docker build -t dex-quant-server .
docker run -p 8000:8000 -e PROXY_URL=http://host:port dex-quant-server
```

## 环境变量

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `PROXY_URL` | *(空)* | HTTP 代理地址，用于访问 Binance API |
| `DATA_DIR` | `data` | 数据存储目录 |
| `API_PREFIX` | `/api/v1` | API 路由前缀 |
| `DEFAULT_FEE_RATE` | `0.0005` | 默认手续费率 |
| `DEFAULT_SLIPPAGE_BPS` | `2.0` | 默认滑点（基点） |
| `MAX_BACKTEST_BARS` | `500000` | 单次回测最大 K 线数 |

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
│  │   SQLite DB   │  │  持久化（策略 / 回测结果 / K 线缓存）
│  └───────────────┘  │
└─────────────────────┘
```
