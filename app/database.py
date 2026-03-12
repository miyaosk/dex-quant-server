"""
异步 SQLite 数据库层

使用 aiosqlite 管理策略、回测结果和 K 线缓存。
"""

from __future__ import annotations

import json
from contextlib import asynccontextmanager
from pathlib import Path
from typing import AsyncGenerator, Optional

import aiosqlite
from loguru import logger

DB_DIR = Path("data")
DB_PATH = DB_DIR / "quant.db"

# ═══════════════════════════════════════════
#  建表 SQL
# ═══════════════════════════════════════════

_CREATE_TABLES_SQL = """
CREATE TABLE IF NOT EXISTS strategies (
    strategy_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    version TEXT DEFAULT 'v1.0',
    spec_json TEXT NOT NULL,
    lifecycle_state TEXT DEFAULT 'draft',
    created_at TEXT DEFAULT (datetime('now')),
    updated_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS backtest_results (
    backtest_id TEXT PRIMARY KEY,
    strategy_id TEXT NOT NULL,
    config_json TEXT NOT NULL,
    metrics_json TEXT,
    trades_json TEXT,
    equity_json TEXT,
    status TEXT DEFAULT 'running',
    error TEXT,
    created_at TEXT DEFAULT (datetime('now')),
    elapsed_ms INTEGER DEFAULT 0,
    FOREIGN KEY (strategy_id) REFERENCES strategies(strategy_id)
);

CREATE TABLE IF NOT EXISTS kline_cache (
    cache_key TEXT PRIMARY KEY,
    symbol TEXT NOT NULL,
    interval TEXT NOT NULL,
    market TEXT NOT NULL,
    data_json TEXT NOT NULL,
    row_count INTEGER DEFAULT 0,
    fetched_at TEXT DEFAULT (datetime('now'))
);
"""


# ═══════════════════════════════════════════
#  连接管理
# ═══════════════════════════════════════════


@asynccontextmanager
async def get_db() -> AsyncGenerator[aiosqlite.Connection, None]:
    """获取数据库连接（异步上下文管理器）。"""
    DB_DIR.mkdir(parents=True, exist_ok=True)
    db = await aiosqlite.connect(str(DB_PATH))
    db.row_factory = aiosqlite.Row
    try:
        yield db
    finally:
        await db.close()


async def init_db() -> None:
    """初始化数据库，创建所有表。"""
    DB_DIR.mkdir(parents=True, exist_ok=True)
    async with get_db() as db:
        await db.executescript(_CREATE_TABLES_SQL)
        await db.commit()
    logger.info(f"数据库初始化完成: {DB_PATH}")


# ═══════════════════════════════════════════
#  策略 CRUD
# ═══════════════════════════════════════════


async def save_strategy(
    strategy_id: str,
    name: str,
    version: str,
    spec_json: str,
    lifecycle_state: str = "draft",
) -> None:
    """保存或更新策略。"""
    async with get_db() as db:
        await db.execute(
            """
            INSERT INTO strategies (strategy_id, name, version, spec_json, lifecycle_state)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(strategy_id) DO UPDATE SET
                name = excluded.name,
                version = excluded.version,
                spec_json = excluded.spec_json,
                lifecycle_state = excluded.lifecycle_state,
                updated_at = datetime('now')
            """,
            (strategy_id, name, version, spec_json, lifecycle_state),
        )
        await db.commit()


async def get_strategy(strategy_id: str) -> Optional[dict]:
    """按 ID 获取策略，返回 dict 或 None。"""
    async with get_db() as db:
        cursor = await db.execute(
            "SELECT * FROM strategies WHERE strategy_id = ?",
            (strategy_id,),
        )
        row = await cursor.fetchone()
        if row is None:
            return None
        return dict(row)


async def list_strategies() -> list[dict]:
    """列出所有策略。"""
    async with get_db() as db:
        cursor = await db.execute(
            "SELECT strategy_id, name, version, lifecycle_state, created_at, updated_at "
            "FROM strategies ORDER BY updated_at DESC"
        )
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]


# ═══════════════════════════════════════════
#  回测结果
# ═══════════════════════════════════════════


async def save_backtest_result(
    backtest_id: str,
    strategy_id: str,
    config_json: str,
    metrics_json: Optional[str],
    trades_json: Optional[str],
    equity_json: Optional[str],
    status: str,
    error: Optional[str],
    elapsed_ms: int,
) -> None:
    """保存回测结果。"""
    async with get_db() as db:
        await db.execute(
            """
            INSERT INTO backtest_results
                (backtest_id, strategy_id, config_json, metrics_json,
                 trades_json, equity_json, status, error, elapsed_ms)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(backtest_id) DO UPDATE SET
                metrics_json = excluded.metrics_json,
                trades_json = excluded.trades_json,
                equity_json = excluded.equity_json,
                status = excluded.status,
                error = excluded.error,
                elapsed_ms = excluded.elapsed_ms
            """,
            (backtest_id, strategy_id, config_json, metrics_json,
             trades_json, equity_json, status, error, elapsed_ms),
        )
        await db.commit()


async def get_backtest_result(backtest_id: str) -> Optional[dict]:
    """按 ID 获取回测结果。"""
    async with get_db() as db:
        cursor = await db.execute(
            "SELECT * FROM backtest_results WHERE backtest_id = ?",
            (backtest_id,),
        )
        row = await cursor.fetchone()
        if row is None:
            return None
        return dict(row)


# ═══════════════════════════════════════════
#  K 线缓存
# ═══════════════════════════════════════════


async def get_cached_klines(cache_key: str) -> Optional[str]:
    """获取缓存的 K 线 JSON 字符串，未命中返回 None。"""
    async with get_db() as db:
        cursor = await db.execute(
            "SELECT data_json FROM kline_cache WHERE cache_key = ?",
            (cache_key,),
        )
        row = await cursor.fetchone()
        if row is None:
            return None
        return row["data_json"]


async def save_kline_cache(
    cache_key: str,
    symbol: str,
    interval: str,
    market: str,
    data_json: str,
    row_count: int,
) -> None:
    """写入 K 线缓存。"""
    async with get_db() as db:
        await db.execute(
            """
            INSERT INTO kline_cache
                (cache_key, symbol, interval, market, data_json, row_count)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(cache_key) DO UPDATE SET
                data_json = excluded.data_json,
                row_count = excluded.row_count,
                fetched_at = datetime('now')
            """,
            (cache_key, symbol, interval, market, data_json, row_count),
        )
        await db.commit()
