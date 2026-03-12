"""
数据库层 — 基于 MysqlSQL 连接池封装

使用 asyncio.to_thread 将同步 pymysql 操作包装为异步，
避免阻塞 FastAPI 事件循环。
"""

from __future__ import annotations

import asyncio
import json
from typing import Optional

from loguru import logger

from app.utils.mysql_client import mysql

# ═══════════════════════════════════════════
#  建表 SQL
# ═══════════════════════════════════════════

_CREATE_TABLES_SQL = [
    """
    CREATE TABLE IF NOT EXISTS dex_strategies (
        strategy_id VARCHAR(64) PRIMARY KEY COMMENT '策略唯一ID',
        name VARCHAR(255) NOT NULL COMMENT '策略名称',
        version VARCHAR(32) DEFAULT 'v1.0' COMMENT '策略版本号',
        spec_json JSON NOT NULL COMMENT '策略完整规范(交易对/时间周期/入场出场规则/仓位管理等)',
        lifecycle_state VARCHAR(32) DEFAULT 'draft' COMMENT '生命周期状态: draft/active/archived',
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '最后更新时间',
        INDEX idx_lifecycle (lifecycle_state),
        INDEX idx_updated (updated_at)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='量化策略定义表'
    """,
    """
    CREATE TABLE IF NOT EXISTS dex_backtest_results (
        backtest_id VARCHAR(64) PRIMARY KEY COMMENT '回测任务唯一ID',
        strategy_id VARCHAR(64) NOT NULL COMMENT '关联策略ID',
        config_json JSON NOT NULL COMMENT '回测配置(起止日期/初始资金/手续费率/杠杆等)',
        metrics_json JSON COMMENT '绩效指标(收益率/夏普比率/最大回撤/胜率等)',
        trades_json LONGTEXT COMMENT '全部交易记录JSON',
        equity_json LONGTEXT COMMENT '权益曲线数据JSON',
        status VARCHAR(32) DEFAULT 'running' COMMENT '回测状态: running/completed/failed',
        error TEXT COMMENT '失败时的错误信息',
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
        elapsed_ms INT DEFAULT 0 COMMENT '回测耗时(毫秒)',
        INDEX idx_strategy (strategy_id),
        INDEX idx_status (status),
        INDEX idx_created (created_at)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='回测结果表'
    """,
    """
    CREATE TABLE IF NOT EXISTS dex_kline_cache (
        cache_key VARCHAR(512) PRIMARY KEY COMMENT '缓存键(market:symbol:interval:start:end)',
        symbol VARCHAR(64) NOT NULL COMMENT '交易对(如BTCUSDT)',
        `interval` VARCHAR(16) NOT NULL COMMENT 'K线周期(1m/5m/15m/1h/4h/1d)',
        market VARCHAR(32) NOT NULL COMMENT '市场类型: crypto_futures/crypto_spot/stock/commodity/metal',
        data_json LONGTEXT NOT NULL COMMENT 'K线数据JSON(OHLCV)',
        row_count INT DEFAULT 0 COMMENT 'K线条数',
        fetched_at DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '数据拉取时间(用于缓存过期判断)',
        INDEX idx_symbol (symbol),
        INDEX idx_fetched (fetched_at)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='K线数据缓存表'
    """,
    """
    CREATE TABLE IF NOT EXISTS dex_signals (
        signal_id VARCHAR(64) PRIMARY KEY COMMENT '信号唯一ID',
        strategy_id VARCHAR(64) NOT NULL COMMENT '关联策略ID',
        symbol VARCHAR(64) NOT NULL COMMENT '交易对',
        timeframe VARCHAR(16) NOT NULL COMMENT 'K线周期',
        signal_type VARCHAR(32) NOT NULL COMMENT '信号类型: entry_long/entry_short/exit_long/exit_short',
        strength DOUBLE DEFAULT 0.5 COMMENT '信号强度(0-1)',
        price_at_signal DOUBLE NOT NULL COMMENT '信号触发时价格',
        stop_loss_price DOUBLE COMMENT '建议止损价',
        take_profit_price DOUBLE COMMENT '建议止盈价',
        triggered_by TEXT COMMENT '触发规则列表JSON',
        feature_snapshot JSON COMMENT '触发时的指标快照',
        confidence DOUBLE COMMENT '置信度(0-1)',
        ttl_seconds INT COMMENT '信号有效期(秒)',
        metadata JSON COMMENT '附加元数据',
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
        INDEX idx_strategy (strategy_id),
        INDEX idx_symbol (symbol),
        INDEX idx_type (signal_type),
        INDEX idx_created (created_at)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='交易信号表'
    """,
]


# ═══════════════════════════════════════════
#  初始化
# ═══════════════════════════════════════════


async def init_db() -> None:
    """建表（如果不存在）。"""
    def _init():
        for sql in _CREATE_TABLES_SQL:
            mysql.execute_sql(sql)

    await asyncio.to_thread(_init)
    logger.info(f"MySQL 初始化完成: {mysql.db_conf['host']}:{mysql.db_conf['port']}/{mysql.db_conf['db']}")


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
    data = {
        "strategy_id": strategy_id,
        "name": name,
        "version": version,
        "spec_json": spec_json,
        "lifecycle_state": lifecycle_state,
    }
    await asyncio.to_thread(mysql.upsert, data, "dex_strategies")


async def get_strategy(strategy_id: str) -> Optional[dict]:
    """按 ID 获取策略，返回 dict 或 None。"""
    rows = await asyncio.to_thread(
        mysql.select_where, "dex_strategies", {"strategy_id": strategy_id}, True
    )
    return rows[0] if rows else None


async def list_strategies() -> list[dict]:
    """列出所有策略（按更新时间倒序）。"""
    rows = await asyncio.to_thread(
        mysql.execute_sql,
        "SELECT strategy_id, name, version, spec_json, lifecycle_state, created_at, updated_at "
        "FROM dex_strategies ORDER BY updated_at DESC",
        None,
        True,
    )
    return rows


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
    data = {
        "backtest_id": backtest_id,
        "strategy_id": strategy_id,
        "config_json": config_json,
        "metrics_json": metrics_json,
        "trades_json": trades_json,
        "equity_json": equity_json,
        "status": status,
        "error": error,
        "elapsed_ms": elapsed_ms,
    }
    await asyncio.to_thread(mysql.upsert, data, "dex_backtest_results")


async def get_backtest_result(backtest_id: str) -> Optional[dict]:
    """按 ID 获取回测结果。"""
    rows = await asyncio.to_thread(
        mysql.select_where, "dex_backtest_results", {"backtest_id": backtest_id}, True
    )
    return rows[0] if rows else None


# ═══════════════════════════════════════════
#  K 线缓存
# ═══════════════════════════════════════════


async def get_cached_klines(cache_key: str) -> Optional[dict]:
    """获取缓存的 K 线记录，未命中返回 None。"""
    rows = await asyncio.to_thread(
        mysql.execute_sql,
        "SELECT data_json, fetched_at FROM dex_kline_cache WHERE cache_key = %s",
        (cache_key,),
        True,
    )
    return rows[0] if rows else None


# ═══════════════════════════════════════════
#  信号
# ═══════════════════════════════════════════


async def save_signal(signal: dict) -> None:
    """保存一条交易信号。"""
    data = {
        "signal_id": signal.get("signal_id", ""),
        "strategy_id": signal.get("strategy_id", ""),
        "symbol": signal.get("symbol", ""),
        "timeframe": signal.get("timeframe", ""),
        "signal_type": signal.get("signal_type", ""),
        "strength": signal.get("strength", 0.5),
        "price_at_signal": signal.get("price_at_signal", 0),
        "stop_loss_price": signal.get("stop_loss_price"),
        "take_profit_price": signal.get("take_profit_price"),
        "triggered_by": json.dumps(signal.get("triggered_by", []), ensure_ascii=False),
        "feature_snapshot": json.dumps(signal.get("feature_snapshot", {}), ensure_ascii=False, default=str),
        "confidence": signal.get("confidence"),
        "ttl_seconds": signal.get("ttl_seconds"),
        "metadata": json.dumps(signal.get("metadata", {}), ensure_ascii=False, default=str),
    }
    await asyncio.to_thread(mysql.upsert, data, "dex_signals")


async def list_signals(
    strategy_id: str = None,
    symbol: str = None,
    limit: int = 100,
) -> list[dict]:
    """查询信号列表。"""
    conditions = []
    params = []
    if strategy_id:
        conditions.append("strategy_id = %s")
        params.append(strategy_id)
    if symbol:
        conditions.append("symbol = %s")
        params.append(symbol)

    where_clause = " AND ".join(conditions) if conditions else "1=1"
    sql = f"SELECT * FROM dex_signals WHERE {where_clause} ORDER BY created_at DESC LIMIT %s"
    params.append(limit)

    rows = await asyncio.to_thread(mysql.execute_sql, sql, tuple(params), True)
    return rows


async def get_signal(signal_id: str) -> Optional[dict]:
    """按 ID 获取单条信号。"""
    rows = await asyncio.to_thread(
        mysql.select_where, "dex_signals", {"signal_id": signal_id}, True
    )
    return rows[0] if rows else None


# ═══════════════════════════════════════════
#  K 线缓存
# ═══════════════════════════════════════════


async def save_kline_cache(
    cache_key: str,
    symbol: str,
    interval: str,
    market: str,
    data_json: str,
    row_count: int,
) -> None:
    """写入 K 线缓存。"""
    data = {
        "cache_key": cache_key,
        "symbol": symbol,
        "interval": interval,
        "market": market,
        "data_json": data_json,
        "row_count": row_count,
    }
    await asyncio.to_thread(mysql.upsert, data, "dex_kline_cache")
