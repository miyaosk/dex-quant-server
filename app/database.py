"""
数据库层 — 信号驱动架构的 MySQL 表结构

表:
  dex_machine_tokens   — 机器码 → Token 映射（免费 3 策略配额）
  dex_strategies       — 策略定义（含脚本源码，关联 machine_code）
  dex_backtest_results — 回测结果（含信号快照）
  dex_kline_cache      — K 线缓存
  dex_signals          — 策略信号记录
"""

from __future__ import annotations

import asyncio
import json
import uuid
from typing import Optional

from loguru import logger

from app.utils.mysql_client import mysql

# ═══════════════════════════════════════════
#  建表 SQL
# ═══════════════════════════════════════════

_CREATE_TABLES_SQL = [
    """
    CREATE TABLE IF NOT EXISTS dex_machine_tokens (
        id INT AUTO_INCREMENT PRIMARY KEY,
        machine_code VARCHAR(64) NOT NULL COMMENT '客户端硬件指纹哈希',
        token VARCHAR(128) NOT NULL COMMENT '分配的 API Token',
        max_strategies INT DEFAULT 3 COMMENT '最大策略数（免费配额）',
        status VARCHAR(32) DEFAULT 'active' COMMENT 'active / suspended',
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY uk_machine_code (machine_code),
        UNIQUE KEY uk_token (token),
        INDEX idx_status (status)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='机器码-Token 映射表'
    """,
    """
    CREATE TABLE IF NOT EXISTS dex_strategies (
        strategy_id VARCHAR(64) PRIMARY KEY COMMENT '策略唯一ID',
        machine_code VARCHAR(64) DEFAULT '' COMMENT '所属机器码',
        name VARCHAR(255) NOT NULL COMMENT '策略名称',
        description TEXT COMMENT '策略描述（自然语言）',
        script_content LONGTEXT COMMENT '策略脚本源码（.py）',
        symbol VARCHAR(64) DEFAULT 'BTCUSDT' COMMENT '主交易对',
        timeframe VARCHAR(16) DEFAULT '1h' COMMENT 'K线周期: 15m/1h/2h/1d',
        direction VARCHAR(32) DEFAULT 'long_short' COMMENT '方向: long/short/long_short',
        version VARCHAR(32) DEFAULT 'v1.0' COMMENT '版本号',
        tags TEXT COMMENT '标签JSON数组',
        status VARCHAR(32) DEFAULT 'draft' COMMENT '状态: draft/active/archived',
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
        INDEX idx_machine_code (machine_code),
        INDEX idx_status (status),
        INDEX idx_symbol (symbol),
        INDEX idx_updated (updated_at)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='策略定义表（脚本为核心）'
    """,
    """
    CREATE TABLE IF NOT EXISTS dex_backtest_results (
        backtest_id VARCHAR(64) PRIMARY KEY COMMENT '回测任务唯一ID',
        strategy_id VARCHAR(64) NOT NULL COMMENT '关联策略ID',
        strategy_name VARCHAR(255) DEFAULT '' COMMENT '策略名称快照',
        config_json JSON NOT NULL COMMENT '回测配置',
        signals_json LONGTEXT COMMENT '回测使用的信号列表（JSON快照）',
        metrics_json JSON COMMENT '绩效指标',
        trades_json LONGTEXT COMMENT '交易记录JSON',
        equity_json LONGTEXT COMMENT '权益曲线JSON',
        conclusion VARCHAR(32) DEFAULT '' COMMENT '评估结论: approved/paper_trade_first/rejected/failed',
        status VARCHAR(32) DEFAULT 'running' COMMENT '状态: running/completed/failed',
        error TEXT COMMENT '错误信息',
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
        elapsed_ms INT DEFAULT 0 COMMENT '耗时(毫秒)',
        INDEX idx_strategy (strategy_id),
        INDEX idx_status (status),
        INDEX idx_conclusion (conclusion),
        INDEX idx_created (created_at)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='回测结果表'
    """,
    """
    CREATE TABLE IF NOT EXISTS dex_kline_cache (
        cache_key VARCHAR(512) PRIMARY KEY COMMENT '缓存键',
        symbol VARCHAR(64) NOT NULL COMMENT '交易对',
        `interval` VARCHAR(16) NOT NULL COMMENT 'K线周期',
        market VARCHAR(32) NOT NULL COMMENT '市场类型',
        data_json LONGTEXT NOT NULL COMMENT 'K线数据JSON',
        row_count INT DEFAULT 0 COMMENT 'K线条数',
        fetched_at DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '拉取时间',
        INDEX idx_symbol (symbol),
        INDEX idx_fetched (fetched_at)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='K线数据缓存表'
    """,
    """
    CREATE TABLE IF NOT EXISTS dex_signals (
        signal_id VARCHAR(64) PRIMARY KEY COMMENT '信号唯一ID',
        strategy_id VARCHAR(64) NOT NULL COMMENT '关联策略ID',
        timestamp VARCHAR(64) NOT NULL COMMENT '信号时间戳',
        symbol VARCHAR(64) NOT NULL COMMENT '交易对',
        action VARCHAR(16) NOT NULL COMMENT 'buy/sell/close/hold',
        direction VARCHAR(16) DEFAULT 'long' COMMENT 'long/short',
        confidence DOUBLE DEFAULT 1.0 COMMENT '置信度(0-1)',
        reason TEXT COMMENT '触发原因',
        source_type VARCHAR(32) DEFAULT 'technical' COMMENT '信号来源类型',
        price_at_signal DOUBLE DEFAULT 0 COMMENT '信号时价格',
        suggested_stop_loss DOUBLE COMMENT '建议止损价',
        suggested_take_profit DOUBLE COMMENT '建议止盈价',
        metadata JSON COMMENT '附加元数据',
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
        INDEX idx_strategy (strategy_id),
        INDEX idx_symbol (symbol),
        INDEX idx_action (action),
        INDEX idx_timestamp (timestamp),
        INDEX idx_created (created_at)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='策略信号表'
    """,
]


# ═══════════════════════════════════════════
#  初始化
# ═══════════════════════════════════════════


_MIGRATIONS_SQL = [
    "ALTER TABLE dex_strategies ADD COLUMN machine_code VARCHAR(64) DEFAULT '' COMMENT '所属机器码' AFTER strategy_id",
    "ALTER TABLE dex_strategies ADD INDEX idx_machine_code (machine_code)",
]


async def init_db() -> None:
    """建表（如果不存在）+ 兼容性迁移。"""
    def _init():
        for sql in _CREATE_TABLES_SQL:
            mysql.execute_sql(sql)
        for sql in _MIGRATIONS_SQL:
            try:
                mysql.execute_sql(sql)
            except Exception:
                pass

    await asyncio.to_thread(_init)
    logger.info(f"MySQL 初始化完成: {mysql.db_conf['host']}:{mysql.db_conf['port']}/{mysql.db_conf['db']}")


# ═══════════════════════════════════════════
#  策略 CRUD
# ═══════════════════════════════════════════


async def save_strategy(
    strategy_id: str,
    name: str,
    description: str = "",
    script_content: str = "",
    symbol: str = "BTCUSDT",
    timeframe: str = "1h",
    direction: str = "long_short",
    version: str = "v1.0",
    tags: str = "[]",
    status: str = "draft",
    machine_code: str = "",
) -> None:
    data = {
        "strategy_id": strategy_id,
        "machine_code": machine_code,
        "name": name,
        "description": description,
        "script_content": script_content,
        "symbol": symbol,
        "timeframe": timeframe,
        "direction": direction,
        "version": version,
        "tags": tags,
        "status": status,
    }
    await asyncio.to_thread(mysql.upsert, data, "dex_strategies")


async def get_strategy(strategy_id: str) -> Optional[dict]:
    rows = await asyncio.to_thread(
        mysql.select_where, "dex_strategies", {"strategy_id": strategy_id}, True
    )
    return rows[0] if rows else None


async def list_strategies() -> list[dict]:
    rows = await asyncio.to_thread(
        mysql.execute_sql,
        "SELECT strategy_id, name, symbol, timeframe, version, status, created_at, updated_at "
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
    strategy_name: str,
    config_json: str,
    signals_json: Optional[str],
    metrics_json: Optional[str],
    trades_json: Optional[str],
    equity_json: Optional[str],
    conclusion: str,
    status: str,
    error: Optional[str],
    elapsed_ms: int,
) -> None:
    data = {
        "backtest_id": backtest_id,
        "strategy_id": strategy_id,
        "strategy_name": strategy_name,
        "config_json": config_json,
        "signals_json": signals_json,
        "metrics_json": metrics_json,
        "trades_json": trades_json,
        "equity_json": equity_json,
        "conclusion": conclusion,
        "status": status,
        "error": error,
        "elapsed_ms": elapsed_ms,
    }
    await asyncio.to_thread(mysql.upsert, data, "dex_backtest_results")


async def get_backtest_result(backtest_id: str) -> Optional[dict]:
    rows = await asyncio.to_thread(
        mysql.select_where, "dex_backtest_results", {"backtest_id": backtest_id}, True
    )
    return rows[0] if rows else None


# ═══════════════════════════════════════════
#  K 线缓存
# ═══════════════════════════════════════════


async def get_cached_klines(cache_key: str) -> Optional[dict]:
    rows = await asyncio.to_thread(
        mysql.execute_sql,
        "SELECT data_json, fetched_at FROM dex_kline_cache WHERE cache_key = %s",
        (cache_key,),
        True,
    )
    return rows[0] if rows else None


async def save_kline_cache(
    cache_key: str,
    symbol: str,
    interval: str,
    market: str,
    data_json: str,
    row_count: int,
) -> None:
    data = {
        "cache_key": cache_key,
        "symbol": symbol,
        "interval": interval,
        "market": market,
        "data_json": data_json,
        "row_count": row_count,
    }
    await asyncio.to_thread(mysql.upsert, data, "dex_kline_cache")


# ═══════════════════════════════════════════
#  信号
# ═══════════════════════════════════════════


async def save_signal(strategy_id: str, signal: dict) -> None:
    signal_id = f"sig_{uuid.uuid4().hex[:12]}"
    data = {
        "signal_id": signal_id,
        "strategy_id": strategy_id,
        "timestamp": signal.get("timestamp", ""),
        "symbol": signal.get("symbol", ""),
        "action": signal.get("action", ""),
        "direction": signal.get("direction", "long"),
        "confidence": signal.get("confidence", 1.0),
        "reason": signal.get("reason", ""),
        "source_type": signal.get("source_type", "technical"),
        "price_at_signal": signal.get("price_at_signal", 0),
        "suggested_stop_loss": signal.get("suggested_stop_loss"),
        "suggested_take_profit": signal.get("suggested_take_profit"),
        "metadata": json.dumps(signal.get("metadata", {}), ensure_ascii=False, default=str),
    }
    await asyncio.to_thread(mysql.upsert, data, "dex_signals")


async def list_signals(
    strategy_id: str = None,
    symbol: str = None,
    start_date: str = None,
    end_date: str = None,
    limit: int = 200,
) -> list[dict]:
    conditions = []
    params = []
    if strategy_id:
        conditions.append("strategy_id = %s")
        params.append(strategy_id)
    if symbol:
        conditions.append("symbol = %s")
        params.append(symbol)
    if start_date:
        conditions.append("timestamp >= %s")
        params.append(start_date)
    if end_date:
        conditions.append("timestamp <= %s")
        params.append(end_date)

    where_clause = " AND ".join(conditions) if conditions else "1=1"
    sql = f"SELECT * FROM dex_signals WHERE {where_clause} ORDER BY timestamp DESC LIMIT %s"
    params.append(limit)

    rows = await asyncio.to_thread(mysql.execute_sql, sql, tuple(params), True)
    return rows


# ═══════════════════════════════════════════
#  机器码 Token & 配额
# ═══════════════════════════════════════════


async def get_token_by_machine_code(machine_code: str) -> Optional[dict]:
    rows = await asyncio.to_thread(
        mysql.select_where, "dex_machine_tokens", {"machine_code": machine_code}, True
    )
    return rows[0] if rows else None


async def get_token_record(token: str) -> Optional[dict]:
    rows = await asyncio.to_thread(
        mysql.select_where, "dex_machine_tokens", {"token": token}, True
    )
    return rows[0] if rows else None


async def create_token(machine_code: str, token: str, max_strategies: int = 3) -> None:
    data = {
        "machine_code": machine_code,
        "token": token,
        "max_strategies": max_strategies,
        "status": "active",
    }
    await asyncio.to_thread(mysql.upsert, data, "dex_machine_tokens")


async def count_strategies_by_machine(machine_code: str) -> int:
    rows = await asyncio.to_thread(
        mysql.execute_sql,
        "SELECT COUNT(*) as cnt FROM dex_strategies WHERE machine_code = %s AND status != 'archived'",
        (machine_code,),
        True,
    )
    return rows[0]["cnt"] if rows else 0


async def list_strategies_by_machine(machine_code: str) -> list[dict]:
    rows = await asyncio.to_thread(
        mysql.execute_sql,
        "SELECT strategy_id, name, symbol, timeframe, version, status, created_at, updated_at "
        "FROM dex_strategies WHERE machine_code = %s AND status != 'archived' ORDER BY updated_at DESC",
        (machine_code,),
        True,
    )
    return rows
