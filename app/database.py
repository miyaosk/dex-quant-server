"""
数据库层 — 信号驱动架构的 MySQL 表结构

表:
  dex_machine_tokens   — 机器码 → Token 映射
  dex_strategies       — 策略定义（含脚本源码，关联 machine_code）
  dex_backtest_results — 回测结果（含信号快照）
  dex_kline_cache      — K 线缓存
  dex_signals          — 策略信号记录
  dex_monitor_jobs     — 监控任务（持久化，重启可恢复）
  dex_daily_reports    — 每日策略统计报告
  dex_vault_keys       — 加密存储的交易私钥
  dex_vault_tokens     — 一次性密钥提交链接 token
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
    """
    CREATE TABLE IF NOT EXISTS dex_monitor_jobs (
        job_id VARCHAR(64) PRIMARY KEY COMMENT '监控任务ID',
        machine_code VARCHAR(64) NOT NULL COMMENT '所属用户',
        strategy_name VARCHAR(255) DEFAULT '' COMMENT '策略名称',
        script_content LONGTEXT NOT NULL COMMENT '策略脚本源码',
        symbol VARCHAR(64) DEFAULT 'BTCUSDT' COMMENT '交易对',
        timeframe VARCHAR(16) DEFAULT '4h' COMMENT 'K线周期',
        interval_seconds INT DEFAULT 14400 COMMENT '执行间隔（秒）',
        risk_rules JSON COMMENT '风控规则',
        status VARCHAR(32) DEFAULT 'running' COMMENT 'running/stopped/error',
        total_cycles INT DEFAULT 0 COMMENT '已执行轮次',
        total_signals INT DEFAULT 0 COMMENT '累计可执行信号数',
        last_run_at DATETIME COMMENT '最后执行时间',
        last_error TEXT COMMENT '最后一次错误',
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
        INDEX idx_machine (machine_code),
        INDEX idx_status (status),
        INDEX idx_created (created_at)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='策略监控任务表'
    """,
    """
    CREATE TABLE IF NOT EXISTS dex_monitor_signals (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        job_id VARCHAR(64) NOT NULL COMMENT '关联监控任务',
        cycle_num INT DEFAULT 0 COMMENT '第几轮产生的',
        timestamp VARCHAR(64) NOT NULL COMMENT '信号时间戳',
        symbol VARCHAR(64) NOT NULL COMMENT '交易对',
        action VARCHAR(16) NOT NULL COMMENT 'buy/sell',
        direction VARCHAR(16) DEFAULT 'long' COMMENT 'long/short',
        confidence DOUBLE DEFAULT 1.0 COMMENT '置信度',
        reason TEXT COMMENT '触发原因',
        price_at_signal DOUBLE DEFAULT 0 COMMENT '信号时价格',
        suggested_stop_loss DOUBLE COMMENT '建议止损',
        suggested_take_profit DOUBLE COMMENT '建议止盈',
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '入库时间',
        INDEX idx_job (job_id),
        INDEX idx_symbol (symbol),
        INDEX idx_action (action),
        INDEX idx_created (created_at)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='监控产生的信号记录'
    """,
    """
    CREATE TABLE IF NOT EXISTS dex_daily_reports (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        report_date DATE NOT NULL COMMENT '报告日期',
        job_id VARCHAR(64) NOT NULL COMMENT '关联监控任务',
        machine_code VARCHAR(64) NOT NULL COMMENT '所属用户',
        strategy_name VARCHAR(255) DEFAULT '' COMMENT '策略名称',
        symbol VARCHAR(64) DEFAULT '' COMMENT '交易对',
        timeframe VARCHAR(16) DEFAULT '' COMMENT 'K线周期',
        cycles_today INT DEFAULT 0 COMMENT '当日执行轮次',
        signals_today INT DEFAULT 0 COMMENT '当日信号数',
        buy_signals INT DEFAULT 0 COMMENT '买入信号数',
        sell_signals INT DEFAULT 0 COMMENT '卖出信号数',
        avg_confidence DOUBLE DEFAULT 0 COMMENT '平均置信度',
        status VARCHAR(32) DEFAULT '' COMMENT '任务状态',
        metrics_json JSON COMMENT '当日绩效指标（模拟盈亏等）',
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '报告生成时间',
        UNIQUE KEY uk_date_job (report_date, job_id),
        INDEX idx_machine (machine_code),
        INDEX idx_date (report_date),
        INDEX idx_job (job_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='每日策略统计报告'
    """,
    """
    CREATE TABLE IF NOT EXISTS dex_vault_keys (
        id INT AUTO_INCREMENT PRIMARY KEY,
        machine_code VARCHAR(128) NOT NULL COMMENT '所属用户',
        key_name VARCHAR(64) NOT NULL DEFAULT 'hyperliquid' COMMENT '密钥用途标识',
        encrypted_key TEXT NOT NULL COMMENT 'AES-GCM 加密后的私钥 (base64)',
        iv VARCHAR(64) NOT NULL COMMENT '初始化向量 (base64)',
        tag VARCHAR(64) NOT NULL DEFAULT '' COMMENT 'GCM 认证标签 (base64)',
        network VARCHAR(32) NOT NULL DEFAULT 'mainnet' COMMENT 'mainnet / testnet',
        status VARCHAR(16) NOT NULL DEFAULT 'active' COMMENT 'active / revoked',
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY uq_machine_key (machine_code, key_name),
        INDEX idx_status (status)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='加密存储的交易私钥'
    """,
    """
    CREATE TABLE IF NOT EXISTS dex_vault_tokens (
        id INT AUTO_INCREMENT PRIMARY KEY,
        token VARCHAR(128) NOT NULL COMMENT '一次性提交 token',
        machine_code VARCHAR(128) NOT NULL COMMENT '关联用户',
        used TINYINT DEFAULT 0 COMMENT '0=未使用 1=已使用',
        expires_at DATETIME NOT NULL COMMENT '过期时间',
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        UNIQUE KEY uq_token (token),
        INDEX idx_machine (machine_code),
        INDEX idx_expires (expires_at)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='一次性密钥提交链接 token'
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
    allowed_strategy_ids: list[str] = None,
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
    elif allowed_strategy_ids is not None:
        if not allowed_strategy_ids:
            return []
        placeholders = ",".join(["%s"] * len(allowed_strategy_ids))
        conditions.append(f"strategy_id IN ({placeholders})")
        params.extend(allowed_strategy_ids)
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


# ═══════════════════════════════════════════
#  监控任务
# ═══════════════════════════════════════════


async def save_monitor_job(job: dict) -> None:
    data = {
        "job_id": job["job_id"],
        "machine_code": job["machine_code"],
        "strategy_name": job.get("strategy_name", ""),
        "script_content": job["script_content"],
        "symbol": job.get("symbol", "BTCUSDT"),
        "timeframe": job.get("timeframe", "4h"),
        "interval_seconds": job.get("interval_seconds", 14400),
        "risk_rules": json.dumps(job.get("risk_rules", {}), ensure_ascii=False),
        "status": job.get("status", "running"),
        "total_cycles": job.get("total_cycles", 0),
        "total_signals": job.get("total_signals", 0),
        "last_run_at": job.get("last_run_at") or None,
        "last_error": job.get("last_error", ""),
    }
    await asyncio.to_thread(mysql.upsert, data, "dex_monitor_jobs")


async def update_monitor_status(job_id: str, **kwargs) -> None:
    sets = []
    params = []
    for k, v in kwargs.items():
        sets.append(f"{k} = %s")
        params.append(v)
    if not sets:
        return
    params.append(job_id)
    sql = f"UPDATE dex_monitor_jobs SET {', '.join(sets)} WHERE job_id = %s"
    await asyncio.to_thread(mysql.execute_sql, sql, tuple(params))


async def get_monitor_job(job_id: str) -> Optional[dict]:
    rows = await asyncio.to_thread(
        mysql.select_where, "dex_monitor_jobs", {"job_id": job_id}, True
    )
    return rows[0] if rows else None


async def list_monitor_jobs_by_machine(machine_code: str) -> list[dict]:
    rows = await asyncio.to_thread(
        mysql.execute_sql,
        "SELECT job_id, machine_code, strategy_name, symbol, timeframe, interval_seconds, "
        "status, total_cycles, total_signals, last_run_at, last_error, created_at "
        "FROM dex_monitor_jobs WHERE machine_code = %s ORDER BY created_at DESC",
        (machine_code,),
        True,
    )
    return rows


async def list_running_monitor_jobs() -> list[dict]:
    """获取所有状态为 running 的监控任务（服务器启动时恢复用）。"""
    rows = await asyncio.to_thread(
        mysql.execute_sql,
        "SELECT * FROM dex_monitor_jobs WHERE status = 'running'",
        None,
        True,
    )
    return rows


async def count_running_monitors_by_machine(machine_code: str) -> int:
    rows = await asyncio.to_thread(
        mysql.execute_sql,
        "SELECT COUNT(*) as cnt FROM dex_monitor_jobs WHERE machine_code = %s AND status = 'running'",
        (machine_code,),
        True,
    )
    return rows[0]["cnt"] if rows else 0


# ═══════════════════════════════════════════
#  监控信号记录
# ═══════════════════════════════════════════


async def save_monitor_signals(job_id: str, cycle_num: int, signals: list[dict]) -> None:
    if not signals:
        return

    sql = (
        "INSERT INTO dex_monitor_signals "
        "(job_id, cycle_num, timestamp, symbol, action, direction, confidence, "
        "reason, price_at_signal, suggested_stop_loss, suggested_take_profit) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    )
    rows = []
    for s in signals:
        rows.append((
            job_id, cycle_num,
            s.get("timestamp", ""),
            s.get("symbol", ""),
            s.get("action", ""),
            s.get("direction", "long"),
            s.get("confidence", 1.0),
            s.get("reason", "")[:500],
            s.get("price_at_signal", 0),
            s.get("suggested_stop_loss"),
            s.get("suggested_take_profit"),
        ))

    def _batch_insert():
        for row in rows:
            mysql.execute_sql(sql, row)

    await asyncio.to_thread(_batch_insert)


async def get_monitor_signals(job_id: str, limit: int = 50) -> list[dict]:
    rows = await asyncio.to_thread(
        mysql.execute_sql,
        "SELECT * FROM dex_monitor_signals WHERE job_id = %s ORDER BY created_at DESC LIMIT %s",
        (job_id, limit),
        True,
    )
    return rows


async def get_monitor_signals_today(job_id: str) -> list[dict]:
    rows = await asyncio.to_thread(
        mysql.execute_sql,
        "SELECT * FROM dex_monitor_signals WHERE job_id = %s AND DATE(created_at) = CURDATE() "
        "ORDER BY created_at DESC",
        (job_id,),
        True,
    )
    return rows


# ═══════════════════════════════════════════
#  每日报告
# ═══════════════════════════════════════════


async def save_daily_report(report: dict) -> None:
    data = {
        "report_date": report["report_date"],
        "job_id": report["job_id"],
        "machine_code": report["machine_code"],
        "strategy_name": report.get("strategy_name", ""),
        "symbol": report.get("symbol", ""),
        "timeframe": report.get("timeframe", ""),
        "cycles_today": report.get("cycles_today", 0),
        "signals_today": report.get("signals_today", 0),
        "buy_signals": report.get("buy_signals", 0),
        "sell_signals": report.get("sell_signals", 0),
        "avg_confidence": report.get("avg_confidence", 0),
        "status": report.get("status", ""),
        "metrics_json": json.dumps(report.get("metrics", {}), ensure_ascii=False, default=str),
    }
    await asyncio.to_thread(mysql.upsert, data, "dex_daily_reports")


async def list_daily_reports(
    machine_code: str = None,
    job_id: str = None,
    start_date: str = None,
    end_date: str = None,
    limit: int = 30,
) -> list[dict]:
    conditions = []
    params = []
    if machine_code:
        conditions.append("machine_code = %s")
        params.append(machine_code)
    if job_id:
        conditions.append("job_id = %s")
        params.append(job_id)
    if start_date:
        conditions.append("report_date >= %s")
        params.append(start_date)
    if end_date:
        conditions.append("report_date <= %s")
        params.append(end_date)

    where = " AND ".join(conditions) if conditions else "1=1"
    sql = f"SELECT * FROM dex_daily_reports WHERE {where} ORDER BY report_date DESC, strategy_name LIMIT %s"
    params.append(limit)

    rows = await asyncio.to_thread(mysql.execute_sql, sql, tuple(params), True)
    return rows


async def list_all_running_jobs_for_report() -> list[dict]:
    """获取所有运行中/已停止的监控任务（用于每日报告统计）。"""
    rows = await asyncio.to_thread(
        mysql.execute_sql,
        "SELECT job_id, machine_code, strategy_name, symbol, timeframe, status, "
        "total_cycles, total_signals, created_at "
        "FROM dex_monitor_jobs WHERE status IN ('running', 'stopped', 'error') "
        "ORDER BY machine_code, created_at",
        None,
        True,
    )
    return rows


# ═══════════════════════════════════════════
#  Vault — 密钥保险箱
# ═══════════════════════════════════════════


async def create_vault_token(token: str, machine_code: str, expires_at: str) -> None:
    data = {"token": token, "machine_code": machine_code, "expires_at": expires_at, "used": 0}
    await asyncio.to_thread(mysql.upsert, data, "dex_vault_tokens")


async def get_vault_token(token: str) -> Optional[dict]:
    rows = await asyncio.to_thread(
        mysql.select_where, "dex_vault_tokens", {"token": token}, True
    )
    return rows[0] if rows else None


async def mark_vault_token_used(token: str) -> None:
    await asyncio.to_thread(
        mysql.execute_sql,
        "UPDATE dex_vault_tokens SET used = 1 WHERE token = %s",
        (token,),
    )


async def save_vault_key(
    machine_code: str,
    encrypted_key: str,
    iv: str,
    tag: str,
    network: str = "mainnet",
    key_name: str = "hyperliquid",
) -> None:
    data = {
        "machine_code": machine_code,
        "key_name": key_name,
        "encrypted_key": encrypted_key,
        "iv": iv,
        "tag": tag,
        "network": network,
        "status": "active",
    }
    await asyncio.to_thread(mysql.upsert, data, "dex_vault_keys")


async def get_vault_key(machine_code: str, key_name: str = "hyperliquid") -> Optional[dict]:
    rows = await asyncio.to_thread(
        mysql.execute_sql,
        "SELECT * FROM dex_vault_keys WHERE machine_code = %s AND key_name = %s AND status = 'active'",
        (machine_code, key_name),
        True,
    )
    return rows[0] if rows else None


async def delete_vault_key(machine_code: str, key_name: str = "hyperliquid") -> bool:
    await asyncio.to_thread(
        mysql.execute_sql,
        "UPDATE dex_vault_keys SET status = 'revoked' WHERE machine_code = %s AND key_name = %s",
        (machine_code, key_name),
    )
    return True


# ═══════════════════════════════════════════
#  Web 排行榜 & 后台统计
# ═══════════════════════════════════════════


async def leaderboard_strategies(
    sort_by: str = "total_return_pct",
    limit: int = 50,
    offset: int = 0,
) -> list[dict]:
    """按回测绩效排序的策略排行榜，每个策略取最新一次已完成的回测。"""
    allowed_sorts = {
        "total_return_pct": "total_return_pct",
        "sharpe_ratio": "sharpe_ratio",
        "win_rate": "win_rate",
        "max_drawdown_pct": "max_drawdown_pct",
        "total_trades": "total_trades",
    }
    order_col = allowed_sorts.get(sort_by, "total_return_pct")

    sql = f"""
        SELECT
            s.strategy_id, s.name, s.symbol, s.timeframe, s.direction, s.status AS strategy_status,
            b.backtest_id, b.conclusion,
            b.metrics_json,
            b.created_at AS backtest_at
        FROM dex_strategies s
        INNER JOIN dex_backtest_results b ON b.backtest_id = (
            SELECT b2.backtest_id FROM dex_backtest_results b2
            WHERE b2.strategy_id = s.strategy_id AND b2.status = 'completed'
            ORDER BY b2.created_at DESC LIMIT 1
        )
        WHERE b.status = 'completed'
        ORDER BY
            CAST(JSON_EXTRACT(b.metrics_json, '$.{order_col}') AS DOUBLE) DESC
        LIMIT %s OFFSET %s
    """
    rows = await asyncio.to_thread(mysql.execute_sql, sql, (limit, offset), True)
    for row in rows:
        if row.get("metrics_json"):
            if isinstance(row["metrics_json"], str):
                row["metrics"] = json.loads(row["metrics_json"])
            else:
                row["metrics"] = row["metrics_json"]
        else:
            row["metrics"] = {}
    return rows


async def get_strategy_detail_with_backtest(strategy_id: str) -> Optional[dict]:
    """获取策略详情 + 最新回测结果。"""
    strategy = await get_strategy(strategy_id)
    if not strategy:
        return None

    backtests = await asyncio.to_thread(
        mysql.execute_sql,
        "SELECT backtest_id, strategy_name, conclusion, status, metrics_json, "
        "trades_json, equity_json, created_at, elapsed_ms "
        "FROM dex_backtest_results WHERE strategy_id = %s ORDER BY created_at DESC LIMIT 5",
        (strategy_id,),
        True,
    )
    for bt in backtests:
        for field in ("metrics_json", "trades_json", "equity_json"):
            if bt.get(field) and isinstance(bt[field], str):
                try:
                    bt[field] = json.loads(bt[field])
                except (json.JSONDecodeError, TypeError):
                    pass

    strategy["backtests"] = backtests
    return strategy


async def admin_dashboard_stats() -> dict:
    """后台 Dashboard 统计数据。"""
    queries = {
        "total_users": "SELECT COUNT(*) AS cnt FROM dex_machine_tokens",
        "active_users": "SELECT COUNT(*) AS cnt FROM dex_machine_tokens WHERE status = 'active'",
        "total_strategies": "SELECT COUNT(*) AS cnt FROM dex_strategies",
        "total_backtests": "SELECT COUNT(*) AS cnt FROM dex_backtest_results",
        "completed_backtests": "SELECT COUNT(*) AS cnt FROM dex_backtest_results WHERE status = 'completed'",
        "running_monitors": "SELECT COUNT(*) AS cnt FROM dex_monitor_jobs WHERE status = 'running'",
        "total_monitors": "SELECT COUNT(*) AS cnt FROM dex_monitor_jobs",
        "total_signals": "SELECT COUNT(*) AS cnt FROM dex_monitor_signals",
    }
    stats = {}

    def _run():
        for key, sql in queries.items():
            rows = mysql.execute_sql(sql, None, True)
            stats[key] = rows[0]["cnt"] if rows else 0

    await asyncio.to_thread(_run)
    return stats


async def admin_list_users(limit: int = 100, offset: int = 0) -> list[dict]:
    """管理后台用户列表，附带策略数和监控数。"""
    sql = """
        SELECT
            t.id, t.machine_code, t.token, t.max_strategies, t.status, t.created_at,
            (SELECT COUNT(*) FROM dex_strategies s WHERE s.machine_code = t.machine_code AND s.status != 'archived') AS strategy_count,
            (SELECT COUNT(*) FROM dex_monitor_jobs m WHERE m.machine_code = t.machine_code AND m.status = 'running') AS running_monitors
        FROM dex_machine_tokens t
        ORDER BY t.created_at DESC
        LIMIT %s OFFSET %s
    """
    rows = await asyncio.to_thread(mysql.execute_sql, sql, (limit, offset), True)
    return rows


async def admin_list_all_monitors(limit: int = 100, offset: int = 0) -> list[dict]:
    """管理后台全部监控任务。"""
    sql = """
        SELECT job_id, machine_code, strategy_name, symbol, timeframe,
               interval_seconds, status, total_cycles, total_signals,
               last_run_at, last_error, created_at, updated_at
        FROM dex_monitor_jobs
        ORDER BY
            FIELD(status, 'running', 'error', 'stopped'),
            updated_at DESC
        LIMIT %s OFFSET %s
    """
    rows = await asyncio.to_thread(mysql.execute_sql, sql, (limit, offset), True)
    return rows


async def admin_list_all_strategies(limit: int = 100, offset: int = 0) -> list[dict]:
    """管理后台全部策略列表。"""
    sql = """
        SELECT s.strategy_id, s.machine_code, s.name, s.symbol, s.timeframe,
               s.direction, s.version, s.status, s.created_at, s.updated_at,
               (SELECT COUNT(*) FROM dex_backtest_results b WHERE b.strategy_id = s.strategy_id) AS backtest_count
        FROM dex_strategies s
        ORDER BY s.updated_at DESC
        LIMIT %s OFFSET %s
    """
    rows = await asyncio.to_thread(mysql.execute_sql, sql, (limit, offset), True)
    return rows


async def admin_list_all_backtests(limit: int = 100, offset: int = 0) -> list[dict]:
    """管理后台全部回测列表。"""
    sql = """
        SELECT backtest_id, strategy_id, strategy_name, conclusion, status,
               metrics_json, elapsed_ms, created_at
        FROM dex_backtest_results
        ORDER BY created_at DESC
        LIMIT %s OFFSET %s
    """
    rows = await asyncio.to_thread(mysql.execute_sql, sql, (limit, offset), True)
    for row in rows:
        if row.get("metrics_json"):
            if isinstance(row["metrics_json"], str):
                try:
                    row["metrics"] = json.loads(row["metrics_json"])
                except (json.JSONDecodeError, TypeError):
                    row["metrics"] = {}
            else:
                row["metrics"] = row["metrics_json"]
        else:
            row["metrics"] = {}
    return rows


async def public_list_monitors(limit: int = 50) -> list[dict]:
    """公开的监控任务列表（仅展示 running 状态）。"""
    sql = """
        SELECT job_id, strategy_name, symbol, timeframe, interval_seconds,
               status, total_cycles, total_signals, last_run_at, created_at
        FROM dex_monitor_jobs
        WHERE status = 'running'
        ORDER BY last_run_at DESC
        LIMIT %s
    """
    rows = await asyncio.to_thread(mysql.execute_sql, sql, (limit,), True)
    return rows
