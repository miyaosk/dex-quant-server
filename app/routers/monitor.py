"""
策略监控 API — 服务器端定时执行策略、生成信号、自动下单

限制：每个用户同时最多运行 3 个策略监控，超出需在本地运行。

持久化：
  - 监控任务存 dex_monitor_jobs 表，服务器重启自动恢复 running 任务
  - 每轮产生的可执行信号存 dex_monitor_signals 表
  - 任务状态实时同步到 DB

自动下单：
  - 用户通过 Vault 安全链接提交私钥后，信号产生时自动下单到 Hyperliquid
  - 未配置 Vault 的用户仅推送信号，不下单

API:
  1. POST /monitor/start      — 启动监控
  2. GET  /monitor/list        — 列出我的监控任务
  3. GET  /monitor/{job_id}    — 查看状态 + 最近信号
  4. POST /monitor/{job_id}/stop — 停止监控
"""

import asyncio
import json
import time
import uuid
from datetime import datetime

from fastapi import APIRouter, Header, HTTPException
from loguru import logger
from pydantic import BaseModel, Field

from app import config, database
from app.routers.auth import validate_token
from app.core.script_executor import execute_strategy, ScriptSecurityError


def _validate_admin_key(x_admin_key: str):
    admin_key = config.ADMIN_API_KEY
    if not admin_key:
        raise HTTPException(status_code=503, detail="管理员密钥未配置，请设置环境变量 ADMIN_API_KEY")
    if not x_admin_key or x_admin_key != admin_key:
        raise HTTPException(status_code=403, detail="无权访问，需要管理员密钥")

router = APIRouter(prefix="/monitor", tags=["策略监控"])

MAX_CONCURRENT_MONITORS = 3
SCRIPT_TIMEOUT = 120


# ═══════════════ 请求模型 ═══════════════

class MonitorStartRequest(BaseModel):
    script_content: str = Field(description="策略脚本源码")
    strategy_name: str = Field(default="", description="策略名称")
    symbol: str = Field(default="BTCUSDT")
    timeframe: str = Field(default="4h")
    interval_seconds: int = Field(default=14400, description="执行间隔（秒），默认 4h")
    risk_rules: dict = Field(default_factory=lambda: {
        "min_confidence": 0.6,
        "max_position_pct": 10.0,
        "max_concurrent": 3,
    })


# ═══════════════ 内存中的 asyncio 任务引用 ═══════════════

_running_tasks: dict[str, asyncio.Task] = {}


# ═══════════════ API 路由 ═══════════════


@router.post("/start")
async def start_monitor(req: MonitorStartRequest, x_token: str = Header(default="")):
    """启动策略监控任务。同一用户最多同时 3 个。"""
    record = await validate_token(x_token)
    machine_code = record["machine_code"]

    active_count = await database.count_running_monitors_by_machine(machine_code)
    if active_count >= MAX_CONCURRENT_MONITORS:
        raise HTTPException(
            status_code=429,
            detail=f"同时最多运行 {MAX_CONCURRENT_MONITORS} 个策略监控，请先停止一个或改用本地运行",
        )

    if req.interval_seconds < 60:
        raise HTTPException(status_code=400, detail="间隔不能小于 60 秒")
    if req.interval_seconds > 86400:
        raise HTTPException(status_code=400, detail="间隔不能超过 24 小时")

    job_id = f"mon_{uuid.uuid4().hex[:12]}"
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    job = {
        "job_id": job_id,
        "machine_code": machine_code,
        "strategy_name": req.strategy_name or "unnamed",
        "script_content": req.script_content,
        "symbol": req.symbol,
        "timeframe": req.timeframe,
        "interval_seconds": req.interval_seconds,
        "risk_rules": req.risk_rules,
        "status": "running",
        "total_cycles": 0,
        "total_signals": 0,
        "last_run_at": None,
        "last_error": "",
        "created_at": now_str,
    }

    await database.save_monitor_job(job)

    task = asyncio.create_task(_monitor_loop(job_id, job))
    _running_tasks[job_id] = task

    logger.info(
        f"[{job_id}] 监控已启动 | {req.strategy_name} | {req.symbol} {req.timeframe} "
        f"| 间隔={req.interval_seconds}s | 已用 {active_count+1}/{MAX_CONCURRENT_MONITORS}"
    )

    return {
        "job_id": job_id,
        "status": "running",
        "strategy_name": req.strategy_name,
        "symbol": req.symbol,
        "interval_seconds": req.interval_seconds,
        "quota_used": active_count + 1,
        "quota_max": MAX_CONCURRENT_MONITORS,
        "message": f"监控已启动，每 {req.interval_seconds}s 执行一次。",
    }


@router.get("/list")
async def list_monitors(x_token: str = Header(default="")):
    """列出当前用户的所有监控任务（从 DB 读取）。"""
    record = await validate_token(x_token)
    machine_code = record["machine_code"]

    monitors = await database.list_monitor_jobs_by_machine(machine_code)
    active_count = sum(1 for m in monitors if m.get("status") == "running")

    return {
        "monitors": [
            {
                "job_id": m["job_id"],
                "strategy_name": m.get("strategy_name", ""),
                "symbol": m.get("symbol", ""),
                "timeframe": m.get("timeframe", ""),
                "interval_seconds": m.get("interval_seconds", 0),
                "status": m.get("status", ""),
                "total_cycles": m.get("total_cycles", 0),
                "total_signals": m.get("total_signals", 0),
                "last_run_at": str(m["last_run_at"]) if m.get("last_run_at") else "",
                "created_at": str(m.get("created_at", "")),
            }
            for m in monitors
        ],
        "quota_used": active_count,
        "quota_max": MAX_CONCURRENT_MONITORS,
    }


@router.get("/{job_id}")
async def get_monitor_status(job_id: str, x_token: str = Header(default="")):
    """查看监控任务状态 + 最近信号（从 DB 读取）。"""
    record = await validate_token(x_token)

    job = await database.get_monitor_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail=f"监控任务 {job_id} 不存在")
    if job["machine_code"] != record["machine_code"]:
        raise HTTPException(status_code=403, detail="无权访问该监控任务")

    recent_signals = await database.get_monitor_signals(job_id, limit=20)
    signal_list = [
        {
            "timestamp": s.get("timestamp", ""),
            "symbol": s.get("symbol", ""),
            "action": s.get("action", ""),
            "direction": s.get("direction", ""),
            "confidence": s.get("confidence", 0),
            "price_at_signal": s.get("price_at_signal", 0),
            "reason": s.get("reason", ""),
            "created_at": str(s.get("created_at", "")),
        }
        for s in recent_signals
    ]

    risk_rules = job.get("risk_rules", {})
    if isinstance(risk_rules, str):
        try:
            risk_rules = json.loads(risk_rules)
        except json.JSONDecodeError:
            risk_rules = {}

    return {
        "job_id": job["job_id"],
        "strategy_name": job.get("strategy_name", ""),
        "symbol": job.get("symbol", ""),
        "timeframe": job.get("timeframe", ""),
        "interval_seconds": job.get("interval_seconds", 0),
        "status": job.get("status", ""),
        "total_cycles": job.get("total_cycles", 0),
        "total_signals": job.get("total_signals", 0),
        "last_run_at": str(job["last_run_at"]) if job.get("last_run_at") else "",
        "last_error": job.get("last_error", ""),
        "last_signals": signal_list,
        "created_at": str(job.get("created_at", "")),
        "risk_rules": risk_rules,
    }


@router.post("/{job_id}/stop")
async def stop_monitor(job_id: str, x_token: str = Header(default="")):
    """停止监控任务。"""
    record = await validate_token(x_token)

    job = await database.get_monitor_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail=f"监控任务 {job_id} 不存在")
    if job["machine_code"] != record["machine_code"]:
        raise HTTPException(status_code=403, detail="无权操作该监控任务")

    if job.get("status") == "running":
        await database.update_monitor_status(job_id, status="stopped")

        task = _running_tasks.pop(job_id, None)
        if task and not task.done():
            task.cancel()

        logger.info(f"[{job_id}] 监控已停止 | {job.get('strategy_name', '')}")

    active_count = await database.count_running_monitors_by_machine(record["machine_code"])

    return {
        "job_id": job_id,
        "status": "stopped",
        "message": "监控已停止",
        "quota_used": active_count,
        "quota_max": MAX_CONCURRENT_MONITORS,
    }


@router.get("/admin/all")
async def admin_list_all_monitors(x_admin_key: str = Header(default="")):
    """[管理员] 查看所有用户的监控任务。"""
    _validate_admin_key(x_admin_key)

    jobs = await database.list_all_running_jobs_for_report()
    return {
        "total": len(jobs),
        "monitors": [
            {
                "job_id": j["job_id"],
                "machine_code": j.get("machine_code", ""),
                "strategy_name": j.get("strategy_name", ""),
                "symbol": j.get("symbol", ""),
                "timeframe": j.get("timeframe", ""),
                "status": j.get("status", ""),
                "total_cycles": j.get("total_cycles", 0),
                "total_signals": j.get("total_signals", 0),
                "created_at": str(j.get("created_at", "")),
            }
            for j in jobs
        ],
    }


@router.get("/{job_id}/signals")
async def get_monitor_signal_history(
    job_id: str,
    limit: int = 50,
    x_token: str = Header(default=""),
):
    """查看监控任务的历史信号记录。"""
    record = await validate_token(x_token)
    job = await database.get_monitor_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail=f"监控任务 {job_id} 不存在")
    if job["machine_code"] != record["machine_code"]:
        raise HTTPException(status_code=403, detail="无权访问")

    signals = await database.get_monitor_signals(job_id, limit=min(limit, 200))
    return {
        "job_id": job_id,
        "total": len(signals),
        "signals": [
            {
                "timestamp": s.get("timestamp", ""),
                "symbol": s.get("symbol", ""),
                "action": s.get("action", ""),
                "direction": s.get("direction", ""),
                "confidence": s.get("confidence", 0),
                "price_at_signal": s.get("price_at_signal", 0),
                "reason": s.get("reason", ""),
                "cycle_num": s.get("cycle_num", 0),
                "created_at": str(s.get("created_at", "")),
            }
            for s in signals
        ],
    }


@router.get("/reports/daily")
async def get_daily_reports(
    start_date: str = "",
    end_date: str = "",
    job_id: str = "",
    machine_code: str = "",
    limit: int = 30,
    x_admin_key: str = Header(default=""),
):
    """查询每日策略报告（仅后台管理员，需 X-Admin-Key 头）。"""
    _validate_admin_key(x_admin_key)

    reports = await database.list_daily_reports(
        machine_code=machine_code or None,
        job_id=job_id or None,
        start_date=start_date or None,
        end_date=end_date or None,
        limit=min(limit, 100),
    )

    result = []
    for r in reports:
        metrics = r.get("metrics_json", {})
        if isinstance(metrics, str):
            try:
                metrics = json.loads(metrics)
            except json.JSONDecodeError:
                metrics = {}

        result.append({
            "report_date": str(r.get("report_date", "")),
            "job_id": r.get("job_id", ""),
            "strategy_name": r.get("strategy_name", ""),
            "symbol": r.get("symbol", ""),
            "timeframe": r.get("timeframe", ""),
            "signals_today": r.get("signals_today", 0),
            "buy_signals": r.get("buy_signals", 0),
            "sell_signals": r.get("sell_signals", 0),
            "avg_confidence": r.get("avg_confidence", 0),
            "status": r.get("status", ""),
            "metrics": metrics,
        })

    return {"reports": result, "total": len(result)}


# ═══════════════ 后台调度循环 ═══════════════


async def _monitor_loop(job_id: str, job_data: dict):
    """后台循环：定时执行策略脚本，信号入库。"""

    script_content = job_data["script_content"]
    interval = job_data.get("interval_seconds", 14400)
    risk_rules = job_data.get("risk_rules", {})
    if isinstance(risk_rules, str):
        try:
            risk_rules = json.loads(risk_rules)
        except json.JSONDecodeError:
            risk_rules = {}
    min_conf = risk_rules.get("min_confidence", 0.6)

    total_cycles = job_data.get("total_cycles", 0)
    total_signals = job_data.get("total_signals", 0)

    await asyncio.sleep(2)

    while True:
        db_job = await database.get_monitor_job(job_id)
        if not db_job or db_job.get("status") != "running":
            break

        total_cycles += 1
        now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        logger.info(f"[{job_id}] cycle {total_cycles} | {job_data.get('strategy_name', '')}")

        last_error = ""
        new_signals = 0

        try:
            result = await asyncio.wait_for(
                asyncio.to_thread(
                    execute_strategy,
                    script_content=script_content,
                    mode="live",
                    start_date=None,
                    end_date=None,
                ),
                timeout=SCRIPT_TIMEOUT,
            )

            signals = result.get("signals", [])
            actionable = [
                s for s in signals
                if s.get("action", "").lower() in ("buy", "sell")
                and s.get("confidence", 0) >= min_conf
            ]

            new_signals = len(actionable)
            total_signals += new_signals

            if actionable:
                await database.save_monitor_signals(job_id, total_cycles, actionable)

                await _try_auto_trade(job_id, job_data, actionable, risk_rules)

            logger.info(
                f"[{job_id}] 信号: 总{len(signals)} / 可执行{new_signals} | "
                f"累计={total_signals}"
            )

        except asyncio.CancelledError:
            logger.info(f"[{job_id}] 监控任务已取消")
            break
        except ScriptSecurityError as e:
            last_error = f"脚本安全检查失败: {e}"
            logger.error(f"[{job_id}] {last_error}")
            await database.update_monitor_status(job_id, status="error", last_error=last_error)
            break
        except asyncio.TimeoutError:
            last_error = f"脚本执行超时 ({SCRIPT_TIMEOUT}s)"
            logger.warning(f"[{job_id}] {last_error}")
        except Exception as e:
            last_error = str(e)
            logger.error(f"[{job_id}] 执行异常: {e}")

        await database.update_monitor_status(
            job_id,
            total_cycles=total_cycles,
            total_signals=total_signals,
            last_run_at=now_str,
            last_error=last_error,
        )

        try:
            await asyncio.sleep(interval)
        except asyncio.CancelledError:
            logger.info(f"[{job_id}] 监控任务在等待期间被取消")
            break

    _running_tasks.pop(job_id, None)
    logger.info(f"[{job_id}] 监控循环结束 | 共 {total_cycles} 轮")


# ═══════════════ 自动下单 ═══════════════


async def _try_auto_trade(job_id: str, job_data: dict, signals: list[dict], risk_rules: dict):
    """有 vault 密钥时自动执行交易信号，否则跳过。"""
    machine_code = job_data.get("machine_code", "")
    if not machine_code:
        return

    try:
        from app.routers.vault import get_decrypted_key, get_vault_network
    except ImportError:
        return

    private_key = await get_decrypted_key(machine_code)
    if not private_key:
        return

    network = await get_vault_network(machine_code)
    max_pos_pct = risk_rules.get("max_position_pct", 10.0)
    max_concurrent = risk_rules.get("max_concurrent", 3)

    try:
        from app.core.trade_executor import HyperliquidExecutor
        executor = await asyncio.to_thread(HyperliquidExecutor, private_key, network)
    except Exception as e:
        logger.error(f"[{job_id}] HyperliquidExecutor init failed: {e}")
        return

    for sig in signals:
        try:
            result = await asyncio.to_thread(
                executor.execute_signal,
                symbol=sig.get("symbol", ""),
                action=sig.get("action", ""),
                direction=sig.get("direction", "long"),
                confidence=sig.get("confidence", 0.7),
                max_position_pct=max_pos_pct,
                max_concurrent=max_concurrent,
            )
            status = result.get("status", "unknown")
            if status == "executed":
                logger.info(
                    f"[{job_id}] TRADE {result.get('action','')} {result.get('coin','')} "
                    f"size={result.get('size',0)} @ {result.get('price',0):.2f} | {network}"
                )
            elif status == "skipped":
                logger.info(f"[{job_id}] TRADE skipped: {result.get('reason','')}")
            else:
                logger.warning(f"[{job_id}] TRADE error: {result.get('reason','')}")
        except Exception as e:
            logger.error(f"[{job_id}] TRADE exception: {e}")


# ═══════════════ 启动恢复 ═══════════════


async def restore_running_monitors():
    """服务器启动时从 DB 恢复所有 status='running' 的监控任务。"""
    jobs = await database.list_running_monitor_jobs()
    if not jobs:
        logger.info("无需恢复的监控任务")
        return

    logger.info(f"恢复 {len(jobs)} 个监控任务...")
    for job in jobs:
        job_id = job["job_id"]
        if job_id in _running_tasks:
            continue

        risk_rules = job.get("risk_rules", {})
        if isinstance(risk_rules, str):
            try:
                risk_rules = json.loads(risk_rules)
            except json.JSONDecodeError:
                risk_rules = {}

        job_data = {
            "job_id": job_id,
            "machine_code": job["machine_code"],
            "strategy_name": job.get("strategy_name", ""),
            "script_content": job.get("script_content", ""),
            "symbol": job.get("symbol", ""),
            "timeframe": job.get("timeframe", ""),
            "interval_seconds": job.get("interval_seconds", 14400),
            "risk_rules": risk_rules,
            "total_cycles": job.get("total_cycles", 0),
            "total_signals": job.get("total_signals", 0),
        }

        task = asyncio.create_task(_monitor_loop(job_id, job_data))
        _running_tasks[job_id] = task
        logger.info(f"[{job_id}] 已恢复 | {job.get('strategy_name', '')} | {job.get('symbol', '')}")


# ═══════════════ 每日报告定时任务 ═══════════════


def start_daily_report_scheduler():
    """启动每日凌晨统计任务。"""
    asyncio.create_task(_daily_report_loop())
    logger.info("每日报告定时任务已启动（每天 00:05 执行）")


async def _daily_report_loop():
    """每天凌晨 00:05 统计前一天所有监控策略的信号报告，入库 dex_daily_reports。"""
    while True:
        now = datetime.now()
        tomorrow_midnight = now.replace(hour=0, minute=5, second=0, microsecond=0)
        if now >= tomorrow_midnight:
            from datetime import timedelta
            tomorrow_midnight += timedelta(days=1)

        wait_seconds = (tomorrow_midnight - now).total_seconds()
        logger.info(f"每日报告: 下次执行 {tomorrow_midnight.strftime('%Y-%m-%d %H:%M')}, 等待 {wait_seconds:.0f}s")

        try:
            await asyncio.sleep(wait_seconds)
        except asyncio.CancelledError:
            break

        await _generate_daily_reports()


async def _generate_daily_reports():
    """统计昨天所有监控任务的信号数据，生成每日报告。"""
    from datetime import timedelta

    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    logger.info(f"开始生成 {yesterday} 的每日报告...")

    try:
        all_jobs = await database.list_all_running_jobs_for_report()
    except Exception as e:
        logger.error(f"获取监控任务列表失败: {e}")
        return

    report_count = 0
    for job in all_jobs:
        job_id = job["job_id"]
        try:
            signals = await database.get_monitor_signals(job_id, limit=10000)
            today_signals = [
                s for s in signals
                if str(s.get("created_at", "")).startswith(yesterday)
            ]

            buy_count = sum(1 for s in today_signals if s.get("action") == "buy")
            sell_count = sum(1 for s in today_signals if s.get("action") == "sell")
            confidences = [s.get("confidence", 0) for s in today_signals if s.get("confidence")]
            avg_conf = sum(confidences) / len(confidences) if confidences else 0

            prices = [s.get("price_at_signal", 0) for s in today_signals if s.get("price_at_signal")]
            price_high = max(prices) if prices else 0
            price_low = min(prices) if prices else 0

            report = {
                "report_date": yesterday,
                "job_id": job_id,
                "machine_code": job.get("machine_code", ""),
                "strategy_name": job.get("strategy_name", ""),
                "symbol": job.get("symbol", ""),
                "timeframe": job.get("timeframe", ""),
                "cycles_today": 0,
                "signals_today": len(today_signals),
                "buy_signals": buy_count,
                "sell_signals": sell_count,
                "avg_confidence": round(avg_conf, 4),
                "status": job.get("status", ""),
                "metrics": {
                    "signal_count": len(today_signals),
                    "buy_count": buy_count,
                    "sell_count": sell_count,
                    "avg_confidence": round(avg_conf, 4),
                    "price_high": price_high,
                    "price_low": price_low,
                },
            }

            await database.save_daily_report(report)
            report_count += 1

        except Exception as e:
            logger.error(f"[{job_id}] 生成每日报告失败: {e}")

    logger.info(f"每日报告完成 | 日期={yesterday} | 共 {report_count} 个策略")
