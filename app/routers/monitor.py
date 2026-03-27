"""
策略监控 API — 服务器端定时执行策略、生成信号

配额：每个用户（machine_code）免费 3 个监控任务。

  1. POST /monitor/start      — 启动监控（上传脚本+配置）
  2. GET  /monitor/list        — 列出我的监控任务
  3. GET  /monitor/{job_id}    — 查看状态 + 最近信号
  4. POST /monitor/{job_id}/stop — 停止监控
"""

import asyncio
import time
import uuid
from datetime import datetime

from fastapi import APIRouter, Header, HTTPException
from loguru import logger
from pydantic import BaseModel, Field

from app import database
from app.routers.auth import validate_token
from app.core.script_executor import execute_strategy, ScriptSecurityError

router = APIRouter(prefix="/monitor", tags=["策略监控"])

FREE_MONITOR_SLOTS = 3
SCRIPT_TIMEOUT = 120

# ═══════════════ 请求/响应模型 ═══════════════

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


class MonitorStatusResponse(BaseModel):
    job_id: str
    strategy_name: str
    symbol: str
    timeframe: str
    interval_seconds: int
    status: str
    total_cycles: int = 0
    total_signals: int = 0
    last_run_at: str = ""
    last_signals: list[dict] = Field(default_factory=list)
    created_at: str = ""
    risk_rules: dict = Field(default_factory=dict)


# ═══════════════ 内存调度器 ═══════════════

_monitor_tasks: dict[str, dict] = {}


def _get_user_active_count(machine_code: str) -> int:
    return sum(
        1 for t in _monitor_tasks.values()
        if t["machine_code"] == machine_code and t["status"] == "running"
    )


def _get_user_monitors(machine_code: str) -> list[dict]:
    return [t for t in _monitor_tasks.values() if t["machine_code"] == machine_code]


# ═══════════════ API 路由 ═══════════════


@router.post("/start")
async def start_monitor(req: MonitorStartRequest, x_token: str = Header(default="")):
    """
    启动策略监控任务（占 1 个免费配额）。

    服务器定时执行策略脚本的 generate_signals(mode='live')，
    存储可执行信号供客户端轮询。
    """
    record = await validate_token(x_token)
    machine_code = record["machine_code"]

    active_count = _get_user_active_count(machine_code)
    if active_count >= FREE_MONITOR_SLOTS:
        raise HTTPException(
            status_code=429,
            detail=f"已达免费配额上限（{FREE_MONITOR_SLOTS} 个），请先停止一个监控任务",
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
        "last_run_at": "",
        "last_signals": [],
        "last_error": "",
        "created_at": now_str,
        "_task": None,
    }

    _monitor_tasks[job_id] = job

    task = asyncio.create_task(_monitor_loop(job_id))
    job["_task"] = task

    logger.info(
        f"[{job_id}] 监控已启动 | {req.strategy_name} | {req.symbol} {req.timeframe} "
        f"| 间隔={req.interval_seconds}s | 用户配额 {active_count+1}/{FREE_MONITOR_SLOTS}"
    )

    return {
        "job_id": job_id,
        "status": "running",
        "strategy_name": req.strategy_name,
        "symbol": req.symbol,
        "interval_seconds": req.interval_seconds,
        "quota_used": active_count + 1,
        "quota_max": FREE_MONITOR_SLOTS,
        "message": f"监控已启动，每 {req.interval_seconds}s 执行一次。用 GET /monitor/{job_id} 查看状态。",
    }


@router.get("/list")
async def list_monitors(x_token: str = Header(default="")):
    """列出当前用户的所有监控任务。"""
    record = await validate_token(x_token)
    machine_code = record["machine_code"]

    monitors = _get_user_monitors(machine_code)
    active_count = sum(1 for m in monitors if m["status"] == "running")

    return {
        "monitors": [
            {
                "job_id": m["job_id"],
                "strategy_name": m["strategy_name"],
                "symbol": m["symbol"],
                "timeframe": m["timeframe"],
                "interval_seconds": m["interval_seconds"],
                "status": m["status"],
                "total_cycles": m["total_cycles"],
                "total_signals": m["total_signals"],
                "last_run_at": m["last_run_at"],
                "created_at": m["created_at"],
            }
            for m in monitors
        ],
        "quota_used": active_count,
        "quota_max": FREE_MONITOR_SLOTS,
    }


@router.get("/{job_id}")
async def get_monitor_status(job_id: str, x_token: str = Header(default="")):
    """查看监控任务状态 + 最近信号。"""
    record = await validate_token(x_token)

    job = _monitor_tasks.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail=f"监控任务 {job_id} 不存在")
    if job["machine_code"] != record["machine_code"]:
        raise HTTPException(status_code=403, detail="无权访问该监控任务")

    return {
        "job_id": job["job_id"],
        "strategy_name": job["strategy_name"],
        "symbol": job["symbol"],
        "timeframe": job["timeframe"],
        "interval_seconds": job["interval_seconds"],
        "status": job["status"],
        "total_cycles": job["total_cycles"],
        "total_signals": job["total_signals"],
        "last_run_at": job["last_run_at"],
        "last_signals": job["last_signals"],
        "last_error": job.get("last_error", ""),
        "created_at": job["created_at"],
        "risk_rules": job["risk_rules"],
    }


@router.post("/{job_id}/stop")
async def stop_monitor(job_id: str, x_token: str = Header(default="")):
    """停止监控任务（释放配额）。"""
    record = await validate_token(x_token)

    job = _monitor_tasks.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail=f"监控任务 {job_id} 不存在")
    if job["machine_code"] != record["machine_code"]:
        raise HTTPException(status_code=403, detail="无权操作该监控任务")

    if job["status"] == "running":
        job["status"] = "stopped"
        task = job.get("_task")
        if task and not task.done():
            task.cancel()
        logger.info(f"[{job_id}] 监控已停止 | {job['strategy_name']}")

    active_count = _get_user_active_count(record["machine_code"])

    return {
        "job_id": job_id,
        "status": "stopped",
        "message": "监控已停止",
        "quota_used": active_count,
        "quota_max": FREE_MONITOR_SLOTS,
    }


# ═══════════════ 后台调度循环 ═══════════════


async def _monitor_loop(job_id: str):
    """后台循环：定时执行策略脚本，筛选可执行信号。"""
    job = _monitor_tasks.get(job_id)
    if not job:
        return

    await asyncio.sleep(2)

    while job["status"] == "running":
        job["total_cycles"] += 1
        now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        job["last_run_at"] = now_str

        logger.info(f"[{job_id}] cycle {job['total_cycles']} | {job['strategy_name']}")

        try:
            result = await asyncio.wait_for(
                asyncio.to_thread(
                    execute_strategy,
                    script_content=job["script_content"],
                    mode="live",
                    start_date=None,
                    end_date=None,
                ),
                timeout=SCRIPT_TIMEOUT,
            )

            signals = result.get("signals", [])
            min_conf = job["risk_rules"].get("min_confidence", 0.6)
            actionable = [
                s for s in signals
                if s.get("action", "").lower() in ("buy", "sell")
                and s.get("confidence", 0) >= min_conf
            ]

            job["last_signals"] = actionable[-20:]
            job["total_signals"] += len(actionable)
            job["last_error"] = ""

            logger.info(
                f"[{job_id}] 信号: 总{len(signals)} / 可执行{len(actionable)} | "
                f"累计信号={job['total_signals']}"
            )

        except asyncio.CancelledError:
            logger.info(f"[{job_id}] 监控任务已取消")
            break
        except ScriptSecurityError as e:
            job["last_error"] = f"脚本安全检查失败: {e}"
            job["status"] = "error"
            logger.error(f"[{job_id}] {job['last_error']}")
            break
        except asyncio.TimeoutError:
            job["last_error"] = f"脚本执行超时 ({SCRIPT_TIMEOUT}s)"
            logger.warning(f"[{job_id}] {job['last_error']}")
        except Exception as e:
            job["last_error"] = str(e)
            logger.error(f"[{job_id}] 执行异常: {e}")

        if job["status"] != "running":
            break

        try:
            await asyncio.sleep(job["interval_seconds"])
        except asyncio.CancelledError:
            logger.info(f"[{job_id}] 监控任务在等待期间被取消")
            break

    if job["status"] == "running":
        job["status"] = "stopped"
    logger.info(f"[{job_id}] 监控循环结束 | 共 {job['total_cycles']} 轮")
