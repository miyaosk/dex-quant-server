"""
回测 API（免费无限制，不占配额）

  1. POST /run              — 客户端传信号，服务器回测
  2. POST /run-server       — 客户端传脚本，服务器执行脚本+回测（一站式，推荐）
  3. POST /optimize         — 参数优化：提交异步任务，返回 job_id
  4. GET  /optimize/{job_id} — 查询优化进度和结果

需要 X-Token 头认证，但不消耗配额
"""

import asyncio
import json
import re
import time
import uuid
import threading

from fastapi import APIRouter, Header, HTTPException
from loguru import logger

from app import config
from app.models import (
    BacktestRequest, BacktestResponse, ServerBacktestRequest,
    OptimizeRequest, OptimizeResponse, OptimizeResultItem,
)
from app.services.backtest_service import BacktestService
from app.services.data_service import DataService
from app import database
from app.routers.auth import validate_token
from app.core.script_executor import execute_strategy, ScriptSecurityError
from app.core.optimizer import ParameterSpace, GridSearch, GeneticOptimizer

SCRIPT_TIMEOUT_SECONDS = 120
OPTIMIZE_SCRIPT_TIMEOUT = 60

# 内存中的优化任务状态（进程级别，Railway 单实例足够）
_optimize_jobs: dict[str, dict] = {}

router = APIRouter(prefix="/backtest", tags=["回测"])


def _build_backtest_service() -> tuple[BacktestService, DataService]:
    ds = DataService(proxy=config.PROXY_URL)
    return BacktestService(data_service=ds), ds


@router.post("/run", response_model=BacktestResponse)
async def run_backtest(req: BacktestRequest, x_token: str = Header(default="")):
    """
    执行信号驱动回测

    流程:
    1. Skill 端运行策略脚本生成信号
    2. 将信号列表 + 配置 POST 到这里（需 X-Token）
    3. Server 拉 K 线（带缓存）+ 用信号驱动回测引擎
    4. 返回绩效指标、交易记录、权益曲线、评估结论
    """
    await validate_token(x_token)

    svc, ds = _build_backtest_service()
    try:
        signals = [s.model_dump() for s in req.signals]

        bt_config = {
            "initial_capital": req.initial_capital,
            "leverage": req.leverage,
            "fee_rate": req.fee_rate,
            "slippage_bps": req.slippage_bps,
            "margin_mode": req.margin_mode,
            "direction": req.direction,
        }

        result = await svc.execute(
            strategy_name=req.strategy_name,
            strategy_id=req.strategy_id,
            symbol=req.symbol,
            timeframe=req.timeframe,
            start_date=req.start_date,
            end_date=req.end_date,
            signals=signals,
            config=bt_config,
        )

        return BacktestResponse(**result)

    except Exception as e:
        logger.error(f"回测接口异常: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        ds.close()


@router.post("/run-server", response_model=BacktestResponse)
async def run_server_backtest(req: ServerBacktestRequest, x_token: str = Header(default="")):
    """
    服务器端一站式回测 — 上传脚本，服务器执行生成信号 + 回测

    流程:
    1. 客户端上传策略脚本（或指定已保存的 strategy_id）
    2. 服务器执行脚本的 generate_signals()
    3. 拿到信号后走回测引擎
    4. 返回绩效结果

    脚本来源（二选一）:
    - script_content: 直接传脚本源码
    - strategy_id: 从数据库加载已保存的脚本
    """
    await validate_token(x_token)

    script = req.script_content
    strategy_name = req.strategy_name

    if not script and req.strategy_id:
        row = await database.get_strategy(req.strategy_id)
        if row is None:
            raise HTTPException(status_code=404, detail=f"策略 {req.strategy_id} 不存在")
        script = row.get("script_content", "")
        strategy_name = strategy_name or row.get("name", "")

    if not script:
        raise HTTPException(
            status_code=400,
            detail="需要提供 script_content 或有效的 strategy_id",
        )

    try:
        result = await asyncio.wait_for(
            asyncio.to_thread(
                execute_strategy,
                script_content=script,
                mode="backtest",
                start_date=req.start_date,
                end_date=req.end_date,
            ),
            timeout=SCRIPT_TIMEOUT_SECONDS,
        )
    except asyncio.TimeoutError:
        raise HTTPException(status_code=408, detail=f"策略脚本执行超时（{SCRIPT_TIMEOUT_SECONDS}秒）")
    except ScriptSecurityError as e:
        raise HTTPException(status_code=403, detail=f"脚本安全检查未通过: {e}")
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"脚本执行异常: {e}")
        raise HTTPException(status_code=500, detail=f"脚本执行失败: {e}")

    signals = result.get("signals", [])
    if not signals:
        raise HTTPException(status_code=400, detail="脚本执行后未产生任何信号")

    strategy_name = strategy_name or result.get("strategy_name", "unnamed")

    svc, ds = _build_backtest_service()
    try:
        bt_config = {
            "initial_capital": req.initial_capital,
            "leverage": req.leverage,
            "fee_rate": req.fee_rate,
            "slippage_bps": req.slippage_bps,
            "margin_mode": req.margin_mode,
            "direction": req.direction,
        }

        bt_result = await svc.execute(
            strategy_name=strategy_name,
            strategy_id=req.strategy_id,
            symbol=req.symbol,
            timeframe=req.timeframe,
            start_date=req.start_date,
            end_date=req.end_date,
            signals=signals,
            config=bt_config,
        )

        return BacktestResponse(**bt_result)

    except Exception as e:
        logger.error(f"回测异常: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        ds.close()


@router.post("/optimize")
async def optimize_strategy(req: OptimizeRequest, x_token: str = Header(default="")):
    """
    提交参数优化任务（异步）。

    立即返回 job_id，后台执行。通过 GET /optimize/{job_id} 轮询进度和结果。
    """
    await validate_token(x_token)

    space = ParameterSpace()
    for p in req.params:
        if p.type == "int":
            space.add_int(p.name, int(p.low), int(p.high), int(p.step or 1))
        elif p.type == "float":
            space.add_float(p.name, p.low, p.high, p.step)
        elif p.type == "choice":
            space.add_choice(p.name, p.choices)

    grid = space.get_grid()
    if len(grid) > req.max_combinations:
        raise HTTPException(
            status_code=400,
            detail=f"参数组合数 {len(grid)} 超过上限 {req.max_combinations}，请缩小参数范围或增大 step",
        )

    job_id = f"opt_{uuid.uuid4().hex[:12]}"
    _optimize_jobs[job_id] = {
        "status": "running",
        "total": len(grid),
        "completed": 0,
        "failed": 0,
        "current_best_fitness": 0,
        "current_best_params": {},
        "elapsed_ms": 0,
        "results": [],
        "start_ts": time.time(),
    }

    logger.info(f"[{job_id}] 优化任务已提交 | 组合数={len(grid)} | {req.symbol} {req.timeframe}")

    asyncio.create_task(_run_optimize_job(job_id, req, grid))

    return {
        "job_id": job_id,
        "status": "running",
        "total_combinations": len(grid),
        "message": f"优化任务已提交，共 {len(grid)} 种参数组合。请用 GET /backtest/optimize/{job_id} 查询进度。",
    }


@router.get("/optimize/{job_id}")
async def get_optimize_progress(job_id: str, x_token: str = Header(default="")):
    """
    查询优化任务进度。

    返回当前进度、已完成数、当前最优参数。任务完成后返回完整结果。
    """
    await validate_token(x_token)

    job = _optimize_jobs.get(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail=f"优化任务 {job_id} 不存在")

    elapsed_ms = int((time.time() - job["start_ts"]) * 1000) if job["status"] == "running" else job["elapsed_ms"]
    progress_pct = (job["completed"] / job["total"] * 100) if job["total"] > 0 else 0

    response = {
        "job_id": job_id,
        "status": job["status"],
        "total": job["total"],
        "completed": job["completed"],
        "failed": job["failed"],
        "progress_pct": round(progress_pct, 1),
        "current_best_fitness": job["current_best_fitness"],
        "current_best_params": job["current_best_params"],
        "elapsed_ms": elapsed_ms,
    }

    if job["status"] == "completed":
        response["best_params"] = job["current_best_params"]
        response["best_fitness"] = job["current_best_fitness"]
        response["results"] = job["results"]

    if job["status"] == "failed":
        response["error"] = job.get("error", "")

    return response


async def _run_optimize_job(job_id: str, req: OptimizeRequest, grid: list[dict]):
    """后台执行优化任务，实时更新进度。"""
    job = _optimize_jobs[job_id]

    ds = DataService(proxy=config.PROXY_URL)
    try:
        df = await ds.get_klines(
            symbol=req.symbol,
            interval=req.timeframe,
            start_date=req.start_date,
            end_date=req.end_date,
            market="crypto_futures",
        )
    except Exception as e:
        job["status"] = "failed"
        job["error"] = f"拉取K线失败: {e}"
        job["elapsed_ms"] = int((time.time() - job["start_ts"]) * 1000)
        ds.close()
        return

    if df.empty:
        job["status"] = "failed"
        job["error"] = "未获取到K线数据"
        job["elapsed_ms"] = int((time.time() - job["start_ts"]) * 1000)
        ds.close()
        return

    from app.core.backtest_engine import run_backtest as _run_bt

    bt_config = {
        "symbol": req.symbol,
        "initial_capital": req.initial_capital,
        "leverage": req.leverage,
        "fee_rate": req.fee_rate,
        "slippage_bps": req.slippage_bps,
        "margin_mode": req.margin_mode,
        "direction": req.direction,
        "risk_per_trade": 0.02,
    }

    all_results: list[dict] = []

    for i, params in enumerate(grid):
        injected_script = _inject_params(req.script_content, params)
        try:
            sig_result = await asyncio.wait_for(
                asyncio.to_thread(
                    execute_strategy,
                    script_content=injected_script,
                    mode="backtest",
                    start_date=req.start_date,
                    end_date=req.end_date,
                ),
                timeout=OPTIMIZE_SCRIPT_TIMEOUT,
            )
            signals = sig_result.get("signals", [])
            if not signals:
                job["failed"] += 1
                job["completed"] += 1
                continue

            bt_result = _run_bt(df=df, signals=signals, config=bt_config)
            metrics = bt_result.get("metrics", {})

            fitness = metrics.get(req.fitness_metric, 0)
            if isinstance(fitness, (int, float)) and fitness != fitness:
                fitness = 0

            all_results.append({
                "params": params,
                "fitness": fitness,
                "metrics": metrics,
            })

            if fitness > job["current_best_fitness"]:
                job["current_best_fitness"] = fitness
                job["current_best_params"] = params.copy()

        except Exception as e:
            logger.warning(f"[{job_id}][{i+1}/{len(grid)}] 失败 params={params}: {e}")
            job["failed"] += 1

        job["completed"] += 1

        if (i + 1) % max(1, len(grid) // 10) == 0:
            logger.info(
                f"[{job_id}] 进度 {i+1}/{len(grid)} | "
                f"当前最优 fitness={job['current_best_fitness']:.4f}"
            )

    ds.close()

    all_results.sort(key=lambda x: x["fitness"], reverse=True)

    top_results = []
    for rank, r in enumerate(all_results[:20], 1):
        m = r["metrics"]
        top_results.append({
            "rank": rank,
            "params": r["params"],
            "fitness": r["fitness"],
            "total_return_pct": m.get("total_return_pct", 0),
            "sharpe_ratio": m.get("sharpe_ratio", 0),
            "sortino_ratio": m.get("sortino_ratio", 0),
            "max_drawdown_pct": m.get("max_drawdown_pct", 0),
            "win_rate": m.get("win_rate", 0),
            "total_trades": m.get("total_trades", 0),
            "profit_loss_ratio": m.get("profit_loss_ratio", 0),
            "final_balance": m.get("final_balance", 0),
        })

    job["status"] = "completed"
    job["results"] = top_results
    job["elapsed_ms"] = int((time.time() - job["start_ts"]) * 1000)

    logger.info(
        f"[{job_id}] 优化完成 | 评估={len(all_results)} 失败={job['failed']} | "
        f"最优fitness={job['current_best_fitness']:.4f} | 耗时={job['elapsed_ms']}ms"
    )


def _inject_params(script: str, params: dict) -> str:
    """将 PARAMS 字典注入到脚本头部。"""
    params_line = f"PARAMS = {repr(params)}\n"
    if "PARAMS" in script:
        script = re.sub(r'^PARAMS\s*=\s*\{[^}]*\}', params_line.strip(), script, count=1, flags=re.MULTILINE)
        if params_line.strip() not in script:
            script = params_line + script
    else:
        script = params_line + script
    return script


async def _get_owned_backtest(backtest_id: str, machine_code: str) -> dict:
    """获取回测结果并通过关联策略校验归属"""
    row = await database.get_backtest_result(backtest_id)
    if row is None:
        raise HTTPException(status_code=404, detail=f"回测 {backtest_id} 不存在")

    strategy = await database.get_strategy(row["strategy_id"])
    if strategy is None or strategy.get("machine_code") != machine_code:
        raise HTTPException(status_code=403, detail="无权访问该回测结果")

    return row


@router.get("/{backtest_id}", response_model=BacktestResponse)
async def get_backtest(backtest_id: str, x_token: str = Header(default="")):
    """获取已保存的回测结果，仅限本人策略的回测"""
    record = await validate_token(x_token)
    row = await _get_owned_backtest(backtest_id, record["machine_code"])

    return BacktestResponse(
        backtest_id=row["backtest_id"],
        strategy_id=row["strategy_id"],
        strategy_name=row.get("strategy_name", ""),
        status=row["status"],
        metrics=json.loads(row["metrics_json"]) if row.get("metrics_json") else None,
        trades=json.loads(row["trades_json"]) if row.get("trades_json") else [],
        equity_curve=json.loads(row["equity_json"]) if row.get("equity_json") else [],
        conclusion=row.get("conclusion", ""),
        error=row.get("error"),
        created_at=str(row.get("created_at", "")),
        elapsed_ms=row.get("elapsed_ms", 0),
    )


@router.get("/{backtest_id}/trades")
async def get_trades(backtest_id: str, x_token: str = Header(default="")):
    """获取回测交易记录，仅限本人策略的回测"""
    record = await validate_token(x_token)
    row = await _get_owned_backtest(backtest_id, record["machine_code"])

    trades = json.loads(row["trades_json"]) if row.get("trades_json") else []
    return {
        "backtest_id": backtest_id,
        "total": len(trades),
        "trades": trades,
    }


@router.get("/{backtest_id}/equity")
async def get_equity(backtest_id: str, x_token: str = Header(default="")):
    """获取权益曲线，仅限本人策略的回测"""
    record = await validate_token(x_token)
    row = await _get_owned_backtest(backtest_id, record["machine_code"])

    equity = json.loads(row["equity_json"]) if row.get("equity_json") else []
    return {
        "backtest_id": backtest_id,
        "points": len(equity),
        "equity_curve": equity,
    }
