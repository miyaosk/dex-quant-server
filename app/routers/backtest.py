"""
回测 API（免费无限制，不占配额）

  1. POST /run              — 客户端传信号，服务器回测
  2. POST /run-server       — 客户端传脚本，服务器执行脚本+回测（一站式，同步）
  3. POST /submit           — 客户端传脚本，异步提交，立即返回 job_id（推荐）
  4. GET  /job/{job_id}     — 查询异步回测进度和结果
  5. POST /optimize         — 参数优化：提交异步任务，返回 job_id
  6. GET  /optimize/{job_id} — 查询优化进度和结果

需要 X-Token 头认证，但不消耗配额
"""

import asyncio
import gc
import json
import os
import re
import resource
import time
import uuid
import threading

from fastapi import APIRouter, Header, HTTPException
from loguru import logger


def _mem_mb() -> float:
    """当前进程 RSS 内存 (MB)。"""
    try:
        ru = resource.getrusage(resource.RUSAGE_SELF)
        if os.uname().sysname == "Darwin":
            return ru.ru_maxrss / (1024 * 1024)
        return ru.ru_maxrss / 1024
    except Exception:
        return 0.0

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
from app.core.optimizer import (
    ParameterSpace, GridSearch, GeneticOptimizer,
    RandomSearch, BayesianOptimizer, SimulatedAnnealing, ParticleSwarmOptimizer,
)

SCRIPT_TIMEOUT_SECONDS = 120
OPTIMIZE_SCRIPT_TIMEOUT = 60
JOB_TTL_SECONDS = 600          # 已完成的 job 保留 10 分钟后清理
JOB_MAX_KEPT = 20              # 最多保留最近 20 个已完成的 job

# 内存中的任务状态（进程级别，Railway 单实例足够）
_optimize_jobs: dict[str, dict] = {}
_backtest_jobs: dict[str, dict] = {}

router = APIRouter(prefix="/backtest", tags=["回测"])


def _evict_old_jobs(store: dict[str, dict], max_kept: int = JOB_MAX_KEPT, ttl: float = JOB_TTL_SECONDS):
    """清理已完成/失败的过期 job，防止内存无限增长。"""
    now = time.time()
    to_delete = []
    finished = []

    for jid, job in store.items():
        if job.get("status") in ("completed", "failed"):
            finish_time = job.get("start_ts", 0) + job.get("elapsed_ms", 0) / 1000
            finished.append((jid, finish_time))
            if now - finish_time > ttl:
                to_delete.append(jid)

    if len(finished) > max_kept:
        finished.sort(key=lambda x: x[1])
        for jid, _ in finished[: len(finished) - max_kept]:
            if jid not in to_delete:
                to_delete.append(jid)

    for jid in to_delete:
        store.pop(jid, None)

    if to_delete:
        logger.debug(f"清理过期 job: {len(to_delete)} 个, 剩余 {len(store)} 个")


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


# ═══════════════ 异步回测（submit + poll） ═══════════════


@router.post("/submit")
async def submit_server_backtest(req: ServerBacktestRequest, x_token: str = Header(default="")):
    """
    异步提交回测任务，立即返回 job_id。

    用 GET /backtest/job/{job_id} 轮询进度。
    进度阶段: script_running → fetching_klines → backtesting → completed/failed
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
        raise HTTPException(status_code=400, detail="需要提供 script_content 或有效的 strategy_id")

    _evict_old_jobs(_backtest_jobs)

    job_id = f"bt_{uuid.uuid4().hex[:12]}"
    _backtest_jobs[job_id] = {
        "status": "running",
        "stage": "script_running",
        "stage_label": "正在执行策略脚本生成信号...",
        "progress_pct": 10,
        "start_ts": time.time(),
        "elapsed_ms": 0,
        "result": None,
        "error": None,
    }

    logger.info(f"[{job_id}] 异步回测已提交 | {req.symbol} {req.timeframe} {req.start_date} → {req.end_date} | mem={_mem_mb():.0f}MB")

    asyncio.create_task(_run_backtest_job(job_id, script, strategy_name, req))

    return {
        "job_id": job_id,
        "status": "running",
        "stage": "script_running",
        "message": f"回测任务已提交，用 GET /backtest/job/{job_id} 查询进度。",
    }


@router.get("/job/{job_id}")
async def get_backtest_job(job_id: str, x_token: str = Header(default="")):
    """查询异步回测任务进度和结果。"""
    await validate_token(x_token)

    job = _backtest_jobs.get(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail=f"回测任务 {job_id} 不存在")

    elapsed_ms = int((time.time() - job["start_ts"]) * 1000) if job["status"] == "running" else job["elapsed_ms"]

    response = {
        "job_id": job_id,
        "status": job["status"],
        "stage": job["stage"],
        "stage_label": job["stage_label"],
        "progress_pct": job["progress_pct"],
        "elapsed_ms": elapsed_ms,
    }

    if job["status"] == "completed" and job["result"]:
        response.update(job["result"])

    if job["status"] == "failed":
        response["error"] = job.get("error", "")

    return response


async def _run_backtest_job(job_id: str, script: str, strategy_name: str, req: ServerBacktestRequest):
    """后台执行回测任务，更新 _backtest_jobs 中的进度。"""
    job = _backtest_jobs[job_id]
    try:
        logger.info(f"[{job_id}] 开始执行脚本 | mem={_mem_mb():.0f}MB")
        job.update(stage="script_running", stage_label="正在执行策略脚本生成信号...", progress_pct=15)
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

        signals = result.get("signals", [])
        logger.info(f"[{job_id}] 脚本完成 | 信号={len(signals)} | mem={_mem_mb():.0f}MB")
        if not signals:
            job.update(status="failed", error="脚本执行后未产生任何信号",
                       elapsed_ms=int((time.time() - job["start_ts"]) * 1000))
            return

        job.update(stage="fetching_klines", stage_label=f"信号 {len(signals)} 个，正在拉取K线数据...", progress_pct=40)
        strategy_name = strategy_name or result.get("strategy_name", "unnamed")

        svc, ds = _build_backtest_service()
        try:
            job.update(stage="backtesting", stage_label="回测引擎模拟交易中...", progress_pct=60)

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

            job.update(stage="calculating", stage_label="计算绩效指标...", progress_pct=90)

            # 权益曲线降采样：超过 500 点时等距取 500 个，减少内存和传输
            equity = bt_result.get("equity_curve", [])
            if len(equity) > 500:
                step = len(equity) / 500
                bt_result["equity_curve"] = [equity[int(i * step)] for i in range(500)]

            job.update(
                status="completed",
                stage="done",
                stage_label="回测完成",
                progress_pct=100,
                elapsed_ms=int((time.time() - job["start_ts"]) * 1000),
                result=bt_result,
            )
            logger.info(f"[{job_id}] 异步回测完成 | 耗时 {job['elapsed_ms']}ms")

            _evict_old_jobs(_backtest_jobs)

        finally:
            ds.close()

    except asyncio.TimeoutError:
        job.update(status="failed", error=f"脚本执行超时（{SCRIPT_TIMEOUT_SECONDS}秒）",
                   elapsed_ms=int((time.time() - job["start_ts"]) * 1000))
    except ScriptSecurityError as e:
        job.update(status="failed", error=f"脚本安全检查未通过: {e}",
                   elapsed_ms=int((time.time() - job["start_ts"]) * 1000))
    except Exception as e:
        logger.error(f"[{job_id}] 异步回测异常: {e}")
        job.update(status="failed", error=str(e),
                   elapsed_ms=int((time.time() - job["start_ts"]) * 1000))


@router.post("/optimize")
async def optimize_strategy(req: OptimizeRequest, x_token: str = Header(default="")):
    """
    提交参数优化任务（异步）。

    立即返回 job_id，后台执行。通过 GET /optimize/{job_id} 轮询进度和结果。
    """
    await validate_token(x_token)

    valid_methods = ("grid", "genetic", "random", "bayesian", "annealing", "pso")
    method = req.method or "grid"
    if method not in valid_methods:
        raise HTTPException(status_code=400, detail=f"method 必须为: {', '.join(valid_methods)}")

    space = ParameterSpace()
    for p in req.params:
        if p.type == "int":
            space.add_int(p.name, int(p.low), int(p.high), int(p.step or 1))
        elif p.type == "float":
            space.add_float(p.name, p.low, p.high, p.step)
        elif p.type == "choice":
            space.add_choice(p.name, p.choices)

    if method == "grid":
        grid = space.get_grid()
        if len(grid) > req.max_combinations:
            raise HTTPException(
                status_code=400,
                detail=f"参数组合数 {len(grid)} 超过上限 {req.max_combinations}，请缩小参数范围或增大 step，或换用 genetic/bayesian",
            )
        n_evals = len(grid)
    else:
        n_evals = min(req.max_combinations, space.total_combinations)

    _evict_old_jobs(_optimize_jobs)

    job_id = f"opt_{uuid.uuid4().hex[:12]}"
    _optimize_jobs[job_id] = {
        "status": "running",
        "method": method,
        "total": n_evals,
        "completed": 0,
        "failed": 0,
        "current_best_fitness": 0,
        "current_best_params": {},
        "elapsed_ms": 0,
        "results": [],
        "start_ts": time.time(),
    }

    logger.info(f"[{job_id}] 优化任务已提交 | method={method} 评估数={n_evals} | {req.symbol} {req.timeframe} | mem={_mem_mb():.0f}MB")

    asyncio.create_task(_run_optimize_job(job_id, req, space, n_evals))

    return {
        "job_id": job_id,
        "status": "running",
        "method": method,
        "total_combinations": n_evals,
        "message": f"优化任务已提交 (method={method})，共 {n_evals} 次评估。请用 GET /backtest/optimize/{job_id} 查询进度。",
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


async def _run_optimize_job(job_id: str, req: OptimizeRequest, space: ParameterSpace, n_evals: int):
    """后台执行优化任务，实时更新进度。支持所有搜索方法。"""
    job = _optimize_jobs[job_id]
    method = job.get("method", "grid")

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

    # 预构建 K 线缓存，所有评估复用同一份数据
    kline_cache = {f"{req.symbol}:{req.timeframe}": df}
    logger.info(f"[{job_id}] K线缓存已建 | {req.symbol}:{req.timeframe} {len(df)} 根 | mem={_mem_mb():.0f}MB")

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

    def _evaluate(params: dict) -> float:
        """同步评估单组参数，供优化器调用。"""
        injected_script = _inject_params(req.script_content, params)
        try:
            import asyncio as _aio
            loop = _aio.new_event_loop()
            sig_result = loop.run_until_complete(
                _aio.wait_for(
                    _aio.to_thread(
                        execute_strategy,
                        script_content=injected_script,
                        mode="backtest",
                        start_date=req.start_date,
                        end_date=req.end_date,
                        cached_klines=kline_cache,
                    ),
                    timeout=OPTIMIZE_SCRIPT_TIMEOUT,
                )
            )
            loop.close()
        except Exception as e:
            logger.warning(f"[{job_id}] 脚本执行失败 params={params}: {e}")
            job["failed"] += 1
            job["completed"] += 1
            return float("-inf")

        signals = sig_result.get("signals", [])
        if not signals:
            job["failed"] += 1
            job["completed"] += 1
            return float("-inf")

        bt_result = _run_bt(df=df, signals=signals, config=bt_config, metrics_only=True)
        metrics = bt_result.get("metrics", {})

        fitness = metrics.get(req.fitness_metric, 0)
        if isinstance(fitness, (int, float)) and fitness != fitness:
            fitness = 0

        job["completed"] += 1

        if fitness > job["current_best_fitness"]:
            job["current_best_fitness"] = fitness
            job["current_best_params"] = params.copy()

        _eval_cache.append({"params": params.copy(), "fitness": fitness, "metrics": metrics})

        if job["completed"] % 5 == 0:
            gc.collect()
            logger.info(f"[{job_id}] eval {job['completed']}/{job['total']} | best={job['current_best_fitness']:.4f} | mem={_mem_mb():.0f}MB")

        return fitness

    _eval_cache: list[dict] = []

    try:
        if method == "grid":
            grid = space.get_grid()
            job["total"] = len(grid)
            all_results = await _run_grid_loop(job_id, job, grid, req, df, bt_config, kline_cache=kline_cache)
        else:
            await asyncio.to_thread(_run_optimizer_sync, method, space, _evaluate, n_evals)
            all_results = _eval_cache
    except Exception as e:
        job["status"] = "failed"
        job["error"] = str(e)
        job["elapsed_ms"] = int((time.time() - job["start_ts"]) * 1000)
        ds.close()
        return

    ds.close()
    _finalize_job(job_id, job, all_results)
    _evict_old_jobs(_optimize_jobs)


def _run_optimizer_sync(method: str, space: ParameterSpace, evaluate_fn, n_evals: int):
    """在线程中同步运行非 grid 优化器。"""
    if method == "genetic":
        pop_size = min(50, max(10, n_evals // 5))
        gens = max(5, n_evals // pop_size)
        opt = GeneticOptimizer(space, evaluate_fn, population_size=pop_size, generations=gens)
    elif method == "random":
        opt = RandomSearch(space, evaluate_fn, n_samples=n_evals)
    elif method == "bayesian":
        n_init = max(5, n_evals // 5)
        n_iter = n_evals - n_init
        opt = BayesianOptimizer(space, evaluate_fn, n_initial=n_init, n_iterations=n_iter)
    elif method == "annealing":
        opt = SimulatedAnnealing(space, evaluate_fn, n_iterations=n_evals)
    elif method == "pso":
        n_particles = min(30, max(10, n_evals // 5))
        n_iter = max(5, n_evals // n_particles)
        opt = ParticleSwarmOptimizer(space, evaluate_fn, n_particles=n_particles, n_iterations=n_iter)
    else:
        raise ValueError(f"未知方法: {method}")

    opt.run()


async def _run_grid_loop(job_id, job, grid, req, df, bt_config, kline_cache=None) -> list[dict]:
    """网格搜索保持原有逐步评估+实时进度的逻辑。"""
    from app.core.backtest_engine import run_backtest as _run_bt

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
                    cached_klines=kline_cache,
                ),
                timeout=OPTIMIZE_SCRIPT_TIMEOUT,
            )
            signals = sig_result.get("signals", [])
            if not signals:
                job["failed"] += 1
                job["completed"] += 1
                continue

            bt_result = _run_bt(df=df, signals=signals, config=bt_config, metrics_only=True)
            metrics = bt_result.get("metrics", {})

            fitness = metrics.get(req.fitness_metric, 0)
            if isinstance(fitness, (int, float)) and fitness != fitness:
                fitness = 0

            all_results.append({"params": params, "fitness": fitness, "metrics": metrics})

            if fitness > job["current_best_fitness"]:
                job["current_best_fitness"] = fitness
                job["current_best_params"] = params.copy()

        except Exception as e:
            logger.warning(f"[{job_id}][{i+1}/{len(grid)}] 失败 params={params}: {e}")
            job["failed"] += 1

        job["completed"] += 1

        if (i + 1) % 5 == 0:
            gc.collect()
            logger.info(f"[{job_id}] grid {i+1}/{len(grid)} | best={job['current_best_fitness']:.4f} | mem={_mem_mb():.0f}MB")

        if (i + 1) % max(1, len(grid) // 10) == 0:
            logger.info(
                f"[{job_id}] 进度 {i+1}/{len(grid)} | "
                f"当前最优 fitness={job['current_best_fitness']:.4f}"
            )

    return all_results


def _finalize_job(job_id: str, job: dict, all_results: list[dict]):
    """排序结果、更新 job 状态。"""
    all_results.sort(key=lambda x: x.get("fitness", 0), reverse=True)

    top_results = []
    for rank, r in enumerate(all_results[:20], 1):
        m = r.get("metrics", {})
        top_results.append({
            "rank": rank,
            "params": r["params"],
            "fitness": r.get("fitness", 0),
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
        f"[{job_id}] 优化完成 | method={job.get('method','grid')} 评估={len(all_results)} 失败={job['failed']} | "
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
