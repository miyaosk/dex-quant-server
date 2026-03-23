"""
回测 API — 两种模式（免费无限制，不占配额）

  1. POST /run         — 客户端传信号，服务器回测
  2. POST /run-server  — 客户端传脚本，服务器执行脚本+回测（一站式，推荐）

需要 X-Token 头认证，但不消耗配额
"""

import asyncio
import json

from fastapi import APIRouter, Header, HTTPException
from loguru import logger

from app import config
from app.models import BacktestRequest, BacktestResponse, ServerBacktestRequest
from app.services.backtest_service import BacktestService
from app.services.data_service import DataService
from app import database
from app.routers.auth import validate_token
from app.core.script_executor import execute_strategy, ScriptSecurityError

SCRIPT_TIMEOUT_SECONDS = 120

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
