"""
回测 API — 执行回测、查询结果、交易记录、权益曲线

最核心的路由：接收策略与配置，端到端完成回测并返回结果。
"""

import json

from fastapi import APIRouter, HTTPException
from loguru import logger

from app import config
from app.models import BacktestRequest, BacktestResponse
from app.services.backtest_service import BacktestService
from app.services.data_service import DataService
from app import database

router = APIRouter(prefix="/backtest", tags=["回测"])


def _build_backtest_service() -> tuple[BacktestService, DataService]:
    """构建 BacktestService 及其依赖的 DataService。"""
    ds = DataService(proxy=config.PROXY_URL)
    return BacktestService(data_service=ds), ds


@router.post("/run", response_model=BacktestResponse)
async def run_backtest(req: BacktestRequest):
    """
    执行回测

    完整流程：
    1. 保存策略（如果是新的）
    2. 拉取数据（带缓存）
    3. 运行回测引擎
    4. 保存结果到数据库
    5. 返回 BacktestResponse
    """
    svc, ds = _build_backtest_service()
    try:
        spec_dict = req.strategy.model_dump()
        config_dict = {
            "start_date": req.start_date,
            "end_date": req.end_date,
            "initial_capital": req.initial_capital,
            "fee_rate": req.fee_rate,
            "slippage_bps": req.slippage_bps,
            "margin_mode": req.margin_mode,
            "funding_rate_enabled": req.funding_rate_enabled,
        }

        # 先保存策略
        if req.strategy.strategy_id:
            await database.save_strategy(
                strategy_id=req.strategy.strategy_id,
                name=req.strategy.name,
                version=req.strategy.version,
                spec_json=req.strategy.model_dump_json(),
                lifecycle_state=req.strategy.lifecycle_state,
            )

        result = await svc.execute(spec_dict, config_dict)
        return BacktestResponse(**result)

    except Exception as e:
        logger.error(f"回测接口异常: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        ds.close()


@router.get("/{backtest_id}", response_model=BacktestResponse)
async def get_backtest(backtest_id: str):
    """获取回测结果"""
    row = await database.get_backtest_result(backtest_id)
    if row is None:
        raise HTTPException(status_code=404, detail=f"回测 {backtest_id} 不存在")

    return BacktestResponse(
        backtest_id=row["backtest_id"],
        strategy_id=row["strategy_id"],
        status=row["status"],
        metrics=json.loads(row["metrics_json"]) if row["metrics_json"] else None,
        trades=json.loads(row["trades_json"]) if row["trades_json"] else [],
        equity_curve=json.loads(row["equity_json"]) if row["equity_json"] else [],
        error=row["error"],
        created_at=str(row["created_at"]),
        elapsed_ms=row["elapsed_ms"],
    )


@router.get("/{backtest_id}/trades")
async def get_trades(backtest_id: str):
    """获取回测交易记录"""
    row = await database.get_backtest_result(backtest_id)
    if row is None:
        raise HTTPException(status_code=404, detail=f"回测 {backtest_id} 不存在")

    trades = json.loads(row["trades_json"]) if row["trades_json"] else []
    return {
        "backtest_id": backtest_id,
        "total": len(trades),
        "trades": trades,
    }


@router.get("/{backtest_id}/equity")
async def get_equity(backtest_id: str):
    """获取权益曲线"""
    row = await database.get_backtest_result(backtest_id)
    if row is None:
        raise HTTPException(status_code=404, detail=f"回测 {backtest_id} 不存在")

    equity = json.loads(row["equity_json"]) if row["equity_json"] else []
    return {
        "backtest_id": backtest_id,
        "points": len(equity),
        "equity_curve": equity,
    }
