"""
信号 API — 批量保存和查询策略脚本产出的信号

所有接口均需 X-Token 认证，且只能操作自己策略的信号
"""

import json

from fastapi import APIRouter, Header, HTTPException
from loguru import logger

from app import database
from app.models import SignalItem, SignalQuery
from app.routers.auth import validate_token

router = APIRouter(prefix="/signals", tags=["信号"])


async def _verify_strategy_ownership(strategy_id: str, machine_code: str) -> None:
    """校验策略归属当前用户"""
    strategy = await database.get_strategy(strategy_id)
    if strategy is None:
        raise HTTPException(status_code=404, detail=f"策略 {strategy_id} 不存在")
    if strategy.get("machine_code") != machine_code:
        raise HTTPException(status_code=403, detail="无权操作该策略的信号")


@router.post("/batch")
async def save_signals_batch(
    strategy_id: str,
    signals: list[SignalItem],
    x_token: str = Header(default=""),
):
    """批量保存策略信号，仅限本人策略"""
    record = await validate_token(x_token)
    await _verify_strategy_ownership(strategy_id, record["machine_code"])

    for sig in signals:
        await database.save_signal(strategy_id, sig.model_dump())
    logger.info(f"批量保存 {len(signals)} 条信号 | strategy={strategy_id}")
    return {"saved": len(signals), "strategy_id": strategy_id}


@router.post("/query")
async def query_signals(q: SignalQuery, x_token: str = Header(default="")):
    """查询信号，仅返回本人策略的信号"""
    record = await validate_token(x_token)
    machine_code = record["machine_code"]

    if q.strategy_id:
        await _verify_strategy_ownership(q.strategy_id, machine_code)
        allowed_strategy_ids = None
    else:
        user_strategies = await database.list_strategies_by_machine(machine_code)
        allowed_strategy_ids = [s["strategy_id"] for s in user_strategies]
        if not allowed_strategy_ids:
            return {"total": 0, "signals": []}

    rows = await database.list_signals(
        strategy_id=q.strategy_id,
        allowed_strategy_ids=allowed_strategy_ids,
        symbol=q.symbol,
        start_date=q.start_date,
        end_date=q.end_date,
        limit=q.limit,
    )

    result = []
    for r in rows:
        metadata = r.get("metadata", "{}")
        if isinstance(metadata, str):
            try:
                metadata = json.loads(metadata)
            except (json.JSONDecodeError, TypeError):
                metadata = {}

        result.append({
            "signal_id": r.get("signal_id", ""),
            "strategy_id": r.get("strategy_id", ""),
            "timestamp": str(r.get("timestamp", "")),
            "symbol": r.get("symbol", ""),
            "action": r.get("action", ""),
            "direction": r.get("direction", "long"),
            "confidence": r.get("confidence", 1.0),
            "reason": r.get("reason", ""),
            "source_type": r.get("source_type", "technical"),
            "price_at_signal": r.get("price_at_signal", 0),
            "suggested_stop_loss": r.get("suggested_stop_loss"),
            "suggested_take_profit": r.get("suggested_take_profit"),
            "metadata": metadata,
            "created_at": str(r.get("created_at", "")),
        })

    return {"total": len(result), "signals": result}
