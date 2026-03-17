"""
信号 API — 批量保存和查询策略脚本产出的信号
"""

import json

from fastapi import APIRouter, HTTPException
from loguru import logger

from app import database
from app.models import SignalItem, SignalQuery

router = APIRouter(prefix="/signals", tags=["信号"])


@router.post("/batch")
async def save_signals_batch(
    strategy_id: str,
    signals: list[SignalItem],
):
    """批量保存策略信号"""
    for sig in signals:
        await database.save_signal(strategy_id, sig.model_dump())
    logger.info(f"批量保存 {len(signals)} 条信号 | strategy={strategy_id}")
    return {"saved": len(signals), "strategy_id": strategy_id}


@router.post("/query")
async def query_signals(q: SignalQuery):
    """查询信号"""
    rows = await database.list_signals(
        strategy_id=q.strategy_id,
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
