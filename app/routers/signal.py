"""
信号 API — 保存、查询交易信号
"""

import json

from fastapi import APIRouter, HTTPException
from loguru import logger

from app import database
from app.models import SignalEvent, SignalQuery

router = APIRouter(prefix="/signals", tags=["信号"])


@router.post("/", response_model=SignalEvent)
async def save_signal(signal: SignalEvent):
    """保存一条交易信号"""
    await database.save_signal(signal.model_dump())
    logger.info(f"信号已保存: {signal.signal_id} | {signal.symbol} {signal.signal_type}")
    return signal


@router.post("/batch")
async def save_signals_batch(signals: list[SignalEvent]):
    """批量保存信号"""
    for sig in signals:
        await database.save_signal(sig.model_dump())
    logger.info(f"批量保存 {len(signals)} 条信号")
    return {"saved": len(signals)}


@router.post("/query", response_model=list[SignalEvent])
async def query_signals(q: SignalQuery):
    """查询信号列表"""
    rows = await database.list_signals(
        strategy_id=q.strategy_id,
        symbol=q.symbol,
        limit=q.limit,
    )
    result = []
    for r in rows:
        triggered_by = r.get("triggered_by", "[]")
        if isinstance(triggered_by, str):
            try:
                triggered_by = json.loads(triggered_by)
            except (json.JSONDecodeError, TypeError):
                triggered_by = []

        feature_snapshot = r.get("feature_snapshot", "{}")
        if isinstance(feature_snapshot, str):
            try:
                feature_snapshot = json.loads(feature_snapshot)
            except (json.JSONDecodeError, TypeError):
                feature_snapshot = {}

        metadata = r.get("metadata", "{}")
        if isinstance(metadata, str):
            try:
                metadata = json.loads(metadata)
            except (json.JSONDecodeError, TypeError):
                metadata = {}

        result.append(SignalEvent(
            signal_id=r["signal_id"],
            strategy_id=r["strategy_id"],
            symbol=r["symbol"],
            timeframe=r["timeframe"],
            signal_type=r["signal_type"],
            strength=r.get("strength", 0.5),
            price_at_signal=r["price_at_signal"],
            stop_loss_price=r.get("stop_loss_price"),
            take_profit_price=r.get("take_profit_price"),
            triggered_by=triggered_by,
            feature_snapshot=feature_snapshot,
            confidence=r.get("confidence"),
            ttl_seconds=r.get("ttl_seconds"),
            metadata=metadata,
            created_at=str(r.get("created_at", "")),
        ))
    return result


@router.get("/{signal_id}", response_model=SignalEvent)
async def get_signal(signal_id: str):
    """获取单条信号详情"""
    row = await database.get_signal(signal_id)
    if row is None:
        raise HTTPException(status_code=404, detail=f"信号 {signal_id} 不存在")

    triggered_by = row.get("triggered_by", "[]")
    if isinstance(triggered_by, str):
        try:
            triggered_by = json.loads(triggered_by)
        except (json.JSONDecodeError, TypeError):
            triggered_by = []

    feature_snapshot = row.get("feature_snapshot", "{}")
    if isinstance(feature_snapshot, str):
        try:
            feature_snapshot = json.loads(feature_snapshot)
        except (json.JSONDecodeError, TypeError):
            feature_snapshot = {}

    metadata = row.get("metadata", "{}")
    if isinstance(metadata, str):
        try:
            metadata = json.loads(metadata)
        except (json.JSONDecodeError, TypeError):
            metadata = {}

    return SignalEvent(
        signal_id=row["signal_id"],
        strategy_id=row["strategy_id"],
        symbol=row["symbol"],
        timeframe=row["timeframe"],
        signal_type=row["signal_type"],
        strength=row.get("strength", 0.5),
        price_at_signal=row["price_at_signal"],
        stop_loss_price=row.get("stop_loss_price"),
        take_profit_price=row.get("take_profit_price"),
        triggered_by=triggered_by,
        feature_snapshot=feature_snapshot,
        confidence=row.get("confidence"),
        ttl_seconds=row.get("ttl_seconds"),
        metadata=metadata,
        created_at=str(row.get("created_at", "")),
    )
