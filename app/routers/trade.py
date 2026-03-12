"""
交易 API — 记录实盘交易、管理持仓、计算 PnL

交易流程:
  1. 信号触发 → 调用交易所 API 下单 → 记录交易 (POST /trades/)
  2. 自动更新持仓 (开仓/加仓/减仓/平仓)
  3. 查询 PnL (GET /trades/pnl/{strategy_id})
"""

import json
import uuid
from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException
from loguru import logger

from app import database
from app.models import (
    TradeCreate, TradeDetail, TradeQuery,
    PositionDetail, PositionQuery, PnLSummary,
)

router = APIRouter(prefix="/trades", tags=["交易"])


@router.post("/", response_model=TradeDetail)
async def record_trade(trade: TradeCreate):
    """
    记录一笔交易

    提交交易后会自动更新对应持仓:
    - buy → 开多仓 / 平空仓
    - sell → 开空仓 / 平多仓
    """
    if not trade.trade_id:
        trade.trade_id = f"trade_{uuid.uuid4().hex[:12]}"

    trade_dict = trade.model_dump()
    await database.save_trade(trade_dict)
    logger.info(
        f"交易已记录: {trade.trade_id} | {trade.exchange} {trade.symbol} "
        f"{trade.side} {trade.quantity} @ {trade.price}"
    )

    # 自动更新持仓
    await _update_position_from_trade(trade)

    return TradeDetail(**trade_dict)


@router.post("/batch")
async def record_trades_batch(trades: list[TradeCreate]):
    """批量记录交易"""
    for t in trades:
        if not t.trade_id:
            t.trade_id = f"trade_{uuid.uuid4().hex[:12]}"
        await database.save_trade(t.model_dump())
        await _update_position_from_trade(t)

    logger.info(f"批量记录 {len(trades)} 笔交易")
    return {"recorded": len(trades)}


@router.post("/query", response_model=list[TradeDetail])
async def query_trades(q: TradeQuery):
    """查询交易记录"""
    rows = await database.list_trades(
        strategy_id=q.strategy_id,
        exchange=q.exchange,
        symbol=q.symbol,
        limit=q.limit,
    )
    return [_row_to_trade(r) for r in rows]


@router.get("/{trade_id}", response_model=TradeDetail)
async def get_trade(trade_id: str):
    """获取单条交易详情"""
    row = await database.get_trade(trade_id)
    if row is None:
        raise HTTPException(status_code=404, detail=f"交易 {trade_id} 不存在")
    return _row_to_trade(row)


# ═══════════════ 持仓 ═══════════════


@router.post("/positions/query", response_model=list[PositionDetail])
async def query_positions(q: PositionQuery):
    """查询当前持仓"""
    rows = await database.get_open_positions(
        strategy_id=q.strategy_id,
        exchange=q.exchange,
    )
    return [_row_to_position(r) for r in rows]


@router.get("/positions/{position_id}", response_model=PositionDetail)
async def get_position(position_id: str):
    """获取单条持仓详情"""
    row = await database.get_position(position_id)
    if row is None:
        raise HTTPException(status_code=404, detail=f"持仓 {position_id} 不存在")
    return _row_to_position(row)


# ═══════════════ PnL ═══════════════


@router.get("/pnl/{strategy_id}", response_model=PnLSummary)
async def get_pnl(strategy_id: str):
    """获取策略 PnL 汇总"""
    result = await database.calc_strategy_pnl(strategy_id)
    return PnLSummary(**result)


# ═══════════════ 内部逻辑 ═══════════════


async def _update_position_from_trade(trade: TradeCreate) -> None:
    """
    根据交易记录自动更新持仓。

    逻辑:
      buy  + 无持仓/多仓 → 开多/加多
      buy  + 空仓       → 减空/平空
      sell + 无持仓/空仓 → 开空/加空
      sell + 多仓       → 减多/平多
    """
    try:
        if trade.side == "buy":
            # 先看有没有空仓要平
            short_pos = await database.get_position_by_key(
                trade.strategy_id, trade.exchange, trade.symbol, "short"
            )
            if short_pos:
                await _reduce_position(short_pos, trade)
            else:
                await _add_to_position(trade, "long")
        elif trade.side == "sell":
            long_pos = await database.get_position_by_key(
                trade.strategy_id, trade.exchange, trade.symbol, "long"
            )
            if long_pos:
                await _reduce_position(long_pos, trade)
            else:
                await _add_to_position(trade, "short")
    except Exception as e:
        logger.error(f"持仓更新失败: {e}")


async def _add_to_position(trade: TradeCreate, side: str) -> None:
    """开仓或加仓。"""
    pos = await database.get_position_by_key(
        trade.strategy_id, trade.exchange, trade.symbol, side
    )

    if pos:
        old_qty = float(pos["quantity"])
        old_avg = float(pos["avg_entry_price"])
        new_qty = old_qty + trade.quantity
        new_avg = (old_avg * old_qty + trade.price * trade.quantity) / new_qty

        await database.save_position({
            "position_id": pos["position_id"],
            "strategy_id": trade.strategy_id,
            "exchange": trade.exchange,
            "symbol": trade.symbol,
            "side": side,
            "quantity": new_qty,
            "avg_entry_price": new_avg,
            "leverage": trade.leverage,
            "margin_mode": trade.margin_mode,
            "total_fee": float(pos.get("total_fee", 0)) + trade.fee,
            "status": "open",
        })
        logger.info(f"加仓 | {trade.symbol} {side} | {old_qty} → {new_qty}")
    else:
        position_id = f"pos_{uuid.uuid4().hex[:12]}"
        await database.save_position({
            "position_id": position_id,
            "strategy_id": trade.strategy_id,
            "exchange": trade.exchange,
            "symbol": trade.symbol,
            "side": side,
            "quantity": trade.quantity,
            "avg_entry_price": trade.price,
            "leverage": trade.leverage,
            "margin_mode": trade.margin_mode,
            "realized_pnl": 0,
            "total_fee": trade.fee,
            "status": "open",
        })
        logger.info(f"开仓 | {trade.symbol} {side} | qty={trade.quantity} @ {trade.price}")


async def _reduce_position(pos: dict, trade: TradeCreate) -> None:
    """减仓或平仓，计算已实现 PnL。"""
    pos_qty = float(pos["quantity"])
    pos_avg = float(pos["avg_entry_price"])
    close_qty = min(trade.quantity, pos_qty)

    if pos["side"] == "long":
        pnl = close_qty * (trade.price - pos_avg)
    else:
        pnl = close_qty * (pos_avg - trade.price)

    remaining = pos_qty - close_qty
    old_realized = float(pos.get("realized_pnl", 0))
    old_fee = float(pos.get("total_fee", 0))

    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    if remaining <= 1e-10:
        await database.save_position({
            "position_id": pos["position_id"],
            "strategy_id": pos["strategy_id"],
            "exchange": pos["exchange"],
            "symbol": pos["symbol"],
            "side": pos["side"],
            "quantity": 0,
            "avg_entry_price": pos_avg,
            "leverage": pos.get("leverage", 1),
            "margin_mode": pos.get("margin_mode", "isolated"),
            "realized_pnl": old_realized + pnl,
            "total_fee": old_fee + trade.fee,
            "status": "closed",
            "closed_at": now_str,
        })
        logger.info(
            f"平仓 | {pos['symbol']} {pos['side']} | "
            f"PnL={pnl:+.2f} | 累计PnL={old_realized + pnl:+.2f}"
        )
    else:
        await database.save_position({
            "position_id": pos["position_id"],
            "strategy_id": pos["strategy_id"],
            "exchange": pos["exchange"],
            "symbol": pos["symbol"],
            "side": pos["side"],
            "quantity": remaining,
            "avg_entry_price": pos_avg,
            "leverage": pos.get("leverage", 1),
            "margin_mode": pos.get("margin_mode", "isolated"),
            "realized_pnl": old_realized + pnl,
            "total_fee": old_fee + trade.fee,
            "status": "open",
        })
        logger.info(
            f"减仓 | {pos['symbol']} {pos['side']} | "
            f"{pos_qty} → {remaining} | 本次PnL={pnl:+.2f}"
        )


def _row_to_trade(r: dict) -> TradeDetail:
    return TradeDetail(
        trade_id=r["trade_id"],
        signal_id=r.get("signal_id"),
        strategy_id=r["strategy_id"],
        exchange=r["exchange"],
        symbol=r["symbol"],
        side=r["side"],
        quantity=float(r["quantity"]),
        price=float(r["price"]),
        fee=float(r.get("fee", 0)),
        fee_asset=r.get("fee_asset", "USDT"),
        order_type=r.get("order_type", "market"),
        leverage=r.get("leverage", 1),
        margin_mode=r.get("margin_mode", "isolated"),
        status=r.get("status", "filled"),
        exchange_order_id=r.get("exchange_order_id"),
        notes=r.get("notes"),
        created_at=str(r.get("created_at", "")),
    )


def _row_to_position(r: dict) -> PositionDetail:
    return PositionDetail(
        position_id=r["position_id"],
        strategy_id=r["strategy_id"],
        exchange=r["exchange"],
        symbol=r["symbol"],
        side=r["side"],
        quantity=float(r.get("quantity", 0)),
        avg_entry_price=float(r.get("avg_entry_price", 0)),
        leverage=r.get("leverage", 1),
        margin_mode=r.get("margin_mode", "isolated"),
        realized_pnl=float(r.get("realized_pnl", 0)),
        total_fee=float(r.get("total_fee", 0)),
        status=r.get("status", "open"),
        opened_at=str(r.get("opened_at", "")),
        closed_at=str(r.get("closed_at", "")) if r.get("closed_at") else None,
        updated_at=str(r.get("updated_at", "")),
    )
