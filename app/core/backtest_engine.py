"""
信号驱动回测引擎 — 永续合约专项

核心变化：不再解析规则字符串，改为接收外部信号列表驱动交易。
Skill 端运行策略脚本生成信号 → 信号发送到 Server → 本引擎回放信号。

能力：
  - 多空双向持仓、逐仓保证金、杠杆 1x-125x
  - 资金费率 8h 结算、强平、止损/止盈、滑点、手续费
  - 输出标准化 metrics + trades + equity_curve

入口函数：
  run_backtest(df, signals, config) -> dict
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

import numpy as np
import pandas as pd
from loguru import logger


# ═══════════════════════════════════════════
#  数据结构
# ═══════════════════════════════════════════


@dataclass
class _Position:
    symbol: str
    side: str = "none"
    quantity: float = 0.0
    avg_entry_price: float = 0.0
    leverage: int = 1
    margin: float = 0.0
    margin_mode: str = "isolated"
    maintenance_margin_rate: float = 0.005
    stop_loss: Optional[float] = None
    take_profit: Optional[float] = None
    unrealized_pnl: float = 0.0
    liquidation_price: float = 0.0

    @property
    def nominal_value(self) -> float:
        return self.quantity * self.avg_entry_price

    def calc_unrealized_pnl(self, mark_price: float) -> float:
        if self.side == "long":
            return self.quantity * (mark_price - self.avg_entry_price)
        elif self.side == "short":
            return self.quantity * (self.avg_entry_price - mark_price)
        return 0.0

    def calc_liquidation_price(self) -> float:
        if self.quantity == 0 or self.side == "none":
            return 0.0
        mmr = self.maintenance_margin_rate
        if self.side == "long":
            return self.avg_entry_price * (1 - 1 / self.leverage + mmr)
        return self.avg_entry_price * (1 + 1 / self.leverage - mmr)

    def calc_margin_ratio(self, mark_price: float) -> float:
        nominal = self.quantity * mark_price
        if nominal == 0:
            return float("inf")
        pnl = self.calc_unrealized_pnl(mark_price)
        return (self.margin + pnl) / nominal


@dataclass
class _TradeRecord:
    datetime: str
    symbol: str
    side: str
    action: str
    quantity: float
    price: float
    mark_price: float
    leverage: int
    margin_used: float
    commission: float
    slippage: float
    funding_fee: float
    realized_pnl: float
    reason: str = ""


@dataclass
class _Account:
    initial_capital: float
    balance: float = 0.0
    positions: dict = field(default_factory=dict)
    trade_log: list = field(default_factory=list)
    equity_curve: list = field(default_factory=list)
    funding_log: list = field(default_factory=list)
    liquidation_count: int = 0
    total_commission: float = 0.0
    total_slippage_cost: float = 0.0
    total_funding_paid: float = 0.0
    total_funding_received: float = 0.0

    def __post_init__(self):
        self.balance = self.initial_capital

    def get_position(self, symbol: str) -> _Position:
        if symbol not in self.positions:
            self.positions[symbol] = _Position(symbol=symbol)
        return self.positions[symbol]

    @property
    def total_unrealized_pnl(self) -> float:
        return sum(p.unrealized_pnl for p in self.positions.values())

    @property
    def equity(self) -> float:
        return self.balance + self.total_unrealized_pnl

    @property
    def used_margin(self) -> float:
        return sum(p.margin for p in self.positions.values() if p.side != "none")

    @property
    def available_balance(self) -> float:
        return self.balance - self.used_margin


# ═══════════════════════════════════════════
#  回测引擎
# ═══════════════════════════════════════════


class BacktestEngine:
    """永续合约回测引擎。"""

    def __init__(
        self,
        initial_capital: float = 100_000.0,
        default_leverage: int = 1,
        margin_mode: str = "isolated",
        slippage_bps: float = 5.0,
        taker_fee: float = 0.0005,
        maker_fee: float = 0.0002,
        enable_funding: bool = True,
        enable_liquidation: bool = True,
        maintenance_margin_rate: float = 0.005,
    ):
        self.default_leverage = default_leverage
        self.margin_mode = margin_mode
        self.slippage_bps = slippage_bps
        self.taker_fee = taker_fee
        self.maker_fee = maker_fee
        self.enable_funding = enable_funding
        self.enable_liquidation = enable_liquidation
        self.maintenance_margin_rate = maintenance_margin_rate
        self.account = _Account(initial_capital=initial_capital)

    def open_long(self, symbol: str, qty: float, price: float,
                  mark_price: float, dt: str, leverage: int = None,
                  reason: str = ""):
        self._open_position(symbol, "long", qty, price, mark_price, dt, leverage, reason)

    def open_short(self, symbol: str, qty: float, price: float,
                   mark_price: float, dt: str, leverage: int = None,
                   reason: str = ""):
        self._open_position(symbol, "short", qty, price, mark_price, dt, leverage, reason)

    def close_long(self, symbol: str, qty: float, price: float,
                   mark_price: float, dt: str, reason: str = ""):
        self._close_position(symbol, "long", qty, price, mark_price, dt, "close", reason)

    def close_short(self, symbol: str, qty: float, price: float,
                    mark_price: float, dt: str, reason: str = ""):
        self._close_position(symbol, "short", qty, price, mark_price, dt, "close", reason)

    def _open_position(self, symbol: str, side: str, qty: float, price: float,
                       mark_price: float, dt: str, leverage: int = None,
                       reason: str = ""):
        pos = self.account.get_position(symbol)
        lev = leverage or pos.leverage or self.default_leverage
        pos.leverage = lev
        pos.margin_mode = pos.margin_mode or self.margin_mode
        pos.maintenance_margin_rate = self.maintenance_margin_rate

        slippage = price * self.slippage_bps / 10000
        fill_price = price + slippage if side == "long" else price - slippage

        nominal = qty * fill_price
        required_margin = nominal / lev
        commission = nominal * self.taker_fee

        if self.account.available_balance < required_margin + commission:
            return

        if pos.side == side and pos.quantity > 0:
            total_qty = pos.quantity + qty
            pos.avg_entry_price = (
                pos.avg_entry_price * pos.quantity + fill_price * qty
            ) / total_qty
            pos.quantity = total_qty
            pos.margin += required_margin
        else:
            pos.side = side
            pos.quantity = qty
            pos.avg_entry_price = fill_price
            pos.margin = required_margin

        pos.liquidation_price = pos.calc_liquidation_price()
        self.account.balance -= commission
        self.account.total_commission += commission
        self.account.total_slippage_cost += abs(slippage * qty)

        self.account.trade_log.append(_TradeRecord(
            datetime=dt, symbol=symbol, side=side, action="open",
            quantity=qty, price=fill_price, mark_price=mark_price,
            leverage=lev, margin_used=required_margin,
            commission=commission, slippage=abs(slippage * qty),
            funding_fee=0.0, realized_pnl=0.0, reason=reason,
        ))

    def _close_position(self, symbol: str, side: str, qty: float, price: float,
                        mark_price: float, dt: str, action: str = "close",
                        reason: str = ""):
        pos = self.account.get_position(symbol)
        if pos.side != side or pos.quantity == 0:
            return

        close_qty = min(qty, pos.quantity) if qty else pos.quantity

        slippage = price * self.slippage_bps / 10000
        fill_price = price - slippage if side == "long" else price + slippage

        if side == "long":
            realized_pnl = close_qty * (fill_price - pos.avg_entry_price)
        else:
            realized_pnl = close_qty * (pos.avg_entry_price - fill_price)

        nominal = close_qty * fill_price
        commission = nominal * self.taker_fee

        margin_released = pos.margin * (close_qty / pos.quantity)
        pos.margin -= margin_released
        self.account.balance += realized_pnl - commission
        self.account.total_commission += commission
        self.account.total_slippage_cost += abs(slippage * close_qty)

        pos.quantity -= close_qty
        if pos.quantity <= 1e-10:
            pos.quantity = 0
            pos.side = "none"
            pos.margin = 0
            pos.unrealized_pnl = 0
            pos.stop_loss = None
            pos.take_profit = None

        self.account.trade_log.append(_TradeRecord(
            datetime=dt, symbol=symbol, side=side, action=action,
            quantity=close_qty, price=fill_price, mark_price=mark_price,
            leverage=pos.leverage, margin_used=0,
            commission=commission, slippage=abs(slippage * close_qty),
            funding_fee=0.0, realized_pnl=realized_pnl, reason=reason,
        ))

    def on_bar(self, dt: str, prices: dict[str, dict],
               funding_rates: dict[str, float] = None):
        """每 bar：更新盈亏 → 资金费率 → 止损止盈 → 强平 → 记录净值"""
        for symbol, pos in list(self.account.positions.items()):
            if pos.side == "none":
                continue

            bar = prices.get(symbol, {})
            mark = bar.get("mark_price", bar.get("close", pos.avg_entry_price))
            pos.unrealized_pnl = pos.calc_unrealized_pnl(mark)

            if self.enable_funding and funding_rates and symbol in funding_rates:
                self._settle_funding(pos, funding_rates[symbol], mark, dt)

            if pos.side != "none":
                self._check_sl_tp(pos, bar, dt)

            if self.enable_liquidation and pos.side != "none":
                self._check_liquidation(pos, mark, dt)

        self.account.equity_curve.append({
            "datetime": dt,
            "equity": self.account.equity,
            "balance": self.account.balance,
            "unrealized_pnl": self.account.total_unrealized_pnl,
            "used_margin": self.account.used_margin,
        })

    def _settle_funding(self, pos: _Position, funding_rate: float,
                        mark_price: float, dt: str):
        nominal = pos.quantity * mark_price
        fee = nominal * funding_rate

        if pos.side == "long":
            pos.margin -= fee
            self.account.balance -= fee
            if fee > 0:
                self.account.total_funding_paid += fee
            else:
                self.account.total_funding_received += abs(fee)
        else:
            pos.margin += fee
            self.account.balance += fee
            if fee > 0:
                self.account.total_funding_received += fee
            else:
                self.account.total_funding_paid += abs(fee)

        self.account.funding_log.append({
            "datetime": dt, "symbol": pos.symbol, "side": pos.side,
            "funding_rate": funding_rate, "position_value": nominal, "fee": fee,
        })

    def _check_sl_tp(self, pos: _Position, bar: dict, dt: str):
        high = bar.get("high", bar.get("close", 0))
        low = bar.get("low", bar.get("close", 0))
        mark = bar.get("mark_price", bar.get("close", 0))

        if pos.stop_loss is not None:
            triggered = (
                (pos.side == "long" and low <= pos.stop_loss)
                or (pos.side == "short" and high >= pos.stop_loss)
            )
            if triggered:
                logger.info(f"[{dt}] 止损触发: {pos.symbol} {pos.side} @ {pos.stop_loss}")
                self._close_position(
                    pos.symbol, pos.side, pos.quantity, pos.stop_loss, mark, dt,
                    "stop_loss", "止损触发",
                )
                return

        if pos.take_profit is not None:
            triggered = (
                (pos.side == "long" and high >= pos.take_profit)
                or (pos.side == "short" and low <= pos.take_profit)
            )
            if triggered:
                logger.info(f"[{dt}] 止盈触发: {pos.symbol} {pos.side} @ {pos.take_profit}")
                self._close_position(
                    pos.symbol, pos.side, pos.quantity, pos.take_profit, mark, dt,
                    "take_profit", "止盈触发",
                )

    def _check_liquidation(self, pos: _Position, mark_price: float, dt: str):
        if pos.side == "none" or pos.quantity == 0:
            return

        margin_ratio = pos.calc_margin_ratio(mark_price)
        if margin_ratio <= pos.maintenance_margin_rate:
            logger.warning(
                f"[{dt}] 强平: {pos.symbol} {pos.side} "
                f"保证金率 {margin_ratio:.4f} <= {pos.maintenance_margin_rate}"
            )
            lost_margin = pos.margin
            pos.quantity = 0
            pos.side = "none"
            pos.margin = 0
            pos.unrealized_pnl = 0
            pos.stop_loss = None
            pos.take_profit = None

            self.account.liquidation_count += 1
            self.account.trade_log.append(_TradeRecord(
                datetime=dt, symbol=pos.symbol, side="none", action="liquidation",
                quantity=0, price=mark_price, mark_price=mark_price,
                leverage=pos.leverage, margin_used=0,
                commission=0, slippage=0, funding_fee=0,
                realized_pnl=-lost_margin, reason="强制平仓",
            ))

    def get_result(self) -> dict:
        eq_df = pd.DataFrame(self.account.equity_curve)
        if eq_df.empty:
            return {"error": "无回测数据"}

        equities = eq_df["equity"].values.astype(float)
        equities = np.where(np.isfinite(equities), equities, 0.0)
        peak = np.maximum.accumulate(np.maximum(equities, 1e-10))
        drawdowns = np.where(peak > 0, (equities - peak) / peak, 0.0)

        prev = equities[:-1] if len(equities) > 1 else np.array([1.0])
        prev = np.where(prev != 0, prev, 1e-10)
        returns = np.diff(equities) / prev if len(equities) > 1 else np.array([0.0])
        returns = np.where(np.isfinite(returns), returns, 0.0)

        total_return = (equities[-1] / max(equities[0], 1e-10)) - 1
        n_days = len(equities)
        annual_return = (1 + total_return) ** (365 / max(n_days, 1)) - 1
        volatility = float(np.std(returns) * np.sqrt(365)) if len(returns) > 1 else 0

        sharpe = annual_return / volatility if volatility > 0 else 0
        downside = returns[returns < 0]
        downside_std = float(np.std(downside) * np.sqrt(365)) if len(downside) > 0 else 0
        sortino = annual_return / downside_std if downside_std > 0 else 0

        max_dd = float(np.min(drawdowns))
        calmar = annual_return / abs(max_dd) if max_dd != 0 else 0
        net_funding = self.account.total_funding_received - self.account.total_funding_paid

        return {
            "performance": {
                "total_return": total_return,
                "annual_return": annual_return,
                "sharpe_ratio": sharpe,
                "sortino_ratio": sortino,
                "max_drawdown": max_dd,
                "calmar_ratio": calmar,
                "volatility": volatility,
                "total_commission": self.account.total_commission,
                "total_slippage_cost": self.account.total_slippage_cost,
                "total_funding_paid": self.account.total_funding_paid,
                "total_funding_received": self.account.total_funding_received,
                "net_funding": net_funding,
                "liquidation_count": self.account.liquidation_count,
            },
            "equity_curve": eq_df.to_dict("records"),
            "trade_log": [vars(t) for t in self.account.trade_log],
            "funding_log": self.account.funding_log,
        }


# ═══════════════════════════════════════════
#  入口：信号驱动回测
# ═══════════════════════════════════════════


def run_backtest(
    df: pd.DataFrame,
    signals: list[dict],
    config: dict,
) -> dict:
    """
    信号驱动回测。

    参数:
        df: OHLCV DataFrame (datetime/open/high/low/close/volume)
        signals: 信号列表 [{timestamp, symbol, action, direction, confidence,
                  reason, price_at_signal, suggested_stop_loss, suggested_take_profit}]
        config: 回测配置 {symbol, initial_capital, leverage, fee_rate,
                slippage_bps, margin_mode, direction, risk_per_trade}

    返回:
        {metrics, trades, equity_curve}
    """
    if df.empty:
        return {"metrics": {}, "trades": [], "equity_curve": [], "error": "数据为空"}

    symbol = config.get("symbol", "UNKNOWN")
    leverage = config.get("leverage", 1)
    direction = config.get("direction", "long_short")
    risk_per_trade = config.get("risk_per_trade", 0.02)

    engine = BacktestEngine(
        initial_capital=config.get("initial_capital", 100_000.0),
        default_leverage=leverage,
        margin_mode=config.get("margin_mode", "isolated"),
        slippage_bps=config.get("slippage_bps", 5.0),
        taker_fee=config.get("fee_rate", 0.0005),
        maker_fee=config.get("fee_rate", 0.0005) * 0.4,
        enable_funding=True,
        enable_liquidation=True,
        maintenance_margin_rate=0.005,
    )

    # 将信号按时间戳索引，支持同一时刻多个信号
    # 标准化时间戳：统一转为 "YYYY-MM-DD HH:MM:SS" 格式匹配，避免格式差异导致匹配失败
    signal_map: dict[str, list[dict]] = {}
    for sig in signals:
        ts = _normalize_ts(sig.get("timestamp", ""))
        if ts not in signal_map:
            signal_map[ts] = []
        signal_map[ts].append(sig)

    holding_bars: list[int] = []
    current_hold_start: int = -1
    signals_executed = 0

    for idx in range(len(df)):
        row = df.iloc[idx]
        dt_str = str(row["datetime"])
        dt_key = _normalize_ts(dt_str)
        close = float(row["close"])
        high = float(row["high"])
        low = float(row["low"])

        pos = engine.account.get_position(symbol)

        # 查找当前 bar 是否有信号（用标准化的时间戳匹配）
        bar_signals = signal_map.get(dt_key, [])

        for sig in bar_signals:
            action = sig.get("action", "").lower()
            sig_direction = sig.get("direction", "long").lower()
            reason = sig.get("reason", "")
            sl = sig.get("suggested_stop_loss")
            tp = sig.get("suggested_take_profit")

            if action == "buy":
                # 先平掉反向仓位
                if sig_direction == "long" and pos.side == "short":
                    engine.close_short(symbol, 0, close, close, dt_str, reason)
                    if current_hold_start >= 0:
                        holding_bars.append(idx - current_hold_start)
                        current_hold_start = -1
                elif sig_direction == "short" and pos.side == "long":
                    engine.close_long(symbol, 0, close, close, dt_str, reason)
                    if current_hold_start >= 0:
                        holding_bars.append(idx - current_hold_start)
                        current_hold_start = -1

                pos = engine.account.get_position(symbol)
                if pos.side == "none":
                    qty = _calc_position_size(
                        engine.account.available_balance, close, leverage, risk_per_trade,
                    )
                    if qty > 0:
                        if sig_direction == "long" and direction in ("long", "long_short", "long_only"):
                            engine.open_long(symbol, qty, close, close, dt_str, leverage, reason)
                        elif sig_direction == "short" and direction in ("short", "short_only", "long_short"):
                            engine.open_short(symbol, qty, close, close, dt_str, leverage, reason)

                        pos = engine.account.get_position(symbol)
                        if pos.side != "none":
                            if sl is not None:
                                pos.stop_loss = sl
                            if tp is not None:
                                pos.take_profit = tp
                            current_hold_start = idx
                            signals_executed += 1

            elif action in ("sell", "close"):
                if pos.side == "long":
                    engine.close_long(symbol, 0, close, close, dt_str, reason)
                    signals_executed += 1
                elif pos.side == "short":
                    engine.close_short(symbol, 0, close, close, dt_str, reason)
                    signals_executed += 1
                if current_hold_start >= 0:
                    holding_bars.append(idx - current_hold_start)
                    current_hold_start = -1

        engine.on_bar(dt_str, {
            symbol: {"close": close, "high": high, "low": low, "mark_price": close},
        })

    # 尾部平仓
    pos = engine.account.get_position(symbol)
    if pos.side != "none" and len(df) > 0:
        last_row = df.iloc[-1]
        dt_str = str(last_row["datetime"])
        close = float(last_row["close"])
        if pos.side == "long":
            engine.close_long(symbol, 0, close, close, dt_str, "回测结束平仓")
        else:
            engine.close_short(symbol, 0, close, close, dt_str, "回测结束平仓")
        if current_hold_start >= 0:
            holding_bars.append(len(df) - 1 - current_hold_start)

    raw = engine.get_result()
    perf = raw.get("performance", {})
    trades_raw = raw.get("trade_log", [])

    close_trades = [t for t in trades_raw if t.get("action") in ("close", "stop_loss", "take_profit", "liquidation")]
    wins = [t for t in close_trades if t.get("realized_pnl", 0) > 0]
    losses_list = [t for t in close_trades if t.get("realized_pnl", 0) < 0]
    win_rate = len(wins) / len(close_trades) if close_trades else 0
    avg_win = float(np.mean([t["realized_pnl"] for t in wins])) if wins else 0
    avg_loss = float(abs(np.mean([t["realized_pnl"] for t in losses_list]))) if losses_list else 0
    profit_loss_ratio = avg_win / avg_loss if avg_loss > 0 else 0

    final_balance = engine.account.equity
    eq_data = raw.get("equity_curve", [])
    peak_balance = float(np.max([e["equity"] for e in eq_data])) if eq_data else final_balance
    avg_hold = float(np.mean(holding_bars)) if holding_bars else 0.0

    formatted_trades = []
    running_balance = engine.account.initial_capital
    for i, t in enumerate(trades_raw):
        pnl = t.get("realized_pnl", 0)
        running_balance += pnl - t.get("commission", 0)
        price = t.get("price", 0)
        qty = t.get("quantity", 0)
        pnl_pct = pnl / (price * qty / leverage) if price * qty > 0 else 0
        formatted_trades.append({
            "trade_id": i + 1,
            "datetime": t.get("datetime", ""),
            "action": t.get("action", ""),
            "side": t.get("side", ""),
            "price": price,
            "quantity": qty,
            "leverage": t.get("leverage", 1),
            "fee": t.get("commission", 0),
            "pnl": pnl,
            "pnl_pct": pnl_pct,
            "balance_after": running_balance,
            "reason": t.get("reason", ""),
        })

    equity_curve_out = [
        {"datetime": e["datetime"], "equity": e["equity"]}
        for e in eq_data
    ]

    metrics = {
        "total_return": perf.get("total_return", 0) * engine.account.initial_capital,
        "total_return_pct": perf.get("total_return", 0),
        "annual_return_pct": perf.get("annual_return", 0),
        "sharpe_ratio": perf.get("sharpe_ratio", 0),
        "sortino_ratio": perf.get("sortino_ratio", 0),
        "max_drawdown_pct": perf.get("max_drawdown", 0),
        "calmar_ratio": perf.get("calmar_ratio", 0),
        "win_rate": win_rate,
        "profit_loss_ratio": profit_loss_ratio,
        "total_trades": len(trades_raw),
        "winning_trades": len(wins),
        "losing_trades": len(losses_list),
        "avg_holding_bars": avg_hold,
        "total_commission": perf.get("total_commission", 0),
        "total_slippage_cost": perf.get("total_slippage_cost", 0),
        "net_funding": perf.get("net_funding", 0),
        "liquidation_count": perf.get("liquidation_count", 0),
        "final_balance": final_balance,
        "peak_balance": peak_balance,
        "total_signals": len(signals),
        "signals_executed": signals_executed,
    }

    metrics = _sanitize_floats(metrics)
    formatted_trades = [_sanitize_floats(t) for t in formatted_trades]
    equity_curve_out = [_sanitize_floats(e) for e in equity_curve_out]

    return {
        "metrics": metrics,
        "trades": formatted_trades,
        "equity_curve": equity_curve_out,
    }


def _sanitize_floats(d: dict) -> dict:
    """将 dict 中所有 NaN/Inf/numpy 类型转为 JSON 安全值。"""
    clean = {}
    for k, v in d.items():
        if isinstance(v, float):
            if v != v or v == float("inf") or v == float("-inf"):
                clean[k] = 0.0
            else:
                clean[k] = v
        elif isinstance(v, (np.floating, np.integer)):
            fv = float(v)
            clean[k] = 0.0 if (fv != fv or fv == float("inf") or fv == float("-inf")) else fv
        elif isinstance(v, np.bool_):
            clean[k] = bool(v)
        else:
            clean[k] = v
    return clean


def _normalize_ts(ts: str) -> str:
    """
    将各种时间戳格式统一为 'YYYY-MM-DD HH:MM:SS' 用于匹配。
    处理: ISO 8601 (T分隔), 带时区(+00:00/Z), pandas Timestamp 等。
    """
    if not ts:
        return ""
    s = str(ts).strip()
    s = s.replace("T", " ")
    # 去掉时区后缀
    if s.endswith("Z"):
        s = s[:-1]
    for tz_suffix in ("+00:00", "+0000", "-00:00", "-0000"):
        if s.endswith(tz_suffix):
            s = s[:-len(tz_suffix)]
            break
    # 去掉毫秒/微秒
    dot_pos = s.rfind(".")
    if dot_pos > 0:
        s = s[:dot_pos]
    return s.strip()


def _calc_position_size(
    available: float,
    price: float,
    leverage: int,
    risk_per_trade: float,
) -> float:
    """用可用余额的 risk_per_trade 比例作为保证金，杠杆放大。"""
    margin_to_use = available * risk_per_trade
    nominal = margin_to_use * leverage
    qty = nominal / price if price > 0 else 0
    return qty
