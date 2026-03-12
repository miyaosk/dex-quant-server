"""
回测引擎 — 永续合约专项 + 字符串规则求值

核心能力:
  - 从 StrategySpec 的 features 配置自动计算指标
  - 解析 entry_rules / exit_rules 字符串表达式并动态求值
  - 多空双向持仓、逐仓/全仓保证金、杠杆 1x-125x
  - 资金费率 8h 结算、强平、止损/止盈、滑点、手续费
  - 输出标准化 metrics + trades + equity_curve

入口函数:
  run_backtest(df, spec, config) -> dict
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Optional

import numpy as np
import pandas as pd
from loguru import logger

from app.core.indicators import Indicators


# ═══════════════════════════════════════════
#  数据结构（内部使用）
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
    """永续合约回测引擎。保留完整的交易执行能力。"""

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

    # ── 交易操作 ──

    def open_long(self, symbol: str, qty: float, price: float,
                  mark_price: float, dt: str, leverage: int = None):
        self._open_position(symbol, "long", qty, price, mark_price, dt, leverage)

    def open_short(self, symbol: str, qty: float, price: float,
                   mark_price: float, dt: str, leverage: int = None):
        self._open_position(symbol, "short", qty, price, mark_price, dt, leverage)

    def close_long(self, symbol: str, qty: float, price: float,
                   mark_price: float, dt: str):
        self._close_position(symbol, "long", qty, price, mark_price, dt)

    def close_short(self, symbol: str, qty: float, price: float,
                    mark_price: float, dt: str):
        self._close_position(symbol, "short", qty, price, mark_price, dt)

    # ── 内部: 开仓 ──

    def _open_position(self, symbol: str, side: str, qty: float, price: float,
                       mark_price: float, dt: str, leverage: int = None):
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
            funding_fee=0.0, realized_pnl=0.0,
        ))

    # ── 内部: 平仓 ──

    def _close_position(self, symbol: str, side: str, qty: float, price: float,
                        mark_price: float, dt: str, action: str = "close"):
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
        self.account.balance += margin_released + realized_pnl - commission
        self.account.total_commission += commission
        self.account.total_slippage_cost += abs(slippage * close_qty)

        pos.quantity -= close_qty
        if pos.quantity <= 1e-10:
            pos.quantity = 0
            pos.side = "none"
            pos.margin = 0
            pos.stop_loss = None
            pos.take_profit = None

        self.account.trade_log.append(_TradeRecord(
            datetime=dt, symbol=symbol, side=side, action=action,
            quantity=close_qty, price=fill_price, mark_price=mark_price,
            leverage=pos.leverage, margin_used=0,
            commission=commission, slippage=abs(slippage * close_qty),
            funding_fee=0.0, realized_pnl=realized_pnl,
        ))

    # ── 每 bar 检查 ──

    def on_bar(self, dt: str, prices: dict[str, dict],
               funding_rates: dict[str, float] = None):
        """每 bar 执行: 更新盈亏 → 资金费率 → 止损止盈 → 强平 → 记录净值"""
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
            "drawdown": 0.0,
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
                    pos.symbol, pos.side, pos.quantity, pos.stop_loss, mark, dt, "close",
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
                    pos.symbol, pos.side, pos.quantity, pos.take_profit, mark, dt, "close",
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
                commission=0, slippage=0, funding_fee=0, realized_pnl=-lost_margin,
            ))

    # ── 结果汇总 ──

    def get_result(self) -> dict:
        eq_df = pd.DataFrame(self.account.equity_curve)
        if eq_df.empty:
            return {"error": "无回测数据"}

        equities = eq_df["equity"].values
        peak = np.maximum.accumulate(equities)
        drawdowns = (equities - peak) / peak
        eq_df["drawdown"] = drawdowns

        returns = np.diff(equities) / equities[:-1] if len(equities) > 1 else np.array([0])

        total_return = (equities[-1] / equities[0]) - 1
        n_days = len(equities)
        annual_return = (1 + total_return) ** (365 / max(n_days, 1)) - 1
        volatility = float(np.std(returns) * np.sqrt(365)) if len(returns) > 1 else 0

        sharpe = (annual_return) / volatility if volatility > 0 else 0
        downside = returns[returns < 0]
        downside_std = float(np.std(downside) * np.sqrt(365)) if len(downside) > 0 else 0
        sortino = annual_return / downside_std if downside_std > 0 else 0

        max_dd = float(np.min(drawdowns))
        max_dd_idx = int(np.argmin(drawdowns))
        peak_idx = int(np.argmax(equities[:max_dd_idx + 1])) if max_dd_idx > 0 else 0
        max_dd_duration = max_dd_idx - peak_idx
        calmar = annual_return / abs(max_dd) if max_dd != 0 else 0

        trades = self.account.trade_log
        close_trades = [
            t for t in trades
            if t.action in ("close", "liquidation") and t.realized_pnl != 0
        ]
        wins = [t for t in close_trades if t.realized_pnl > 0]
        losses = [t for t in close_trades if t.realized_pnl < 0]
        win_rate = len(wins) / len(close_trades) if close_trades else 0
        avg_win = float(np.mean([t.realized_pnl for t in wins])) if wins else 0
        avg_loss = float(abs(np.mean([t.realized_pnl for t in losses]))) if losses else 0
        profit_loss_ratio = avg_win / avg_loss if avg_loss > 0 else float("inf")

        net_funding = self.account.total_funding_received - self.account.total_funding_paid

        return {
            "performance": {
                "total_return": total_return,
                "annual_return": annual_return,
                "sharpe_ratio": sharpe,
                "sortino_ratio": sortino,
                "max_drawdown": max_dd,
                "max_drawdown_duration": int(max_dd_duration),
                "calmar_ratio": calmar,
                "volatility": volatility,
                "win_rate": win_rate,
                "profit_loss_ratio": profit_loss_ratio,
                "total_trades": len(trades),
                "total_funding_paid": self.account.total_funding_paid,
                "total_funding_received": self.account.total_funding_received,
                "net_funding": net_funding,
                "total_commission": self.account.total_commission,
                "total_slippage_cost": self.account.total_slippage_cost,
                "liquidation_count": self.account.liquidation_count,
            },
            "equity_curve": eq_df.to_dict("records"),
            "trade_log": [vars(t) for t in trades],
            "funding_log": self.account.funding_log,
        }


# ═══════════════════════════════════════════
#  指标计算：从 StrategySpec.features 生成列
# ═══════════════════════════════════════════


_INDICATOR_BUILDERS = {
    "sma": lambda df, p: Indicators.sma(df["close"].values, p.get("period", 20)),
    "ema": lambda df, p: Indicators.ema(df["close"].values, p.get("period", 20)),
    "rsi": lambda df, p: Indicators.rsi(df["close"].values, p.get("period", 14)),
    "atr": lambda df, p: Indicators.atr(
        df["high"].values, df["low"].values, df["close"].values, p.get("period", 14),
    ),
    "volume_ma": lambda df, p: Indicators.volume_ma(df["volume"].values, p.get("period", 20)),
}


def _build_macd(df: pd.DataFrame, params: dict) -> dict[str, np.ndarray]:
    fast = params.get("fast_period", 12)
    slow = params.get("slow_period", 26)
    signal = params.get("signal_period", 9)
    line, sig, hist = Indicators.macd(df["close"].values, fast, slow, signal)
    return {"macd_line": line, "macd_signal": sig, "macd_hist": hist}


def _build_bollinger(df: pd.DataFrame, params: dict) -> dict[str, np.ndarray]:
    period = params.get("period", 20)
    num_std = params.get("num_std", 2.0)
    upper, middle, lower = Indicators.bollinger_bands(df["close"].values, period, num_std)
    return {"bb_upper": upper, "bb_middle": middle, "bb_lower": lower}


def _build_kdj(df: pd.DataFrame, params: dict) -> dict[str, np.ndarray]:
    k_period = params.get("k_period", 9)
    d_period = params.get("d_period", 3)
    j_smooth = params.get("j_smooth", 3)
    k, d, j = Indicators.kdj(
        df["high"].values, df["low"].values, df["close"].values,
        k_period, d_period, j_smooth,
    )
    return {"kdj_k": k, "kdj_d": d, "kdj_j": j}


def compute_indicators(df: pd.DataFrame, features: list[dict]) -> pd.DataFrame:
    """
    根据 features 配置列表，在 df 上计算所有指标并添加为新列。

    features 示例:
        [
            {"name": "sma_20", "indicator": "sma", "params": {"period": 20}},
            {"name": "rsi_14", "indicator": "rsi", "params": {"period": 14}},
            {"name": "macd",   "indicator": "macd", "params": {}},
            {"name": "bb",     "indicator": "bollinger", "params": {"period": 20}},
        ]

    对于返回多列的指标 (macd/bollinger/kdj)，name 字段被忽略，
    使用预设列名 (macd_line/macd_signal/macd_hist 等)。
    """
    df = df.copy()

    for feat in features:
        indicator = feat.get("indicator", feat.get("name", "")).lower()
        params = feat.get("params", {})
        name = feat.get("name", indicator)

        if indicator == "macd":
            for col_name, values in _build_macd(df, params).items():
                df[col_name] = values
        elif indicator in ("bollinger", "bb", "bollinger_bands"):
            for col_name, values in _build_bollinger(df, params).items():
                df[col_name] = values
        elif indicator == "kdj":
            for col_name, values in _build_kdj(df, params).items():
                df[col_name] = values
        elif indicator in _INDICATOR_BUILDERS:
            df[name] = _INDICATOR_BUILDERS[indicator](df, params)
        else:
            logger.warning(f"未知指标: {indicator}，跳过")

    return df


# ═══════════════════════════════════════════
#  规则求值器：解析 "close > sma_20" 等字符串
# ═══════════════════════════════════════════


_COMPARISON_OPS = {
    ">=": lambda a, b: a >= b,
    "<=": lambda a, b: a <= b,
    ">":  lambda a, b: a > b,
    "<":  lambda a, b: a < b,
    "==": lambda a, b: a == b,
    "!=": lambda a, b: a != b,
}

_OP_PATTERN = re.compile(r"(>=|<=|!=|==|>|<)")


def _resolve_value(token: str, row: pd.Series) -> float:
    """
    解析一个 token：如果是 DataFrame 列名则取值，否则作为浮点字面量。

    支持的列名: close, open, high, low, volume, sma_20, rsi_14,
    macd_line, macd_hist, bb_upper, bb_lower 等任意已计算列。
    """
    token = token.strip()
    if token in row.index:
        val = row[token]
        return float(val) if not pd.isna(val) else float("nan")
    try:
        return float(token)
    except ValueError:
        raise ValueError(f"无法解析 token: '{token}'，不是已知列名也不是数值")


def evaluate_rule(rule: str, row: pd.Series) -> bool:
    """
    对单行数据求值一条规则。

    规则格式: "<左操作数> <比较运算符> <右操作数>"
    示例: "close > sma_20", "rsi_14 < 30", "macd_hist > 0"

    任一操作数为 NaN 时返回 False。
    """
    parts = _OP_PATTERN.split(rule, maxsplit=1)
    if len(parts) != 3:
        logger.warning(f"规则格式错误: '{rule}'，需要 '<left> <op> <right>'")
        return False

    left_token, op, right_token = parts
    try:
        left_val = _resolve_value(left_token, row)
        right_val = _resolve_value(right_token, row)
    except ValueError as e:
        logger.warning(f"规则求值失败: {e}")
        return False

    if np.isnan(left_val) or np.isnan(right_val):
        return False

    return _COMPARISON_OPS[op](left_val, right_val)


def evaluate_rules(rules: list[str], row: pd.Series) -> bool:
    """所有规则取 AND：全部为 True 才触发信号。"""
    if not rules:
        return False
    return all(evaluate_rule(r, row) for r in rules)


# ═══════════════════════════════════════════
#  顶层入口: run_backtest
# ═══════════════════════════════════════════


def run_backtest(
    df: pd.DataFrame,
    spec: dict,
    config: dict,
    funding_df: pd.DataFrame = None,
) -> dict:
    """
    端到端回测。

    参数:
        df: OHLCV DataFrame，必须包含 datetime/open/high/low/close/volume
        spec: StrategySpec dict (可由 StrategySpec.model_dump() 得到)
        config: BacktestRequest 中的回测参数 dict，字段:
            initial_capital, fee_rate, slippage_bps, margin_mode,
            funding_rate_enabled, start_date, end_date
        funding_df: 可选的资金费率 DataFrame [datetime, funding_rate]

    返回:
        {
            "metrics": { ... BacktestMetrics 各字段 ... },
            "trades": [ ... 交易记录列表 ... ],
            "equity_curve": [ ... {datetime, equity} 列表 ... ],
        }
    """
    if df.empty:
        return {"metrics": {}, "trades": [], "equity_curve": [], "error": "数据为空"}

    # ── 1) 计算指标 ──
    features = spec.get("features", [])
    if isinstance(features, list) and features:
        feat_dicts = []
        for f in features:
            if isinstance(f, dict):
                feat_dicts.append(f)
            elif hasattr(f, "model_dump"):
                feat_dicts.append(f.model_dump())
            else:
                feat_dicts.append({"name": str(f), "indicator": str(f), "params": {}})
        df = compute_indicators(df, feat_dicts)

    # ── 2) 初始化引擎 ──
    pos_sizing = spec.get("position_sizing", {})
    if hasattr(pos_sizing, "model_dump"):
        pos_sizing = pos_sizing.model_dump()

    risk_limits = spec.get("risk_limits", {})
    if hasattr(risk_limits, "model_dump"):
        risk_limits = risk_limits.model_dump()

    leverage = pos_sizing.get("leverage", 1)

    engine = BacktestEngine(
        initial_capital=config.get("initial_capital", 100_000.0),
        default_leverage=leverage,
        margin_mode=config.get("margin_mode", "isolated"),
        slippage_bps=config.get("slippage_bps", 2.0),
        taker_fee=config.get("fee_rate", 0.0005),
        maker_fee=config.get("fee_rate", 0.0005) * 0.4,
        enable_funding=config.get("funding_rate_enabled", True),
        enable_liquidation=True,
        maintenance_margin_rate=0.005,
    )

    entry_rules = spec.get("entry_rules", [])
    exit_rules = spec.get("exit_rules", [])
    direction = spec.get("direction", "long_short")
    universe = spec.get("universe", ["UNKNOWN"])
    symbol = universe[0] if universe else "UNKNOWN"

    stop_loss_pct = risk_limits.get("stop_loss")
    take_profit_pct = risk_limits.get("take_profit")
    risk_per_trade = pos_sizing.get("risk_per_trade", 0.005)
    sizing_mode = pos_sizing.get("mode", "risk_based")

    # ── 3) 预处理资金费率索引 ──
    funding_map: dict[str, float] = {}
    if funding_df is not None and not funding_df.empty:
        for _, fr_row in funding_df.iterrows():
            dt_key = str(fr_row["datetime"])
            funding_map[dt_key] = float(fr_row["funding_rate"])

    # ── 4) 遍历每根 bar ──
    holding_bars: list[int] = []
    current_hold_start: int = -1

    for idx in range(len(df)):
        row = df.iloc[idx]
        dt_str = str(row["datetime"])
        close = float(row["close"])
        high = float(row["high"])
        low = float(row["low"])

        pos = engine.account.get_position(symbol)

        # 资金费率查找
        fr = funding_map.get(dt_str)
        funding_rates = {symbol: fr} if fr is not None else None

        # ── 检查退出 ──
        if pos.side != "none":
            exit_signal = evaluate_rules(exit_rules, row)
            if exit_signal:
                if pos.side == "long":
                    engine.close_long(symbol, 0, close, close, dt_str)
                else:
                    engine.close_short(symbol, 0, close, close, dt_str)
                if current_hold_start >= 0:
                    holding_bars.append(idx - current_hold_start)
                    current_hold_start = -1

        # ── 检查进入 ──
        if pos.side == "none":
            entry_signal = evaluate_rules(entry_rules, row)
            if entry_signal:
                qty = _calc_position_size(
                    engine.account.available_balance, close, leverage,
                    sizing_mode, risk_per_trade,
                    pos_sizing.get("fixed_quantity"),
                )
                if qty > 0:
                    if direction in ("long", "long_short", "long_only"):
                        engine.open_long(symbol, qty, close, close, dt_str, leverage)
                    elif direction in ("short", "short_only"):
                        engine.open_short(symbol, qty, close, close, dt_str, leverage)

                    # 设置止损止盈
                    pos = engine.account.get_position(symbol)
                    if pos.side != "none" and stop_loss_pct:
                        if pos.side == "long":
                            pos.stop_loss = close * (1 - stop_loss_pct)
                        else:
                            pos.stop_loss = close * (1 + stop_loss_pct)
                    if pos.side != "none" and take_profit_pct:
                        if pos.side == "long":
                            pos.take_profit = close * (1 + take_profit_pct)
                        else:
                            pos.take_profit = close * (1 - take_profit_pct)

                    current_hold_start = idx

        # ── 每 bar 更新 ──
        engine.on_bar(dt_str, {
            symbol: {"close": close, "high": high, "low": low, "mark_price": close},
        }, funding_rates)

    # ── 5) 尾部平仓 ──
    pos = engine.account.get_position(symbol)
    if pos.side != "none" and len(df) > 0:
        last_row = df.iloc[-1]
        dt_str = str(last_row["datetime"])
        close = float(last_row["close"])
        if pos.side == "long":
            engine.close_long(symbol, 0, close, close, dt_str)
        else:
            engine.close_short(symbol, 0, close, close, dt_str)
        if current_hold_start >= 0:
            holding_bars.append(len(df) - 1 - current_hold_start)

    # ── 6) 汇总结果 ──
    raw = engine.get_result()
    perf = raw.get("performance", {})

    trades_raw = raw.get("trade_log", [])
    close_trades = [t for t in trades_raw if t.get("action") in ("close", "liquidation")]
    wins = [t for t in close_trades if t.get("realized_pnl", 0) > 0]
    losses_list = [t for t in close_trades if t.get("realized_pnl", 0) < 0]

    final_balance = engine.account.equity
    peak_balance = float(np.max([e["equity"] for e in raw.get("equity_curve", [{"equity": final_balance}])]))
    avg_hold = float(np.mean(holding_bars)) if holding_bars else 0.0

    # 构建标准化 trades 输出
    formatted_trades = []
    for i, t in enumerate(trades_raw):
        pnl = t.get("realized_pnl", 0)
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
            "balance_after": 0.0,
            "reason": t.get("action", ""),
        })

    # 构建简化 equity_curve
    equity_curve_out = [
        {"datetime": e["datetime"], "equity": e["equity"]}
        for e in raw.get("equity_curve", [])
    ]

    metrics = {
        "total_return": perf.get("total_return", 0) * engine.account.initial_capital,
        "total_return_pct": perf.get("total_return", 0),
        "annual_return_pct": perf.get("annual_return", 0),
        "sharpe_ratio": perf.get("sharpe_ratio", 0),
        "max_drawdown_pct": perf.get("max_drawdown", 0),
        "win_rate": perf.get("win_rate", 0),
        "profit_loss_ratio": perf.get("profit_loss_ratio", 0),
        "total_trades": perf.get("total_trades", 0),
        "winning_trades": len(wins),
        "losing_trades": len(losses_list),
        "avg_holding_bars": avg_hold,
        "total_commission": perf.get("total_commission", 0),
        "net_funding": perf.get("net_funding", 0),
        "liquidation_count": perf.get("liquidation_count", 0),
        "final_balance": final_balance,
        "peak_balance": peak_balance,
    }

    return {
        "metrics": metrics,
        "trades": formatted_trades,
        "equity_curve": equity_curve_out,
    }


def _calc_position_size(
    available: float,
    price: float,
    leverage: int,
    mode: str,
    risk_per_trade: float,
    fixed_quantity: float = None,
) -> float:
    """根据仓位管理模式计算下单数量。"""
    if mode == "fixed" and fixed_quantity:
        return fixed_quantity

    # risk_based: 用可用余额的 risk_per_trade 比例作为保证金
    margin_to_use = available * risk_per_trade
    # 杠杆放大名义价值
    nominal = margin_to_use * leverage
    qty = nominal / price if price > 0 else 0
    return qty
