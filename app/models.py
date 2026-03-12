"""
Pydantic 数据模型 — API 请求/响应 & 策略规范

与 shared/schemas/strategy_spec.json 保持一致。
"""

from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, Field


# ═══════════════════════════════════════════
#  策略规范子模型
# ═══════════════════════════════════════════


class FeatureConfig(BaseModel):
    """特征/指标配置。"""
    name: str
    indicator: str = ""
    params: dict = {}


class PositionSizing(BaseModel):
    """仓位管理配置。"""
    mode: str = "risk_based"
    risk_per_trade: float = 0.005
    fixed_quantity: Optional[float] = None
    leverage: int = 1


class RiskLimits(BaseModel):
    """风控限制。"""
    max_position_pct: float = 0.2
    max_daily_loss: float = 0.02
    max_concurrent_positions: int = 1
    stop_loss: Optional[float] = None
    take_profit: Optional[float] = None


class ExecutionConstraints(BaseModel):
    """执行约束。"""
    bar_close_only: bool = True
    order_type: str = "market"
    allow_pyramiding: bool = False


class DataRequirements(BaseModel):
    """数据需求声明。"""
    ohlcv: bool = True
    funding_rate: bool = False
    open_interest: bool = False
    onchain: bool = False


# ═══════════════════════════════════════════
#  策略规范（顶层）
# ═══════════════════════════════════════════


class StrategySpec(BaseModel):
    """策略完整规范，对应 strategy_spec.json 结构。"""
    strategy_id: str = ""
    version: str = "v1.0"
    name: str
    market: str = "crypto"
    venue: list[str] = Field(default_factory=lambda: ["binance_futures"])
    universe: list[str]
    timeframe: str = "1h"
    direction: str = "long_short"
    data_requirements: DataRequirements = Field(default_factory=DataRequirements)
    features: list[FeatureConfig] = Field(default_factory=list)
    entry_rules: list[str] = Field(default_factory=list)
    exit_rules: list[str] = Field(default_factory=list)
    position_sizing: PositionSizing = Field(default_factory=PositionSizing)
    risk_limits: RiskLimits = Field(default_factory=RiskLimits)
    execution_constraints: ExecutionConstraints = Field(default_factory=ExecutionConstraints)
    review_status: str = "pending"
    lifecycle_state: str = "draft"


# ═══════════════════════════════════════════
#  回测请求/响应
# ═══════════════════════════════════════════


class BacktestRequest(BaseModel):
    """回测请求参数。"""
    strategy: StrategySpec
    start_date: str
    end_date: str
    initial_capital: float = 100_000.0
    fee_rate: float = 0.0005
    slippage_bps: float = 2.0
    margin_mode: str = "isolated"
    funding_rate_enabled: bool = True


class TradeRecord(BaseModel):
    """单笔交易记录。"""
    trade_id: int
    datetime: str
    action: str
    side: str
    price: float
    quantity: float
    leverage: int
    fee: float
    pnl: float
    pnl_pct: float
    balance_after: float
    reason: str = ""


class BacktestMetrics(BaseModel):
    """回测绩效指标。"""
    total_return: float
    total_return_pct: float
    annual_return_pct: float
    sharpe_ratio: float
    max_drawdown_pct: float
    win_rate: float
    profit_loss_ratio: float
    total_trades: int
    winning_trades: int
    losing_trades: int
    avg_holding_bars: float
    total_commission: float
    net_funding: float
    liquidation_count: int
    final_balance: float
    peak_balance: float


class BacktestResponse(BaseModel):
    """回测结果响应。"""
    backtest_id: str
    strategy_id: str
    status: str
    metrics: Optional[BacktestMetrics] = None
    trades: list[TradeRecord] = Field(default_factory=list)
    equity_curve: list[dict] = Field(default_factory=list)
    error: Optional[str] = None
    created_at: str
    elapsed_ms: int


# ═══════════════════════════════════════════
#  K 线数据请求
# ═══════════════════════════════════════════


class KlineRequest(BaseModel):
    """K 线数据拉取请求。"""
    symbol: str
    interval: str = "1h"
    start_date: str
    end_date: str
    market: str = "crypto_futures"


# ═══════════════════════════════════════════
#  策略列表条目
# ═══════════════════════════════════════════


class StrategyListItem(BaseModel):
    """策略列表中的摘要条目。"""
    strategy_id: str
    name: str
    version: str
    universe: list[str]
    timeframe: str
    lifecycle_state: str
    created_at: str
    updated_at: str
