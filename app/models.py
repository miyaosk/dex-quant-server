"""
Pydantic 数据模型 — 信号驱动回测架构

核心流程：Skill 生成策略脚本 → 跑脚本产出信号 → 发信号到 Server → Server 拉 K 线 + 回测
"""

from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, Field


# ═══════════════════════════════════════════
#  策略（脚本为核心）
# ═══════════════════════════════════════════


class StrategyCreate(BaseModel):
    """创建/更新策略。脚本源码是策略的唯一真相。"""
    name: str
    description: str = ""
    script_content: str = ""
    symbol: str = "BTCUSDT"
    timeframe: str = "1h"
    direction: str = "long_short"
    version: str = "v1.0"
    tags: list[str] = Field(default_factory=list)


class StrategyDetail(BaseModel):
    """策略完整详情。"""
    strategy_id: str
    name: str
    description: str = ""
    script_content: str = ""
    symbol: str = "BTCUSDT"
    timeframe: str = "1h"
    direction: str = "long_short"
    version: str = "v1.0"
    tags: list[str] = Field(default_factory=list)
    status: str = "draft"
    created_at: str = ""
    updated_at: str = ""


class StrategyListItem(BaseModel):
    """策略列表摘要。"""
    strategy_id: str
    name: str
    symbol: str
    timeframe: str
    version: str
    status: str
    created_at: str
    updated_at: str


# ═══════════════════════════════════════════
#  信号（策略脚本的输出）
# ═══════════════════════════════════════════


class SignalItem(BaseModel):
    """单个交易信号 — 策略脚本 generate_signals() 的输出。"""
    timestamp: str
    symbol: str
    action: str = Field(description="buy / sell / close / hold")
    direction: str = Field(default="long", description="long / short")
    confidence: float = Field(default=1.0, ge=0, le=1)
    reason: str = ""
    source_type: str = Field(default="technical", description="technical / social / onchain / mixed")
    price_at_signal: float = 0
    suggested_stop_loss: Optional[float] = None
    suggested_take_profit: Optional[float] = None
    metadata: dict = Field(default_factory=dict)


class SignalQuery(BaseModel):
    """信号查询参数。"""
    strategy_id: Optional[str] = None
    symbol: Optional[str] = None
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    limit: int = 200


# ═══════════════════════════════════════════
#  回测请求/响应
# ═══════════════════════════════════════════


class BacktestRequest(BaseModel):
    """
    回测请求 — Skill 端发过来的核心数据。

    包含：策略信息 + 信号列表 + 回测配置。
    Server 负责拉 K 线、用信号驱动引擎、返回结果。
    """
    strategy_name: str
    strategy_id: str = ""
    symbol: str = "BTCUSDT"
    timeframe: str = Field(default="1h", description="K 线周期：15m / 1h / 2h / 1d")
    start_date: str
    end_date: str
    signals: list[SignalItem]
    initial_capital: float = 100_000.0
    leverage: int = 1
    fee_rate: float = 0.0005
    slippage_bps: float = 5.0
    margin_mode: str = "isolated"
    direction: str = "long_short"


class BacktestMetrics(BaseModel):
    """回测绩效指标。"""
    total_return: float = 0
    total_return_pct: float = 0
    annual_return_pct: float = 0
    sharpe_ratio: float = 0
    sortino_ratio: float = 0
    max_drawdown_pct: float = 0
    calmar_ratio: float = 0
    win_rate: float = 0
    profit_loss_ratio: float = 0
    total_trades: int = 0
    winning_trades: int = 0
    losing_trades: int = 0
    avg_holding_bars: float = 0
    total_commission: float = 0
    total_slippage_cost: float = 0
    net_funding: float = 0
    liquidation_count: int = 0
    final_balance: float = 0
    peak_balance: float = 0
    total_signals: int = 0
    signals_executed: int = 0


class TradeRecord(BaseModel):
    """单笔交易记录。"""
    trade_id: int = 0
    datetime: str = ""
    action: str = ""
    side: str = ""
    price: float = 0
    quantity: float = 0
    leverage: int = 1
    fee: float = 0
    pnl: float = 0
    pnl_pct: float = 0
    balance_after: float = 0
    reason: str = ""


class BacktestResponse(BaseModel):
    """回测结果响应。"""
    backtest_id: str
    strategy_id: str
    strategy_name: str = ""
    status: str
    metrics: Optional[BacktestMetrics] = None
    trades: list[TradeRecord] = Field(default_factory=list)
    equity_curve: list[dict] = Field(default_factory=list)
    conclusion: str = ""
    error: Optional[str] = None
    created_at: str = ""
    elapsed_ms: int = 0


# ═══════════════════════════════════════════
#  K 线数据请求
# ═══════════════════════════════════════════


class KlineRequest(BaseModel):
    """K 线数据请求。"""
    symbol: str
    interval: str = Field(default="1h", description="15m / 1h / 2h / 1d")
    start_date: str
    end_date: str
    exchange: str = "binance"


# ═══════════════════════════════════════════
#  服务器端执行回测（上传脚本，服务器跑）
# ═══════════════════════════════════════════


class ServerBacktestRequest(BaseModel):
    """
    服务器端执行回测 — 脚本上传到服务器执行。

    两种方式（二选一）:
      1. 传 strategy_id → 从数据库加载已保存的脚本
      2. 传 script_content → 直接执行传入的脚本
    """
    strategy_id: str = ""
    script_content: str = ""
    strategy_name: str = ""
    symbol: str = "BTCUSDT"
    timeframe: str = Field(default="1h", description="K 线周期：15m / 1h / 2h / 1d")
    start_date: str = Field(description="回测起始日期 YYYY-MM-DD")
    end_date: str = Field(description="回测结束日期 YYYY-MM-DD")
    initial_capital: float = 100_000.0
    leverage: int = 1
    fee_rate: float = 0.0005
    slippage_bps: float = 5.0
    margin_mode: str = "isolated"
    direction: str = "long_short"


# ═══════════════════════════════════════════
#  认证 & 配额
# ═══════════════════════════════════════════


# ═══════════════════════════════════════════
#  参数优化
# ═══════════════════════════════════════════


class ParamDef(BaseModel):
    """单个参数定义。"""
    name: str
    type: str = Field(description="int / float / choice")
    low: Optional[float] = None
    high: Optional[float] = None
    step: Optional[float] = None
    choices: list = Field(default_factory=list)


class OptimizeRequest(BaseModel):
    """
    参数优化请求 — 批量回测寻找最优参数。

    脚本中用 PARAMS['xxx'] 引用参数，服务器自动替换。
    """
    script_content: str = Field(description="策略脚本源码，用 PARAMS['xxx'] 引用可调参数")
    params: list[ParamDef] = Field(description="参数空间定义列表")
    strategy_name: str = ""
    symbol: str = "BTCUSDT"
    timeframe: str = "4h"
    start_date: str = Field(description="回测起始日期")
    end_date: str = Field(description="回测结束日期")
    initial_capital: float = 100_000.0
    leverage: int = 3
    fee_rate: float = 0.0005
    slippage_bps: float = 5.0
    margin_mode: str = "isolated"
    direction: str = "long_short"
    method: str = Field(default="grid", description="grid / genetic / random / bayesian / annealing / pso")
    max_combinations: int = Field(default=200, description="最大组合数限制")
    fitness_metric: str = Field(default="sharpe_ratio", description="优化目标指标")


class OptimizeResultItem(BaseModel):
    """单个参数组合的回测结果。"""
    rank: int = 0
    params: dict = Field(default_factory=dict)
    fitness: float = 0
    total_return_pct: float = 0
    sharpe_ratio: float = 0
    sortino_ratio: float = 0
    max_drawdown_pct: float = 0
    win_rate: float = 0
    total_trades: int = 0
    profit_loss_ratio: float = 0
    final_balance: float = 0


class OptimizeResponse(BaseModel):
    """参数优化响应。"""
    status: str = "completed"
    method: str = "grid"
    total_combinations: int = 0
    evaluated: int = 0
    failed: int = 0
    best_params: dict = Field(default_factory=dict)
    best_fitness: float = 0
    results: list[OptimizeResultItem] = Field(default_factory=list, description="按fitness降序的Top结果")
    elapsed_ms: int = 0
    error: Optional[str] = None


class MachineRegisterRequest(BaseModel):
    """机器码注册请求。"""
    machine_code: str = Field(description="客户端硬件指纹哈希")


class MachineRegisterResponse(BaseModel):
    """注册/查询返回。"""
    token: str
    machine_code: str
    max_strategies: int = 3
    used_strategies: int = 0
    remaining: int = 3
    status: str = "active"


class QuotaResponse(BaseModel):
    """配额详情。"""
    machine_code: str
    max_strategies: int
    used_strategies: int
    remaining: int
    strategies: list[dict] = Field(default_factory=list, description="已注册的策略列表")
