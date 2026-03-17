"""
回测编排服务 — 信号驱动架构

流程:
  1. 接收信号列表 + 回测配置
  2. 从 DataService 拉 K 线（15m/1h/2h/1d，带 MySQL 缓存）
  3. 调用 run_backtest 用信号驱动回测引擎
  4. 生成评估结论（通过/先模拟/驳回）
  5. 持久化结果到 MySQL
  6. 返回标准化 BacktestResponse dict
"""

from __future__ import annotations

import json
import time
import uuid
from datetime import datetime, timezone

import numpy as np
import pandas as pd
from loguru import logger

from app.core.backtest_engine import run_backtest
from app.services.data_service import DataService
from app import database


class BacktestService:
    """回测编排服务。"""

    def __init__(self, data_service: DataService):
        self.data_service = data_service

    async def execute(
        self,
        strategy_name: str,
        strategy_id: str,
        symbol: str,
        timeframe: str,
        start_date: str,
        end_date: str,
        signals: list[dict],
        config: dict,
    ) -> dict:
        """
        执行完整回测流程。

        返回 BacktestResponse 格式的 dict。
        """
        backtest_id = f"bt_{uuid.uuid4().hex[:12]}"
        start_ts = time.time()
        created_at = datetime.now(timezone.utc).isoformat()

        try:
            if not signals:
                raise ValueError("信号列表为空，无法回测")

            logger.info(
                f"[{backtest_id}] 开始回测: {symbol} {timeframe} "
                f"{start_date} → {end_date} | {len(signals)} 个信号"
            )

            # 1) 拉 K 线数据（带缓存）
            df = await self.data_service.get_klines(
                symbol=symbol,
                interval=timeframe,
                start_date=start_date,
                end_date=end_date,
                market="crypto_futures",
            )

            if df.empty:
                raise ValueError(f"未获取到 {symbol} 的 K 线数据")

            logger.info(f"[{backtest_id}] 获取到 {len(df)} 根 K 线")

            # 2) 运行信号驱动回测
            bt_config = {
                "symbol": symbol,
                "initial_capital": config.get("initial_capital", 100_000.0),
                "leverage": config.get("leverage", 1),
                "fee_rate": config.get("fee_rate", 0.0005),
                "slippage_bps": config.get("slippage_bps", 5.0),
                "margin_mode": config.get("margin_mode", "isolated"),
                "direction": config.get("direction", "long_short"),
                "risk_per_trade": config.get("risk_per_trade", 0.02),
            }

            result = run_backtest(df=df, signals=signals, config=bt_config)

            elapsed_ms = int((time.time() - start_ts) * 1000)
            status = "completed"
            error = result.get("error")
            if error:
                status = "failed"

            metrics = result.get("metrics", {})
            trades = result.get("trades", [])
            equity_curve = result.get("equity_curve", [])

            # 3) 生成评估结论
            conclusion = self._evaluate_conclusion(metrics)

            logger.info(
                f"[{backtest_id}] 回测完成: "
                f"收益率 {metrics.get('total_return_pct', 0):.2%}, "
                f"夏普 {metrics.get('sharpe_ratio', 0):.3f}, "
                f"交易 {metrics.get('total_trades', 0)} 笔, "
                f"结论 {conclusion}, "
                f"耗时 {elapsed_ms}ms"
            )

            # 4) 持久化
            await database.save_backtest_result(
                backtest_id=backtest_id,
                strategy_id=strategy_id,
                strategy_name=strategy_name,
                config_json=json.dumps(bt_config, ensure_ascii=False),
                signals_json=json.dumps(signals, ensure_ascii=False, default=str),
                metrics_json=json.dumps(metrics, ensure_ascii=False, default=str),
                trades_json=json.dumps(trades, ensure_ascii=False, default=str),
                equity_json=json.dumps(equity_curve, ensure_ascii=False, default=str),
                conclusion=conclusion,
                status=status,
                error=error,
                elapsed_ms=elapsed_ms,
            )

            return {
                "backtest_id": backtest_id,
                "strategy_id": strategy_id,
                "strategy_name": strategy_name,
                "status": status,
                "metrics": metrics,
                "trades": trades,
                "equity_curve": equity_curve,
                "conclusion": conclusion,
                "error": error,
                "created_at": created_at,
                "elapsed_ms": elapsed_ms,
            }

        except Exception as e:
            elapsed_ms = int((time.time() - start_ts) * 1000)
            error_msg = str(e)
            logger.error(f"[{backtest_id}] 回测失败: {error_msg}")

            await database.save_backtest_result(
                backtest_id=backtest_id,
                strategy_id=strategy_id,
                strategy_name=strategy_name,
                config_json=json.dumps(config, ensure_ascii=False, default=str),
                signals_json=json.dumps(signals[:5], ensure_ascii=False, default=str),
                metrics_json=None,
                trades_json=None,
                equity_json=None,
                conclusion="failed",
                status="failed",
                error=error_msg,
                elapsed_ms=elapsed_ms,
            )

            return {
                "backtest_id": backtest_id,
                "strategy_id": strategy_id,
                "strategy_name": strategy_name,
                "status": "failed",
                "metrics": None,
                "trades": [],
                "equity_curve": [],
                "conclusion": "failed",
                "error": error_msg,
                "created_at": created_at,
                "elapsed_ms": elapsed_ms,
            }

    @staticmethod
    def _evaluate_conclusion(metrics: dict) -> str:
        """
        综合评估回测结果，给出三选一结论。

        approved:         核心指标全部达标
        paper_trade_first: 部分指标不理想，建议先模拟观察
        rejected:         指标太差或有明显问题
        """
        if not metrics:
            return "rejected"

        total_return_pct = metrics.get("total_return_pct", 0)
        sharpe = metrics.get("sharpe_ratio", 0)
        max_dd = abs(metrics.get("max_drawdown_pct", 0))
        win_rate = metrics.get("win_rate", 0)
        total_trades = metrics.get("total_trades", 0)
        liquidation_count = metrics.get("liquidation_count", 0)

        if liquidation_count > 0 or total_return_pct < -0.1:
            return "rejected"

        if total_trades < 5:
            return "rejected"

        score = 0
        if total_return_pct > 0:
            score += 1
        if total_return_pct > 0.1:
            score += 1
        if sharpe > 1.0:
            score += 1
        if sharpe > 1.5:
            score += 1
        if max_dd < 0.2:
            score += 1
        if max_dd < 0.1:
            score += 1
        if win_rate > 0.4:
            score += 1
        if total_trades >= 30:
            score += 1

        if score >= 6:
            return "approved"
        elif score >= 3:
            return "paper_trade_first"
        else:
            return "rejected"
