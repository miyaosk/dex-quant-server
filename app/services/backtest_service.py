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

from app.core.backtest_engine import run_backtest, run_backtest_multi
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
        执行完整回测流程，支持多币种信号。

        返回 BacktestResponse 格式的 dict。
        """
        backtest_id = f"bt_{uuid.uuid4().hex[:12]}"
        start_ts = time.time()
        created_at = datetime.now(timezone.utc).isoformat()

        try:
            if not signals:
                raise ValueError("信号列表为空，无法回测")

            sig_symbols = list({s.get("symbol", symbol) for s in signals if s.get("symbol")})
            if not sig_symbols:
                sig_symbols = [symbol]

            all_symbols = list(set(sig_symbols + ([symbol] if symbol else [])))

            logger.info(
                f"[{backtest_id}] 开始回测: {','.join(all_symbols)} {timeframe} "
                f"{start_date} → {end_date} | {len(signals)} 个信号"
            )

            dfs: dict[str, pd.DataFrame] = {}
            for sym in all_symbols:
                df = await self.data_service.get_klines(
                    symbol=sym,
                    interval=timeframe,
                    start_date=start_date,
                    end_date=end_date,
                    market="crypto_futures",
                )
                if not df.empty:
                    dfs[sym] = df
                    logger.info(f"[{backtest_id}] {sym}: {len(df)} 根 K 线")

            if not dfs:
                raise ValueError(f"未获取到任何币种的 K 线数据")

            primary_symbol = symbol if symbol in dfs else list(dfs.keys())[0]

            bt_config = {
                "symbol": primary_symbol,
                "symbols": list(dfs.keys()),
                "initial_capital": config.get("initial_capital", 100_000.0),
                "leverage": config.get("leverage", 1),
                "fee_rate": config.get("fee_rate", 0.0005),
                "slippage_bps": config.get("slippage_bps", 5.0),
                "margin_mode": config.get("margin_mode", "isolated"),
                "direction": config.get("direction", "long_short"),
                "risk_per_trade": config.get("risk_per_trade", 0.02),
            }

            if len(dfs) == 1:
                result = run_backtest(df=list(dfs.values())[0], signals=signals, config=bt_config)
            else:
                result = run_backtest_multi(dfs=dfs, signals=signals, config=bt_config)

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
    def _evaluate_conclusion(metrics: dict) -> str | dict:
        """
        综合评估回测结果。返回结论字符串，同时把评分细节写入 metrics。

        结论:
          approved         — A/B 级，可以实盘或小仓实盘
          paper_trade_first — C 级，建议先模拟观察
          rejected         — D/F 级，不建议使用

        评分标准 (每项 0-2 分，满分 14):
          收益率:  >20% = 2,  >0% = 1,  <=0% = 0
          Sharpe:  >1.5 = 2,  >0.5 = 1, <=0.5 = 0
          回撤:    <10% = 2,  <20% = 1, >=20% = 0
          胜率:    >50% = 2,  >35% = 1, <=35% = 0
          盈亏比:  >1.5 = 2,  >1.0 = 1, <=1.0 = 0
          交易数:  >=30 = 2,  >=10 = 1, <10 = 0
          爆仓:    0次 = 2,   --       >0 = 0

        等级:
          A (12-14): 优秀策略 → approved
          B (9-11):  良好策略 → approved
          C (6-8):   及格策略 → paper_trade_first
          D (3-5):   较差策略 → rejected
          F (0-2):   失败策略 → rejected
        """
        if not metrics:
            return "rejected"

        ret = metrics.get("total_return_pct", 0)
        sharpe = metrics.get("sharpe_ratio", 0)
        max_dd = abs(metrics.get("max_drawdown_pct", 0))
        win_rate = metrics.get("win_rate", 0)
        plr = metrics.get("profit_loss_ratio", 0)
        trades = metrics.get("total_trades", 0)
        liquidations = metrics.get("liquidation_count", 0)

        items = []

        # 收益率
        s = 2 if ret > 0.20 else (1 if ret > 0 else 0)
        items.append({"name": "收益率", "value": f"{ret:+.2%}",
                       "score": s, "max": 2,
                       "thresholds": ">20%=优 >0%=及格 ≤0%=差"})

        # Sharpe
        s = 2 if sharpe > 1.5 else (1 if sharpe > 0.5 else 0)
        items.append({"name": "Sharpe", "value": f"{sharpe:.2f}",
                       "score": s, "max": 2,
                       "thresholds": ">1.5=优 >0.5=及格 ≤0.5=差"})

        # 最大回撤
        s = 2 if max_dd < 0.10 else (1 if max_dd < 0.20 else 0)
        items.append({"name": "最大回撤", "value": f"{max_dd:.2%}",
                       "score": s, "max": 2,
                       "thresholds": "<10%=优 <20%=及格 ≥20%=差"})

        # 胜率
        s = 2 if win_rate > 0.50 else (1 if win_rate > 0.35 else 0)
        items.append({"name": "胜率", "value": f"{win_rate:.1%}",
                       "score": s, "max": 2,
                       "thresholds": ">50%=优 >35%=及格 ≤35%=差"})

        # 盈亏比
        s = 2 if plr > 1.5 else (1 if plr > 1.0 else 0)
        items.append({"name": "盈亏比", "value": f"{plr:.2f}",
                       "score": s, "max": 2,
                       "thresholds": ">1.5=优 >1.0=及格 ≤1.0=差"})

        # 交易数
        s = 2 if trades >= 30 else (1 if trades >= 10 else 0)
        items.append({"name": "交易数", "value": str(trades),
                       "score": s, "max": 2,
                       "thresholds": "≥30=优 ≥10=及格 <10=差"})

        # 爆仓
        s = 2 if liquidations == 0 else 0
        items.append({"name": "爆仓", "value": f"{liquidations}次",
                       "score": s, "max": 2,
                       "thresholds": "0次=优 >0=差"})

        total_score = sum(i["score"] for i in items)
        max_score = sum(i["max"] for i in items)

        if total_score >= 12:
            grade, conclusion = "A", "approved"
        elif total_score >= 9:
            grade, conclusion = "B", "approved"
        elif total_score >= 6:
            grade, conclusion = "C", "paper_trade_first"
        elif total_score >= 3:
            grade, conclusion = "D", "rejected"
        else:
            grade, conclusion = "F", "rejected"

        grade_labels = {
            "A": "优秀策略，可直接实盘",
            "B": "良好策略，建议小仓实盘验证",
            "C": "及格策略，建议先模拟观察",
            "D": "较差策略，需要优化后再测",
            "F": "失败策略，建议重新设计",
        }

        metrics["evaluation"] = {
            "score": total_score,
            "max_score": max_score,
            "grade": grade,
            "grade_label": grade_labels[grade],
            "items": items,
        }

        return conclusion
