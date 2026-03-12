"""
回测编排服务 — 串联数据获取、引擎执行、结果持久化

调用流程:
  1. 从 DataService 获取 K 线数据（带缓存）
  2. 按需获取资金费率
  3. 调用 run_backtest 执行回测
  4. 将结果写入 SQLite
  5. 返回标准化 BacktestResponse dict
"""

from __future__ import annotations

import json
import time
import uuid
from datetime import datetime, timezone

import pandas as pd
from loguru import logger

from app.core.backtest_engine import run_backtest
from app.services.data_service import DataService
from app import database


class BacktestService:
    """回测编排服务。"""

    def __init__(self, data_service: DataService):
        self.data_service = data_service

    async def execute(self, spec: dict, config: dict) -> dict:
        """
        执行完整回测流程。

        参数:
            spec: StrategySpec dict
            config: 回测配置 dict，包含:
                start_date, end_date, initial_capital,
                fee_rate, slippage_bps, margin_mode, funding_rate_enabled

        返回:
            BacktestResponse 格式的 dict
        """
        backtest_id = f"bt_{uuid.uuid4().hex[:12]}"
        strategy_id = spec.get("strategy_id", "unknown")
        start_ts = time.time()
        created_at = datetime.now(timezone.utc).isoformat()

        try:
            # ── 1) 获取数据 ──
            universe = spec.get("universe", [])
            if not universe:
                raise ValueError("策略 universe 为空，至少需要一个交易对")

            symbol = universe[0]
            timeframe = spec.get("timeframe", "1h")
            start_date = config.get("start_date", "2024-01-01")
            end_date = config.get("end_date", "2025-01-01")

            # 判断市场类型
            venue = spec.get("venue", ["binance_futures"])
            if isinstance(venue, list):
                venue_str = venue[0] if venue else "binance_futures"
            else:
                venue_str = str(venue)

            market = "crypto_futures"
            if "spot" in venue_str:
                market = "crypto_spot"
            elif symbol.startswith("RWA:"):
                market = "stock"
            elif symbol.startswith("COMM:"):
                market = "commodity"
            elif symbol.startswith("METAL:"):
                market = "metal"

            logger.info(
                f"[{backtest_id}] 开始回测: {symbol} {timeframe} "
                f"{start_date} → {end_date} ({market})"
            )

            df = await self.data_service.get_klines(
                symbol=symbol,
                interval=timeframe,
                start_date=start_date,
                end_date=end_date,
                market=market,
            )

            if df.empty:
                raise ValueError(f"未获取到 {symbol} 的 K 线数据")

            logger.info(f"[{backtest_id}] 获取到 {len(df)} 根 K 线")

            # ── 2) 获取资金费率 ──
            funding_df = None
            data_req = spec.get("data_requirements", {})
            if hasattr(data_req, "model_dump"):
                data_req = data_req.model_dump()

            funding_enabled = config.get("funding_rate_enabled", True)
            needs_funding = data_req.get("funding_rate", False)

            if funding_enabled and needs_funding and market == "crypto_futures":
                try:
                    funding_df = await self.data_service.get_funding_rate(
                        symbol=symbol,
                        start_date=start_date,
                        end_date=end_date,
                    )
                    logger.info(f"[{backtest_id}] 获取到 {len(funding_df)} 条资金费率")
                except Exception as e:
                    logger.warning(f"[{backtest_id}] 资金费率获取失败: {e}")

            # ── 3) 执行回测 ──
            result = run_backtest(
                df=df,
                spec=spec,
                config=config,
                funding_df=funding_df,
            )

            elapsed_ms = int((time.time() - start_ts) * 1000)
            status = "completed"
            error = result.get("error")
            if error:
                status = "failed"

            metrics = result.get("metrics", {})
            trades = result.get("trades", [])
            equity_curve = result.get("equity_curve", [])

            logger.info(
                f"[{backtest_id}] 回测完成: "
                f"收益率 {metrics.get('total_return_pct', 0):.2%}, "
                f"夏普 {metrics.get('sharpe_ratio', 0):.3f}, "
                f"交易 {metrics.get('total_trades', 0)} 笔, "
                f"耗时 {elapsed_ms}ms"
            )

            # ── 4) 持久化 ──
            await database.save_backtest_result(
                backtest_id=backtest_id,
                strategy_id=strategy_id,
                config_json=json.dumps(config, ensure_ascii=False),
                metrics_json=json.dumps(metrics, ensure_ascii=False, default=str),
                trades_json=json.dumps(trades, ensure_ascii=False, default=str),
                equity_json=json.dumps(equity_curve, ensure_ascii=False, default=str),
                status=status,
                error=error,
                elapsed_ms=elapsed_ms,
            )

            # ── 5) 返回响应 ──
            return {
                "backtest_id": backtest_id,
                "strategy_id": strategy_id,
                "status": status,
                "metrics": metrics,
                "trades": trades,
                "equity_curve": equity_curve,
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
                config_json=json.dumps(config, ensure_ascii=False, default=str),
                metrics_json=None,
                trades_json=None,
                equity_json=None,
                status="failed",
                error=error_msg,
                elapsed_ms=elapsed_ms,
            )

            return {
                "backtest_id": backtest_id,
                "strategy_id": strategy_id,
                "status": "failed",
                "metrics": None,
                "trades": [],
                "equity_curve": [],
                "error": error_msg,
                "created_at": created_at,
                "elapsed_ms": elapsed_ms,
            }
