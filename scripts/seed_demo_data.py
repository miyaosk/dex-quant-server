"""
生成演示数据 — 创建多个策略 + 模拟回测结果，让排行榜和后台有丰富内容。
直接写入数据库，不走 API 认证。

用法: python scripts/seed_demo_data.py
环境变量: MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DB
"""

import json
import random
import uuid
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.utils.mysql_client import mysql

DEMO_STRATEGIES = [
    {"name": "BTC EMA 双线交叉", "symbol": "BTCUSDT", "timeframe": "4h", "direction": "long_short",
     "description": "EMA12 上穿 EMA26 买入，死叉卖出，配合成交量过滤", "tags": ["EMA", "趋势"]},
    {"name": "ETH 布林带回归", "symbol": "ETHUSDT", "timeframe": "1h", "direction": "long_short",
     "description": "价格触及布林下轨且 RSI<30 买入，触及上轨且 RSI>70 卖出", "tags": ["布林带", "RSI"]},
    {"name": "SOL 动量突破", "symbol": "SOLUSDT", "timeframe": "2h", "direction": "long",
     "description": "价格突破 20 日高点 + 成交量放大 2 倍以上做多", "tags": ["动量", "突破"]},
    {"name": "BTC RSI 超卖抄底", "symbol": "BTCUSDT", "timeframe": "1d", "direction": "long",
     "description": "日线 RSI<25 且价格低于 SMA50 时买入", "tags": ["RSI", "均值回归"]},
    {"name": "ETH MACD+KDJ 共振", "symbol": "ETHUSDT", "timeframe": "4h", "direction": "long_short",
     "description": "MACD 金叉 + KDJ 金叉同时触发时买入，双死叉卖出", "tags": ["MACD", "KDJ"]},
    {"name": "BNB ATR 波动跟踪", "symbol": "BNBUSDT", "timeframe": "4h", "direction": "long_short",
     "description": "ATR 放大时沿趋势方向入场，ATR 收窄时离场", "tags": ["ATR", "波动率"]},
    {"name": "DOGE 社媒情绪", "symbol": "DOGEUSDT", "timeframe": "1h", "direction": "long",
     "description": "Twitter 提及量暴增 + RSI 未超买时做多", "tags": ["社媒", "情绪"]},
    {"name": "BTC 周线趋势", "symbol": "BTCUSDT", "timeframe": "1d", "direction": "long_short",
     "description": "周线 SMA20 上方做多，下方做空，日线入场", "tags": ["趋势", "多周期"]},
    {"name": "ETH 资金费率套利", "symbol": "ETHUSDT", "timeframe": "1h", "direction": "long_short",
     "description": "资金费率极端正值时做空，极端负值时做多", "tags": ["资金费率", "套利"]},
    {"name": "SOL VWAP 回测", "symbol": "SOLUSDT", "timeframe": "1h", "direction": "long_short",
     "description": "价格从 VWAP 下方回到上方买入，从上方跌破卖出", "tags": ["VWAP", "日内"]},
    {"name": "AVAX 均线多头排列", "symbol": "AVAXUSDT", "timeframe": "4h", "direction": "long",
     "description": "EMA5>EMA10>EMA20>EMA60 多头排列时做多", "tags": ["EMA", "趋势"]},
    {"name": "BTC 期现价差", "symbol": "BTCUSDT", "timeframe": "4h", "direction": "long_short",
     "description": "永续溢价过高做空，贴水过深做多", "tags": ["期现", "套利"]},
]

CONCLUSIONS = ["approved", "paper_trade_first", "rejected"]


def _gen_metrics(bias: float = 0) -> dict:
    """生成随机但合理的回测指标。bias>0 偏向盈利。"""
    total_return = random.gauss(bias, 15)
    sharpe = total_return / 10 + random.gauss(0, 0.3)
    sortino = sharpe * random.uniform(1.1, 1.6)
    max_dd = -abs(random.gauss(8, 5))
    win_rate = max(0.2, min(0.75, 0.45 + total_return / 100 + random.gauss(0, 0.08)))
    total_trades = random.randint(15, 120)
    winning = int(total_trades * win_rate)
    losing = total_trades - winning
    calmar = total_return / abs(max_dd) if max_dd != 0 else 0

    return {
        "total_return_pct": round(total_return, 2),
        "annual_return_pct": round(total_return * random.uniform(0.8, 1.5), 2),
        "sharpe_ratio": round(sharpe, 3),
        "sortino_ratio": round(sortino, 3),
        "max_drawdown_pct": round(max_dd, 2),
        "calmar_ratio": round(calmar, 3),
        "win_rate": round(win_rate, 4),
        "profit_loss_ratio": round(random.uniform(1.0, 3.5), 2),
        "total_trades": total_trades,
        "winning_trades": winning,
        "losing_trades": losing,
        "avg_holding_bars": round(random.uniform(3, 30), 1),
        "total_commission": round(random.uniform(50, 500), 2),
        "total_slippage_cost": round(random.uniform(30, 300), 2),
        "net_funding": round(random.gauss(0, 50), 2),
        "liquidation_count": 0 if total_return > -20 else random.randint(1, 3),
        "final_balance": round(100000 * (1 + total_return / 100), 2),
        "peak_balance": round(100000 * (1 + abs(total_return) / 100 + random.uniform(0, 0.05)), 2),
        "total_signals": total_trades + random.randint(10, 50),
        "signals_executed": total_trades,
    }


def _conclusion_from_metrics(m: dict) -> str:
    ret = m.get("total_return_pct", 0)
    sharpe = m.get("sharpe_ratio", 0)
    dd = abs(m.get("max_drawdown_pct", 0))
    if ret > 10 and sharpe > 1.5 and dd < 10:
        return "approved"
    elif ret < -10 or m.get("liquidation_count", 0) > 0:
        return "rejected"
    else:
        return "paper_trade_first"


def _gen_equity(initial: float, final: float, points: int = 50) -> list:
    """生成简单的权益曲线。"""
    equity = [initial]
    for i in range(1, points):
        target = initial + (final - initial) * (i / points)
        noise = random.gauss(0, abs(final - initial) * 0.05)
        equity.append(round(target + noise, 2))
    equity.append(round(final, 2))
    return [{"datetime": f"2025-{1+i*11//points:02d}-{1+i%28:02d}", "balance": v} for i, v in enumerate(equity)]


def _gen_trades(metrics: dict) -> list:
    """生成模拟交易记录。"""
    trades = []
    balance = 100000
    for i in range(min(metrics["total_trades"], 30)):
        is_win = i < metrics["winning_trades"]
        pnl = random.uniform(100, 3000) if is_win else -random.uniform(50, 2000)
        balance += pnl
        trades.append({
            "trade_id": i + 1,
            "datetime": f"2025-{1+i%12:02d}-{1+i*2%28:02d} {8+i%12:02d}:00:00",
            "action": "close",
            "side": random.choice(["long", "short"]),
            "price": round(random.uniform(30000, 100000), 2),
            "quantity": round(random.uniform(0.01, 0.5), 4),
            "leverage": random.choice([1, 2, 3]),
            "fee": round(random.uniform(5, 50), 2),
            "pnl": round(pnl, 2),
            "pnl_pct": round(pnl / 100000 * 100, 2),
            "balance_after": round(balance, 2),
            "reason": "止盈" if is_win else "止损",
        })
    return trades


def seed():
    print("=== 生成演示用户 ===")
    demo_machines = []
    for i in range(5):
        mc = f"demo_{uuid.uuid4().hex[:8]}"
        tk = f"tok_{uuid.uuid4().hex[:16]}"
        mysql.upsert({
            "machine_code": mc, "token": tk,
            "max_strategies": random.choice([3, 5, 10]),
            "status": "active",
        }, "dex_machine_tokens")
        demo_machines.append(mc)
        print(f"  用户 {i+1}: {mc[:16]}...")

    print("\n=== 生成策略和回测 ===")
    for i, strat_def in enumerate(DEMO_STRATEGIES):
        sid = f"strat_{uuid.uuid4().hex[:12]}"
        mc = random.choice(demo_machines)

        mysql.upsert({
            "strategy_id": sid,
            "machine_code": mc,
            "name": strat_def["name"],
            "description": strat_def["description"],
            "symbol": strat_def["symbol"],
            "timeframe": strat_def["timeframe"],
            "direction": strat_def["direction"],
            "version": f"v1.{random.randint(0,5)}",
            "tags": json.dumps(strat_def["tags"], ensure_ascii=False),
            "status": random.choice(["active", "active", "draft"]),
        }, "dex_strategies")

        bias = random.gauss(5, 10)
        n_backtests = random.randint(1, 3)
        for j in range(n_backtests):
            bid = f"bt_{uuid.uuid4().hex[:12]}"
            metrics = _gen_metrics(bias=bias + random.gauss(0, 5))
            conclusion = _conclusion_from_metrics(metrics)
            equity = _gen_equity(100000, metrics["final_balance"])
            trades = _gen_trades(metrics)

            mysql.upsert({
                "backtest_id": bid,
                "strategy_id": sid,
                "strategy_name": strat_def["name"],
                "config_json": json.dumps({"symbol": strat_def["symbol"], "timeframe": strat_def["timeframe"],
                                           "leverage": random.choice([1, 2, 3]), "initial_capital": 100000}),
                "metrics_json": json.dumps(metrics),
                "trades_json": json.dumps(trades),
                "equity_json": json.dumps(equity),
                "conclusion": conclusion,
                "status": "completed",
                "elapsed_ms": random.randint(500, 15000),
            }, "dex_backtest_results")

        print(f"  #{i+1:2d} {strat_def['name']:<20s} | {strat_def['symbol']} | {n_backtests} 次回测 | 收益≈{bias:+.1f}%")

    print(f"\n=== 完成！共 {len(DEMO_STRATEGIES)} 个策略 + 5 个用户 ===")
    print("刷新排行榜和后台查看效果。")


if __name__ == "__main__":
    seed()
