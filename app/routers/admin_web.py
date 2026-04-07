"""
后台管理 Web 页面 — 管理员登录、Dashboard、用户/监控/策略/回测管理
"""

from __future__ import annotations

import hmac
import secrets
from typing import Optional

from fastapi import APIRouter, Request, Form
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

import json
import random
import uuid

from app import config, database
from app.utils.mysql_client import mysql

router = APIRouter(prefix="/admin", tags=["Admin Web"])
templates = Jinja2Templates(directory="app/templates")

_SESSION_TOKENS: dict[str, bool] = {}


def _check_admin(request: Request) -> bool:
    token = request.cookies.get("admin_session")
    return bool(token and _SESSION_TOKENS.get(token))


@router.get("/login", response_class=HTMLResponse)
async def login_page(request: Request):
    if _check_admin(request):
        return RedirectResponse("/admin/", status_code=302)
    return templates.TemplateResponse(request, "admin/login.html")


@router.post("/login", response_class=HTMLResponse)
async def login_submit(request: Request, api_key: str = Form(...)):
    if not config.ADMIN_API_KEY:
        return templates.TemplateResponse(
            request, "admin/login.html",
            {"error": "服务器未配置 ADMIN_API_KEY"},
        )

    if not hmac.compare_digest(api_key, config.ADMIN_API_KEY):
        return templates.TemplateResponse(
            request, "admin/login.html",
            {"error": "API Key 错误"},
        )

    session_token = secrets.token_urlsafe(32)
    _SESSION_TOKENS[session_token] = True
    resp = RedirectResponse("/admin/", status_code=302)
    resp.set_cookie(
        "admin_session", session_token,
        httponly=True, secure=True, samesite="lax", max_age=86400,
    )
    return resp


@router.get("/logout")
async def logout(request: Request):
    token = request.cookies.get("admin_session")
    if token:
        _SESSION_TOKENS.pop(token, None)
    resp = RedirectResponse("/admin/login", status_code=302)
    resp.delete_cookie("admin_session")
    return resp


def _require_admin(request: Request) -> Optional[RedirectResponse]:
    if not _check_admin(request):
        return RedirectResponse("/admin/login", status_code=302)
    return None


@router.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    redirect = _require_admin(request)
    if redirect:
        return redirect
    stats = await database.admin_dashboard_stats()
    return templates.TemplateResponse(request, "admin/dashboard.html", {"stats": stats})


@router.get("/users", response_class=HTMLResponse)
async def users_page(request: Request):
    redirect = _require_admin(request)
    if redirect:
        return redirect
    users = await database.admin_list_users()
    return templates.TemplateResponse(request, "admin/users.html", {"users": users})


@router.get("/monitors", response_class=HTMLResponse)
async def monitors_page(request: Request):
    redirect = _require_admin(request)
    if redirect:
        return redirect
    monitors = await database.admin_list_all_monitors()
    return templates.TemplateResponse(request, "admin/monitors.html", {"monitors": monitors})


@router.get("/strategies", response_class=HTMLResponse)
async def strategies_page(request: Request):
    redirect = _require_admin(request)
    if redirect:
        return redirect
    strategies = await database.admin_list_all_strategies()
    return templates.TemplateResponse(request, "admin/strategies.html", {"strategies": strategies})


@router.get("/backtests", response_class=HTMLResponse)
async def backtests_page(request: Request):
    redirect = _require_admin(request)
    if redirect:
        return redirect
    backtests = await database.admin_list_all_backtests()
    return templates.TemplateResponse(request, "admin/backtests.html", {"backtests": backtests})


@router.post("/seed-demo")
async def seed_demo(request: Request):
    """生成演示数据，让排行榜和后台有内容。"""
    redirect = _require_admin(request)
    if redirect:
        return redirect

    import asyncio

    strategies = [
        {"name": "BTC EMA 双线交叉", "symbol": "BTCUSDT", "tf": "4h", "dir": "long_short", "desc": "EMA12/26 交叉 + 成交量过滤"},
        {"name": "ETH 布林带回归", "symbol": "ETHUSDT", "tf": "1h", "dir": "long_short", "desc": "布林带下轨+RSI超卖买入"},
        {"name": "SOL 动量突破", "symbol": "SOLUSDT", "tf": "2h", "dir": "long", "desc": "价格突破20日高点+放量做多"},
        {"name": "BTC RSI 超卖抄底", "symbol": "BTCUSDT", "tf": "1d", "dir": "long", "desc": "日线RSI<25且价格低于SMA50"},
        {"name": "ETH MACD+KDJ 共振", "symbol": "ETHUSDT", "tf": "4h", "dir": "long_short", "desc": "MACD金叉+KDJ金叉双共振"},
        {"name": "BNB ATR 波动跟踪", "symbol": "BNBUSDT", "tf": "4h", "dir": "long_short", "desc": "ATR放大沿趋势入场"},
        {"name": "DOGE 社媒情绪", "symbol": "DOGEUSDT", "tf": "1h", "dir": "long", "desc": "Twitter提及量暴增+RSI未超买"},
        {"name": "BTC 周线趋势", "symbol": "BTCUSDT", "tf": "1d", "dir": "long_short", "desc": "周线SMA20上方做多下方做空"},
        {"name": "ETH 资金费率套利", "symbol": "ETHUSDT", "tf": "1h", "dir": "long_short", "desc": "资金费率极端时反向操作"},
        {"name": "SOL VWAP 回测", "symbol": "SOLUSDT", "tf": "1h", "dir": "long_short", "desc": "价格与VWAP交叉入场"},
        {"name": "AVAX 均线多头排列", "symbol": "AVAXUSDT", "tf": "4h", "dir": "long", "desc": "EMA5>10>20>60多头排列做多"},
        {"name": "BTC 期现价差", "symbol": "BTCUSDT", "tf": "4h", "dir": "long_short", "desc": "永续溢价高做空贴水深做多"},
    ]

    def _seed():
        machines = []
        for i in range(5):
            mc = f"demo_{uuid.uuid4().hex[:8]}"
            tk = f"tok_{uuid.uuid4().hex[:16]}"
            mysql.upsert({"machine_code": mc, "token": tk, "max_strategies": random.choice([3, 5, 10]), "status": "active"}, "dex_machine_tokens")
            machines.append(mc)

        count = 0
        for s in strategies:
            sid = f"strat_{uuid.uuid4().hex[:12]}"
            mysql.upsert({
                "strategy_id": sid, "machine_code": random.choice(machines),
                "name": s["name"], "description": s["desc"], "symbol": s["symbol"],
                "timeframe": s["tf"], "direction": s["dir"], "version": f"v1.{random.randint(0,5)}",
                "tags": json.dumps(["demo"], ensure_ascii=False), "status": "active",
            }, "dex_strategies")

            bias = random.gauss(5, 10)
            for _ in range(random.randint(1, 2)):
                bid = f"bt_{uuid.uuid4().hex[:12]}"
                ret = round(random.gauss(bias, 12), 2)
                sharpe = round(ret / 10 + random.gauss(0, 0.3), 3)
                dd = round(-abs(random.gauss(8, 5)), 2)
                wr = round(max(0.2, min(0.75, 0.45 + ret / 100)), 4)
                trades_n = random.randint(15, 80)
                final_bal = round(100000 * (1 + ret / 100), 2)

                metrics = {
                    "total_return_pct": ret, "annual_return_pct": round(ret * 1.2, 2),
                    "sharpe_ratio": sharpe, "sortino_ratio": round(sharpe * 1.3, 3),
                    "max_drawdown_pct": dd, "calmar_ratio": round(ret / abs(dd) if dd else 0, 3),
                    "win_rate": wr, "profit_loss_ratio": round(random.uniform(1.0, 3.0), 2),
                    "total_trades": trades_n, "winning_trades": int(trades_n * wr),
                    "losing_trades": trades_n - int(trades_n * wr),
                    "avg_holding_bars": round(random.uniform(3, 25), 1),
                    "total_commission": round(random.uniform(50, 400), 2),
                    "total_slippage_cost": round(random.uniform(30, 200), 2),
                    "net_funding": 0, "liquidation_count": 0,
                    "final_balance": final_bal, "peak_balance": round(final_bal * 1.02, 2),
                    "total_signals": trades_n + random.randint(10, 40),
                    "signals_executed": trades_n,
                }

                concl = "approved" if ret > 10 and sharpe > 1.5 else ("rejected" if ret < -10 else "paper_trade_first")
                equity = [{"datetime": f"2025-{1+i*11//50:02d}-{1+i%28:02d}", "balance": round(100000 + (final_bal - 100000) * i / 50 + random.gauss(0, abs(ret) * 20), 2)} for i in range(51)]

                mysql.upsert({
                    "backtest_id": bid, "strategy_id": sid, "strategy_name": s["name"],
                    "config_json": json.dumps({"symbol": s["symbol"], "timeframe": s["tf"], "leverage": 2}),
                    "metrics_json": json.dumps(metrics), "equity_json": json.dumps(equity),
                    "trades_json": json.dumps([]), "conclusion": concl,
                    "status": "completed", "elapsed_ms": random.randint(500, 10000),
                }, "dex_backtest_results")
                count += 1

        return {"users": 5, "strategies": len(strategies), "backtests": count}

    result = await asyncio.to_thread(_seed)
    return RedirectResponse("/admin/", status_code=302)
