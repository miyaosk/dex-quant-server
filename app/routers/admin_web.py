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

from app import config, database

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
    return templates.TemplateResponse("admin/login.html", {"request": request})


@router.post("/login", response_class=HTMLResponse)
async def login_submit(request: Request, api_key: str = Form(...)):
    if not config.ADMIN_API_KEY:
        return templates.TemplateResponse("admin/login.html", {
            "request": request,
            "error": "服务器未配置 ADMIN_API_KEY",
        })

    if not hmac.compare_digest(api_key, config.ADMIN_API_KEY):
        return templates.TemplateResponse("admin/login.html", {
            "request": request,
            "error": "API Key 错误",
        })

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
    return templates.TemplateResponse("admin/dashboard.html", {
        "request": request,
        "stats": stats,
    })


@router.get("/users", response_class=HTMLResponse)
async def users_page(request: Request):
    redirect = _require_admin(request)
    if redirect:
        return redirect
    users = await database.admin_list_users()
    return templates.TemplateResponse("admin/users.html", {
        "request": request,
        "users": users,
    })


@router.get("/monitors", response_class=HTMLResponse)
async def monitors_page(request: Request):
    redirect = _require_admin(request)
    if redirect:
        return redirect
    monitors = await database.admin_list_all_monitors()
    return templates.TemplateResponse("admin/monitors.html", {
        "request": request,
        "monitors": monitors,
    })


@router.get("/strategies", response_class=HTMLResponse)
async def strategies_page(request: Request):
    redirect = _require_admin(request)
    if redirect:
        return redirect
    strategies = await database.admin_list_all_strategies()
    return templates.TemplateResponse("admin/strategies.html", {
        "request": request,
        "strategies": strategies,
    })


@router.get("/backtests", response_class=HTMLResponse)
async def backtests_page(request: Request):
    redirect = _require_admin(request)
    if redirect:
        return redirect
    backtests = await database.admin_list_all_backtests()
    return templates.TemplateResponse("admin/backtests.html", {
        "request": request,
        "backtests": backtests,
    })
