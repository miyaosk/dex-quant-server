"""
前台 Web 页面 — 排行榜、策略详情、实时监控、平台介绍
"""

from __future__ import annotations

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from app import database

router = APIRouter(tags=["Web"])
templates = Jinja2Templates(directory="app/templates")


@router.get("/", response_class=HTMLResponse)
async def leaderboard(request: Request, sort: str = "total_return_pct"):
    strategies = await database.leaderboard_strategies(sort_by=sort, limit=50)
    stats = await database.public_stats()
    return templates.TemplateResponse(
        request, "public/leaderboard.html",
        {"strategies": strategies, "current_sort": sort, "stats": stats},
    )


@router.get("/strategy/{strategy_id}", response_class=HTMLResponse)
async def strategy_detail(request: Request, strategy_id: str):
    detail = await database.get_strategy_detail_with_backtest(strategy_id)
    if not detail:
        return templates.TemplateResponse(
            request, "public/404.html",
            {"message": "策略不存在"},
            status_code=404,
        )
    return templates.TemplateResponse(
        request, "public/strategy_detail.html",
        {"strategy": detail},
    )


@router.get("/monitors", response_class=HTMLResponse)
async def monitors_page(request: Request):
    monitors = await database.public_list_monitors(limit=50)
    monitor_stats = await database.public_monitor_stats()
    return templates.TemplateResponse(
        request, "public/monitors.html",
        {"monitors": monitors, "stats": monitor_stats},
    )


@router.get("/about", response_class=HTMLResponse)
async def about_page(request: Request):
    stats = await database.public_stats()
    return templates.TemplateResponse(
        request, "public/about.html",
        {"stats": stats},
    )
