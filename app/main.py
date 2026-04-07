"""
DEX Quant Server — FastAPI 应用入口

信号驱动架构：
  Skill 生成策略脚本 → 跑脚本产出信号 → 发信号到 Server → Server 拉 K 线 + 回测
"""

from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.openapi.docs import get_redoc_html, get_swagger_ui_html
from fastapi.responses import JSONResponse
from loguru import logger

from app import database
from app import config
from app.routers import auth, data, strategy, backtest, signal, monitor, vault
from app.routers import web, admin_web


@asynccontextmanager
async def lifespan(app: FastAPI):
    await database.init_db()

    from app.routers.monitor import restore_running_monitors, start_daily_report_scheduler
    await restore_running_monitors()
    start_daily_report_scheduler()

    import resource, os
    ru = resource.getrusage(resource.RUSAGE_SELF)
    rss = ru.ru_maxrss / 1024 if os.uname().sysname != "Darwin" else ru.ru_maxrss / (1024 * 1024)
    logger.info(f"启动完成 | 基线内存={rss:.0f}MB | pid={os.getpid()}")
    yield
    logger.info("服务关闭")


app = FastAPI(
    title="DEX Quant Server",
    description="信号驱动回测服务 — 接收策略信号，拉取 K 线（带缓存），执行回测",
    version="2.1.0",
    lifespan=lifespan,
    redoc_url=None,
    docs_url=None,
)


def _is_admin(request: Request) -> bool:
    token = request.cookies.get("admin_session")
    return bool(token and admin_web._SESSION_TOKENS.get(token))


@app.get("/docs", include_in_schema=False)
async def custom_swagger_ui(request: Request):
    if not _is_admin(request):
        return JSONResponse({"detail": "Not Found"}, status_code=404)
    return get_swagger_ui_html(
        openapi_url=app.openapi_url,
        title=app.title + " - Swagger UI",
        swagger_js_url="https://unpkg.com/swagger-ui-dist@5/swagger-ui-bundle.js",
        swagger_css_url="https://unpkg.com/swagger-ui-dist@5/swagger-ui.css",
    )


@app.get("/redoc", include_in_schema=False)
async def custom_redoc(request: Request):
    if not _is_admin(request):
        return JSONResponse({"detail": "Not Found"}, status_code=404)
    return get_redoc_html(
        openapi_url=app.openapi_url,
        title=app.title + " - ReDoc",
        redoc_js_url="https://unpkg.com/redoc@2.1.5/bundles/redoc.standalone.js",
    )


@app.get("/openapi.json", include_in_schema=False)
async def custom_openapi(request: Request):
    if not _is_admin(request):
        return JSONResponse({"detail": "Not Found"}, status_code=404)
    return app.openapi()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(auth.router, prefix=config.API_PREFIX)
app.include_router(data.router, prefix=config.API_PREFIX)
app.include_router(strategy.router, prefix=config.API_PREFIX)
app.include_router(backtest.router, prefix=config.API_PREFIX)
app.include_router(signal.router, prefix=config.API_PREFIX)
app.include_router(monitor.router, prefix=config.API_PREFIX)
app.include_router(vault.router, prefix=config.API_PREFIX)

app.include_router(web.router)
app.include_router(admin_web.router)


@app.get("/health")
async def health():
    return {"status": "ok", "version": "2.1.0"}
