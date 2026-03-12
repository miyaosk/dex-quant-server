"""
DEX Quant Server — FastAPI 应用入口
"""

from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.openapi.docs import get_redoc_html, get_swagger_ui_html
from loguru import logger

from app import database
from app import config
from app.routers import data, strategy, backtest, signal


@asynccontextmanager
async def lifespan(app: FastAPI):
    await database.init_db()
    yield
    logger.info("服务关闭")


app = FastAPI(
    title="DEX Quant Server",
    description="量化回测服务 — 策略管理、数据获取、回测执行",
    version="1.0.0",
    lifespan=lifespan,
    redoc_url=None,
    docs_url=None,
)


@app.get("/docs", include_in_schema=False)
async def custom_swagger_ui():
    return get_swagger_ui_html(
        openapi_url=app.openapi_url,
        title=app.title + " - Swagger UI",
        swagger_js_url="https://unpkg.com/swagger-ui-dist@5/swagger-ui-bundle.js",
        swagger_css_url="https://unpkg.com/swagger-ui-dist@5/swagger-ui.css",
    )


@app.get("/redoc", include_in_schema=False)
async def custom_redoc():
    return get_redoc_html(
        openapi_url=app.openapi_url,
        title=app.title + " - ReDoc",
        redoc_js_url="https://unpkg.com/redoc@2.1.5/bundles/redoc.standalone.js",
    )

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(data.router, prefix=config.API_PREFIX)
app.include_router(strategy.router, prefix=config.API_PREFIX)
app.include_router(backtest.router, prefix=config.API_PREFIX)
app.include_router(signal.router, prefix=config.API_PREFIX)


@app.get("/health")
async def health():
    return {"status": "ok", "version": "1.0.0"}
