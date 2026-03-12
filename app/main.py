"""
DEX Quant Server — FastAPI 应用入口
"""

from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from loguru import logger

from app import database
from app.config import settings
from app.routers import data, strategy, backtest


@asynccontextmanager
async def lifespan(app: FastAPI):
    await database.init_db()
    logger.info("数据库初始化完成")
    yield
    logger.info("服务关闭")


app = FastAPI(
    title="DEX Quant Server",
    description="量化回测服务 — 策略管理、数据获取、回测执行",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(data.router, prefix=settings.API_PREFIX)
app.include_router(strategy.router, prefix=settings.API_PREFIX)
app.include_router(backtest.router, prefix=settings.API_PREFIX)


@app.get("/health")
async def health():
    return {"status": "ok", "version": "1.0.0"}
