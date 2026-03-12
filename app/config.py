"""
应用配置 — 通过环境变量或默认值加载
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class Settings:
    """全局配置，优先读取环境变量。"""

    DATABASE_URL: str = field(
        default_factory=lambda: os.getenv("DATABASE_URL", "sqlite:///data/quant.db")
    )
    DATA_DIR: str = field(
        default_factory=lambda: os.getenv("DATA_DIR", "data")
    )
    PROXY_URL: Optional[str] = field(
        default_factory=lambda: os.getenv("PROXY_URL")
    )
    DEFAULT_FEE_RATE: float = field(
        default_factory=lambda: float(os.getenv("DEFAULT_FEE_RATE", "0.0005"))
    )
    DEFAULT_SLIPPAGE_BPS: float = field(
        default_factory=lambda: float(os.getenv("DEFAULT_SLIPPAGE_BPS", "2.0"))
    )
    MAX_BACKTEST_BARS: int = field(
        default_factory=lambda: int(os.getenv("MAX_BACKTEST_BARS", "500000"))
    )
    API_PREFIX: str = field(
        default_factory=lambda: os.getenv("API_PREFIX", "/api/v1")
    )


settings = Settings()
