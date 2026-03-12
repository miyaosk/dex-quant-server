"""
应用配置 — 通过环境变量或默认值加载
"""

from __future__ import annotations

import os
import json
from typing import Optional


# MySQL 配置（与其他项目保持一致的 dict 格式）
mysql_db = json.loads(os.getenv(
    "mysql_db",
    '{"host": "qa-dex-tidb.djdog.ai", "port": 4000, "user": "sol-chain", "password": "sol-chian", "db": "sol"}'
))

PROXY_URL: Optional[str] = os.getenv("PROXY_URL")
DEFAULT_FEE_RATE: float = float(os.getenv("DEFAULT_FEE_RATE", "0.0005"))
DEFAULT_SLIPPAGE_BPS: float = float(os.getenv("DEFAULT_SLIPPAGE_BPS", "2.0"))
MAX_BACKTEST_BARS: int = int(os.getenv("MAX_BACKTEST_BARS", "500000"))
API_PREFIX: str = os.getenv("API_PREFIX", "/api/v1")
