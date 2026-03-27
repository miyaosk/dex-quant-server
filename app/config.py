"""
应用配置 — 通过环境变量加载，禁止硬编码密码

必需环境变量:
  MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DB
  或旧格式: mysql_db (JSON 字符串)
"""

from __future__ import annotations

import os
import json
from typing import Optional


def _load_mysql_config() -> dict:
    """优先读独立环境变量，兼容旧 JSON 格式的 mysql_db。"""
    raw = os.getenv("mysql_db", "")
    if raw:
        return json.loads(raw)

    host = os.getenv("MYSQL_HOST", "")
    if not host:
        raise RuntimeError(
            "MySQL 未配置。请设置环境变量: MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DB "
            "或 mysql_db (JSON 字符串)"
        )
    return {
        "host": host,
        "port": int(os.getenv("MYSQL_PORT", "3306")),
        "user": os.getenv("MYSQL_USER", "root"),
        "password": os.getenv("MYSQL_PASSWORD", ""),
        "db": os.getenv("MYSQL_DB", "dex_quant"),
    }


mysql_db = _load_mysql_config()

PROXY_URL: Optional[str] = os.getenv("PROXY_URL")
DEFAULT_FEE_RATE: float = float(os.getenv("DEFAULT_FEE_RATE", "0.0005"))
DEFAULT_SLIPPAGE_BPS: float = float(os.getenv("DEFAULT_SLIPPAGE_BPS", "2.0"))
MAX_BACKTEST_BARS: int = int(os.getenv("MAX_BACKTEST_BARS", "500000"))
API_PREFIX: str = os.getenv("API_PREFIX", "/api/v1")

# docker  = Docker 容器隔离（断网 + 内存限 + 非root），默认推荐
# process = 进程内沙箱（AST 扫描 + 受限 builtins），用于无 Docker 环境（如 Railway PaaS）
SANDBOX_MODE: str = os.getenv("SANDBOX_MODE", "docker")

ADMIN_API_KEY: str = os.getenv("ADMIN_API_KEY", "")
