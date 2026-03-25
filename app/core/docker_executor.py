"""
Docker 沙箱执行器 — 在隔离容器中运行策略脚本

安全措施:
  1. --network=none     完全断网
  2. --memory=512m      内存硬限
  3. --cpus=1           CPU 限制
  4. --read-only        只读文件系统
  5. --user=65534       非 root 运行
  6. --cap-drop=ALL     移除所有 Linux Capabilities
  7. --security-opt=no-new-privileges  禁止提权
  8. 超时强制 kill

数据流:
  主服务预拉 K 线 → stdin JSON → 容器执行 → stdout JSON → 主服务解析
"""

from __future__ import annotations

import asyncio
import json
import os
from typing import Optional

from loguru import logger

SANDBOX_IMAGE = os.getenv("SANDBOX_IMAGE", "dex-sandbox:latest")
SANDBOX_TIMEOUT = int(os.getenv("SANDBOX_TIMEOUT", "120"))
SANDBOX_MEMORY = os.getenv("SANDBOX_MEMORY", "512m")
SANDBOX_CPUS = os.getenv("SANDBOX_CPUS", "1")


async def execute_in_docker(
    script_content: str,
    mode: str = "backtest",
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    kline_data: Optional[dict] = None,
    params: Optional[dict] = None,
) -> dict:
    """
    在 Docker 容器中执行策略脚本。

    参数:
        script_content: 策略 Python 源码
        mode: 执行模式 (backtest/live)
        start_date: 开始日期
        end_date: 结束日期
        kline_data: 预拉取的 K 线数据 {"BTCUSDT:4h": [records...]}
        params: 优化参数 (可选)

    返回:
        {"signals": [...], "strategy_name": "..."}
    """
    payload = {
        "script_content": script_content,
        "mode": mode,
        "start_date": start_date,
        "end_date": end_date,
        "klines": kline_data or {},
    }
    if params:
        payload["params"] = params

    payload_json = json.dumps(payload, default=str, ensure_ascii=False)

    cmd = [
        "docker", "run",
        "--rm",
        "-i",
        "--network=none",
        f"--memory={SANDBOX_MEMORY}",
        f"--cpus={SANDBOX_CPUS}",
        "--read-only",
        "--tmpfs", "/tmp:rw,noexec,size=64m",
        "--user=65534",
        "--cap-drop=ALL",
        "--security-opt=no-new-privileges",
        "--pids-limit=64",
        "--log-driver=none",
        SANDBOX_IMAGE,
    ]

    logger.info(
        "Docker 沙箱启动 | image={} mem={} cpu={} timeout={}s",
        SANDBOX_IMAGE, SANDBOX_MEMORY, SANDBOX_CPUS, SANDBOX_TIMEOUT,
    )

    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )

        stdout, stderr = await asyncio.wait_for(
            proc.communicate(input=payload_json.encode("utf-8")),
            timeout=SANDBOX_TIMEOUT,
        )

        exit_code = proc.returncode
        stdout_str = stdout.decode("utf-8", errors="replace").strip()
        stderr_str = stderr.decode("utf-8", errors="replace").strip()

        if exit_code == 137:
            raise MemoryError("容器被 OOM Killer 终止（内存超限）")

        if exit_code != 0:
            error_msg = stderr_str or stdout_str or f"容器退出码 {exit_code}"
            raise RuntimeError(f"沙箱执行失败: {error_msg}")

        if not stdout_str:
            raise RuntimeError("沙箱未返回任何输出")

        result = json.loads(stdout_str)

        if "error" in result:
            raise RuntimeError(f"策略脚本错误: {result['error']}")

        logger.info(
            "Docker 沙箱完成 | 信号={} | exit={}",
            len(result.get("signals", [])), exit_code,
        )
        return result

    except asyncio.TimeoutError:
        logger.error("Docker 沙箱超时 ({}s)，强制终止", SANDBOX_TIMEOUT)
        try:
            proc.kill()
            await proc.wait()
        except Exception:
            pass
        raise TimeoutError(f"沙箱执行超时（{SANDBOX_TIMEOUT}秒）")

    except json.JSONDecodeError as e:
        raise RuntimeError(f"沙箱输出不是有效 JSON: {e}\n输出: {stdout_str[:500]}")


async def prefetch_klines(
    symbol: str,
    timeframe: str,
    start_date: str,
    end_date: str,
) -> dict:
    """
    预拉取 K 线数据用于注入容器。

    返回 {"BTCUSDT:4h": [records...]} 格式，
    DataFrame 转为 JSON-serializable 的 records 列表。
    """
    from app.core.data_client import DataClient

    dc = DataClient()
    try:
        bn_symbol = symbol.upper().replace("-PERP", "").replace("-SPOT", "").replace("-", "")
        df = dc.get_perp_klines(
            symbol=symbol,
            interval=timeframe,
            start_date=start_date,
            end_date=end_date,
        )

        if df.empty:
            logger.warning("预拉取 K 线为空 | {} {} {} → {}", symbol, timeframe, start_date, end_date)
            return {}

        records = json.loads(df.to_json(orient="records", date_format="iso"))

        key = f"{bn_symbol}:{timeframe}"
        logger.info("预拉取 K 线 | {} | {} 条", key, len(records))
        return {key: records}

    finally:
        dc.close()


async def check_docker_available() -> bool:
    """检查 Docker 是否可用。"""
    try:
        proc = await asyncio.create_subprocess_exec(
            "docker", "info",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        await asyncio.wait_for(proc.communicate(), timeout=5)
        return proc.returncode == 0
    except Exception:
        return False


async def check_sandbox_image() -> bool:
    """检查沙箱镜像是否已构建。"""
    try:
        proc = await asyncio.create_subprocess_exec(
            "docker", "image", "inspect", SANDBOX_IMAGE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        await asyncio.wait_for(proc.communicate(), timeout=5)
        return proc.returncode == 0
    except Exception:
        return False
