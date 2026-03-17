"""
策略脚本执行器 — 在服务器端安全运行用户上传的策略脚本

流程:
  1. 接收脚本源码（字符串）
  2. 注入服务器端的 data_client / indicators 模块
  3. exec() 执行脚本，调用 generate_signals()
  4. 返回信号列表

安全措施:
  - 通过 sys.modules 注入受控模块，脚本的 import 只能拿到我们提供的
  - 执行完毕后恢复 sys.modules
  - 超时控制由调用方负责
"""

from __future__ import annotations

import sys
import types
from typing import Optional

from loguru import logger


def execute_strategy(
    script_content: str,
    mode: str = "backtest",
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> dict:
    """
    执行策略脚本的 generate_signals() 函数。

    参数:
        script_content: 策略脚本 Python 源码
        mode: "backtest" 或 "live"
        start_date: 回测起始日期
        end_date: 回测结束日期

    返回:
        generate_signals() 的返回值（含 signals 列表）

    异常:
        ValueError: 脚本中没有 generate_signals 函数
        Exception: 脚本执行错误
    """
    from app.core import data_client as server_dc
    from app.core import indicators as server_ind

    fake_data_client = types.ModuleType("data_client")
    fake_data_client.DataClient = server_dc.DataClient

    fake_indicators = types.ModuleType("indicators")
    fake_indicators.Indicators = server_ind.Indicators

    # 注入 strategy_runner 的空壳，防止 __main__ 里的 import 报错
    fake_runner = types.ModuleType("strategy_runner")
    fake_runner.run = lambda *a, **kw: None

    saved = {}
    inject_names = ["data_client", "indicators", "strategy_runner"]
    for name in inject_names:
        saved[name] = sys.modules.get(name)
        sys.modules[name] = {
            "data_client": fake_data_client,
            "indicators": fake_indicators,
            "strategy_runner": fake_runner,
        }[name]

    try:
        namespace = {"__name__": "__script__", "__file__": "<uploaded>"}
        exec(compile(script_content, "<strategy>", "exec"), namespace)

        generate_fn = namespace.get("generate_signals")
        if generate_fn is None:
            raise ValueError(
                "脚本中未找到 generate_signals() 函数，"
                "请确保脚本定义了 def generate_signals(mode, start_date, end_date)"
            )

        logger.info("服务器执行策略脚本 | mode={} {} → {}", mode, start_date, end_date)
        result = generate_fn(mode=mode, start_date=start_date, end_date=end_date)

        signals = result.get("signals", [])
        logger.info("脚本执行完成 | 产出 {} 个信号", len(signals))

        return result

    finally:
        for name in inject_names:
            if saved[name] is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = saved[name]
