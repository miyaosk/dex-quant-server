"""
策略脚本执行器 — 在沙箱中安全运行用户上传的策略脚本

安全层级:
  1. AST 预扫描 — 拒绝包含危险 import / 属性访问的脚本
  2. 受限 builtins — 移除 open / exec / eval / compile 等
  3. 模块白名单 — 自定义 __import__ 只放行安全模块
  4. sys.modules 注入 — 让脚本的 import data_client 拿到受控实例
  5. 执行超时 — 由调用方通过 asyncio.wait_for 控制
"""

from __future__ import annotations

import ast
import builtins
import sys
import types
from typing import Optional

from loguru import logger


# ── 白名单 ──────────────────────────────────────────────────

ALLOWED_MODULES: frozenset[str] = frozenset({
    # 注入的受控模块
    "data_client",
    "indicators",
    "strategy_runner",
    # 安全标准库
    "sys", "os.path",
    "math", "cmath", "decimal", "fractions", "statistics",
    "datetime", "time", "calendar", "zoneinfo",
    "collections", "itertools", "functools", "operator",
    "copy", "json", "re", "enum", "dataclasses",
    "typing", "abc", "numbers", "string", "textwrap",
    "bisect", "heapq", "random", "hashlib", "hmac",
    "uuid", "pprint",
    # 常用数据分析库（只读计算）
    "numpy", "pandas", "ta", "talib",
})

FORBIDDEN_ATTRS: frozenset[str] = frozenset({
    "__subclasses__", "__bases__", "__mro__",
    "__globals__", "__code__", "__builtins__",
    "__loader__", "__spec__",
})

DANGEROUS_BUILTINS: frozenset[str] = frozenset({
    "open", "exec", "eval", "compile",
    "__import__",
    "globals", "locals", "vars",
    "getattr", "setattr", "delattr",
    "breakpoint", "exit", "quit", "input", "help",
    "memoryview",
})


class ScriptSecurityError(Exception):
    """脚本安全检查未通过"""


# ── AST 预扫描 ──────────────────────────────────────────────

def _audit_ast(source: str) -> None:
    """解析脚本 AST，拒绝危险模式"""
    try:
        tree = ast.parse(source, filename="<strategy>")
    except SyntaxError as e:
        raise ScriptSecurityError(f"脚本语法错误: {e}") from e

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                _check_module(alias.name, node.lineno)
        elif isinstance(node, ast.ImportFrom):
            if node.module:
                _check_module(node.module, node.lineno)
        elif isinstance(node, ast.Attribute):
            if node.attr in FORBIDDEN_ATTRS:
                raise ScriptSecurityError(
                    f"第 {node.lineno} 行: 禁止访问属性 '{node.attr}'"
                )


def _check_module(module: str, lineno: int) -> None:
    top_level = module.split(".")[0]
    if top_level not in ALLOWED_MODULES:
        raise ScriptSecurityError(
            f"第 {lineno} 行: 禁止导入模块 '{module}'"
        )


# ── 受限 builtins ──────────────────────────────────────────

_original_import = builtins.__import__


def _safe_import(name, globals=None, locals=None, fromlist=(), level=0):
    """只允许白名单模块的 import"""
    if level != 0:
        raise ImportError("沙箱中禁止相对导入")
    top_level = name.split(".")[0]
    if top_level not in ALLOWED_MODULES:
        raise ImportError(f"沙箱禁止导入模块 '{name}'")
    return _original_import(name, globals, locals, fromlist, level)


def _make_safe_builtins() -> dict:
    """构造受限的 __builtins__ 字典"""
    safe = {}
    for name in dir(builtins):
        if name.startswith("_") and name != "__name__":
            continue
        if name in DANGEROUS_BUILTINS:
            continue
        safe[name] = getattr(builtins, name)
    safe["__import__"] = _safe_import
    safe["__name__"] = "__script__"
    safe["__build_class__"] = builtins.__build_class__
    return safe


# ── 主入口 ──────────────────────────────────────────────────

def execute_strategy(
    script_content: str,
    mode: str = "backtest",
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> dict:
    """
    在沙箱中执行策略脚本的 generate_signals()。

    安全流程:
      1. AST 扫描 → 拒绝危险 import / 属性访问
      2. 构造受限 builtins + 白名单 __import__
      3. 注入 data_client / indicators / strategy_runner
      4. exec() 在隔离 namespace 中执行

    超时保护由调用方通过 asyncio.wait_for 控制。
    """
    # 自动剥离 sys.path.insert 行（本地脚本用于导入，服务器端已注入模块不需要）
    import re as _re
    script_content = _re.sub(
        r'^.*sys\.path\.insert.*$', '', script_content, flags=_re.MULTILINE
    )

    _audit_ast(script_content)

    from app.core import data_client as server_dc
    from app.core import indicators as server_ind

    fake_data_client = types.ModuleType("data_client")
    fake_data_client.DataClient = server_dc.DataClient

    fake_indicators = types.ModuleType("indicators")
    fake_indicators.Indicators = server_ind.Indicators

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
        safe_builtins = _make_safe_builtins()
        namespace = {
            "__name__": "__script__",
            "__file__": "<uploaded>",
            "__builtins__": safe_builtins,
        }

        code = compile(script_content, "<strategy>", "exec")
        exec(code, namespace)

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
