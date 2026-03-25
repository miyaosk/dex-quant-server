"""
沙箱容器执行入口

从 stdin 读取 JSON payload:
  {
    "script_content": "策略脚本源码",
    "mode": "backtest",
    "start_date": "2024-01-01",
    "end_date": "2024-12-31",
    "klines": {"BTCUSDT:4h": [{"datetime":..,"open":..,"high":..},...]}
    "params": {"fast_ema": 9}  // 可选，优化参数
  }

执行 generate_signals()，将结果 JSON 写到 stdout。
所有错误通过 JSON {"error": "..."} 返回。
"""

import ast
import builtins
import json
import sys
import types

import numpy as np
import pandas as pd

# ── 安全配置 ──────────────────────────────────────

ALLOWED_MODULES = frozenset({
    "data_client", "indicators", "strategy_runner",
    "sys", "os.path",
    "math", "cmath", "decimal", "fractions", "statistics",
    "datetime", "time", "calendar", "zoneinfo",
    "collections", "itertools", "functools", "operator",
    "copy", "json", "re", "enum", "dataclasses",
    "typing", "abc", "numbers", "string", "textwrap",
    "bisect", "heapq", "random", "hashlib", "hmac",
    "uuid", "pprint",
    "numpy", "pandas", "ta", "talib",
})

FORBIDDEN_ATTRS = frozenset({
    "__subclasses__", "__bases__", "__mro__",
    "__globals__", "__code__", "__builtins__",
    "__loader__", "__spec__",
})

DANGEROUS_BUILTINS = frozenset({
    "open", "exec", "eval", "compile", "__import__",
    "globals", "locals", "vars",
    "getattr", "setattr", "delattr",
    "breakpoint", "exit", "quit", "input", "help",
    "memoryview",
})


def _audit_ast(source: str) -> None:
    try:
        tree = ast.parse(source, filename="<strategy>")
    except SyntaxError as e:
        raise RuntimeError(f"脚本语法错误: {e}") from e

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                _check_module(alias.name, node.lineno)
        elif isinstance(node, ast.ImportFrom):
            if node.module:
                _check_module(node.module, node.lineno)
        elif isinstance(node, ast.Attribute):
            if node.attr in FORBIDDEN_ATTRS:
                raise RuntimeError(f"第 {node.lineno} 行: 禁止访问属性 '{node.attr}'")


def _check_module(module: str, lineno: int) -> None:
    top_level = module.split(".")[0]
    if top_level not in ALLOWED_MODULES:
        raise RuntimeError(f"第 {lineno} 行: 禁止导入模块 '{module}'")


_original_import = builtins.__import__


def _safe_import(name, globals=None, locals=None, fromlist=(), level=0):
    if level != 0:
        raise ImportError("沙箱中禁止相对导入")
    top_level = name.split(".")[0]
    if top_level not in ALLOWED_MODULES:
        raise ImportError(f"沙箱禁止导入模块 '{name}'")
    return _original_import(name, globals, locals, fromlist, level)


def _make_safe_builtins() -> dict:
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


# ── 预加载数据的 DataClient 代理 ──────────────────

class SandboxDataClient:
    """返回主服务预注入的 K 线数据，不做任何网络请求。"""

    def __init__(self, kline_cache: dict[str, list]):
        self._cache = kline_cache

    def _resolve(self, symbol, interval, **kw):
        interval = kw.get("timeframe") or kw.get("period") or interval
        symbol = symbol.upper().replace("-PERP", "").replace("-SPOT", "").replace("-", "")
        key = f"{symbol}:{interval}"
        if key not in self._cache:
            avail = ", ".join(self._cache.keys()) or "(空)"
            raise RuntimeError(
                f"沙箱未预加载 {key} 的数据。可用: {avail}。"
                f"请确保回测请求的 symbol/timeframe 与策略脚本一致。"
            )
        records = self._cache[key]
        df = pd.DataFrame(records)
        if "datetime" in df.columns:
            df["datetime"] = pd.to_datetime(df["datetime"], utc=True)
        return df

    def get_perp_klines(self, symbol, interval="1d", start_date=None, end_date=None, **kw):
        return self._resolve(symbol, interval, **kw)

    get_ohlcv = get_perp_klines
    get_klines = get_perp_klines
    get_candles = get_perp_klines
    fetch_ohlcv = get_perp_klines

    def get_spot_klines(self, symbol, interval="1d", start_date=None, end_date=None, **kw):
        return self._resolve(symbol, interval, **kw)

    def close(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *a):
        pass


# ── 主入口 ──────────────────────────────────────

def main():
    try:
        raw = sys.stdin.read()
        payload = json.loads(raw)

        script_content = payload["script_content"]
        mode = payload.get("mode", "backtest")
        start_date = payload.get("start_date")
        end_date = payload.get("end_date")
        kline_cache = payload.get("klines", {})
        params = payload.get("params")

        import re
        script_content = re.sub(
            r'^.*sys\.path\.insert.*$', '', script_content, flags=re.MULTILINE
        )

        if params:
            script_content = f"PARAMS = {json.dumps(params)}\n" + script_content

        _audit_ast(script_content)

        from indicators import Indicators

        dc_instance = SandboxDataClient(kline_cache)
        fake_data_client = types.ModuleType("data_client")
        fake_data_client.DataClient = lambda **kw: dc_instance

        fake_indicators = types.ModuleType("indicators")
        fake_indicators.Indicators = Indicators

        fake_runner = types.ModuleType("strategy_runner")
        fake_runner.run = lambda *a, **kw: None

        saved = {}
        inject = {
            "data_client": fake_data_client,
            "indicators": fake_indicators,
            "strategy_runner": fake_runner,
        }
        for name, mod in inject.items():
            saved[name] = sys.modules.get(name)
            sys.modules[name] = mod

        try:
            safe_builtins = _make_safe_builtins()
            namespace = {
                "__name__": "__script__",
                "__file__": "<sandbox>",
                "__builtins__": safe_builtins,
            }

            code = compile(script_content, "<strategy>", "exec")
            exec(code, namespace)

            generate_fn = namespace.get("generate_signals")
            if generate_fn is None:
                raise ValueError("脚本中未找到 generate_signals() 函数")

            result = generate_fn(mode=mode, start_date=start_date, end_date=end_date)

            print(json.dumps(result, default=str, ensure_ascii=False))

        finally:
            for name in inject:
                if saved[name] is None:
                    sys.modules.pop(name, None)
                else:
                    sys.modules[name] = saved[name]

    except Exception as e:
        print(json.dumps({"error": f"{type(e).__name__}: {e}"}, ensure_ascii=False))
        sys.exit(1)


if __name__ == "__main__":
    main()
