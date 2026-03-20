"""
沙箱容器内的脚本执行入口

从 stdin 读取 JSON（script_content + 参数），
执行 generate_signals()，将结果 JSON 写到 stdout。
"""

import json
import sys


def main():
    payload = json.loads(sys.stdin.read())
    script_content = payload["script_content"]
    mode = payload.get("mode", "backtest")
    start_date = payload.get("start_date")
    end_date = payload.get("end_date")

    namespace = {"__name__": "__script__", "__file__": "<sandbox>"}
    exec(compile(script_content, "<strategy>", "exec"), namespace)

    generate_fn = namespace.get("generate_signals")
    if generate_fn is None:
        print(json.dumps({"error": "未找到 generate_signals()"}))
        sys.exit(1)

    result = generate_fn(mode=mode, start_date=start_date, end_date=end_date)
    print(json.dumps(result, default=str))


if __name__ == "__main__":
    main()
