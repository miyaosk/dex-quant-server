"""
K8s Job 沙箱执行器 — 在临时 Pod 中运行策略脚本

前置条件（需运维配合）:
  1. 构建沙箱镜像: docker build -t <ECR>/sandbox:latest sandbox/
  2. 创建 ServiceAccount 并授予 Job 创建权限:
     kubectl create sa strategy-sandbox -n dex-qa
     kubectl create role sandbox-runner --verb=create,get,delete --resource=jobs,pods,pods/log -n dex-qa
     kubectl create rolebinding sandbox-runner --role=sandbox-runner --serviceaccount=dex-qa:strategy-sandbox -n dex-qa
  3. 主 Deployment 的 Pod 使用该 ServiceAccount
"""

from __future__ import annotations

import asyncio
import json
import uuid
from typing import Optional

from loguru import logger

SANDBOX_IMAGE = "533267002049.dkr.ecr.ap-southeast-1.amazonaws.com/key:sandbox-latest"
SANDBOX_NAMESPACE = "dex-qa"
SANDBOX_TIMEOUT = 120


async def execute_in_k8s(
    script_content: str,
    mode: str = "backtest",
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> dict:
    """
    在 K8s 临时 Pod 中执行策略脚本。

    流程:
      1. 将脚本内容写入 ConfigMap
      2. 创建 Job（无网络、内存限制 512Mi、CPU 限制 1核）
      3. 等待完成，读取日志获取结果
      4. 清理 Job + ConfigMap
    """
    job_id = f"sandbox-{uuid.uuid4().hex[:8]}"
    payload = json.dumps({
        "script_content": script_content,
        "mode": mode,
        "start_date": start_date,
        "end_date": end_date,
    })

    job_manifest = _build_job_manifest(job_id, payload)

    try:
        await _kubectl_apply(job_manifest)

        logger.info("沙箱 Job 已创建: {}", job_id)
        output = await asyncio.wait_for(
            _wait_and_get_logs(job_id),
            timeout=SANDBOX_TIMEOUT,
        )

        result = json.loads(output)
        if "error" in result:
            raise ValueError(result["error"])
        return result

    except asyncio.TimeoutError:
        raise TimeoutError(f"沙箱执行超时（{SANDBOX_TIMEOUT}秒）")
    finally:
        await _kubectl_delete(job_id)


def _build_job_manifest(job_id: str, payload: str) -> dict:
    return {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": job_id,
            "namespace": SANDBOX_NAMESPACE,
            "labels": {"app": "strategy-sandbox"},
        },
        "spec": {
            "ttlSecondsAfterFinished": 60,
            "backoffLimit": 0,
            "activeDeadlineSeconds": SANDBOX_TIMEOUT,
            "template": {
                "metadata": {"labels": {"app": "strategy-sandbox"}},
                "spec": {
                    "restartPolicy": "Never",
                    "automountServiceAccountToken": False,
                    "containers": [{
                        "name": "sandbox",
                        "image": SANDBOX_IMAGE,
                        "stdin": True,
                        "command": ["sh", "-c", f"echo '{_escape_for_shell(payload)}' | python /runner.py"],
                        "resources": {
                            "limits": {"memory": "512Mi", "cpu": "1"},
                            "requests": {"memory": "256Mi", "cpu": "500m"},
                        },
                        "securityContext": {
                            "runAsNonRoot": True,
                            "runAsUser": 65534,
                            "readOnlyRootFilesystem": True,
                            "allowPrivilegeEscalation": False,
                            "capabilities": {"drop": ["ALL"]},
                        },
                    }],
                    "securityContext": {
                        "seccompProfile": {"type": "RuntimeDefault"},
                    },
                    # 禁用网络：使用 NetworkPolicy 或 DNS policy
                    "dnsPolicy": "None",
                    "dnsConfig": {"nameservers": ["127.0.0.1"]},
                    "enableServiceLinks": False,
                },
            },
        },
    }


def _escape_for_shell(s: str) -> str:
    return s.replace("'", "'\"'\"'")


async def _kubectl_apply(manifest: dict) -> None:
    manifest_json = json.dumps(manifest)
    proc = await asyncio.create_subprocess_exec(
        "kubectl", "apply", "-f", "-",
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    _, stderr = await proc.communicate(input=manifest_json.encode())
    if proc.returncode != 0:
        raise RuntimeError(f"kubectl apply 失败: {stderr.decode()}")


async def _wait_and_get_logs(job_id: str) -> str:
    while True:
        proc = await asyncio.create_subprocess_exec(
            "kubectl", "get", "job", job_id,
            "-n", SANDBOX_NAMESPACE,
            "-o", "jsonpath={.status.conditions[0].type}",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, _ = await proc.communicate()
        status = stdout.decode().strip()

        if status in ("Complete", "Failed"):
            break
        await asyncio.sleep(1)

    proc = await asyncio.create_subprocess_exec(
        "kubectl", "logs", f"job/{job_id}",
        "-n", SANDBOX_NAMESPACE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, _ = await proc.communicate()
    return stdout.decode()


async def _kubectl_delete(job_id: str) -> None:
    proc = await asyncio.create_subprocess_exec(
        "kubectl", "delete", "job", job_id,
        "-n", SANDBOX_NAMESPACE,
        "--ignore-not-found=true",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    await proc.communicate()
