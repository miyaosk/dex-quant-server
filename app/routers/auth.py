"""
认证 & 配额 API — 机器码注册、Token 分配、配额查询

流程:
  1. 客户端首次使用时生成 machine_code（硬件指纹哈希）
  2. POST /auth/register 注册 → 获得 token
  3. 后续请求通过 X-Token 头携带 token
  4. 免费配额: 每个 token 可在 Server 上运行 3 个策略
  5. 超出配额 → 需在本地运行策略脚本
"""

import uuid

from fastapi import APIRouter, Header, HTTPException
from loguru import logger

from app import database
from app.models import MachineRegisterRequest, MachineRegisterResponse, QuotaResponse

router = APIRouter(prefix="/auth", tags=["认证"])

FREE_QUOTA = 3


async def validate_token(x_token: str = Header(default="")) -> dict:
    """公共 Token 校验逻辑，被其他路由复用。"""
    if not x_token:
        raise HTTPException(status_code=401, detail="缺少 X-Token 头，请先注册机器码")

    record = await database.get_token_record(x_token)
    if record is None:
        raise HTTPException(status_code=401, detail="Token 无效，请重新注册")

    if record.get("status") != "active":
        raise HTTPException(status_code=403, detail="Token 已被停用")

    return record


@router.post("/register", response_model=MachineRegisterResponse)
async def register_machine(req: MachineRegisterRequest):
    """
    注册机器码 → 获取 Token

    - 首次注册：生成新 Token，配额 3
    - 重复注册：返回已有 Token
    """
    existing = await database.get_token_by_machine_code(req.machine_code)

    if existing:
        used = await database.count_strategies_by_machine(req.machine_code)
        max_s = existing.get("max_strategies", FREE_QUOTA)
        logger.info(f"机器码已注册: {req.machine_code[:8]}... | 已用 {used}/{max_s}")
        return MachineRegisterResponse(
            token=existing["token"],
            machine_code=req.machine_code,
            max_strategies=max_s,
            used_strategies=used,
            remaining=max(0, max_s - used),
            status=existing.get("status", "active"),
        )

    token = f"tok_{uuid.uuid4().hex}"
    await database.create_token(req.machine_code, token, FREE_QUOTA)
    logger.info(f"新机器码注册: {req.machine_code[:8]}... → {token[:12]}...")

    return MachineRegisterResponse(
        token=token,
        machine_code=req.machine_code,
        max_strategies=FREE_QUOTA,
        used_strategies=0,
        remaining=FREE_QUOTA,
        status="active",
    )


@router.get("/quota", response_model=QuotaResponse)
async def get_quota(x_token: str = Header(default="")):
    """查询当前 Token 的配额使用情况"""
    record = await validate_token(x_token)
    machine_code = record["machine_code"]
    max_s = record.get("max_strategies", FREE_QUOTA)

    strategies = await database.list_strategies_by_machine(machine_code)
    used = len(strategies)

    strategy_list = [
        {
            "strategy_id": s["strategy_id"],
            "name": s["name"],
            "symbol": s.get("symbol", ""),
            "status": s.get("status", ""),
        }
        for s in strategies
    ]

    return QuotaResponse(
        machine_code=machine_code,
        max_strategies=max_s,
        used_strategies=used,
        remaining=max(0, max_s - used),
        strategies=strategy_list,
    )
