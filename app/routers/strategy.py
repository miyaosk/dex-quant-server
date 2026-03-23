"""
策略 CRUD API — 保存脚本为核心的策略

配额限制：每个 Token（机器码）最多 3 个「定时监控任务」
回测不占配额，可无限次调用
所有接口均需 X-Token 认证，且只能操作自己的策略
"""

import json
import uuid

from fastapi import APIRouter, Header, HTTPException
from loguru import logger

from app import database
from app.models import StrategyCreate, StrategyDetail, StrategyListItem
from app.routers.auth import validate_token

router = APIRouter(prefix="/strategies", tags=["策略"])


async def _get_owned_strategy(strategy_id: str, machine_code: str) -> dict:
    """获取策略并校验归属，不属于当前用户则 403"""
    row = await database.get_strategy(strategy_id)
    if row is None:
        raise HTTPException(status_code=404, detail=f"策略 {strategy_id} 不存在")
    if row.get("machine_code") != machine_code:
        raise HTTPException(status_code=403, detail="无权访问该策略")
    return row


@router.post("/", response_model=StrategyDetail)
async def create_strategy(spec: StrategyCreate, x_token: str = Header(default="")):
    """
    创建策略（脚本源码是核心）

    需要 X-Token 头，校验配额：
    - 免费用户每个机器码最多 3 个策略
    - 超出配额返回 403，提示在本地运行
    """
    record = await validate_token(x_token)
    machine_code = record["machine_code"]
    max_strategies = record.get("max_strategies", 3)

    used = await database.count_strategies_by_machine(machine_code)
    if used >= max_strategies:
        raise HTTPException(
            status_code=403,
            detail=f"已达到免费监控任务上限（{max_strategies}个），"
                   f"回测不受此限制可随时使用，如需更多监控配额请联系我们升级",
        )

    strategy_id = f"strat_{uuid.uuid4().hex[:12]}"

    await database.save_strategy(
        strategy_id=strategy_id,
        name=spec.name,
        description=spec.description,
        script_content=spec.script_content,
        symbol=spec.symbol,
        timeframe=spec.timeframe,
        direction=spec.direction,
        version=spec.version,
        tags=json.dumps(spec.tags, ensure_ascii=False),
        status="draft",
        machine_code=machine_code,
    )
    logger.info(f"策略已创建: {strategy_id} ({spec.name}) | 机器={machine_code[:8]}... | 配额 {used+1}/{max_strategies}")

    return StrategyDetail(strategy_id=strategy_id, **spec.model_dump())


@router.get("/", response_model=list[StrategyListItem])
async def list_strategies(x_token: str = Header(default="")):
    """列出当前用户的策略"""
    record = await validate_token(x_token)
    machine_code = record["machine_code"]

    rows = await database.list_strategies_by_machine(machine_code)
    return [
        StrategyListItem(
            strategy_id=r["strategy_id"],
            name=r["name"],
            symbol=r.get("symbol", ""),
            timeframe=r.get("timeframe", ""),
            version=r.get("version", "v1.0"),
            status=r.get("status", "draft"),
            created_at=str(r.get("created_at", "")),
            updated_at=str(r.get("updated_at", "")),
        )
        for r in rows
    ]


@router.get("/{strategy_id}", response_model=StrategyDetail)
async def get_strategy(strategy_id: str, x_token: str = Header(default="")):
    """获取策略详情（含脚本源码），仅限本人策略"""
    record = await validate_token(x_token)
    row = await _get_owned_strategy(strategy_id, record["machine_code"])

    tags = []
    raw_tags = row.get("tags", "[]")
    if isinstance(raw_tags, str):
        try:
            tags = json.loads(raw_tags)
        except (json.JSONDecodeError, TypeError):
            tags = []

    return StrategyDetail(
        strategy_id=row["strategy_id"],
        name=row["name"],
        description=row.get("description", ""),
        script_content=row.get("script_content", ""),
        symbol=row.get("symbol", ""),
        timeframe=row.get("timeframe", ""),
        direction=row.get("direction", "long_short"),
        version=row.get("version", "v1.0"),
        tags=tags,
        status=row.get("status", "draft"),
        created_at=str(row.get("created_at", "")),
        updated_at=str(row.get("updated_at", "")),
    )


@router.put("/{strategy_id}", response_model=StrategyDetail)
async def update_strategy(strategy_id: str, spec: StrategyCreate, x_token: str = Header(default="")):
    """更新策略，仅限本人策略"""
    record = await validate_token(x_token)
    existing = await _get_owned_strategy(strategy_id, record["machine_code"])

    await database.save_strategy(
        strategy_id=strategy_id,
        name=spec.name,
        description=spec.description,
        script_content=spec.script_content,
        symbol=spec.symbol,
        timeframe=spec.timeframe,
        direction=spec.direction,
        version=spec.version,
        tags=json.dumps(spec.tags, ensure_ascii=False),
        status=existing.get("status", "draft"),
        machine_code=record["machine_code"],
    )
    logger.info(f"策略已更新: {strategy_id}")

    return StrategyDetail(
        strategy_id=strategy_id,
        status=existing.get("status", "draft"),
        created_at=str(existing.get("created_at", "")),
        **spec.model_dump(),
    )
