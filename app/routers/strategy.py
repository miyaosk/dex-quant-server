"""
策略 CRUD API — 创建、查询、列表、更新
"""

import json
import uuid

from fastapi import APIRouter, HTTPException
from loguru import logger

from app import database
from app.models import StrategySpec, StrategyListItem

router = APIRouter(prefix="/strategies", tags=["策略"])


@router.post("/", response_model=StrategySpec)
async def create_strategy(spec: StrategySpec):
    """创建策略"""
    if not spec.strategy_id:
        spec.strategy_id = f"strat_{uuid.uuid4().hex[:12]}"

    await database.save_strategy(
        strategy_id=spec.strategy_id,
        name=spec.name,
        version=spec.version,
        spec_json=spec.model_dump_json(),
        lifecycle_state=spec.lifecycle_state,
    )
    logger.info(f"策略已创建: {spec.strategy_id} ({spec.name})")
    return spec


@router.get("/", response_model=list[StrategyListItem])
async def list_all_strategies():
    """列出所有策略"""
    rows = await database.list_strategies()
    result = []
    for r in rows:
        spec = {}
        try:
            spec = json.loads(r["spec_json"]) if r.get("spec_json") else {}
        except (json.JSONDecodeError, TypeError):
            pass
        result.append(StrategyListItem(
            strategy_id=r["strategy_id"],
            name=r["name"],
            version=r["version"],
            universe=spec.get("universe", []),
            timeframe=spec.get("timeframe", ""),
            lifecycle_state=r["lifecycle_state"],
            created_at=str(r["created_at"]),
            updated_at=str(r["updated_at"]),
        ))
    return result


@router.get("/{strategy_id}", response_model=StrategySpec)
async def get_strategy(strategy_id: str):
    """获取策略详情"""
    row = await database.get_strategy(strategy_id)
    if row is None:
        raise HTTPException(status_code=404, detail=f"策略 {strategy_id} 不存在")
    spec = StrategySpec(**json.loads(row["spec_json"]))
    return spec


@router.put("/{strategy_id}", response_model=StrategySpec)
async def update_strategy(strategy_id: str, spec: StrategySpec):
    """更新策略"""
    existing = await database.get_strategy(strategy_id)
    if existing is None:
        raise HTTPException(status_code=404, detail=f"策略 {strategy_id} 不存在")

    spec.strategy_id = strategy_id
    await database.save_strategy(
        strategy_id=strategy_id,
        name=spec.name,
        version=spec.version,
        spec_json=spec.model_dump_json(),
        lifecycle_state=spec.lifecycle_state,
    )
    logger.info(f"策略已更新: {strategy_id}")
    return spec
