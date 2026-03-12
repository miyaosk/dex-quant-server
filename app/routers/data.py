"""
数据 API — K 线获取 & 交易对列表
"""

from fastapi import APIRouter, HTTPException
from loguru import logger

from app.config import settings
from app.models import KlineRequest
from app.services.data_service import DataService

router = APIRouter(prefix="/data", tags=["数据"])


def _get_data_service() -> DataService:
    return DataService(proxy=settings.PROXY_URL)


@router.post("/klines")
async def fetch_klines(req: KlineRequest):
    """获取 K 线数据（带缓存）"""
    svc = _get_data_service()
    try:
        df = await svc.get_klines(
            req.symbol, req.interval, req.start_date, req.end_date, req.market,
        )
        return {
            "symbol": req.symbol,
            "interval": req.interval,
            "rows": len(df),
            "data": df.to_dict(orient="records"),
        }
    except Exception as e:
        logger.error(f"获取 K 线失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        svc.close()


@router.get("/symbols")
async def list_symbols():
    """列出可用永续合约交易对"""
    svc = _get_data_service()
    try:
        symbols = svc.client.list_perp_symbols()
        return {"count": len(symbols), "symbols": symbols}
    except Exception as e:
        logger.error(f"获取交易对失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        svc.close()
