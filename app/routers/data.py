"""
数据 API — K 线获取（带 MySQL 缓存）& 交易对列表

支持周期: 15m / 1h / 2h / 1d
"""

from fastapi import APIRouter, HTTPException
from loguru import logger

from app import config
from app.models import KlineRequest
from app.services.data_service import DataService

router = APIRouter(prefix="/data", tags=["数据"])


def _get_data_service() -> DataService:
    return DataService(proxy=config.PROXY_URL)


@router.post("/klines")
async def fetch_klines(req: KlineRequest):
    """
    获取 K 线数据（带 MySQL 缓存）

    支持周期: 15m / 1h / 2h / 1d
    同一个币同一个周期已缓存的不重复下载
    """
    svc = _get_data_service()
    try:
        market = "crypto_futures"
        if req.exchange == "hyperliquid":
            market = "hyperliquid"

        df = await svc.get_klines(
            req.symbol, req.interval, req.start_date, req.end_date, market,
        )
        return {
            "exchange": req.exchange,
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
async def list_symbols(exchange: str = "binance"):
    """列出可用永续合约交易对"""
    svc = _get_data_service()
    try:
        if exchange == "hyperliquid":
            symbols = svc.client.list_hl_perp_symbols()
        else:
            symbols = svc.client.list_perp_symbols()
        return {"exchange": exchange, "count": len(symbols), "symbols": symbols}
    except Exception as e:
        logger.error(f"获取交易对失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        svc.close()
