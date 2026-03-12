"""
数据服务层 — 在 DataClient 之上封装 SQLite 缓存

缓存策略:
  - crypto 数据缓存有效期 1 小时
  - 股票/大宗商品数据缓存有效期 6 小时
  - 命中缓存时直接从 DB 返回 DataFrame，不调用外部 API
"""

from __future__ import annotations

import json
from datetime import datetime, timezone

import pandas as pd
from loguru import logger

from app.core.data_client import DataClient
from app import database


# 缓存有效期（秒）
_CACHE_TTL = {
    "crypto_futures": 3600,
    "crypto_spot": 3600,
    "stock": 21600,
    "commodity": 21600,
    "metal": 21600,
    "defi": 7200,
}


class DataService:
    """带 SQLite 缓存的数据获取服务。"""

    def __init__(self, proxy: str = None):
        self.client = DataClient(proxy=proxy)

    async def get_klines(
        self,
        symbol: str,
        interval: str,
        start_date: str,
        end_date: str,
        market: str = "crypto_futures",
    ) -> pd.DataFrame:
        """
        获取 K 线数据，优先从缓存读取。

        市场类型:
          crypto_futures — 永续合约 K 线
          crypto_spot    — 现货 K 线
          stock          — 美股/ETF
          commodity      — 大宗商品
          metal          — 贵金属现货
        """
        cache_key = f"{market}:{symbol}:{interval}:{start_date}:{end_date}"

        # 尝试读取缓存
        cached = await self._get_fresh_cache(cache_key, market)
        if cached is not None:
            logger.debug(f"缓存命中: {cache_key}")
            return cached

        # 调用 API 获取数据
        logger.info(f"从 API 获取: {cache_key}")
        df = self._fetch_from_api(symbol, interval, start_date, end_date, market)

        if not df.empty:
            await self._save_cache(cache_key, symbol, interval, market, df)

        return df

    async def get_funding_rate(
        self,
        symbol: str,
        start_date: str,
        end_date: str,
    ) -> pd.DataFrame:
        """获取资金费率历史，带缓存。"""
        cache_key = f"funding:{symbol}:{start_date}:{end_date}"

        cached = await self._get_fresh_cache(cache_key, "crypto_futures")
        if cached is not None:
            logger.debug(f"资金费率缓存命中: {cache_key}")
            return cached

        logger.info(f"从 API 获取资金费率: {cache_key}")
        df = self.client.get_funding_rate(symbol, start_date, end_date)

        if not df.empty:
            await self._save_cache(cache_key, symbol, "funding", "crypto_futures", df)

        return df

    def _fetch_from_api(
        self,
        symbol: str,
        interval: str,
        start_date: str,
        end_date: str,
        market: str,
    ) -> pd.DataFrame:
        """根据市场类型调用对应 DataClient 方法。"""
        if market == "crypto_futures":
            return self.client.get_perp_klines(symbol, interval, start_date, end_date)
        elif market == "crypto_spot":
            return self.client.get_spot_klines(symbol, interval, start_date, end_date)
        elif market == "stock":
            return DataClient.get_stock_klines(symbol, start_date, end_date, interval)
        elif market == "commodity":
            return DataClient.get_commodity_klines(symbol, start_date, end_date, interval)
        elif market == "metal":
            return DataClient.get_metal_spot_klines(symbol, start_date, end_date)
        else:
            raise ValueError(f"不支持的市场类型: {market}")

    async def _get_fresh_cache(
        self, cache_key: str, market: str
    ) -> pd.DataFrame | None:
        """从数据库读取缓存，检查是否过期。"""
        try:
            async with database.get_db() as db:
                cursor = await db.execute(
                    "SELECT data_json, fetched_at FROM kline_cache WHERE cache_key = ?",
                    (cache_key,),
                )
                row = await cursor.fetchone()

            if row is None:
                return None

            # 检查是否过期
            fetched_at = datetime.fromisoformat(row["fetched_at"]).replace(tzinfo=timezone.utc)
            age_seconds = (datetime.now(timezone.utc) - fetched_at).total_seconds()
            ttl = _CACHE_TTL.get(market, 3600)

            if age_seconds > ttl:
                return None

            data = json.loads(row["data_json"])
            df = pd.DataFrame(data)
            if "datetime" in df.columns:
                df["datetime"] = pd.to_datetime(df["datetime"], utc=True)
            return df

        except Exception as e:
            logger.warning(f"缓存读取失败: {e}")
            return None

    async def _save_cache(
        self,
        cache_key: str,
        symbol: str,
        interval: str,
        market: str,
        df: pd.DataFrame,
    ) -> None:
        """将 DataFrame 序列化后写入缓存。"""
        try:
            df_copy = df.copy()
            if "datetime" in df_copy.columns:
                df_copy["datetime"] = df_copy["datetime"].astype(str)
            data_json = df_copy.to_json(orient="records")

            await database.save_kline_cache(
                cache_key=cache_key,
                symbol=symbol,
                interval=interval,
                market=market,
                data_json=data_json,
                row_count=len(df),
            )
        except Exception as e:
            logger.warning(f"缓存写入失败: {e}")

    def close(self):
        self.client.close()
