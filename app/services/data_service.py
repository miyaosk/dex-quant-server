"""
数据服务层 — 按月分段缓存

缓存策略:
  - 按月拆分: 请求 2024-01 ~ 2024-12 → 拆成 12 个月独立缓存
  - cache_key 格式: {market}:{symbol}:{interval}:{YYYY-MM}
  - 已完成的月份永不过期（历史数据不变）
  - 当前月缓存有效期 1 小时（数据还在更新）
  - 跨月请求自动合并多段缓存

好处:
  - 15m 一年 8.5MB → 拆成 12 段 × ~700KB
  - 缓存复用率高：不同时间范围请求可共享已缓存的月份
  - 新请求只需拉未缓存的月份
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from dateutil.relativedelta import relativedelta

import pandas as pd
from loguru import logger

from app.core.data_client import DataClient
from app import database


_CURRENT_MONTH_TTL = 3600  # 当月缓存 1 小时过期


class DataService:
    """按月分段缓存的数据获取服务。"""

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
        获取 K 线数据，按月分段缓存。

        1. 将请求时间范围拆成多个月
        2. 每个月独立检查缓存
        3. 未命中的月份从 API 拉取并缓存
        4. 合并所有月份数据返回
        """
        months = self._split_months(start_date, end_date)
        all_dfs = []
        cached_count = 0
        fetched_count = 0

        for month_start, month_end in months:
            cache_key = f"{market}:{symbol}:{interval}:{month_start[:7]}"

            cached_df = await self._get_month_cache(cache_key, month_start)
            if cached_df is not None:
                all_dfs.append(cached_df)
                cached_count += 1
                continue

            logger.info(f"从 API 获取: {symbol} {interval} {month_start[:7]}")
            df = self._fetch_from_api(symbol, interval, month_start, month_end, market)

            if not df.empty:
                await self._save_cache(cache_key, symbol, interval, market, df)
                all_dfs.append(df)
                fetched_count += 1

        if cached_count > 0 or fetched_count > 0:
            logger.info(
                f"K线数据: {symbol} {interval} | "
                f"{len(months)} 个月 | 缓存命中 {cached_count} | API拉取 {fetched_count}"
            )

        if not all_dfs:
            return pd.DataFrame()

        result = pd.concat(all_dfs, ignore_index=True)
        result = result.sort_values("datetime").drop_duplicates(subset=["datetime"]).reset_index(drop=True)

        start_dt = pd.Timestamp(start_date, tz="UTC")
        end_dt = pd.Timestamp(end_date, tz="UTC")
        result = result[(result["datetime"] >= start_dt) & (result["datetime"] <= end_dt)]

        return result.reset_index(drop=True)

    async def get_funding_rate(
        self,
        symbol: str,
        start_date: str,
        end_date: str,
        market: str = "crypto_futures",
    ) -> pd.DataFrame:
        """获取资金费率历史，按月缓存。"""
        months = self._split_months(start_date, end_date)
        all_dfs = []

        for month_start, month_end in months:
            cache_key = f"funding:{market}:{symbol}:{month_start[:7]}"

            cached_df = await self._get_month_cache(cache_key, month_start)
            if cached_df is not None:
                all_dfs.append(cached_df)
                continue

            logger.info(f"从 API 获取资金费率: {symbol} {month_start[:7]}")
            if market == "hyperliquid":
                df = self.client.get_hl_funding_rate(symbol, month_start, month_end)
            else:
                df = self.client.get_funding_rate(symbol, month_start, month_end)

            if not df.empty:
                await self._save_cache(cache_key, symbol, "funding", market, df)
                all_dfs.append(df)

        if not all_dfs:
            return pd.DataFrame()

        result = pd.concat(all_dfs, ignore_index=True)
        return result.sort_values("datetime").drop_duplicates(subset=["datetime"]).reset_index(drop=True)

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
        elif market == "hyperliquid":
            return self.client.get_hl_perp_klines(symbol, interval, start_date, end_date)
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

    @staticmethod
    def _split_months(start_date: str, end_date: str) -> list[tuple[str, str]]:
        """
        将日期范围拆成按月的段。

        "2024-07-01" ~ "2024-09-15" →
          [("2024-07-01", "2024-07-31"),
           ("2024-08-01", "2024-08-31"),
           ("2024-09-01", "2024-09-15")]
        """
        start = datetime.strptime(start_date[:10], "%Y-%m-%d")
        end = datetime.strptime(end_date[:10], "%Y-%m-%d")

        months = []
        current = start.replace(day=1)

        while current <= end:
            month_start = max(current, start)
            next_month = current + relativedelta(months=1)
            month_end = min(next_month - pd.Timedelta(days=1), end)

            months.append((
                month_start.strftime("%Y-%m-%d"),
                month_end.strftime("%Y-%m-%d"),
            ))
            current = next_month

        return months

    async def _get_month_cache(
        self, cache_key: str, month_start: str
    ) -> pd.DataFrame | None:
        """
        读取月度缓存。

        - 已完成的月份（非当前月）：永不过期
        - 当前月：1 小时过期
        """
        try:
            row = await database.get_cached_klines(cache_key)
            if row is None:
                return None

            now = datetime.now(timezone.utc)
            month_dt = datetime.strptime(month_start[:7], "%Y-%m")
            is_current_month = (month_dt.year == now.year and month_dt.month == now.month)

            if is_current_month:
                fetched_at = row["fetched_at"]
                if isinstance(fetched_at, datetime):
                    fetched_at = fetched_at.replace(tzinfo=timezone.utc)
                else:
                    fetched_at = datetime.fromisoformat(str(fetched_at)).replace(tzinfo=timezone.utc)
                age_seconds = (now - fetched_at).total_seconds()
                if age_seconds > _CURRENT_MONTH_TTL:
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
        """将单月 DataFrame 写入缓存。"""
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
