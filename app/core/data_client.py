"""
多源数据客户端 — 直接调用公开 API

数据源:
  - Binance Futures: K线、资金费率、持仓量、合约信息、标记价格
  - Binance Spot: 现货 K线
  - CoinGecko: PAXG/XAUT 等加密代币价格
  - yfinance: 美股、大宗商品、贵金属现货
  - DeFi Llama: 协议 TVL、手续费收入

全部免费公开端点，无需 API Key。
国内访问 Binance 可通过 Settings.PROXY_URL 或环境变量配置代理。
"""

import time
from datetime import datetime, timezone
from typing import Optional

import httpx
import pandas as pd
from loguru import logger

BINANCE_FUTURES_BASE = "https://fapi.binance.com"
BINANCE_SPOT_BASE = "https://api.binance.com"
HYPERLIQUID_BASE = "https://api.hyperliquid.xyz"
COINGECKO_BASE = "https://api.coingecko.com/api/v3"
DEFILLAMA_BASE = "https://api.llama.fi"

INTERVAL_MAP = {
    "1m": "1m", "5m": "5m", "15m": "15m",
    "1h": "1h", "4h": "4h", "1d": "1d",
}

COINGECKO_IDS = {
    "PAXG": "pax-gold",
    "XAUT": "tether-gold",
    "OUSG": "ondo-us-government-bond-fund",
    "OMMF": "ondo-us-dollar-yield",
}

YFINANCE_TICKERS = {
    "RWA:AAPL": "AAPL", "RWA:NVDA": "NVDA", "RWA:TSLA": "TSLA",
    "RWA:MSFT": "MSFT", "RWA:GOOGL": "GOOGL", "RWA:AMZN": "AMZN",
    "RWA:META": "META", "RWA:SPY": "SPY", "RWA:QQQ": "QQQ",
    "COMM:WTI": "CL=F", "COMM:BRENT": "BZ=F",
    "COMM:NG": "NG=F", "COMM:COPPER": "HG=F",
    "METAL:XAU-SPOT": "GC=F", "METAL:XAG-SPOT": "SI=F",
}


def _ts_ms(dt_str: str) -> int:
    """日期字符串 (YYYY-MM-DD) 转毫秒时间戳。"""
    dt = datetime.strptime(dt_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def _symbol_to_binance(symbol: str) -> str:
    """BTC-USDT-PERP → BTCUSDT"""
    parts = symbol.upper().replace("-PERP", "").replace("-SPOT", "").split("-")
    return "".join(parts)


class DataClient:
    """多源数据客户端，支持 Binance / CoinGecko / yfinance / DeFi Llama。"""

    def __init__(self, proxy: Optional[str] = None):
        """
        初始化数据客户端。

        proxy 优先级: 显式参数 > 环境变量 PROXY_URL
        """
        import os
        proxy_url = proxy or os.environ.get("PROXY_URL")
        self._client = httpx.Client(
            timeout=30.0,
            proxy=proxy_url,
        )

    def _get(self, url: str, params: dict = None) -> dict | list:
        """带 429 限流重试的 GET 请求。"""
        resp = self._client.get(url, params=params)
        if resp.status_code == 429:
            retry = int(resp.headers.get("Retry-After", "5"))
            logger.warning(f"429 限流，等待 {retry}s")
            time.sleep(retry)
            resp = self._client.get(url, params=params)
        resp.raise_for_status()
        return resp.json()

    def _post(self, url: str, json_body: dict) -> dict | list:
        """POST JSON 请求（Hyperliquid API 使用 POST）。"""
        resp = self._client.post(url, json=json_body)
        if resp.status_code == 429:
            retry = int(resp.headers.get("Retry-After", "5"))
            logger.warning(f"429 限流，等待 {retry}s")
            time.sleep(retry)
            resp = self._client.post(url, json=json_body)
        resp.raise_for_status()
        return resp.json()

    # ════════════════════════════════════════
    #  Binance Futures — 永续合约
    # ════════════════════════════════════════

    def get_perp_klines(
        self,
        symbol: str,
        interval: str = "1d",
        start_date: str = None,
        end_date: str = None,
        limit: int = 1500,
        timeframe: str = None,
        period: str = None,
    ) -> pd.DataFrame:
        """永续合约 K 线。自动分页拉取完整历史。"""
        interval = timeframe or period or interval
        bn_symbol = _symbol_to_binance(symbol)
        all_rows: list = []
        params: dict = {
            "symbol": bn_symbol,
            "interval": INTERVAL_MAP.get(interval, interval),
            "limit": limit,
        }
        if start_date:
            params["startTime"] = _ts_ms(start_date)
        if end_date:
            params["endTime"] = _ts_ms(end_date)

        while True:
            data = self._get(f"{BINANCE_FUTURES_BASE}/fapi/v1/klines", params)
            if not data:
                break
            all_rows.extend(data)
            if len(data) < limit:
                break
            params["startTime"] = data[-1][0] + 1
            if end_date and params["startTime"] > _ts_ms(end_date):
                break
            time.sleep(0.1)

        if not all_rows:
            return pd.DataFrame()

        df = pd.DataFrame(all_rows, columns=[
            "open_time", "open", "high", "low", "close", "volume",
            "close_time", "quote_volume", "trades", "taker_buy_volume",
            "taker_buy_quote_volume", "ignore",
        ])
        df["datetime"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
        for col in ["open", "high", "low", "close", "volume", "quote_volume",
                     "taker_buy_volume", "taker_buy_quote_volume"]:
            df[col] = df[col].astype(float)
        df = df.rename(columns={
            "quote_volume": "volume_usd",
            "taker_buy_quote_volume": "taker_buy_volume_usd",
        })
        df["taker_sell_volume_usd"] = df["volume_usd"] - df["taker_buy_volume_usd"]
        return df[["datetime", "open", "high", "low", "close", "volume",
                    "volume_usd", "trades", "taker_buy_volume_usd",
                    "taker_sell_volume_usd"]].reset_index(drop=True)

    def get_funding_rate(
        self,
        symbol: str,
        start_date: str = None,
        end_date: str = None,
        limit: int = 1000,
    ) -> pd.DataFrame:
        """资金费率历史。每 8 小时一条，自动分页拉取。"""
        bn_symbol = _symbol_to_binance(symbol)
        all_rows: list = []
        params: dict = {"symbol": bn_symbol, "limit": limit}
        if start_date:
            params["startTime"] = _ts_ms(start_date)
        if end_date:
            params["endTime"] = _ts_ms(end_date)

        while True:
            data = self._get(f"{BINANCE_FUTURES_BASE}/fapi/v1/fundingRate", params)
            if not data:
                break
            all_rows.extend(data)
            if len(data) < limit:
                break
            params["startTime"] = data[-1]["fundingTime"] + 1
            time.sleep(0.1)

        if not all_rows:
            return pd.DataFrame()

        df = pd.DataFrame(all_rows)
        df["datetime"] = pd.to_datetime(df["fundingTime"], unit="ms", utc=True)
        df["funding_rate"] = df["fundingRate"].astype(float)
        df["mark_price"] = df["markPrice"].astype(float)
        return df[["datetime", "funding_rate", "mark_price"]].reset_index(drop=True)

    def get_open_interest(self, symbol: str) -> dict:
        """当前持仓量快照。"""
        bn_symbol = _symbol_to_binance(symbol)
        data = self._get(
            f"{BINANCE_FUTURES_BASE}/fapi/v1/openInterest",
            {"symbol": bn_symbol},
        )
        return {
            "symbol": symbol,
            "open_interest": float(data["openInterest"]),
            "timestamp": data["time"],
        }

    def get_open_interest_hist(
        self,
        symbol: str,
        period: str = "1d",
        limit: int = 30,
    ) -> pd.DataFrame:
        """持仓量历史统计（仅最近 30 天）。"""
        pair = _symbol_to_binance(symbol).replace("USDT", "")
        data = self._get(f"{BINANCE_FUTURES_BASE}/futures/data/openInterestHist", {
            "pair": pair,
            "contractType": "PERPETUAL",
            "period": period,
            "limit": limit,
        })
        if not data:
            return pd.DataFrame()

        df = pd.DataFrame(data)
        df["datetime"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
        df["open_interest"] = df["sumOpenInterest"].astype(float)
        df["open_interest_usd"] = df["sumOpenInterestValue"].astype(float)
        return df[["datetime", "open_interest", "open_interest_usd"]].reset_index(drop=True)

    def get_long_short_ratio(
        self,
        symbol: str,
        period: str = "1d",
        limit: int = 30,
    ) -> pd.DataFrame:
        """Top Trader 多空持仓比（仅最近 30 天）。"""
        bn_symbol = _symbol_to_binance(symbol)
        data = self._get(
            f"{BINANCE_FUTURES_BASE}/futures/data/topLongShortPositionRatio",
            {"symbol": bn_symbol, "period": period, "limit": limit},
        )
        if not data:
            return pd.DataFrame()

        df = pd.DataFrame(data)
        df["datetime"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
        df["long_short_ratio"] = df["longShortRatio"].astype(float)
        df["long_account"] = df["longAccount"].astype(float)
        df["short_account"] = df["shortAccount"].astype(float)
        return df[["datetime", "long_short_ratio", "long_account",
                    "short_account"]].reset_index(drop=True)

    def get_mark_price(self, symbol: str) -> dict:
        """当前标记价格和资金费率。"""
        bn_symbol = _symbol_to_binance(symbol)
        data = self._get(
            f"{BINANCE_FUTURES_BASE}/fapi/v1/premiumIndex",
            {"symbol": bn_symbol},
        )
        return {
            "symbol": symbol,
            "mark_price": float(data["markPrice"]),
            "index_price": float(data["indexPrice"]),
            "funding_rate": float(data["lastFundingRate"]),
            "next_funding_time": data["nextFundingTime"],
        }

    def get_exchange_info(self, symbol: str = None) -> dict | list:
        """合约信息（面值、杠杆上限、最小下单量等）。"""
        data = self._get(f"{BINANCE_FUTURES_BASE}/fapi/v1/exchangeInfo")
        symbols = data.get("symbols", [])

        if symbol:
            bn_symbol = _symbol_to_binance(symbol)
            for s in symbols:
                if s["symbol"] == bn_symbol:
                    return self._parse_contract_info(s, symbol)
            raise ValueError(f"合约 {symbol} 未找到")

        return [
            self._parse_contract_info(s, f"{s.get('baseAsset', '')}-{s.get('quoteAsset', '')}-PERP")
            for s in symbols
            if s.get("contractType") == "PERPETUAL"
        ]

    @staticmethod
    def _parse_contract_info(raw: dict, symbol: str) -> dict:
        filters = {f["filterType"]: f for f in raw.get("filters", [])}
        price_filter = filters.get("PRICE_FILTER", {})
        lot_filter = filters.get("LOT_SIZE", {})
        return {
            "symbol": symbol,
            "base_asset": raw.get("baseAsset", ""),
            "quote_asset": raw.get("quoteAsset", ""),
            "contract_type": raw.get("contractType", ""),
            "tick_size": float(price_filter.get("tickSize", 0)),
            "min_qty": float(lot_filter.get("minQty", 0)),
            "max_qty": float(lot_filter.get("maxQty", 0)),
            "step_size": float(lot_filter.get("stepSize", 0)),
            "maintenance_margin_rate": float(raw.get("maintMarginPercent", 2.5)) / 100,
            "required_margin_rate": float(raw.get("requiredMarginPercent", 5)) / 100,
        }

    def list_perp_symbols(self) -> list[str]:
        """列出 Binance 所有永续合约代码。"""
        data = self._get(f"{BINANCE_FUTURES_BASE}/fapi/v1/exchangeInfo")
        return [
            f"{s['baseAsset']}-{s['quoteAsset']}-PERP"
            for s in data.get("symbols", [])
            if s.get("contractType") == "PERPETUAL" and s.get("status") == "TRADING"
        ]

    # ════════════════════════════════════════
    #  Binance Spot — 现货
    # ════════════════════════════════════════

    def get_spot_klines(
        self,
        symbol: str,
        interval: str = "1d",
        start_date: str = None,
        end_date: str = None,
        limit: int = 1000,
        timeframe: str = None,
        period: str = None,
    ) -> pd.DataFrame:
        """现货 K 线。自动分页，无限历史。"""
        interval = timeframe or period or interval
        bn_symbol = _symbol_to_binance(symbol)
        all_rows: list = []
        params: dict = {"symbol": bn_symbol, "interval": interval, "limit": limit}
        if start_date:
            params["startTime"] = _ts_ms(start_date)
        if end_date:
            params["endTime"] = _ts_ms(end_date)

        while True:
            data = self._get(f"{BINANCE_SPOT_BASE}/api/v3/klines", params)
            if not data:
                break
            all_rows.extend(data)
            if len(data) < limit:
                break
            params["startTime"] = data[-1][0] + 1
            time.sleep(0.1)

        if not all_rows:
            return pd.DataFrame()

        df = pd.DataFrame(all_rows, columns=[
            "open_time", "open", "high", "low", "close", "volume",
            "close_time", "quote_volume", "trades", "taker_buy_volume",
            "taker_buy_quote_volume", "ignore",
        ])
        df["datetime"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
        for col in ["open", "high", "low", "close", "volume", "quote_volume"]:
            df[col] = df[col].astype(float)
        df = df.rename(columns={"quote_volume": "volume_usd"})
        return df[["datetime", "open", "high", "low", "close", "volume",
                    "volume_usd"]].reset_index(drop=True)

    # ════════════════════════════════════════
    #  别名兼容（AI 常用的错误方法名）
    # ════════════════════════════════════════

    get_ohlcv = get_perp_klines
    get_klines = get_perp_klines
    get_candles = get_perp_klines
    fetch_ohlcv = get_perp_klines

    # ════════════════════════════════════════
    #  Hyperliquid — 永续合约
    # ════════════════════════════════════════

    @staticmethod
    def _symbol_to_hl_coin(symbol: str) -> str:
        """BTCUSDT / BTC-USDT-PERP / BTC → BTC"""
        s = symbol.upper().replace("-PERP", "").replace("-SPOT", "")
        for suffix in ("USDT", "USDC", "USD", "BUSD"):
            if s.endswith(suffix) and len(s) > len(suffix):
                s = s[:-len(suffix)]
                break
        return s.split("-")[0]

    def get_hl_perp_klines(
        self,
        symbol: str,
        interval: str = "1h",
        start_date: str = None,
        end_date: str = None,
    ) -> pd.DataFrame:
        """Hyperliquid 永续合约 K 线。最多 5000 根/次，自动分页。"""
        coin = self._symbol_to_hl_coin(symbol)
        hl_interval = INTERVAL_MAP.get(interval, interval)

        start_ms = _ts_ms(start_date) if start_date else int((time.time() - 86400 * 30) * 1000)
        end_ms = _ts_ms(end_date) if end_date else int(time.time() * 1000)

        all_rows: list = []
        cursor = start_ms

        while cursor < end_ms:
            body = {
                "type": "candleSnapshot",
                "req": {
                    "coin": coin,
                    "interval": hl_interval,
                    "startTime": cursor,
                    "endTime": end_ms,
                },
            }
            data = self._post(f"{HYPERLIQUID_BASE}/info", body)
            if not data:
                break
            all_rows.extend(data)
            if len(data) < 5000:
                break
            cursor = data[-1]["T"] + 1
            time.sleep(0.2)

        if not all_rows:
            return pd.DataFrame()

        df = pd.DataFrame(all_rows)
        df["datetime"] = pd.to_datetime(df["t"], unit="ms", utc=True)
        df["open"] = df["o"].astype(float)
        df["high"] = df["h"].astype(float)
        df["low"] = df["l"].astype(float)
        df["close"] = df["c"].astype(float)
        df["volume"] = df["v"].astype(float)
        df["trades"] = df["n"].astype(int)
        df["volume_usd"] = df["close"] * df["volume"]

        return df[["datetime", "open", "high", "low", "close", "volume",
                    "volume_usd", "trades"]].reset_index(drop=True)

    def get_hl_funding_rate(
        self,
        symbol: str,
        start_date: str = None,
        end_date: str = None,
    ) -> pd.DataFrame:
        """Hyperliquid 资金费率历史（每 8h）。"""
        coin = self._symbol_to_hl_coin(symbol)

        start_ms = _ts_ms(start_date) if start_date else int((time.time() - 86400 * 30) * 1000)
        end_ms = _ts_ms(end_date) if end_date else int(time.time() * 1000)

        body: dict = {
            "type": "fundingHistory",
            "coin": coin,
            "startTime": start_ms,
        }
        if end_date:
            body["endTime"] = end_ms

        data = self._post(f"{HYPERLIQUID_BASE}/info", body)
        if not data:
            return pd.DataFrame()

        df = pd.DataFrame(data)
        df["datetime"] = pd.to_datetime(df["time"], unit="ms", utc=True)
        df["funding_rate"] = df["fundingRate"].astype(float)
        df["mark_price"] = df.get("premium", pd.Series([0.0] * len(df))).astype(float)
        return df[["datetime", "funding_rate", "mark_price"]].reset_index(drop=True)

    def list_hl_perp_symbols(self) -> list[str]:
        """列出 Hyperliquid 所有永续合约代码。"""
        data = self._post(f"{HYPERLIQUID_BASE}/info", {"type": "meta"})
        universe = data.get("universe", [])
        return [item["name"] for item in universe if isinstance(item, dict) and "name" in item]

    # ════════════════════════════════════════
    #  CoinGecko — 代币价格
    # ════════════════════════════════════════

    def get_token_history(
        self,
        token: str,
        days: int = 365,
    ) -> pd.DataFrame:
        """代币价格历史（日线）。免费版限流 10-30 次/分钟。"""
        cg_id = COINGECKO_IDS.get(token.upper(), token.lower())
        data = self._get(f"{COINGECKO_BASE}/coins/{cg_id}/market_chart", {
            "vs_currency": "usd",
            "days": days,
            "interval": "daily",
        })
        prices = data.get("prices", [])
        volumes = data.get("total_volumes", [])
        caps = data.get("market_caps", [])

        if not prices:
            return pd.DataFrame()

        df = pd.DataFrame(prices, columns=["timestamp", "close"])
        df["datetime"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
        if volumes and len(volumes) == len(prices):
            df["volume_usd"] = [v[1] for v in volumes]
        if caps and len(caps) == len(prices):
            df["market_cap"] = [c[1] for c in caps]
        return df.drop(columns=["timestamp"]).reset_index(drop=True)

    # ════════════════════════════════════════
    #  yfinance — 美股 / 大宗商品 / 贵金属
    # ════════════════════════════════════════

    @staticmethod
    def get_stock_klines(
        symbol: str,
        start_date: str,
        end_date: str,
        interval: str = "1d",
    ) -> pd.DataFrame:
        """美股/ETF K 线 (yfinance)。"""
        import yfinance as yf

        ticker = YFINANCE_TICKERS.get(symbol.upper(), symbol.replace("RWA:", ""))
        yf_interval = {"1d": "1d", "1h": "1h", "5m": "5m", "1m": "1m"}.get(interval, "1d")

        tk = yf.Ticker(ticker)
        df = tk.history(start=start_date, end=end_date, interval=yf_interval)
        if df.empty:
            return pd.DataFrame()

        df = df.reset_index()
        date_col = "Date" if "Date" in df.columns else "Datetime"
        df = df.rename(columns={
            date_col: "datetime",
            "Open": "open", "High": "high", "Low": "low",
            "Close": "close", "Volume": "volume",
        })
        df["datetime"] = pd.to_datetime(df["datetime"], utc=True)
        df["volume_usd"] = df["close"] * df["volume"]

        cols = ["datetime", "open", "high", "low", "close", "volume", "volume_usd"]
        if "Dividends" in df.columns:
            df["dividends"] = df["Dividends"]
            cols.append("dividends")

        return df[cols].reset_index(drop=True)

    @staticmethod
    def get_commodity_klines(
        symbol: str,
        start_date: str,
        end_date: str,
        interval: str = "1d",
    ) -> pd.DataFrame:
        """大宗商品期货 K 线 (yfinance)。"""
        import yfinance as yf

        ticker = YFINANCE_TICKERS.get(symbol.upper())
        if not ticker:
            raise ValueError(
                f"未知大宗商品 Symbol: {symbol}，"
                f"支持: {[k for k in YFINANCE_TICKERS if k.startswith('COMM:')]}"
            )

        tk = yf.Ticker(ticker)
        df = tk.history(start=start_date, end=end_date, interval=interval)
        if df.empty:
            return pd.DataFrame()

        df = df.reset_index()
        date_col = "Date" if "Date" in df.columns else "Datetime"
        df = df.rename(columns={
            date_col: "datetime",
            "Open": "open", "High": "high", "Low": "low",
            "Close": "close", "Volume": "volume",
        })
        df["datetime"] = pd.to_datetime(df["datetime"], utc=True)
        df["volume_usd"] = df["close"] * df["volume"]
        return df[["datetime", "open", "high", "low", "close",
                    "volume", "volume_usd"]].reset_index(drop=True)

    @staticmethod
    def get_metal_spot_klines(
        symbol: str,
        start_date: str,
        end_date: str,
    ) -> pd.DataFrame:
        """贵金属现货 K 线 (yfinance 期货合约代理)。"""
        import yfinance as yf

        ticker = YFINANCE_TICKERS.get(symbol.upper())
        if not ticker:
            raise ValueError(
                f"未知贵金属 Symbol: {symbol}，支持: METAL:XAU-SPOT / METAL:XAG-SPOT"
            )

        tk = yf.Ticker(ticker)
        df = tk.history(start=start_date, end=end_date, interval="1d")
        if df.empty:
            return pd.DataFrame()

        df = df.reset_index()
        date_col = "Date" if "Date" in df.columns else "Datetime"
        df = df.rename(columns={
            date_col: "datetime",
            "Open": "open", "High": "high", "Low": "low",
            "Close": "close", "Volume": "volume",
        })
        df["datetime"] = pd.to_datetime(df["datetime"], utc=True)
        return df[["datetime", "open", "high", "low", "close",
                    "volume"]].reset_index(drop=True)

    # ════════════════════════════════════════
    #  DeFi Llama — 协议 TVL / 手续费
    # ════════════════════════════════════════

    def get_protocol_tvl(self, protocol: str) -> pd.DataFrame:
        """协议 TVL 历史 (DeFi Llama)。"""
        data = self._get(f"{DEFILLAMA_BASE}/api/protocol/{protocol}")
        tvl_history = data.get("tvl", [])
        if not tvl_history:
            return pd.DataFrame()

        df = pd.DataFrame(tvl_history)
        df["datetime"] = pd.to_datetime(df["date"], unit="s", utc=True)
        df["tvl_usd"] = df["totalLiquidityUSD"].astype(float)
        return df[["datetime", "tvl_usd"]].reset_index(drop=True)

    def get_protocol_info(self, protocol: str) -> dict:
        """协议当前信息（TVL、类别、链等）。"""
        data = self._get(f"{DEFILLAMA_BASE}/api/protocol/{protocol}")
        return {
            "name": data.get("name", ""),
            "category": data.get("category", ""),
            "chains": data.get("chains", []),
            "current_tvl": data.get("currentChainTvls", {}),
            "total_tvl": float(data.get("tvl", [{}])[-1].get("totalLiquidityUSD", 0))
            if data.get("tvl") else 0,
        }

    def get_defi_fees(self, protocol: str = None) -> pd.DataFrame:
        """协议手续费/收入数据 (24h/7d/30d)。"""
        data = self._get(f"{DEFILLAMA_BASE}/api/overview/fees")
        protocols = data.get("protocols", [])

        if protocol:
            protocols = [
                p for p in protocols
                if p.get("name", "").lower() == protocol.lower()
                or p.get("slug", "") == protocol.lower()
            ]

        if not protocols:
            return pd.DataFrame()

        rows = []
        for p in protocols:
            rows.append({
                "name": p.get("name", ""),
                "category": p.get("category", ""),
                "fees_24h": float(p.get("total24h", 0) or 0),
                "fees_7d": float(p.get("total7d", 0) or 0),
                "fees_30d": float(p.get("total30d", 0) or 0),
                "revenue_24h": float(p.get("revenue24h", 0) or 0),
            })
        return pd.DataFrame(rows)

    def list_defi_protocols(self) -> pd.DataFrame:
        """所有 DeFi 协议列表及 TVL（前 200 个）。"""
        data = self._get(f"{DEFILLAMA_BASE}/api/protocols")
        rows = []
        for p in data[:200]:
            rows.append({
                "name": p.get("name", ""),
                "slug": p.get("slug", ""),
                "category": p.get("category", ""),
                "chains": ", ".join(p.get("chains", [])),
                "tvl": float(p.get("tvl", 0) or 0),
            })
        return pd.DataFrame(rows)

    # ════════════════════════════════════════
    #  生命周期
    # ════════════════════════════════════════

    def close(self):
        self._client.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
