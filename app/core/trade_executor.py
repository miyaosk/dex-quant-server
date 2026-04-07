"""
Hyperliquid 交易执行器

服务器端监控产生信号后，用 vault 中的加密私钥自动下单。
使用 hyperliquid-python-sdk 官方 SDK。

流程:
  1. vault 解密用户私钥
  2. 构建 Exchange 实例
  3. 根据信号执行市价单
  4. 返回执行结果
"""

from __future__ import annotations

from typing import Optional

import eth_account
from eth_account.signers.local import LocalAccount
from hyperliquid.exchange import Exchange
from hyperliquid.info import Info
from hyperliquid.utils import constants
from loguru import logger


class HyperliquidExecutor:
    """Hyperliquid 交易执行器"""

    def __init__(self, private_key: str, network: str = "mainnet"):
        self._network = network
        base_url = constants.MAINNET_API_URL if network == "mainnet" else constants.TESTNET_API_URL

        account: LocalAccount = eth_account.Account.from_key(private_key)
        self._address = account.address
        self._info = Info(base_url, skip_ws=True)
        self._exchange = Exchange(account, base_url)

        logger.info(f"HyperliquidExecutor | {network} | {self._address[:8]}...{self._address[-4:]}")

    def get_account_value(self) -> float:
        state = self._info.user_state(self._address)
        return float(state["marginSummary"]["accountValue"])

    def get_positions(self) -> list[dict]:
        state = self._info.user_state(self._address)
        return [p["position"] for p in state.get("assetPositions", [])]

    def has_position(self, coin: str) -> bool:
        for pos in self.get_positions():
            if pos.get("coin", "").upper() == coin.upper():
                if float(pos.get("szi", 0)) != 0:
                    return True
        return False

    def _normalize_coin(self, symbol: str) -> str:
        """BTCUSDT → BTC, SOLUSDT → SOL"""
        s = symbol.upper()
        for suffix in ("USDT", "USDC", "USD", "PERP"):
            if s.endswith(suffix):
                return s[: -len(suffix)]
        return s

    def _get_price(self, coin: str) -> float:
        all_mids = self._info.all_mids()
        return float(all_mids.get(coin, 0))

    def execute_signal(
        self,
        symbol: str,
        action: str,
        direction: str = "long",
        confidence: float = 0.7,
        max_position_pct: float = 10.0,
        max_concurrent: int = 3,
    ) -> dict:
        """
        执行交易信号。

        返回: {"status": "executed"/"skipped"/"error", "details": ...}
        """
        coin = self._normalize_coin(symbol)
        action = action.lower()

        try:
            account_value = self.get_account_value()
            if account_value <= 0:
                return {"status": "skipped", "reason": "account_value <= 0"}

            current_positions = self.get_positions()
            active_count = sum(1 for p in current_positions if float(p.get("szi", 0)) != 0)

            if action == "buy":
                if active_count >= max_concurrent:
                    return {"status": "skipped", "reason": f"max_concurrent ({max_concurrent}) reached"}

                price = self._get_price(coin)
                if price <= 0:
                    return {"status": "error", "reason": f"cannot get price for {coin}"}

                position_usd = account_value * (max_position_pct / 100)
                size = round(position_usd / price, 6)
                if size <= 0:
                    return {"status": "skipped", "reason": "calculated size <= 0"}

                is_buy = direction.lower() != "short"

                logger.info(f"EXECUTE | {coin} {'BUY' if is_buy else 'SELL'} {size} @ market | ${position_usd:.0f}")
                result = self._exchange.market_open(coin, is_buy, size)

                return {
                    "status": "executed",
                    "action": "buy",
                    "coin": coin,
                    "side": "long" if is_buy else "short",
                    "size": size,
                    "price": price,
                    "position_usd": position_usd,
                    "result": result,
                }

            elif action == "sell":
                has_pos = self.has_position(coin)
                if not has_pos:
                    return {"status": "skipped", "reason": f"no position in {coin}"}

                for pos in current_positions:
                    if pos.get("coin", "").upper() == coin:
                        size = abs(float(pos.get("szi", 0)))
                        is_buy = float(pos.get("szi", 0)) < 0

                        logger.info(f"EXECUTE | {coin} CLOSE {size} @ market")
                        result = self._exchange.market_close(coin)

                        return {
                            "status": "executed",
                            "action": "close",
                            "coin": coin,
                            "size": size,
                            "result": result,
                        }

                return {"status": "skipped", "reason": f"position not found for {coin}"}

            else:
                return {"status": "skipped", "reason": f"unknown action: {action}"}

        except Exception as e:
            logger.error(f"Trade execution failed | {coin} {action} | {e}")
            return {"status": "error", "reason": str(e)}
