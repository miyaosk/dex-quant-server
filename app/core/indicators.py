"""
技术指标库 — 纯 NumPy 向量化计算

支持指标:
  SMA, EMA, RSI, MACD, 布林带, ATR, KDJ, 成交量均线

所有方法接受 numpy 数组，返回 numpy 数组。
无 pandas 依赖，可直接嵌入回测循环。
"""

from __future__ import annotations

import numpy as np


class Indicators:
    """向量化技术指标计算器。"""

    # ════════════════════════════════════════
    #  均线类
    # ════════════════════════════════════════

    @staticmethod
    def sma(data: np.ndarray, period: int) -> np.ndarray:
        """简单移动平均线 (SMA)。前 period-1 个值为 NaN。"""
        if len(data) < period:
            return np.full_like(data, np.nan, dtype=float)

        result = np.full(len(data), np.nan, dtype=float)
        cumsum = np.cumsum(data, dtype=float)
        result[period - 1:] = (cumsum[period - 1:] - np.concatenate(([0], cumsum[:-period]))) / period
        return result

    @staticmethod
    def ema(data: np.ndarray, period: int) -> np.ndarray:
        """指数移动平均线 (EMA)。α = 2 / (period + 1)。"""
        if len(data) < period:
            return np.full_like(data, np.nan, dtype=float)

        alpha = 2.0 / (period + 1)
        result = np.full(len(data), np.nan, dtype=float)
        result[period - 1] = np.mean(data[:period])

        for i in range(period, len(data)):
            result[i] = alpha * data[i] + (1 - alpha) * result[i - 1]

        return result

    @staticmethod
    def volume_ma(volume: np.ndarray, period: int) -> np.ndarray:
        """成交量移动平均线。"""
        return Indicators.sma(volume, period)

    # ════════════════════════════════════════
    #  动量/震荡类
    # ════════════════════════════════════════

    @staticmethod
    def rsi(data: np.ndarray, period: int = 14) -> np.ndarray:
        """相对强弱指标 (RSI)，Wilder 平滑法。返回 0-100，前 period 个值为 NaN。"""
        if len(data) < period + 1:
            return np.full_like(data, np.nan, dtype=float)

        deltas = np.diff(data)
        gains = np.where(deltas > 0, deltas, 0.0)
        losses = np.where(deltas < 0, -deltas, 0.0)

        result = np.full(len(data), np.nan, dtype=float)
        avg_gain = np.mean(gains[:period])
        avg_loss = np.mean(losses[:period])

        if avg_loss == 0:
            result[period] = 100.0
        else:
            rs = avg_gain / avg_loss
            result[period] = 100.0 - 100.0 / (1.0 + rs)

        for i in range(period, len(deltas)):
            avg_gain = (avg_gain * (period - 1) + gains[i]) / period
            avg_loss = (avg_loss * (period - 1) + losses[i]) / period
            if avg_loss == 0:
                result[i + 1] = 100.0
            else:
                rs = avg_gain / avg_loss
                result[i + 1] = 100.0 - 100.0 / (1.0 + rs)

        return result

    @staticmethod
    def macd(
        data: np.ndarray,
        fast_period: int = 12,
        slow_period: int = 26,
        signal_period: int = 9,
    ) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
        """MACD 指标。返回 (macd_line, signal_line, histogram)。"""
        ema_fast = Indicators.ema(data, fast_period)
        ema_slow = Indicators.ema(data, slow_period)

        macd_line = ema_fast - ema_slow

        valid_start = slow_period - 1
        signal_line = np.full(len(data), np.nan, dtype=float)
        valid_macd = macd_line[valid_start:]

        if len(valid_macd) >= signal_period:
            signal_ema = Indicators.ema(valid_macd, signal_period)
            signal_line[valid_start:] = signal_ema

        histogram = (macd_line - signal_line) * 2
        return macd_line, signal_line, histogram

    @staticmethod
    def kdj(
        high: np.ndarray,
        low: np.ndarray,
        close: np.ndarray,
        k_period: int = 9,
        d_period: int = 3,
        j_smooth: int = 3,
    ) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
        """KDJ 随机指标。返回 (K, D, J)。"""
        n = len(close)
        rsv = np.full(n, np.nan, dtype=float)

        for i in range(k_period - 1, n):
            period_high = np.max(high[i - k_period + 1: i + 1])
            period_low = np.min(low[i - k_period + 1: i + 1])
            if period_high == period_low:
                rsv[i] = 50.0
            else:
                rsv[i] = (close[i] - period_low) / (period_high - period_low) * 100.0

        k_values = np.full(n, np.nan, dtype=float)
        d_values = np.full(n, np.nan, dtype=float)

        first_valid = k_period - 1
        k_values[first_valid] = rsv[first_valid]
        d_values[first_valid] = k_values[first_valid]

        for i in range(first_valid + 1, n):
            if np.isnan(rsv[i]):
                continue
            k_values[i] = (k_values[i - 1] * (d_period - 1) + rsv[i]) / d_period
            d_values[i] = (d_values[i - 1] * (j_smooth - 1) + k_values[i]) / j_smooth

        j_values = 3.0 * k_values - 2.0 * d_values
        return k_values, d_values, j_values

    # ════════════════════════════════════════
    #  波动率/通道类
    # ════════════════════════════════════════

    @staticmethod
    def bollinger_bands(
        data: np.ndarray,
        period: int = 20,
        num_std: float = 2.0,
    ) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
        """布林带。返回 (upper, middle, lower)。"""
        middle = Indicators.sma(data, period)

        std = np.full(len(data), np.nan, dtype=float)
        for i in range(period - 1, len(data)):
            std[i] = np.std(data[i - period + 1: i + 1], ddof=0)

        upper = middle + num_std * std
        lower = middle - num_std * std
        return upper, middle, lower

    @staticmethod
    def atr(
        high: np.ndarray,
        low: np.ndarray,
        close: np.ndarray,
        period: int = 14,
    ) -> np.ndarray:
        """平均真实波幅 (ATR)。"""
        n = len(close)
        tr = np.full(n, np.nan, dtype=float)
        tr[0] = high[0] - low[0]

        for i in range(1, n):
            hl = high[i] - low[i]
            hc = abs(high[i] - close[i - 1])
            lc = abs(low[i] - close[i - 1])
            tr[i] = max(hl, hc, lc)

        atr_values = np.full(n, np.nan, dtype=float)
        if n >= period:
            atr_values[period - 1] = np.mean(tr[:period])
            for i in range(period, n):
                atr_values[i] = (atr_values[i - 1] * (period - 1) + tr[i]) / period

        return atr_values

    # ════════════════════════════════════════
    #  辅助方法
    # ════════════════════════════════════════

    @staticmethod
    def crossover(series_a: np.ndarray, series_b: np.ndarray) -> np.ndarray:
        """金叉判断: series_a 从下方穿越 series_b。"""
        result = np.zeros(len(series_a), dtype=bool)
        for i in range(1, len(series_a)):
            if (np.isnan(series_a[i]) or np.isnan(series_b[i]) or
                    np.isnan(series_a[i - 1]) or np.isnan(series_b[i - 1])):
                continue
            result[i] = (series_a[i - 1] <= series_b[i - 1]) and (series_a[i] > series_b[i])
        return result

    @staticmethod
    def crossunder(series_a: np.ndarray, series_b: np.ndarray) -> np.ndarray:
        """死叉判断: series_a 从上方穿越 series_b。"""
        result = np.zeros(len(series_a), dtype=bool)
        for i in range(1, len(series_a)):
            if (np.isnan(series_a[i]) or np.isnan(series_b[i]) or
                    np.isnan(series_a[i - 1]) or np.isnan(series_b[i - 1])):
                continue
            result[i] = (series_a[i - 1] >= series_b[i - 1]) and (series_a[i] < series_b[i])
        return result

    @staticmethod
    def highest(data: np.ndarray, period: int) -> np.ndarray:
        """滚动最高值。"""
        result = np.full(len(data), np.nan, dtype=float)
        for i in range(period - 1, len(data)):
            result[i] = np.max(data[i - period + 1: i + 1])
        return result

    @staticmethod
    def lowest(data: np.ndarray, period: int) -> np.ndarray:
        """滚动最低值。"""
        result = np.full(len(data), np.nan, dtype=float)
        for i in range(period - 1, len(data)):
            result[i] = np.min(data[i - period + 1: i + 1])
        return result

    @staticmethod
    def pct_change(data: np.ndarray, period: int = 1) -> np.ndarray:
        """百分比变化率。"""
        result = np.full(len(data), np.nan, dtype=float)
        for i in range(period, len(data)):
            if data[i - period] != 0:
                result[i] = (data[i] - data[i - period]) / data[i - period]
        return result
