# backtest/strategy.py
# ──────────────────────────────────────────────────────────────────────────────
# 策略基類 + 範例策略
#
# 自訂策略只需繼承 BaseStrategy，實作 on_bar()：
#
#   class MyStrategy(BaseStrategy):
#       def on_bar(self, symbol, bar, ts):
#           ...
# ──────────────────────────────────────────────────────────────────────────────
from __future__ import annotations

from abc import ABC, abstractmethod
from collections import deque

import pandas as pd


class BaseStrategy(ABC):
    """
    策略基類。BacktestEngine 會在 run() 前把 portfolio 注入進來。

    可用的工具方法：
      self.ma(symbol, period)        → float | None
      self.position(symbol)          → int（持股股數）
      self.buy(symbol, shares, price, ts)
      self.sell(symbol, shares, price, ts)
    """

    def __init__(self):
        self.portfolio = None                       # 由 BacktestEngine 注入
        self._price_buf: dict[str, deque] = {}      # symbol → 收盤價滾動緩衝區

    # ── 技術指標工具 ──────────────────────────────────────────────────────────

    def _push(self, symbol: str, price: float, maxlen: int = 300):
        if symbol not in self._price_buf:
            self._price_buf[symbol] = deque(maxlen=maxlen)
        self._price_buf[symbol].append(price)

    def ma(self, symbol: str, period: int) -> float | None:
        """計算最近 period 根收盤價的簡單移動平均。資料不足時回傳 None。"""
        buf = self._price_buf.get(symbol)
        if buf is None or len(buf) < period:
            return None
        tail = list(buf)[-period:]
        return sum(tail) / period

    # ── 下單 helper ───────────────────────────────────────────────────────────

    def buy(self, symbol: str, shares: int, price: float, ts=None):
        self.portfolio.order(symbol, shares, price, ts)

    def sell(self, symbol: str, shares: int, price: float, ts=None):
        self.portfolio.order(symbol, -shares, price, ts)

    def position(self, symbol: str) -> int:
        return self.portfolio.positions.get(symbol, 0)

    # ── 子類必須實作 ──────────────────────────────────────────────────────────

    @abstractmethod
    def on_bar(self, symbol: str, bar: pd.Series, ts):
        """
        每根 K 棒呼叫一次。
        bar 欄位: open / high / low / close / volume
        成交假設: 呼叫 buy()/sell() 時以 bar['close'] 當根收盤立即成交。
        """


# ─────────────────────────────────────────────────────────────────────────────
# 範例策略：MA 均線交叉
# ─────────────────────────────────────────────────────────────────────────────

class MACrossStrategy(BaseStrategy):
    """
    快線上穿慢線 → 買進（全倉投入可買股數）
    快線下穿慢線 → 賣出全部持股

    參數：
      fast_period    : 快線週期（預設 5）
      slow_period    : 慢線週期（預設 20）
      shares_per_trade: 每次買進股數（預設 1000 股 = 1 張）
    """

    def __init__(
        self,
        fast_period: int = 5,
        slow_period: int = 20,
        shares_per_trade: int = 1000,
    ):
        super().__init__()
        self.fast_period = fast_period
        self.slow_period = slow_period
        self.shares_per_trade = shares_per_trade

        # 記錄上一根的 MA 值，用來偵測穿越（cross）
        self._prev_fast: dict[str, float | None] = {}
        self._prev_slow: dict[str, float | None] = {}

    def on_bar(self, symbol: str, bar: pd.Series, ts):
        self._push(symbol, bar['close'], maxlen=self.slow_period + 10)

        fast = self.ma(symbol, self.fast_period)
        slow = self.ma(symbol, self.slow_period)

        if fast is None or slow is None:
            return

        prev_fast = self._prev_fast.get(symbol)
        prev_slow = self._prev_slow.get(symbol)
        self._prev_fast[symbol] = fast
        self._prev_slow[symbol] = slow

        if prev_fast is None or prev_slow is None:
            return

        price = bar['close']

        # 黃金交叉：快線由下往上穿越慢線 → 買進
        if prev_fast <= prev_slow and fast > slow and self.position(symbol) == 0:
            self.buy(symbol, self.shares_per_trade, price, ts)
            print(
                f"  ▲ BUY  {symbol} x{self.shares_per_trade:,} @ {price:.2f}"
                f"  MA{self.fast_period}={fast:.2f} > MA{self.slow_period}={slow:.2f}"
                f"  [{ts}]"
            )

        # 死亡交叉：快線由上往下穿越慢線 → 全出
        elif prev_fast >= prev_slow and fast < slow and self.position(symbol) > 0:
            pos = self.position(symbol)
            self.sell(symbol, pos, price, ts)
            print(
                f"  ▼ SELL {symbol} x{pos:,} @ {price:.2f}"
                f"  MA{self.fast_period}={fast:.2f} < MA{self.slow_period}={slow:.2f}"
                f"  [{ts}]"
            )
