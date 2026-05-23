# backtest/engine.py
# ──────────────────────────────────────────────────────────────────────────────
# 回測引擎：逐根 K 棒驅動策略、追蹤部位與資金、輸出績效報告
# ──────────────────────────────────────────────────────────────────────────────
from __future__ import annotations

import pandas as pd


class Portfolio:
    """
    部位與資金管理。
    成交假設：呼叫 order() 時以傳入的 price 立即成交（當根收盤價）。
    台股手續費：買賣各 0.1425%；賣出另加交易稅 0.3%。
    """

    BUY_COMMISSION  = 0
    SELL_COMMISSION = 0

    def __init__(self, initial_capital: float):
        self.initial_capital = initial_capital
        self.cash: float = initial_capital
        self.positions: dict[str, int] = {}     # symbol → 持股張數（股數）
        self.trades: list[dict] = []
        self.equity_curve: list[dict] = []

    # ── 下單 ──────────────────────────────────────────────────────────────────

    def order(self, symbol: str, shares: int, price: float, ts=None):
        """
        shares > 0：買進；shares < 0：賣出。
        買進若資金不足，自動縮減至可買量；賣出不能超過持股。
        """
        if shares == 0 or price <= 0:
            return

        if shares > 0:
            cost_per_share = price * (1 + self.BUY_COMMISSION)
            affordable = int(self.cash / cost_per_share)
            shares = min(shares, affordable)
            if shares == 0:
                return
            self.cash -= shares * cost_per_share

        else:  # shares < 0
            owned = self.positions.get(symbol, 0)
            shares = max(shares, -owned)        # 不得放空
            if shares == 0:
                return
            self.cash += abs(shares) * price * (1 - self.SELL_COMMISSION)

        prev = self.positions.get(symbol, 0)
        self.positions[symbol] = prev + shares

        self.trades.append({
            'ts':     ts,
            'symbol': symbol,
            'side':   'buy' if shares > 0 else 'sell',
            'shares': abs(shares),
            'price':  price,
        })

    # ── 市值快照 ──────────────────────────────────────────────────────────────

    def mark_equity(self, ts, market_prices: dict):
        pos_value = sum(
            self.positions.get(sym, 0) * px
            for sym, px in market_prices.items()
        )
        self.equity_curve.append({'ts': ts, 'equity': self.cash + pos_value})


# ─────────────────────────────────────────────────────────────────────────────

class BacktestEngine:
    """
    事件驅動回測引擎。

    data: {symbol: DataFrame}，DataFrame 以 ts 為 index，
          欄位 open / high / low / close / volume。
    """

    def __init__(self, data: dict, initial_capital: float = 1_000_000):
        self.data = data
        self.portfolio = Portfolio(initial_capital)

    def run(self, strategy) -> dict:
        strategy.portfolio = self.portfolio

        # 將所有標的的 K 棒合併，按時間排序
        events: list[tuple] = []
        for symbol, df in self.data.items():
            for ts, row in df.iterrows():
                events.append((ts, symbol, row))
        events.sort(key=lambda x: x[0])

        current_prices: dict[str, float] = {}
        prev_ts = None

        for ts, symbol, bar in events:
            # 新時間點：先對上一個 ts 做市值快照
            if ts != prev_ts and prev_ts is not None:
                self.portfolio.mark_equity(prev_ts, current_prices.copy())
            current_prices[symbol] = bar['close']

            strategy.on_bar(symbol, bar, ts)
            prev_ts = ts

        # 最後一根
        if prev_ts is not None:
            self.portfolio.mark_equity(prev_ts, current_prices)

        return self._report()

    # ── 績效計算 ──────────────────────────────────────────────────────────────

    def _report(self) -> dict:
        curve  = pd.DataFrame(self.portfolio.equity_curve).set_index('ts')
        trades = (pd.DataFrame(self.portfolio.trades)
                  if self.portfolio.trades else pd.DataFrame())

        initial = self.portfolio.initial_capital
        final   = curve['equity'].iloc[-1] if not curve.empty else initial
        total_return = (final - initial) / initial * 100

        # 最大回撤
        rolling_max  = curve['equity'].cummax()
        drawdown_pct = (curve['equity'] - rolling_max) / rolling_max * 100
        max_drawdown = drawdown_pct.min()

        # 勝率
        win_rate = None
        if not trades.empty and 'side' in trades.columns:
            sell_trades = trades[trades['side'] == 'sell']
            # 簡易勝率：賣出金額 > 對應買入金額的比例（多標的時為近似值）
            win_rate = None  # 需要配對買賣才能精確計算，保留欄位供後續擴充

        print(f"\n{'═'*42}")
        print(f"  回測結果摘要")
        print(f"{'═'*42}")
        print(f"  初始資金    : {initial:>15,.0f} 元")
        print(f"  最終資產    : {final:>15,.0f} 元")
        print(f"  總報酬率    : {total_return:>14.2f} %")
        print(f"  最大回撤    : {max_drawdown:>14.2f} %")
        print(f"  交易筆數    : {len(trades):>15,}")
        print(f"{'═'*42}\n")

        return {
            'equity_curve':      curve,
            'trades':            trades,
            'total_return_pct':  total_return,
            'max_drawdown_pct':  max_drawdown,
        }
