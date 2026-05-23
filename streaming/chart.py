# streaming/chart.py
# ──────────────────────────────────────────────────────────────────────────────
# 即時行情圖表
# - Tick  模式：1 分鐘蠟燭圖 + MA5/MA10/MA20 + 成交量
# - BidAsk 模式：Bid1/Ask1 折線圖 + MA 參考線 + 委買委賣量能
# 使用 FuncAnimation 在主執行緒定時刷新，Callback 資料透過 manager 存取
# ──────────────────────────────────────────────────────────────────────────────
from typing import Optional

import numpy as np
import pandas as pd
import matplotlib
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
from matplotlib.animation import FuncAnimation

# 若 matplotlib 後端無法自動選擇，可手動指定，例如：
# matplotlib.use('TkAgg')

# ── 深色主題色碼 ──────────────────────────────────────────────────────────────
_BG      = '#131722'
_AX_BG   = '#1e222d'
_TEXT    = '#d1d4dc'
_GRID    = '#2a2e39'
_UP      = '#26a69a'   # 上漲（台股收紅）
_DOWN    = '#ef5350'   # 下跌（台股收綠）
_BID     = '#26a69a'
_ASK     = '#ef5350'
_MA_CLR  = ['#f5c518', '#4fc3f7', '#ce93d8']   # MA5 / MA10 / MA20


class StreamingChart:
    """
    即時行情圖表。
    建立後呼叫 start(manager) 啟動 FuncAnimation（阻塞主執行緒直到視窗關閉）。
    """

    def __init__(
        self,
        symbols: list,
        warmup_data: dict,
        data_type: str,
        ma_periods: list = None,
        chart_window: int = 60,
        interval_ms: int = 500,
    ):
        self.symbols      = symbols
        self.warmup_data  = warmup_data
        self.data_type    = data_type
        self.ma_periods   = ma_periods or [5, 10, 20]
        self.chart_window = chart_window
        self.interval_ms  = interval_ms
        self.manager      = None
        self._ani         = None

        self._build_figure()

    # ── 建立視窗 ──────────────────────────────────────────────────────────────

    def _build_figure(self):
        n = len(self.symbols)

        plt.rcParams.update({
            'figure.facecolor':  _BG,
            'axes.facecolor':    _AX_BG,
            'axes.edgecolor':    _GRID,
            'axes.labelcolor':   _TEXT,
            'xtick.color':       _TEXT,
            'ytick.color':       _TEXT,
            'text.color':        _TEXT,
            'grid.color':        _GRID,
            'grid.linewidth':    0.4,
        })

        # 每個標的佔 2 列：price(比例3) + lower(比例1)
        height_ratios = [3, 1] * n
        self.fig = plt.figure(
            figsize=(16, max(4 * n, 5)),
            facecolor=_BG,
        )
        title = f"即時行情監控 ── {'Tick (1 分 K)' if self.data_type == 'tick' else 'BidAsk'}"
        self.fig.suptitle(title, color=_TEXT, fontsize=11, y=0.99)

        gs = gridspec.GridSpec(
            n * 2, 1,
            height_ratios=height_ratios,
            hspace=0.08,
            top=0.97, bottom=0.04,
        )

        self.ax_price: dict[str, plt.Axes] = {}
        self.ax_lower: dict[str, plt.Axes] = {}

        for i, sym in enumerate(self.symbols):
            ax_p = self.fig.add_subplot(gs[i * 2])
            ax_v = self.fig.add_subplot(gs[i * 2 + 1], sharex=ax_p)
            self.ax_price[sym] = ax_p
            self.ax_lower[sym] = ax_v

    # ── 啟動動畫 ──────────────────────────────────────────────────────────────

    def start(self, manager):
        """
        傳入 StreamingManager，啟動 FuncAnimation。
        此呼叫會阻塞主執行緒（plt.show block=True），直到使用者關閉視窗。
        """
        self.manager = manager
        self._ani = FuncAnimation(
            self.fig,
            self._update,
            interval=self.interval_ms,
            blit=False,
            cache_frame_data=False,
        )
        plt.show(block=True)

    # ── 每幀更新 ──────────────────────────────────────────────────────────────

    def _update(self, _frame):
        if self.manager is None:
            return
        for sym in self.symbols:
            if self.data_type == "tick":
                self._draw_tick(sym)
            else:
                self._draw_bidask(sym)

    # ── Tick / 蠟燭圖 ─────────────────────────────────────────────────────────

    def _draw_tick(self, symbol: str):
        ax_p = self.ax_price[symbol]
        ax_v = self.ax_lower[symbol]
        ax_p.cla()
        ax_v.cla()
        self._apply_theme(ax_p, ax_v)

        # 取得資料
        history = self.manager.get_kbar_history(symbol)
        cur     = self.manager.get_current_kbar(symbol)

        if history.empty and cur is None:
            ax_p.set_title(f"{symbol}  等待行情...", color=_TEXT, fontsize=9, loc='left', pad=3)
            return

        # 組合顯示用 DataFrame（歷史尾段 + 當前未完成 K 棒）
        tail_n = self.chart_window - (1 if cur else 0)
        df = history.tail(max(tail_n, 0)).copy()
        if cur:
            df = pd.concat(
                [df, pd.DataFrame([{
                    'ts': cur['ts'], 'open': cur['open'],
                    'high': cur['high'], 'low': cur['low'],
                    'close': cur['close'], 'volume': cur['volume'],
                }])],
                ignore_index=True,
            )

        if df.empty:
            return

        n      = len(df)
        xs     = np.arange(n)
        opens  = df['open'].values.astype(float)
        highs  = df['high'].values.astype(float)
        lows   = df['low'].values.astype(float)
        closes = df['close'].values.astype(float)
        vols   = df['volume'].values.astype(float)

        # ── 蠟燭主體 + 影線 ──────────────────────────────────────────
        for i in range(n):
            color  = _UP if closes[i] >= opens[i] else _DOWN
            body_b = min(opens[i], closes[i])
            body_h = abs(closes[i] - opens[i]) or closes[i] * 0.0002
            ax_p.bar(i, body_h, bottom=body_b, color=color, width=0.6, alpha=0.9)
            ax_p.plot([i, i], [lows[i], highs[i]], color=color, linewidth=0.8)

        # ── 均線（用完整歷史計算，只顯示最後 n 個值）──────────────────
        full_closes = self._full_closes(history, cur)
        for period, color in zip(self.ma_periods, _MA_CLR):
            if len(full_closes) >= period:
                ma      = pd.Series(full_closes).rolling(period).mean().values
                ma_disp = ma[-n:]
                mask    = ~np.isnan(ma_disp)
                if mask.any():
                    ax_p.plot(
                        xs[mask], ma_disp[mask],
                        color=color, linewidth=1.2,
                        label=f'MA{period}', zorder=5,
                    )

        # ── 標題 & 圖例 ───────────────────────────────────────────────
        ax_p.set_title(
            f"{symbol}    C: {closes[-1]:.2f}    O: {opens[-1]:.2f}"
            f"    H: {highs[-1]:.2f}    L: {lows[-1]:.2f}",
            color=_TEXT, fontsize=8, loc='left', pad=3,
        )
        legend = ax_p.legend(
            loc='upper left', fontsize=7,
            facecolor=_AX_BG, edgecolor=_GRID, labelcolor=_TEXT,
        )

        # ── 成交量 ───────────────────────────────────────────────────
        for i in range(n):
            color = _UP if closes[i] >= opens[i] else _DOWN
            ax_v.bar(i, vols[i], color=color, width=0.6, alpha=0.8)
        ax_v.set_ylabel('Vol', color=_TEXT, fontsize=7, labelpad=2)

        # ── X 軸時間標籤（只在 ax_v 顯示）────────────────────────────
        self._set_time_ticks(ax_p, ax_v, df['ts'].tolist(), n)

    # ── BidAsk 折線圖 ─────────────────────────────────────────────────────────

    def _draw_bidask(self, symbol: str):
        ax_p = self.ax_price[symbol]
        ax_v = self.ax_lower[symbol]
        ax_p.cla()
        ax_v.cla()
        self._apply_theme(ax_p, ax_v)

        records = self.manager.get_bidask_records(symbol)

        if not records:
            ax_p.set_title(f"{symbol}  等待行情...", color=_TEXT, fontsize=9, loc='left', pad=3)
            return

        # 取最近 300 筆顯示
        recent   = records[-300:]
        n        = len(recent)
        xs       = np.arange(n)
        bids     = [r['bid']     for r in recent]
        asks     = [r['ask']     for r in recent]
        bid_vols = [r['bid_vol'] for r in recent]
        ask_vols = [r['ask_vol'] for r in recent]

        # ── Bid1 / Ask1 線 ────────────────────────────────────────────
        ax_p.plot(xs, bids, color=_BID, linewidth=1.2, label='Bid1')
        ax_p.plot(xs, asks, color=_ASK, linewidth=1.2, label='Ask1')
        ax_p.fill_between(xs, bids, asks, alpha=0.12, color='#888888')

        # ── MA 參考水平線（來自 warmup 歷史 K 棒最後一個值）─────────────
        history = self.manager.get_kbar_history(symbol)
        if not history.empty:
            all_closes = history['close'].values.astype(float)
            for period, color in zip(self.ma_periods, _MA_CLR):
                if len(all_closes) >= period:
                    ma_val = pd.Series(all_closes).rolling(period).mean().iloc[-1]
                    if not np.isnan(ma_val):
                        ax_p.axhline(
                            ma_val, color=color, linewidth=0.9,
                            linestyle='--', alpha=0.85,
                            label=f'MA{period} {ma_val:.2f}',
                        )

        # ── 標題 & 圖例 ───────────────────────────────────────────────
        last_bid = bids[-1]
        last_ask = asks[-1]
        spread   = (last_ask - last_bid) if last_bid and last_ask else 0
        ax_p.set_title(
            f"{symbol}    Bid: {last_bid}    Ask: {last_ask}    Spread: {spread:.2f}",
            color=_TEXT, fontsize=8, loc='left', pad=3,
        )
        ax_p.legend(
            loc='upper left', fontsize=7,
            facecolor=_AX_BG, edgecolor=_GRID, labelcolor=_TEXT,
        )

        # ── 買賣量能（委買量為負方向，視覺化掛單壓力）────────────────
        ax_v.bar(xs,  ask_vols,             color=_ASK, alpha=0.7, width=0.8, label='Ask 量')
        ax_v.bar(xs, [-v for v in bid_vols], color=_BID, alpha=0.7, width=0.8, label='Bid 量')
        ax_v.axhline(0, color=_GRID, linewidth=0.6)
        ax_v.set_ylabel('委量', color=_TEXT, fontsize=7, labelpad=2)
        ax_v.legend(
            loc='upper left', fontsize=6,
            facecolor=_AX_BG, edgecolor=_GRID, labelcolor=_TEXT,
        )

        # ── X 軸時間標籤 ──────────────────────────────────────────────
        tss = [r['ts'] for r in recent]
        self._set_time_ticks(ax_p, ax_v, tss, n, fmt=slice(11, 19))

    # ── 共用工具 ──────────────────────────────────────────────────────────────

    def _apply_theme(self, ax_p: plt.Axes, ax_v: plt.Axes):
        """cla() 後重新套用深色主題樣式。"""
        for ax in (ax_p, ax_v):
            ax.set_facecolor(_AX_BG)
            ax.tick_params(colors=_TEXT, labelsize=7)
            ax.yaxis.set_tick_params(labelright=True, labelleft=False)
            ax.grid(True, color=_GRID, linewidth=0.4, alpha=0.6)
            for spine in ax.spines.values():
                spine.set_edgecolor(_GRID)
        ax_p.tick_params(labelbottom=False)

    def _set_time_ticks(
        self,
        ax_p: plt.Axes,
        ax_v: plt.Axes,
        timestamps: list,
        n: int,
        fmt=slice(11, 16),
    ):
        """在 ax_v 設定 X 軸時間刻度標籤，ax_p 隱藏底部刻度。"""
        tick_step = max(1, n // 8)
        tick_pos  = list(range(0, n, tick_step))
        labels    = [str(timestamps[i])[fmt] for i in tick_pos if i < len(timestamps)]
        ax_v.set_xticks(tick_pos[:len(labels)])
        ax_v.set_xticklabels(labels, fontsize=7, color=_TEXT)
        ax_p.tick_params(labelbottom=False)

    @staticmethod
    def _full_closes(history: pd.DataFrame, cur: Optional[dict]) -> np.ndarray:
        """合併歷史 + 當前未完成 K 棒的收盤價陣列，供 MA 計算用。"""
        if history.empty:
            closes = np.array([])
        else:
            closes = history['close'].values.astype(float)
        if cur:
            closes = np.append(closes, float(cur['close']))
        return closes
