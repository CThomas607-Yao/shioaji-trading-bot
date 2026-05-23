# streaming/manager.py
# ──────────────────────────────────────────────────────────────────────────────
# 即時行情管理器
# - Tick  → 組裝 1 分鐘 K 棒 → 圖表資料 / 非同步寫 DB
# - BidAsk → 記錄最佳買賣一檔 → 圖表資料 / 非同步寫 DB
# DB 寫入使用獨立背景執行緒，確保 Callback 不被 I/O 阻塞
# ──────────────────────────────────────────────────────────────────────────────
import queue
import sqlite3
import threading
from typing import Optional

import pandas as pd
import shioaji as sj


class StreamingManager:

    def __init__(
        self,
        api: sj.Shioaji,
        db_path: str,
        warmup_data: dict,
        data_type: str,
    ):
        self.api       = api
        self.db_path   = db_path
        self.data_type = data_type
        self._symbols  = list(warmup_data.keys())

        # ── 共享狀態（主執行緒讀、Callback 執行緒寫，需加鎖）────────────────
        self._lock = threading.Lock()

        # 已完成 K 棒（warmup 歷史 + 串流中完成的 K 棒）
        self._kbar_history: dict[str, pd.DataFrame] = {
            s: df.copy() for s, df in warmup_data.items()
        }
        # 當前正在組裝的未完成 K 棒（每分鐘末寫入 history 並歸零）
        self._current_kbar: dict[str, Optional[dict]] = {s: None for s in self._symbols}

        # BidAsk 紀錄列表（最多保留 2000 筆）
        self._bidask_records: dict[str, list] = {s: [] for s in self._symbols}

        # ── 非同步 DB 寫入佇列 ────────────────────────────────────────────────
        self._db_queue  = queue.Queue()
        self._db_thread = threading.Thread(
            target=self._db_worker_loop, daemon=True, name="DBWriter"
        )
        self._db_thread.start()

        # ── 初始化資料庫表格 ──────────────────────────────────────────────────
        self._init_db()

        # ── 斷線重連記錄 ──────────────────────────────────────────────────────
        self.active_subscriptions: list = []

        # ── 綁定 Callback（現貨 + 期貨）─────────────────────────────────────
        self.api.quote.set_on_tick_stk_v1_callback(self._cb_tick_stk)
        self.api.quote.set_on_bidask_stk_v1_callback(self._cb_bidask_stk)
        self.api.quote.set_on_tick_fop_v1_callback(self._cb_tick_fop)
        self.api.quote.set_on_bidask_fop_v1_callback(self._cb_bidask_fop)
        self.api.set_order_callback(self._cb_event)

    # ── 公開介面（供圖表執行緒讀取）──────────────────────────────────────────

    def subscribe_all(self, contracts: list):
        """訂閱所有合約的即時行情。"""
        q_type = (
            sj.constant.QuoteType.Tick
            if self.data_type == "tick"
            else sj.constant.QuoteType.BidAsk
        )
        for contract in contracts:
            print(f"  訂閱 {contract.code} [{self.data_type.upper()}]")
            self.api.quote.subscribe(
                contract,
                quote_type=q_type,
                version=sj.constant.QuoteVersion.v1,
            )
            self.active_subscriptions.append({"contract": contract, "quote_type": q_type})

    def get_kbar_history(self, symbol: str) -> pd.DataFrame:
        """回傳該標的的歷史 K 棒 DataFrame（執行緒安全拷貝）。"""
        with self._lock:
            df = self._kbar_history.get(symbol)
            return df.copy() if df is not None else pd.DataFrame()

    def get_current_kbar(self, symbol: str) -> Optional[dict]:
        """回傳當前分鐘尚未完成的 K 棒（執行緒安全拷貝），無則回傳 None。"""
        with self._lock:
            buf = self._current_kbar.get(symbol)
            return dict(buf) if buf else None

    def get_bidask_records(self, symbol: str) -> list:
        """回傳該標的最近的 BidAsk 紀錄列表（執行緒安全拷貝）。"""
        with self._lock:
            return list(self._bidask_records.get(symbol, []))

    # ── Callback（由 Shioaji 背景執行緒呼叫）────────────────────────────────

    def _cb_tick_stk(self, exchange: sj.Exchange, tick: sj.TickSTKv1):
        self._process_tick(tick.code, float(tick.close), int(tick.volume), tick.datetime)

    def _cb_tick_fop(self, exchange: sj.Exchange, tick: sj.TickFOPv1):
        self._process_tick(tick.code, float(tick.close), int(tick.volume), tick.datetime)

    def _cb_bidask_stk(self, exchange: sj.Exchange, bidask: sj.BidAskSTKv1):
        bid = float(bidask.bid_price[0]) if bidask.bid_price else None
        ask = float(bidask.ask_price[0]) if bidask.ask_price else None
        bv  = int(bidask.bid_volume[0])  if bidask.bid_volume else 0
        av  = int(bidask.ask_volume[0])  if bidask.ask_volume else 0
        self._process_bidask(bidask.code, bidask.datetime, bid, ask, bv, av)

    def _cb_bidask_fop(self, exchange: sj.Exchange, bidask: sj.BidAskFOPv1):
        bid = float(bidask.bid_price[0]) if bidask.bid_price else None
        ask = float(bidask.ask_price[0]) if bidask.ask_price else None
        bv  = int(bidask.bid_volume[0])  if bidask.bid_volume else 0
        av  = int(bidask.ask_volume[0])  if bidask.ask_volume else 0
        self._process_bidask(bidask.code, bidask.datetime, bid, ask, bv, av)

    # ── 核心處理邏輯 ──────────────────────────────────────────────────────────

    def _process_tick(self, code: str, price: float, volume: int, dt):
        """將即時 Tick 組裝成 1 分鐘 K 棒；分鐘結束時寫入 history 並推送 DB。"""
        if code not in self._symbols:
            return

        bar_time = dt.replace(second=0, microsecond=0)

        with self._lock:
            buf = self._current_kbar[code]

            if buf is None or buf['ts'] != bar_time:
                # 本分鐘已結束 → 把完成的 K 棒加入 history
                if buf is not None:
                    completed = dict(buf)
                    new_row = pd.DataFrame([{
                        'ts':     completed['ts'],
                        'open':   completed['open'],
                        'high':   completed['high'],
                        'low':    completed['low'],
                        'close':  completed['close'],
                        'volume': completed['volume'],
                    }])
                    self._kbar_history[code] = pd.concat(
                        [self._kbar_history[code], new_row], ignore_index=True
                    )
                    self._db_queue.put(("kbar", code, completed))

                # 開新分鐘 K 棒
                self._current_kbar[code] = {
                    'ts':     bar_time,
                    'open':   price,
                    'high':   price,
                    'low':    price,
                    'close':  price,
                    'volume': volume,
                }
            else:
                # 同分鐘更新中
                buf['high']   = max(buf['high'],  price)
                buf['low']    = min(buf['low'],   price)
                buf['close']  = price
                buf['volume'] += volume

    def _process_bidask(
        self, code: str, dt, bid: Optional[float], ask: Optional[float],
        bid_vol: int, ask_vol: int
    ):
        """記錄最佳買賣一檔資料並推送 DB。"""
        if code not in self._symbols:
            return

        entry = {
            'ts': dt, 'bid': bid, 'ask': ask,
            'bid_vol': bid_vol, 'ask_vol': ask_vol,
        }
        with self._lock:
            records = self._bidask_records[code]
            records.append(entry)
            # 避免記憶體無限成長
            if len(records) > 2000:
                self._bidask_records[code] = records[-2000:]

        self._db_queue.put(("bidask", code, entry))

    # ── 非同步 DB 寫入 ─────────────────────────────────────────────────────────

    def _db_worker_loop(self):
        """背景執行緒：持續清空寫入佇列，不阻塞 Callback。"""
        while True:
            try:
                item = self._db_queue.get(timeout=1.0)
            except queue.Empty:
                continue

            try:
                kind, code, data = item
                if kind == "kbar":
                    self._db_write_kbar(code, data)
                elif kind == "bidask":
                    self._db_write_bidask(code, data)
            except Exception as e:
                print(f"[DB Worker] ❌ {e}")
            finally:
                self._db_queue.task_done()

    def _db_write_kbar(self, code: str, kbar: dict):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "INSERT OR REPLACE INTO kbars_1min "
                "(code, ts, open, high, low, close, volume) VALUES (?,?,?,?,?,?,?)",
                (code, str(kbar['ts']),
                 kbar['open'], kbar['high'], kbar['low'], kbar['close'], kbar['volume']),
            )

    def _db_write_bidask(self, code: str, entry: dict):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "INSERT OR IGNORE INTO bidask_data "
                "(code, ts, best_bid, best_ask) VALUES (?,?,?,?)",
                (code, str(entry['ts']), entry['bid'], entry['ask']),
            )

    def _init_db(self):
        """確保 DB 表格存在（與 data/database.py 的 schema 相容）。"""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute('''
                CREATE TABLE IF NOT EXISTS kbars_1min (
                    code    TEXT    NOT NULL,
                    ts      DATETIME NOT NULL,
                    open    REAL,
                    high    REAL,
                    low     REAL,
                    close   REAL,
                    volume  INTEGER,
                    PRIMARY KEY (code, ts)
                )
            ''')
            conn.execute('''
                CREATE TABLE IF NOT EXISTS bidask_data (
                    code      TEXT     NOT NULL,
                    ts        DATETIME NOT NULL,
                    best_bid  REAL,
                    best_ask  REAL,
                    PRIMARY KEY (code, ts)
                )
            ''')

    # ── 斷線重連 ──────────────────────────────────────────────────────────────

    def _cb_event(self, event_code: int, event_name: str, description: str):
        if event_code == 13 or "reconnect" in event_name.lower():
            print(f"\n🔔 偵測到斷線重連，恢復 {len(self.active_subscriptions)} 組訂閱...")
            for sub in self.active_subscriptions:
                self.api.quote.subscribe(
                    sub["contract"],
                    quote_type=sub["quote_type"],
                    version=sj.constant.QuoteVersion.v1,
                )
            print("✅ 訂閱已恢復")
        else:
            print(f"[系統事件] 代碼: {event_code} | {event_name}")
