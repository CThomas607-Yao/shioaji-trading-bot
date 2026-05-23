# backtest/downloader.py
# ──────────────────────────────────────────────────────────────────────────────
# 從永豐 API 下載歷史 K 線並存入 SQLite
# ──────────────────────────────────────────────────────────────────────────────
import time
import sqlite3
import pandas as pd
import shioaji as sj


class BacktestDownloader:
    def __init__(self, api: sj.Shioaji, db_path: str = "backtest_data.db"):
        self.api = api
        self.db_path = db_path
        self._init_db()

    # ── 建表 ──────────────────────────────────────────────────────────────────

    def _init_db(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute('''
                CREATE TABLE IF NOT EXISTS kbars (
                    symbol    TEXT    NOT NULL,
                    ts        TEXT    NOT NULL,
                    open      REAL,
                    high      REAL,
                    low       REAL,
                    close     REAL,
                    volume    INTEGER,
                    frequency TEXT    NOT NULL,
                    PRIMARY KEY (symbol, ts, frequency)
                )
            ''')
            conn.execute(
                'CREATE INDEX IF NOT EXISTS idx_kbars_lookup ON kbars (symbol, frequency, ts)'
            )
            conn.commit()

    # ── 公開介面 ──────────────────────────────────────────────────────────────

    def download_kbars(
        self,
        symbols: list,
        start: str,
        end: str,
        frequency: str = "1min",
        overwrite: bool = False,
    ):
        """
        下載並儲存 K 線資料。
        frequency: "1min" 直接從 API 取得；"day" 則由 1 分 K 聚合。
        overwrite: True 會強制重新下載已有的資料。
        """
        from datetime import date
        start_d = date.fromisoformat(start)
        end_d   = date.fromisoformat(end)
        today   = date.today()

        if start_d > end_d:
            raise ValueError(
                f"日期範圍錯誤：start_date ({start}) 晚於 end_date ({end})，"
                f"請確認 config.py 的日期順序。"
            )
        if start_d > today:
            raise ValueError(
                f"start_date ({start}) 是未來日期，無歷史資料可下載。"
            )
        if end_d > today:
            print(f"⚠️  end_date ({end}) 超過今天，自動截斷至今天 ({today})")
            end = str(today)

        total = len(symbols)
        for i, symbol in enumerate(symbols, 1):
            print(f"[{i}/{total}] {symbol} ({start} ~ {end})  frequency={frequency}")

            if not overwrite and self._already_downloaded(symbol, start, end, frequency):
                print(f"  ⏭  資料已存在，跳過（可用 overwrite=True 強制重下）")
                continue

            contract = self._get_contract(symbol)
            if contract is None:
                continue

            try:
                raw = self.api.kbars(contract=contract, start=start, end=end)
                df = pd.DataFrame({**raw})
                if df.empty:
                    print(f"  ⚠️  API 回傳無資料")
                    continue

                # 統一欄位名稱為小寫
                df.columns = df.columns.str.lower()
                df['ts'] = pd.to_datetime(df['ts'])

                if frequency == "day":
                    df = self._resample_to_daily(df)

                self._save_kbars(symbol, df, frequency)
                print(f"  ✅ 已存入 {len(df)} 筆")

            except Exception as e:
                print(f"  ❌ 下載失敗: {e}")

            time.sleep(0.5)  # 保護 API 呼叫頻率

    # ── 內部工具 ──────────────────────────────────────────────────────────────

    def _already_downloaded(self, symbol: str, start: str, end: str, frequency: str) -> bool:
        """檢查該標的在指定區間是否已有資料"""
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute(
                '''SELECT COUNT(*) FROM kbars
                   WHERE symbol=? AND frequency=?
                     AND DATE(ts) BETWEEN ? AND ?''',
                (symbol, frequency, start, end),
            ).fetchone()
        return row[0] > 0

    def _resample_to_daily(self, df: pd.DataFrame) -> pd.DataFrame:
        """將 1 分鐘 K 棒聚合為日 K"""
        df = df.set_index('ts')
        daily = df.resample('1D').agg(
            open=('open', 'first'),
            high=('high', 'max'),
            low=('low', 'min'),
            close=('close', 'last'),
            volume=('volume', 'sum'),
        ).dropna(subset=['open'])   # 非交易日會是全 NaN，一併移除
        return daily.reset_index()

    def _save_kbars(self, symbol: str, df: pd.DataFrame, frequency: str):
        """批次 upsert 至 kbars 表"""
        save_df = df[['ts', 'open', 'high', 'low', 'close', 'volume']].copy()
        save_df['ts'] = save_df['ts'].astype(str)
        save_df.insert(0, 'symbol', symbol)
        save_df['frequency'] = frequency

        with sqlite3.connect(self.db_path) as conn:
            # 先寫入暫存表，再用 INSERT OR REPLACE 整批 upsert
            save_df.to_sql('_kbars_tmp', conn, if_exists='replace', index=False)
            conn.execute('''
                INSERT OR REPLACE INTO kbars
                    (symbol, ts, open, high, low, close, volume, frequency)
                SELECT symbol, ts, open, high, low, close, volume, frequency
                FROM _kbars_tmp
            ''')
            conn.execute('DROP TABLE _kbars_tmp')
            conn.commit()

    def _get_contract(self, symbol: str):
        """先找股票，找不到再試期貨（前三碼為 symbol 代號）"""
        try:
            return self.api.Contracts.Stocks[symbol]
        except (KeyError, TypeError):
            pass
        try:
            return self.api.Contracts.Futures[symbol[:3]][symbol]
        except (KeyError, TypeError):
            pass
        print(f"  ⚠️  找不到合約: {symbol}")
        return None
