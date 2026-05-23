# backtest/loader.py
# ──────────────────────────────────────────────────────────────────────────────
# 從 SQLite 載入資料供回測引擎使用
# ──────────────────────────────────────────────────────────────────────────────
import sqlite3
import pandas as pd


class BacktestLoader:
    def __init__(self, db_path: str):
        self.db_path = db_path

    def load_kbars(
        self,
        symbols: list,
        start: str,
        end: str,
        frequency: str = "1min",
    ) -> dict:
        """
        回傳 {symbol: DataFrame}。
        DataFrame 以 ts 為 index，欄位為 open / high / low / close / volume。
        """
        data = {}
        with sqlite3.connect(self.db_path) as conn:
            for symbol in symbols:
                df = pd.read_sql_query(
                    '''SELECT ts, open, high, low, close, volume
                       FROM kbars
                       WHERE symbol = ?
                         AND frequency = ?
                         AND DATE(ts) BETWEEN ? AND ?
                       ORDER BY ts''',
                    conn,
                    params=(symbol, frequency, start, end),
                )
                if df.empty:
                    print(f"⚠️  {symbol} 無資料，請先執行下載")
                    continue
                df['ts'] = pd.to_datetime(df['ts'])
                df.set_index('ts', inplace=True)
                data[symbol] = df
                print(f"✅ 載入 {symbol}: {len(df):,} 筆")
        return data
