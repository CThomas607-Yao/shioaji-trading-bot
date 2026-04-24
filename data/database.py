import sqlite3
import pandas as pd

class SjDatabase:
    def __init__(self, db_name="market_data.db"):
        self.db_name = db_name
        self.init_db()

    def init_db(self):
        """初始化資料庫：如果表格不存在則建立"""
        with sqlite3.connect(self.db_name) as conn:
            cursor = conn.cursor()
            # 建立 1 分鐘 K 線表
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS kbars_1min (
                    code TEXT,
                    ts DATETIME,
                    open REAL,
                    high REAL,
                    low REAL,
                    close REAL,
                    volume INTEGER,
                    PRIMARY KEY (code, ts) -- 避免重複存入相同時間的資料
                )
            ''')
            conn.commit()

    def add_kbar(self, code, kbar):
        """將單根 K 線存入資料庫"""
        try:
            with sqlite3.connect(self.db_name) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    INSERT OR REPLACE INTO kbars_1min (code, ts, open, high, low, close, volume)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', (
                    code, 
                    kbar['time'].strftime('%Y-%m-%d %H:%M:%S'), 
                    kbar['open'], 
                    kbar['high'], 
                    kbar['low'], 
                    kbar['close'], 
                    kbar['volume']
                ))
                conn.commit()
        except Exception as e:
            print(f"資料庫寫入錯誤: {e}")

    def query_kbars(self, code, limit=100):
        """查詢最近的 K 線資料並轉為 DataFrame"""
        with sqlite3.connect(self.db_name) as conn:
            query = f"SELECT * FROM kbars_1min WHERE code = '{code}' ORDER BY ts DESC LIMIT {limit}"
            return pd.read_sql_query(query, conn)