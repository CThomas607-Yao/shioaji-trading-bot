import sqlite3
import pandas as pd

class SjDatabase:
    def __init__(self, db_name="market_data.db"):
        self.db_name = db_name
        self.init_db()

    # ==========================================
    # 模塊一：初始化與建表 (支援多檔股票共用 Table)
    # ==========================================
    def init_db(self):
        """初始化資料庫：建立 K線表 與 BidAsk 表"""
        with sqlite3.connect(self.db_name) as conn:
            cursor = conn.cursor()
            
            # 建立 1 分鐘 K 線表 (使用 REPLACE 確保同一分鐘 K 線可更新)
            # PRIMARY KEY (code, ts) 確保不同股票的資料互不干擾
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS kbars_1min (
                    code TEXT,
                    ts DATETIME,
                    open REAL,
                    high REAL,
                    low REAL,
                    close REAL,
                    volume INTEGER,
                    PRIMARY KEY (code, ts)
                )
            ''')
            
            # 建立即時 BidAsk 表 (最佳五檔)
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS bidask_data (
                    code TEXT,
                    ts DATETIME,
                    best_bid REAL,
                    best_ask REAL,
                    PRIMARY KEY (code, ts)
                )
            ''')
            conn.commit()

    # ==========================================
    # 模塊二：單筆 KBar 寫入 (歷史預熱/即時行情 共用)
    # ==========================================
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
            print(f"❌ KBar 資料庫寫入錯誤: {e}")

    # ==========================================
    # 模塊三：即時 BidAsk 寫入
    # ==========================================
    def add_bidask(self, code, ts, best_bid, best_ask):
        """將即時 BidAsk 存入資料庫"""
        try:
            with sqlite3.connect(self.db_name) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    INSERT OR IGNORE INTO bidask_data (code, ts, best_bid, best_ask)
                    VALUES (?, ?, ?, ?)
                ''', (
                    code, 
                    ts.strftime('%Y-%m-%d %H:%M:%S.%f'), # 保留微秒
                    best_bid, 
                    best_ask
                ))
                conn.commit()
        except Exception as e:
            print(f"❌ BidAsk 資料庫寫入錯誤: {e}")

    # ==========================================
    # 模塊四：歷史 Tick 批次寫入 (效能優化)
    # ==========================================
    def add_historical_ticks_batch(self, code, df_ticks):
        """將整包歷史 Tick DataFrame 批次存入資料庫"""
        if df_ticks is None or df_ticks.empty:
            return
            
        try:
            with sqlite3.connect(self.db_name) as conn:
                # 幫 DataFrame 加上股票代號欄位，標記這是哪一檔股票的 Tick
                df_ticks['code'] = code
                
                # 利用 Pandas to_sql 直接塞入資料庫，if_exists='append' 代表接在舊資料下方
                df_ticks.to_sql('historical_ticks', conn, if_exists='append', index=False)
                print(f">>> 成功將 {len(df_ticks)} 筆歷史 Tick 寫入資料庫！")
        except Exception as e:
            print(f"❌ 歷史 Tick 寫入失敗: {e}")

    # ==========================================
    # 查詢模塊 (供未來策略回測或畫圖使用)
    # ==========================================
    def query_kbars(self, code, limit=100):
        """查詢特定股票最近的 K 線資料並轉為 DataFrame"""
        with sqlite3.connect(self.db_name) as conn:
            # 利用 code 精準撈取特定標的
            query = f"SELECT * FROM kbars_1min WHERE code = '{code}' ORDER BY ts DESC LIMIT {limit}"
            return pd.read_sql_query(query, conn)