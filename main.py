import time
import pandas as pd
from core.sj_client import SjClient
from data.database import SjDatabase
from data.history_data import HistoryDataManager
from data.market_data import MarketDataManager
from data.market_info import MarketInfoManager

# ==========================================
# Control Panel
# ==========================================
TARGETS = [
    {"type": "Stock", "code": "8046"},
    {"type": "Future", "code": "LYFR1"}
]

START_DATE = "2026-04-28"
END_DATE = "2026-04-28"

CONFIG = {
    "RUN_MARKET_SCAN": False,        # 階段 0: 盤前市場掃描 (排行、處置股)
    "ENABLE_HIST_KBAR": True,        # 階段 1: 下載歷史 KBar (寫入 kbars_1min)
    "ENABLE_HIST_TICK": True,        # 階段 1: 下載歷史 Tick (寫入 historical_ticks，耗時較長)
    "ENABLE_STREAM_TICK": False,     # 階段 2: 訂閱即時行情 (動態組裝 KBar)
    "ENABLE_STREAM_BIDASK": True,    # 階段 2: 訂閱即時五檔 (寫入 bidask_data)
}

# ==========================================
# Step 0: Scan
# ==========================================
def run_market_scanning(api, db):
    """執行全市場掃描任務"""
    print("\n--- Step 0: 市場掃描 ---")
    info_manager = MarketInfoManager(api)
    
    df_rank = info_manager.get_market_rankings(scan_type="VolumeRank", count=50)
    print(df_rank.head(50))
    if not df_rank.empty:
        pass 
        
    df_attention = info_manager.get_attention_stocks()
    print(df_attention.head(50))
    if not df_attention.empty:
        pass

# ==========================================
# Step 1: Data Warmup
# ==========================================
def run_data_warmup(api, db, valid_contracts):
    """執行個別標的歷史資料預熱任務"""
    print("\n--- Step 1: 載入歷史資料 ---")

    h_manager = HistoryDataManager(api)
    
    date_list = pd.bdate_range(start=START_DATE, end=END_DATE).strftime('%Y-%m-%d').tolist()

    for contract in valid_contracts:
        print(f"\n 正在處理標的: {contract.code}")
        # -----------------------------------
        # 1. 歷史 KBar 預熱
        # -----------------------------------
        if CONFIG["ENABLE_HIST_KBAR"]:
            print(f">>> 獲取 {contract.code} 歷史 KBar ({START_DATE} ~ {END_DATE})...")
            df_kbar = h_manager.get_kbars(contract, START_DATE, END_DATE)
            
            if df_kbar is not None and not df_kbar.empty:
                print(f">>> 成功獲取 {len(df_kbar)} 筆 KBar，寫入資料庫...")
                for _, row in df_kbar.iterrows():
                    kbar_dict = {
                        'time': row['ts'], 
                        'open': row['Open'], 
                        'high': row['High'],
                        'low': row['Low'], 
                        'close': row['Close'], 
                        'volume': row['Volume']
                    }
                    db.add_kbar(contract.code, kbar_dict)
                print(f"✅ {contract.code} 歷史 KBar 寫入完成！")
            
            time.sleep(0.5)

        # -----------------------------------
        # 2. 歷史 Tick 預熱 (耗時較久)
        # -----------------------------------
        if CONFIG["ENABLE_HIST_TICK"]:
            print(f">>> 獲取 {contract.code} 歷史 Tick (逐日抓取)...")
            for current_date in date_list:
                df_tick = h_manager.get_ticks(contract, current_date)
                
                if df_tick is not None and not df_tick.empty:
                    # 呼叫 database.py 的批次寫入功能
                    db.add_historical_ticks_batch(contract.code, df_tick)
                
                time.sleep(0.3) # 逐日抓取必須有暫停時間
            print(f"✅ {contract.code} 歷史 Tick 寫入完成！")

# ==========================================
# Step 2: Streaming
# ==========================================
def run_streaming(api, db, valid_contracts):
    """執行即時行情訂閱任務"""
    print("\n--- Step 2: 即時行情串流 ---")
    market_data = MarketDataManager(api, db)
    
    for contract in valid_contracts:
        if CONFIG["ENABLE_STREAM_TICK"]:
            print(f">>> 訂閱 {contract.code} 即時 Tick...")
            market_data.subscribe_data(contract, quote_type="tick")
            
        if CONFIG["ENABLE_STREAM_BIDASK"]:
            print(f">>> 訂閱 {contract.code} 即時 BidAsk...")
            market_data.subscribe_data(contract, quote_type="bidask")
            
    print("\n✅ 系統正式運行中，等待行情觸發 (按 Ctrl+C 結束)...")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n系統安全關閉。")

# ==========================================
# Main Entry
# ==========================================
def main():
    client = SjClient()
    client.login(fetch_contract=True)
    db = SjDatabase("market_data.db")
    
    # collect valid contracts
    valid_contracts = []
    for t in TARGETS:
        t_type = t["type"]
        t_code = t["code"]
        contract = None
        
        try:
            if t_type == "Stock":
                contract = client.api.Contracts.Stocks[t_code]
            elif t_type == "Future":
                symbol = t_code[:3]
                contract = client.api.Contracts.Futures[symbol][t_code]
                
            if contract:
                valid_contracts.append(contract)
            else:
                print(f"⚠️ 無法取得合約，跳過: {t_code}")
                
        except KeyError:
            print(f"⚠️ 無效標的或找不到合約: {t_code} ({t_type})")

    # no any valid contracts
    if not valid_contracts:
        print("\n⚠️ 找不到任何有效的合約，系統退出。")
        return

    # ==========================================
    # Functions
    # ==========================================
    if CONFIG["RUN_MARKET_SCAN"]:
        run_market_scanning(client.api, db)
        
    if CONFIG["ENABLE_HIST_KBAR"] or CONFIG["ENABLE_HIST_TICK"]:
        run_data_warmup(client.api, db, valid_contracts)
        
    if CONFIG["ENABLE_STREAM_TICK"] or CONFIG["ENABLE_STREAM_BIDASK"]:
        run_streaming(client.api, db, valid_contracts)
    else:
        print("\n>>> 任務結束，未開啟即時行情。")

if __name__ == "__main__":
    main()