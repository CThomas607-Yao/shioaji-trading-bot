import time
import pandas as pd
from data.history_data import HistoryDataManager

def run_data_warmup(api, db, valid_contracts, start_date, end_date, config):
    """
    執行個別標的歷史資料預熱任務
    :param api: Shioaji API 連線物件
    :param db: SjDatabase 資料庫連線物件
    :param valid_contracts: 已驗證的合約物件清單 (List[Contract])
    :param start_date: 開始日期 (YYYY-MM-DD)
    :param end_date: 結束日期 (YYYY-MM-DD)
    :param config: 包含 ENABLE_HIST_KBAR 與 ENABLE_HIST_TICK 的字典
    """
    print("\n--- [階段一] 啟動個別標的歷史預熱 ---")
    h_manager = HistoryDataManager(api)
    
    date_list = pd.bdate_range(start=start_date, end=end_date).strftime('%Y-%m-%d').tolist()

    for contract in valid_contracts:
        print(f"\n🚀 處理標的: {contract.code}")
        
        # -----------------------------------
        # 1. 歷史 KBar 預熱 (1 分 K)
        # -----------------------------------
        if config.get("ENABLE_HIST_KBAR"):
            print(f">>> 獲取 {contract.code} 歷史 KBar ({start_date} ~ {end_date})...")
            df_kbar = h_manager.get_kbars(contract, start_date, end_date)
            
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
            
            time.sleep(0.5) # API 頻率保護

        # -----------------------------------
        # 2. 歷史 Tick 預熱 (逐日抓取)
        # -----------------------------------
        if config.get("ENABLE_HIST_TICK"):
            print(f">>> 獲取 {contract.code} 歷史 Tick (逐日迴圈抓取)...")
            for current_date in date_list:
                df_tick = h_manager.get_ticks(contract, current_date)
                
                if df_tick is not None and not df_tick.empty:
                    db.add_historical_ticks_batch(contract.code, df_tick)
                    print(f"    - {current_date}: 已寫入 {len(df_tick)} 筆")
                time.sleep(0.3)

            print(f"✅ {contract.code} 歷史 Tick 預熱完成！")