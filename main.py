import time
from datetime import datetime
from core.sj_client import SjClient
from data.market_data import MarketDataManager
from data.history_data import HistoryDataManager
from data.database import SjDatabase

# ==========================================
# 系統運行控制面板 (Control Panel)
# ==========================================
TARGET_CODE = "1303"                                      # 在這裡更換您想監控/下載的標的 (例如 "2330", "1303")
TARGET_DATE = "2026-04-22"                                # 歷史資料的目標日期 (可改為 datetime.today().strftime('%Y-%m-%d'))
# TARGET_DATE = datetime.today().strftime('%Y-%m-%d')     

CONFIG = {
    "ENABLE_HIST_KBAR": True,       # 模式 1: 下載歷史 KBar ----> db:kbars_1min
    "ENABLE_HIST_TICK": True,       # 模式 2: 下載歷史 Tick ----> db:historical_ticks
    "ENABLE_STREAM_TICK": True,    # 模式 3: 訂閱即時行情 Tick (用來動態組 KBar)
    "ENABLE_STREAM_BIDASK": True,  # 模式 4: 訂閱即時五檔 BidAsk
}

def main():
    client = SjClient()
    client.login(fetch_contract=True)
    db = SjDatabase("market_data.db")
    
    contract = client.api.Contracts.Stocks[TARGET_CODE]
    if not contract:
        print(f"⚠️ 無法取得標的 {TARGET_CODE} 的合約資訊，系統退出。")
        return

    # ---------------------------------------------------------
    # 【階段一：歷史資料獲取 (Data Warm-up)】
    # ---------------------------------------------------------
    h_manager = HistoryDataManager(client.api)
    
    if CONFIG["ENABLE_HIST_KBAR"]:
        print(f"\n>>> [啟動] 載入 {TARGET_CODE} 歷史 KBar...")
        df_kbar = h_manager.get_kbars(contract, TARGET_DATE, TARGET_DATE)
        if df_kbar is not None and not df_kbar.empty:
            print(f">>> 成功獲取 {len(df_kbar)} 筆 KBar，寫入資料庫...")
            # 將 KBar 逐筆寫入資料庫 (會由 db.add_kbar 處理分類與更新)
            for _, row in df_kbar.iterrows():
                kbar_dict = {
                    'time': row['ts'], 'open': row['Open'], 'high': row['High'],
                    'low': row['Low'], 'close': row['Close'], 'volume': row['Volume']
                }
                db.add_kbar(contract.code, kbar_dict)
            print(">>> 歷史 KBar 預熱完成！")

    if CONFIG["ENABLE_HIST_TICK"]:
        print(f"\n>>> [啟動] 載入 {TARGET_CODE} 歷史 Tick (含 BidAsk)...")
        df_tick = h_manager.get_ticks(contract, TARGET_DATE)
        if df_tick is not None and not df_tick.empty:
            # 呼叫資料庫的批次寫入功能
            db.add_historical_ticks_batch(contract.code, df_tick)
            print(">>> 歷史 Tick 獲取並存檔完成！")
        else:
            print(">>> ⚠️ 查無歷史 Tick 資料")

    # ---------------------------------------------------------
    # 【階段二：即時行情訂閱 (Streaming)】
    # ---------------------------------------------------------
    needs_streaming = CONFIG["ENABLE_STREAM_TICK"] or CONFIG["ENABLE_STREAM_BIDASK"]
    
    if needs_streaming:
        print("\n>>> [啟動] 準備訂閱即時行情...")
        market_data = MarketDataManager(client.api, db)
        
        if CONFIG["ENABLE_STREAM_TICK"]:
            print(f">>> 訂閱 {contract.code} 即時 Tick...")
            market_data.subscribe_data(contract, quote_type="tick")
            
        if CONFIG["ENABLE_STREAM_BIDASK"]:
            print(f">>> 訂閱 {contract.code} 即時 BidAsk...")
            market_data.subscribe_data(contract, quote_type="bidask")

        print("\n系統正式運行中，等待行情觸發 (按 Ctrl+C 結束)...")
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("\n系統安全關閉。")
    else:
        print("\n>>> 任務結束：目前未開啟即時行情訂閱模式，系統自動退出。")

if __name__ == "__main__":
    main()