from data.market_data import MarketDataManager

def run_streaming(api, db, valid_contracts, config):
    """
    執行即時行情訂閱任務
    :param api: Shioaji API 連線物件
    :param db: SjDatabase 資料庫連線物件
    :param valid_contracts: 已驗證的合約物件清單 (List[Contract])
    :param config: 包含 ENABLE_STREAM_TICK 與 ENABLE_STREAM_BIDASK 的字典
    """
    print("\n--- [階段二] 啟動即時行情串流 ---")
    
    market_data = MarketDataManager(api, db)
    
    for contract in valid_contracts:
        # Tick
        if config.get("ENABLE_STREAM_TICK"):
            print(f">>> 訂閱 {contract.code} 即時 Tick...")
            market_data.subscribe_data(contract, quote_type="tick")
            
        # BidAsk
        if config.get("ENABLE_STREAM_BIDASK"):
            print(f">>> 訂閱 {contract.code} 即時 BidAsk...")
            market_data.subscribe_data(contract, quote_type="bidask")
            
    print("\n✅ 所有標的訂閱請求已送出，等待行情觸發...")