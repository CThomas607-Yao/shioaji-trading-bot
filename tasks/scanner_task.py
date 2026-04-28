from data.market_info import MarketInfoManager

def run_volume_scanning(api, db):
    """執行全成交量掃描任務"""
    print("\n--- 成量排行列表 ---")
    info_manager = MarketInfoManager(api)
    
    df_rank = info_manager.get_market_rankings(scan_type="VolumeRank", count=10)
    print(">> 成交量排行:")
    print(df_rank.head(10))
    if not df_rank.empty:
        pass

def run_price_scanning(api, db):
    """執行全價格掃描任務"""
    print("\n--- 漲幅排行列表 ---")
    info_manager = MarketInfoManager(api)
    
    df_rank = info_manager.get_market_rankings(scan_type="ChangePercentRank", count=10)
    print(">> 價格排行:")
    print(df_rank.head(10))
    if not df_rank.empty:
        pass

def run_amount_scanning(api, db):
    """執行全成交額掃描任務"""
    print("\n--- 成交額排行列表 ---")
    info_manager = MarketInfoManager(api)
    
    df_rank = info_manager.get_market_rankings(scan_type="AmountRank", count=10)
    print(">> 成交金額排行:")
    print(df_rank.head(10))
    if not df_rank.empty:
        pass

def run_attention_scanning(api, db):
    """執行全市場注意股掃描任務"""
    print("\n--- 注意/處置股列表 ---")
    info_manager = MarketInfoManager(api)
    
    df_attention = info_manager.get_attention_stocks()
    print(">> 注意股列表:")
    print(df_attention.head(50))
    if not df_attention.empty:
        pass