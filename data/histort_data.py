import pandas as pd
import shioaji as sj

class HistoryDataManager:
    def __init__(self, api: sj.Shioaji):
        self.api = api

    # def get_kbars(self, contract, start_date: str, end_date: str):
    #     """
    #     獲取歷史 K 線資料 (股票、期貨、連續期貨皆適用)
    #     :param contract: 商品合約物件
    #     :param start_date: 開始日期 (YYYY-MM-DD)
    #     :param end_date: 結束日期 (YYYY-MM-DD)
    #     """
    #     print(f"--- 抓取歷史 K 線: {contract.code} ({start_date} ~ {end_date}) ---")
        
    #     kbars = self.api.kbars(
    #         contract=contract,
    #         start=start_date,
    #         end=end_date,
    #     )
        
    #     df = pd.DataFrame({**kbars})
    #     if not df.empty:
    #         df.ts = pd.to_datetime(df.ts) # 將時間戳記轉換為 Pandas 易讀格式
    #         return df
    #     else:
    #         print(f"⚠️ 查無 {contract.code} 的 K 線資料")
    #         return None

    def get_ticks(self, contract, date: str, time_start: str = None, time_end: str = None):
        """
        獲取特定日期的歷史 Tick 資料 (支援指定時間區間)
        :param contract: 商品合約物件
        :param date: 指定日期 (YYYY-MM-DD)
        :param time_start: 開始時間 (HH:MM:SS) - 選填
        :param time_end: 結束時間 (HH:MM:SS) - 選填
        """
        if time_start and time_end:
            print(f"--- 抓取歷史 Ticks: {contract.code} | 日期: {date} | 時間: {time_start} ~ {time_end} ---")
            ticks = self.api.ticks(
                contract=contract, 
                date=date,
                query_type=sj.constant.TicksQueryType.RangeTime,
                time_start=time_start,
                time_end=time_end
            )
        else:
            print(f"--- 抓取歷史 Ticks: {contract.code} | 日期: {date} (全天) ---")
            ticks = self.api.ticks(contract=contract, date=date)
            
        df = pd.DataFrame({**ticks})
        if not df.empty:
            df.ts = pd.to_datetime(df.ts)
            return df
        else:
            print(f"⚠️ 查無 {contract.code} 的 Tick 資料")
            return None


if __name__ == "__main__":
    import sys
    import os

    # 確保能從父目錄 import core
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if project_root not in sys.path:
        sys.path.append(project_root)

    from core.sj_client import SjClient

    print("=== 啟動歷史行情抓取測試 ===")
    
    client = SjClient()
    client.login(fetch_contract=True)
    
    h_manager = HistoryDataManager(client.api)
    
    ticks = h_manager.get_ticks(
        contract=client.api.Contracts.Stocks["2330"],
        date="2026-01-16"
    )
    ticks




    # stock_contract = client.get_stock_info("2330")

    # # ==========================================
    # # 測試情境一：抓取 2330 歷史 K 線
    # # ==========================================
    # print("\n[測試 1] 抓取股票 K 線 (2330)")
    # if stock_contract:
    #     print(stock_contract)
    #     df_kbar = h_manager.get_kbars(stock_contract, "2026-04-22", "2026-04-22")
    #     if df_kbar is not None:
    #         print(df_kbar.head())

    # # ==========================================
    # # 測試情境二：抓取 2330 歷史 Ticks (指定時間區間)
    # # ==========================================
    # print("\n[測試 2] 抓取股票 Ticks 區間 (2330, 09:00:00~09:20:01)")
    # if stock_contract:
    #     print('stock_contract: ', stock_contract)
    #     print(type(stock_contract))
    #     print('stock_contract.code: ', stock_contract.code)
    #     print(type(stock_contract.code))
        
    #     df_ticks = h_manager.get_ticks(
    #         contract=stock_contract.code, 
    #         date="2026-04-22", 
    #         time_start="09:00:00", 
    #         time_end="09:20:01"
    #     )
    #     if df_ticks is not None:
    #         print(df_ticks.head())
    #         print(f"... 共抓取 {len(df_ticks)} 筆 Tick 資料")

    # # ==========================================
    # # 測試情境三：抓取連續期貨合約 (TXFR1) K 線
    # # ==========================================
    # print("\n[測試 3] 抓取近月連續期貨 K 線 (TXFR1)")
    # # 注意：在 Shioaji 中，連續期貨可以直接用期貨代碼 "TXFR1" 查詢
    # future_contract_r1 = client.get_future_info("TXFR1")
    
    # if future_contract_r1:
    #     df_future_kbar = h_manager.get_kbars(future_contract_r1, "2026-04-22", "2026-04-22")
    #     if df_future_kbar is not None:
    #         print(df_future_kbar.head())
            
    # print("\n=== 測試結束 ===")