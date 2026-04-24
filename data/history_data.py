import pandas as pd
import shioaji as sj

class HistoryDataManager:
    def __init__(self, api: sj.Shioaji):
        self.api = api

    def get_kbars(self, contract, start_date: str, end_date: str):
        """
        獲取歷史 K 線資料 (股票、期貨皆適用)
        :param contract: 商品合約物件 (請傳入完整的 Contract，勿傳入字串)
        :param start_date: 開始日期 (YYYY-MM-DD)
        :param end_date: 結束日期 (YYYY-MM-DD)
        """
        print(f"--- 抓取歷史 K 線: {contract.code} ({start_date} ~ {end_date}) ---")
        
        try:
            kbars = self.api.kbars(
                contract=contract,
                start=start_date,
                end=end_date,
            )
            
            # 將 Kbars 轉換成 DataFrame
            df = pd.DataFrame({**kbars})
            if not df.empty:
                df.ts = pd.to_datetime(df.ts)
                return df
            else:
                print(f"⚠️ 查無 {contract.code} 的 K 線資料")
                return None
        except Exception as e:
            print(f"❌ KBar 抓取失敗: {e}")
            return None

    def get_ticks(self, contract, date: str, time_start: str = None, time_end: str = None):
        """
        獲取特定日期的歷史 Tick 資料 (支援指定時間區間)
        :param contract: 商品合約物件 (請傳入完整的 Contract，勿傳入字串)
        :param date: 指定日期 (YYYY-MM-DD)
        :param time_start: 開始時間 (HH:MM:SS) - 選填
        :param time_end: 結束時間 (HH:MM:SS) - 選填
        """
        try:
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
                
            # 將 Ticks 轉換成 DataFrame
            df = pd.DataFrame({**ticks})
            if not df.empty:
                df.ts = pd.to_datetime(df.ts)
                return df
            else:
                print(f"⚠️ 查無 {contract.code} 的 Tick 資料")
                return None
        except Exception as e:
            print(f"❌ Tick 抓取失敗: {e}")
            return None


if __name__ == "__main__":
    import sys
    import os

    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if project_root not in sys.path:
        sys.path.append(project_root)

    from core.sj_client import SjClient

    print("=== 啟動歷史行情抓取測試 ===")
    
    client = SjClient()
    client.login(fetch_contract=True)
    
    h_manager = HistoryDataManager(client.api)
    
    # 【重點注意】此處必須取得「完整合約物件」
    stock_contract = client.api.Contracts.Stocks["2330"]

    if stock_contract:
        # ==========================================
        # 測試一：抓取 2330 歷史 K 線 (1分K)
        # ==========================================
        print("\n[測試 1] 抓取股票 K 線 (2330)")
        df_kbar = h_manager.get_kbars(stock_contract, "2024-04-22", "2024-04-22")
        if df_kbar is not None:
            print(df_kbar.head())

        # ==========================================
        # 測試二：抓取 2330 歷史 Ticks (指定時間區間)
        # ==========================================
        print("\n[測試 2] 抓取股票 Ticks 區間 (2330, 09:00:00~09:20:00)")
        df_ticks = h_manager.get_ticks(
            contract=stock_contract,
            date="2024-04-22", 
            time_start="09:00:00", 
            time_end="09:20:00"
        )
        if df_ticks is not None:
            print(df_ticks.head())