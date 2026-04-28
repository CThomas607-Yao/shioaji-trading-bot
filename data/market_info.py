# data/market_info.py
import pandas as pd
import shioaji as sj

class MarketInfoManager:
    def __init__(self, api: sj.Shioaji):
        self.api = api

    def get_market_rankings(self, scan_type="VolumeRank", count=50):
        """
        獲取市場排行
        :param scan_type: 排行榜類型 (預設為成交量排行 VolumeRank)

            ChangePercentRank: 依價格漲跌幅排序
            ChangePriceRank: 依價格漲跌排序
            DayRangeRank: 依高低價差排序
            VolumeRank: 依成交量排序
            AmountRank: 依成交金額排序

        :param count: 抓取前 N 名
        """
        print(f">>> 正在獲取全市場排行榜 (類型: {scan_type})...")
        try:
            scanners = self.api.scanners(
                scanner_type=scan_type,
                count=count
            )
            
            if scanners:
                df = pd.DataFrame([s.__dict__ for s in scanners])
                print(f"成功取得 {len(df)} 檔排行資料")
                return df
            else:
                print("⚠️ 排行榜無資料回傳")
                return pd.DataFrame()
        except Exception as e:
            print(f"❌ 獲取市場排行失敗: {e}")
            return pd.DataFrame()

    def get_credit_balance(self, contracts: list):
        """
        獲取多檔標的之資券餘額
        傳入參數: contracts (List[shioaji.contracts.Stock])
        """
        if not contracts:
            print("⚠️ 未提供合約，無法查詢資券餘額")
            return pd.DataFrame()
            
        print(f">>> 正在獲取 {len(contracts)} 檔標的之資券餘額...")
        try:
            credit_data = self.api.credit_enquires(contracts=contracts)
            
            if credit_data:
                df = pd.DataFrame([c.__dict__ for c in credit_data])
                print(f">> 成功取得 {len(df)} 筆資券餘額資料")
                return df
            return pd.DataFrame()
        except Exception as e:
            print(f"❌ 獲取資券餘額失敗: {e}")
            return pd.DataFrame()

    def get_attention_stocks(self):
        """
        獲取全市場處置及注意股名單
        """
        print(">>> 正在獲取全市場處置及注意股名單...")
        try:
            # 依照您的規格，使用 api.punish() 獲取全市場處置資訊
            punish_data = self.api.punish()
            
            if punish_data:
                df = pd.DataFrame({**punish_data})
                print(f">> 成功取得處置/注意股資料 (共 {len(df)} 筆)")
                return df
            return pd.DataFrame()
        except Exception as e:
            print(f"❌ 獲取處置及注意股失敗: {e}")
            return pd.DataFrame()