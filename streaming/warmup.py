# streaming/warmup.py
# ──────────────────────────────────────────────────────────────────────────────
# 串流前預熱：抓取歷史 1 分鐘 K 棒，供均線初始計算使用
# ──────────────────────────────────────────────────────────────────────────────
import time
from datetime import date, timedelta

import pandas as pd
import shioaji as sj


class WarmupLoader:
    def __init__(self, api: sj.Shioaji):
        self.api = api

    def load(self, contracts: list, warmup_days: int = 5) -> dict:
        """
        為每個合約抓取最近 warmup_days 個日曆日的 1 分鐘 K 棒。
        回傳 {symbol: pd.DataFrame(columns=[ts, open, high, low, close, volume])}
        ts 欄位為 pd.Timestamp，已按時間升冪排序。
        """
        end_date   = str(date.today())
        start_date = str(date.today() - timedelta(days=warmup_days))

        result = {}
        total  = len(contracts)

        for i, contract in enumerate(contracts, 1):
            symbol = contract.code
            print(f"  [{i}/{total}] 預熱 {symbol}  ({start_date} ~ {end_date})")

            try:
                raw = self.api.kbars(contract=contract, start=start_date, end=end_date)
                df  = pd.DataFrame({**raw})

                if not df.empty:
                    df.columns = df.columns.str.lower()
                    df['ts']   = pd.to_datetime(df['ts'])
                    df = df.sort_values('ts').reset_index(drop=True)
                    result[symbol] = df
                    print(f"      ✅ 載入 {len(df)} 根 K 棒")
                else:
                    print(f"      ⚠️ 查無歷史資料")
                    result[symbol] = _empty_kbar_df()

            except Exception as e:
                print(f"      ❌ 預熱失敗: {e}")
                result[symbol] = _empty_kbar_df()

            time.sleep(0.3)   # 保護 API 呼叫頻率

        return result


def _empty_kbar_df() -> pd.DataFrame:
    return pd.DataFrame(columns=['ts', 'open', 'high', 'low', 'close', 'volume'])
