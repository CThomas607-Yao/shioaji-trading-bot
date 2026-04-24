import time
from core.sj_client import SjClient
from data.market_data import MarketDataManager
from data.database import SjDatabase # 引入資料庫

def main():
    # 初始化連線
    client = SjClient()
    client.login(fetch_contract=True)

    # 初始化資料庫
    db = SjDatabase("market_data.db")

    # 初始化行情處理模組 (傳入 api 與 db)
    market_data = MarketDataManager(client.api, db)

    # 訂閱
    contract = client.get_stock_info("1303")
    if contract:
        market_data.subscribe_data(contract)

    print("\n系統運行中，資料將自動存入 SQLite 資料庫...")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n系統關閉。")

if __name__ == "__main__":
    main()