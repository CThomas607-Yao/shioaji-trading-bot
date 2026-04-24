import os
import shioaji as sj
from dotenv import load_dotenv

load_dotenv()

class SjClient:
    def __init__(self):
        self.api = sj.Shioaji(simulation=True)

    def login(self, fetch_contract=False):
        print("=========================")
        print("        Logging In       ")
        print("=========================")
        self.api.login(
            api_key=os.environ.get("API_KEY"),
            secret_key=os.environ.get("SECRET_KEY"),
            fetch_contract=fetch_contract
        )

    def log_ca(self):
        print("=========================")
        print("      Activating CA      ")
        print("=========================")
        self.api.activate_ca(
            ca_path=os.environ.get("CA_CERT_PATH"),
            ca_passwd=os.environ.get("CA_PASSWORD"),
        )

    def log_contract(self):
        print("=========================")
        print("    Loading Contracts    ")
        print("=========================")
        self.api.fetch_contracts(contract_download=True)
        print("合約種類:", self.api.Contracts.status)

    # 查詢商品檔功能 (回傳合約物件供其他模組使用)
    def get_stock_info(self, pid):
        try:
            return self.api.Contracts.Stocks[str(pid)]
        except KeyError:
            print(f"沒有股票 {pid} ")
            return None

    def get_future_info(self, pid):
        try:
            return self.api.Contracts.Futures[str(pid)]
        except KeyError:
            print(f"沒有期貨 {pid} ")
            return None

    def get_option_info(self, pid):
        try:
            return self.api.Contracts.Options[str(pid)]
        except KeyError:
            print(f"沒有選擇權 {pid} ")
            return None

    def get_index_info(self, pid):
        try:
            return self.api.Contracts.Indexs.TSE[str(pid)]
        except KeyError:
            print(f"沒有指數 {pid} ")
            return None