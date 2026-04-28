# main.py
import time
from core.sj_client import SjClient
from data.database import SjDatabase
from tasks.contract_task import get_valid_contracts 
from tasks.warmup_task import run_data_warmup
from tasks.streaming_task import run_streaming
from tasks.scanner_task import run_volume_scanning, run_price_scanning, run_amount_scanning, run_attention_scanning

# ==========================================
# 系統控制面板 (Control Panel)
# ==========================================
TARGETS = [
    {"type": "Stock", "code": "8046"}, 
    {"type": "Future", "code": "QSFR1"}
]

START_DATE = "2026-04-22"
END_DATE = "2026-04-28"

CONFIG = {
    "RUN_MARKET_VOLUME_SCAN": True,      
    "RUN_MARKET_PRICE_SCAN": True,       
    "RUN_MARKET_AMOUNT_SCAN": True,      
    "RUN_MARKET_ATTENTION_SCAN": False,

    "ENABLE_HIST_KBAR": True,     
    "ENABLE_HIST_TICK": True, 

    "ENABLE_STREAM_TICK": False,   
    "ENABLE_STREAM_BIDASK": False, 
}

def main():
    client = SjClient()
    client.login(fetch_contract=True)
    db = SjDatabase("market_data.db")
    
    valid_contracts = get_valid_contracts(client.api, TARGETS)
    
    if not valid_contracts:
        print("\n❌ 找不到任何有效合約，系統退出。")
        return

    if CONFIG["RUN_MARKET_VOLUME_SCAN"]:
        run_volume_scanning(client.api, db)
    if CONFIG["RUN_MARKET_PRICE_SCAN"]:
        run_price_scanning(client.api, db)
    if CONFIG["RUN_MARKET_AMOUNT_SCAN"]:
        run_amount_scanning(client.api, db)
    if CONFIG["RUN_MARKET_ATTENTION_SCAN"]:
        run_attention_scanning(client.api, db)

    if CONFIG["ENABLE_HIST_KBAR"] or CONFIG["ENABLE_HIST_TICK"]:
        run_data_warmup(client.api, db, valid_contracts, START_DATE, END_DATE, CONFIG)
        
    if CONFIG["ENABLE_STREAM_TICK"] or CONFIG["ENABLE_STREAM_BIDASK"]:
        run_streaming(client.api, db, valid_contracts, CONFIG)
        
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("\n👋 收到終止訊號，系統安全關閉。")

if __name__ == "__main__":
    main()