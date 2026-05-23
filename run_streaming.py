# run_streaming.py
# ──────────────────────────────────────────────────────────────────────────────
# 即時行情串流入口
#
# 使用方式：
#   1. 修改 streaming/config.py 中的 STREAMING_CONFIG（標的、模式、顯示參數）
#   2. python run_streaming.py
#   3. 關閉圖表視窗即結束程式
# ──────────────────────────────────────────────────────────────────────────────
import time

from core.sj_client import SjClient
from streaming.config import STREAMING_CONFIG, SYMBOL_POOLS
from streaming.warmup import WarmupLoader
from streaming.manager import StreamingManager
from streaming.chart import StreamingChart


def resolve_symbols(symbols_input) -> list:
    """
    支援兩種格式：
      - list: ["2330", "2317"]
      - str:  "SEMIS"（對應 SYMBOL_POOLS 中的群組名稱）
    """
    if isinstance(symbols_input, list):
        return symbols_input
    if isinstance(symbols_input, str):
        if symbols_input in SYMBOL_POOLS:
            return SYMBOL_POOLS[symbols_input]
        raise ValueError(
            f"找不到股票池群組 '{symbols_input}'。"
            f"請確認 SYMBOL_POOLS，或直接填代號清單。"
        )
    raise TypeError(f"symbols 格式錯誤: {type(symbols_input)}")


def get_contract(api, symbol: str):
    """先找現貨，找不到再試期貨（取前三碼為 Symbol）。"""
    try:
        return api.Contracts.Stocks[symbol]
    except (KeyError, TypeError):
        pass
    try:
        return api.Contracts.Futures[symbol[:3]][symbol]
    except (KeyError, TypeError):
        pass
    return None


def main():
    cfg     = STREAMING_CONFIG
    symbols = resolve_symbols(cfg["symbols"])

    print("╔══════════════════════════════════════╗")
    print("║     即時行情串流  Streaming Monitor    ║")
    print("╚══════════════════════════════════════╝")
    print(f"  模式     : {cfg['data_type'].upper()}")
    print(f"  標的數量 : {len(symbols)} 個")
    print(f"  預熱天數 : {cfg['warmup_days']} 天")
    print(f"  顯示標的 : 最多 {cfg['max_chart_symbols']} 個（超出部分仍訂閱+寫 DB）")

    # ── Step 1: 登入 ─────────────────────────────────────────────────────────
    print("\n[Step 1] 連線登入")
    client = SjClient()
    client.login(fetch_contract=True)

    # ── Step 2: 解析合約 ──────────────────────────────────────────────────────
    print("\n[Step 2] 載入合約")
    contracts = []
    for sym in symbols:
        c = get_contract(client.api, sym)
        if c:
            contracts.append(c)
            print(f"  ✅ {sym}")
        else:
            print(f"  ⚠️ 找不到合約，已跳過: {sym}")

    if not contracts:
        print("❌ 無有效合約，中止。")
        return

    # 決定哪些標的顯示圖表（前 N 個）
    n_chart         = min(len(contracts), cfg.get("max_chart_symbols", 4))
    chart_contracts = contracts[:n_chart]
    chart_codes     = [c.code for c in chart_contracts]

    # ── Step 3: 預熱歷史 K 棒 ─────────────────────────────────────────────────
    print("\n[Step 3] 預熱歷史資料（用於均線計算）")
    warmup_loader = WarmupLoader(client.api)
    warmup_data   = warmup_loader.load(
        contracts=contracts,
        warmup_days=cfg["warmup_days"],
    )

    # ── Step 4: 初始化串流管理器 ──────────────────────────────────────────────
    print("\n[Step 4] 初始化串流管理器")
    manager = StreamingManager(
        api         = client.api,
        db_path     = cfg["db_path"],
        warmup_data = warmup_data,
        data_type   = cfg["data_type"],
    )

    # ── Step 5: 訂閱即時行情 ──────────────────────────────────────────────────
    print("\n[Step 5] 訂閱即時行情")
    manager.subscribe_all(contracts)
    print(f"\n✅ 已訂閱 {len(contracts)} 個標的，等待行情推送...")

    # ── Step 6: 啟動即時圖表 ──────────────────────────────────────────────────
    if cfg.get("show_chart", True) and chart_contracts:
        print(f"\n[Step 6] 啟動圖表（顯示 {chart_codes}）")
        print("  ← 關閉圖表視窗即可結束程式 →\n")

        chart = StreamingChart(
            symbols      = chart_codes,
            warmup_data  = {s: warmup_data[s] for s in chart_codes},
            data_type    = cfg["data_type"],
            ma_periods   = cfg.get("ma_periods", [5, 10, 20]),
            chart_window = cfg.get("chart_window", 60),
            interval_ms  = cfg.get("chart_interval_ms", 500),
        )
        chart.start(manager)   # 阻塞直到視窗關閉

    else:
        print("\n[Step 6] 圖表已停用（show_chart=False）")
        print("  使用 Ctrl+C 結束串流\n")
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("\n串流已停止。")


if __name__ == "__main__":
    main()
