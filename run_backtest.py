# run_backtest.py
# ──────────────────────────────────────────────────────────────────────────────
# 回測入口
#
# 使用方式：
#   1. 修改 backtest/config.py 中的 BACKTEST_CONFIG（標的、日期、頻率）
#   2. 在下方 main() 的 Step 3 替換成你自己的策略類別
#   3. python run_backtest.py
# ──────────────────────────────────────────────────────────────────────────────
from core.sj_client import SjClient
from backtest.config import BACKTEST_CONFIG, SYMBOL_POOLS
from backtest.downloader import BacktestDownloader
from backtest.loader import BacktestLoader
from backtest.engine import BacktestEngine
from backtest.strategy import MACrossStrategy  # ← 替換成自訂策略


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
            f"請檢查 SYMBOL_POOLS，或直接給代號清單。"
        )
    raise TypeError(f"symbols 格式錯誤: {type(symbols_input)}")


def main():
    cfg     = BACKTEST_CONFIG
    symbols = resolve_symbols(cfg["symbols"])

    print("╔══════════════════════════════════════╗")
    print("║          回測系統  Backtester         ║")
    print("╚══════════════════════════════════════╝")
    print(f"  標的  : {symbols}")
    print(f"  區間  : {cfg['start_date']} ~ {cfg['end_date']}")
    print(f"  頻率  : {cfg['frequency']}")
    print(f"  資金  : {cfg['initial_capital']:,} 元")

    # ── Step 1: 下載歷史資料 ──────────────────────────────────────────────────
    print("\n[Step 1] 連線並下載歷史 K 線")
    client = SjClient()
    client.login(fetch_contract=True)

    downloader = BacktestDownloader(client.api, cfg["db_path"])

    if cfg["data_type"] == "kbar":
        downloader.download_kbars(
            symbols=symbols,
            start=cfg["start_date"],
            end=cfg["end_date"],
            frequency=cfg["frequency"],
            overwrite=False,    # 已下載的標的直接跳過；改 True 可強制重下
        )
    else:
        print(f"❌ 尚未支援 data_type='{cfg['data_type']}'")
        return

    # ── Step 2: 載入資料 ──────────────────────────────────────────────────────
    print("\n[Step 2] 從資料庫載入資料")
    loader = BacktestLoader(cfg["db_path"])
    data   = loader.load_kbars(
        symbols=symbols,
        start=cfg["start_date"],
        end=cfg["end_date"],
        frequency=cfg["frequency"],
    )

    if not data:
        print("❌ 無任何可用資料，請確認下載是否成功。")
        return

    # ── Step 3: 執行回測 ──────────────────────────────────────────────────────
    print("\n[Step 3] 執行回測策略")

    # ↓↓↓ 在這裡替換成你自己的策略 ↓↓↓
    strategy = MACrossStrategy(
        fast_period=5,
        slow_period=20,
        shares_per_trade=1000,
    )
    # ↑↑↑

    engine = BacktestEngine(data, initial_capital=cfg["initial_capital"])
    result = engine.run(strategy)

    # result 包含：
    #   result['equity_curve']     → DataFrame (ts index, equity 欄位)
    #   result['trades']           → DataFrame (每筆交易)
    #   result['total_return_pct'] → float
    #   result['max_drawdown_pct'] → float


if __name__ == "__main__":
    main()
