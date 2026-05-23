# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

This is a Taiwan stock/futures market data collection system built on the [Shioaji](https://sinotrade.github.io/) API (永豐金 API). It connects to a brokerage account, fetches historical and real-time market data, and stores it in a local SQLite database.

## Environment Setup

Uses Conda with Python 3.10. The environment is defined in `environment.yml`.

```bash
# Create and activate environment
conda env create -f environment.yml
conda activate test_env

# Run the system
python main.py
```

Credentials are loaded from a `.env` file (gitignored). Required variables:
```
API_KEY=
SECRET_KEY=
CA_CERT_PATH=      # optional, for CA activation
CA_PASSWORD=       # optional, for CA activation
```

## Running Individual Modules

Each data module has a `__main__` block for standalone testing:
```bash
python -m data.history_data   # test historical kbar/tick fetching
```

## Architecture

The system follows a layered architecture with a top-level config in `main.py`:

```
main.py            ← Control panel: TARGETS list, CONFIG flags, START/END dates
├── core/
│   └── sj_client.py         ← SjClient: wraps Shioaji login and contract lookup
├── tasks/
│   ├── contract_task.py     ← Validates TARGETS into contract objects
│   ├── warmup_task.py       ← Fetches historical kbars/ticks and writes to DB
│   ├── streaming_task.py    ← Subscribes to real-time tick/bidask feeds
│   └── scanner_task.py      ← Market-wide rankings (volume, price, amount, attention)
└── data/
    ├── database.py          ← SjDatabase: SQLite wrapper (kbars_1min, bidask_data, historical_ticks)
    ├── history_data.py      ← HistoryDataManager: api.kbars() / api.ticks() → DataFrame
    ├── market_data.py       ← MarketDataManager: real-time callbacks, assembles 1-min kbars from ticks
    └── market_info.py       ← MarketInfoManager: market scanners, credit balance, attention stocks
```

### Key Design Decisions

**CONFIG flags in `main.py`** control which phases run. Toggle individual features without code changes:
- `RUN_MARKET_*_SCAN` — pre-session market scans
- `ENABLE_HIST_KBAR` / `ENABLE_HIST_TICK` — historical data warmup
- `ENABLE_STREAM_TICK` / `ENABLE_STREAM_BIDASK` — live streaming

**TARGETS list** defines which instruments to track. Stocks use full code (e.g. `"8046"`); Futures use the continuous contract code (e.g. `"QSFR1"`), and `contract_task.py` automatically strips to the 3-char symbol for lookup.

**Real-time 1-min kbar assembly** happens in `MarketDataManager._process_tick()`: incoming ticks are aggregated in-memory per `code` and flushed to the DB when the minute boundary changes.

**Reconnection handling**: `MarketDataManager` listens for system event code 13 (reconnect) via `set_order_callback` and calls `_resubscribe_all()` to restore subscriptions from `self.active_subscriptions`.

**Database**: SQLite at `market_data.db` (gitignored). Tables use `(code, ts)` composite primary keys so multiple instruments share the same table without collision. Historical ticks use `pandas.to_sql()` for bulk insert; kbars and bidask use individual `INSERT OR REPLACE`/`INSERT OR IGNORE`.

### Simulation Mode

`SjClient` initializes Shioaji in simulation mode (`simulation=True`). Change to `simulation=False` for live trading data.

---

## Backtesting System

Entry point: `run_backtest.py`. Configure in `backtest/config.py`, then run:
```bash
python run_backtest.py
```

```
backtest/
├── config.py       ← BACKTEST_CONFIG (dates, symbols, frequency) + SYMBOL_POOLS groups
├── downloader.py   ← BacktestDownloader: Shioaji API → backtest_data.db (separate from live DB)
├── loader.py       ← BacktestLoader: SQLite → {symbol: DataFrame} for the engine
├── engine.py       ← BacktestEngine (event loop) + Portfolio (positions, cash, equity curve)
└── strategy.py     ← BaseStrategy (ABC) + MACrossStrategy example
```

### Backtesting Design

**Event-driven**: `BacktestEngine.run()` sorts all bars across all symbols by timestamp and calls `strategy.on_bar(symbol, bar, ts)` for each one.

**Fill model**: orders placed inside `on_bar()` fill at the current bar's close price (same bar). Next-bar-open execution is not yet implemented.

**Frequency**: `"1min"` pulls 1-min kbars directly from the API (one API call per symbol covers the full date range). `"day"` resamples the 1-min data to daily via pandas — no separate daily API call needed.

**Data separation**: the backtest uses `backtest_data.db` (separate from the live system's `market_data.db`). Re-running skips already-downloaded symbols unless `overwrite=True`.

**Custom strategy**: subclass `BaseStrategy` and implement `on_bar()`. Helpers available: `self.ma(symbol, period)`, `self.position(symbol)`, `self.buy()`, `self.sell()`.

**Commission**: `Portfolio` applies Taiwan stock rates: buy 0.1425%, sell 0.1425% + 0.3% tax.
