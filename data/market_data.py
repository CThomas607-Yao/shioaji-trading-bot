import shioaji as sj

class MarketDataManager:
    def __init__(self, api: sj.Shioaji, db):
        self.api = api
        self.db = db
        self.kbars = {}
        self.api.quote.set_on_tick_stk_v1_callback(self.on_tick_stk)
        
        # 綁定 Callback (包含 Tick 與 BidAsk)
        self.api.quote.set_on_tick_stk_v1_callback(self.on_tick_stk)
        self.api.quote.set_on_bidask_stk_v1_callback(self.on_bidask_stk)

    def subscribe_data(self, contract, quote_type="tick"):
        if not contract:
            print("無法訂閱：無效的商品檔")
            return
        
        # data type
        if quote_type == "tick":
            q_type = sj.constant.QuoteType.Tick
        elif quote_type == "bidask":
            q_type = sj.constant.QuoteType.BidAsk
        else:
            print("不支援的 quote_type")
            return

        print(f"--- 訂閱 {contract.code} {quote_type.upper()} ---")
        self.api.quote.subscribe(
            contract,
            quote_type=q_type,
            version=sj.constant.QuoteVersion.v1
        )

    def on_tick_stk(self, exchange: sj.Exchange, tick: sj.TickSTKv1):
        code = tick.code
        price = float(tick.close)
        volume = tick.volume
        tick_time = tick.datetime.replace(second=0, microsecond=0)

        if code not in self.kbars: self.kbars[code] = None
        current_kbar = self.kbars[code]

        if current_kbar is None or current_kbar['time'] != tick_time:
            if current_kbar is not None:
                self.save_to_database(code, current_kbar)
            
            self.kbars[code] = {'time': tick_time, 'open': price, 'high': price, 'low': price, 'close': price, 'volume': volume}
        else:
            current_kbar['high'] = max(current_kbar['high'], price)
            current_kbar['low'] = min(current_kbar['low'], price)
            current_kbar['close'] = price
            current_kbar['volume'] += volume

    def on_bidask_stk(self, exchange: sj.Exchange, bidask: sj.BidAskSTKv1):
        """處理最佳五檔並寫入資料庫"""
        best_bid = bidask.bid_price[0] if bidask.bid_price else None
        best_ask = bidask.ask_price[0] if bidask.ask_price else None
        
        # 印出至終端機監控
        print(f"[BidAsk] {bidask.code} | 買一: {best_bid} | 賣一: {best_ask}")
        
        # 寫入資料庫
        self.db.add_bidask(
            code=bidask.code, 
            ts=bidask.datetime, 
            best_bid=best_bid, 
            best_ask=best_ask
        )

    def save_to_database(self, code, kbar):
        """正式寫入 SQLite 資料庫"""
        print(f"📝 正在存入資料庫: {code} @ {kbar['time']}")
        self.db.add_kbar(code, kbar)