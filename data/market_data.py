import shioaji as sj

class MarketDataManager:
    def __init__(self, api: sj.Shioaji, db):
        self.api = api
        self.db = db
        self.kbars = {}
        self.api.quote.set_on_tick_stk_v1_callback(self.on_tick_stk)
        
        # 現貨 (STK) Callback
        self.api.quote.set_on_tick_stk_v1_callback(self.on_tick_stk)
        self.api.quote.set_on_bidask_stk_v1_callback(self.on_bidask_stk)

        # 期貨 (FUT) Callback
        self.api.quote.set_on_tick_fop_v1_callback(self.on_tick_fop)
        self.api.quote.set_on_bidask_fop_v1_callback(self.on_bidask_fop)

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

    # ==========================================
    # 現貨 (Stock)
    # ==========================================
    def on_tick_stk(self, exchange: sj.Exchange, tick: sj.TickSTKv1):
        self._process_tick(tick.code, float(tick.close), tick.volume, tick.datetime)

    def on_bidask_stk(self, exchange: sj.Exchange, bidask: sj.BidAskSTKv1):
        self._process_bidask(bidask.code, bidask.datetime, bidask.bid_price, bidask.ask_price)

    # ==========================================
    # 期貨 (Future)
    # ==========================================
    def on_tick_fop(self, exchange: sj.Exchange, tick: sj.TickFOPv1):
        self._process_tick(tick.code, float(tick.close), tick.volume, tick.datetime)

    def on_bidask_fop(self, exchange: sj.Exchange, bidask: sj.BidAskFOPv1):
        self._process_bidask(bidask.code, bidask.datetime, bidask.bid_price, bidask.ask_price)

    # ==========================================
    # 共用核心模組 (將重複邏輯抽離，方便維護)
    # ==========================================
    def _process_tick(self, code, price, volume, dt):
        """將收到的即時 Tick 動態組裝成 1 分鐘 KBar"""
        tick_time = dt.replace(second=0, microsecond=0)

        if code not in self.kbars: 
            self.kbars[code] = None
            
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

    def _process_bidask(self, code, dt, bid_price_list, ask_price_list):
        """處理最佳五檔並寫入資料庫"""
        best_bid = bid_price_list[0] if bid_price_list else None
        best_ask = ask_price_list[0] if ask_price_list else None
        
        print(f"[BidAsk] {code} | 買一: {best_bid} | 賣一: {best_ask}")
        
        self.db.add_bidask(
            code=code, 
            ts=dt, 
            best_bid=best_bid, 
            best_ask=best_ask
        )

    def save_to_database(self, code, kbar):
        print(f"📝 正在存入資料庫: {code} @ {kbar['time']}")
        self.db.add_kbar(code, kbar)