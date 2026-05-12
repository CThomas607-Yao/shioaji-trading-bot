import shioaji as sj

class MarketDataManager:
    def __init__(self, api: sj.Shioaji, db):
        self.api = api
        self.db = db
        self.kbars = {}
        
        # ==========================================
        # 綁定報價 Callback (同時支援現貨與期貨)
        # ==========================================
        self.api.quote.set_on_tick_stk_v1_callback(self.on_tick_stk)
        self.api.quote.set_on_bidask_stk_v1_callback(self.on_bidask_stk)
        
        self.api.quote.set_on_tick_fop_v1_callback(self.on_tick_fop)
        self.api.quote.set_on_bidask_fop_v1_callback(self.on_bidask_fop)

        # for reconnection
        self.api.set_order_callback(self.on_event)
        self.active_subscriptions = []

    # ==========================================
    # 系統事件處理 (防斷線機制)
    # ==========================================
    def on_event(self, event_code: int, event_name: str, description: str):
        """處理 Shioaji 底層系統事件 (例如斷線重連)"""
        if event_code == 13 or "reconnected" in event_name.lower():
            print(f"\n🔔 [系統事件] 偵測到斷線重連 ({event_name})！準備自動恢復報價訂閱...")
            self._resubscribe_all()
        else:
            print(f" [系統事件] 代碼: {event_code} | 名稱: {event_name} | 說明: {description}")

    def _resubscribe_all(self):
        """斷線後自動重新訂閱所有已記錄的合約"""
        if not self.active_subscriptions:
            return
            
        print(f"🔄 正在重新訂閱 {len(self.active_subscriptions)} 組行情...")
        for sub in self.active_subscriptions:
            self.api.quote.subscribe(
                sub["contract"],
                quote_type=sub["quote_type"],
                version=sj.constant.QuoteVersion.v1
            )
            print(f"✅ 已恢復: {sub['contract'].code}")

    # ==========================================
    # 行情訂閱入口
    # ==========================================
    def subscribe_data(self, contract, quote_type="tick"):
        if not contract:
            print("無法訂閱：無效的合約")
            return
        
        if quote_type == "tick":
            q_type = sj.constant.QuoteType.Tick
        elif quote_type == "bidask":
            q_type = sj.constant.QuoteType.BidAsk
        else:
            print("不支援的 quote_type")
            return

        print(f"--- 發送訂閱 {contract.code} {quote_type.upper()} ---")
        self.api.quote.subscribe(
            contract,
            quote_type=q_type,
            version=sj.constant.QuoteVersion.v1
        )
        
        sub_record = {"contract": contract, "quote_type": q_type}
        if sub_record not in self.active_subscriptions:
            self.active_subscriptions.append(sub_record)

    # ==========================================
    # 現貨 (Stock) 處理邏輯
    # ==========================================
    def on_tick_stk(self, exchange: sj.Exchange, tick: sj.TickSTKv1):
        self._process_tick(tick.code, float(tick.close), tick.volume, tick.datetime)

    def on_bidask_stk(self, exchange: sj.Exchange, bidask: sj.BidAskSTKv1):
        best_bid = float(bidask.bid_price[0]) if bidask.bid_price else None
        best_ask = float(bidask.ask_price[0]) if bidask.ask_price else None
        self._process_bidask(bidask.code, bidask.datetime, best_bid, best_ask)

    # ==========================================
    # 期貨 (Future) 處理邏輯
    # ==========================================
    def on_tick_fop(self, exchange: sj.Exchange, tick: sj.TickFOPv1):
        self._process_tick(tick.code, float(tick.close), tick.volume, tick.datetime)

    def on_bidask_fop(self, exchange: sj.Exchange, bidask: sj.BidAskFOPv1):
        best_bid = float(bidask.bid_price[0]) if bidask.bid_price else None
        best_ask = float(bidask.ask_price[0]) if bidask.ask_price else None
        self._process_bidask(bidask.code, bidask.datetime, best_bid, best_ask)

    # ==========================================
    # 共用核心模組 (將重複邏輯抽離，方便維護)
    # ==========================================
    def _process_tick(self, code, price, volume, dt):
        """將收到的即時 Tick 動態組裝成 1 分鐘 KBar 並存入 DB"""
        tick_time = dt.replace(second=0, microsecond=0)

        if code not in self.kbars: 
            self.kbars[code] = None
            
        current_kbar = self.kbars[code]

        if current_kbar is None or current_kbar['time'] != tick_time:
            if current_kbar is not None:
                self.save_to_database(code, current_kbar)
            
            self.kbars[code] = {
                'time': tick_time, 'open': price, 'high': price, 
                'low': price, 'close': price, 'volume': volume
            }
        else:
            current_kbar['high'] = max(current_kbar['high'], price)
            current_kbar['low'] = min(current_kbar['low'], price)
            current_kbar['close'] = price
            current_kbar['volume'] += volume

    def _process_bidask(self, code, dt, best_bid, best_ask):
        """處理最佳買賣一檔並寫入資料庫"""
        print(f"[BidAsk] {code} | 買一: {best_bid} | 賣一: {best_ask}")
        self.db.add_bidask(code=code, ts=dt, best_bid=best_bid, best_ask=best_ask)

    def save_to_database(self, code, kbar):
        print(f"📝 正在存入資料庫: {code} @ {kbar['time']}")
        self.db.add_kbar(code, kbar)