# data_sources/websocket.py - PRIMARY LIVE DATA SOURCE
# Moved from Groups/group_d/D04_websocket.py
# Now primary source for all live data

import os, sys, time, json
import websocket
import threading

# Add parent path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

class WebSocketSource:
    def __init__(self):
        self.ws = None
        self.ws_prices = {}
        self.is_connected = False
        self.subscribed_symbols = []
        print("[WebSocketSource] Initialized - PRIMARY LIVE DATA SOURCE")
    
    def connect(self, symbols=None):
        """Connect to Binance WebSocket stream"""
        if symbols is None:
            symbols = ["btcusdt", "ethusdt"]  # Default
        
        self.subscribed_symbols = [s.lower() for s in symbols]
        
        # Create WebSocket connection
        stream_name = "@ticker".join(self.subscribed_symbols)
        ws_url = f"wss://stream.binance.com:9443/ws/{stream_name}"
        
        self.ws = websocket.WebSocketApp(
            ws_url,
            on_open=self._on_open,
            on_message=self._on_message,
            on_error=self._on_error,
            on_close=self._on_close
        )
        
        # Run in separate thread
        ws_thread = threading.Thread(target=self.ws.run_forever)
        ws_thread.daemon = True
        ws_thread.start()
        
        print(f"[WebSocketSource] Connecting to {len(symbols)} symbols...")
        return True
    
    def _on_open(self, ws):
        self.is_connected = True
        print("[WebSocketSource] ✅ CONNECTED - Primary live data active")
    
    def _on_message(self, ws, message):
        """Handle incoming price updates"""
        try:
            data = json.loads(message)
            symbol = data.get('s', '').lower()
            price = float(data.get('c', 0))
            
            if symbol and price > 0:
                self.ws_prices[symbol] = {
                    'price': price,
                    'timestamp': time.time(),
                    'change_24h': float(data.get('P', 0)),
                    'high_24h': float(data.get('h', 0)),
                    'low_24h': float(data.get('l', 0)),
                    'volume': float(data.get('v', 0))
                }
        except Exception as e:
            print(f"[WebSocketSource] Message error: {e}")
    
    def _on_error(self, ws, error):
        print(f"[WebSocketSource] Error: {error}")
        self.is_connected = False
    
    def _on_close(self, ws, close_status_code, close_msg):
        print("[WebSocketSource] Connection closed")
        self.is_connected = False
    
    def get_price(self, symbol):
        """Get live price from WebSocket"""
        sym = symbol.lower().replace("/", "")
        data = self.ws_prices.get(sym)
        if data:
            return data['price']
        return None
    
    def get_price_data(self, symbol):
        """Get full price data"""
        sym = symbol.lower().replace("/", "")
        return self.ws_prices.get(sym)
    
    def is_live(self, symbol):
        """Check if symbol has live data"""
        sym = symbol.lower().replace("/", "")
        if sym in self.ws_prices:
            age = time.time() - self.ws_prices[sym].get('timestamp', 0)
            return age < 60  # Data less than 60 seconds old
        return False
    
    def disconnect(self):
        """Close WebSocket connection"""
        if self.ws:
            self.ws.close()
        self.is_connected = False
        print("[WebSocketSource] Disconnected")

# Stage functions for compatibility with old D04 system
def stage_z(symbol, interval="15m", ws_prices=None):
    """
    Stage Z - Fast live price for scanner
    Uses WebSocket cached price — instant, no API call
    """
    sym = symbol.upper().strip().replace("/","")
    live = ws_prices.get(sym.lower()) if ws_prices else None
    
    # Import here to avoid circular dependency
    try:
        from data_manager import get_rows
        rows = get_rows(sym, interval, 5)
    except:
        rows = []
    
    if not rows:
        return {"live_price": 0, "change_pct": 0,
                "direction": "FLAT", "source": "offline"}
    
    last_close = rows[-1]["close"]
    prev_close = rows[-2]["close"] if len(rows) >= 2 else last_close
    price = live if live else last_close
    
    if prev_close > 0:
        change_pct = round((price - prev_close) / prev_close * 100, 4)
    else:
        change_pct = 0
    
    direction = "UP" if change_pct > 0.01 else "DOWN" if change_pct < -0.01 else "FLAT"
    
    return {
        "symbol": sym,
        "live_price": price,
        "last_close": last_close,
        "change_pct": change_pct,
        "direction": direction,
        "source": "WebSocket" if live else "REST cache",
        "ts": time.time(),
    }

def stage_a(symbol, interval="15m", ws_prices=None):
    """Stage A - Single pair live OHLC for GO layer"""
    sym = symbol.upper().strip().replace("/","")
    
    try:
        from data_manager import get_rows
        rows = get_rows(sym, interval, 20)
    except:
        rows = []
    
    live = ws_prices.get(sym.lower()) if ws_prices else None
    
    if not rows:
        return {"status": "No data", "candle": {}, "momentum": "FLAT"}
    
    current = rows[-1].copy()
    if live:
        current["close"] = live
        if live > current["high"]: current["high"] = live
        if live < current["low"]: current["low"] = live
    
    o, h, l, c = current["open"], current["high"], current["low"], current["close"]
    body = abs(c - o)
    rng = h - l if h > l else 0.0001
    body_pct = round(body / rng * 100, 1)
    
    if c > o:
        bias = "BULL"
        pct = round((c-o)/o*100, 4) if o > 0 else 0
    elif c < o:
        bias = "BEAR"
        pct = round((o-c)/o*100, 4) if o > 0 else 0
    else:
        bias = "FLAT"
        pct = 0
    
    vols = [r["volume"] for r in rows[-5:] if r.get("volume",0)>0]
    vol_trend = "RISING" if len(vols)>=2 and vols[-1]>vols[-2] else "FALLING"
    
    prev = rows[-2]["close"] if len(rows)>=2 else o
    chg = round((c-prev)/prev*100,4) if prev>0 else 0
    
    return {
        "symbol": sym,
        "interval": interval,
        "candle": {
            "open": o,
            "high": h,
            "low": l,
            "close": c,
            "body_pct": body_pct,
            "bias": bias,
            "move_pct": pct,
        },
        "change_pct": chg,
        "vol_trend": vol_trend,
        "live_price": live if live else c,
        "source": "WS+REST" if live else "REST cache",
        "ts": time.time(),
    }

def stage_d(symbol, interval="15m", ws_prices=None):
    """Stage D - Full live data for detail analysis"""
    sym = symbol.upper().strip().replace("/","")
    
    try:
        from data_manager import get_rows
        rows = get_rows(sym, interval, 50)
    except:
        rows = []
    
    live = ws_prices.get(sym.lower()) if ws_prices else None
    
    if not rows:
        return {"status":"No data","live_analysis":{}}
    
    if live:
        rows[-1] = rows[-1].copy()
        rows[-1]["close"] = live
        if live > rows[-1]["high"]: rows[-1]["high"] = live
        if live < rows[-1]["low"]: rows[-1]["low"] = live
    
    closes = [r["close"] for r in rows]
    current = closes[-1]
    
    roc3 = (closes[-1]-closes[-3])/closes[-3]*100 if closes[-3]>0 else 0
    roc1 = (closes[-1]-closes[-2])/closes[-2]*100 if closes[-2]>0 else 0
    
    trs=[]
    for i in range(1,len(rows)):
        h,l,pc=rows[i]["high"],rows[i]["low"],rows[i-1]["close"]
        trs.append(max(h-l,abs(h-pc),abs(l-pc)))
    live_atr = round(sum(trs[-14:])/14,6) if len(trs)>=14 else 0
    
    if live_atr > 0:
        sl_buy = round(current - live_atr*1.5, 6)
        tp_buy = round(current + live_atr*2.5, 6)
        sl_sell = round(current + live_atr*1.5, 6)
        tp_sell = round(current - live_atr*2.5, 6)
    else:
        sl_buy=tp_buy=sl_sell=tp_sell=0
    
    hi50 = max(r["high"] for r in rows[-50:])
    lo50 = min(r["low"] for r in rows[-50:])
    rng50 = hi50 - lo50
    pos_pct = round((current-lo50)/rng50*100,1) if rng50>0 else 50
    
    signal = "WAIT"
    score = 50
    if roc3 > 0.08 and roc1 > 0:
        signal = "BUY"; score = min(75, 55+int(roc3*20))
    elif roc3 < -0.08 and roc1 < 0:
        signal = "SELL"; score = min(75, 55+int(abs(roc3)*20))
    
    return {
        "symbol": sym,
        "interval": interval,
        "live_price": current,
        "live_atr": live_atr,
        "roc_1": round(roc1,4),
        "roc_3": round(roc3,4),
        "pos_in_range": pos_pct,
        "range_high": round(hi50,6),
        "range_low": round(lo50,6),
        "signal": signal,
        "score": score,
        "sl_if_buy": sl_buy,
        "tp_if_buy": tp_buy,
        "sl_if_sell": sl_sell,
        "tp_if_sell": tp_sell,
        "source": "WS+REST" if live else "REST cache",
        "ts": time.time(),
    }

def analyze(symbol, interval="15m", stage="z", ws_prices=None):
    """Unified analysis function"""
    if stage == "z":
        return stage_z(symbol, interval, ws_prices)
    elif stage == "a":
        return stage_a(symbol, interval, ws_prices)
    elif stage == "d":
        return stage_d(symbol, interval, ws_prices)
    return {}

# Export
__all__ = ['WebSocketSource', 'analyze', 'stage_z', 'stage_a', 'stage_d']