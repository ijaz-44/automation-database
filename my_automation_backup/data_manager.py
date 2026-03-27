# data_manager.py
import time
from data_sources import DataHub, FinnhubWebSocket
from pairs_config import get_ws_pairs

# ========== YOUR FINNHUB API KEY (hardcoded) ==========
FINNHUB_API_KEY = "d6u7u8pr01qp1k9bjte0d6u7u8pr01qp1k9bjteg"
# ======================================================

class DataManager:
    def __init__(self):
        self.hub = DataHub()
        self.cache = {}
        self.cache_time = {}
        self.is_initialized = False
        print("[DataManager] Initialized")

    def initialize(self, finnhub_api_key=None):
        """Initialize with Finnhub WebSocket as primary source."""
        if self.is_initialized:
            return len(self.cache) // 2  # each pair has one key per timeframe? we store by symbol_interval

        print("[DataManager] Initializing...")

        ws_pairs = get_ws_pairs()   # all 139 symbols

        # Use the provided key or the hardcoded constant
        key = finnhub_api_key if finnhub_api_key else FINNHUB_API_KEY

        if key:
            try:
                finnhub = FinnhubWebSocket(key)
                finnhub.connect(ws_pairs)   # subscribe to ALL symbols in one connection
                self.hub.add_source(finnhub, priority=1)   # highest priority
                print(f"[DataManager] ✅ Finnhub connected – live data for all {len(ws_pairs)} symbols")
            except Exception as e:
                print(f"[DataManager] ❌ Finnhub connection failed: {e}")
                print(f"[DataManager] Using dummy data only")
        else:
            print("[DataManager] No Finnhub API key – using dummy data only")

        # Initialize empty cache for all pairs
        for pair in ws_pairs:
            self.cache[f"{pair}_5m"] = []
            self.cache_time[f"{pair}_5m"] = time.time()

        self.is_initialized = True
        print(f"[DataManager] Ready – {len(ws_pairs)} pairs")
        return len(ws_pairs)

    def get_price(self, symbol, require_confirmation=False):
        """Get price from DataHub (will try sources in priority order)."""
        sym = symbol.lower().replace("/", "")
        price = self.hub.get_price(sym, require_confirmation)
        if price:
            return price
        # Fallback to cache if available
        for interval in ["1m", "5m", "15m"]:
            key = f"{symbol}_{interval}"
            if key in self.cache and self.cache[key]:
                return float(self.cache[key][-1]["close"])
        return 0

    def get_data(self, symbol, interval="5m", limit=70):
        """Get candlestick data; live price from WebSocket, fallback to cache."""
        key = f"{symbol}_{interval}"
        price = self.get_price(symbol)

        if price and price > 0:
            current_time = int(time.time() * 1000)
            live_candle = {
                'timestamp': current_time,
                'open': price,
                'high': price,
                'low': price,
                'close': price,
                'volume': 0
            }
            if key not in self.cache:
                self.cache[key] = []
            self.cache[key].append(live_candle)
            if len(self.cache[key]) > limit:
                self.cache[key] = self.cache[key][-limit:]
            return self.cache[key][-limit:]

        if key in self.cache and len(self.cache[key]) >= limit:
            return self.cache[key][-limit:]

        # No data at all – return empty (no dummy generation)
        return []

    def stage_z(self, symbol, interval="15m"):
        """Fast live price for scanner"""
        sym = symbol.upper().strip().replace("/", "")
        live = self.get_price(sym)
        rows = self.get_data(sym, interval, 5)
        if not rows:
            return {
                "live_price": 0,
                "change_pct": 0,
                "direction": "FLAT",
                "source": "offline",
                "symbol": sym,
                "last_close": 0,
                "ts": time.time()
            }
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
            "source": "Live" if live else "Cache",
            "ts": time.time(),
        }

    def stage_a(self, symbol, interval="15m"):
        """Single pair live OHLC for GO layer"""
        sym = symbol.upper().strip().replace("/", "")
        rows = self.get_data(sym, interval, 20)
        live = self.get_price(sym)
        if not rows:
            return {"status": "No data", "candle": {}, "momentum": "FLAT"}
        current = rows[-1].copy()
        if live and live > 0:
            current["close"] = live
            if live > current["high"]: current["high"] = live
            if live < current["low"]: current["low"] = live
        o, h, l, c = current["open"], current["high"], current["low"], current["close"]
        body = abs(c - o)
        rng = h - l if h > l else 0.0001
        body_pct = round(body / rng * 100, 1)
        if c > o:
            bias = "BULL"
            pct = round((c - o) / o * 100, 4) if o > 0 else 0
        elif c < o:
            bias = "BEAR"
            pct = round((o - c) / o * 100, 4) if o > 0 else 0
        else:
            bias = "FLAT"
            pct = 0
        prev = rows[-2]["close"] if len(rows) >= 2 else o
        chg = round((c - prev) / prev * 100, 4) if prev > 0 else 0
        return {
            "symbol": sym,
            "interval": interval,
            "candle": {"open": o, "high": h, "low": l, "close": c, "body_pct": body_pct, "bias": bias, "move_pct": pct},
            "change_pct": chg,
            "live_price": live if live else c,
            "source": "Live" if live else "Cache",
            "ts": time.time(),
        }

    def stage_d(self, symbol, interval="15m"):
        """Full live data for detail analysis"""
        sym = symbol.upper().strip().replace("/", "")
        rows = self.get_data(sym, interval, 50)
        live = self.get_price(sym)
        if not rows:
            return {"status": "No data", "live_analysis": {}}
        if live and live > 0:
            rows[-1] = rows[-1].copy()
            rows[-1]["close"] = live
            if live > rows[-1]["high"]: rows[-1]["high"] = live
            if live < rows[-1]["low"]: rows[-1]["low"] = live
        closes = [r["close"] for r in rows]
        current = closes[-1]
        roc3 = (closes[-1] - closes[-3]) / closes[-3] * 100 if closes[-3] > 0 else 0
        roc1 = (closes[-1] - closes[-2]) / closes[-2] * 100 if closes[-2] > 0 else 0
        trs = []
        for i in range(1, len(rows)):
            h, l, pc = rows[i]["high"], rows[i]["low"], rows[i-1]["close"]
            trs.append(max(h - l, abs(h - pc), abs(l - pc)))
        live_atr = round(sum(trs[-14:]) / 14, 6) if len(trs) >= 14 else 0
        if live_atr > 0:
            sl_buy = round(current - live_atr * 1.5, 6)
            tp_buy = round(current + live_atr * 2.5, 6)
            sl_sell = round(current + live_atr * 1.5, 6)
            tp_sell = round(current - live_atr * 2.5, 6)
        else:
            sl_buy = tp_buy = sl_sell = tp_sell = 0
        hi50 = max(r["high"] for r in rows[-50:])
        lo50 = min(r["low"] for r in rows[-50:])
        rng50 = hi50 - lo50
        pos_pct = round((current - lo50) / rng50 * 100, 1) if rng50 > 0 else 50
        signal = "WAIT"
        score = 50
        if roc3 > 0.08 and roc1 > 0:
            signal = "BUY"
            score = min(75, 55 + int(roc3 * 20))
        elif roc3 < -0.08 and roc1 < 0:
            signal = "SELL"
            score = min(75, 55 + int(abs(roc3) * 20))
        return {
            "symbol": sym,
            "interval": interval,
            "live_price": current,
            "live_atr": live_atr,
            "roc_1": round(roc1, 4),
            "roc_3": round(roc3, 4),
            "pos_in_range": pos_pct,
            "range_high": round(hi50, 6),
            "range_low": round(lo50, 6),
            "signal": signal,
            "score": score,
            "sl_if_buy": sl_buy,
            "tp_if_buy": tp_buy,
            "sl_if_sell": sl_sell,
            "tp_if_sell": tp_sell,
            "source": "Live" if live else "Cache",
            "ts": time.time(),
        }

    def get_cache_info(self):
        return {
            "total_symbols": len(self.cache),
            "websocket_connected": self.hub.is_any_connected(),
            "sources": [s.name for _, s in self.hub.sources]
        }

    def clear_cache(self):
        self.cache.clear()
        self.cache_time.clear()


# Global instance
_data_manager = None

def get_data_manager():
    global _data_manager
    if _data_manager is None:
        _data_manager = DataManager()
    return _data_manager

# Backward compatibility functions
def get_rows(symbol, interval="5m", limit=50):
    return get_data_manager().get_data(symbol, interval, limit)

def get_price(symbol):
    return get_data_manager().get_price(symbol)

def prefetch(pairs, interval="5m", finnhub_api_key=None):
    """Initialize data with optional Finnhub API key (will fallback to hardcoded)"""
    return get_data_manager().initialize(finnhub_api_key)

def start_ws():
    return get_data_manager().hub.is_any_connected()

def cache_info():
    return get_data_manager().get_cache_info()

def stage_z(symbol, interval="15m"):
    return get_data_manager().stage_z(symbol, interval)

def stage_a(symbol, interval="15m"):
    return get_data_manager().stage_a(symbol, interval)

def stage_d(symbol, interval="15m"):
    return get_data_manager().stage_d(symbol, interval)