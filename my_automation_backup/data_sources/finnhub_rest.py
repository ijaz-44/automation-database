# data_sources/finnhub_rest.py
import requests
import json
import time
import datetime
import os

_BASE    = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(_BASE, "market_data", "finnhub")

FINNHUB_API_KEY = "d6u7u8pr01qp1k9bjte0d6u7u8pr01qp1k9bjteg"
BASE_URL        = "https://finnhub.io/api/v1"
HOLIDAYS        = {(1,1), (12,25), (7,4)}

class FinnhubREST:
    _total_calls = 0

    def __init__(self):
        os.makedirs(DATA_DIR, exist_ok=True)
        self.data_dir     = DATA_DIR
        self.candles_file = os.path.join(DATA_DIR, "candles.tsv")
        self._lock        = __import__('threading').Lock()
        self._load()
        print("✅ [FinnhubREST] Initialized (TSV storage)")

    def _load(self):
        self.candles = {}
        if os.path.exists(self.candles_file):
            try:
                with open(self.candles_file, 'r') as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        parts = line.split('\t')
                        if len(parts) >= 7:
                            sym = parts[0]
                            candle = {
                                "timestamp": int(parts[1]),
                                "open":      float(parts[2]),
                                "high":      float(parts[3]),
                                "low":       float(parts[4]),
                                "close":     float(parts[5]),
                                "volume":    float(parts[6]),
                                "closed":    True
                            }
                            if sym not in self.candles:
                                self.candles[sym] = []
                            self.candles[sym].append(candle)
            except Exception as e:
                print(f"❌ [FinnhubREST] TSV load error: {e}")
                self.candles = {}

    def _save(self):
        try:
            with open(self.candles_file, 'w') as f:
                for sym, candles in self.candles.items():
                    for c in candles:
                        line = f"{sym}\t{c['timestamp']}\t{c['open']}\t{c['high']}\t{c['low']}\t{c['close']}\t{c['volume']}\n"
                        f.write(line)
        except Exception as e:
            print(f"❌ [FinnhubREST] TSV save error: {e}")

    @staticmethod
    def _is_market_closed():
        now = datetime.datetime.utcnow()
        if now.weekday() >= 5:
            return True
        if (now.month, now.day) in HOLIDAYS:
            return True
        return False

    def needs_fill(self, symbol, minutes=60):
        sym     = symbol.lower().replace("/", "")
        candles = self.candles.get(sym, [])
        closed  = [c for c in candles if c.get('closed', True)]
        if len(closed) >= minutes:
            last_ts = closed[-1]['timestamp'] / 1000
            if time.time() - last_ts < 180:
                return False
        return True

    def fill_gaps(self, symbol, minutes=60):
        if self._is_market_closed():
            print(f"[FinnhubREST] Market closed, skipping {symbol}")
            return
        sym_key = symbol.lower().replace("/", "")
        if not self.needs_fill(symbol, minutes):
            print(f"[FinnhubREST] {symbol}: data OK, skipping REST call")
            return

        FinnhubREST._total_calls += 1
        print(f"[FinnhubREST] Fetching {symbol} (REST call #{FinnhubREST._total_calls})")

        klines = self._fetch(symbol, limit=max(minutes+10, 100))
        if not klines:
            print(f"❌ [FinnhubREST] No data for {symbol}")
            return

        with self._lock:
            existing    = self.candles.get(sym_key, [])
            existing_ts = {c['timestamp'] for c in existing}
            new_ones    = [c for c in klines if c['timestamp'] not in existing_ts]
            if new_ones:
                merged = existing + new_ones
                merged.sort(key=lambda c: c['timestamp'])
                self.candles[sym_key] = merged[-120:]
                self._save()
                print(f"✅ [FinnhubREST] {symbol}: +{len(new_ones)} candles")
            else:
                print(f"[FinnhubREST] {symbol}: no new candles")

    def _fetch(self, symbol, resolution="1", limit=100):
        to_ts   = int(time.time())
        from_ts = to_ts - (limit * 60)

        su = symbol.upper().replace("/", "")
        if len(su) == 6 and su.isalpha():
            sym      = f"OANDA:{su[:3]}/{su[3:]}"
            endpoint = f"{BASE_URL}/forex/candle"
        else:
            sym      = su
            endpoint = f"{BASE_URL}/stock/candle"

        try:
            r = requests.get(endpoint, params={
                "symbol": sym, "resolution": resolution,
                "from": from_ts, "to": to_ts,
                "token": FINNHUB_API_KEY
            }, timeout=10)
            r.raise_for_status()
            data = r.json()
            if data.get('s') != 'ok':
                return []
            return [{
                "timestamp": data['t'][i] * 1000,
                "open":      float(data['o'][i]),
                "high":      float(data['h'][i]),
                "low":       float(data['l'][i]),
                "close":     float(data['c'][i]),
                "volume":    float(data['v'][i]),
                "closed":    True
            } for i in range(len(data.get('t', [])))]
        except Exception as e:
            print(f"❌ [FinnhubREST] Error {symbol}: {e}")
            return []

    # ------------------- ### [NEW] Methods for Macro Data -------------------
    def get_macro_history(self, symbol, minutes=120):
        """
        Fetch last `minutes` minutes of 1‑minute candles for a macro symbol (e.g., XAUUSD, USOIL, DXY, ES).
        Returns a tuple (timestamps_list, closes_list) where lists are in chronological order (oldest first).
        If fails, returns ([], []).
        """
        # Map common macro symbols to Finnhub-compatible symbols
        mapping = {
            "XAUUSD": "OANDA:XAU/USD",
            "USOIL":  "OANDA:WTI_USD",
            "DXY":    "DXY",
            "ES":     "ES",
        }
        finnhub_sym = mapping.get(symbol.upper(), symbol)
        # Fetch candles with limit = minutes
        candles = self._fetch(finnhub_sym, resolution="1", limit=minutes)
        if not candles:
            return [], []
        timestamps = [c['timestamp'] for c in candles]
        closes = [c['close'] for c in candles]
        # Return oldest first
        return timestamps, closes

    def get_macro_current(self, symbol):
        """
        Get current price for a macro symbol using the /quote endpoint.
        Returns float price or None on error.
        """
        mapping = {
            "XAUUSD": "OANDA:XAU/USD",
            "USOIL":  "OANDA:WTI_USD",
            "DXY":    "DXY",
            "ES":     "ES",
        }
        finnhub_sym = mapping.get(symbol.upper(), symbol)
        try:
            url = f"{BASE_URL}/quote?symbol={finnhub_sym}&token={FINNHUB_API_KEY}"
            r = requests.get(url, timeout=5)
            if r.status_code == 200:
                data = r.json()
                # 'c' is current price
                return float(data.get('c', 0))
        except Exception as e:
            print(f"❌ [FinnhubREST] get_macro_current({symbol}) error: {e}")
        return None

    @classmethod
    def get_total_calls(cls):
        return cls._total_calls

    @classmethod
    def reset_calls(cls):
        cls._total_calls = 0

print("✅ [finnhub_rest] Module loaded")