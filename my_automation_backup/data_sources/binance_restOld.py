# data_sources/binance_rest.py
import requests
import time
import os

_BASE    = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(_BASE, "market_data", "binance")
BASE_URL = "https://api.binance.com/api/v3"

class BinanceREST:
    _total_calls = 0

    def __init__(self):
        os.makedirs(DATA_DIR, exist_ok=True)
        self.symbols_dir = os.path.join(DATA_DIR, "symbols")
        os.makedirs(self.symbols_dir, exist_ok=True)

        self._lock = __import__('threading').Lock()
        self._load_all()
        print("✅ [BinanceREST] Initialized (TSV storage)")

    def _get_symbol_file(self, symbol):
        return os.path.join(self.symbols_dir, f"{symbol}.tsv")

    def _load_symbol(self, symbol):
        filepath = self._get_symbol_file(symbol)
        if os.path.exists(filepath):
            try:
                candles = []
                with open(filepath, 'r') as f:
                    first_line = True
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        if first_line and line.startswith('timestamp'):
                            first_line = False
                            continue
                        first_line = False
                        parts = line.split('\t')
                        if len(parts) >= 6:
                            candles.append({
                                "timestamp": int(parts[0]),
                                "open":      float(parts[1]),
                                "high":      float(parts[2]),
                                "low":       float(parts[3]),
                                "close":     float(parts[4]),
                                "volume":    float(parts[5]),
                                "closed":    True
                            })
                return candles
            except Exception as e:
                print(f"❌ [BinanceREST] TSV load error for {symbol}: {e}")
                return []
        return []

    def _save_symbol(self, symbol, candles):
        filepath = self._get_symbol_file(symbol)
        try:
            with open(filepath, 'w') as f:
                for c in candles:
                    line = f"{c['timestamp']}\t{c['open']}\t{c['high']}\t{c['low']}\t{c['close']}\t{c['volume']}\n"
                    f.write(line)
        except Exception as e:
            print(f"❌ [BinanceREST] TSV save error for {symbol}: {e}")

    def _load_all(self):
        self.candles = {}
        if os.path.exists(self.symbols_dir):
            for filename in os.listdir(self.symbols_dir):
                if filename.endswith('.tsv') and not filename.endswith('_cvd.tsv') and not filename.endswith('_depth.tsv'):
                    sym = filename[:-4]
                    self.candles[sym] = self._load_symbol(sym)
        total = sum(len(v) for v in self.candles.values())
        print(f"✅ [BinanceREST] Loaded {len(self.candles)} symbols ({total} candles) from TSV")

    def needs_fill(self, symbol, minutes=120):
        candles = self.candles.get(symbol.lower(), [])
        closed = [c for c in candles if c.get('closed', True)]
        if len(closed) >= minutes:
            last_ts = closed[-1]['timestamp'] / 1000
            if time.time() - last_ts < 180:
                return False
        return True

    def fill_gaps(self, symbol, minutes=120):
        sym = symbol.upper()
        if not self.needs_fill(symbol, minutes):
            print(f"[BinanceREST] {sym}: data OK, skipping REST call")
            return

        BinanceREST._total_calls += 1
        print(f"[BinanceREST] Fetching {sym} (REST call #{BinanceREST._total_calls})")

        # Fetch exactly 'minutes' candles (default 120 = 2 hours)
        klines = self._fetch_klines(sym, limit=minutes)
        if not klines:
            print(f"❌ [BinanceREST] No data returned for {sym}")
            return

        with self._lock:
            existing = self.candles.get(symbol.lower(), [])
            existing_ts = {c['timestamp'] for c in existing}
            new_ones = [c for c in klines if c['timestamp'] not in existing_ts]
            if new_ones:
                merged = existing + new_ones
                merged.sort(key=lambda c: c['timestamp'])
                # Keep last 360 candles (6 hours) to avoid unlimited growth
                self.candles[symbol.lower()] = merged[-360:]
                self._save_symbol(symbol.lower(), self.candles[symbol.lower()])
                print(f"✅ [BinanceREST] {sym}: added {len(new_ones)} candles (target {minutes})")
            else:
                print(f"[BinanceREST] {sym}: no new candles found")

    def _fetch_klines(self, symbol, interval="1m", limit=120):
        try:
            r = requests.get(
                f"{BASE_URL}/klines",
                params={"symbol": symbol, "interval": interval, "limit": limit},
                timeout=10
            )
            r.raise_for_status()
            data = r.json()
            result = []
            for c in data:
                result.append({
                    "timestamp": c[0],
                    "open":      float(c[1]),
                    "high":      float(c[2]),
                    "low":       float(c[3]),
                    "close":     float(c[4]),
                    "volume":    float(c[5]),
                    "closed":    True
                })
            return result
        except Exception as e:
            print(f"❌ [BinanceREST] Fetch error for {symbol}: {e}")
            return []

    @classmethod
    def get_total_calls(cls):
        return cls._total_calls

    @classmethod
    def reset_calls(cls):
        cls._total_calls = 0

    def get_candles_for_symbol(self, symbol):
        return self.candles.get(symbol.lower(), [])

print("✅ [binance_rest] Module loaded")