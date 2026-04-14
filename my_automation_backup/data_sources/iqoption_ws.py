# data_sources/iqoption_ws.py
import json
import time
import os
import threading

_BASE    = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(_BASE, "market_data", "iqoption")

IQ_EMAIL    = ""
IQ_PASSWORD = ""

IQ_PAIR_MAP = {
    "EURUSD": "EURUSD-OTC", "GBPUSD": "GBPUSD-OTC",
    "USDJPY": "USDJPY-OTC", "USDCHF": "USDCHF-OTC",
    "AUDUSD": "AUDUSD-OTC", "NZDUSD": "NZDUSD-OTC",
    "USDCAD": "USDCAD-OTC", "EURGBP": "EURGBP-OTC",
    "EURJPY": "EURJPY-OTC", "GBPJPY": "GBPJPY-OTC",
    "BTCUSDT": "BITCOIN",   "ETHUSDT": "ETHEREUM",
    "XAUUSD": "XAUUSD-OTC", "XAGUSD": "XAGUSD-OTC",
}

class IQOptionWS:
    _total_calls = 0

    def __init__(self):
        os.makedirs(DATA_DIR, exist_ok=True)
        self.data_dir      = DATA_DIR
        self.candles_file  = os.path.join(DATA_DIR, "candles.tsv")
        self._lock         = threading.Lock()
        self._load_data()
        self._api          = None
        self._connected    = False
        self._running      = False
        self._subscribed   = []
        self._callback     = None
        self._live_candles = {}
        self._fin_thread   = threading.Thread(target=self._finalise_loop, daemon=True)
        self._fin_thread.start()
        print("✅ [IQOptionWS] Initialized (TSV storage)")

    def _load_data(self):
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
                total = sum(len(v) for v in self.candles.values())
                print(f"✅ [IQOptionWS] Loaded {len(self.candles)} symbols ({total} candles) from TSV")
            except Exception as e:
                print(f"❌ [IQOptionWS] Load error: {e}")
                self.candles = {}
        else:
            self.candles = {}

    def _save(self):
        try:
            with open(self.candles_file, 'w') as f:
                for sym, candles in self.candles.items():
                    for c in candles:
                        line = f"{sym}\t{c['timestamp']}\t{c['open']}\t{c['high']}\t{c['low']}\t{c['close']}\t{c['volume']}\n"
                        f.write(line)
        except Exception as e:
            print(f"❌ [IQOptionWS] Save error: {e}")

    def connect(self, symbols, email="", password=""):
        global IQ_EMAIL, IQ_PASSWORD
        if email:
            IQ_EMAIL = email
        if password:
            IQ_PASSWORD = password

        if not IQ_EMAIL or not IQ_PASSWORD:
            print("❌ [IQOptionWS] Email/password not set")
            return False

        try:
            from iqoptionapi.stable_api import IQ_Option
        except ImportError:
            print("❌ [IQOptionWS] iqoptionapi not installed.")
            return False

        self._running = True
        self._api = IQ_Option(IQ_EMAIL, IQ_PASSWORD)
        check, reason = self._api.connect()
        if not check:
            print(f"❌ [IQOptionWS] Connection failed: {reason}")
            return False

        self._connected = True
        self._subscribed = symbols
        print(f"✅ [IQOptionWS] Connected to IQ Option")

        t = threading.Thread(target=self._initial_history, args=(symbols,), daemon=True)
        t.start()
        self._poll_thread = threading.Thread(target=self._poll_loop, daemon=True)
        self._poll_thread.start()
        return True

    def _initial_history(self, symbols):
        for sym in symbols:
            iq_sym = IQ_PAIR_MAP.get(sym.upper(), sym.upper() + "-OTC")
            try:
                IQOptionWS._total_calls += 1
                candles = self._api.get_candles(iq_sym, 60, 60, time.time())
                if candles:
                    self._store_candles(sym.lower(), candles, from_history=True)
                    print(f"✅ [IQOptionWS] History loaded for {sym} ({len(candles)} candles)")
                else:
                    print(f"❌ [IQOptionWS] No history for {sym}")
            except Exception as e:
                print(f"❌ [IQOptionWS] History error {sym}: {e}")
            time.sleep(0.3)

    def _store_candles(self, sym_key, raw_candles, from_history=False):
        formatted = []
        for c in raw_candles:
            try:
                formatted.append({
                    "timestamp": int(c.get('from', c.get('id', 0))) * 1000,
                    "open":      float(c.get('open',  c.get('o', 0))),
                    "high":      float(c.get('max',   c.get('h', 0))),
                    "low":       float(c.get('min',   c.get('l', 0))),
                    "close":     float(c.get('close', c.get('c', 0))),
                    "volume":    float(c.get('volume',c.get('v', 0))),
                    "closed":    True
                })
            except Exception:
                continue

        with self._lock:
            existing    = self.candles.get(sym_key, [])
            existing_ts = {c['timestamp'] for c in existing}
            new_ones    = [c for c in formatted if c['timestamp'] not in existing_ts]
            if new_ones:
                merged = existing + new_ones
                merged.sort(key=lambda c: c['timestamp'])
                self.candles[sym_key] = merged[-120:]
                self._save()

    def _poll_loop(self):
        while self._running and self._connected:
            try:
                for sym in self._subscribed:
                    iq_sym = IQ_PAIR_MAP.get(sym.upper(), sym.upper() + "-OTC")
                    price  = self._api.get_last_quote(iq_sym)
                    if price and price > 0:
                        now    = time.time()
                        minute = int(now / 60)
                        sym_key = sym.lower()
                        with self._lock:
                            lc = self._live_candles.get(sym_key)
                            if lc is None or lc['minute'] != minute:
                                self._live_candles[sym_key] = {
                                    "minute":    minute,
                                    "timestamp": int(minute * 60 * 1000),
                                    "open":  price, "high":  price,
                                    "low":   price, "close": price,
                                    "volume": 0,    "closed": False
                                }
                            else:
                                lc['high']  = max(lc['high'], price)
                                lc['low']   = min(lc['low'],  price)
                                lc['close'] = price
                        if self._callback:
                            self._callback(sym_key, price, now)
            except Exception as e:
                print(f"❌ [IQOptionWS] Poll error: {e}")
            time.sleep(5)

    def _finalise_loop(self):
        last_minute = 0
        while True:
            minute = int(time.time() / 60)
            if minute != last_minute and last_minute != 0:
                with self._lock:
                    for sym, lc in list(self._live_candles.items()):
                        if lc['minute'] == last_minute:
                            closed = dict(lc, closed=True)
                            lst    = self.candles.setdefault(sym, [])
                            self.candles[sym] = [c for c in lst if c['timestamp'] != closed['timestamp']]
                            self.candles[sym].append(closed)
                            self.candles[sym] = self.candles[sym][-120:]
                            del self._live_candles[sym]
                            print(f"[IQOptionWS] ✔ Candle closed: {sym} c={closed['close']}")
                self._save()
            last_minute = minute
            time.sleep(1)

    @property
    def is_connected(self):
        return self._connected

    @property
    def name(self):
        return "IQOptionWS"

    def set_callback(self, cb):
        self._callback = cb

    def get_candles(self, symbol, interval="1m", limit=100):
        sym = symbol.lower()
        with self._lock:
            closed = [c for c in self.candles.get(sym, []) if c.get('closed', True)]
        if not closed:
            return []
        if interval == "1m":
            return closed[-limit:]
        m_map = {"2m":2,"5m":5,"10m":10,"15m":15,"30m":30,"1h":60,"4h":240}
        m = m_map.get(interval, 5)
        agg = []
        for i in range(0, len(closed), m):
            chunk = closed[i:i+m]
            if len(chunk) < m:
                continue
            agg.append({
                "timestamp": chunk[0]["timestamp"],
                "open":      chunk[0]["open"],
                "high":      max(c["high"]   for c in chunk),
                "low":       min(c["low"]    for c in chunk),
                "close":     chunk[-1]["close"],
                "volume":    sum(c["volume"] for c in chunk),
            })
        return agg[-limit:]

    def get_price(self, symbol):
        sym = symbol.lower()
        with self._lock:
            raw = self.candles.get(sym, [])
        return float(raw[-1]['close']) if raw else 0.0

    def get_closed_count(self, symbol):
        sym = symbol.lower()
        with self._lock:
            raw = self.candles.get(sym, [])
        return sum(1 for c in raw if c.get('closed', True))

    def has_enough_data(self, symbol, minutes=60):
        return self.get_closed_count(symbol) >= minutes

    def disconnect(self):
        self._running   = False
        self._connected = False
        try:
            if self._api:
                self._api.disconnect()
        except Exception:
            pass
        print("[IQOptionWS] Disconnected")

    @classmethod
    def get_total_calls(cls):
        return cls._total_calls

print("✅ [iqoption_ws] Module loaded")