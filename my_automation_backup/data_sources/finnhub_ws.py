# data_sources/finnhub_ws.py
import websocket
import threading
import json
import time
import ssl
import os

_BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(_BASE, "market_data", "finnhub")

class FinnhubWebSocket:
    FINNHUB_API_KEY = "d6u7u8pr01qp1k9bjte0d6u7u8pr01qp1k9bjteg"

    def __init__(self, api_key=None):
        self.api_key = api_key if api_key else self.FINNHUB_API_KEY
        os.makedirs(DATA_DIR, exist_ok=True)
        self.data_dir      = DATA_DIR
        self.candles_file  = os.path.join(DATA_DIR, "candles.tsv")
        self._lock         = threading.Lock()
        self._load_data()

        self.ws              = None
        self._connected      = False
        self.subscribed      = []
        self._thread         = None
        self._callback       = None
        self._running        = False
        self._reconnect_delay = 5
        self._live_candles   = {}
        self.tick_count      = 0
        self._last_log       = time.time()
        self._finalise_thread = None
        print("✅ [FinnhubWS] Initialized (TSV storage)")

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
                print(f"✅ [FinnhubWS] Loaded {len(self.candles)} symbols ({total} candles) from TSV")
            except Exception as e:
                print(f"❌ [FinnhubWS] Load error: {e}")
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
            print(f"❌ [FinnhubWS] Save error: {e}")

    def connect(self, symbols):
        self._running    = True
        self.subscribed = []
        for s in symbols:
            s_upper = s.upper()
            if len(s_upper) == 6 and s_upper.isalpha():
                s_upper = s_upper[:3] + "/" + s_upper[3:]
            self.subscribed.append(s_upper)
        if not self.subscribed:
            print("❌ [FinnhubWS] No symbols to subscribe")
            return False
        self._thread = threading.Thread(target=self._run_with_reconnect, daemon=True)
        self._thread.start()
        self._finalise_thread = threading.Thread(target=self._finalise_loop, daemon=True)
        self._finalise_thread.start()
        return True

    def _run_with_reconnect(self):
        ws_url = f"wss://ws.finnhub.io?token={self.api_key}"
        while self._running:
            try:
                self.ws = websocket.WebSocketApp(
                    ws_url,
                    on_open=self._on_open,
                    on_message=self._on_message,
                    on_error=self._on_error,
                    on_close=self._on_close,
                )
                self.ws.run_forever(sslopt={"cert_reqs": ssl.CERT_NONE})
            except Exception as e:
                print(f"❌ [FinnhubWS] Connection exception: {e}")
            if self._running:
                print(f"[FinnhubWS] Reconnecting in {self._reconnect_delay}s…")
                time.sleep(self._reconnect_delay)

    def _on_open(self, ws):
        for sym in self.subscribed:
            ws.send(json.dumps({"type": "subscribe", "symbol": sym}))
        self._connected = True
        self._reconnect_delay = 5
        print(f"✅ [FinnhubWS] Connected — subscribed to {len(self.subscribed)} symbols")

    def _on_error(self, ws, error):
        self._connected = False
        print(f"❌ [FinnhubWS] Error: {error}")

    def _on_close(self, ws, code, msg):
        self._connected = False
        print(f"[FinnhubWS] Closed (code={code})")

    def _on_message(self, ws, message):
        try:
            data = json.loads(message)
            if data.get('type') == 'trade':
                for trade in data.get('data', []):
                    symbol = trade.get('s', '').lower()
                    price = float(trade.get('p', 0))
                    if symbol and price > 0:
                        now = time.time()
                        self.tick_count += 1
                        if now - self._last_log > 30:
                            self._last_log = now
                            print(f"[FinnhubWS] {self.tick_count} ticks received | connected={self._connected}")
                        minute = int(now / 60)
                        with self._lock:
                            live = self._live_candles.get(symbol)
                            if live and live.get('minute') == minute:
                                live['high'] = max(live['high'], price)
                                live['low'] = min(live['low'], price)
                                live['close'] = price
                                live['timestamp'] = int(now * 1000)
                            else:
                                self._live_candles[symbol] = {
                                    "minute": minute,
                                    "open": price,
                                    "high": price,
                                    "low": price,
                                    "close": price,
                                    "timestamp": int(now * 1000),
                                    "volume": 0,
                                    "closed": False
                                }
                        if self._callback:
                            self._callback(symbol, price, now)
        except Exception as e:
            print(f"❌ [FinnhubWS] Message parse error: {e}")

    def _finalise_loop(self):
        last_minute = 0
        while self._running:
            now = time.time()
            minute = int(now / 60)
            if minute != last_minute:
                last_minute = minute
                self._finalise_minute(minute - 1)
            time.sleep(0.5)

    def _finalise_minute(self, minute):
        with self._lock:
            for sym, candle in list(self._live_candles.items()):
                if candle.get('minute') == minute:
                    closed_candle = {
                        "timestamp": candle['timestamp'],
                        "open": candle['open'],
                        "high": candle['high'],
                        "low": candle['low'],
                        "close": candle['close'],
                        "volume": candle['volume'],
                        "closed": True
                    }
                    if sym not in self.candles:
                        self.candles[sym] = []
                    self.candles[sym].append(closed_candle)
                    if len(self.candles[sym]) > 120:
                        self.candles[sym] = self.candles[sym][-120:]
                    self._save()
                    print(f"[FinnhubWS] ✔ Candle closed: {sym} c={closed_candle['close']}")
                    del self._live_candles[sym]

    @property
    def is_connected(self):
        return self._connected

    def set_callback(self, cb):
        self._callback = cb

    def get_candles(self, symbol, interval="1m", limit=100):
        sym = symbol.lower()
        with self._lock:
            raw = list(self.candles.get(sym, []))
        closed = [c for c in raw if c.get('closed', True)]
        if not closed:
            return []

        if interval == "1m":
            return closed[-limit:]

        m_map = {"2m":2, "5m":5, "10m":10, "15m":15, "30m":30, "1h":60, "4h":240}
        m = m_map.get(interval, 5)
        agg = []
        for i in range(0, len(closed), m):
            chunk = closed[i:i+m]
            if len(chunk) < m:
                continue
            agg.append({
                "timestamp": chunk[0]["timestamp"],
                "open": chunk[0]["open"],
                "high": max(c["high"] for c in chunk),
                "low": min(c["low"] for c in chunk),
                "close": chunk[-1]["close"],
                "volume": sum(c["volume"] for c in chunk),
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

    def get_live_candle(self, symbol):
        sym = symbol.lower()
        with self._lock:
            return self._live_candles.get(sym)

    def has_enough_data(self, symbol, minutes=60):
        return self.get_closed_count(symbol) >= minutes

    def add_candles(self, symbol, candles):
        sym = symbol.lower()
        with self._lock:
            existing = self.candles.get(sym, [])
            existing_ts = {c['timestamp'] for c in existing}
            new_candles = [c for c in candles if c['timestamp'] not in existing_ts]
            if new_candles:
                existing.extend(new_candles)
                existing.sort(key=lambda c: c['timestamp'])
                self.candles[sym] = existing[-120:]
                self._save()
                print(f"[FinnhubWS] Added {len(new_candles)} candles for {sym} from REST fill")
            else:
                print(f"[FinnhubWS] No new candles for {sym} from REST fill")

    def disconnect(self):
        self._running = False
        self._connected = False
        try:
            if self.ws:
                self.ws.close()
        except Exception:
            pass
        print("[FinnhubWS] Disconnected")

    @property
    def name(self):
        return "FinnhubWS"

print("✅ [finnhub_ws] Module loaded")