# data_sources/binance_ws.py
import websocket
import threading
import json
import time
import ssl
import os

_BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(_BASE, "market_data", "binance")

class BinanceWebSocket:
    def __init__(self):
        os.makedirs(DATA_DIR, exist_ok=True)
        self.data_dir      = DATA_DIR
        self.symbols_dir   = os.path.join(DATA_DIR, "symbols")
        os.makedirs(self.symbols_dir, exist_ok=True)

        self._lock         = threading.Lock()
        self._load_all()

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
        print("✅ [BinanceWS] Initialized (TSV storage)")

    # ── Disk I/O (TSV) ────────────────────────────────────────────────
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
                        # Skip header if present
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
                print(f"❌ [BinanceWS] TSV load error for {symbol}: {e}")
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
            print(f"❌ [BinanceWS] TSV save error for {symbol}: {e}")

    def _load_all(self):
        self.candles = {}
        if os.path.exists(self.symbols_dir):
            for filename in os.listdir(self.symbols_dir):
                if filename.endswith('.tsv') and not filename.endswith('_cvd.tsv') and not filename.endswith('_depth.tsv'):
                    sym = filename[:-4]
                    self.candles[sym] = self._load_symbol(sym)
        total = sum(len(v) for v in self.candles.values())
        print(f"✅ [BinanceWS] Loaded {len(self.candles)} symbols ({total} candles) from TSV")

    # ── Connect ───────────────────────────────────────────────────────
    def connect(self, symbols):
        self._running    = True
        self.subscribed  = [s.lower() for s in symbols]
        if not self.subscribed:
            print("❌ [BinanceWS] No symbols to subscribe")
            return False
        self._thread = threading.Thread(target=self._run_with_reconnect, daemon=True)
        self._thread.start()
        return True

    def _build_url(self):
        streams = [f"{s}@kline_1m" for s in self.subscribed[:200]]
        return "wss://stream.binance.com:9443/stream?streams=" + "/".join(streams)

    def _run_with_reconnect(self):
        while self._running:
            try:
                self.ws = websocket.WebSocketApp(
                    self._build_url(),
                    on_open=self._on_open,
                    on_message=self._on_message,
                    on_error=self._on_error,
                    on_close=self._on_close,
                )
                self.ws.run_forever(sslopt={"cert_reqs": ssl.CERT_NONE})
            except Exception as e:
                print(f"❌ [BinanceWS] Connection exception: {e}")
            if self._running:
                print(f"[BinanceWS] Reconnecting in {self._reconnect_delay}s…")
                time.sleep(self._reconnect_delay)

    def _on_open(self, ws):
        self._connected = True
        self._reconnect_delay = 5
        print(f"✅ [BinanceWS] Connected — {len(self.subscribed)} kline streams")

    def _on_error(self, ws, error):
        self._connected = False
        print(f"❌ [BinanceWS] Error: {error}")

    def _on_close(self, ws, code, msg):
        self._connected = False
        print(f"[BinanceWS] Closed (code={code})")

    def _on_message(self, ws, message):
        try:
            data = json.loads(message)
            payload = data.get('data', data)
            if payload.get('e') != 'kline':
                return

            k = payload['k']
            symbol = k['s'].lower()
            is_closed = bool(k['x'])

            candle = {
                "timestamp": k['t'],
                "open": float(k['o']),
                "high": float(k['h']),
                "low": float(k['l']),
                "close": float(k['c']),
                "volume": float(k['v']),
                "closed": is_closed
            }

            self.tick_count += 1
            now = time.time()
            if now - self._last_log > 30:
                self._last_log = now
                print(f"[BinanceWS] {self.tick_count} kline updates received | connected={self._connected}")

            with self._lock:
                lst = self.candles.setdefault(symbol, [])

                if is_closed:
                    self.candles[symbol] = [c for c in lst if c['timestamp'] != candle['timestamp']]
                    self.candles[symbol].append(candle)
                    self.candles[symbol] = self.candles[symbol][-360:]   # keep 6 hours
                    self._save_symbol(symbol, self.candles[symbol])
                    if symbol in self._live_candles:
                        del self._live_candles[symbol]
                    print(f"[BinanceWS] ✔ Candle closed: {symbol} c={candle['close']} v={candle['volume']:.2f}")
                else:
                    self._live_candles[symbol] = candle
                    if lst and lst[-1]['timestamp'] == candle['timestamp']:
                        self.candles[symbol][-1] = candle
                    else:
                        self.candles[symbol].append(candle)
                        self.candles[symbol] = self.candles[symbol][-360:]

            if self._callback:
                self._callback(symbol, candle['close'], now)

        except Exception as e:
            print(f"❌ [BinanceWS] Message parse error: {e}")

    # ── Public API ────────────────────────────────────────────────────
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
                self.candles[sym] = existing[-360:]  # keep 6 hours
                self._save_symbol(sym, self.candles[sym])
                print(f"[BinanceWS] Added {len(new_candles)} candles for {sym} from REST fill")
            else:
                print(f"[BinanceWS] No new candles for {sym} from REST fill")

    def disconnect(self):
        self._running = False
        self._connected = False
        try:
            if self.ws:
                self.ws.close()
        except Exception:
            pass
        print("[BinanceWS] Disconnected")

    @property
    def name(self):
        return "BinanceWS"

print("✅ [binance_ws] Module loaded")