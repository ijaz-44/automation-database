"""
Binance WebSocket – TSV storage (only 1m candles, no resampling)
- Stores 'candles_1m' in {symbol}.tsv (last 120 candles per symbol)
- Periodic flush to disk (every 10 min)
- Multi‑symbol support
- SSL verification disabled by default (for Android QPython)
- Logs summary every minute (not per candle)
"""

import websocket
import threading
import json
import time
import ssl
import os
from collections import deque
from datetime import datetime

_BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(_BASE, "market_data", "binance")

class BinanceWebSocket:
    def __init__(self, flush_interval_minutes=10, verify_ssl=False):
        os.makedirs(DATA_DIR, exist_ok=True)
        self.data_dir = DATA_DIR
        self.symbols_dir = os.path.join(DATA_DIR, "symbols")
        os.makedirs(self.symbols_dir, exist_ok=True)

        self._lock = threading.Lock()
        self._candles = {}          # symbol -> deque of candles (maxlen=120)
        self._live_candles = {}     # symbol -> current live (unclosed) candle
        self._last_update = {}      # symbol -> last update timestamp
        self._flush_interval = flush_interval_minutes * 60
        self._last_flush = time.time()
        self._verify_ssl = verify_ssl

        self.ws = None
        self._connected = False
        self.subscribed = []
        self._thread = None
        self._callback = None
        self._running = False
        self._reconnect_delay = 5
        self._update_counter = 0           # total updates since last log
        self._last_log_time = time.time()
        self._active_symbols = set()       # symbols that received updates in last minute

        self._load_all_from_tsv()
        print(f"✅ [BinanceWS] TSV mode – flush every {flush_interval_minutes} min (no resampling)")
        if not verify_ssl:
            print("⚠️ [BinanceWS] SSL verification disabled (for Android compatibility)")

    # ---------- Disk I/O (TSV) ----------
    def _get_symbol_file(self, symbol):
        return os.path.join(self.symbols_dir, f"{symbol}.tsv")

    def _ensure_file_exists(self, symbol):
        filepath = self._get_symbol_file(symbol)
        if not os.path.exists(filepath):
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write("timestamp\topen\thigh\tlow\tclose\tvolume\n")

    def _read_candles_from_tsv(self, symbol):
        """Read last 120 candles from TSV (returns list of dicts)."""
        filepath = self._get_symbol_file(symbol)
        if not os.path.exists(filepath):
            return []
        candles = []
        with open(filepath, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        if len(lines) < 2:
            return []
        # Skip header
        for line in lines[1:]:
            line = line.strip()
            if not line:
                continue
            parts = line.split('\t')
            if len(parts) >= 6:
                try:
                    candles.append({
                        "ts": int(parts[0]),
                        "o": float(parts[1]),
                        "h": float(parts[2]),
                        "l": float(parts[3]),
                        "c": float(parts[4]),
                        "v": float(parts[5]),
                        "closed": True
                    })
                except:
                    continue
        # Keep only last 120
        return candles[-120:]

    def _load_all_from_tsv(self):
        if not os.path.exists(self.symbols_dir):
            return
        loaded = 0
        for filename in os.listdir(self.symbols_dir):
            if filename.endswith('.tsv'):
                sym = filename[:-4]
                candles = self._read_candles_from_tsv(sym)
                if candles:
                    self._candles[sym] = deque(candles, maxlen=120)
                    loaded += len(candles)
        print(f"✅ [BinanceWS] Loaded {loaded} 1m candles from {len(self._candles)} symbols")

    def _flush_symbol_to_disk(self, symbol):
        """Rewrite entire TSV file with current deque."""
        filepath = self._get_symbol_file(symbol)
        self._ensure_file_exists(symbol)
        with self._lock:
            dq = self._candles.get(symbol, deque())
        if not dq:
            return
        # Build TSV content
        lines = ["timestamp\topen\thigh\tlow\tclose\tvolume"]
        for c in dq:
            ts_sec = c['ts'] // 1000 if 'ts' in c else int(c.get('timestamp', 0))
            lines.append(f"{ts_sec}\t{c['o']:.8f}\t{c['h']:.8f}\t{c['l']:.8f}\t{c['c']:.8f}\t{c['v']:.8f}")
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write("\n".join(lines))
        except Exception as e:
            print(f"❌ [WS] Flush write error {symbol}: {e}")

    def _flush_all(self):
        with self._lock:
            symbols = list(self._candles.keys())
        for sym in symbols:
            self._flush_symbol_to_disk(sym)

    # ---------- WebSocket (SSL context fixed) ----------
    def connect(self, symbols):
        self._running = True
        self.subscribed = [s.lower() for s in symbols]
        if not self.subscribed:
            print("❌ [BinanceWS] No symbols")
            return False
        for sym in self.subscribed:
            self._ensure_file_exists(sym)
        self._thread = threading.Thread(target=self._run_with_reconnect, daemon=True)
        self._thread.start()
        return True

    def _build_url(self):
        streams = [f"{s}@kline_1m" for s in self.subscribed[:200]]
        return "wss://stream.binance.com:9443/stream?streams=" + "/".join(streams)

    def _run_with_reconnect(self):
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
        while self._running:
            try:
                self.ws = websocket.WebSocketApp(
                    self._build_url(),
                    on_open=self._on_open,
                    on_message=self._on_message,
                    on_error=self._on_error,
                    on_close=self._on_close,
                )
                self.ws.run_forever(sslopt={"context": ssl_context})
            except Exception as e:
                print(f"❌ [BinanceWS] Connection: {e}")
            if self._running:
                time.sleep(self._reconnect_delay)

    def _on_open(self, ws):
        self._connected = True
        print(f"✅ [BinanceWS] Connected — {len(self.subscribed)} streams")

    def _on_error(self, ws, error):
        self._connected = False
        print(f"❌ [BinanceWS] Error: {error}")

    def _on_close(self, ws, code, msg):
        self._connected = False
        print(f"[BinanceWS] Closed ({code})")

    def _on_message(self, ws, message):
        try:
            data = json.loads(message)
            payload = data.get('data', data)
            if payload.get('e') != 'kline':
                return
            k = payload['k']
            symbol = k['s'].lower()
            is_closed = bool(k['x'])

            ts_ms = k['t']
            candle = {
                "ts": ts_ms,
                "o": round(float(k['o']), 8),
                "h": round(float(k['h']), 8),
                "l": round(float(k['l']), 8),
                "c": round(float(k['c']), 8),
                "v": round(float(k['v']), 8),
                "closed": is_closed
            }

            self._last_update[symbol] = int(time.time())
            self._active_symbols.add(symbol)
            self._update_counter += 1

            with self._lock:
                if symbol not in self._candles:
                    self._candles[symbol] = deque(maxlen=120)
                dq = self._candles[symbol]

                if is_closed:
                    # Remove any existing candle with same timestamp (should not happen)
                    new_dq = [c for c in dq if c['ts'] != candle['ts']]
                    dq.clear()
                    dq.extend(new_dq)
                    dq.append(candle)
                    self._live_candles.pop(symbol, None)
                else:
                    self._live_candles[symbol] = candle
                    if dq and dq[-1]['ts'] == candle['ts']:
                        dq[-1] = candle
                    else:
                        dq.append(candle)

            # Periodic flush
            now = time.time()
            if now - self._last_flush >= self._flush_interval:
                self._flush_all()
                self._last_flush = now

            # Minute‑wise summary logging
            if now - self._last_log_time >= 60:
                active_count = len(self._active_symbols)
                print(f"[BinanceWS] {self._update_counter} updates in last minute ({active_count} symbols active)")
                self._update_counter = 0
                self._active_symbols.clear()
                self._last_log_time = now

            if self._callback:
                self._callback(symbol, candle['c'], now)

        except Exception as e:
            print(f"❌ [BinanceWS] Parse error: {e}")

    # ---------- Public API ----------
    @property
    def is_connected(self):
        return self._connected

    def set_callback(self, cb):
        self._callback = cb

    def get_candles(self, symbol, limit=100):
        sym = symbol.lower()
        with self._lock:
            dq = self._candles.get(sym, deque())
        candles = list(dq)
        # Add timestamp (seconds) for compatibility
        for c in candles:
            if 'timestamp' not in c:
                c['timestamp'] = c['ts'] // 1000
        return candles[-limit:]

    def get_price(self, symbol):
        sym = symbol.lower()
        with self._lock:
            dq = self._candles.get(sym)
        return dq[-1]['c'] if dq else 0.0

    def get_closed_count(self, symbol):
        sym = symbol.lower()
        with self._lock:
            dq = self._candles.get(sym)
        return len(dq) if dq else 0

    def get_live_candle(self, symbol):
        sym = symbol.lower()
        with self._lock:
            return self._live_candles.get(sym)

    def get_last_update(self, symbol):
        return self._last_update.get(symbol.lower(), 0)

    def is_ws_alive(self, symbol, max_age_sec=120):
        last = self.get_last_update(symbol)
        return last > 0 and (time.time() - last) < max_age_sec

    def has_enough_data(self, symbol, minutes=60):
        return self.get_closed_count(symbol) >= minutes

    def replace_candles(self, symbol, candles):
        """Merge REST candles into deque (used for backfill)."""
        sym = symbol.lower()
        new_candles = []
        for c in candles:
            ts_sec = c['timestamp']
            ts_ms = ts_sec * 1000
            new_candles.append({
                "ts": ts_ms,
                "o": round(c['open'], 8),
                "h": round(c['high'], 8),
                "l": round(c['low'], 8),
                "c": round(c['close'], 8),
                "v": round(c['volume'], 8),
                "closed": True
            })
        with self._lock:
            # Merge: keep existing candles with timestamps > last new candle timestamp
            existing = self._candles.get(sym, deque())
            existing_list = list(existing)
            # Filter out any with ts >= min new ts? Simpler: replace the deque
            all_candles = new_candles + [c for c in existing_list if c['ts'] > new_candles[-1]['ts']]
            all_candles.sort(key=lambda x: x['ts'])
            self._candles[sym] = deque(all_candles[-120:], maxlen=120)
            self._live_candles.pop(sym, None)
        self._flush_symbol_to_disk(sym)
        print(f"[BinanceWS] Replaced {sym} candles via REST merge")

    def disconnect(self):
        self._running = False
        self._flush_all()
        try:
            if self.ws:
                self.ws.close()
        except:
            pass
        print("[BinanceWS] Disconnected")

    @property
    def name(self):
        return "BinanceWS"

print("✅ [binance_ws] TSV version loaded (SSL verification disabled for Android)")