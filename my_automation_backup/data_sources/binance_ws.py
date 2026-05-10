"""
Binance WebSocket – TOON storage (only 1m candles, no resampling)
- Stores 'candles_1m' array in .toon file (last 120 candles per symbol)
- Periodic flush to disk (every 10 min) to reduce I/O
- Multi‑symbol support
- SSL verification disabled by default (fix for Android QPython)
"""

import websocket
import threading
import json
import time
import ssl
import os
import re
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
        self._candles = {}
        self._live_candles = {}
        self._last_update = {}
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
        self.tick_count = 0
        self._last_log = time.time()

        self._load_all_from_toon()
        print(f"✅ [BinanceWS] TOON mode – flush every {flush_interval_minutes} min (no resampling)")
        if not verify_ssl:
            print("⚠️ [BinanceWS] SSL verification disabled (for Android compatibility)")

    # ---------- Disk I/O (TOON) ----------
    def _get_symbol_file(self, symbol):
        return os.path.join(self.symbols_dir, f"{symbol}.toon")

    def _ensure_file_exists(self, symbol):
        filepath = self._get_symbol_file(symbol)
        if os.path.exists(filepath):
            return
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(f"# Real‑time 1m candles for {symbol.upper()} – TOON format\n")
            f.write(f"generated: {datetime.now().isoformat()}\n")
            f.write(f"source: websocket\n\n")
            f.write("candles_4h[0]{ts,dt,o,h,l,c,v}:\n\n")
            f.write("candles_1h[0]{ts,dt,o,h,l,c,v}:\n\n")
            f.write("candles_15m[0]{ts,dt,o,h,l,c,v}:\n\n")
            f.write("candles_1m[0]{ts,dt,o,h,l,c,v}:\n\n")
            f.write("# ========== END OF TOON DATA ==========\n")

    # FIXED: use group(1) and handle empty rows
    def _read_candles_1m_from_toon(self, symbol):
        filepath = self._get_symbol_file(symbol)
        if not os.path.exists(filepath):
            return []
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()
        pattern = r'candles_1m\[\d+\]\{ts,dt,o,h,l,c,v\}:\s*\n(?:\s+([^\n]+(?:\n\s+[^\n]+)*))?'
        match = re.search(pattern, content, re.DOTALL)
        if not match:
            return []
        rows_text = match.group(1)   # <-- fixed: group(1) not group(2)
        if not rows_text:
            return []
        candles = []
        for row in rows_text.split(' | '):
            row = row.strip()
            if not row:
                continue
            parts = row.split(',')
            if len(parts) >= 7:
                try:
                    candles.append({
                        "ts": int(parts[0]),
                        "dt": parts[1],
                        "o": float(parts[2]),
                        "h": float(parts[3]),
                        "l": float(parts[4]),
                        "c": float(parts[5]),
                        "v": float(parts[6]),
                        "closed": True
                    })
                except:
                    continue
        return candles

    def _load_all_from_toon(self):
        if not os.path.exists(self.symbols_dir):
            return
        loaded = 0
        for filename in os.listdir(self.symbols_dir):
            if filename.endswith('.toon'):
                sym = filename[:-5]
                candles = self._read_candles_1m_from_toon(sym)
                if candles:
                    self._candles[sym] = deque(candles, maxlen=120)
                    loaded += len(candles)
        print(f"✅ [BinanceWS] Loaded {loaded} 1m candles from {len(self._candles)} symbols")

    def _flush_symbol_to_disk(self, symbol):
        filepath = self._get_symbol_file(symbol)
        if not os.path.exists(filepath):
            self._ensure_file_exists(symbol)
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                content = f.read()
        except Exception as e:
            print(f"❌ [WS] Flush read error {symbol}: {e}")
            return

        pattern = r'(candles_1m\[\d+\]\{ts,dt,o,h,l,c,v\}:\s*\n)((?:\s+[^\n]+\n)*)(?=\s*\n?\s*candles_15m|\Z)'
        match = re.search(pattern, content, re.DOTALL)
        if not match:
            print(f"❌ [WS] No candles_1m block for {symbol}")
            return

        prefix = match.group(1)
        rows = []
        for c in self._candles.get(symbol, []):
            row = f"{c['ts']},{c['dt']},{c['o']},{c['h']},{c['l']},{c['c']},{c['v']}"
            rows.append(row)
        new_rows_block = "  " + " |\n  ".join(rows) + "\n" if rows else ""
        new_count = len(rows)
        new_prefix = re.sub(r'\[\d+\]', f'[{new_count}]', prefix)
        new_content = content[:match.start(1)] + new_prefix + new_rows_block + content[match.end(2):]

        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(new_content)
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
            ts_sec = ts_ms // 1000
            dt_str = time.strftime('%y%m%dT%H%M', time.localtime(ts_sec))
            candle = {
                "ts": ts_ms,
                "dt": dt_str,
                "o": round(float(k['o']), 4),
                "h": round(float(k['h']), 4),
                "l": round(float(k['l']), 4),
                "c": round(float(k['c']), 4),
                "v": round(float(k['v']), 4),
                "closed": is_closed,
                "timestamp": ts_sec
            }

            self._last_update[symbol] = int(time.time())
            self.tick_count += 1
            now = time.time()
            if now - self._last_log > 30:
                self._last_log = now
                print(f"[BinanceWS] {self.tick_count} updates")

            with self._lock:
                if symbol not in self._candles:
                    self._candles[symbol] = deque(maxlen=120)
                dq = self._candles[symbol]

                if is_closed:
                    new_dq = [c for c in dq if c['ts'] != candle['ts']]
                    dq.clear()
                    dq.extend(new_dq)
                    dq.append(candle)
                    self._live_candles.pop(symbol, None)
                    print(f"[BinanceWS] ✔ Closed {symbol} {dt_str} close={candle['c']}")
                else:
                    self._live_candles[symbol] = candle
                    if dq and dq[-1]['ts'] == candle['ts']:
                        dq[-1] = candle
                    else:
                        dq.append(candle)

            if now - self._last_flush >= self._flush_interval:
                self._flush_all()
                self._last_flush = now

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
        sym = symbol.lower()
        new_candles = []
        for c in candles:
            ts_sec = c['timestamp']
            ts_ms = ts_sec * 1000
            dt_str = time.strftime('%y%m%dT%H%M', time.localtime(ts_sec))
            new_candles.append({
                "ts": ts_ms,
                "dt": dt_str,
                "o": round(c['open'], 4),
                "h": round(c['high'], 4),
                "l": round(c['low'], 4),
                "c": round(c['close'], 4),
                "v": round(c['volume'], 4),
                "closed": True,
                "timestamp": ts_sec
            })
        with self._lock:
            self._candles[sym] = deque(new_candles[-120:], maxlen=120)
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

print("✅ [binance_ws] TOON version loaded (SSL verification disabled for Android)")