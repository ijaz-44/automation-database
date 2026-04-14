# Groups/group_x/X04_cvd_ws.py
import websocket
import threading
import json
import time
import ssl
import os
import traceback
from collections import deque

class CVDWebSocket:
    def __init__(self, base_dir):
        self.base_dir = base_dir
        self.symbols_dir = os.path.join(base_dir, "symbols")
        os.makedirs(self.symbols_dir, exist_ok=True)
        print(f"[X04_cvd_ws] symbols_dir: {self.symbols_dir}")

        self.metrics = {}
        self._lock = threading.Lock()
        self.ws = None
        self._running = False
        self._thread = None
        self._subscribed = []
        self._reconnect_delay = 5
        self._last_save = {}
        self._cvd_history = {}
        self._price_history = {}
        self._history_len = 50
        self._connected = False

    def set_symbols(self, symbols):
        new_list = [s.lower() for s in symbols]
        print(f"[X04_cvd_ws] set_symbols called with {new_list}")
        with self._lock:
            if set(new_list) == set(self._subscribed) and self._running:
                print(f"[X04_cvd_ws] Symbols already set to {self._subscribed}, no change")
                return
            self._subscribed = new_list
            print(f"[X04_cvd_ws] Setting symbols to: {self._subscribed}")

            if not self._running:
                print("[X04_cvd_ws] Starting thread because not running")
                self._running = True
                self._thread = threading.Thread(target=self._run, daemon=True)
                self._thread.start()
                print("[X04_cvd_ws] Started thread")
            else:
                print("[X04_cvd_ws] Thread already running, closing existing connection to update")
                if self.ws:
                    self.ws.close()
                    time.sleep(1)

    def _run(self):
        print("[X04_cvd_ws] _run thread entered")
        while self._running:
            try:
                with self._lock:
                    streams = [f"{s}@aggTrade" for s in self._subscribed]
                    if not streams:
                        url = "wss://stream.binance.com:9443/stream"
                    else:
                        url = "wss://stream.binance.com:9443/stream?streams=" + "/".join(streams)
                print(f"[X04_cvd_ws] Connecting to {url}")
                self.ws = websocket.WebSocketApp(
                    url,
                    on_open=self._on_open,
                    on_message=self._on_message,
                    on_error=self._on_error,
                    on_close=self._on_close,
                )
                self.ws.run_forever(sslopt={"cert_reqs": ssl.CERT_NONE})
            except Exception as e:
                print(f"[X04_cvd_ws] Exception in run loop: {e}")
                traceback.print_exc()
            if self._running:
                print(f"[X04_cvd_ws] Reconnecting in {self._reconnect_delay}s…")
                time.sleep(self._reconnect_delay)

    def _on_open(self, ws):
        self._connected = True
        with self._lock:
            print(f"[X04_cvd_ws] Connected — {len(self._subscribed)} streams. Subscribed to: {self._subscribed}")
        self._reconnect_delay = 5

    def _on_error(self, ws, error):
        print(f"[X04_cvd_ws] Error: {error}")

    def _on_close(self, ws, code, msg):
        self._connected = False
        print(f"[X04_cvd_ws] Closed (code={code})")

    def _on_message(self, ws, message):
        try:
            data = json.loads(message)
            stream = data.get('stream', '')
            payload = data.get('data', data)
            if 'aggTrade' not in stream:
                return
            symbol = payload.get('s', '').lower()
            if symbol not in self.metrics:
                return

            price = float(payload['p'])
            qty = float(payload['q'])
            is_sell = payload['m']
            timestamp = payload['T']

            with self._lock:
                m = self.metrics[symbol]
                m['cvd'] = m.get('cvd', 0.0)
                footprint = m.get('footprint', {})
                if price not in footprint:
                    footprint[price] = {'buy': 0.0, 'sell': 0.0}
                if is_sell:
                    footprint[price]['sell'] += qty
                    m['cvd'] -= qty
                else:
                    footprint[price]['buy'] += qty
                    m['cvd'] += qty
                if len(footprint) > 100:
                    items = [(p, d['buy'] + d['sell']) for p, d in footprint.items()]
                    items.sort(key=lambda x: x[1], reverse=True)
                    footprint = {p: footprint[p] for p, _ in items[:50]}
                m['footprint'] = footprint

                b = footprint[price]['buy']
                s = footprint[price]['sell']
                if s > 0 and b / s >= 3.0:
                    event = {
                        'price': price,
                        'ratio': round(b / s, 2),
                        'type': 'buy',
                        'timestamp': timestamp
                    }
                    m.setdefault('imbalance_events', []).append(event)
                    if len(m['imbalance_events']) > 10:
                        m['imbalance_events'] = m['imbalance_events'][-10:]
                elif b > 0 and s / b >= 3.0:
                    event = {
                        'price': price,
                        'ratio': round(s / b, 2),
                        'type': 'sell',
                        'timestamp': timestamp
                    }
                    m.setdefault('imbalance_events', []).append(event)
                    if len(m['imbalance_events']) > 10:
                        m['imbalance_events'] = m['imbalance_events'][-10:]

                if symbol not in self._cvd_history:
                    self._cvd_history[symbol] = deque(maxlen=self._history_len)
                    self._price_history[symbol] = deque(maxlen=self._history_len)
                hist_cvd = self._cvd_history[symbol]
                hist_price = self._price_history[symbol]
                hist_cvd.append(m['cvd'])
                hist_price.append(price)
                if len(hist_cvd) >= 10:
                    cvd_delta = m['cvd'] - hist_cvd[-10]
                    price_delta = price - hist_price[-10]
                    if price_delta <= 0 and cvd_delta > 5.0:
                        event = {
                            'price': price,
                            'cvd_delta': round(cvd_delta, 2),
                            'price_delta': round(price_delta, 6),
                            'type': 'bullish',
                            'timestamp': timestamp
                        }
                        m.setdefault('absorption_events', []).append(event)
                        if len(m['absorption_events']) > 10:
                            m['absorption_events'] = m['absorption_events'][-10:]
                    elif price_delta >= 0 and cvd_delta < -5.0:
                        event = {
                            'price': price,
                            'cvd_delta': round(cvd_delta, 2),
                            'price_delta': round(price_delta, 6),
                            'type': 'bearish',
                            'timestamp': timestamp
                        }
                        m.setdefault('absorption_events', []).append(event)
                        if len(m['absorption_events']) > 10:
                            m['absorption_events'] = m['absorption_events'][-10:]

                m['trade_count'] = m.get('trade_count', 0) + 1
                m['last_price'] = price
                m['timestamp'] = timestamp

                now = time.time()
                if now - self._last_save.get(symbol, 0) >= 5:
                    self._save_symbol(symbol)
                    self._last_save[symbol] = now

        except Exception as e:
            print(f"[X04_cvd_ws] Parse error: {e}")
            traceback.print_exc()

    def _save_symbol(self, symbol):
        filepath = os.path.join(self.symbols_dir, f"{symbol}_cvd.tsv")
        try:
            with self._lock:
                m = self.metrics.get(symbol, {}).copy()
            with open(filepath, 'w') as f:
                f.write("timestamp\tcvd\ttrade_count\n")
                f.write(f"{m.get('timestamp', 0)}\t{m.get('cvd', 0)}\t{m.get('trade_count', 0)}\n")
            print(f"[X04_cvd_ws] Saved metrics for {symbol} to TSV")
        except Exception as e:
            print(f"[X04_cvd_ws] Save error for {symbol}: {e}")

    def add_symbol_metrics(self, symbol, initial_metrics):
        sym = symbol.lower()
        with self._lock:
            if sym in self.metrics:
                print(f"[X04_cvd_ws] {sym} metrics already exist")
                return
            initial_metrics.setdefault('cvd', 0.0)
            initial_metrics.setdefault('footprint', {})
            initial_metrics.setdefault('imbalance_events', [])
            initial_metrics.setdefault('absorption_events', [])
            initial_metrics.setdefault('trade_count', 0)
            initial_metrics['symbol'] = sym
            initial_metrics['timestamp'] = int(time.time() * 1000)
            self.metrics[sym] = initial_metrics
            print(f"[X04_cvd_ws] Added metrics for {sym}")
            # Also add to subscribed symbols if not already
            if sym not in self._subscribed:
                new_subs = self._subscribed + [sym]
                self.set_symbols(new_subs)
            self._save_symbol(sym)

    def stop(self):
        self._running = False
        if self.ws:
            self.ws.close()
        print("[X04_cvd_ws] Stopped")