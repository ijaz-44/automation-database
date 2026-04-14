import websocket
import threading
import json
import time
import ssl

class BinanceKlinesWebSocket:
    """WebSocket handler for klines; calls callback(symbol, candle) on each kline."""
    def __init__(self):
        self.ws = None
        self._running = False
        self._connected = False
        self._thread = None
        self._callback = None
        self._subscribed = []
        self._reconnect_delay = 5

    def connect(self, symbols, callback):
        self._subscribed = [s.lower() for s in symbols]
        self._callback = callback
        self._running = True
        self._thread = threading.Thread(target=self._run_with_reconnect, daemon=True)
        self._thread.start()

    def _build_url(self):
        streams = [f"{s}@kline_1m" for s in self._subscribed[:200]]
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
                print(f"❌ [X02_klines_ws] Connection exception: {e}")
            if self._running:
                print(f"[X02_klines_ws] Reconnecting in {self._reconnect_delay}s…")
                time.sleep(self._reconnect_delay)

    def _on_open(self, ws):
        self._connected = True
        print(f"✅ [X02_klines_ws] Connected — {len(self._subscribed)} streams")
        self._reconnect_delay = 5

    def _on_error(self, ws, error):
        self._connected = False
        print(f"❌ [X02_klines_ws] Error: {error}")

    def _on_close(self, ws, code, msg):
        self._connected = False
        print(f"[X02_klines_ws] Closed (code={code})")

    def _on_message(self, ws, message):
        try:
            data = json.loads(message)
            payload = data.get('data', data)
            if payload.get('e') != 'kline':
                return
            k = payload['k']
            symbol = k['s'].lower()
            candle = {
                "timestamp": k['t'],
                "open": float(k['o']),
                "high": float(k['h']),
                "low": float(k['l']),
                "close": float(k['c']),
                "volume": float(k['v']),
                "closed": bool(k['x'])
            }
            if self._callback:
                self._callback(symbol, candle)
        except Exception as e:
            print(f"❌ [X02_klines_ws] Message parse error: {e}")

    @property
    def is_connected(self):
        return self._connected

    def disconnect(self):
        self._running = False
        self._connected = False
        try:
            if self.ws:
                self.ws.close()
        except:
            pass