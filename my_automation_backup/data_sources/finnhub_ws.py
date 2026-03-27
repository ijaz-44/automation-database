# data_sources/finnhub_ws.py
import websocket
import threading
import json
import time
import ssl
from .data_hub import DataSource

class FinnhubWebSocket(DataSource):
    """
    Finnhub WebSocket client – live prices for all symbols in a single connection.
    Free tier: unlimited symbols per connection, 1 active WebSocket per API key.
    """
    def __init__(self, api_key):
        self.api_key = api_key
        self.ws = None
        self._prices = {}
        self._connected = False
        self.subscribed = []
        self._thread = None
        print("[FinnhubWS] Initialized")

    @property
    def name(self):
        return "Finnhub"

    @property
    def is_connected(self):
        return self._connected

    def connect(self, symbols):
        """
        Connect to Finnhub WebSocket and subscribe to all symbols.
        symbols: list of symbol strings (e.g., ['AAPL', 'EURUSD', 'BTCUSDT'])
        """
        self.subscribed = [s.upper() for s in symbols]
        ws_url = f"wss://ws.finnhub.io?token={self.api_key}"

        self.ws = websocket.WebSocketApp(
            ws_url,
            on_open=self._on_open,
            on_message=self._on_message,
            on_error=self._on_error,
            on_close=self._on_close
        )

        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        return True

    def _run(self):
        """Run WebSocket with SSL options to bypass certificate errors."""
        try:
            # Try to use sslopt in run_forever (works in newer versions)
            self.ws.run_forever(sslopt={"cert_reqs": ssl.CERT_NONE})
        except TypeError:
            # Fallback for older websocket-client versions: run without sslopt
            try:
                self.ws.run_forever()
            except Exception as e:
                print(f"[FinnhubWS] Run error (no sslopt): {e}")
        except Exception as e:
            print(f"[FinnhubWS] Run error: {e}")

    def _on_open(self, ws):
        # Subscribe to each symbol (Finnhub requires separate subscribe messages)
        for sym in self.subscribed:
            ws.send(json.dumps({"type": "subscribe", "symbol": sym}))
        self._connected = True
        print(f"[FinnhubWS] Connected – subscribed to {len(self.subscribed)} symbols")

    def _on_message(self, ws, message):
        try:
            data = json.loads(message)
            if data.get('type') == 'trade':
                for trade in data.get('data', []):
                    symbol = trade.get('s', '').lower()
                    price = float(trade.get('p', 0))
                    if symbol and price > 0:
                        self._prices[symbol] = price
        except Exception:
            # ignore parsing errors
            pass

    def _on_error(self, ws, error):
        print(f"[FinnhubWS] Error: {error}")
        self._connected = False

    def _on_close(self, ws, close_status_code, close_msg):
        print("[FinnhubWS] Connection closed")
        self._connected = False

    def get_price(self, symbol):
        """Get latest price for symbol (case-insensitive)."""
        return self._prices.get(symbol.lower())

    def disconnect(self):
        if self.ws:
            self.ws.close()
        self._connected = False