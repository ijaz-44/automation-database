# data_sources/binance_ws.py
import websocket
import threading
import json
import time
from .data_hub import DataSource

class BinanceWebSocket(DataSource):
    def __init__(self):
        self.ws = None
        self._prices = {}
        self._connected = False
        self.subscribed = []
        self._thread = None
        print("[BinanceWS] Initialized")
    
    @property
    def name(self):
        return "Binance"
    
    @property
    def is_connected(self):
        return self._connected
    
    def connect(self, symbols):
        self.subscribed = [s.lower() for s in symbols[:50]]
        if not self.subscribed:
            return True
        
        stream_name = "@ticker".join(self.subscribed)
        ws_url = f"wss://stream.binance.com:9443/ws/{stream_name}"
        
        self.ws = websocket.WebSocketApp(
            ws_url,
            on_open=self._on_open,
            on_message=self._on_message,
            on_error=self._on_error,
            on_close=self._on_close
        )
        
        self._thread = threading.Thread(target=self._run)
        self._thread.daemon = True
        self._thread.start()
        return True
    
    def _run(self):
        try:
            self.ws.run_forever()
        except Exception as e:
            print(f"[BinanceWS] Run error: {e}")
    
    def _on_open(self, ws):
        self._connected = True
        print(f"[BinanceWS] Connected ({len(self.subscribed)} symbols)")
    
    def _on_message(self, ws, message):
        try:
            data = json.loads(message)
            symbol = data.get('s', '').lower()
            price = float(data.get('c', 0))
            if symbol and price > 0:
                self._prices[symbol] = price
        except:
            pass
    
    def _on_error(self, ws, error):
        self._connected = False
    
    def _on_close(self, ws, code, msg):
        self._connected = False
    
    def get_price(self, symbol):
        return self._prices.get(symbol.lower())
    
    def disconnect(self):
        if self.ws:
            self.ws.close()
        self._connected = False