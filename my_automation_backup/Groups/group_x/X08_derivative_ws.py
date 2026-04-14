# Groups/group_x/X08_derivative_ws.py
import websocket
import threading
import json
import time
import ssl
import os
import traceback
from collections import deque

class DerivativeWebSocket:
    def __init__(self, base_dir, derivative_rest, min_quantity=1.0):
        self.base_dir = base_dir
        self.derivative_rest = derivative_rest
        self.min_quantity = min_quantity
        self._running = False
        self._ws = None
        self._thread = None
        self._subscribed = []
        self._reconnect_delay = 5
        self._lock = threading.Lock()
        self._event_counter = {}
        self._last_analysis_time = {}
        print("[X08_derivative_ws] Live raw rows + analysis every 10 events")

    def start(self, symbols):
        self._subscribed = [s.lower() for s in symbols]
        self._running = True
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        for sym in self._subscribed:
            self._event_counter[sym] = 0
            self._last_analysis_time[sym] = 0
        print(f"[X08_derivative_ws] Started for {len(self._subscribed)} symbols")

    def _run(self):
        while self._running:
            try:
                streams = [f"{s}@forceOrder" for s in self._subscribed]
                url = "wss://fstream.binance.com/stream?streams=" + "/".join(streams)
                self._ws = websocket.WebSocketApp(
                    url,
                    on_open=self._on_open,
                    on_message=self._on_message,
                    on_error=self._on_error,
                    on_close=self._on_close,
                )
                self._ws.run_forever(sslopt={"cert_reqs": ssl.CERT_NONE})
            except Exception as e:
                print(f"[X08_derivative_ws] Exception: {e}")
                traceback.print_exc()
            if self._running:
                print(f"[X08_derivative_ws] Reconnecting in {self._reconnect_delay}s")
                time.sleep(self._reconnect_delay)

    def _on_open(self, ws):
        print(f"[X08_derivative_ws] Connected — {len(self._subscribed)} liquidation streams")

    def _on_error(self, ws, error):
        print(f"[X08_derivative_ws] Error: {error}")

    def _on_close(self, ws, code, msg):
        print(f"[X08_derivative_ws] Closed (code={code})")

    def _on_message(self, ws, message):
        try:
            data = json.loads(message)
            stream = data.get('stream', '')
            if not stream.endswith('@forceOrder'):
                return
            symbol = stream.split('@')[0].lower()
            payload = data.get('data', {}).get('o', {})
            qty = float(payload.get('q', 0))
            if qty < self.min_quantity:
                return
            liquidation = {
                'timestamp': payload.get('T', int(time.time()*1000)),
                'price': float(payload.get('p', 0)),
                'quantity': qty,
                'side': payload.get('S', '')
            }
            # Write raw row (will create file if needed, but file should already exist from REST)
            self._write_raw_liquidation(symbol, liquidation)
            # Update in-memory history
            self.derivative_rest.update_liquidation_history(symbol, liquidation)
            # Increment counter
            self._event_counter[symbol] = self._event_counter.get(symbol, 0) + 1
            # Every 10 events, write a new analysis row
            if self._event_counter[symbol] % 10 == 0:
                self.derivative_rest._write_analysis_row(symbol)
                self._last_analysis_time[symbol] = int(time.time())
            print(f"[X08_derivative_ws] 🔥 {symbol}: {liquidation['side']} {liquidation['quantity']:.2f} @ {liquidation['price']:.2f} (count={self._event_counter[symbol]})")
        except Exception as e:
            print(f"[X08_derivative_ws] Message error: {e}")
            traceback.print_exc()

    def _write_raw_liquidation(self, symbol, liquidation):
        sym = symbol.lower()
        filepath = os.path.join(self.base_dir, "symbols", f"{sym}_liquidations.tsv")
        # If file doesn't exist, create with header (should not happen if REST already ran, but just in case)
        if not os.path.exists(filepath):
            try:
                with open(filepath, 'w') as f:
                    f.write("event_type\ttimestamp\tprice\tquantity\tside\ttotal_liq_vol\trecent_liq_vol\tlong_liq\tshort_liq\tdelta_liq_5m\tmagnet_levels\treversal_signal\toi_relation\tretail_sentiment\toi_trend\tprice_trend\n")
                print(f"[X08_derivative_ws] Created liquidation file for {sym} (via WS)")
            except Exception as e:
                print(f"[X08_derivative_ws] Header write error: {e}")
                return
        try:
            with open(filepath, 'a') as f:
                f.write(f"raw\t{liquidation['timestamp']}\t{liquidation['price']:.2f}\t{liquidation['quantity']:.2f}\t{liquidation['side']}\t0\t0\t0\t0\t0\t\t\t\t\t\t\n")
        except Exception as e:
            print(f"[X08_derivative_ws] Raw write error: {e}")

    def stop(self):
        self._running = False
        if self._ws:
            self._ws.close()
        print("[X08_derivative_ws] Stopped")