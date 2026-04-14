# Groups/group_x/X05_depth_ws.py
import websocket
import threading
import json
import time
import ssl
import os
import traceback
import math
from collections import deque

from .X06_depth_rest import fetch_depth_snapshot

class DepthWebSocket:
    def __init__(self, base_dir):
        self.base_dir = base_dir
        self.symbols_dir = os.path.join(base_dir, "symbols")
        os.makedirs(self.symbols_dir, exist_ok=True)

        self.data = {}               # symbol -> {bids, asks, timestamp}
        self.previous_books = {}     # symbol -> deque of last 10 snapshots (bids, asks, timestamp)
        self.trade_buffer = {}       # symbol -> deque of recent trades (for spoofing validation)
        self._lock = threading.Lock()
        self.ws = None
        self._running = False
        self._thread = None
        self._subscribed = []
        self._reconnect_delay = 5
        self._last_save = {}
        self.MAX_SNAPSHOTS = 500

    def add_symbol(self, symbol):
        sym = symbol.lower()
        with self._lock:
            if sym in self._subscribed:
                return
            # Try to fetch initial snapshot, but don't block if fails
            self._fetch_initial_snapshot(sym)
            self._subscribed.append(sym)
            if sym not in self.trade_buffer:
                self.trade_buffer[sym] = deque(maxlen=2000)
            print(f"[X05_depth_ws] Added {sym}")
            if not self._running:
                self._running = True
                self._thread = threading.Thread(target=self._run, daemon=True)
                self._thread.start()
            else:
                if self.ws:
                    self.ws.close()

    def _fetch_initial_snapshot(self, symbol):
        print(f"[X05_depth_ws] Fetching initial REST snapshot for {symbol}")
        snapshot = fetch_depth_snapshot(symbol, limit=500, retries=2)
        if snapshot:
            with self._lock:
                self.data[symbol] = {
                    'bids': snapshot['bids'],
                    'asks': snapshot['asks'],
                    'timestamp': snapshot['timestamp']
                }
                if symbol not in self.previous_books:
                    self.previous_books[symbol] = deque(maxlen=10)
                # Save immediately
                self._save_enhanced_snapshot(symbol, force=True)
                self._last_save[symbol] = time.time()
        else:
            print(f"[X05_depth_ws] Initial snapshot failed for {symbol}, will use WebSocket data")
            # Create empty data placeholder; WebSocket will fill later
            with self._lock:
                self.data[symbol] = {
                    'bids': [],
                    'asks': [],
                    'timestamp': int(time.time() * 1000)
                }
                if symbol not in self.previous_books:
                    self.previous_books[symbol] = deque(maxlen=10)

    def _run(self):
        while self._running:
            try:
                # Subscribe to both depth and aggTrade streams
                streams = []
                for s in self._subscribed:
                    streams.append(f"{s}@depth")
                    streams.append(f"{s}@aggTrade")
                url = "wss://stream.binance.com:9443/stream?streams=" + "/".join(streams)
                self.ws = websocket.WebSocketApp(
                    url,
                    on_open=self._on_open,
                    on_message=self._on_message,
                    on_error=self._on_error,
                    on_close=self._on_close,
                )
                self.ws.run_forever(sslopt={"cert_reqs": ssl.CERT_NONE})
            except Exception as e:
                print(f"[X05_depth_ws] Exception: {e}")
                traceback.print_exc()
                time.sleep(self._reconnect_delay)

    def _on_open(self, ws):
        print(f"[X05_depth_ws] Connected to streams")

    def _on_error(self, ws, error):
        print(f"[X05_depth_ws] Error: {error}")

    def _on_close(self, ws, code, msg):
        print(f"[X05_depth_ws] Closed (code={code})")

    def _on_message(self, ws, message):
        try:
            data = json.loads(message)
            stream = data.get('stream', '')
            payload = data.get('data', {})
            symbol = stream.split('@')[0].lower()

            # Handle aggTrade for trade buffer
            if '@aggTrade' in stream:
                with self._lock:
                    self.trade_buffer[symbol].append({
                        'price': float(payload['p']),
                        'quantity': float(payload['q']),
                        'timestamp': payload['T']
                    })
                return

            # Handle depth update
            if not stream.endswith('@depth'):
                return

            with self._lock:
                if symbol not in self.data:
                    # Initialize if not exists (should not happen, but safe)
                    self.data[symbol] = {'bids': [], 'asks': [], 'timestamp': 0}
                    if symbol not in self.previous_books:
                        self.previous_books[symbol] = deque(maxlen=10)
                book = self.data[symbol]
                # Convert to dict for easy update
                bids_dict = {p: q for p, q in book['bids']}
                asks_dict = {p: q for p, q in book['asks']}

                for price, qty in payload.get('bids', []):
                    p_f, q_f = float(price), float(qty)
                    if q_f == 0.0:
                        bids_dict.pop(p_f, None)
                    else:
                        bids_dict[p_f] = q_f

                for price, qty in payload.get('asks', []):
                    p_f, q_f = float(price), float(qty)
                    if q_f == 0.0:
                        asks_dict.pop(p_f, None)
                    else:
                        asks_dict[p_f] = q_f

                book['bids'] = sorted(bids_dict.items(), key=lambda x: -x[0])
                book['asks'] = sorted(asks_dict.items(), key=lambda x: x[0])
                book['timestamp'] = payload.get('E', int(time.time() * 1000))

                now = time.time()
                if now - self._last_save.get(symbol, 0) >= 1.0:  # save every second
                    self._save_enhanced_snapshot(symbol)
                    self._last_save[symbol] = now
        except Exception as e:
            print(f"[X05_depth_ws] Message error: {e}")
            traceback.print_exc()

    def _detect_icebergs_strong(self, bids_list, asks_list, z_threshold=1.8):
        """
        Iceberg detection using z-score (no numpy).
        Looks at 5 levels above and below, calculates mean and std.
        """
        icebergs = []

        def detect_side(items, side):
            if len(items) < 10:
                return []
            result = []
            qtys = [q for _, q in items]
            for i, (price, qty) in enumerate(items):
                start = max(0, i - 5)
                end = min(len(items), i + 6)
                neighbors = [qtys[j] for j in range(start, end) if j != i]
                if len(neighbors) < 2:
                    continue
                mean = sum(neighbors) / len(neighbors)
                variance = sum((x - mean) ** 2 for x in neighbors) / len(neighbors)
                std = math.sqrt(variance) if variance > 0 else 1.0
                zscore = (qty - mean) / std if std > 0 else 0
                # Dynamic threshold: also require qty > mean * 1.5
                if zscore >= z_threshold and qty > mean * 1.5:
                    result.append({
                        'price': price,
                        'qty': qty,
                        'side': side,
                        'zscore': round(zscore, 2)
                    })
            return result

        icebergs.extend(detect_side(bids_list, 'bid'))
        icebergs.extend(detect_side(asks_list, 'ask'))
        return icebergs

    def _detect_spoofing_strong(self, symbol, current_bids, current_asks, current_ts):
        """
        Spoofing detection: large order disappears without being filled.
        Uses trade buffer to check actual trades at that price.
        Dynamic threshold based on average quantity.
        """
        spoofs = []
        history = self.previous_books.get(symbol)
        if not history or len(history) < 1:
            return spoofs

        # Previous snapshot (last saved)
        prev_bids, prev_asks, prev_ts = history[-1]
        curr_bid_dict = {p: q for p, q in current_bids}
        curr_ask_dict = {p: q for p, q in current_asks}

        # Get trades that occurred between previous snapshot and now
        with self._lock:
            recent_trades = [t for t in self.trade_buffer.get(symbol, []) if t['timestamp'] >= prev_ts]

        def check_spoof(prev_items, curr_dict, side):
            results = []
            # Calculate average quantity for dynamic threshold
            if len(prev_items) > 0:
                avg_qty = sum(q for _, q in prev_items) / len(prev_items)
                threshold = max(10.0, avg_qty * 0.5)  # dynamic for big/small coins
            else:
                threshold = 10.0
            for price, qty in prev_items:
                # If a large order was removed or drastically reduced
                if qty > threshold and (price not in curr_dict or curr_dict[price] < qty * 0.2):
                    # Calculate total traded volume at this price level (within 0.01% price tolerance)
                    total_traded = sum(t['quantity'] for t in recent_trades if abs(t['price'] - price) / price < 0.0001)
                    # If less than 10% of the order was actually traded -> spoof
                    if total_traded < qty * 0.1:
                        results.append({
                            'price': price,
                            'qty': qty,
                            'side': side,
                            'action': 'spoof_cancelled'
                        })
            return results

        spoofs.extend(check_spoof(prev_bids, curr_bid_dict, 'bid'))
        spoofs.extend(check_spoof(prev_asks, curr_ask_dict, 'ask'))
        return spoofs

    def _save_enhanced_snapshot(self, symbol, force=False):
        with self._lock:
            if symbol not in self.data:
                return
            book = self.data[symbol]
            bids_list = book['bids'][:200]  # keep top 200
            asks_list = book['asks'][:200]
            timestamp = book['timestamp']

            # Compute icebergs and spoofs (if enough data)
            icebergs = []
            spoofs = []
            if len(bids_list) > 10 and len(asks_list) > 10:
                icebergs = self._detect_icebergs_strong(bids_list, asks_list, z_threshold=1.8)
                spoofs = self._detect_spoofing_strong(symbol, bids_list, asks_list, timestamp)

            # Update history (keep last 10 snapshots)
            self.previous_books[symbol].append((bids_list, asks_list, timestamp))

            # Save to TSV file
            complete_file = os.path.join(self.symbols_dir, f"{symbol}_depth_complete.tsv")
            try:
                # Rotate if too large
                if os.path.exists(complete_file) and os.path.getsize(complete_file) > 10 * 1024 * 1024:
                    self._rotate_file(complete_file)

                with open(complete_file, 'a') as f:
                    if os.path.getsize(complete_file) == 0:
                        f.write("timestamp\tfeature_type\tprice\tquantity\ticeberg_zscore\tspoof_action\tspoof_side\n")
                    # Bids
                    for p, q in bids_list:
                        f.write(f"{timestamp}\tdepth_bid\t{p}\t{q}\t\t\t\n")
                    # Asks
                    for p, q in asks_list:
                        f.write(f"{timestamp}\tdepth_ask\t{p}\t{q}\t\t\t\n")
                    # Icebergs
                    for ice in icebergs:
                        f.write(f"{timestamp}\ticeberg\t{ice['price']}\t{ice['qty']}\t{ice['zscore']}\t\t{ice['side']}\n")
                    # Spoofs
                    for sp in spoofs:
                        f.write(f"{timestamp}\tspoofing\t{sp['price']}\t{sp['qty']}\t\t{sp['action']}\t{sp['side']}\n")
                print(f"[X05_depth_ws] Saved snapshot for {symbol}: {len(bids_list)} bids, {len(asks_list)} asks, {len(icebergs)} iceberg, {len(spoofs)} spoofs")
            except Exception as e:
                print(f"[X05_depth_ws] Save error: {e}")

    def _rotate_file(self, filepath):
        """Keep only last MAX_SNAPSHOTS snapshots."""
        try:
            with open(filepath, 'r') as f:
                lines = f.readlines()
            if not lines:
                return
            header = lines[0]
            data_lines = lines[1:]
            # Group by timestamp
            snapshots = {}
            for line in data_lines:
                parts = line.strip().split('\t')
                if len(parts) > 0:
                    ts = parts[0]
                    if ts not in snapshots:
                        snapshots[ts] = []
                    snapshots[ts].append(line)
            sorted_ts = sorted(snapshots.keys())
            if len(sorted_ts) > self.MAX_SNAPSHOTS:
                keep_ts = set(sorted_ts[-self.MAX_SNAPSHOTS:])
                new_lines = [header]
                for ts in sorted_ts:
                    if ts in keep_ts:
                        new_lines.extend(snapshots[ts])
                with open(filepath, 'w') as f:
                    f.writelines(new_lines)
                print(f"[X05_depth_ws] Rotated file, kept {len(keep_ts)} snapshots")
        except Exception as e:
            print(f"[X05_depth_ws] Rotation error: {e}")

    def stop(self):
        self._running = False
        if self.ws:
            self.ws.close()
        print("[X05_depth_ws] Stopped")