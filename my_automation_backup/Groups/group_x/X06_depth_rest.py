# Groups/group_x/X06_depth_rest.py
import requests
import time

BASE_URL = "https://api.binance.com/api/v3"

def fetch_depth_snapshot(symbol, limit=500, retries=2):
    """Fetch depth snapshot with retries and longer timeout."""
    for attempt in range(retries):
        try:
            print(f"[X06_depth_rest] Fetching depth for {symbol}, limit={limit} (attempt {attempt+1})")
            r = requests.get(
                f"{BASE_URL}/depth",
                params={"symbol": symbol.upper(), "limit": limit},
                timeout=20  # increased timeout
            )
            r.raise_for_status()
            data = r.json()
            result = {
                "bids": [[float(p), float(q)] for p, q in data['bids']],
                "asks": [[float(p), float(q)] for p, q in data['asks']],
                "timestamp": data.get('lastUpdateId', int(time.time() * 1000))
            }
            print(f"[X06_depth_rest] Got {len(result['bids'])} bids, {len(result['asks'])} asks")
            return result
        except Exception as e:
            print(f"[X06_depth_rest] Error (attempt {attempt+1}): {e}")
            if attempt < retries - 1:
                time.sleep(2)
    return None

def compute_liquidity_heatmap(book, price_bucket_size=1.0):
    from collections import defaultdict
    heatmap = defaultdict(float)
    for price, qty in book.get('bids', []):
        bucket = round(price / price_bucket_size) * price_bucket_size
        heatmap[bucket] += qty
    for price, qty in book.get('asks', []):
        bucket = round(price / price_bucket_size) * price_bucket_size
        heatmap[bucket] += qty
    return {str(k): v for k, v in heatmap.items()}

def detect_iceberg_orders(book, threshold_qty=10.0, top_n=10):
    candidates = []
    bids = sorted(book.get('bids', []), key=lambda x: -x[0])[:top_n]
    for i, (price, qty) in enumerate(bids):
        if i + 1 < len(bids):
            next_qty = bids[i+1][1]
            if qty > threshold_qty and qty > next_qty * 2:
                candidates.append({"price": price, "qty": qty, "type": "bid", "iceberg": True})
    asks = sorted(book.get('asks', []), key=lambda x: x[0])[:top_n]
    for i, (price, qty) in enumerate(asks):
        if i + 1 < len(asks):
            next_qty = asks[i+1][1]
            if qty > threshold_qty and qty > next_qty * 2:
                candidates.append({"price": price, "qty": qty, "type": "ask", "iceberg": True})
    return candidates

def detect_spoofing(prev_book, curr_book, threshold_qty=50.0):
    if not prev_book:
        return []
    spoofs = []
    prev_bids = {p: q for p, q in prev_book.get('bids', [])}
    curr_bids = {p: q for p, q in curr_book.get('bids', [])}
    for price, qty in curr_book.get('bids', []):
        if price not in prev_bids and qty > threshold_qty:
            spoofs.append({"price": price, "qty": qty, "type": "bid", "action": "added"})
    for price, qty in prev_book.get('bids', []):
        if price not in curr_bids and qty > threshold_qty:
            spoofs.append({"price": price, "qty": qty, "type": "bid", "action": "removed"})
    prev_asks = {p: q for p, q in prev_book.get('asks', [])}
    curr_asks = {p: q for p, q in curr_book.get('asks', [])}
    for price, qty in curr_book.get('asks', []):
        if price not in prev_asks and qty > threshold_qty:
            spoofs.append({"price": price, "qty": qty, "type": "ask", "action": "added"})
    for price, qty in prev_book.get('asks', []):
        if price not in curr_asks and qty > threshold_qty:
            spoofs.append({"price": price, "qty": qty, "type": "ask", "action": "removed"})
    return spoofs