# Groups/group_x/X01_klines_rest.py
"""
Fetch candles for CFD trading:
- 1m (120 candles = 2 hours)
- 15m (120 candles = 30 hours)
- 1h (120 candles = 5 days)
Save to a single TSV file: {symbol}.tsv (sections: 1m, 15m, 1h)
Update WebSocket memory with 1m candles only.
"""

import requests
import os

BASE_URL = "https://api.binance.com/api/v3"

def get_data_dir():
    base = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    data_dir = os.path.join(base, "market_data", "binance", "symbols")
    os.makedirs(data_dir, exist_ok=True)
    return data_dir

def fetch_klines(symbol, interval, limit):
    try:
        r = requests.get(f"{BASE_URL}/klines",
                         params={"symbol": symbol.upper(), "interval": interval, "limit": limit},
                         timeout=10)
        r.raise_for_status()
        data = r.json()
        return [{
            "timestamp": c[0],
            "open": float(c[1]), "high": float(c[2]),
            "low": float(c[3]), "close": float(c[4]), "volume": float(c[5])
        } for c in data]
    except Exception as e:
        print(f"❌ Fetch error {symbol} {interval}: {e}")
        return []

def save_to_single_tsv(symbol, candles_1m, candles_15m, candles_1h):
    """Save 1m, then 15m, then 1h into one TSV file."""
    data_dir = get_data_dir()
    filepath = os.path.join(data_dir, f"{symbol.lower()}.tsv")
    with open(filepath, 'w') as f:
        f.write("timestamp\topen\thigh\tlow\tclose\tvolume\n")
        # 1m candles
        for c in candles_1m:
            f.write(f"{c['timestamp']}\t{c['open']}\t{c['high']}\t{c['low']}\t{c['close']}\t{c['volume']}\n")
        # 15m candles
        for c in candles_15m:
            f.write(f"{c['timestamp']}\t{c['open']}\t{c['high']}\t{c['low']}\t{c['close']}\t{c['volume']}\n")
        # 1h candles
        for c in candles_1h:
            f.write(f"{c['timestamp']}\t{c['open']}\t{c['high']}\t{c['low']}\t{c['close']}\t{c['volume']}\n")
    total = len(candles_1m) + len(candles_15m) + len(candles_1h)
    print(f"✅ TSV saved: {filepath} (1m:{len(candles_1m)} rows, 15m:{len(candles_15m)} rows, 1h:{len(candles_1h)} rows, total {total} rows)")

def fetch_and_update_ws(symbol, ws_instance):
    print(f"[X01] Fetching 1m candles for {symbol} (limit=120)...")
    candles_1m = fetch_klines(symbol, "1m", 120)
    if not candles_1m:
        return False
    # Update WebSocket with 1m candles (for real-time engine)
    ws_instance.add_candles(symbol.lower(), candles_1m)
    print(f"[X01] Fetching 15m candles for {symbol} (limit=120)...")
    candles_15m = fetch_klines(symbol, "15m", 120)
    if not candles_15m:
        candles_15m = []
    print(f"[X01] Fetching 1h candles for {symbol} (limit=120)...")
    candles_1h = fetch_klines(symbol, "1h", 120)
    if not candles_1h:
        candles_1h = []
    save_to_single_tsv(symbol, candles_1m, candles_15m, candles_1h)
    return True