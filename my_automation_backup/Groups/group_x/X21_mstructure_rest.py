#!/usr/bin/env python3
"""
X21_mstructure_rest.py – Raw Market Structure Data Downloader
- Fetches 15m, 1h, 4h, and daily candles from Binance API
- Writes raw candles to TSV: {symbol}_mstructure.tmp_x
- Logs to global file: X21_mstructure.log (in symbols directory)
- No analysis, no SQLite, no derived features.
"""

import os
import sys
import time
import requests

# ========== CONFIGURATION ==========
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SYMBOLS_DIR = os.path.join(BASE_DIR, "market_data", "binance", "symbols")
os.makedirs(SYMBOLS_DIR, exist_ok=True)

BINANCE_KLINES_URL = "https://api.binance.com/api/v3/klines"
RATE_LIMIT_SEC = 1

# ========== GLOBAL LOGGING ==========
LOG_FILE = os.path.join(SYMBOLS_DIR, "X21_mstructure.log")
LOG_MAX_SIZE = 5_000_000

def rotate_log_if_needed():
    if os.path.exists(LOG_FILE) and os.path.getsize(LOG_FILE) > LOG_MAX_SIZE:
        backup = LOG_FILE + ".old"
        try:
            os.replace(LOG_FILE, backup)
        except:
            pass

def log_issue(symbol, level, msg, **kwargs):
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    line = f"{ts} [{level}] [{symbol}] {msg}"
    if kwargs:
        line += " " + str(kwargs)
    print(line)
    rotate_log_if_needed()
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(line + "\n")

# ========== RATE LIMITED FETCH ==========
_last_call = 0
def rate_limited_fetch(url, params=None):
    global _last_call
    now = time.time()
    elapsed = now - _last_call
    if elapsed < RATE_LIMIT_SEC:
        time.sleep(RATE_LIMIT_SEC - elapsed)
    _last_call = time.time()
    try:
        r = requests.get(url, params=params, timeout=15)
        if r.status_code == 200:
            return r.json()
        else:
            return None
    except Exception as e:
        return None

# ---------- Fetch candles (generic) ----------
def fetch_candles(symbol, interval, limit):
    params = {"symbol": symbol.upper(), "interval": interval, "limit": limit}
    data = rate_limited_fetch(BINANCE_KLINES_URL, params=params)
    if data is None:
        log_issue(symbol, "WARNING", f"Failed to fetch {interval} candles (limit={limit})")
        return []
    candles = []
    for c in data:
        candles.append({
            "timestamp": int(c[0]),
            "open": float(c[1]),
            "high": float(c[2]),
            "low": float(c[3]),
            "close": float(c[4]),
            "volume": float(c[5])
        })
    return candles

# ---------- Main downloader ----------
def run_download(symbol):
    log_issue(symbol, "INFO", f"Starting raw market structure download")
    start = time.time()

    # Define timeframes and their candle limits
    tf_config = [
        ("15m", 192),   # 2 days (approx)
        ("1h", 120),    # 5 days
        ("4h", 96),     # 16 days
        ("1d", 35)      # 35 days
    ]

    # Write to .tmp_x TSV
    tmp_x_path = os.path.join(SYMBOLS_DIR, f"{symbol.lower()}_mstructure.tmp_x")
    with open(tmp_x_path, "w", encoding="utf-8") as f:
        f.write("timeframe\ttimestamp\topen\thigh\tlow\tclose\tvolume\n")
        for tf, limit in tf_config:
            candles = fetch_candles(symbol, tf, limit)
            log_issue(symbol, "INFO", f"Fetched {len(candles)} {tf} candles")
            for c in candles:
                f.write(f"{tf}\t{c['timestamp']}\t{c['open']:.8f}\t{c['high']:.8f}\t{c['low']:.8f}\t{c['close']:.8f}\t{c['volume']:.8f}\n")

    log_issue(symbol, "INFO", f"Raw market structure data saved to {tmp_x_path}")
    elapsed = time.time() - start
    log_issue(symbol, "INFO", f"Download complete in {elapsed:.2f}s")
    return True

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python X21_mstructure_rest.py SYMBOL")
        sys.exit(1)
    symbol = sys.argv[1].upper()
    success = run_download(symbol)
    sys.exit(0 if success else 1)