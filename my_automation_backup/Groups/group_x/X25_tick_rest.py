#!/usr/bin/env python3
"""
X25_tick_rest.py – Raw Tick Data Downloader (Only .tmp_x)
- Fetches last 1000 trades from Binance public endpoint
- Writes raw trades to TSV: {symbol}_tick.tmp_x
- Logs to market_data/binance/symbols/X25_tick.log
- No processing, no TOON, no metrics.
"""

import os
import sys
import time
import requests
from datetime import datetime

# ========== CONFIGURATION ==========
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SYMBOLS_DIR = os.path.join(BASE_DIR, "market_data", "binance", "symbols")
os.makedirs(SYMBOLS_DIR, exist_ok=True)

# LOG_FILE name changed to X25_tick.log (user requirement)
LOG_FILE = os.path.join(SYMBOLS_DIR, "X25_tick.log")
LOG_MAX_SIZE = 5_000_000

# ========== LOGGING ==========
def rotate_log_if_needed():
    if os.path.exists(LOG_FILE) and os.path.getsize(LOG_FILE) > LOG_MAX_SIZE:
        backup = LOG_FILE + ".old"
        try:
            os.replace(LOG_FILE, backup)
        except:
            pass

def log_issue(level, msg, **kwargs):
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    line = f"{ts} [{level}] {msg}"
    if kwargs:
        line += " " + str(kwargs)
    print(line)
    rotate_log_if_needed()
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(line + "\n")

# ========== FETCH TRADES ==========
def fetch_trades(symbol, limit=1000):
    url = "https://api.binance.com/api/v3/trades"
    params = {"symbol": symbol.upper(), "limit": min(limit, 1000)}
    try:
        r = requests.get(url, params=params, timeout=10)
        if r.status_code == 200:
            return r.json()
        else:
            log_issue("WARNING", f"Binance API HTTP {r.status_code}")
            return None
    except Exception as e:
        log_issue("ERROR", f"Fetch error: {e}")
        return None

# ========== MAIN DOWNLOADER ==========
def run_download(symbol):
    log_issue("INFO", f"Starting tick raw download for {symbol}")
    start_time = time.time()

    trades = fetch_trades(symbol, limit=1000)
    if not trades:
        log_issue("ERROR", "No trades fetched")
        return False

    tmp_x_path = os.path.join(SYMBOLS_DIR, f"{symbol.lower()}_tick.tmp_x")
    with open(tmp_x_path, "w", encoding="utf-8") as f:
        # Write header
        f.write("timestamp\tprice\tquantity\tquoteQty\tisBuyerMaker\n")
        for t in trades:
            f.write(f"{t['time']}\t{t['price']}\t{t['qty']}\t{t['quoteQty']}\t{t['isBuyerMaker']}\n")

    log_issue("INFO", f"Raw tick data saved to {tmp_x_path} ({len(trades)} trades)")
    elapsed = time.time() - start_time
    log_issue("INFO", f"Download complete in {elapsed:.2f}s")
    return True

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python X25_tick_rest.py SYMBOL")
        sys.exit(1)
    success = run_download(sys.argv[1].upper())
    sys.exit(0 if success else 1)