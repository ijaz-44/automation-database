#!/usr/bin/env python3
"""
X05_depth_rest.py – Raw Order Book Downloader (Only .tmp_x)
- Fetches full order book depth (limit=1000 levels)
- Writes raw bids and asks to temporary TSV: {symbol}_depth.tmp_x
- Logs issues to GLOBAL LOG: market_data/binance/symbols/X05_depth.log
- No compression, no SQLite, no processing.
"""

import os
import sys
import time
import requests
import datetime

# ========== CONFIGURATION ==========
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SYMBOLS_DIR = os.path.join(BASE_DIR, "market_data", "binance", "symbols")
os.makedirs(SYMBOLS_DIR, exist_ok=True)

BASE_URL = "https://api.binance.com/api/v3"

# ========== GLOBAL LOG FILE ==========
LOG_FILE = os.path.join(SYMBOLS_DIR, "X05_depth.log")
LOG_MAX_SIZE = 5_000_000

def rotate_log_if_needed():
    if os.path.exists(LOG_FILE) and os.path.getsize(LOG_FILE) > LOG_MAX_SIZE:
        backup = LOG_FILE + ".old"
        try:
            os.replace(LOG_FILE, backup)
        except:
            pass

def log_message(level, msg):
    """Log to global X05_depth.log"""
    rotate_log_if_needed()
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"{timestamp} [{level}] {msg}\n")
    print(f"[X05_depth] {msg}")

# ========== FETCH ==========
def fetch_depth_snapshot(symbol, limit=1000, retries=2):
    effective_limit = min(limit, 5000)
    for attempt in range(retries):
        try:
            log_message("INFO", f"Fetching depth for {symbol}, limit={effective_limit} (attempt {attempt+1})")
            r = requests.get(f"{BASE_URL}/depth", params={"symbol": symbol.upper(), "limit": effective_limit}, timeout=20)
            r.raise_for_status()
            data = r.json()
            result = {
                "bids": [[float(p), float(q)] for p, q in data['bids']],
                "asks": [[float(p), float(q)] for p, q in data['asks']],
                "timestamp": data.get('lastUpdateId', int(time.time() * 1000))
            }
            log_message("INFO", f"Got {len(result['bids'])} bids, {len(result['asks'])} asks for {symbol}")
            return result
        except Exception as e:
            log_message("WARNING", f"Error (attempt {attempt+1}) for {symbol}: {e}")
            if attempt < retries - 1:
                time.sleep(2)
    log_message("ERROR", f"Failed to fetch depth for {symbol}")
    return None

# ========== SAVE RAW TSV ==========
def save_raw_tmp_x(symbol, book):
    tmp_path = os.path.join(SYMBOLS_DIR, f"{symbol.lower()}_depth.tmp_x")
    with open(tmp_path, 'w', encoding='utf-8') as f:
        # Write header
        f.write("side\tprice\tquantity\ttimestamp\n")
        # Write bids (sorted high to low)
        for price, qty in book['bids']:
            f.write(f"bid\t{price:.8f}\t{qty:.8f}\t{book['timestamp']}\n")
        # Write asks (sorted low to high)
        for price, qty in book['asks']:
            f.write(f"ask\t{price:.8f}\t{qty:.8f}\t{book['timestamp']}\n")
    log_message("INFO", f"Raw depth saved to {tmp_path} (bids={len(book['bids'])}, asks={len(book['asks'])})")
    return tmp_path

# ========== MAIN DOWNLOADER ==========
def run_download(symbol):
    log_message("INFO", f"Starting depth download for {symbol}")
    start = time.time()
    book = fetch_depth_snapshot(symbol, limit=1000)
    if not book:
        log_message("ERROR", f"Download failed for {symbol}")
        return False
    save_raw_tmp_x(symbol, book)
    elapsed = time.time() - start
    log_message("INFO", f"Download complete for {symbol} in {elapsed:.2f}s")
    return True

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python X05_depth_rest.py SYMBOL")
        sys.exit(1)
    symbol = sys.argv[1].upper()
    success = run_download(symbol)
    sys.exit(0 if success else 1)