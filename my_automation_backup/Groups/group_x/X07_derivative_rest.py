#!/usr/bin/env python3
"""
X07_derivative_rest.py – Raw Derivatives Downloader (Only .tmp_x)
- Fetches spot price, mark price, funding rate, open interest, OI history, LS history, funding history.
- Writes raw data to temporary TSV: {symbol}_derivative.tmp_x
- Logs issues to GLOBAL log file: X07_derivative.log (overwritten on each run)
- No processing, no SQLite, no database.
- Liquidations removed (handled by X13).
"""

import os
import sys
import time
import requests
import datetime

FUTURES_BASE_URL = "https://fapi.binance.com"
SPOT_BASE_URL = "https://api.binance.com/api/v3"

# ========== CONFIGURATION ==========
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SYMBOLS_DIR = os.path.join(BASE_DIR, "market_data", "binance", "symbols")
os.makedirs(SYMBOLS_DIR, exist_ok=True)

# GLOBAL LOG FILE (overwritten each run)
GLOBAL_LOG_FILE = os.path.join(SYMBOLS_DIR, "X07_derivative.log")

# ========== LOGGING (global, overwritten) ==========
def log_issue(level, msg, **kwargs):
    """Write to global log file (append mode after first truncation)."""
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"{timestamp} [{level}] {msg}"
    if kwargs:
        line += " " + str(kwargs)
    print(line)
    # 'a' mode – file already truncated at start of run_download()
    with open(GLOBAL_LOG_FILE, "a", encoding="utf-8") as f:
        f.write(line + "\n")

# ========== API FETCHERS ==========
def fetch_open_interest(symbol):
    try:
        r = requests.get(f"{FUTURES_BASE_URL}/fapi/v1/openInterest",
                         params={"symbol": symbol.upper()}, timeout=10)
        r.raise_for_status()
        return float(r.json().get('openInterest', 0))
    except Exception as e:
        log_issue("WARNING", f"OI fetch error: {e}")
        return 0

def fetch_oi_history(symbol, period="5m", limit=24):
    try:
        r = requests.get(f"{FUTURES_BASE_URL}/futures/data/openInterestHist",
                         params={"symbol": symbol.upper(), "period": period, "limit": limit}, timeout=10)
        r.raise_for_status()
        data = r.json()
        history = []
        for item in data:
            history.append({
                'timestamp': item.get('timestamp', 0),
                'value': float(item.get('sumOpenInterest', 0))
            })
        return history
    except Exception as e:
        log_issue("WARNING", f"OI history error: {e}")
        return []

def fetch_long_short_ratio_history(symbol, period="5m", limit=24):
    try:
        r = requests.get(f"{FUTURES_BASE_URL}/futures/data/topLongShortAccountRatio",
                         params={"symbol": symbol.upper(), "period": period, "limit": limit}, timeout=10)
        r.raise_for_status()
        data = r.json()
        history = []
        for item in data:
            history.append({
                'timestamp': item.get('timestamp', 0),
                'long_short_ratio': float(item.get('longShortRatio', 0)),
                'long_account': float(item.get('longAccount', 0)),
                'short_account': float(item.get('shortAccount', 0))
            })
        return history
    except Exception as e:
        log_issue("WARNING", f"L/S history error: {e}")
        return []

def fetch_funding_rate_history(symbol, limit=24):
    try:
        r = requests.get(f"{FUTURES_BASE_URL}/fapi/v1/fundingRate",
                         params={"symbol": symbol.upper(), "limit": limit}, timeout=10)
        r.raise_for_status()
        data = r.json()
        history = []
        for item in data:
            history.append({
                'timestamp': item.get('fundingTime', 0),
                'funding_rate': float(item.get('fundingRate', 0))
            })
        return history
    except Exception as e:
        log_issue("WARNING", f"Funding history error: {e}")
        return []

def fetch_spot_price(symbol):
    try:
        r = requests.get(f"{SPOT_BASE_URL}/ticker/price",
                         params={"symbol": symbol.upper()}, timeout=10)
        r.raise_for_status()
        return float(r.json().get('price', 0))
    except Exception as e:
        log_issue("WARNING", f"Spot price error: {e}")
        return 0

def fetch_current_funding_and_mark(symbol):
    try:
        r = requests.get(f"{FUTURES_BASE_URL}/fapi/v1/premiumIndex",
                         params={"symbol": symbol.upper()}, timeout=10)
        r.raise_for_status()
        data = r.json()
        return {
            'funding_rate': float(data.get('lastFundingRate', 0)),
            'mark_price': float(data.get('markPrice', 0)),
            'index_price': float(data.get('indexPrice', 0))
        }
    except Exception as e:
        log_issue("WARNING", f"Funding/mark error: {e}")
        return {'funding_rate': 0, 'mark_price': 0, 'index_price': 0}

# ========== MAIN DOWNLOADER (liquidations removed) ==========
def run_download(symbol):
    # Reset global log file (overwrite previous)
    with open(GLOBAL_LOG_FILE, "w", encoding="utf-8") as f:
        f.write(f"=== X07_derivative run for {symbol.upper()} at {datetime.datetime.now().isoformat()} ===\n")
    log_issue("INFO", f"Starting derivatives download for {symbol}")

    start = time.time()
    sym = symbol.upper()
    timestamp = int(time.time() * 1000)

    # Fetch all data (excluding liquidations)
    spot = fetch_spot_price(sym)
    current = fetch_current_funding_and_mark(sym)
    oi_current = fetch_open_interest(sym)
    oi_hist = fetch_oi_history(sym, period="5m", limit=24)
    ls_hist = fetch_long_short_ratio_history(sym, period="5m", limit=24)
    funding_hist = fetch_funding_rate_history(sym, limit=24)

    # Write to .tmp_x TSV (no liquidation rows)
    tmp_path = os.path.join(SYMBOLS_DIR, f"{symbol.lower()}_derivative.tmp_x")
    with open(tmp_path, "w", encoding="utf-8") as f:
        f.write("type\ttimestamp\tvalue1\tvalue2\tvalue3\tvalue4\tvalue5\n")
        # Snapshot line
        f.write(f"snapshot\t{timestamp}\t{spot}\t{current['mark_price']}\t{current['funding_rate']}\t{oi_current}\t\n")
        # OI history lines
        for h in oi_hist:
            f.write(f"oi_history\t{h['timestamp']}\t{h['value']}\t\t\t\t\n")
        # LS history lines
        for h in ls_hist:
            f.write(f"ls_history\t{h['timestamp']}\t{h['long_short_ratio']}\t{h['long_account']}\t{h['short_account']}\t\t\n")
        # Funding history lines
        for h in funding_hist:
            f.write(f"funding_history\t{h['timestamp']}\t{h['funding_rate']}\t\t\t\t\n")
        # No liquidation lines

    log_issue("INFO", f"Raw derivatives saved to {tmp_path}")
    elapsed = time.time() - start
    log_issue("INFO", f"Download complete in {elapsed:.2f}s")
    return True

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python X07_derivative_rest.py SYMBOL")
        sys.exit(1)
    success = run_download(sys.argv[1].upper())
    sys.exit(0 if success else 1)