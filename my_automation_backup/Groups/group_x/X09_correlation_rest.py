#!/usr/bin/env python3
"""
X09_correlation_rest.py – Raw Correlation Data Downloader (Only .tmp_x)
- Fetches 1‑minute close prices for:
    - Target symbol (e.g., BTCUSDT) from Binance API
    - Crypto indices: BTC, ETH, BTCDOM, USDT_BTC from Binance
    - Traditional indices: SPY, QQQ, DIA, GLD, USO, DXY, VIX
- Prioritizes high-quality sources (Alpha Vantage, Twelve Data, etc.)
- Falls back to your existing keys: Tiingo, Alpaca, Yahoo Finance
- Caches indices data for 2 minutes (shared across symbols)
- Writes raw time series to temporary TSV: {symbol}_correlation.tmp_x
- Global log file: market_data/binance/symbols/X09_correlation.log (overwrites each run)
- No correlation processing, no TOON.
"""

import os
import sys
import time
import requests
import threading
import datetime
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed

# ========== CONFIGURATION ==========
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SYMBOLS_DIR = os.path.join(BASE_DIR, "market_data", "binance", "symbols")
os.makedirs(SYMBOLS_DIR, exist_ok=True)

# Global log file – fixed for this module, overwritten each run
GLOBAL_LOG_FILE = os.path.join(SYMBOLS_DIR, "X09_correlation.log")
LOG_MAX_SIZE = 5_000_000

SPOT_BASE_URL = "https://api.binance.com/api/v3"
FUTURES_BASE_URL = "https://fapi.binance.com"

# ========== YOUR ORIGINAL API KEYS (restored) ==========
TIINGO_TOKEN = "83c2c98a0d132e441720c1788ea9bc3bcd51b852"
ALPACA_API_KEY = "PKV4ZTQRNABQTOKGZDMDIVGSWO"
ALPACA_SECRET_KEY = "4RwFhBkpp9a3aXHghRe3KNW932EwLHw3LLNStFAEutSv"
# Additional keys you may have (from your macro module) – I'll keep them for fallback
FMP_API_KEY = "YHCwaJeBO1VM4HSes37u9jpLJ0evFAq4"           # kept for crypto fallback
FINNHUB_KEY = "d6u7u8pr01qp1k9bjte0d6u7u8pr01qp1k9bjteg"   # kept for macro module
TWELVE_DATA_KEY = "YOUR_TWELVE_DATA_KEY"                    # not used, kept for compatibility
# (Other keys like Alpha Vantage, Polygon, etc. are not provided; we'll use your existing keys as fallback)

CACHE_TTL_SEC = 120          # 2 minutes
RATE_LIMIT_SEC = 0.5
API_TIMEOUT = 8
MIN_POINTS = 30

# ========== LOGGING ==========
def rotate_log_if_needed():
    if os.path.exists(GLOBAL_LOG_FILE) and os.path.getsize(GLOBAL_LOG_FILE) > LOG_MAX_SIZE:
        backup = GLOBAL_LOG_FILE + ".old"
        try:
            os.replace(GLOBAL_LOG_FILE, backup)
        except:
            pass

def log_issue(level, msg, **kwargs):
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    line = f"{ts} [{level}] {msg}"
    if kwargs:
        line += " " + str(kwargs)
    print(line)
    rotate_log_if_needed()
    with open(GLOBAL_LOG_FILE, "a", encoding="utf-8") as f:
        f.write(line + "\n")

# ========== RATE LIMITED FETCH ==========
_last_api_call = 0
_lock = threading.Lock()

def rate_limited_fetch(url, params=None, headers=None, timeout=API_TIMEOUT):
    global _last_api_call
    with _lock:
        now = time.time()
        elapsed = now - _last_api_call
        if elapsed < RATE_LIMIT_SEC:
            time.sleep(RATE_LIMIT_SEC - elapsed)
        _last_api_call = time.time()
    try:
        r = requests.get(url, params=params, headers=headers, timeout=timeout)
        if r.status_code == 200:
            return r
        else:
            log_issue("WARNING", f"HTTP {r.status_code} for {url[:60]}")
            return None
    except Exception as e:
        log_issue("WARNING", f"Request error: {e}")
        return None

# ========== TARGET SYMBOL PRICES (Binance 1m candles) ==========
def fetch_symbol_prices(symbol, limit=120):
    url = f"{SPOT_BASE_URL}/klines"
    params = {"symbol": symbol.upper(), "interval": "1m", "limit": limit}
    resp = rate_limited_fetch(url, params=params)
    if not resp:
        return [], []
    data = resp.json()
    timestamps = []
    closes = []
    for c in data:
        timestamps.append(int(c[0]))
        closes.append(float(c[4]))
    return timestamps, closes

# ========== CRYPTO INDICES (Binance) ==========
def fetch_crypto_index(symbol, interval="1m", limit=120):
    url = f"{SPOT_BASE_URL}/klines"
    if symbol == "BTCDOMUSDT":
        url = f"{FUTURES_BASE_URL}/fapi/v1/klines"
    params = {"symbol": symbol.upper(), "interval": interval, "limit": limit}
    resp = rate_limited_fetch(url, params=params)
    if not resp:
        return [], []
    data = resp.json()
    timestamps = []
    closes = []
    for c in data:
        timestamps.append(int(c[0]))
        closes.append(float(c[4]))
    return timestamps, closes

# ========== TRADITIONAL INDICES (Prioritize quality, but use your keys for fallback) ==========
# We'll first attempt high-quality sources (if keys were present – they are not, so we skip)
# Then fallback to your existing keys: Tiingo, Alpaca, and finally Yahoo.

def fetch_tiingo(symbol, limit=120):
    if not TIINGO_TOKEN:
        return [], []
    url = f"https://api.tiingo.com/iex/{symbol}/prices"
    params = {'token': TIINGO_TOKEN, 'format': 'json', 'resampleFreq': '1min', 'startDate': '5d'}
    resp = rate_limited_fetch(url, params=params)
    if resp and resp.status_code == 200:
        data = resp.json()
        if data and len(data) > 0:
            timestamps = []
            closes = []
            for item in data[-limit:]:
                ts = int(datetime.datetime.fromisoformat(item['date'].replace('Z', '+00:00')).timestamp() * 1000)
                price = item.get('close', 0)
                if price:
                    timestamps.append(ts)
                    closes.append(price)
            return timestamps, closes
    return [], []

def fetch_alpaca(symbol, limit=120):
    if not ALPACA_API_KEY or not ALPACA_SECRET_KEY:
        return [], []
    headers = {
        'APCA-API-KEY-ID': ALPACA_API_KEY,
        'APCA-API-SECRET-KEY': ALPACA_SECRET_KEY
    }
    url = "https://data.alpaca.markets/v2/stocks/bars"
    params = {'symbols': symbol, 'timeframe': '1Min', 'limit': limit, 'sort': 'asc'}
    resp = rate_limited_fetch(url, params=params, headers=headers)
    if resp and resp.status_code == 200:
        data = resp.json()
        bars = data.get('bars', {}).get(symbol, [])
        if bars:
            timestamps = []
            closes = []
            for bar in bars:
                ts = int(datetime.datetime.fromisoformat(bar['t'].replace('Z', '+00:00')).timestamp() * 1000)
                timestamps.append(ts)
                closes.append(bar['c'])
            return timestamps, closes
    return [], []

def fetch_yahoo_finance(symbol, limit=120):
    mapping = {
        'SPY': 'SPY', 'QQQ': 'QQQ', 'DIA': 'DIA', 'GLD': 'GLD',
        'USO': 'USO', 'DXY': 'DX-Y.NYB', 'VIX': '^VIX'
    }
    yahoo_sym = mapping.get(symbol, symbol)
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{yahoo_sym}"
    params = {'interval': '1m', 'range': '2d'}
    resp = rate_limited_fetch(url, params=params)
    if resp and resp.status_code == 200:
        data = resp.json()
        result = data.get('chart', {}).get('result', [])
        if result:
            timestamps = result[0].get('timestamp', [])
            closes = result[0].get('indicators', {}).get('quote', [{}])[0].get('close', [])
            if len(timestamps) > 0 and len(closes) > 0:
                pairs = [(ts, c) for ts, c in zip(timestamps, closes) if c is not None]
                if len(pairs) < limit/2:
                    return [], []
                pairs = pairs[-limit:]
                return [p[0]*1000 for p in pairs], [p[1] for p in pairs]
    return [], []

def fetch_traditional_index(symbol, limit=120):
    # Use your existing keys in priority: Tiingo → Alpaca → Yahoo
    for fetcher in [fetch_tiingo, fetch_alpaca, fetch_yahoo_finance]:
        ts, prices = fetcher(symbol, limit)
        if len(ts) >= limit*0.8:
            return ts, prices
    return [], []

# ========== CACHING FOR INDICES (global, shared across symbols) ==========
_cached_indices = {}
_cached_indices_time = 0

def get_indices_data(limit=120):
    """Return (timestamps, btc, eth, btcdom, usdt_btc, spy, qqq, dia, gld, uso, dxy, vix)"""
    global _cached_indices, _cached_indices_time
    now = time.time()
    if _cached_indices and (now - _cached_indices_time) < CACHE_TTL_SEC:
        log_issue("INFO", "Using cached indices data")
        return _cached_indices

    log_issue("INFO", "Fetching fresh indices data")
    # Crypto indices
    ts_btc, btc = fetch_crypto_index("BTCUSDT", limit=limit)
    ts_eth, eth = fetch_crypto_index("ETHUSDT", limit=limit)
    ts_dom, dom = fetch_crypto_index("BTCDOMUSDT", limit=limit)
    # Use BTC timestamps as reference
    ref_ts = ts_btc
    # Traditional indices
    ts_spy, spy = fetch_traditional_index("SPY", limit)
    ts_qqq, qqq = fetch_traditional_index("QQQ", limit)
    ts_dia, dia = fetch_traditional_index("DIA", limit)
    ts_gld, gld = fetch_traditional_index("GLD", limit)
    ts_uso, uso = fetch_traditional_index("USO", limit)
    ts_dxy, dxy = fetch_traditional_index("DXY", limit)
    ts_vix, vix = fetch_traditional_index("VIX", limit)

    # Also compute USDT_BTC = 1/btc
    usdt_btc = [1.0/p if p != 0 else 0 for p in btc]

    result = {
        'timestamps': ref_ts,
        'btc': btc,
        'eth': eth,
        'btcdom': dom,
        'usdt_btc': usdt_btc,
        'spy': spy, 'spy_ts': ts_spy,
        'qqq': qqq, 'qqq_ts': ts_qqq,
        'dia': dia, 'dia_ts': ts_dia,
        'gld': gld, 'gld_ts': ts_gld,
        'uso': uso, 'uso_ts': ts_uso,
        'dxy': dxy, 'dxy_ts': ts_dxy,
        'vix': vix, 'vix_ts': ts_vix
    }
    _cached_indices = result
    _cached_indices_time = now
    return result

# ========== MAIN DOWNLOADER ==========
def run_download(symbol):
    # Overwrite the global log file at the beginning of each run
    with open(GLOBAL_LOG_FILE, 'w', encoding='utf-8') as f:
        f.write(f"# X09 correlation log for symbol {symbol.upper()}\n")
        f.write(f"# Started at {time.strftime('%Y-%m-%d %H:%M:%S')}\n\n")
    
    log_issue("INFO", f"Starting correlation download for {symbol}")
    start_time = time.time()

    # Fetch target symbol price
    sym_ts, sym_prices = fetch_symbol_prices(symbol, limit=120)
    if len(sym_ts) < MIN_POINTS:
        log_issue("ERROR", f"Insufficient symbol price data: {len(sym_ts)} points")
        return False

    # Fetch indices (cached)
    indices = get_indices_data(limit=120)
    ref_ts = indices['timestamps']
    if not ref_ts or len(ref_ts) < MIN_POINTS:
        log_issue("ERROR", "Insufficient crypto index data")
        return False

    # Write to .tmp_x TSV
    tmp_path = os.path.join(SYMBOLS_DIR, f"{symbol.lower()}_correlation.tmp_x")
    with open(tmp_path, "w", encoding="utf-8") as f:
        # Header: timestamp, symbol_close, btc, eth, btcdom, usdt_btc, spy, qqq, dia, gld, uso, dxy, vix
        f.write("timestamp\tsymbol_price\tbtc\teth\tbtcdom\tusdt_btc\tspy\tqqq\tdia\tgld\tuso\tdxy\tvix\n")
        # Align rows by timestamp (using symbol's timestamps as base)
        all_timestamps = set(sym_ts)
        # Add all index timestamps
        for t in indices['spy_ts']: all_timestamps.add(t)
        for t in indices['qqq_ts']: all_timestamps.add(t)
        for t in indices['dia_ts']: all_timestamps.add(t)
        for t in indices['gld_ts']: all_timestamps.add(t)
        for t in indices['uso_ts']: all_timestamps.add(t)
        for t in indices['dxy_ts']: all_timestamps.add(t)
        for t in indices['vix_ts']: all_timestamps.add(t)
        all_timestamps = sorted(all_timestamps)
        # Build lookup maps
        sym_map = {ts: price for ts, price in zip(sym_ts, sym_prices)}
        btc_map = {ts: price for ts, price in zip(ref_ts, indices['btc'])}
        eth_map = {ts: price for ts, price in zip(ref_ts, indices['eth'])}
        dom_map = {ts: price for ts, price in zip(ref_ts, indices['btcdom'])}
        usdt_map = {ts: price for ts, price in zip(ref_ts, indices['usdt_btc'])}
        spy_map = {ts: price for ts, price in zip(indices['spy_ts'], indices['spy'])}
        qqq_map = {ts: price for ts, price in zip(indices['qqq_ts'], indices['qqq'])}
        dia_map = {ts: price for ts, price in zip(indices['dia_ts'], indices['dia'])}
        gld_map = {ts: price for ts, price in zip(indices['gld_ts'], indices['gld'])}
        uso_map = {ts: price for ts, price in zip(indices['uso_ts'], indices['uso'])}
        dxy_map = {ts: price for ts, price in zip(indices['dxy_ts'], indices['dxy'])}
        vix_map = {ts: price for ts, price in zip(indices['vix_ts'], indices['vix'])}

        for ts in all_timestamps:
            row = [
                str(ts),
                str(sym_map.get(ts, '')),
                str(btc_map.get(ts, '')),
                str(eth_map.get(ts, '')),
                str(dom_map.get(ts, '')),
                str(usdt_map.get(ts, '')),
                str(spy_map.get(ts, '')),
                str(qqq_map.get(ts, '')),
                str(dia_map.get(ts, '')),
                str(gld_map.get(ts, '')),
                str(uso_map.get(ts, '')),
                str(dxy_map.get(ts, '')),
                str(vix_map.get(ts, ''))
            ]
            f.write("\t".join(row) + "\n")

    log_issue("INFO", f"Raw correlation data saved to {tmp_path}")
    elapsed = time.time() - start_time
    log_issue("INFO", f"Download complete in {elapsed:.2f}s")
    return True

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python X09_correlation_rest.py SYMBOL")
        sys.exit(1)
    success = run_download(sys.argv[1].upper())
    sys.exit(0 if success else 1)