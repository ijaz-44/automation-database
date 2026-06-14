#!/usr/bin/env python3
"""
X01_klines_rest.py – Raw Candle Downloader (Only .tmp_x + Global Log)
- Fetches candles (limits: 1m:200,5m:180,15m:120,1h:120,4h:80)
- Writes raw candles to temporary TSV: {symbol}.tmp_x
- Logs issues to market_data/binance/symbols/X01_candles.log (global, append)
- No processing, no E01, no compression, no final TSV.
"""

import os
import sys
import time
import requests
import random
import json
import shutil
from typing import List, Dict, Tuple

# ========== CONFIGURATION ==========
TIMEFRAME_SECONDS = {"1m":60,"5m":300,"15m":900,"1h":3600,"4h":14400}
LIMITS = {
    "1m": 200,
    "5m": 180,
    "15m": 120,
    "1h": 120,
    "4h": 80
}
MIN_REQUIRED_CANDLES = {
    "1m": 180,
    "5m": 160,
    "15m": 100,
    "1h": 100,
    "4h": 70
}
MAX_RETRIES = 3
REQUEST_TIMEOUT = 10
RATE_PER_SEC = 10
RATE_BURST = 5
FEATURES_BASE_DIR = os.path.join("market_data", "binance", "symbols")
TIMEFRAME_FETCH_TIMEOUT = 25

os.makedirs(FEATURES_BASE_DIR, exist_ok=True)

# ========== GLOBAL LOG FILE ==========
LOG_FILE = os.path.join(FEATURES_BASE_DIR, "X01_candles.log")
LOG_MAX_SIZE = 5 * 1024 * 1024  # 5 MB

def rotate_log_if_needed():
    if os.path.exists(LOG_FILE) and os.path.getsize(LOG_FILE) > LOG_MAX_SIZE:
        backup = LOG_FILE + ".old"
        try:
            os.replace(LOG_FILE, backup)
        except:
            pass

def log_issue(level, msg, **kwargs):
    """Append to global log file X01_candles.log"""
    rotate_log_if_needed()
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    line = f"{ts} [{level}] {msg}"
    if kwargs:
        line += " " + json.dumps(kwargs)
    print(line)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(line + "\n")

# ========== SYMBOL VALIDATION ==========
def is_valid_symbol(symbol: str) -> bool:
    return bool(symbol) and symbol.isalnum() and len(symbol) <= 15

# ========== DISK SPACE CHECK ==========
def check_disk_space():
    try:
        total, used, free = shutil.disk_usage(".")
        if free < 50 * 1024 * 1024:
            raise Exception("Low disk space: less than 50 MB free")
        if not os.access(FEATURES_BASE_DIR, os.W_OK):
            raise Exception(f"No write permission in {FEATURES_BASE_DIR}")
    except Exception as e:
        log_issue("ERROR", "Pre‑flight check failed", error=str(e))
        raise

# ========== RATE LIMITER ==========
class RateLimiter:
    def __init__(self, rate, burst):
        self.rate = rate
        self.cap = rate + burst
        self.tokens = self.cap
        self.last = time.monotonic()
    def wait(self):
        now = time.monotonic()
        self.tokens = min(self.cap, self.tokens + (now - self.last) * self.rate)
        self.last = now
        if self.tokens < 1:
            sleep_time = (1 - self.tokens) / self.rate
            time.sleep(sleep_time + random.uniform(0,0.02))
            self.last = time.monotonic()
            self.tokens = 0
        else:
            self.tokens -= 1
_rate_limiters = {"binance": RateLimiter(RATE_PER_SEC, RATE_BURST)}

# ========== HTTP SESSION ==========
_session = requests.Session()
_session.headers.update({"User-Agent": "X01-ETL/1.0"})
adapter = requests.adapters.HTTPAdapter(max_retries=0)
_session.mount("https://", adapter)
_session.mount("http://", adapter)

# ========== FETCH CANDLES ==========
def fetch_candles(symbol: str, interval: str, limit: int, timeout_sec: float) -> Tuple[List[Dict], bool, int, str]:
    fetch_start = time.time()
    symbol = symbol.upper()
    _rate_limiters["binance"].wait()
    url = "https://api.binance.com/api/v3/klines"
    params = {"symbol": symbol, "interval": interval, "limit": limit}
    for attempt in range(MAX_RETRIES):
        if time.time() - fetch_start > timeout_sec:
            log_issue("ERROR", "Fetch timed out", symbol=symbol, interval=interval)
            return [], False, 0, ""
        try:
            r = _session.get(url, params=params, timeout=REQUEST_TIMEOUT)
            if r.status_code == 429:
                time.sleep(2**attempt + random.uniform(0,0.5))
                continue
            r.raise_for_status()
            data = r.json()
            unique = {}
            now_ms = int(time.time() * 1000)
            interval_ms = TIMEFRAME_SECONDS.get(interval, 60) * 1000
            for c in data:
                try:
                    ts_ms = c[0]
                    if ts_ms > now_ms + 60000 or ts_ms < now_ms - (10 * 365 * 86400 * 1000):
                        continue
                    open_p = float(c[1]); high_p = float(c[2]); low_p = float(c[3])
                    close_p = float(c[4]); volume_p = float(c[5])
                    if volume_p < 0 or not (low_p <= open_p <= high_p and low_p <= close_p <= high_p):
                        continue
                except (ValueError, TypeError):
                    continue
                if len(unique) > limit * 1.1:
                    oldest = min(unique.keys())
                    del unique[oldest]
                unique[ts_ms] = {
                    "timestamp_ms": ts_ms,
                    "open": open_p, "high": high_p, "low": low_p,
                    "close": close_p, "volume": volume_p
                }
            candles = sorted(unique.values(), key=lambda x: x['timestamp_ms'])
            min_needed = MIN_REQUIRED_CANDLES.get(interval, limit * 0.8)
            if len(candles) < min_needed:
                log_issue("ERROR", "Too few candles", tf=interval, got=len(candles), needed=min_needed)
                return [], False, 0, ""
            max_gap_found = 0
            gap_detected = False
            gap_type = "none"
            for i in range(1, len(candles)):
                diff = candles[i]['timestamp_ms'] - candles[i-1]['timestamp_ms']
                if diff != interval_ms:
                    gap_detected = True
                    if diff > max_gap_found: max_gap_found = diff
                    if diff > interval_ms * 3: gap_type = "large_missing_candle"
                    elif diff > interval_ms * 1.5: gap_type = "missed_candle"
                    else: gap_type = "latency_jitter"
                    log_issue("WARNING", "Gap detected", symbol=symbol, tf=interval, gap_ms=diff, type=gap_type)
            last_ts = candles[-1]['timestamp_ms']
            min_stale_ms = max(2 * interval_ms, 120000)
            age = now_ms - last_ts
            if age > min_stale_ms:
                log_issue("ERROR", "Dataset too stale", tf=interval, age_sec=age//1000)
                return [], False, 0, ""
            return candles, gap_detected, max_gap_found, gap_type
        except Exception as e:
            if attempt == MAX_RETRIES-1:
                log_issue("ERROR", f"Fetch failed {symbol} {interval}", error=str(e))
                return [], False, 0, ""
            time.sleep((1.5**attempt) + random.uniform(0,0.3))
    return [], False, 0, ""

def write_raw_tsv(symbol: str, candles_by_tf: Dict[str, List[Dict]], tmp_tsv_path: str):
    """Write raw candles to temporary TSV (one row per candle)."""
    with open(tmp_tsv_path, "w") as f:
        for tf in sorted(candles_by_tf.keys()):
            for c in candles_by_tf[tf]:
                f.write(f"{symbol.upper()}\t{tf}\t{c['timestamp_ms']}\t{c['open']}\t{c['high']}\t{c['low']}\t{c['close']}\t{c['volume']}\n")
    log_issue("INFO", f"Raw TSV written: {tmp_tsv_path}", rows=sum(len(v) for v in candles_by_tf.values()))

# ========== MAIN DOWNLOADER ==========
def run_download(symbol: str) -> bool:
    """Only download candles and save as {symbol}.tmp_x, log to global X01_candles.log"""
    if not is_valid_symbol(symbol):
        print(f"[X01] Invalid symbol: {symbol}")
        log_issue("ERROR", f"Invalid symbol: {symbol}")
        return False

    log_issue("INFO", f"=== Starting download for {symbol} ===")
    try:
        tmp_x_path = os.path.join(FEATURES_BASE_DIR, f"{symbol.lower()}.tmp_x")
        check_disk_space()
        timeframes = ["1m","5m","15m","1h","4h"]
        fetched = {}
        for tf in timeframes:
            limit = LIMITS.get(tf, 500)
            candles, _, max_gap, gap_type = fetch_candles(symbol, tf, limit, TIMEFRAME_FETCH_TIMEOUT)
            if not candles:
                time.sleep(2)
                candles, _, max_gap, gap_type = fetch_candles(symbol, tf, limit, TIMEFRAME_FETCH_TIMEOUT)
            if not candles:
                log_issue("ERROR", f"Missing timeframe {tf} after retry", symbol=symbol)
                return False
            fetched[tf] = candles
            if max_gap:
                log_issue("WARNING", f"Gap in {tf}", max_gap_ms=max_gap, type=gap_type)

        write_raw_tsv(symbol, fetched, tmp_x_path)
        log_issue("INFO", f"Download complete -> {tmp_x_path}")
        return True
    except Exception as e:
        log_issue("ERROR", f"Download failed for {symbol}", error=str(e))
        return False

# ========== EXPORTED FUNCTIONS ==========
def fetch_and_update_ws(symbol: str, ws_instance=None):
    return run_download(symbol)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python X01_klines_rest.py SYMBOL")
        sys.exit(1)
    success = run_download(sys.argv[1].upper())
    sys.exit(0 if success else 1)