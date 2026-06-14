#!/usr/bin/env python3
"""
X19_volProfile_rest.py – Optimized Raw Volume Profile Downloader (two files, lossless)
- Fetches daily (15), 1h (168), 4h (42) candles – plain TSV.
- Fetches 1m candles for 15 days + last 60 minutes – compressed into rows of 30 candles using integer scaling.
- Splits into two files:
    {symbol}_volProfile1.tmp_x = daily+1h+4h + first 8 days of 1m (compressed)
    {symbol}_volProfile2.tmp_x = remaining 7 days of 1m (compressed) + last 60m (compressed)
- Compression uses delta encoding on timestamps, OHLC, volume with multiplier 1e8 (lossless).
- Output files open instantly on mobile.
- Logs to market_data/binance/symbols/X19_volProfile.log
"""

import os
import sys
import time
import requests
import datetime

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SYMBOLS_DIR = os.path.join(BASE_DIR, "market_data", "binance", "symbols")
os.makedirs(SYMBOLS_DIR, exist_ok=True)

# Global log file
LOG_FILE = os.path.join(SYMBOLS_DIR, "X19_volProfile.log")
LOG_MAX_SIZE = 5_000_000

def rotate_log_if_needed():
    if os.path.exists(LOG_FILE) and os.path.getsize(LOG_FILE) > LOG_MAX_SIZE:
        backup = LOG_FILE + ".old"
        try:
            os.replace(LOG_FILE, backup)
        except:
            pass

def log_issue(level, msg, **kwargs):
    rotate_log_if_needed()
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    line = f"{ts} [{level}] {msg}"
    if kwargs:
        line += " " + str(kwargs)
    # Always write to log file
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(line + "\n")
    # Print only errors and important milestones to console
    if level == "ERROR" or msg.startswith("Downloading") or msg.startswith("Done"):
        print(line)

BINANCE_KLINES_URL = "https://api.binance.com/api/v3/klines"
RATE_LIMIT_SEC = 0.2

def rate_limited_fetch(url, params=None, retries=2):
    _last_call = getattr(rate_limited_fetch, '_last_call', 0)
    for _ in range(retries):
        now = time.time()
        if now - _last_call < RATE_LIMIT_SEC:
            time.sleep(RATE_LIMIT_SEC - (now - _last_call))
        _last_call = time.time()
        try:
            r = requests.get(url, params=params, timeout=20)
            if r.status_code == 200:
                return r.json()
            elif r.status_code == 429:
                time.sleep(2)
                continue
        except:
            pass
        time.sleep(1)
    return None

def fetch_candles(symbol, interval, start_ms=None, end_ms=None, limit=1000):
    params = {"symbol": symbol.upper(), "interval": interval}
    if start_ms:
        params["startTime"] = start_ms
    if end_ms:
        params["endTime"] = end_ms
    params["limit"] = limit
    data = rate_limited_fetch(BINANCE_KLINES_URL, params)
    if not data:
        return []
    return [{
        "timestamp": int(c[0]),
        "open": float(c[1]),
        "high": float(c[2]),
        "low": float(c[3]),
        "close": float(c[4]),
        "volume": float(c[5])
    } for c in data]

def fetch_1m_candles_for_days(symbol, start_day_offset, days):
    """Fetch 1m candles for a range of days, return list."""
    candles = []
    now = int(time.time() * 1000)
    today_start = int(datetime.datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0).timestamp() * 1000)
    for d in range(start_day_offset, start_day_offset + days):
        day_start = today_start - (d + 1) * 86400000
        day_end = day_start + 86400000
        if day_end > now:
            continue
        current_start = day_start
        while current_start < day_end:
            params = {
                "symbol": symbol.upper(),
                "interval": "1m",
                "startTime": current_start,
                "endTime": day_end,
                "limit": 1000
            }
            data = rate_limited_fetch(BINANCE_KLINES_URL, params)
            if not data:
                break
            for c in data:
                candles.append({
                    "timestamp": int(c[0]),
                    "open": float(c[1]),
                    "high": float(c[2]),
                    "low": float(c[3]),
                    "close": float(c[4]),
                    "volume": float(c[5])
                })
            if len(data) < 1000:
                break
            current_start = data[-1][0] + 60000
    return candles

# ---------- Compression helpers ----------
MULT = 100_000_000  # 8 decimals

def compress_1m_candles(candles):
    """Group 30 candles per row, first absolute, next 29 deltas. Returns list of compressed row strings."""
    if not candles:
        return []
    rows = []
    ROW_SIZE = 30
    for i in range(0, len(candles), ROW_SIZE):
        chunk = candles[i:i+ROW_SIZE]
        parts = []
        # First candle: absolute values (int)
        c0 = chunk[0]
        t0 = c0['timestamp'] // 1000
        o0 = int(round(c0['open'] * MULT))
        h0 = int(round(c0['high'] * MULT))
        l0 = int(round(c0['low'] * MULT))
        co0 = int(round(c0['close'] * MULT))
        v0 = int(round(c0['volume'] * MULT))
        parts.append(f"{t0},{o0},{h0},{l0},{co0},{v0}")
        prev_t, prev_o, prev_h, prev_l, prev_c, prev_v = t0, o0, h0, l0, co0, v0
        for c in chunk[1:]:
            t = c['timestamp'] // 1000
            o = int(round(c['open'] * MULT))
            h = int(round(c['high'] * MULT))
            l = int(round(c['low'] * MULT))
            co = int(round(c['close'] * MULT))
            v = int(round(c['volume'] * MULT))
            dt = t - prev_t
            do = o - prev_o
            dh = h - prev_h
            dl = l - prev_l
            dc = co - prev_c
            dv = v - prev_v
            parts.append(f"{dt},{do},{dh},{dl},{dc},{dv}")
            prev_t, prev_o, prev_h, prev_l, prev_c, prev_v = t, o, h, l, co, v
        rows.append("|".join(parts))
    return rows

def write_combined_file(symbol, plain_candles_dict, compressed_candles, suffix):
    """Write one file containing plain TSV lines (daily/1h/4h) followed by compressed 1m rows."""
    path = os.path.join(SYMBOLS_DIR, f"{symbol.lower()}_volProfile{suffix}.tmp_x")
    with open(path, "w", encoding="utf-8") as f:
        # Write plain candles (daily, 1h, 4h)
        for typ, lst in plain_candles_dict.items():
            for c in lst:
                f.write(f"{typ}\t{c['timestamp']}\t{c['open']:.8f}\t{c['high']:.8f}\t{c['low']:.8f}\t{c['close']:.8f}\t{c['volume']:.8f}\n")
        # Write a marker for compressed 1m candles
        if compressed_candles:
            f.write("#1m_compressed_start\n")
            f.write(f"#mult={MULT}\n")
            f.write(f"#rows={len(compressed_candles)}\n")
            for idx, row in enumerate(compressed_candles):
                f.write(f"1m_comp\t{idx}\t{row}\n")
            f.write("#1m_compressed_end\n")
    return path

def run_download(symbol):
    log_issue("INFO", f"Downloading {symbol}")
    start_time = time.time()

    # Fetch plain candles (few)
    daily = fetch_candles(symbol, "1d", limit=15)
    log_issue("INFO", f"Fetched {len(daily)} daily candles")
    oneh = fetch_candles(symbol, "1h", limit=168)
    log_issue("INFO", f"Fetched {len(oneh)} 1h candles")
    fourh = fetch_candles(symbol, "4h", limit=42)
    log_issue("INFO", f"Fetched {len(fourh)} 4h candles")

    # Fetch 1m candles: first 8 days, then next 7 days, then last 60 minutes
    days1 = fetch_1m_candles_for_days(symbol, 0, 8)
    log_issue("INFO", f"Fetched {len(days1)} 1m candles (first 8 days)")
    days2 = fetch_1m_candles_for_days(symbol, 8, 7)
    log_issue("INFO", f"Fetched {len(days2)} 1m candles (next 7 days)")
    last60 = fetch_candles(symbol, "1m", limit=60)
    log_issue("INFO", f"Fetched {len(last60)} 1m candles (last 60 minutes)")

    # Compress 1m groups
    comp1 = compress_1m_candles(days1)
    comp2 = compress_1m_candles(days2 + last60)

    # Write file 1: plain (daily+1h+4h) + comp1
    file1 = write_combined_file(symbol, {"daily": daily, "1h": oneh, "4h": fourh}, comp1, "1")
    # Write file 2: only comp2
    file2 = write_combined_file(symbol, {}, comp2, "2")

    elapsed = time.time() - start_time
    log_issue("INFO", f"File 1: {os.path.basename(file1)} ({len(daily)+len(oneh)+len(fourh)} plain + {len(comp1)} compressed rows)")
    log_issue("INFO", f"File 2: {os.path.basename(file2)} ({len(comp2)} compressed rows)")
    log_issue("INFO", f"Done in {elapsed:.1f}s")
    return True

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python X19_volProfile_rest.py SYMBOL")
        sys.exit(1)
    success = run_download(sys.argv[1].upper())
    sys.exit(0 if success else 1)