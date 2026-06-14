#!/usr/bin/env python3
"""
P01_volatility_price_action.py
Processes raw candles from X01 (.tmp_x) and computes high‑impact features.
Input:  market_data/binance/symbols/{symbol}.tmp_x
Output: market_data/binance/symbols/{symbol}.tmp_p
Log:    market_data/binance/symbols/p01_issues.log
"""

import os
import sys
import math
import time
from datetime import datetime
from collections import defaultdict

# ========== CONFIGURATION ==========
FEATURES_BASE_DIR = os.path.join("market_data", "binance", "symbols")
LOG_FILE = os.path.join(FEATURES_BASE_DIR, "p01_issues.log")
LOG_MAX_SIZE = 5_000_000

os.makedirs(FEATURES_BASE_DIR, exist_ok=True)

# ========== LOGGING (minimal console) ==========
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
    # Print only errors and start/end messages
    if level == "ERROR" or msg.startswith("Starting processing") or msg.startswith("Processing complete"):
        print(line)
    rotate_log_if_needed()
    with open(LOG_FILE, "a") as f:
        f.write(line + "\n")

# ========== DATA READING ==========
def read_tmp_x(symbol):
    """Read {symbol}.tmp_x and return list of candles (1m only)."""
    tmp_path = os.path.join(FEATURES_BASE_DIR, f"{symbol.lower()}.tmp_x")
    if not os.path.exists(tmp_path):
        log_issue("ERROR", f"Input file not found: {tmp_path}")
        return None
    candles = []
    with open(tmp_path, "r") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            parts = line.split('\t')
            if len(parts) < 8:
                continue
            symbol_tf, tf, ts, o, h, l, c, v = parts
            if tf != "1m":
                continue
            candles.append({
                'timestamp': int(ts),
                'open': float(o),
                'high': float(h),
                'low': float(l),
                'close': float(c),
                'volume': float(v)
            })
    candles.sort(key=lambda x: x['timestamp'])
    return candles

# ========== UTILITY FUNCTIONS ==========
def log_returns(prices):
    rets = [0.0]
    for i in range(1, len(prices)):
        if prices[i-1] == 0:
            rets.append(0.0)
        else:
            rets.append(math.log(prices[i] / prices[i-1]))
    return rets

def std_dev(values):
    n = len(values)
    if n < 2:
        return 0.0
    mean = sum(values) / n
    var = sum((x - mean) ** 2 for x in values) / n
    return math.sqrt(var)

# ========== VOLATILITY METRICS ==========
def realized_volatility(candles, period=20):
    closes = [c['close'] for c in candles]
    log_rets = log_returns(closes)
    rvol = [None] * len(candles)
    for i in range(period-1, len(candles)):
        window = log_rets[i-period+1:i+1]
        rvol[i] = std_dev(window)
    return rvol

def parkinson_volatility(candles, period=20):
    pvol = [None] * len(candles)
    for i in range(period-1, len(candles)):
        sum_sq = 0.0
        for j in range(i-period+1, i+1):
            hl_ratio = candles[j]['high'] / candles[j]['low']
            if hl_ratio > 0:
                sum_sq += (math.log(hl_ratio)) ** 2
        mean_sq = sum_sq / period
        pvol[i] = math.sqrt(mean_sq / (4 * math.log(2))) if mean_sq > 0 else 0.0
    return pvol

def yang_zhang_volatility(candles, period=20):
    n = len(candles)
    yz = [None] * n
    if n < period+1:
        return yz
    for i in range(period, n):
        overnight = []
        open_close = []
        close_open = []
        for j in range(i-period+1, i+1):
            if j > 0 and candles[j-1]['close'] > 0:
                overnight.append(math.log(candles[j]['open'] / candles[j-1]['close']))
            if candles[j]['open'] > 0:
                open_close.append(math.log(candles[j]['close'] / candles[j]['open']))
            if j+1 < n and candles[j+1]['open'] > 0:
                close_open.append(math.log(candles[j+1]['open'] / candles[j]['close']))
        var_o = std_dev(overnight) ** 2 if len(overnight) > 1 else 0
        var_oc = std_dev(open_close) ** 2 if len(open_close) > 1 else 0
        var_co = std_dev(close_open) ** 2 if len(close_open) > 1 else 0
        k = 0.34
        total_var = var_o + k * var_oc + (1 - k) * var_co
        yz[i] = math.sqrt(total_var)
    return yz

def atr(candles, period=14):
    tr = [0.0] * len(candles)
    for i in range(1, len(candles)):
        hl = candles[i]['high'] - candles[i]['low']
        hc = abs(candles[i]['high'] - candles[i-1]['close'])
        lc = abs(candles[i]['low'] - candles[i-1]['close'])
        tr[i] = max(hl, hc, lc)
    atr_vals = [None] * len(candles)
    for i in range(period-1, len(candles)):
        atr_vals[i] = sum(tr[i-period+1:i+1]) / period
    return atr_vals

# ========== PRICE MOMENTUM ==========
def rate_of_change(candles, period=5):
    roc = [None] * len(candles)
    for i in range(period, len(candles)):
        prev_close = candles[i-period]['close']
        if prev_close != 0:
            roc[i] = (candles[i]['close'] - prev_close) / prev_close * 100
        else:
            roc[i] = 0.0
    return roc

def linear_slope(candles, period=10):
    slopes = [None] * len(candles)
    for i in range(period-1, len(candles)):
        xs = list(range(period))
        ys = [candles[i-period+1+j]['close'] for j in range(period)]
        n = period
        sum_x = sum(xs)
        sum_y = sum(ys)
        sum_xy = sum(xs[j] * ys[j] for j in range(n))
        sum_x2 = sum(x * x for x in xs)
        denom = n * sum_x2 - sum_x * sum_x
        slope = (n * sum_xy - sum_x * sum_y) / denom if denom != 0 else 0.0
        slopes[i] = slope
    return slopes

# ========== TIME FEATURES ==========
def time_features(candles):
    hours = []
    minutes = []
    weekdays = []
    for c in candles:
        dt = datetime.utcfromtimestamp(c['timestamp'] / 1000)
        hours.append(dt.hour)
        minutes.append(dt.minute)
        weekdays.append(dt.weekday())  # Monday=0
    return hours, minutes, weekdays

# ========== CANDLESTICK PATTERNS ==========
def is_bullish(c): return c['close'] > c['open']
def is_bearish(c): return c['close'] < c['open']
def body_len(c): return abs(c['close'] - c['open'])
def wick_upper(c): return c['high'] - max(c['open'], c['close'])
def wick_lower(c): return min(c['open'], c['close']) - c['low']
def total_range(c): return c['high'] - c['low']

def detect_engulfing(candles, idx):
    if idx < 1:
        return None
    prev, curr = candles[idx-1], candles[idx]
    if is_bearish(prev) and is_bullish(curr) and curr['open'] < prev['close'] and curr['close'] > prev['open']:
        return "bullish_engulfing"
    if is_bullish(prev) and is_bearish(curr) and curr['open'] > prev['close'] and curr['close'] < prev['open']:
        return "bearish_engulfing"
    return None

def detect_inside_bar(candles, idx):
    if idx < 1:
        return None
    prev, curr = candles[idx-1], candles[idx]
    if curr['high'] <= prev['high'] and curr['low'] >= prev['low']:
        return "inside_bar"
    return None

def detect_pin_bar(candles, idx):
    c = candles[idx]
    body = body_len(c)
    rng = total_range(c)
    if rng == 0:
        return None
    up = wick_upper(c)
    low_w = wick_lower(c)
    if low_w > 2 * body and up < body and is_bullish(c):
        return "bullish_pin_bar"
    if up > 2 * body and low_w < body and is_bearish(c):
        return "bearish_pin_bar"
    return None

def patterns_for_candle(candles, idx):
    pats = []
    e = detect_engulfing(candles, idx)
    if e: pats.append(e)
    ib = detect_inside_bar(candles, idx)
    if ib: pats.append(ib)
    pb = detect_pin_bar(candles, idx)
    if pb: pats.append(pb)
    return '|'.join(pats) if pats else ''

# ========== TREND STRUCTURE ==========
def find_swings(candles, lookback=2):
    n = len(candles)
    highs = [None] * n
    lows = [None] * n
    for i in range(lookback, n - lookback):
        # swing high
        if all(candles[i]['high'] > candles[i-j]['high'] for j in range(1, lookback+1)) and \
           all(candles[i]['high'] > candles[i+j]['high'] for j in range(1, lookback+1)):
            highs[i] = candles[i]['high']
        # swing low
        if all(candles[i]['low'] < candles[i-j]['low'] for j in range(1, lookback+1)) and \
           all(candles[i]['low'] < candles[i+j]['low'] for j in range(1, lookback+1)):
            lows[i] = candles[i]['low']
    return highs, lows

def higher_highs_lower_lows(candles, lookback=5):
    n = len(candles)
    hh = [False] * n
    hl = [False] * n
    lh = [False] * n
    ll = [False] * n
    for i in range(lookback, n):
        prev_highs = [c['high'] for c in candles[i-lookback:i]]
        prev_lows = [c['low'] for c in candles[i-lookback:i]]
        hh[i] = candles[i]['high'] > max(prev_highs) if prev_highs else False
        hl[i] = candles[i]['low'] > max(prev_lows) if prev_lows else False
        lh[i] = candles[i]['high'] < min(prev_highs) if prev_highs else False
        ll[i] = candles[i]['low'] < min(prev_lows) if prev_lows else False
    return hh, hl, lh, ll

# ========== MAIN PROCESSING ==========
def process_and_save(symbol):
    log_issue("INFO", f"Starting processing for {symbol}")
    start_time = time.time()

    candles = read_tmp_x(symbol)
    if candles is None:
        return False
    if len(candles) < 50:
        log_issue("ERROR", f"Insufficient candles ({len(candles)}) for {symbol}")
        return False

    n = len(candles)
    rvol = realized_volatility(candles, 20)
    pvol = parkinson_volatility(candles, 20)
    yzvol = yang_zhang_volatility(candles, 20)
    atr_vals = atr(candles, 14)
    roc5 = rate_of_change(candles, 5)
    roc10 = rate_of_change(candles, 10)
    slope10 = linear_slope(candles, 10)
    hours, minutes, weekdays = time_features(candles)
    patterns = [patterns_for_candle(candles, i) for i in range(n)]
    swing_high, swing_low = find_swings(candles, 2)
    hh, hl, lh, ll = higher_highs_lower_lows(candles, 5)

    tmp_p_path = os.path.join(FEATURES_BASE_DIR, f"{symbol.lower()}.tmp_p")
    with open(tmp_p_path, "w") as out:
        header = [
            "symbol", "timeframe", "timestamp", "open", "high", "low", "close", "volume",
            "realized_vol", "parkinson_vol", "yang_zhang_vol", "atr",
            "roc_5", "roc_10", "slope_10",
            "hour", "minute", "weekday", "candlestick_patterns",
            "swing_high", "swing_low",
            "higher_high", "higher_low", "lower_high", "lower_low"
        ]
        out.write("\t".join(header) + "\n")

        for i in range(n):
            row = [
                symbol.upper(), "1m", str(candles[i]['timestamp']),
                str(candles[i]['open']), str(candles[i]['high']), str(candles[i]['low']),
                str(candles[i]['close']), str(candles[i]['volume']),
                str(rvol[i]) if rvol[i] is not None else "",
                str(pvol[i]) if pvol[i] is not None else "",
                str(yzvol[i]) if yzvol[i] is not None else "",
                str(atr_vals[i]) if atr_vals[i] is not None else "",
                str(roc5[i]) if roc5[i] is not None else "",
                str(roc10[i]) if roc10[i] is not None else "",
                str(slope10[i]) if slope10[i] is not None else "",
                str(hours[i]), str(minutes[i]), str(weekdays[i]),
                patterns[i],
                str(swing_high[i]) if swing_high[i] is not None else "",
                str(swing_low[i]) if swing_low[i] is not None else "",
                "1" if hh[i] else "0", "1" if hl[i] else "0",
                "1" if lh[i] else "0", "1" if ll[i] else "0"
            ]
            out.write("\t".join(row) + "\n")

    elapsed = time.time() - start_time
    log_issue("INFO", f"Processing complete for {symbol} in {elapsed:.2f}s -> {tmp_p_path}")
    return True

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python P01_volatility_price_action.py SYMBOL")
        sys.exit(1)
    success = process_and_save(sys.argv[1].upper())
    sys.exit(0 if success else 1)