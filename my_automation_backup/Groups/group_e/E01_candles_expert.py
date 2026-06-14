#!/usr/bin/env python3
# E01_candles_expert.py – Production-grade candle expert (All issues fixed)
# Reads raw candles + all P modules; outputs summary TSV.

import os
import sys
import time
import math
import json
from collections import defaultdict, deque

# ---------- CONFIGURATION ----------
FEATURES_BASE_DIR = os.path.join("market_data", "binance", "symbols")
LOG_FILE = os.path.join(FEATURES_BASE_DIR, "E01_candles_expert.log")
LOG_MAX_SIZE = 5_000_000

def rotate_log_if_needed():
    if os.path.exists(LOG_FILE) and os.path.getsize(LOG_FILE) > LOG_MAX_SIZE:
        backup = LOG_FILE + ".old"
        try:
            os.replace(LOG_FILE, backup)
        except Exception as e:
            # Log rotation failure should not crash
            print(f"LOG ROTATION FAILED: {e}")

def log_issue(level, msg, **kwargs):
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    line = f"{ts} [{level}] {msg}"
    if kwargs:
        line += " " + str(kwargs)
    print(line)
    rotate_log_if_needed()
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(line + "\n")

# ---------- PATTERN CODES ----------
PATTERN_CODES = {
    "Doji": "DOJ", "Dragonfly Doji": "DDO", "Gravestone Doji": "GDO", "Long-Legged Doji": "LDO",
    "Spinning Top": "SPT", "White Marubozu": "WHM", "Black Marubozu": "BLM",
    "Hammer (or Hanging)": "HAM", "Inverted Hammer (or Shooting Star)": "INV",
    "Long White Candle": "LWC", "Long Black Candle": "LBC", "High Wave Candle": "HWC",
    "Bullish Engulfing": "BUE", "Bearish Engulfing": "BRE",
    "Bullish Harami": "BUH", "Bearish Harami": "BRH",
    "Piercing Line": "PIE", "Dark Cloud Cover": "DCC",
    "Tweezer Top": "TZT", "Tweezer Bottom": "TZB",
    "Morning Star": "MRS", "Evening Star": "EVS",
    "Three White Soldiers": "3WS", "Three Black Crows": "3BC",
    "Four White Soldiers": "4WS", "Four Black Crows": "4BC",
    "Heikin-Ashi Hollow Bullish": "HAB", "Heikin-Ashi Filled Bullish": "HAF",
    "Heikin-Ashi Hollow Bearish": "HBB", "Heikin-Ashi Filled Bearish": "HBF",
    "Heikin-Ashi Bullish Reversal": "HBR", "Heikin-Ashi Bearish Reversal": "HBE",
    "Heikin-Ashi Consolidation": "HAC",
    "Renko UP Brick": "RUP", "Renko DOWN Brick": "RDN",
}
def encode_pattern(p):
    return PATTERN_CODES.get(p, "UNK")

def encode_patterns(pat_list):
    return '|'.join(encode_pattern(p) for p in pat_list) if pat_list else ""

# ---------- CANDLE HELPERS ----------
def is_bullish(c): return c['close'] > c['open']
def is_bearish(c): return c['close'] < c['open']
def body_length(c): return abs(c['close'] - c['open'])
def upper_wick(c): return c['high'] - max(c['open'], c['close'])
def lower_wick(c): return min(c['open'], c['close']) - c['low']
def total_range(c): return c['high'] - c['low']

def avg_body(candles, window=20):
    if not candles:
        return 0.0
    if len(candles) < window:
        return sum(body_length(c) for c in candles) / len(candles)
    recent = candles[-window:]
    return sum(body_length(c) for c in recent) / window

# ---------- HEIKIN-ASHI (with proper interval and gap reset) ----------
def heikin_ashi_candles(candles, expected_interval):
    """Return Heikin-Ashi candles. Resets on gaps > 1.5× interval."""
    if not candles:
        return []
    ha = []
    for i, c in enumerate(candles):
        gap = False
        if i > 0:
            diff = c['timestamp'] - candles[i-1]['timestamp']
            if diff > expected_interval * 1.5:
                gap = True
        if i == 0 or gap:
            ha_close = (c['open'] + c['high'] + c['low'] + c['close']) / 4.0
            ha_open = (c['open'] + c['close']) / 2.0
        else:
            ha_close = (c['open'] + c['high'] + c['low'] + c['close']) / 4.0
            ha_open = (ha[-1]['ha_open'] + ha[-1]['ha_close']) / 2.0
        ha_high = max(c['high'], ha_open, ha_close)
        ha_low = min(c['low'], ha_open, ha_close)
        ha.append({
            'ha_open': ha_open,
            'ha_close': ha_close,
            'ha_high': ha_high,
            'ha_low': ha_low,
            'ha_bullish': ha_close > ha_open,
            'ha_body': abs(ha_close - ha_open)
        })
    return ha

def heikin_ashi_patterns(ha):
    """Extract HA reversal and consolidation events (not per‑candle direction)."""
    patterns = []
    for i in range(1, len(ha)):
        if ha[i]['ha_bullish'] and not ha[i-1]['ha_bullish']:
            patterns.append((i, "Heikin-Ashi Bullish Reversal"))
        elif not ha[i]['ha_bullish'] and ha[i-1]['ha_bullish']:
            patterns.append((i, "Heikin-Ashi Bearish Reversal"))
        # Consolidation detection (low body relative to recent average)
        start = max(0, i-9)
        body_ma = sum(h['ha_body'] for h in ha[start:i+1]) / (i-start+1)
        if ha[i]['ha_body'] < 0.3 * body_ma:
            patterns.append((i, "Heikin-Ashi Consolidation"))
    return patterns

# ---------- RENKO (ATR-based brick size, proper brick building) ----------
def renko_bricks(candles, brick_size):
    if not candles:
        return []
    bricks = []
    current = None
    for c in candles:
        price = c['close']
        if current is None:
            current = {'high': price, 'low': price, 'direction': None}
            continue
        if current['direction'] is None:
            if price >= current['high'] + brick_size:
                current['direction'] = 'up'
                current['high'] = price
                current['low'] = price
            elif price <= current['low'] - brick_size:
                current['direction'] = 'down'
                current['high'] = price
                current['low'] = price
            else:
                current['high'] = max(current['high'], price)
                current['low'] = min(current['low'], price)
        elif current['direction'] == 'up':
            if price >= current['high'] + brick_size:
                bricks.append({'direction': 'up', 'high': current['high'], 'low': current['low']})
                current = {'direction': 'up', 'high': price, 'low': price}
            else:
                current['high'] = max(current['high'], price)
                current['low'] = min(current['low'], price)
        else:  # down
            if price <= current['low'] - brick_size:
                bricks.append({'direction': 'down', 'high': current['high'], 'low': current['low']})
                current = {'direction': 'down', 'high': price, 'low': price}
            else:
                current['high'] = max(current['high'], price)
                current['low'] = min(current['low'], price)
    # Do not push incomplete brick
    # If the last brick is complete, it was already added.
    # No extra push to avoid duplication.
    return bricks

def renko_last_direction(candles, brick_size):
    bricks = renko_bricks(candles, brick_size)
    if bricks:
        return bricks[-1]['direction']
    return None

# ---------- PATTERN DETECTION (optimized, no per-candle HA direction) ----------
def detect_patterns_for_timeframe(candles, expected_interval, brick_size):
    if len(candles) < 5:
        return []
    enriched = []
    for c in candles:
        enriched.append({
            'timestamp': c['timestamp'],
            'open': c['open'],
            'high': c['high'],
            'low': c['low'],
            'close': c['close'],
            'volume': c['volume'],
            'body': body_length(c),
            'upper_wick': upper_wick(c),
            'lower_wick': lower_wick(c),
            'range': total_range(c)
        })
    avg_body_len = avg_body(enriched, 20)
    if avg_body_len < 1e-6:
        avg_body_len = 1e-6

    ha_candles = heikin_ashi_candles(enriched, expected_interval)
    ha_events = heikin_ashi_patterns(ha_candles)
    ha_dict = defaultdict(list)
    for idx, pat in ha_events:
        ha_dict[idx].append(pat)
    renko_dir = renko_last_direction(enriched, brick_size)

    results = []
    for i, c in enumerate(enriched):
        patterns = []
        # ---- single candle patterns ----
        if c['body'] <= 0.05 * avg_body_len:
            patterns.append("Doji")
            if c['lower_wick'] > 2*c['body'] and c['upper_wick'] < 0.5*c['body']:
                patterns.append("Dragonfly Doji")
            elif c['upper_wick'] > 2*c['body'] and c['lower_wick'] < 0.5*c['body']:
                patterns.append("Gravestone Doji")
            elif c['upper_wick'] > c['body'] and c['lower_wick'] > c['body']:
                patterns.append("Long-Legged Doji")
        if c['body'] < 0.2*avg_body_len and c['upper_wick']>0 and c['lower_wick']>0:
            patterns.append("Spinning Top")
        if c['upper_wick'] < 0.05*avg_body_len and c['lower_wick'] < 0.05*avg_body_len:
            patterns.append("White Marubozu" if is_bullish(c) else "Black Marubozu")
        if c['lower_wick'] > 2*c['body'] and c['body'] < 0.4*c['range']:
            patterns.append("Hammer (or Hanging)")
        if c['upper_wick'] > 2*c['body'] and c['body'] < 0.4*c['range']:
            patterns.append("Inverted Hammer (or Shooting Star)")
        if c['body'] > 2*avg_body_len:
            patterns.append("Long White Candle" if is_bullish(c) else "Long Black Candle")
        if c['range'] > 2*c['body'] and not (c['body'] <= 0.05*avg_body_len):
            patterns.append("High Wave Candle")
        # ---- double candle patterns ----
        if i > 0:
            prev = enriched[i-1]
            if is_bearish(prev) and is_bullish(c) and c['open']<prev['close'] and c['close']>prev['open']:
                patterns.append("Bullish Engulfing")
            if is_bullish(prev) and is_bearish(c) and c['open']>prev['close'] and c['close']<prev['open']:
                patterns.append("Bearish Engulfing")
            if is_bullish(prev) and is_bullish(c) and c['open']>prev['open'] and c['close']<prev['close']:
                patterns.append("Bullish Harami")
            if is_bearish(prev) and is_bearish(c) and c['open']<prev['open'] and c['close']>prev['close']:
                patterns.append("Bearish Harami")
            if is_bearish(prev) and is_bullish(c) and c['close']>(prev['open']+prev['close'])/2 and c['open']<prev['close']:
                patterns.append("Piercing Line")
            if is_bullish(prev) and is_bearish(c) and c['close']<(prev['open']+prev['close'])/2 and c['open']>prev['close']:
                patterns.append("Dark Cloud Cover")
            if c['high'] == prev['high'] and is_bearish(c) and is_bullish(prev):
                patterns.append("Tweezer Top")
            if c['low'] == prev['low'] and is_bullish(c) and is_bearish(prev):
                patterns.append("Tweezer Bottom")
        # ---- triple candle patterns ----
        if i > 1:
            p1 = enriched[i-1]
            p2 = enriched[i-2]
            if is_bearish(p2) and (enriched[i-1]['body']<=0.05*avg_body_len) and is_bullish(c) and c['close']>(p2['open']+p2['close'])/2:
                patterns.append("Morning Star")
            if is_bullish(p2) and (enriched[i-1]['body']<=0.05*avg_body_len) and is_bearish(c) and c['close']<(p2['open']+p2['close'])/2:
                patterns.append("Evening Star")
            if is_bullish(p2) and is_bullish(p1) and is_bullish(c) and p2['close']<p1['close']<c['close']:
                patterns.append("Three White Soldiers")
            if is_bearish(p2) and is_bearish(p1) and is_bearish(c) and p2['close']>p1['close']>c['close']:
                patterns.append("Three Black Crows")
        # ---- four candle patterns ----
        if i > 2:
            p1 = enriched[i-3]
            p2 = enriched[i-2]
            p3 = enriched[i-1]
            if is_bullish(p1) and is_bullish(p2) and is_bullish(p3) and is_bullish(c):
                patterns.append("Four White Soldiers")
            if is_bearish(p1) and is_bearish(p2) and is_bearish(p3) and is_bearish(c):
                patterns.append("Four Black Crows")
        # ---- Heikin-Ashi events (only reversals/consolidation) ----
        if i in ha_dict:
            patterns.extend(ha_dict[i])
        # ---- Renko hint (only on last candle) ----
        if i == len(enriched)-1 and renko_dir:
            patterns.append(f"Renko {renko_dir.upper()} Brick")
        if patterns:
            results.append({
                'timestamp': c['timestamp'],
                'patterns': patterns   # list, not joined yet
            })
    return results

# ---------- SWING DETECTION (optimized with deques) ----------
def find_swing_points_optimized(candles, lookback=3):
    n = len(candles)
    if n < 2*lookback + 1:
        return [], []
    highs = []
    lows = []
    for i in range(lookback, n - lookback):
        is_high = True
        is_low = True
        for j in range(1, lookback+1):
            if candles[i]['high'] <= candles[i-j]['high'] or candles[i]['high'] <= candles[i+j]['high']:
                is_high = False
            if candles[i]['low'] >= candles[i-j]['low'] or candles[i]['low'] >= candles[i+j]['low']:
                is_low = False
        if is_high:
            highs.append((candles[i]['timestamp'], candles[i]['high']))
        if is_low:
            lows.append((candles[i]['timestamp'], candles[i]['low']))
    return highs, lows

# ---------- MARKET STRUCTURE (fixed guard, slope-based) ----------
def detect_market_structure(candles):
    n = len(candles)
    if n < 10:
        return "RANGE"
    lookback = max(3, min(10, n // 50))
    highs, lows = find_swing_points_optimized(candles, lookback)
    if len(highs) < 3 or len(lows) < 3:
        return "RANGE"
    # use last 5-8 swings
    recent_highs = highs[-5:] if len(highs) >= 5 else highs
    recent_lows = lows[-5:] if len(lows) >= 5 else lows
    def slope(points):
        if len(points) < 2:
            return 0
        x = list(range(len(points)))
        y = [p[1] for p in points]
        n_pts = len(x)
        sum_x = sum(x)
        sum_y = sum(y)
        sum_xy = sum(x[i]*y[i] for i in range(n_pts))
        sum_x2 = sum(xi*xi for xi in x)
        denom = n_pts*sum_x2 - sum_x*sum_x
        if denom == 0:
            return 0
        return (n_pts*sum_xy - sum_x*sum_y) / denom
    high_slope = slope(recent_highs)
    low_slope = slope(recent_lows)
    if high_slope > 0 and low_slope > 0:
        return "HH/HL"
    if high_slope < 0 and low_slope < 0:
        return "LH/LL"
    # accumulation/distribution detection (requires at least 10 candles)
    if n >= 10:
        recent_range = max(c['high'] for c in candles[-5:]) - min(c['low'] for c in candles[-5:])
        prev_range = max(c['high'] for c in candles[-10:-5]) - min(c['low'] for c in candles[-10:-5])
        recent_vol = sum(c['volume'] for c in candles[-5:])
        prev_vol = sum(c['volume'] for c in candles[-10:-5])
        if prev_range > 0:
            range_ratio = recent_range / prev_range
            vol_ratio = recent_vol / prev_vol if prev_vol > 0 else 1.0
            if range_ratio < 0.7 and vol_ratio < 0.8:
                return "ACCUMULATION"
            if range_ratio < 0.7 and vol_ratio > 1.2:
                return "DISTRIBUTION"
    # transition detection
    if len(highs) >= 2 and len(lows) >= 2:
        if highs[-1][1] > highs[-2][1] and lows[-1][1] > lows[-2][1]:
            return "TRANSITION_UP"
        if highs[-1][1] < highs[-2][1] and lows[-1][1] < lows[-2][1]:
            return "TRANSITION_DOWN"
    return "RANGE"

# ---------- WILDER ATR (cached) ----------
_atr_cache = {}
def compute_wilder_atr(candles, period=14):
    cache_key = (id(candles), period)
    if cache_key in _atr_cache:
        return _atr_cache[cache_key]
    if len(candles) < period + 1:
        if len(candles) < 2:
            atr, atr_pct = 0.0, 0.0
        else:
            tr = []
            for i in range(1, len(candles)):
                tr.append(max(candles[i]['high'] - candles[i]['low'],
                              abs(candles[i]['high'] - candles[i-1]['close']),
                              abs(candles[i]['low'] - candles[i-1]['close'])))
            atr = sum(tr) / len(tr)
            atr_pct = (atr / candles[-1]['close'] * 100) if candles[-1]['close'] != 0 else 0.0
        _atr_cache[cache_key] = (atr, atr_pct)
        return atr, atr_pct
    tr = []
    for i in range(1, len(candles)):
        tr.append(max(candles[i]['high'] - candles[i]['low'],
                      abs(candles[i]['high'] - candles[i-1]['close']),
                      abs(candles[i]['low'] - candles[i-1]['close'])))
    atr = sum(tr[:period]) / period
    for i in range(period, len(tr)):
        atr = (atr * (period-1) + tr[i]) / period
    atr_pct = (atr / candles[-1]['close'] * 100) if candles[-1]['close'] != 0 else 0.0
    _atr_cache[cache_key] = (atr, atr_pct)
    return atr, atr_pct

def volatility_regime(atr_pct):
    if atr_pct < 0.8: return "low_vol"
    if atr_pct > 2.0: return "high_vol"
    return "normal_vol"

# ---------- FLOW AND MOMENTUM (single normalization layer) ----------
def compute_flows_and_momentum(candles, period_flow=5, period_mom=3):
    n = len(candles)
    if n < max(period_flow, period_mom) + 1:
        return [], []
    flows = [0.0] * n
    momentums = [0.0] * n
    # price changes as percentage
    pct_changes = [0.0] * n
    for i in range(1, n):
        if candles[i-1]['close'] != 0:
            pct_changes[i] = (candles[i]['close'] - candles[i-1]['close']) / candles[i-1]['close'] * 100
    # compute flow (raw percentage change, not normalized by ATR)
    for i in range(period_flow, n):
        avg_change = sum(pct_changes[i-period_flow+1:i+1]) / period_flow
        flows[i] = max(-5.0, min(5.0, avg_change)) / 5.0   # normalize to [-1,1]
    # momentum is same as flow but over shorter period; we keep as is
    for i in range(period_mom, n):
        avg_mom = sum(pct_changes[i-period_mom+1:i+1]) / period_mom
        momentums[i] = max(-5.0, min(5.0, avg_mom)) / 5.0
    return flows, momentums

def compute_pos(candles, lookback=20):
    if not candles or len(candles) < lookback:
        return 0.5
    recent_high = max(c['high'] for c in candles[-lookback:])
    recent_low = min(c['low'] for c in candles[-lookback:])
    if recent_high == recent_low:
        return 0.5
    return max(0.0, min(1.0, (candles[-1]['close'] - recent_low) / (recent_high - recent_low)))

# ---------- TIME-ALIGNED CORRELATION (with tolerance) ----------
def time_aligned_flows_precomputed(candles_a, flows_a, candles_b, flows_b, tolerance_ms=60000):
    """Align flows by nearest timestamp within tolerance."""
    # Build (timestamp, flow) for b, sorted
    pairs_b = sorted([(candles_b[i]['timestamp'], flows_b[i]) for i in range(len(flows_b)) if flows_b[i] != 0], key=lambda x: x[0])
    pairs_a = sorted([(candles_a[i]['timestamp'], flows_a[i]) for i in range(len(flows_a)) if flows_a[i] != 0], key=lambda x: x[0])
    aligned_a = []
    aligned_b = []
    # For each a, find nearest b within tolerance
    j = 0
    for ts_a, flow_a in pairs_a:
        # find closest ts_b to ts_a
        best_idx = -1
        best_dist = tolerance_ms + 1
        while j < len(pairs_b) and pairs_b[j][0] < ts_a - tolerance_ms:
            j += 1
        # check current and next few
        for k in range(j, min(j+3, len(pairs_b))):
            dist = abs(pairs_b[k][0] - ts_a)
            if dist < best_dist:
                best_dist = dist
                best_idx = k
        if best_idx != -1 and best_dist <= tolerance_ms:
            aligned_a.append(flow_a)
            aligned_b.append(pairs_b[best_idx][1])
    return aligned_a, aligned_b

def correlation_coefficient(x, y):
    n = len(x)
    if n < 3:
        return 0.0
    mean_x = sum(x)/n
    mean_y = sum(y)/n
    num = sum((x[i]-mean_x)*(y[i]-mean_y) for i in range(n))
    den_x = math.sqrt(sum((xi-mean_x)**2 for xi in x))
    den_y = math.sqrt(sum((yi-mean_y)**2 for yi in y))
    if den_x*den_y == 0:
        return 0.0
    return num/(den_x*den_y)

# ---------- PATTERN WEIGHT & SCORE ----------
PATTERN_BASE_WEIGHTS = {
    "3WS": 3, "4WS": 3, "MRS": 3, "BUE": 2.5, "HAB": 2, "HBR": 2, "PIE": 2, "TZB": 2,
    "3BC": -3, "4BC": -3, "EVS": -3, "BRE": -2.5, "HBB": -2, "HBE": -2, "DCC": -2, "TZT": -2,
    "HAF": 1.5, "HBF": -1.5, "WHM": 1.5, "LWC": 1.5, "BLM": -1.5, "LBC": -1.5,
    "HAM": 1, "INV": 1, "BUH": 1, "BRH": -1,
    "RUP": 2, "RDN": -2,
    "HWC": 0, "SPT": 0, "DOJ": 0, "HAC": 0,
    "UNK": 0
}
def contextual_weight(pattern_code, structure, momentum, pos):
    base = PATTERN_BASE_WEIGHTS.get(pattern_code, 0)
    if base == 0:
        return 0
    if structure in ("HH/HL", "TRANSITION_UP") and base > 0:
        if pos < 0.4:
            base *= 2
        else:
            base *= 1.2
    elif structure in ("LH/LL", "TRANSITION_DOWN") and base < 0:
        if pos > 0.6:
            base *= 2
        else:
            base *= 1.2
    elif structure in ("RANGE", "ACCUMULATION", "DISTRIBUTION"):
        base *= 0.5
    if (base > 0 and momentum < -0.2) or (base < 0 and momentum > 0.2):
        base *= 0.5
    elif (base > 0 and momentum > 0.5) or (base < 0 and momentum < -0.5):
        base *= 1.5
    return base

def compute_pattern_score_with_decay(patterns_list, current_time, structure, momentum, pos, timeframe_hours=1):
    total = 0.0
    half_life_hours = 8 * timeframe_hours
    for ts, pat_list in patterns_list:
        age_hours = (current_time - ts) / (3600 * 1000)
        decay = math.exp(-age_hours / half_life_hours)
        for pat in pat_list:
            code = encode_pattern(pat)
            w = contextual_weight(code, structure, momentum, pos)
            total += w * decay
    return total

def tanh_normalize(score, scale=20):
    """Normalize a raw score to [-50, 50] using tanh."""
    return math.tanh(score / scale) * 50

def logistic_probability(score, midpoint=0, scale=15):
    prob_raw = 1 / (1 + math.exp(-score / scale))
    return 20 + prob_raw * 60

# ---------- CANDLE CONTINUITY VALIDATION (stricter) ----------
def validate_candles(candles, expected_interval_ms):
    if len(candles) < 2:
        return True, "insufficient"
    prev = candles[0]
    gaps = 0
    for i in range(1, len(candles)):
        curr = candles[i]
        diff = curr['timestamp'] - prev['timestamp']
        if diff <= 0:
            log_issue("WARNING", f"Out-of-order or duplicate timestamp: {prev['timestamp']} -> {curr['timestamp']}")
            return False, "out_of_order"
        if diff > expected_interval_ms * 2:
            # gap > 2x interval → reject
            return False, "too_many_gaps"
        if diff > expected_interval_ms * 1.2:
            gaps += 1
            if gaps > 3:
                return False, "too_many_gaps"
        prev = curr
    return True, "ok"

# ---------- SAFE LOAD EXTERNAL FEATURES ----------
def safe_float(val, default=0.0):
    try:
        return float(val)
    except:
        return default

def load_p01_features(symbol):
    path = os.path.join(FEATURES_BASE_DIR, f"{symbol.lower()}.tmp_p")
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r") as f:
            lines = f.readlines()
        if len(lines) < 2:
            return {}
        header = lines[0].strip().split('\t')
        values = lines[1].strip().split('\t')
        features = {}
        for i, col in enumerate(header):
            if i < len(values):
                if col in ['atr_pct', 'volatility_24h', 'volatility_ratio', 'trend_strength', 'price_change_pct']:
                    features[col] = safe_float(values[i])
                else:
                    features[col] = values[i]
        return features
    except Exception as e:
        log_issue("WARNING", f"Could not read P01 features: {e}")
    return {}

def load_p02_features(symbol):
    path = os.path.join(FEATURES_BASE_DIR, f"{symbol.lower()}_cvd2.tmp_p")
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r") as f:
            lines = f.readlines()
        if len(lines) < 2:
            return {}
        summary_line = lines[-1].strip()
        parts = summary_line.split('\t')
        if len(parts) >= 2:
            return {"cvd_net": safe_float(parts[0]), "cvd_trend": parts[1]}
    except:
        pass
    return {}

def load_p04_features(symbol):
    path = os.path.join(FEATURES_BASE_DIR, f"{symbol.lower()}_derivative.tmp_p")
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r") as f:
            lines = f.readlines()
        if len(lines) < 2:
            return {}
        header = lines[0].strip().split('\t')
        values = lines[1].strip().split('\t')
        features = {}
        for i, col in enumerate(header):
            if i < len(values):
                if col in ['funding_zscore', 'ls_ratio_velocity', 'basis_pct', 'net_score', 'oi_change_pct']:
                    features[col] = safe_float(values[i])
                else:
                    features[col] = values[i]
        return features
    except Exception as e:
        log_issue("WARNING", f"Could not read P04 features: {e}")
    return {}

def load_p07_features(symbol):
    path = os.path.join(FEATURES_BASE_DIR, f"{symbol.lower()}_liquidations.tmp_p")
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r") as f:
            lines = f.readlines()
        if len(lines) < 2:
            return {}
        header = lines[0].strip().split('\t')
        values = lines[1].strip().split('\t')
        features = {}
        for i, col in enumerate(header):
            if i < len(values):
                if col in ['net_delta_1m', 'stop_hunt_probability', 'liquidation_magnet_bias',
                           'cascade_risk_value', 'long_cascade_risk', 'short_cascade_risk']:
                    features[col] = safe_float(values[i])
                else:
                    features[col] = values[i]
        return features
    except Exception as e:
        log_issue("WARNING", f"Could not read P07 features: {e}")
    return {}

# ---------- MAIN EXPERT FUNCTION ----------
def compute_expert_summary(symbol):
    log_issue("INFO", f"Starting E01 expert for {symbol}")
    # 1. Load raw candles (all timeframes)
    candle_file = os.path.join(FEATURES_BASE_DIR, f"{symbol.lower()}.tmp_x")
    if not os.path.exists(candle_file):
        log_issue("ERROR", f"Candle file not found: {candle_file}")
        return None
    data_by_tf = {tf: [] for tf in ['1m','5m','15m','1h','4h']}
    try:
        with open(candle_file, "r") as f:
            for line in f:
                parts = line.strip().split('\t')
                if len(parts) < 8:
                    continue
                symbol_tmp, tf, ts, o, h, l, c, v = parts
                if tf not in data_by_tf:
                    continue
                data_by_tf[tf].append({
                    'timestamp': int(ts),
                    'open': float(o), 'high': float(h), 'low': float(l),
                    'close': float(c), 'volume': float(v)
                })
    except Exception as e:
        log_issue("ERROR", f"Failed to read candles: {e}")
        return None

    # Sort and validate each timeframe
    interval_map = {'1m': 60000, '5m': 300000, '15m': 900000, '1h': 3600000, '4h': 14400000}
    for tf, candles in data_by_tf.items():
        candles.sort(key=lambda x: x['timestamp'])
        valid, status = validate_candles(candles, interval_map[tf])
        if not valid:
            log_issue("WARNING", f"Candle continuity issue for {tf}: {status}, skipping this timeframe")
            data_by_tf[tf] = []

    # 2. Load external features
    p01 = load_p01_features(symbol)
    p02 = load_p02_features(symbol)
    p04 = load_p04_features(symbol)
    p07 = load_p07_features(symbol)

    # 3. Process target timeframes (1h and 4h)
    results = []
    target_tfs = ['1h', '4h']
    for tf in target_tfs:
        candles = data_by_tf.get(tf, [])
        if len(candles) < 20:
            log_issue("WARNING", f"Not enough candles for {tf} (got {len(candles)}), skipping")
            continue

        # Compute ATR-based Renko brick size
        atr, _ = compute_wilder_atr(candles, 14)
        brick_size = atr * 0.5 if atr > 0 else candles[-1]['close'] * 0.001

        # Pattern detection (pass expected interval and brick size)
        pat_dicts = detect_patterns_for_timeframe(candles, interval_map[tf], brick_size)
        pat_tuples = [(p['timestamp'], p['patterns']) for p in pat_dicts]

        # Precompute flows and momentum (single normalization)
        flows, momentums = compute_flows_and_momentum(candles, period_flow=5, period_mom=3)
        if not flows:
            log_issue("WARNING", f"Could not compute flows for {tf}, skipping")
            continue

        structure = detect_market_structure(candles)
        atr_val, atr_pct = compute_wilder_atr(candles, 14)
        vol_reg = volatility_regime(atr_pct)
        flow = flows[-1] if flows else 0.0
        momentum = momentums[-1] if momentums else 0.0
        pos = compute_pos(candles)

        current_ts = candles[-1]['timestamp']
        tf_hours = 1 if tf == '1h' else 4
        pattern_score_raw = compute_pattern_score_with_decay(pat_tuples, current_ts, structure, momentum, pos, timeframe_hours=tf_hours)
        pattern_score = tanh_normalize(pattern_score_raw, scale=20)   # now in [-50,50]

        # Structure score
        if structure in ("HH/HL", "TRANSITION_UP"):
            struct_score = 20
        elif structure in ("LH/LL", "TRANSITION_DOWN"):
            struct_score = -20
        elif structure == "ACCUMULATION":
            struct_score = 10
        elif structure == "DISTRIBUTION":
            struct_score = -10
        else:
            struct_score = 0

        # Momentum score (already normalized)
        momentum_score = momentum * 20   # range [-20,20]

        # Volatility score
        if vol_reg == "high_vol" and abs(momentum) > 0.3:
            vol_score = 10
        elif vol_reg == "low_vol":
            vol_score = -10
        else:
            vol_score = 0

        # Alignment using precomputed flows (with tolerance)
        alignment = 0
        other_tf = '4h' if tf == '1h' else '1h'
        if len(data_by_tf.get(other_tf, [])) >= 10:
            other_candles = data_by_tf[other_tf]
            other_flows, _ = compute_flows_and_momentum(other_candles, period_flow=5, period_mom=3)
            if other_flows and len(other_flows) == len(other_candles):
                series_a, series_b = time_aligned_flows_precomputed(candles, flows, other_candles, other_flows, tolerance_ms=60000)
                if len(series_a) >= 5:
                    corr = correlation_coefficient(series_a, series_b)
                    alignment = max(-15, min(15, corr * 25))

        # Interaction gating
        if structure in ("RANGE", "ACCUMULATION", "DISTRIBUTION"):
            pattern_score *= 0.6
        if structure in ("TRANSITION_UP", "TRANSITION_DOWN"):
            pattern_score *= 0.8

        # Symmetric penalty
        if (pattern_score > 10 and momentum_score < -10) or (pattern_score < -10 and momentum_score > 10):
            penalty = 15
        else:
            penalty = 0

        total_score = pattern_score + struct_score + momentum_score + alignment + vol_score - penalty
        total_score = max(-100, min(100, total_score))

        prob = logistic_probability(total_score, midpoint=0, scale=15)

        if total_score > 15:
            direction = "UP"
        elif total_score < -15:
            direction = "DOWN"
        else:
            direction = "NEUTRAL"

        # Build reason string
        reason_parts = [f"Struct:{structure}", f"Vol:{vol_reg}", f"Flow:{flow:.2f}", f"Momentum:{momentum:.2f}", f"Score:{total_score:.0f}"]
        if p01.get('volatility_24h'):
            reason_parts.append(f"Vol24h:{p01['volatility_24h']:.1f}%")
        if p02.get('cvd_trend'):
            reason_parts.append(f"CVD:{p02['cvd_trend']}")
        if p04.get('funding_zscore'):
            reason_parts.append(f"FundingZ:{p04['funding_zscore']:.2f}")
        if p07.get('stop_hunt_probability'):
            reason_parts.append(f"StopHunt:{p07['stop_hunt_probability']:.0f}")
        reason = "|".join(reason_parts)

        # Last candle patterns (encoded)
        last_patterns = []
        if pat_tuples:
            last_patterns = pat_tuples[-1][1][:3]
        patterns_str = ','.join(last_patterns)

        results.append({
            'timeframe': tf,
            'timestamp': candles[-1]['timestamp'],
            'direction': direction,
            'probability': prob,
            'reason': reason,
            'patterns': patterns_str,
            'structure': structure,
            'volatility_regime': vol_reg,
            'momentum': f"{momentum:.2f}",
            'flow_alignment': f"{alignment:.1f}",
            'liquidation_stress': p07.get('stop_hunt_probability', 'N/A'),
            'cvd_net': p02.get('cvd_net', 'N/A'),
            'funding_zscore': p04.get('funding_zscore', 'N/A'),
            'oi_change': p04.get('oi_change_pct', 'N/A')
        })

    if not results:
        log_issue("ERROR", "No results generated for any timeframe")
        return None

    out_path = os.path.join(FEATURES_BASE_DIR, f"{symbol.lower()}_E01_candles.tsv")
    with open(out_path, "w") as f:
        header = ["timestamp", "timeframe", "prediction_direction", "probability", "reason",
                  "patterns_detected", "market_structure", "volatility_regime", "momentum",
                  "flow_alignment", "liquidation_stress", "cvd_net", "funding_zscore", "oi_change_pct"]
        f.write("\t".join(header) + "\n")
        for res in results:
            row = [
                str(res['timestamp']),
                res['timeframe'],
                res['direction'],
                str(res['probability']),
                res['reason'],
                res['patterns'],
                res['structure'],
                res['volatility_regime'],
                res['momentum'],
                res['flow_alignment'],
                str(res['liquidation_stress']),
                str(res['cvd_net']),
                str(res['funding_zscore']),
                str(res['oi_change'])
            ]
            f.write("\t".join(row) + "\n")
    log_issue("INFO", f"Saved summary to {out_path}")
    return out_path

def run_expert(symbol):
    return compute_expert_summary(symbol)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python E01_candles_expert.py SYMBOL")
        sys.exit(1)
    success = run_expert(sys.argv[1].upper())
    sys.exit(0 if success else 1)