#!/usr/bin/env python3
# E12_mstructure_expert.py – Market Structure High‑Probability Scenario Detector (No JSON)
# Reads raw market structure candles from X21 .tmp_x, computes metrics, outputs TSV summary.

import os
import sys
import time
import math
from collections import defaultdict

# ========================== CONFIGURATION ==========================
FEATURES_BASE_DIR = os.path.join("market_data", "binance", "symbols")
LOG_FILE = os.path.join(FEATURES_BASE_DIR, "E12_mstructure_expert.log")
LOG_MAX_SIZE = 5_000_000

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

# ========================== ORIGINAL analyze_market_structure (FIXED) ==========================
def analyze_market_structure(data):
    """
    Args:
        data: dict with keys:
            - trend_score: float (-1..1)
            - bos: str ('bullish', 'bearish', 'none')
            - choch: str ('bullish_reversal', 'bearish_reversal', 'none')
            - sr_levels: list of dicts with 'type','price','touches'
            - sd_zones: list of dicts with 'type','strength','status'
            - order_blocks: list of dicts with 'type','strength'
            - fakeouts: list of dicts with 'type','level','rejection','target'
            - pivot_zones: dict with 'prev_day_high','prev_day_low','prev_week_high','prev_week_low','ath','atl'
            - swings: optional dict with timeframes and swing points (not used heavily)
            - current_price: float (optional)
    Returns:
        dict with bias, confidence, high_prob_scenario, reason, signals, net_score.
    """
    signals = []
    bullish_score = 0
    bearish_score = 0

    trend_score = data.get('trend_score', 0.0)
    if trend_score > 0.5:
        bullish_score += 30
        signals.append(f"Strong bullish trend score ({trend_score:.2f})")
    elif trend_score > 0.2:
        bullish_score += 15
        signals.append(f"Moderate bullish trend ({trend_score:.2f})")
    elif trend_score < -0.5:
        bearish_score += 30
        signals.append(f"Strong bearish trend score ({trend_score:.2f})")
    elif trend_score < -0.2:
        bearish_score += 15
        signals.append(f"Moderate bearish trend ({trend_score:.2f})")
    else:
        signals.append(f"Neutral trend ({trend_score:.2f})")

    bos = data.get('bos', 'none')
    if bos == 'bullish':
        bullish_score += 20
        signals.append("Bullish BOS")
    elif bos == 'bearish':
        bearish_score += 20
        signals.append("Bearish BOS")

    choch = data.get('choch', 'none')
    if choch == 'bullish_reversal':
        bullish_score += 25
        signals.append("Bullish CHoCH – reversal")
    elif choch == 'bearish_reversal':
        bearish_score += 25
        signals.append("Bearish CHoCH – reversal")

    sd_zones = data.get('sd_zones', [])
    fresh_bullish = sum(1 for z in sd_zones if z['type'] == 'demand' and z['status'] == 'fresh')
    fresh_bearish = sum(1 for z in sd_zones if z['type'] == 'supply' and z['status'] == 'fresh')
    if fresh_bullish > 0:
        bullish_score += min(20, fresh_bullish * 8)
        signals.append(f"{fresh_bullish} fresh demand zones")
    if fresh_bearish > 0:
        bearish_score += min(20, fresh_bearish * 8)
        signals.append(f"{fresh_bearish} fresh supply zones")

    order_blocks = data.get('order_blocks', [])
    for ob in order_blocks:
        strength = ob.get('strength', 0)
        if ob['type'] == 'bullish':
            bullish_score += min(15, strength * 10)
        elif ob['type'] == 'bearish':
            bearish_score += min(15, strength * 10)
    if order_blocks:
        signals.append(f"Order blocks: {len(order_blocks)}")

    fakeouts = data.get('fakeouts', [])
    fakeout_up = sum(1 for fo in fakeouts if fo['type'] == 'fakeout_low')
    fakeout_down = sum(1 for fo in fakeouts if fo['type'] == 'fakeout_high')
    if fakeout_up > fakeout_down:
        bullish_score += min(20, 5 + fakeout_up * 5)
        signals.append(f"{fakeout_up} fakeout lows → bullish signal")
    elif fakeout_down > fakeout_up:
        bearish_score += min(20, 5 + fakeout_down * 5)
        signals.append(f"{fakeout_down} fakeout highs → bearish signal")

    current_price = data.get('current_price', 0)
    pivot_zones = data.get('pivot_zones', {})
    if current_price and pivot_zones:
        prev_high = pivot_zones.get('prev_day_high')
        prev_low = pivot_zones.get('prev_day_low')
        if prev_high and current_price > prev_high * 0.99:
            bearish_score += 10
            signals.append("Near previous day high → resistance")
        if prev_low and current_price < prev_low * 1.01:
            bullish_score += 10
            signals.append("Near previous day low → support")
        week_high = pivot_zones.get('prev_week_high')
        week_low = pivot_zones.get('prev_week_low')
        if week_high and current_price > week_high * 0.995:
            bearish_score += 15
            signals.append("Near weekly high → strong resistance")
        if week_low and current_price < week_low * 1.005:
            bullish_score += 15
            signals.append("Near weekly low → strong support")

    swings = data.get('swings', {})
    for tf, points in swings.items():
        if points:
            last = points[-1]
            if last['type'] == 'swing_high':
                bearish_score += 5
                signals.append(f"Last swing on {tf} is high")
            elif last['type'] == 'swing_low':
                bullish_score += 5
                signals.append(f"Last swing on {tf} is low")
            break

    net = bullish_score - bearish_score
    net = max(-100, min(100, net))

    if net >= 30:
        bias = "bullish"
        confidence = min(95, 60 + net // 2)
    elif net <= -30:
        bias = "bearish"
        confidence = min(95, 60 + abs(net) // 2)
    else:
        bias = "neutral"
        confidence = 50 + net // 2 if net else 50

    high_prob = None
    if confidence >= 90 and bias != "neutral":
        high_prob = "UP" if bias == "bullish" else "DOWN"

    # FIX: convert net to int for formatting
    reason = f"Net score {int(net):+d}, dominant: {signals[0] if signals else 'no clear structure'}"

    return {
        "bias": bias,
        "confidence": confidence,
        "high_prob_scenario": high_prob,
        "probability_estimate": confidence,
        "reason": reason,
        "signals": signals,
        "net_score": net
    }

# ========================== LOAD CANDLES FROM X21 ==========================
def load_candles(symbol):
    """Read raw market structure file and return dict of candles per timeframe."""
    path = os.path.join(FEATURES_BASE_DIR, f"{symbol.lower()}_mstructure.tmp_x")
    if not os.path.exists(path):
        log_issue("ERROR", f"Market structure file not found: {path}")
        return {}
    candles_by_tf = defaultdict(list)
    try:
        with open(path, "r") as f:
            # skip header
            header = f.readline()
            for line in f:
                parts = line.strip().split('\t')
                if len(parts) >= 7:
                    tf = parts[0]
                    ts = int(parts[1])
                    open_p = float(parts[2])
                    high = float(parts[3])
                    low = float(parts[4])
                    close = float(parts[5])
                    volume = float(parts[6])
                    candles_by_tf[tf].append({
                        'timestamp': ts,
                        'open': open_p,
                        'high': high,
                        'low': low,
                        'close': close,
                        'volume': volume
                    })
    except Exception as e:
        log_issue("ERROR", f"Failed to read market structure file: {e}")
        return {}
    # sort each timeframe
    for tf in candles_by_tf:
        candles_by_tf[tf].sort(key=lambda x: x['timestamp'])
    return candles_by_tf

# ========================== METRIC COMPUTATION ==========================
def compute_trend_score(candles, lookback=50):
    if len(candles) < lookback:
        return 0.0
    recent = candles[-lookback:]
    x = list(range(len(recent)))
    y = [c['close'] for c in recent]
    n = len(x)
    sum_x = sum(x)
    sum_y = sum(y)
    sum_xy = sum(x[i] * y[i] for i in range(n))
    sum_x2 = sum(xi * xi for xi in x)
    denom = n * sum_x2 - sum_x * sum_x
    if denom == 0:
        return 0.0
    slope = (n * sum_xy - sum_x * sum_y) / denom
    avg_price = sum(y) / n
    slope_pct = (slope / avg_price) * 100 if avg_price else 0
    return max(-1.0, min(1.0, slope_pct / 2.0))

def find_swing_points(candles, lookback=2):
    highs = []
    lows = []
    n = len(candles)
    for i in range(lookback, n - lookback):
        is_high = all(candles[i]['high'] > candles[i-j]['high'] for j in range(1, lookback+1)) and \
                  all(candles[i]['high'] > candles[i+j]['high'] for j in range(1, lookback+1))
        if is_high:
            highs.append((candles[i]['timestamp'], candles[i]['high']))
        is_low = all(candles[i]['low'] < candles[i-j]['low'] for j in range(1, lookback+1)) and \
                 all(candles[i]['low'] < candles[i+j]['low'] for j in range(1, lookback+1))
        if is_low:
            lows.append((candles[i]['timestamp'], candles[i]['low']))
    return highs, lows

def detect_bos(highs, lows, current_price, lookback=3):
    if len(highs) >= 2 and current_price > highs[-2][1]:
        return "bullish"
    if len(lows) >= 2 and current_price < lows[-2][1]:
        return "bearish"
    return "none"

def detect_choch(highs, lows, lookback=2):
    if len(highs) >= 3 and len(lows) >= 3:
        if highs[-1][1] > highs[-2][1] and lows[-1][1] > lows[-2][1]:
            return "bullish_reversal"
        elif highs[-1][1] < highs[-2][1] and lows[-1][1] < lows[-2][1]:
            return "bearish_reversal"
    return "none"

def find_sr_levels(highs, lows, tolerance_pct=0.002):
    levels = {}
    points = [(p, 'resistance') for _, p in highs] + [(p, 'support') for _, p in lows]
    for price, typ in points:
        found = False
        for lev in levels:
            if abs(lev - price) / price < tolerance_pct:
                levels[lev]['touches'] += 1
                found = True
                break
        if not found:
            levels[price] = {'type': typ, 'touches': 1, 'price': price}
    sr_list = [{'type': info['type'], 'price': round(lev, 2), 'touches': info['touches']} for lev, info in levels.items()]
    sr_list.sort(key=lambda x: x['touches'], reverse=True)
    return sr_list[:10]

def detect_supply_demand_zones(candles, lookback=5):
    zones = []
    for i in range(lookback, len(candles)-1):
        c = candles[i]
        body = abs(c['close'] - c['open'])
        range_total = c['high'] - c['low']
        if range_total == 0:
            continue
        upper_wick = c['high'] - max(c['open'], c['close'])
        if upper_wick > body * 2 and upper_wick > range_total * 0.5:
            zones.append({'type': 'supply', 'strength': min(1.0, upper_wick / (body+1)), 'status': 'fresh'})
        lower_wick = min(c['open'], c['close']) - c['low']
        if lower_wick > body * 2 and lower_wick > range_total * 0.5:
            zones.append({'type': 'demand', 'strength': min(1.0, lower_wick / (body+1)), 'status': 'fresh'})
    unique = {}
    for z in zones:
        key = z['type']
        if key not in unique or z['strength'] > unique[key]['strength']:
            unique[key] = z
    return list(unique.values())

def detect_order_blocks(candles, swing_highs, swing_lows):
    obs = []
    if swing_highs:
        last_swing_high = swing_highs[-1][1]
        for i in range(len(candles)-1, max(0, len(candles)-30), -1):
            if candles[i]['close'] > last_swing_high:
                obs.append({'type': 'bearish', 'strength': min(1.0, (candles[i]['close'] - last_swing_high) / last_swing_high * 10)})
                break
    if swing_lows:
        last_swing_low = swing_lows[-1][1]
        for i in range(len(candles)-1, max(0, len(candles)-30), -1):
            if candles[i]['close'] < last_swing_low:
                obs.append({'type': 'bullish', 'strength': min(1.0, (last_swing_low - candles[i]['close']) / last_swing_low * 10)})
                break
    return obs

def detect_fakeouts(candles, sr_levels, tolerance=0.001):
    fakeouts = []
    for lvl in sr_levels:
        level_price = lvl['price']
        for i in range(1, len(candles)):
            prev = candles[i-1]
            curr = candles[i]
            if level_price is None:
                continue
            if prev['close'] <= level_price < curr['close']:
                if i+1 < len(candles) and candles[i+1]['close'] < level_price:
                    fakeouts.append({'type': 'fakeout_high', 'level': level_price, 'rejection': True, 'target': level_price * 0.99})
            elif prev['close'] >= level_price > curr['close']:
                if i+1 < len(candles) and candles[i+1]['close'] > level_price:
                    fakeouts.append({'type': 'fakeout_low', 'level': level_price, 'rejection': True, 'target': level_price * 1.01})
    return fakeouts

def get_pivot_zones(daily_candles, weekly_candles):
    zones = {}
    if daily_candles and len(daily_candles) >= 2:
        zones['prev_day_high'] = daily_candles[-2]['high']
        zones['prev_day_low'] = daily_candles[-2]['low']
    if weekly_candles and len(weekly_candles) >= 2:
        zones['prev_week_high'] = weekly_candles[-2]['high']
        zones['prev_week_low'] = weekly_candles[-2]['low']
    return zones

# ========================== MAIN EXPERT FUNCTION ==========================
def run_expert(symbol):
    log_issue("INFO", f"Starting E12 market structure expert for {symbol}")
    candles_by_tf = load_candles(symbol)
    # If no candles, output neutral TSV
    if not candles_by_tf:
        log_issue("WARNING", "No candles loaded, creating neutral output")
        out_path = os.path.join(FEATURES_BASE_DIR, f"{symbol.lower()}_E12_mstructure.tsv")
        with open(out_path, "w") as f:
            header = ["timestamp", "bias", "confidence", "high_prob_scenario", "probability_estimate",
                      "reason", "signals", "net_score"]
            f.write("\t".join(header) + "\n")
            ts_now = int(time.time() * 1000)
            row = [str(ts_now), "neutral", "50", "", "50", "No market structure data", "", "0"]
            f.write("\t".join(row) + "\n")
        log_issue("INFO", f"Saved neutral market structure summary to {out_path}")
        return out_path

    # Prefer 1h candles, else 4h, else 15m
    primary_tf = '1h' if '1h' in candles_by_tf else '4h' if '4h' in candles_by_tf else '15m'
    candles = candles_by_tf.get(primary_tf, [])
    if len(candles) < 20:
        log_issue("WARNING", f"Not enough candles for timeframe {primary_tf} (got {len(candles)}), creating neutral output")
        out_path = os.path.join(FEATURES_BASE_DIR, f"{symbol.lower()}_E12_mstructure.tsv")
        with open(out_path, "w") as f:
            header = ["timestamp", "bias", "confidence", "high_prob_scenario", "probability_estimate",
                      "reason", "signals", "net_score"]
            f.write("\t".join(header) + "\n")
            ts_now = int(time.time() * 1000)
            row = [str(ts_now), "neutral", "50", "", "50", "Insufficient candle data", "", "0"]
            f.write("\t".join(row) + "\n")
        log_issue("INFO", f"Saved neutral market structure summary to {out_path}")
        return out_path

    # Daily candles for pivot zones
    daily_candles = candles_by_tf.get('1d', [])
    weekly_candles = [c for c in daily_candles if (c['timestamp'] // 86400000) % 7 == 0]

    # Compute metrics
    trend_score = compute_trend_score(candles, lookback=50)
    highs, lows = find_swing_points(candles, lookback=2)
    current_price = candles[-1]['close']
    bos = detect_bos(highs, lows, current_price)
    choch = detect_choch(highs, lows)
    sr_levels = find_sr_levels(highs, lows)
    sd_zones = detect_supply_demand_zones(candles)
    order_blocks = detect_order_blocks(candles, highs, lows)
    fakeouts = detect_fakeouts(candles, sr_levels)
    pivot_zones = get_pivot_zones(daily_candles, weekly_candles)

    # Swings per timeframe (optional)
    swings = {}
    for tf, c in candles_by_tf.items():
        h, l = find_swing_points(c, lookback=2)
        swings[tf] = [{'type': 'swing_high', 'price': h[-1][1], 'timestamp': h[-1][0]} for h in [h] if h] + \
                     [{'type': 'swing_low', 'price': l[-1][1], 'timestamp': l[-1][0]} for l in [l] if l]

    data = {
        "trend_score": trend_score,
        "bos": bos,
        "choch": choch,
        "sr_levels": sr_levels,
        "sd_zones": sd_zones,
        "order_blocks": order_blocks,
        "fakeouts": fakeouts,
        "pivot_zones": pivot_zones,
        "swings": swings,
        "current_price": current_price
    }

    result = analyze_market_structure(data)
    # Convert signals to pipe‑separated string
    signals_str = "|".join(result['signals']) if result['signals'] else ""

    out_path = os.path.join(FEATURES_BASE_DIR, f"{symbol.lower()}_E12_mstructure.tsv")
    with open(out_path, "w") as f:
        header = ["timestamp", "bias", "confidence", "high_prob_scenario", "probability_estimate",
                  "reason", "signals", "net_score"]
        f.write("\t".join(header) + "\n")
        ts_now = int(time.time() * 1000)
        row = [
            str(ts_now),
            result['bias'],
            str(result['confidence']),
            result['high_prob_scenario'] if result['high_prob_scenario'] else "",
            str(result['probability_estimate']),
            result['reason'],
            signals_str,
            str(result['net_score'])
        ]
        f.write("\t".join(row) + "\n")
    log_issue("INFO", f"Saved market structure expert summary to {out_path}")
    return out_path

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python E12_mstructure_expert.py SYMBOL")
        sys.exit(1)
    success = run_expert(sys.argv[1].upper())
    sys.exit(0 if success else 1)