#!/usr/bin/env python3
"""
P11_market_structure.py – Process Raw Market Structure Data
- Reads {symbol}_mstructure.tmp_x (TSV from X21)
- Parses 15m, 1h, 4h, 1d candles
- Computes swing points, S/R levels, FVGs, supply/demand zones,
  order blocks, fakeouts, pivot zones, trend score, BOS/CHoCH
- Outputs TSV {symbol}_mstructure.tmp_p with raw data (commented) + derived features
- Logs to p11_market_structure_issues.log (file only, minimal console)
- Input file is NOT deleted.
"""

import os
import sys
import time
import math
from collections import defaultdict

# ========== CONFIGURATION ==========
FEATURES_BASE_DIR = os.path.join("market_data", "binance", "symbols")
LOG_FILE = os.path.join(FEATURES_BASE_DIR, "p11_market_structure_issues.log")
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
    # Print only errors on console (INFO and others go only to file)
    if level == "ERROR":
        print(line)
    rotate_log_if_needed()
    with open(LOG_FILE, "a") as f:
        f.write(line + "\n")

# ========== HELPER FUNCTIONS ==========
def find_swing_points(candles, lookback=2):
    highs = []
    lows = []
    n = len(candles)
    for i in range(lookback, n - lookback):
        is_high = all(candles[i]['high'] > candles[i-j]['high'] for j in range(1, lookback+1)) and \
                  all(candles[i]['high'] > candles[i+j]['high'] for j in range(1, lookback+1))
        if is_high:
            vol = candles[i]['volume'] + (candles[i-1]['volume'] if i>0 else 0) + (candles[i+1]['volume'] if i+1<n else 0)
            highs.append({"timestamp": candles[i]['timestamp'], "price": candles[i]['high'], "volume": vol, "type": "swing_high"})
        is_low = all(candles[i]['low'] < candles[i-j]['low'] for j in range(1, lookback+1)) and \
                 all(candles[i]['low'] < candles[i+j]['low'] for j in range(1, lookback+1))
        if is_low:
            vol = candles[i]['volume'] + (candles[i-1]['volume'] if i>0 else 0) + (candles[i+1]['volume'] if i+1<n else 0)
            lows.append({"timestamp": candles[i]['timestamp'], "price": candles[i]['low'], "volume": vol, "type": "swing_low"})
    return highs, lows

def find_sr_levels(candles, tolerance=0.002):
    highs, lows = find_swing_points(candles, lookback=2)
    all_points = highs + lows
    if not all_points:
        return []
    groups = defaultdict(list)
    for p in all_points:
        price = p['price']
        found = False
        for key in list(groups.keys()):
            if abs(price - key) / key <= tolerance:
                groups[key].append(p)
                found = True
                break
        if not found:
            groups[price].append(p)
    levels = []
    for price, touches in groups.items():
        if len(touches) >= 2:
            # Determine type from the FIRST swing point's type
            first_type = touches[0].get('type', 'swing_high')
            level_type = "resistance" if first_type == "swing_high" else "support"
            levels.append({
                "price": round(price, 2),
                "touches": len(touches),
                "type": level_type
            })
    levels.sort(key=lambda x: (-x['touches'], x['price']))
    return levels[:10]

def find_fvg(candles):
    fvgs = []
    for i in range(2, len(candles)):
        if candles[i-2]['high'] < candles[i]['low']:
            fvgs.append({
                "type": "bullish",
                "timestamp": candles[i]['timestamp'],
                "gap_top": candles[i]['low'],
                "gap_bottom": candles[i-2]['high'],
                "status": "untouched"
            })
        elif candles[i-2]['low'] > candles[i]['high']:
            fvgs.append({
                "type": "bearish",
                "timestamp": candles[i]['timestamp'],
                "gap_top": candles[i-2]['low'],
                "gap_bottom": candles[i]['high'],
                "status": "untouched"
            })
    return fvgs[-20:]

def find_supply_demand_zones(candles, lookback=3, zone_width=0.005):
    zones = []
    avg_vol = sum(c['volume'] for c in candles) / len(candles) if candles else 1
    for i in range(lookback, len(candles)-lookback):
        if candles[i]['close'] > candles[i-1]['close'] * 1.005:
            zone_high = candles[i-1]['high']
            zone_low = candles[i-1]['low']
            range_zone = (zone_high - zone_low) * zone_width
            strength = ((candles[i]['close'] - candles[i-1]['close']) / candles[i-1]['close']) * min(3.0, candles[i]['volume'] / avg_vol)
            revisited = any(candles[j]['low'] <= zone_high and candles[j]['high'] >= zone_low
                            for j in range(i+1, min(i+50, len(candles))))
            status = "tested" if revisited else "fresh"
            zones.append({
                "type": "demand",
                "start_time": candles[i-1]['timestamp'],
                "high": zone_high + range_zone,
                "low": zone_low - range_zone,
                "strength": round(strength, 6),
                "status": status
            })
        elif candles[i-1]['close'] > candles[i]['close'] * 1.005:
            zone_high = candles[i-1]['high']
            zone_low = candles[i-1]['low']
            range_zone = (zone_high - zone_low) * zone_width
            strength = ((candles[i-1]['close'] - candles[i]['close']) / candles[i]['close']) * min(3.0, candles[i-1]['volume'] / avg_vol)
            revisited = any(candles[j]['low'] <= zone_high and candles[j]['high'] >= zone_low
                            for j in range(i+1, min(i+50, len(candles))))
            status = "tested" if revisited else "fresh"
            zones.append({
                "type": "supply",
                "start_time": candles[i-1]['timestamp'],
                "high": zone_high + range_zone,
                "low": zone_low - range_zone,
                "strength": round(strength, 6),
                "status": status
            })
    zones.sort(key=lambda x: x['strength'], reverse=True)
    return zones[:20]

def find_order_blocks(candles, lookback=3):
    blocks = []
    for i in range(lookback, len(candles)-lookback):
        if candles[i-1]['close'] < candles[i-1]['open'] and candles[i]['close'] > candles[i]['open'] * 1.005:
            blocks.append({
                "type": "bullish",
                "timestamp": candles[i-1]['timestamp'],
                "high": candles[i-1]['high'],
                "low": candles[i-1]['low'],
                "strength": (candles[i]['close'] - candles[i-1]['close']) / candles[i-1]['close']
            })
        elif candles[i-1]['close'] > candles[i-1]['open'] and candles[i]['close'] < candles[i]['open'] * 0.995:
            blocks.append({
                "type": "bearish",
                "timestamp": candles[i-1]['timestamp'],
                "high": candles[i-1]['high'],
                "low": candles[i-1]['low'],
                "strength": (candles[i-1]['close'] - candles[i]['close']) / candles[i]['close']
            })
    blocks.sort(key=lambda x: x['strength'], reverse=True)
    return blocks[:20]

def detect_fakeouts(candles_15m, candles_1h):
    fakeouts = []
    swing_highs, swing_lows = find_swing_points(candles_15m, lookback=2)
    all_swings = swing_highs + swing_lows
    for i in range(5, len(candles_15m)-5):
        for s in all_swings:
            if abs(s['timestamp'] - candles_15m[i]['timestamp']) < 5*15*60*1000:
                continue
            if s['type'] == 'swing_high' and candles_15m[i]['high'] > s['price'] and candles_15m[i]['close'] < s['price']:
                rejection = candles_15m[i]['high'] - s['price']
                fakeouts.append({
                    "type": "fakeout_high",
                    "timestamp": candles_15m[i]['timestamp'],
                    "level": s['price'],
                    "rejection": rejection,
                    "target": s['price'] - rejection * 0.618
                })
            elif s['type'] == 'swing_low' and candles_15m[i]['low'] < s['price'] and candles_15m[i]['close'] > s['price']:
                rejection = s['price'] - candles_15m[i]['low']
                fakeouts.append({
                    "type": "fakeout_low",
                    "timestamp": candles_15m[i]['timestamp'],
                    "level": s['price'],
                    "rejection": rejection,
                    "target": s['price'] + rejection * 0.618
                })
    return fakeouts[-20:]

def structure_trend_score(candles_4h):
    if len(candles_4h) < 10:
        return 0.0
    highs, lows = find_swing_points(candles_4h, lookback=2)
    if not highs or not lows:
        return 0.0
    last_highs = highs[-2:] if len(highs) >= 2 else []
    last_lows = lows[-2:] if len(lows) >= 2 else []
    bull = sum(1 for i in range(1, len(last_highs)) if last_highs[i]['price'] > last_highs[i-1]['price'])
    bear = sum(1 for i in range(1, len(last_lows)) if last_lows[i]['price'] < last_lows[i-1]['price'])
    latest = candles_4h[-1]['close']
    if highs and latest > highs[-1]['price']:
        bull += 1
    if lows and latest < lows[-1]['price']:
        bear += 1
    score = (bull - bear) / 2.0
    return max(-1.0, min(1.0, score))

def detect_bos_choch(candles_4h):
    highs, lows = find_swing_points(candles_4h, lookback=2)
    if len(highs) < 2 or len(lows) < 2:
        return "none", "none"
    last_high = highs[-1]['price']
    prev_high = highs[-2]['price']
    last_low = lows[-1]['price']
    prev_low = lows[-2]['price']
    close = candles_4h[-1]['close']
    if close > last_high:
        bos = "bullish"
        choch = "bullish_reversal" if last_high <= prev_high else "none"
    elif close < last_low:
        bos = "bearish"
        choch = "bearish_reversal" if last_low >= prev_low else "none"
    else:
        bos = "none"
        choch = "none"
    return bos, choch

def get_pivot_zones(candles_daily, all_candles):
    if len(candles_daily) < 2:
        return {}
    prev_day = candles_daily[-2]
    week_high = max(c['high'] for c in candles_daily[-7:]) if len(candles_daily) >= 7 else max(c['high'] for c in candles_daily)
    week_low = min(c['low'] for c in candles_daily[-7:]) if len(candles_daily) >= 7 else min(c['low'] for c in candles_daily)
    month_high = max(c['high'] for c in candles_daily[-30:]) if len(candles_daily) >= 30 else max(c['high'] for c in candles_daily)
    month_low = min(c['low'] for c in candles_daily[-30:]) if len(candles_daily) >= 30 else min(c['low'] for c in candles_daily)
    ath = max(c['high'] for c in all_candles)
    atl = min(c['low'] for c in all_candles)
    return {
        "prev_day_high": prev_day['high'],
        "prev_day_low": prev_day['low'],
        "prev_week_high": week_high,
        "prev_week_low": week_low,
        "prev_month_high": month_high,
        "prev_month_low": month_low,
        "ath": ath,
        "atl": atl
    }

# ========== MAIN PROCESSING ==========
def process_market_structure(symbol):
    print(f"[P11] Starting market structure processing for {symbol}")
    start_time = time.time()
    log_issue("INFO", f"Starting market structure processing for {symbol}")

    tmp_x_path = os.path.join(FEATURES_BASE_DIR, f"{symbol.lower()}_mstructure.tmp_x")
    if not os.path.exists(tmp_x_path):
        log_issue("ERROR", f"Input file not found: {tmp_x_path}")
        return False

    # ---------- Read raw data and store raw lines ----------
    raw_lines = []
    candles = {"15m": [], "1h": [], "4h": [], "1d": []}
    with open(tmp_x_path, "r") as f:
        header = f.readline().strip()
        raw_lines.append(header)  # header line
        for line in f:
            line = line.strip()
            if not line:
                continue
            raw_lines.append(line)
            parts = line.split('\t')
            if len(parts) < 7:
                continue
            tf = parts[0]
            ts = int(parts[1])
            o = float(parts[2])
            h = float(parts[3])
            l = float(parts[4])
            c = float(parts[5])
            v = float(parts[6])
            if tf in candles:
                candles[tf].append({"timestamp": ts, "open": o, "high": h, "low": l, "close": c, "volume": v})

    # Sort candles
    for tf in candles:
        candles[tf].sort(key=lambda x: x['timestamp'])

    if not candles["15m"] or not candles["1h"]:
        log_issue("ERROR", "Missing 15m or 1h candles")
        return False

    current_price = candles["1d"][-1]['close'] if candles["1d"] else candles["15m"][-1]['close']

    # Compute features
    swings_all = {}
    for tf in ["15m", "1h", "4h"]:
        highs, lows = find_swing_points(candles[tf], lookback=2)
        combined = highs + lows
        combined.sort(key=lambda x: x['timestamp'])
        swings_all[tf] = combined[-6:] if len(combined) > 6 else combined

    sr_levels = find_sr_levels(candles["1h"], tolerance=0.002)
    fvgs = find_fvg(candles["15m"])[-10:]
    sd_zones = find_supply_demand_zones(candles["1h"], lookback=3)[:6]
    order_blocks = find_order_blocks(candles["1h"])[:6]
    fakeouts = detect_fakeouts(candles["15m"], candles["1h"])[-10:]

    all_candles = candles["15m"] + candles["1h"] + candles["4h"] + candles["1d"]
    pivot_zones = get_pivot_zones(candles["1d"], all_candles)

    trend_score = structure_trend_score(candles["4h"]) if candles["4h"] else 0.0
    bos, choch = detect_bos_choch(candles["4h"]) if candles["4h"] else ("none", "none")

    fakeout_signal = None
    if fakeouts:
        latest = fakeouts[0]
        if latest['type'] == 'fakeout_low':
            fakeout_signal = {'direction': 'up', 'probability': min(80, 50 + int(latest['rejection']*10)), 'target': latest['target']}
        else:
            fakeout_signal = {'direction': 'down', 'probability': min(80, 50 + int(latest['rejection']*10)), 'target': latest['target']}

    fresh_demand = None
    fresh_supply = None
    for z in sd_zones:
        if z['status'] != 'fresh':
            continue
        zone_mid = (z['high'] + z['low']) / 2
        if z['type'] == 'demand' and zone_mid < current_price:
            if fresh_demand is None or zone_mid > fresh_demand['mid']:
                fresh_demand = {'low': z['low'], 'high': z['high'], 'mid': zone_mid, 'strength': z['strength']}
        elif z['type'] == 'supply' and zone_mid > current_price:
            if fresh_supply is None or zone_mid < fresh_supply['mid']:
                fresh_supply = {'low': z['low'], 'high': z['high'], 'mid': zone_mid, 'strength': z['strength']}

    pivot_proximity = None
    min_dist = float('inf')
    for name, price in pivot_zones.items():
        if price <= 0:
            continue
        dist_pct = abs((price - current_price) / current_price) * 100
        if dist_pct < min_dist and dist_pct < 2.0:
            min_dist = dist_pct
            pivot_proximity = {'name': name, 'price': price, 'distance_pct': round(dist_pct, 2)}

    overall = trend_score * 50
    if bos == 'bullish':
        overall += 15
    elif bos == 'bearish':
        overall -= 15
    if choch == 'bullish_reversal':
        overall += 25
    elif choch == 'bearish_reversal':
        overall -= 25
    if fakeout_signal:
        if fakeout_signal['direction'] == 'up':
            overall += 20
        else:
            overall -= 20
    overall = max(-100, min(100, overall))

    if overall >= 40:
        bias = "bullish"
        confidence = min(95, 55 + overall // 2)
    elif overall <= -40:
        bias = "bearish"
        confidence = min(95, 55 + abs(overall) // 2)
    else:
        bias = "neutral"
        confidence = 50 + abs(overall) // 2

    high_prob_scenario = None
    if confidence >= 90:
        if bias == "bullish":
            high_prob_scenario = "UP"
        elif bias == "bearish":
            high_prob_scenario = "DOWN"

    # ---------- Write output .tmp_p TSV (raw data + derived) ----------
    tmp_p_path = os.path.join(FEATURES_BASE_DIR, f"{symbol.lower()}_mstructure.tmp_p")
    with open(tmp_p_path, "w") as out:
        out.write("# === Raw market structure data ===\n")
        for raw_line in raw_lines:
            out.write("# " + raw_line + "\n")
        out.write("# === Derived features ===\n")
        header = [
            "timestamp", "current_price", "trend_score", "bos", "choch",
            "overall_structure_score", "bias", "confidence", "high_prob_scenario",
            "fakeout_signal_direction", "fakeout_signal_prob", "fakeout_target"
        ]
        out.write("\t".join(header) + "\n")
        ts = int(time.time() * 1000)
        row = [
            str(ts),
            f"{current_price:.2f}",
            f"{trend_score:.2f}",
            bos,
            choch,
            str(overall),
            bias,
            str(confidence),
            high_prob_scenario if high_prob_scenario else "",
            fakeout_signal['direction'] if fakeout_signal else "",
            str(fakeout_signal['probability']) if fakeout_signal else "",
            f"{fakeout_signal['target']:.2f}" if fakeout_signal else ""
        ]
        out.write("\t".join(row) + "\n")

    elapsed = time.time() - start_time
    print(f"[P11] Success ({elapsed:.1f}s) -> {os.path.basename(tmp_p_path)}")
    log_issue("INFO", f"Processing complete for {symbol} in {elapsed:.2f}s -> {tmp_p_path}")
    return True

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python P11_market_structure.py SYMBOL")
        sys.exit(1)
    success = process_market_structure(sys.argv[1].upper())
    sys.exit(0 if success else 1)