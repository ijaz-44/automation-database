#!/usr/bin/env python3
# E11_volProfile_expert.py – Volume Profile High‑Probability Scenario Detector (≥90% setups)
# Reads X19 volProfile1.tmp_x and volProfile2.tmp_x, reconstructs candles, computes volume profile metrics,
# and outputs TSV summary.

import os
import sys
import time
import math
import json
from collections import defaultdict, Counter

# ========================== CONFIGURATION ==========================
FEATURES_BASE_DIR = os.path.join("market_data", "binance", "symbols")
LOG_FILE = os.path.join(FEATURES_BASE_DIR, "E11_volProfile_expert.log")
LOG_MAX_SIZE = 5_000_000

MULT = 100_000_000  # multiplier used in compression

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

# ========================== ORIGINAL analyze_volume_profile (UNCHANGED) ==========================
def analyze_volume_profile(data):
    """
    Args:
        data: dict with keys:
            - last_profile: dict with 'poc', 'vah', 'val', 'shape' (string), 'hvns' (list), 'lvns' (list)
            - daily_profiles: list of dicts (last 7 days) for `poc`, `vah`, `val`, `shape`
            - intraday_profiles: dict with keys '1h', '4h' each containing 'poc', 'vah', 'val', 'shape'
            - developing_poc: float (vPOC) – optional
            - developing_vah_val: dict with 'vvah', 'vval' – optional
            - current_price: float
    Returns:
        dict with:
            'bias' (bullish/bearish/neutral),
            'confidence' (int 0‑100),
            'high_prob_scenario' ('UP'/'DOWN'/None),
            'probability_estimate' (int),
            'reason' (str),
            'signals' (list),
            'net_score' (int)
    """
    signals = []
    bullish_score = 0
    bearish_score = 0

    last = data.get('last_profile', {})
    shape = last.get('shape', 'D-shape')
    poc = last.get('poc', 0)
    vah = last.get('vah', 0)
    val = last.get('val', 0)
    hvns = last.get('hvns', [])
    lvns = last.get('lvns', [])
    current_price = data.get('current_price', poc)
    intra_1h = data.get('intraday_profiles', {}).get('1h', {})
    intra_4h = data.get('intraday_profiles', {}).get('4h', {})
    vpoc = data.get('developing_poc', None)
    vvah = data.get('developing_vah_val', {}).get('vvah', None)
    vval = data.get('developing_vah_val', {}).get('vval', None)

    if shape == 'P-shape':
        bullish_score += 30
        signals.append("P‑shape (value area high) → bullish bias")
    elif shape == 'b-shape':
        bearish_score += 30
        signals.append("b‑shape (value area low) → bearish bias")
    else:
        signals.append("D‑shape – neutral, inside value area")

    if vah and val and current_price > 0:
        if current_price > vah:
            bullish_score += 15
            signals.append(f"Price above VAH ({vah:.2f}) → breakout bullish")
        elif current_price < val:
            bearish_score += 15
            signals.append(f"Price below VAL ({val:.2f}) → breakdown bearish")
        else:
            signals.append(f"Price inside value area ({val:.2f} – {vah:.2f}) → neutral")

    if hvns and current_price > 0:
        hvns_above = [h for h in hvns if h > current_price]
        hvns_below = [h for h in hvns if h < current_price]
        if hvns_above:
            bearish_score += 10
            signals.append(f"HVN resistance above at {min(hvns_above):.2f}")
        if hvns_below:
            bullish_score += 10
            signals.append(f"HVN support below at {max(hvns_below):.2f}")
    if lvns and current_price > 0:
        lvns_above = [l for l in lvns if l > current_price]
        lvns_below = [l for l in lvns if l < current_price]
        if lvns_above:
            bullish_score += 5
            signals.append(f"LVN above at {min(lvns_above):.2f} → potential target")
        if lvns_below:
            bearish_score += 5
            signals.append(f"LVN below at {max(lvns_below):.2f} → potential target")

    daily_shape = data.get('daily_profiles', [{}])[-1].get('shape', '') if data.get('daily_profiles') else ''
    shape_4h = intra_4h.get('shape', '')
    shape_1h = intra_1h.get('shape', '')
    if daily_shape and shape_4h and shape_1h:
        if daily_shape == 'P-shape' and shape_4h == 'P-shape' and shape_1h == 'P-shape':
            bullish_score += 25
            signals.append("All timeframes P‑shape → strong bullish confluence")
        elif daily_shape == 'b-shape' and shape_4h == 'b-shape' and shape_1h == 'b-shape':
            bearish_score += 25
            signals.append("All timeframes b‑shape → strong bearish confluence")
        else:
            signals.append("Mixed timeframes – lower conviction")

    if vpoc and vah and val:
        if vpoc > vah:
            bullish_score += 15
            signals.append(f"Developing POC ({vpoc:.2f}) above VAH → bullish momentum")
        elif vpoc < val:
            bearish_score += 15
            signals.append(f"Developing POC ({vpoc:.2f}) below VAL → bearish momentum")
        else:
            signals.append(f"Developing POC inside value area")

    if vvah and vval and current_price:
        if current_price > vvah:
            bullish_score += 10
            signals.append("Price above developing VAH → current session breakout")
        elif current_price < vval:
            bearish_score += 10
            signals.append("Price below developing VAL → current session breakdown")

    daily_poc = data.get('last_profile', {}).get('poc', 0)
    if vpoc and daily_poc:
        diff_pct = (vpoc - daily_poc) / daily_poc * 100 if daily_poc else 0
        if diff_pct > 1:
            bullish_score += 10
            signals.append(f"Developing POC {diff_pct:.1f}% above daily POC → bullish drift")
        elif diff_pct < -1:
            bearish_score += 10
            signals.append(f"Developing POC {abs(diff_pct):.1f}% below daily POC → bearish drift")

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

    reason = f"Net score {net:+d}, dominant signals: {signals[0] if signals else 'volume profile neutral'}"

    return {
        "bias": bias,
        "confidence": confidence,
        "high_prob_scenario": high_prob,
        "probability_estimate": confidence,
        "reason": reason,
        "signals": signals,
        "net_score": net
    }

# ========================== VOLUME PROFILE COMPUTATION ==========================
def compute_volume_profile(candles, bucket_size=0.1):
    """
    Candles: list of dict with 'price' (close) and 'volume'. For profile we typically use close price.
    Actually better to use typical price (H+L+C)/3 or just close. We'll use close.
    Returns dict with poc, vah, val, shape, hvns, lvns.
    """
    if not candles:
        return {}
    # Create price buckets
    prices = [c['close'] for c in candles]
    min_price = min(prices)
    max_price = max(prices)
    # round to nearest bucket_size
    low_bound = math.floor(min_price / bucket_size) * bucket_size
    high_bound = math.ceil(max_price / bucket_size) * bucket_size
    buckets = []
    bucket_centers = []
    cur = low_bound
    while cur <= high_bound:
        bucket_centers.append(cur)
        cur += bucket_size
    bucket_vol = {center: 0.0 for center in bucket_centers}
    for c in candles:
        price = c['close']
        # find nearest bucket center
        bucket = round(price / bucket_size) * bucket_size
        bucket_vol[bucket] += c['volume']
    # find POC (highest volume bucket)
    poc_bucket = max(bucket_vol.items(), key=lambda x: x[1])[0]
    poc = poc_bucket
    # total volume
    total_vol = sum(bucket_vol.values())
    # value area: 70% of total volume, symmetrical around poc
    target_vol = total_vol * 0.7
    sorted_buckets = sorted(bucket_vol.keys())
    # find index of poc
    poc_idx = sorted_buckets.index(poc_bucket)
    vol_so_far = bucket_vol[poc_bucket]
    lower_idx = poc_idx
    upper_idx = poc_idx
    while vol_so_far < target_vol:
        added = False
        if lower_idx > 0:
            lower_idx -= 1
            vol_so_far += bucket_vol[sorted_buckets[lower_idx]]
            added = True
        if upper_idx < len(sorted_buckets)-1 and vol_so_far < target_vol:
            upper_idx += 1
            vol_so_far += bucket_vol[sorted_buckets[upper_idx]]
            added = True
        if not added:
            break
    val = sorted_buckets[lower_idx]
    vah = sorted_buckets[upper_idx]
    # shape: P-shape if (vah - poc) > (poc - val) else b-shape if opposite else D-shape
    diff_up = vah - poc
    diff_down = poc - val
    if diff_up > diff_down * 1.2:
        shape = "P-shape"
    elif diff_down > diff_up * 1.2:
        shape = "b-shape"
    else:
        shape = "D-shape"
    # HVN/LVN: compare each bucket volume to average volume of all buckets
    avg_vol = total_vol / len(bucket_vol)
    hvns = [price for price, vol in bucket_vol.items() if vol > avg_vol * 1.5]
    lvns = [price for price, vol in bucket_vol.items() if vol < avg_vol * 0.5]
    return {
        "poc": poc,
        "vah": vah,
        "val": val,
        "shape": shape,
        "hvns": hvns,
        "lvns": lvns
    }

# ========================== DECODE X19 FILES ==========================
def decode_compressed_1m(rows):
    """Decode list of compressed row strings into list of candle dicts."""
    candles = []
    for row in rows:
        parts = row.split('|')
        # first part: absolute values
        abs_parts = parts[0].split(',')
        t0 = int(abs_parts[0]) * 1000
        o0 = int(abs_parts[1]) / MULT
        h0 = int(abs_parts[2]) / MULT
        l0 = int(abs_parts[3]) / MULT
        c0 = int(abs_parts[4]) / MULT
        v0 = int(abs_parts[5]) / MULT
        candles.append({
            'timestamp': t0,
            'open': o0,
            'high': h0,
            'low': l0,
            'close': c0,
            'volume': v0
        })
        prev_t, prev_o, prev_h, prev_l, prev_c, prev_v = t0, o0, h0, l0, c0, v0
        for delta_str in parts[1:]:
            d_parts = delta_str.split(',')
            dt = int(d_parts[0])
            do = int(d_parts[1])
            dh = int(d_parts[2])
            dl = int(d_parts[3])
            dc = int(d_parts[4])
            dv = int(d_parts[5])
            t = prev_t + dt * 1000
            o = prev_o + do / MULT
            h = prev_h + dh / MULT
            l = prev_l + dl / MULT
            c = prev_c + dc / MULT
            v = prev_v + dv / MULT
            candles.append({
                'timestamp': t,
                'open': o,
                'high': h,
                'low': l,
                'close': c,
                'volume': v
            })
            prev_t, prev_o, prev_h, prev_l, prev_c, prev_v = t, o, h, l, c, v
    return candles

def load_volprofile_data(symbol):
    """Read both volProfile files, reconstruct all candles."""
    daily_candles = []
    oneh_candles = []
    fourh_candles = []
    one_minute_candles = []
    # Read file1
    path1 = os.path.join(FEATURES_BASE_DIR, f"{symbol.lower()}_volProfile1.tmp_x")
    if os.path.exists(path1):
        with open(path1, "r") as f:
            lines = f.readlines()
        i = 0
        while i < len(lines):
            line = lines[i].strip()
            if not line:
                i += 1
                continue
            if line.startswith('#'):
                if line.startswith('#1m_compressed_start'):
                    # start collecting compressed rows
                    i += 1
                    compressed_rows = []
                    while i < len(lines) and not lines[i].strip().startswith('#1m_compressed_end'):
                        row_line = lines[i].strip()
                        if row_line and not row_line.startswith('#'):
                            # format: "1m_comp\tidx\trow"
                            parts = row_line.split('\t')
                            if len(parts) >= 3:
                                compressed_rows.append(parts[2])
                        i += 1
                    # decode
                    one_minute_candles.extend(decode_compressed_1m(compressed_rows))
                i += 1
                continue
            # plain candle line: format: "type\ttimestamp\topen\thigh\tlow\tclose\tvolume"
            parts = line.split('\t')
            if len(parts) >= 7:
                typ = parts[0]
                ts = int(parts[1])
                o = float(parts[2])
                h = float(parts[3])
                l = float(parts[4])
                c = float(parts[5])
                v = float(parts[6])
                candle = {'timestamp': ts, 'open': o, 'high': h, 'low': l, 'close': c, 'volume': v}
                if typ == 'daily':
                    daily_candles.append(candle)
                elif typ == '1h':
                    oneh_candles.append(candle)
                elif typ == '4h':
                    fourh_candles.append(candle)
            i += 1
    # Read file2 (only compressed 1m)
    path2 = os.path.join(FEATURES_BASE_DIR, f"{symbol.lower()}_volProfile2.tmp_x")
    if os.path.exists(path2):
        with open(path2, "r") as f:
            lines = f.readlines()
        i = 0
        while i < len(lines):
            line = lines[i].strip()
            if not line:
                i += 1
                continue
            if line.startswith('#1m_compressed_start'):
                i += 1
                compressed_rows = []
                while i < len(lines) and not lines[i].strip().startswith('#1m_compressed_end'):
                    row_line = lines[i].strip()
                    if row_line and not row_line.startswith('#'):
                        parts = row_line.split('\t')
                        if len(parts) >= 3:
                            compressed_rows.append(parts[2])
                    i += 1
                one_minute_candles.extend(decode_compressed_1m(compressed_rows))
            i += 1
    return daily_candles, oneh_candles, fourh_candles, one_minute_candles

def group_by_day(candles):
    """Group candles by day (UTC) and return dict day_start_ts -> list"""
    groups = defaultdict(list)
    for c in candles:
        # get start of day (00:00 UTC)
        day_start = (c['timestamp'] // 86400000) * 86400000
        groups[day_start].append(c)
    return groups

def compute_daily_profiles(day_groups, latest_ts=None):
    """Compute volume profile for each day, return list sorted by date (most recent first)."""
    profiles = []
    for day_ts in sorted(day_groups.keys(), reverse=True):
        candles = day_groups[day_ts]
        if not candles:
            continue
        # Use closing prices for profile
        profile = compute_volume_profile(candles, bucket_size=0.5)  # 0.5 dollar buckets for BTC
        if profile:
            profiles.append(profile)
    return profiles[:7]  # last 7 days

def compute_intraday_profile(candles, bucket_size=0.5):
    """Compute volume profile for a set of candles (e.g., last few hours)."""
    if not candles:
        return {}
    return compute_volume_profile(candles, bucket_size)

def get_current_price(one_minute_candles):
    if not one_minute_candles:
        return 0.0
    return one_minute_candles[-1]['close']

def run_expert(symbol):
    log_issue("INFO", f"Starting E11 volume profile expert for {symbol}")
    daily, oneh, fourh, one_min = load_volprofile_data(symbol)
    if not one_min:
        log_issue("ERROR", "No 1m candles found")
        return None
    # Group 1m candles by day
    day_groups = group_by_day(one_min)
    daily_profiles = compute_daily_profiles(day_groups)
    # Last profile (yesterday's or today's? Use the most recent complete day)
    last_profile = daily_profiles[0] if daily_profiles else {}
    # Intraday profiles: from 1h and 4h candles (or aggregate from 1m)
    # Use 1h candles if available, otherwise aggregate from 1m
    if oneh:
        # Aggregate 1h candles by hour
        oneh_by_hour = defaultdict(list)
        for c in oneh:
            hour_start = (c['timestamp'] // 3600000) * 3600000
            oneh_by_hour[hour_start].append(c)
        # For each hour, we need a profile of that hour? Actually intraday profile is usually for the entire 1h or 4h period.
        # We'll compute a single profile for all recent 1h candles (last 24 hours) and one for 4h candles.
        recent_1h = oneh[-24:] if len(oneh) >= 24 else oneh
        intra_1h = compute_intraday_profile(recent_1h, bucket_size=0.5)
        recent_4h = fourh[-6:] if len(fourh) >= 6 else fourh
        intra_4h = compute_intraday_profile(recent_4h, bucket_size=0.5)
    else:
        # Aggregate 1m candles into 1h and 4h intervals
        # For simplicity, we'll use the last 24 hours of 1m to create 1h buckets
        now_ms = int(time.time() * 1000)
        last_24h = [c for c in one_min if c['timestamp'] >= now_ms - 24*3600*1000]
        # Group by hour
        hour_groups = defaultdict(list)
        for c in last_24h:
            hour_start = (c['timestamp'] // 3600000) * 3600000
            hour_groups[hour_start].append(c)
        # Compute profile for each hour? Not needed. For intraday, we compute a single profile over all recent 1h periods? Actually intraday profile is typically for the current session's 1h and 4h blocks. We'll compute a profile over all 1h segments.
        intra_1h = compute_intraday_profile(last_24h, bucket_size=0.5)
        # For 4h, group into 4h blocks
        fourh_groups = defaultdict(list)
        for c in last_24h:
            fourh_start = (c['timestamp'] // (4*3600000)) * (4*3600000)
            fourh_groups[fourh_start].append(c)
        # compute profile over all 4h blocks (or just latest 4h)
        fourh_candles = []
        for f_start in sorted(fourh_groups.keys())[-2:]:
            fourh_candles.extend(fourh_groups[f_start])
        intra_4h = compute_intraday_profile(fourh_candles, bucket_size=0.5)
    # Developing profile: today's 1m candles (from start of day UTC)
    today_start = (int(time.time() * 1000) // 86400000) * 86400000
    today_candles = [c for c in one_min if c['timestamp'] >= today_start]
    dev_profile = compute_volume_profile(today_candles, bucket_size=0.5) if today_candles else {}
    developing_poc = dev_profile.get('poc', None)
    developing_vah_val = {'vvah': dev_profile.get('vah', None), 'vval': dev_profile.get('val', None)} if dev_profile else {}
    current_price = get_current_price(one_min)

    data = {
        "last_profile": last_profile,
        "daily_profiles": daily_profiles,
        "intraday_profiles": {"1h": intra_1h, "4h": intra_4h},
        "developing_poc": developing_poc,
        "developing_vah_val": developing_vah_val,
        "current_price": current_price
    }
    result = analyze_volume_profile(data)
    # Write output TSV
    out_path = os.path.join(FEATURES_BASE_DIR, f"{symbol.lower()}_E11_volProfile.tsv")
    with open(out_path, "w") as f:
        header = ["timestamp", "bias", "confidence", "high_prob_scenario", "probability_estimate",
                  "reason", "signals_json", "net_score"]
        f.write("\t".join(header) + "\n")
        ts_now = int(time.time() * 1000)
        row = [
            str(ts_now),
            result['bias'],
            str(result['confidence']),
            str(result['high_prob_scenario']) if result['high_prob_scenario'] else "",
            str(result['probability_estimate']),
            result['reason'],
            json.dumps(result['signals']),
            str(result['net_score'])
        ]
        f.write("\t".join(row) + "\n")
    log_issue("INFO", f"Saved volume profile expert summary to {out_path}")
    return out_path

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python E11_volProfile_expert.py SYMBOL")
        sys.exit(1)
    success = run_expert(sys.argv[1].upper())
    sys.exit(0 if success else 1)