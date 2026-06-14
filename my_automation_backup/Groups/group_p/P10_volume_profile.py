#!/usr/bin/env python3
"""
P10_volume_profile.py – Process Raw Volume Profile Data (two files)
- Reads {symbol}_volProfile1.tmp_x and {symbol}_volProfile2.tmp_x (TSV from X19)
- Handles both plain and compressed 1m candle rows.
- Combines 1m_full candles from both files
- Computes daily volume profiles, intraday profiles, developing POC/VAH/VAL, price action confluence
- Outputs:
    {symbol}_volProfile1.tmp_p → raw data from first file (commented)
    {symbol}_volProfile2.tmp_p → raw data from second file (commented) + derived features row
- Logs to p10_volume_profile_issues.log
- Input files are NOT deleted.
"""

import os
import sys
import time
import math
from collections import defaultdict
from datetime import datetime

FEATURES_BASE_DIR = os.path.join("market_data", "binance", "symbols")
LOG_FILE = os.path.join(FEATURES_BASE_DIR, "p10_volume_profile_issues.log")
LOG_MAX_SIZE = 5_000_000
TICK_SIZE = {"BTCUSDT": 0.5, "ETHUSDT": 0.1}
DEFAULT_TICK = 0.01

os.makedirs(FEATURES_BASE_DIR, exist_ok=True)

def log_issue(level, msg, **kwargs):
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    line = f"{ts} [{level}] {msg}"
    if kwargs:
        line += " " + str(kwargs)
    if level == "ERROR":
        print(line)
    with open(LOG_FILE, "a") as f:
        f.write(line + "\n")

def get_tick_size(symbol):
    s = symbol.upper()
    if s.startswith("BTC") or s.startswith("ETH"):
        return TICK_SIZE.get(s, 0.5) if s in TICK_SIZE else 0.5
    return DEFAULT_TICK

def compute_volume_profile(candles, tick_size, price_field='close', volume_field='volume'):
    if not candles:
        return None
    vol_profile = defaultdict(float)
    total_vol = 0.0
    for c in candles:
        price = c[price_field]
        binned = round(price / tick_size) * tick_size
        vol = c[volume_field]
        vol_profile[binned] += vol
        total_vol += vol
    if not vol_profile:
        return None
    poc = max(vol_profile.items(), key=lambda x: x[1])[0]
    target_vol = total_vol * 0.70
    sorted_levels = sorted(vol_profile.items(), key=lambda x: x[1], reverse=True)
    cum_vol = 0
    value_prices = []
    for price, v in sorted_levels:
        cum_vol += v
        value_prices.append(price)
        if cum_vol >= target_vol:
            break
    vah = max(value_prices)
    val = min(value_prices)
    all_nodes = sorted(vol_profile.items(), key=lambda x: x[1], reverse=True)
    hvns = [node[0] for node in all_nodes if node[0] != poc][:3]
    hvns += [0.0] * (3 - len(hvns))
    avg_vol = total_vol / len(vol_profile) if vol_profile else 1
    lvns = [p for p, v in vol_profile.items() if v < (avg_vol * 0.2)][:5]
    price_range = max(vol_profile.keys()) - min(vol_profile.keys())
    if price_range == 0:
        shape = "D-shape"
    else:
        pos = (poc - min(vol_profile.keys())) / price_range
        if pos >= 0.75:
            shape = "P-shape"
        elif pos <= 0.25:
            shape = "b-shape"
        else:
            shape = "D-shape"
    return {"poc": poc, "vah": vah, "val": val, "total_volume": total_vol,
            "hvns": hvns, "lvns": lvns, "shape": shape}

def has_rejection_wick(candles, level, direction='above'):
    for c in candles[-3:]:
        high = c['high']
        low = c['low']
        close = c['close']
        if direction == 'above':
            if high > level and close < level:
                total = high - low
                if total == 0:
                    continue
                if (high - max(close, c['open'])) / total > 0.6:
                    return True
        else:
            if low < level and close > level:
                total = high - low
                if total == 0:
                    continue
                if (min(close, c['open']) - low) / total > 0.6:
                    return True
    return False

def decode_compressed_row(row_str, mult):
    """Decode a compressed row: 't0,o0,h0,l0,c0,v0|dt1,do1,dh1,dl1,dc1,dv1|...' into list of candles."""
    parts = row_str.split('|')
    if not parts:
        return []
    candles = []
    # first part is absolute
    first = parts[0].split(',')
    if len(first) != 6:
        return []
    t0 = int(first[0])
    o0 = int(first[1])
    h0 = int(first[2])
    l0 = int(first[3])
    c0 = int(first[4])
    v0 = int(first[5])
    candles.append({
        "timestamp": t0 * 1000,
        "open": o0 / mult,
        "high": h0 / mult,
        "low": l0 / mult,
        "close": c0 / mult,
        "volume": v0 / mult
    })
    prev_t = t0
    prev_o = o0
    prev_h = h0
    prev_l = l0
    prev_c = c0
    prev_v = v0
    for part in parts[1:]:
        if not part:
            continue
        vals = part.split(',')
        if len(vals) != 6:
            continue
        dt = int(vals[0])
        do = int(vals[1])
        dh = int(vals[2])
        dl = int(vals[3])
        dc = int(vals[4])
        dv = int(vals[5])
        t = prev_t + dt
        o = prev_o + do
        h = prev_h + dh
        l = prev_l + dl
        c = prev_c + dc
        v = prev_v + dv
        candles.append({
            "timestamp": t * 1000,
            "open": o / mult,
            "high": h / mult,
            "low": l / mult,
            "close": c / mult,
            "volume": v / mult
        })
        prev_t, prev_o, prev_h, prev_l, prev_c, prev_v = t, o, h, l, c, v
    return candles

def parse_file(filepath, allowed_types):
    candles_by_type = {t: [] for t in allowed_types}
    raw_lines = []
    if not os.path.exists(filepath):
        return raw_lines, candles_by_type
    with open(filepath, "r") as f:
        # Read header (first line)
        header = f.readline().strip()
        raw_lines.append(header)
        line_num = 1
        mult = 100_000_000  # default multiplier
        for line in f:
            line_num += 1
            line = line.strip()
            if not line:
                continue
            raw_lines.append(line)
            # Check for multiplier comment
            if line.startswith('#mult='):
                try:
                    mult = int(line.split('=')[1])
                except:
                    pass
                continue
            # Check for compressed 1m row
            if line.startswith('1m_comp'):
                parts = line.split('\t')
                if len(parts) >= 3:
                    row_str = parts[2]  # format: idx\trow
                    decoded = decode_compressed_row(row_str, mult)
                    candles_by_type["1m_full"].extend(decoded)
                continue
            # Normal TSV line
            parts = line.split('\t')
            if len(parts) < 7:
                continue
            typ = parts[0]
            if typ not in allowed_types:
                continue
            try:
                ts = int(parts[1])
                o = float(parts[2])
                h = float(parts[3])
                l = float(parts[4])
                c = float(parts[5])
                v = float(parts[6])
                candles_by_type[typ].append({"timestamp": ts, "open": o, "high": h, "low": l, "close": c, "volume": v})
            except Exception as e:
                log_issue("WARNING", f"Error parsing line {line_num} in {os.path.basename(filepath)}: {e}")
    for typ in candles_by_type:
        candles_by_type[typ].sort(key=lambda x: x['timestamp'])
    # Log counts
    for typ in allowed_types:
        log_issue("INFO", f"Parsed {len(candles_by_type[typ])} '{typ}' rows from {os.path.basename(filepath)}")
    return raw_lines, candles_by_type

def process_volume_profile(symbol):
    print(f"[P10] Starting volume profile processing for {symbol}")
    start_time = time.time()
    log_issue("INFO", f"Starting volume profile processing for {symbol}")

    tmp1 = os.path.join(FEATURES_BASE_DIR, f"{symbol.lower()}_volProfile1.tmp_x")
    tmp2 = os.path.join(FEATURES_BASE_DIR, f"{symbol.lower()}_volProfile2.tmp_x")

    if not os.path.exists(tmp1):
        log_issue("ERROR", f"Missing input file: {tmp1}")
        print(f"[P10] Error: Missing {tmp1}")
        return False
    if not os.path.exists(tmp2):
        log_issue("ERROR", f"Missing input file: {tmp2}")
        print(f"[P10] Error: Missing {tmp2}")
        return False

    # Parse both files
    raw_lines1, data1 = parse_file(tmp1, ["daily", "1h", "4h", "1m_full"])
    raw_lines2, data2 = parse_file(tmp2, ["1m_full", "1m_last60"])

    daily = data1.get("daily", [])
    oneh = data1.get("1h", [])
    fourh = data1.get("4h", [])
    one_min_full = data1.get("1m_full", []) + data2.get("1m_full", [])
    one_min_last60 = data2.get("1m_last60", [])

    if not daily:
        log_issue("ERROR", f"No daily candles found in {tmp1}")
        print(f"[P10] Error: No daily candles in {tmp1}")
        return False
    if not one_min_full:
        log_issue("ERROR", f"No 1m_full candles found (combined from both files)")
        print(f"[P10] Error: No 1m_full candles")
        return False

    log_issue("INFO", f"Daily candles: {len(daily)}, 1h: {len(oneh)}, 4h: {len(fourh)}, 1m_full: {len(one_min_full)}, 1m_last60: {len(one_min_last60)}")

    tick_size = get_tick_size(symbol)
    current_price = daily[-1]['close']

    # Daily volume profile from full 1m candles
    daily_groups = defaultdict(list)
    for c in one_min_full:
        date = datetime.utcfromtimestamp(c['timestamp'] / 1000).date()
        daily_groups[date].append(c)
    if daily_groups:
        latest_date = max(daily_groups.keys())
        latest_candles = daily_groups[latest_date]
        daily_profile = compute_volume_profile(latest_candles, tick_size)
        if not daily_profile:
            daily_profile = {"poc": current_price, "vah": current_price, "val": current_price,
                             "hvns": [], "lvns": [], "shape": "D-shape"}
    else:
        daily_profile = {"poc": current_price, "vah": current_price, "val": current_price,
                         "hvns": [], "lvns": [], "shape": "D-shape"}

    poc, vah, val, hvns, lvns, shape = daily_profile["poc"], daily_profile["vah"], daily_profile["val"], daily_profile["hvns"], daily_profile["lvns"], daily_profile["shape"]

    # Define shape_bias
    if shape == "P-shape":
        shape_bias = "bullish"
    elif shape == "b-shape":
        shape_bias = "bearish"
    else:
        shape_bias = "neutral"

    intra_1h = compute_volume_profile(oneh, tick_size) if oneh else {"shape": "D-shape"}
    intra_4h = compute_volume_profile(fourh, tick_size) if fourh else {"shape": "D-shape"}
    dev = compute_volume_profile(one_min_last60, tick_size) if one_min_last60 else {"poc": current_price, "vah": current_price, "val": current_price}
    dev_poc, dev_vah, dev_val = dev["poc"], dev["vah"], dev["val"]

    # Scoring
    price_score = 15 if current_price > vah else (-15 if current_price < val else 0)
    shape_score = 30 if shape == "P-shape" else (-30 if shape == "b-shape" else 0)
    dev_poc_diff = (dev_poc - poc) / poc * 100 if poc != 0 else 0
    dev_poc_score = 15 if dev_poc_diff > 1 else (-15 if dev_poc_diff < -1 else 0)
    dev_va_score = 10 if current_price > dev_vah else (-10 if current_price < dev_val else 0)
    hvns_below = [h for h in hvns if h < current_price]
    hvns_above = [h for h in hvns if h > current_price]
    hvn_support = 10 if hvns_below and (current_price - max(hvns_below)) / current_price < 0.005 else 0
    hvn_resistance = -10 if hvns_above and (min(hvns_above) - current_price) / current_price < 0.005 else 0
    mtf_score = 25 if (shape == "P-shape" and intra_1h.get("shape") == "P-shape" and intra_4h.get("shape") == "P-shape") else (-25 if (shape == "b-shape" and intra_1h.get("shape") == "b-shape" and intra_4h.get("shape") == "b-shape") else 0)
    recent_1m = one_min_last60[-10:] if one_min_last60 else []
    pa_rej_above = has_rejection_wick(recent_1m, vah, 'above') if recent_1m else False
    pa_rej_below = has_rejection_wick(recent_1m, val, 'below') if recent_1m else False
    pa_score = -10 if pa_rej_above else (10 if pa_rej_below else 0)

    net_score = shape_score + price_score + dev_poc_score + dev_va_score + hvn_support + hvn_resistance + mtf_score + pa_score
    net_score = max(-100, min(100, net_score))

    if net_score >= 25:
        bias = "bullish"
        confidence = min(95, 60 + net_score // 2)
    elif net_score <= -25:
        bias = "bearish"
        confidence = min(95, 60 + abs(net_score) // 2)
    else:
        bias = "neutral"
        confidence = 50 + abs(net_score) // 2
    high_prob = "UP" if bias == "bullish" and confidence >= 90 else ("DOWN" if bias == "bearish" and confidence >= 90 else None)

    # Write output files
    out1_path = os.path.join(FEATURES_BASE_DIR, f"{symbol.lower()}_volProfile1.tmp_p")
    with open(out1_path, "w") as out1:
        out1.write("# === Raw data from volProfile1.tmp_x ===\n")
        for line in raw_lines1:
            out1.write("# " + line + "\n")
        out1.write("# No derived features in this file (see volProfile2.tmp_p)\n")

    out2_path = os.path.join(FEATURES_BASE_DIR, f"{symbol.lower()}_volProfile2.tmp_p")
    with open(out2_path, "w") as out2:
        out2.write("# === Raw data from volProfile2.tmp_x ===\n")
        for line in raw_lines2:
            out2.write("# " + line + "\n")
        out2.write("# === Derived features ===\n")
        header = [
            "timestamp", "current_price", "poc", "vah", "val", "shape",
            "price_vs_va", "shape_bias", "dev_poc", "dev_poc_diff_pct",
            "dev_vah", "dev_val", "dev_va_state",
            "nearest_hvn_below", "nearest_hvn_above", "hvn_support_score", "hvn_resistance_score",
            "mtf_confluence", "pa_rejection_above", "pa_rejection_below",
            "net_score", "bias", "confidence", "high_prob_scenario"
        ]
        out2.write("\t".join(header) + "\n")
        row = [
            str(int(time.time() * 1000)),
            f"{current_price:.2f}",
            f"{poc:.2f}", f"{vah:.2f}", f"{val:.2f}", shape,
            "above_VAH" if current_price > vah else ("below_VAL" if current_price < val else "inside_VA"),
            shape_bias,
            f"{dev_poc:.2f}", f"{dev_poc_diff:.2f}",
            f"{dev_vah:.2f}", f"{dev_val:.2f}", "above_dev_VAH" if current_price > dev_vah else ("below_dev_VAL" if current_price < dev_val else "inside_dev_VA"),
            f"{max(hvns_below):.2f}" if hvns_below else "",
            f"{min(hvns_above):.2f}" if hvns_above else "",
            str(hvn_support), str(hvn_resistance),
            "strong_bullish" if mtf_score == 25 else ("strong_bearish" if mtf_score == -25 else "mixed"),
            "1" if pa_rej_above else "0",
            "1" if pa_rej_below else "0",
            str(net_score), bias, str(confidence), high_prob if high_prob else ""
        ]
        out2.write("\t".join(row) + "\n")

    elapsed = time.time() - start_time
    print(f"[P10] Success ({elapsed:.1f}s) -> {os.path.basename(out2_path)}")
    log_issue("INFO", f"Processing complete for {symbol} in {elapsed:.2f}s")
    return True

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python P10_volume_profile.py SYMBOL")
        sys.exit(1)
    success = process_volume_profile(sys.argv[1].upper())
    sys.exit(0 if success else 1)