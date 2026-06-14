#!/usr/bin/env python3
"""
P04_derivatives_flow.py – Derivatives Feature Processing (No Liquidations)
- Reads raw derivative data from X07 (.tmp_x)
- Removed liquidation-related parsing and calculations (moved to P07)
- Outputs only derivative-specific features.
- Input file NOT deleted.
"""

import os
import sys
import time
import math
import datetime

FEATURES_BASE_DIR = os.path.join("market_data", "binance", "symbols")
LOG_FILE = os.path.join(FEATURES_BASE_DIR, "p04_derivatives_issues.log")
LOG_MAX_SIZE = 5_000_000

os.makedirs(FEATURES_BASE_DIR, exist_ok=True)

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
    if level == "ERROR":
        print(line)
    rotate_log_if_needed()
    with open(LOG_FILE, "a") as f:
        f.write(line + "\n")

# ---------- Helper functions ----------
def funding_zscore(current_rate, history, window=20, decay_alpha=0.1):
    if not history or len(history) < 5:
        return 0.0, 0.0, 1.0
    recent = list(history[-window:]) if len(history) >= window else history[:]
    weights = []
    values = []
    for i, h in enumerate(recent):
        age = len(recent) - 1 - i
        w = (1 - decay_alpha) ** age
        weights.append(w)
        values.append(h['funding_rate'])
    total_w = sum(weights)
    if total_w == 0:
        return 0.0, 0.0, 1.0
    mean = sum(w * v for w, v in zip(weights, values)) / total_w
    var = sum(w * (v - mean)**2 for w, v in zip(weights, values)) / total_w
    std = math.sqrt(var) if var > 0 else 1e-6
    std = max(std, 1e-5)
    z = (current_rate - mean) / std
    return z, mean, std

def ls_velocity(ls_history, current_ratio):
    if not ls_history or len(ls_history) < 2:
        return 0.0
    sorted_hist = sorted(ls_history, key=lambda x: x['timestamp'])
    last = sorted_hist[-1]['long_short_ratio']
    if len(sorted_hist) >= 2:
        prev = sorted_hist[-2]['long_short_ratio']
        time_diff_sec = (sorted_hist[-1]['timestamp'] - sorted_hist[-2]['timestamp']) / 1000.0
        if time_diff_sec > 0:
            return (last - prev) / (time_diff_sec / 3600.0)
    return last - current_ratio

def safe_float(s, default=0.0):
    try:
        return float(s) if s else default
    except:
        return default

def safe_int(s, default=0):
    try:
        return int(s) if s else default
    except:
        return default

def process_derivatives(symbol):
    print(f"[P04] Starting derivatives processing for {symbol}")
    start_time = time.time()
    log_issue("INFO", f"Starting derivatives processing for {symbol}")

    tmp_x_path = os.path.join(FEATURES_BASE_DIR, f"{symbol.lower()}_derivative.tmp_x")
    if not os.path.exists(tmp_x_path):
        log_issue("ERROR", f"Input file not found: {tmp_x_path}")
        return False

    # ---------- Parse .tmp_x (no liquidation rows) ----------
    snapshot = None
    oi_history = []
    ls_history = []
    funding_history = []

    with open(tmp_x_path, "r") as f:
        header = f.readline()  # skip
        for line_num, line in enumerate(f, 2):
            line = line.strip()
            if not line:
                continue
            parts = line.split('\t')
            if len(parts) < 2:
                continue
            rec_type = parts[0]
            if rec_type == "snapshot" and len(parts) >= 6:
                try:
                    snap = {
                        'timestamp': safe_int(parts[1]),
                        'spot_price': safe_float(parts[2]),
                        'mark_price': safe_float(parts[3]),
                        'funding_rate': safe_float(parts[4]),
                        'oi_current': safe_float(parts[5])
                    }
                    snapshot = snap
                except Exception as e:
                    log_issue("WARNING", f"Line {line_num}: snapshot parse error: {e}")
            elif rec_type == "oi_history" and len(parts) >= 3:
                try:
                    oi_history.append({
                        'timestamp': safe_int(parts[1]),
                        'value': safe_float(parts[2])
                    })
                except:
                    pass
            elif rec_type == "ls_history" and len(parts) >= 5:
                try:
                    ls_history.append({
                        'timestamp': safe_int(parts[1]),
                        'long_short_ratio': safe_float(parts[2]),
                        'long_account': safe_float(parts[3]),
                        'short_account': safe_float(parts[4])
                    })
                except:
                    pass
            elif rec_type == "funding_history" and len(parts) >= 3:
                try:
                    funding_history.append({
                        'timestamp': safe_int(parts[1]),
                        'funding_rate': safe_float(parts[2])
                    })
                except:
                    pass
            # Skip liquidation rows (no longer present, but safe)

    if not snapshot:
        log_issue("WARNING", "No snapshot line found; using history data where possible")
        if oi_history:
            latest_oi = max(oi_history, key=lambda x: x['timestamp'])
            snapshot = {
                'timestamp': latest_oi['timestamp'],
                'spot_price': 0,
                'mark_price': 0,
                'funding_rate': funding_history[-1]['funding_rate'] if funding_history else 0,
                'oi_current': latest_oi['value']
            }
        else:
            log_issue("ERROR", "No snapshot and no OI history – cannot proceed")
            return False

    spot = snapshot.get('spot_price', 0)
    oi_current = snapshot.get('oi_current', 0)
    funding_rate = snapshot.get('funding_rate', 0)
    mark_price = snapshot.get('mark_price', 0)
    current_ts = snapshot.get('timestamp', int(time.time()*1000))

    if spot == 0 and mark_price > 0:
        spot = mark_price
        log_issue("WARNING", f"Spot price missing, using mark price {mark_price} as proxy")

    # OI change percentage
    if oi_history and len(oi_history) >= 2:
        first_oi = oi_history[0]['value']
        last_oi = oi_history[-1]['value']
        oi_change_pct = ((last_oi - first_oi) / first_oi) * 100 if first_oi != 0 else 0
    else:
        oi_change_pct = 0

    # OI trend
    if len(oi_history) >= 5:
        recent_oi = [h['value'] for h in oi_history[-5:]]
        slope = (recent_oi[-1] - recent_oi[0]) / recent_oi[0] if recent_oi[0] != 0 else 0
        oi_trend = "rising" if slope > 0.03 else "falling" if slope < -0.03 else "flat"
    else:
        oi_trend = "flat"

    price_change_pct = 0  # not available from derivatives alone

    # OI + Price State
    if price_change_pct > 0.5 and oi_trend == "rising":
        oi_price_state = "bullish_buildup"
        oi_score = 1
    elif price_change_pct < -0.5 and oi_trend == "rising":
        oi_price_state = "bearish_buildup"
        oi_score = -1
    elif price_change_pct > 0.5 and oi_trend == "falling":
        oi_price_state = "short_covering"
        oi_score = 1
    elif price_change_pct < -0.5 and oi_trend == "falling":
        oi_price_state = "long_liquidation"
        oi_score = -1
    else:
        oi_price_state = "neutral"
        oi_score = 0

    funding_z, funding_mean, funding_std = funding_zscore(funding_rate, funding_history, window=20)

    if funding_z < -1.5 and price_change_pct > 0:
        funding_div = "bullish_oversold"
        funding_score = 1
    elif funding_z > 1.5 and price_change_pct < 0:
        funding_div = "bearish_overbought"
        funding_score = -1
    else:
        funding_div = "none"
        funding_score = 0

    ls_ratio_current = ls_history[-1]['long_short_ratio'] if ls_history else 0.5
    ls_vel = ls_velocity(ls_history, ls_ratio_current)

    # Basis
    if spot > 0:
        basis_pct = (mark_price - spot) / spot * 100
    else:
        basis_pct = 0
    if basis_pct > 0.2:
        basis_state = "premium"
        basis_score = -1 if oi_score == 0 else 0
    elif basis_pct < -0.2:
        basis_state = "discount"
        basis_score = 1
    else:
        basis_state = "neutral"
        basis_score = 0

    # Crowd Agreement
    ls_extreme = (ls_ratio_current > 1.3) or (ls_ratio_current < 0.7)
    funding_extreme = abs(funding_z) > 1.5
    if ls_extreme and funding_extreme:
        crowd = "extreme"
        crowd_penalty = -15
    elif ls_extreme or funding_extreme:
        crowd = "high"
        crowd_penalty = -5
    else:
        crowd = "low"
        crowd_penalty = 0

    # Net score (no liquidation contributions)
    net_score = oi_score + funding_score + basis_score

    if net_score > 0:
        direction_bias = 1
        confidence = min(90, 60 + int(net_score * 15))
    elif net_score < 0:
        direction_bias = -1
        confidence = min(90, 60 + int(abs(net_score) * 15))
    else:
        direction_bias = 0
        confidence = 50

    confidence = max(20, confidence + crowd_penalty)

    expected_move = (abs(funding_z) * 0.5) + (abs(oi_change_pct) * 0.1)
    if oi_price_state in ('bullish_buildup', 'bearish_buildup'):
        expected_move = min(5.0, expected_move * 1.5)
    expected_move = min(5.0, expected_move)

    manip_risk = 0
    if funding_extreme and ls_extreme:
        manip_risk = 80
    elif funding_extreme or ls_extreme:
        manip_risk = 50

    # Write output file (without liquidation fields)
    tmp_p_path = os.path.join(FEATURES_BASE_DIR, f"{symbol.lower()}_derivative.tmp_p")
    with open(tmp_p_path, "w") as out:
        header = [
            "timestamp", "oi_price_state", "oi_price_score", "funding_zscore",
            "funding_divergence", "funding_score", "ls_ratio_velocity",
            "basis_pct", "basis_state", "basis_score",
            "crowd_agreement", "crowd_penalty", "direction_bias", "confidence",
            "expected_move_pct", "manipulation_risk", "net_score"
        ]
        out.write("\t".join(header) + "\n")
        row = [
            str(current_ts),
            oi_price_state,
            str(oi_score),
            f"{funding_z:.2f}",
            funding_div,
            str(funding_score),
            f"{ls_vel:.4f}",
            f"{basis_pct:.4f}",
            basis_state,
            str(basis_score),
            crowd,
            str(crowd_penalty),
            str(direction_bias),
            str(confidence),
            f"{expected_move:.2f}",
            str(manip_risk),
            f"{net_score:.2f}"
        ]
        out.write("\t".join(row) + "\n")

    elapsed = time.time() - start_time
    print(f"[P04] Success ({elapsed:.1f}s) -> {os.path.basename(tmp_p_path)}")
    log_issue("INFO", f"Processing complete for {symbol} in {elapsed:.2f}s -> {tmp_p_path}")
    return True

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python P04_derivatives_flow.py SYMBOL")
        sys.exit(1)
    success = process_derivatives(sys.argv[1].upper())
    sys.exit(0 if success else 1)