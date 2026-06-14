#!/usr/bin/env python3
# E04_cvd_expert.py – CVD High‑Probability Scenario Detector (≥90% setups)
# Reads processed CVD data from P02 (.tmp_p) and raw CVD data (.tmp_x)
# Outputs TSV summary with bias, confidence, scenario, etc.

import os
import sys
import time
import math
import json
from collections import defaultdict

# ========================== CONFIGURATION ==========================
FEATURES_BASE_DIR = os.path.join("market_data", "binance", "symbols")
LOG_FILE = os.path.join(FEATURES_BASE_DIR, "E04_cvd_expert.log")
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

# ========================== ORIGINAL ANALYZE_CVD FUNCTION (UNCHANGED) ==========================
def analyze_cvd(cvd_data):
    """
    Input: dict with keys:
        direction: 'up'/'down'/'neutral'
        confidence: int 0-100
        cvd_slope_10: float
        cvd_acceleration: float
        divergence: 'bullish'/'bearish'/'none'
        absorption_net: int (bullish - bearish absorptions)
        imbalance_score: float
        cumulative_cvd: float (optional)
    Returns:
        dict with:
            'bias': 'bullish'/'bearish'/'neutral'
            'confidence': int (0-100)
            'high_prob_scenario': 'UP'/'DOWN'/None
            'probability_estimate': int (0-100)
            'reason': str
            'signals': list of str
    """
    signals = []
    bullish_score = 0
    bearish_score = 0
    direction = cvd_data.get('direction', 'neutral')
    confidence = cvd_data.get('confidence', 50)
    slope = cvd_data.get('cvd_slope_10', 0)
    acc = cvd_data.get('cvd_acceleration', 0)
    divergence = cvd_data.get('divergence', 'none')
    absorption_net = cvd_data.get('absorption_net', 0)
    imbalance_score = cvd_data.get('imbalance_score', 0)

    # 1. Divergence (most powerful signal)
    if divergence == 'bullish':
        bullish_score += 40
        signals.append("Bullish CVD divergence (price down, CVD up)")
    elif divergence == 'bearish':
        bearish_score += 40
        signals.append("Bearish CVD divergence (price up, CVD down)")

    # 2. Strong slope (>50 units per 10 candles) with acceleration
    if slope > 50:
        bullish_score += 25
        signals.append(f"CVD strong uptrend (slope {slope:.1f})")
    elif slope < -50:
        bearish_score += 25
        signals.append(f"CVD strong downtrend (slope {slope:.1f})")
    # acceleration adds confidence
    if acc > 20 and slope > 0:
        bullish_score += 15
        signals.append("CVD accelerating upward")
    elif acc < -20 and slope < 0:
        bearish_score += 15
        signals.append("CVD accelerating downward")

    # 3. Absorption events (hidden buying/selling)
    if absorption_net >= 2:
        bullish_score += 20
        signals.append(f"Bullish absorption detected (net {absorption_net})")
    elif absorption_net <= -2:
        bearish_score += 20
        signals.append(f"Bearish absorption detected (net {absorption_net})")

    # 4. Imbalance score (>200 strong signal)
    if imbalance_score > 200:
        bullish_score += 15
        signals.append(f"Strong buy imbalance (score {imbalance_score:.0f})")
    elif imbalance_score < -200:
        bearish_score += 15
        signals.append(f"Strong sell imbalance (score {imbalance_score:.0f})")

    # 5. X03's own direction/confidence (moderate weight)
    if direction == 'up':
        bullish_score += confidence / 5  # max 20
    elif direction == 'down':
        bearish_score += confidence / 5

    # Determine net score (-100 to 100)
    net = bullish_score - bearish_score
    net = max(-100, min(100, net))

    # Bias and confidence
    if net >= 30:
        bias = "bullish"
        prob = min(95, 60 + net // 2)
    elif net <= -30:
        bias = "bearish"
        prob = min(95, 60 + abs(net) // 2)
    else:
        bias = "neutral"
        prob = 50

    # High-probability threshold (≥90%)
    high_prob_scenario = None
    if bias == "bullish" and prob >= 90:
        high_prob_scenario = "UP"
    elif bias == "bearish" and prob >= 90:
        high_prob_scenario = "DOWN"
    else:
        high_prob_scenario = None

    # Build reason string
    reason = f"CVD net score {net:+d} – " + ", ".join(signals[:3]) if signals else "No strong signals"

    return {
        "bias": bias,
        "confidence": prob,
        "high_prob_scenario": high_prob_scenario,
        "probability_estimate": prob,
        "reason": reason,
        "signals": signals,
        "net_score": net,
        "cvd_slope": slope,
        "absorption_net": absorption_net,
        "imbalance_score": imbalance_score,
        "divergence": divergence
    }

# ========================== LOAD CVD DATA FROM P02 FILES ==========================
def load_cvd_summary(symbol):
    """Read the summary from P02's cvd2.tmp_p (last line) and also raw CVD data from .tmp_x."""
    # First, get the summary from .tmp_p files
    summary_path = os.path.join(FEATURES_BASE_DIR, f"{symbol.lower()}_cvd2.tmp_p")
    net_cvd = None
    trend = None
    if os.path.exists(summary_path):
        try:
            with open(summary_path, "r") as f:
                lines = f.readlines()
            if lines:
                # last line is summary: cvd_net\ttrend
                last = lines[-1].strip().split('\t')
                if len(last) >= 2:
                    net_cvd = float(last[0])
                    trend = last[1]
        except Exception as e:
            log_issue("WARNING", f"Could not read CVD summary: {e}")
    # Also read raw CVD data from .tmp_x to compute slope, acceleration, etc.
    # Raw CVD files are compressed; we can decode them to get net delta per minute.
    # We'll implement a simple decoder for the X03 compressed format.
    raw_cvd1 = os.path.join(FEATURES_BASE_DIR, f"{symbol.lower()}_cvd1.tmp_x")
    raw_cvd2 = os.path.join(FEATURES_BASE_DIR, f"{symbol.lower()}_cvd2.tmp_x")
    net_per_minute = {}

    def decode_cvd_file(filepath):
        if not os.path.exists(filepath):
            return
        try:
            with open(filepath, "r") as f:
                lines = f.readlines()
            if not lines:
                return
            # parse header (lines starting with '#')
            idx = 0
            while idx < len(lines) and lines[idx].startswith('#'):
                idx += 1
            if idx >= len(lines):
                return
            # row 0: "0\tbase64..."
            row0 = lines[idx].strip().split('\t')
            if len(row0) != 2:
                return
            base_parts = row0[1].split(',')
            if len(base_parts) != 4:
                return
            # decode base values
            ts0 = decode_compressed(base_parts[0]) * 60000
            p0 = decode_compressed(base_parts[1])
            q0 = decode_compressed(base_parts[2])
            sell0 = int(base_parts[3])
            # Convert to net USD (buy positive, sell negative)
            net0 = (p0 * q0) * (-1 if sell0 else 1)
            net_per_minute[ts0] = net0
            prev_ts = decode_compressed(base_parts[0])
            prev_p = p0
            prev_q = q0
            # subsequent rows
            for line in lines[idx+1:]:
                line = line.strip()
                if not line or line.startswith('#'):
                    continue
                parts = line.split('\t')
                if len(parts) != 2:
                    continue
                deltas = parts[1].split(';')
                for d in deltas:
                    if not d:
                        continue
                    d_ts, d_p, d_q, sell = d.split(',')
                    dt = decode_compressed(d_ts)
                    dp = decode_compressed(d_p)
                    dq = decode_compressed(d_q)
                    cur_ts = prev_ts + dt
                    cur_p = prev_p + dp
                    cur_q = prev_q + dq
                    ts_ms = cur_ts * 60000
                    net_usd = (cur_p * cur_q) * (-1 if sell == '1' else 1)
                    net_per_minute[ts_ms] = net_usd
                    prev_ts, prev_p, prev_q = cur_ts, cur_p, cur_q
        except Exception as e:
            log_issue("WARNING", f"Failed to decode CVD file {filepath}: {e}")

    # Helper to decode base62 compressed values
    BASE62 = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
    def decode_compressed(s):
        if not s:
            return 0
        sign = s[0]
        num = 0
        for ch in s[1:]:
            num = num * 62 + BASE62.index(ch)
        return -num if sign == 'm' else num

    decode_cvd_file(raw_cvd1)
    decode_cvd_file(raw_cvd2)

    # Compute cumulative CVD (if we have per‑minute net)
    cumulative = 0
    cum_list = []
    for ts in sorted(net_per_minute.keys()):
        cumulative += net_per_minute[ts]
        cum_list.append((ts, cumulative))

    # Compute slope (last 10 periods) and acceleration
    slope = 0
    acc = 0
    if len(cum_list) >= 10:
        # Use last 10 points (approx 10 minutes)
        last_10 = cum_list[-10:]
        # fit linear regression to slope per minute
        x = list(range(len(last_10)))
        y = [v for _, v in last_10]
        n = len(x)
        sum_x = sum(x)
        sum_y = sum(y)
        sum_xy = sum(x[i] * y[i] for i in range(n))
        sum_x2 = sum(xi * xi for xi in x)
        denom = n * sum_x2 - sum_x * sum_x
        if denom != 0:
            slope = (n * sum_xy - sum_x * sum_y) / denom   # per minute
            # Acceleration: slope of slopes over last 5 points
            if len(last_10) >= 5:
                y2 = [cum_list[i][1] for i in range(len(cum_list)-5, len(cum_list))]
                x2 = list(range(len(y2)))
                n2 = len(x2)
                sum_x2_2 = sum(x2)
                sum_y2 = sum(y2)
                sum_xy2 = sum(x2[i] * y2[i] for i in range(n2))
                sum_x2_2_2 = sum(xi * xi for xi in x2)
                denom2 = n2 * sum_x2_2_2 - sum_x2_2 * sum_x2_2
                if denom2 != 0:
                    slope2 = (n2 * sum_xy2 - sum_x2_2 * sum_y2) / denom2
                    acc = slope2 - slope  # approximate acceleration

    # Determine direction and confidence from net CVD and trend
    direction = "neutral"
    confidence = 50
    if net_cvd is not None:
        if net_cvd > 0:
            direction = "up"
            confidence = min(95, 50 + int(abs(net_cvd) / 10000))
        elif net_cvd < 0:
            direction = "down"
            confidence = min(95, 50 + int(abs(net_cvd) / 10000))
    # Use trend from summary if available
    if trend == "rising":
        if direction == "neutral":
            direction = "up"
            confidence = 65
    elif trend == "falling":
        if direction == "neutral":
            direction = "down"
            confidence = 65

    # Divergence – we don't have price data here, so set to 'none'
    divergence = "none"
    # Absorption net – we don't have absorption events, set to 0
    absorption_net = 0
    # Imbalance score – also not available
    imbalance_score = 0

    # Build the data dict
    cvd_data = {
        "direction": direction,
        "confidence": confidence,
        "cvd_slope_10": slope,
        "cvd_acceleration": acc,
        "divergence": divergence,
        "absorption_net": absorption_net,
        "imbalance_score": imbalance_score,
        "cumulative_cvd": cumulative
    }
    return cvd_data

# ========================== MAIN EXPORT FUNCTION ==========================
def run_expert(symbol):
    log_issue("INFO", f"Starting E04 CVD expert for {symbol}")
    cvd_data = load_cvd_summary(symbol)
    if not cvd_data:
        log_issue("ERROR", "No CVD data found")
        return None
    result = analyze_cvd(cvd_data)
    # Write output TSV
    out_path = os.path.join(FEATURES_BASE_DIR, f"{symbol.lower()}_E04_cvd.tsv")
    with open(out_path, "w") as f:
        header = ["timestamp", "bias", "confidence", "high_prob_scenario", "probability_estimate",
                  "reason", "signals_json", "net_score", "cvd_slope", "absorption_net",
                  "imbalance_score", "divergence"]
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
            str(result['net_score']),
            str(result['cvd_slope']),
            str(result['absorption_net']),
            str(result['imbalance_score']),
            result['divergence']
        ]
        f.write("\t".join(row) + "\n")
    log_issue("INFO", f"Saved CVD expert summary to {out_path}")
    return out_path

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python E04_cvd_expert.py SYMBOL")
        sys.exit(1)
    success = run_expert(sys.argv[1].upper())
    sys.exit(0 if success else 1)