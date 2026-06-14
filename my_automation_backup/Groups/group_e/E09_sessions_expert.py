#!/usr/bin/env python3
# E09_sessions_expert.py – Session Intelligence High‑Probability Scenario Detector (≥90%)
# Reads session data from X15 .tmp_x, builds dict, and outputs TSV summary.

import os
import sys
import time
import json
from collections import defaultdict

# ========================== CONFIGURATION ==========================
FEATURES_BASE_DIR = os.path.join("market_data", "binance", "symbols")
LOG_FILE = os.path.join(FEATURES_BASE_DIR, "E09_sessions_expert.log")
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

# ========================== ORIGINAL analyze_sessions (UNCHANGED) ==========================
def analyze_sessions(data):
    """
    Args:
        data: dict with keys:
            - session_bias: str ('Strong_Bullish', 'Strong_Bearish', 'Neutral')
            - previous_session_high: float
            - previous_session_low: float
            - news_danger_zone: bool
            - london_kill_zone_active: bool (optional)
            - ny_kill_zone_active: bool (optional)
            - london_initial_balance: (high, low) tuple or None
            - ny_initial_balance: (high, low) tuple or None
            - volatility_profile: dict {hour: avg_volatility_pct}
            - current_price: float (optional)
            - current_hour_lahore: int (optional)
    Returns:
        dict with bias, confidence, high_prob_scenario, etc.
    """
    signals = []
    bullish_score = 0
    bearish_score = 0

    bias = data.get('session_bias', 'Neutral')
    if bias == 'Strong_Bullish':
        bullish_score += 30
        signals.append("Price above last 8‑candle highs → strong bullish bias")
    elif bias == 'Strong_Bearish':
        bearish_score += 30
        signals.append("Price below last 8‑candle lows → strong bearish bias")
    else:
        signals.append("Neutral price position within range")

    prev_high = data.get('previous_session_high')
    prev_low = data.get('previous_session_low')
    current_price = data.get('current_price')
    if current_price and prev_high and prev_low:
        dist_to_high = (prev_high - current_price) / current_price * 100 if current_price else 100
        dist_to_low = (current_price - prev_low) / current_price * 100 if current_price else 100
        if 0 < dist_to_high < 0.5:
            bullish_score += 15
            signals.append(f"Price within 0.5% of previous session high → potential breakout")
        elif 0 < dist_to_low < 0.5:
            bearish_score += 15
            signals.append(f"Price within 0.5% of previous session low → potential breakdown")
    else:
        signals.append("Previous session levels not used (no current price)")

    danger_zone_active = data.get('news_danger_zone', False)
    if danger_zone_active:
        signals.append("High‑impact news within 15 minutes → high volatility expected")

    london_active = data.get('london_kill_zone_active', False)
    ny_active = data.get('ny_kill_zone_active', False)
    if london_active:
        signals.append("London kill zone active (2‑5 AM EST) → potential momentum")
        bullish_score += 5
        bearish_score += 5
    if ny_active:
        signals.append("New York kill zone active (8‑11 AM EST) → potential momentum")
        bullish_score += 5
        bearish_score += 5

    london_ib = data.get('london_initial_balance')
    ny_ib = data.get('ny_initial_balance')
    if current_price:
        if london_ib and len(london_ib) == 2:
            high, low = london_ib
            if current_price > high:
                bullish_score += 20
                signals.append(f"Price above London IB high ({high}) → breakout bullish")
            elif current_price < low:
                bearish_score += 20
                signals.append(f"Price below London IB low ({low}) → breakdown bearish")
            else:
                signals.append("Price inside London Initial Balance range → chop")
        if ny_ib and len(ny_ib) == 2:
            high, low = ny_ib
            if current_price > high:
                bullish_score += 20
                signals.append(f"Price above NY IB high ({high}) → breakout bullish")
            elif current_price < low:
                bearish_score += 20
                signals.append(f"Price below NY IB low ({low}) → breakdown bearish")
            else:
                signals.append("Price inside NY Initial Balance range → chop")

    vol_profile = data.get('volatility_profile', {})
    current_hour = data.get('current_hour_lahore')
    if vol_profile and current_hour is not None and current_hour in vol_profile:
        avg_vol = vol_profile[current_hour]
        if avg_vol > 0.5:
            signals.append(f"Current hour volatility {avg_vol:.2f}% above threshold → potential breakout")
            bullish_score += 5
            bearish_score += 5
        else:
            signals.append(f"Current hour volatility {avg_vol:.2f}% → normal")

    net = bullish_score - bearish_score
    net = max(-100, min(100, net))

    if net >= 30:
        bias_out = "bullish"
        confidence = min(95, 60 + net // 2)
    elif net <= -30:
        bias_out = "bearish"
        confidence = min(95, 60 + abs(net) // 2)
    else:
        bias_out = "neutral"
        confidence = 50 + net // 2 if net else 50

    if danger_zone_active and abs(net) < 40:
        confidence = max(30, confidence - 15)

    high_prob = None
    if confidence >= 90 and bias_out != "neutral":
        high_prob = "UP" if bias_out == "bullish" else "DOWN"

    reason = f"Net score {net:+d}, signals: {signals[0] if signals else 'no clear signals'}"

    return {
        "bias": bias_out,
        "confidence": confidence,
        "high_prob_scenario": high_prob,
        "probability_estimate": confidence,
        "reason": reason,
        "signals": signals,
        "net_score": net
    }

# ========================== LOAD SESSION DATA FROM X15 ==========================
def load_session_data(symbol):
    """Read X15 raw .tmp_x file and build dict for analyze_sessions."""
    path = os.path.join(FEATURES_BASE_DIR, f"{symbol.lower()}_sessions.tmp_x")
    if not os.path.exists(path):
        # fallback to processed .tmp_p
        path = os.path.join(FEATURES_BASE_DIR, f"{symbol.lower()}_sessions.tmp_p")
        if not os.path.exists(path):
            log_issue("ERROR", f"Session file not found: {path}")
            return None
    data = {}
    try:
        with open(path, "r") as f:
            lines = f.readlines()
        if not lines:
            return None
        # Determine format: if first line contains 'type' then it's raw TSV
        # Raw format: "type\ttimestamp\tvalue1\tvalue2\t..."
        # Processed format might have header with column names.
        # We'll parse raw format first.
        if lines[0].startswith("type\t"):
            for line in lines[1:]:
                parts = line.strip().split('\t')
                if len(parts) < 2:
                    continue
                typ = parts[0]
                if typ == "snapshot":
                    # expected: snapshot timestamp session_bias prev_high prev_low current_price?
                    # We need to adapt to actual X15 output. Assume columns after timestamp: session_bias, prev_high, prev_low
                    if len(parts) >= 5:
                        data['session_bias'] = parts[2]  # placeholder, may need mapping
                        data['previous_session_high'] = float(parts[3]) if parts[3] else None
                        data['previous_session_low'] = float(parts[4]) if parts[4] else None
                elif typ == "kill_zones":
                    # expected: kill_zones timestamp london_active ny_active
                    if len(parts) >= 4:
                        data['london_kill_zone_active'] = parts[2].lower() == 'true'
                        data['ny_kill_zone_active'] = parts[3].lower() == 'true'
                elif typ == "initial_balance":
                    # expected: initial_balance timestamp zone high low
                    if len(parts) >= 5:
                        zone = parts[2]
                        high = float(parts[3])
                        low = float(parts[4])
                        if zone.lower() == 'london':
                            data['london_initial_balance'] = (high, low)
                        elif zone.lower() == 'ny':
                            data['ny_initial_balance'] = (high, low)
                elif typ == "volatility_profile":
                    # expected: volatility_profile timestamp hour pct
                    if len(parts) >= 4:
                        hour = int(parts[2])
                        pct = float(parts[3])
                        if 'volatility_profile' not in data:
                            data['volatility_profile'] = {}
                        data['volatility_profile'][hour] = pct
                elif typ == "news":
                    # news danger zone: news timestamp danger_zone_flag
                    if len(parts) >= 3:
                        data['news_danger_zone'] = parts[2].lower() == 'true'
                elif typ == "current_price":
                    if len(parts) >= 3:
                        data['current_price'] = float(parts[2])
        else:
            # Assume processed format with header (e.g., from P08 .tmp_p)
            # We'll try to parse the first data line
            if len(lines) < 2:
                return None
            header = lines[0].strip().split('\t')
            values = lines[1].strip().split('\t')
            for i, col in enumerate(header):
                if i < len(values):
                    val = values[i]
                    if col == 'session_bias':
                        data['session_bias'] = val
                    elif col == 'previous_session_high':
                        data['previous_session_high'] = float(val) if val else None
                    elif col == 'previous_session_low':
                        data['previous_session_low'] = float(val) if val else None
                    elif col == 'news_danger_zone':
                        data['news_danger_zone'] = val.lower() == 'true'
                    elif col == 'london_kill_zone_active':
                        data['london_kill_zone_active'] = val.lower() == 'true'
                    elif col == 'ny_kill_zone_active':
                        data['ny_kill_zone_active'] = val.lower() == 'true'
                    elif col == 'london_ib_high' and 'london_ib_low' in header:
                        # find both
                        idx_high = header.index('london_ib_high') if 'london_ib_high' in header else -1
                        idx_low = header.index('london_ib_low') if 'london_ib_low' in header else -1
                        if idx_high >= 0 and idx_low >= 0:
                            high = float(values[idx_high]) if values[idx_high] else None
                            low = float(values[idx_low]) if values[idx_low] else None
                            if high is not None and low is not None:
                                data['london_initial_balance'] = (high, low)
                    elif col == 'ny_ib_high' and 'ny_ib_low' in header:
                        idx_high = header.index('ny_ib_high') if 'ny_ib_high' in header else -1
                        idx_low = header.index('ny_ib_low') if 'ny_ib_low' in header else -1
                        if idx_high >= 0 and idx_low >= 0:
                            high = float(values[idx_high]) if values[idx_high] else None
                            low = float(values[idx_low]) if values[idx_low] else None
                            if high is not None and low is not None:
                                data['ny_initial_balance'] = (high, low)
                    elif col == 'volatility_profile_json':
                        try:
                            data['volatility_profile'] = json.loads(val)
                        except:
                            pass
                    elif col == 'current_price':
                        data['current_price'] = float(val) if val else None
                    elif col == 'current_hour_lahore':
                        data['current_hour_lahore'] = int(val) if val else None
    except Exception as e:
        log_issue("ERROR", f"Failed to load session data: {e}")
        return None

    # Fill defaults for missing optional fields
    data.setdefault('session_bias', 'Neutral')
    data.setdefault('previous_session_high', None)
    data.setdefault('previous_session_low', None)
    data.setdefault('news_danger_zone', False)
    data.setdefault('london_kill_zone_active', False)
    data.setdefault('ny_kill_zone_active', False)
    data.setdefault('london_initial_balance', None)
    data.setdefault('ny_initial_balance', None)
    data.setdefault('volatility_profile', {})
    data.setdefault('current_price', None)
    data.setdefault('current_hour_lahore', None)

    return data

# ========================== MAIN EXPERT FUNCTION ==========================
def run_expert(symbol):
    log_issue("INFO", f"Starting E09 session expert for {symbol}")
    session_data = load_session_data(symbol)
    if not session_data:
        log_issue("ERROR", "No session data found")
        return None
    result = analyze_sessions(session_data)
    # Write output TSV
    out_path = os.path.join(FEATURES_BASE_DIR, f"{symbol.lower()}_E09_sessions.tsv")
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
    log_issue("INFO", f"Saved session expert summary to {out_path}")
    return out_path

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python E09_sessions_expert.py SYMBOL")
        sys.exit(1)
    success = run_expert(sys.argv[1].upper())
    sys.exit(0 if success else 1)