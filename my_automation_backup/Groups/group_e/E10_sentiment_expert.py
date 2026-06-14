#!/usr/bin/env python3
# E10_sentiment_expert.py – Sentiment & OI Intelligence (≥90% Setup Detector)
# Reads data from sentiment, derivative, and price action files, outputs TSV summary.

import os
import sys
import time
import json
from collections import defaultdict

# ========================== CONFIGURATION ==========================
FEATURES_BASE_DIR = os.path.join("market_data", "binance", "symbols")
LOG_FILE = os.path.join(FEATURES_BASE_DIR, "E10_sentiment_expert.log")
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

# ========================== ORIGINAL analyze_sentiment (UNCHANGED) ==========================
def analyze_sentiment(data):
    """
    Args:
        data: dict with keys:
            - news_score: float (range -1..1, positive = bullish)
            - retail_bias: str ('Bullish_Extreme', 'Bearish_Extreme', or 'Neutral')
            - funding_velocity: float (change in funding rate over last period)
            - oi_trend: str ('rising', 'falling', 'flat')
            - price_change_pct: float (percentage change in price over last hour) – optional
            - social_velocity: int (mentions+upvotes) – optional
            - oi_velocity_pct: float (OI percentage change) – optional (used if oi_trend not provided)
            - symbol: str (optional, for logging)
    Returns:
        dict with:
            'bias' (bullish/bearish/neutral),
            'confidence' (int 0‑100, ≥90 indicates high prob),
            'high_prob_scenario' ('UP'/'DOWN'/None),
            'probability_estimate' (int),
            'reason' (str),
            'signals' (list),
            'net_score' (int),
            'retail_bias_raw' (str),
            'oi_price_state' (str)
    """
    signals = []
    bullish_score = 0
    bearish_score = 0

    news = data.get('news_score', 0.0)
    if news > 0.6:
        bullish_score += 25
        signals.append(f"Very bullish news sentiment ({news:.2f})")
    elif news > 0.3:
        bullish_score += 10
        signals.append(f"Moderately bullish news ({news:.2f})")
    elif news < -0.6:
        bearish_score += 25
        signals.append(f"Very bearish news sentiment ({news:.2f})")
    elif news < -0.3:
        bearish_score += 10
        signals.append(f"Moderately bearish news ({news:.2f})")
    else:
        signals.append(f"Neutral news ({news:.2f})")

    retail = data.get('retail_bias', 'Neutral')
    if retail == 'Bullish_Extreme':
        bearish_score += 20
        signals.append("Extreme retail bullishness → contrarian bearish")
    elif retail == 'Bearish_Extreme':
        bullish_score += 20
        signals.append("Extreme retail bearishness → contrarian bullish")
    else:
        signals.append("Retail positioning neutral")

    funding_vel = data.get('funding_velocity', 0.0)
    if funding_vel > 0.00005:
        bearish_score += 15
        signals.append(f"Funding rate rising ({funding_vel:.6f}) → longs increasing, bearish")
    elif funding_vel < -0.00005:
        bullish_score += 15
        signals.append(f"Funding rate falling ({funding_vel:.6f}) → shorts covering, bullish")
    else:
        signals.append(f"Funding velocity neutral ({funding_vel:.6f})")

    oi_trend = data.get('oi_trend', 'flat')
    price_change = data.get('price_change_pct', 0.0)
    oi_vel = data.get('oi_velocity_pct', 0.0)

    if abs(price_change) < 0.01 and oi_vel != 0:
        if oi_vel > 1.5:
            if oi_vel > 3:
                signals.append(f"OI building strongly ({oi_vel:.1f}%) without price move → possible accumulation")
            else:
                signals.append(f"OI rising ({oi_vel:.1f}%) while price flat")
    else:
        if price_change > 0.5 and oi_trend == 'rising':
            bullish_score += 30
            signals.append("Price up + OI rising → strong bullish buildup")
        elif price_change < -0.5 and oi_trend == 'rising':
            bearish_score += 30
            signals.append("Price down + OI rising → aggressive short buildup, bearish")
        elif price_change > 0.5 and oi_trend == 'falling':
            bullish_score += 20
            signals.append("Price up + OI falling → short covering rally, bullish")
        elif price_change < -0.5 and oi_trend == 'falling':
            bearish_score += 20
            signals.append("Price down + OI falling → long liquidation, bearish")
        else:
            signals.append("OI and price dynamics ambiguous")

    social = data.get('social_velocity', 0)
    if social > 500:
        if social > 1000:
            bearish_score += 10
            signals.append("Extreme social buzz → potential top")
        else:
            bullish_score += 5
            signals.append("Moderate social activity")
    elif social > 100:
        signals.append("Noticeable social activity")

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

    reason = f"Net score {net:+d}, signals: {signals[0] if signals else 'no clear signals'}"

    oi_price_state = "unknown"
    if price_change > 0.5:
        oi_price_state = "price_up"
    elif price_change < -0.5:
        oi_price_state = "price_down"
    if oi_trend == "rising":
        oi_price_state += "_oi_rising"
    elif oi_trend == "falling":
        oi_price_state += "_oi_falling"
    else:
        oi_price_state += "_oi_flat"

    return {
        "bias": bias,
        "confidence": confidence,
        "high_prob_scenario": high_prob,
        "probability_estimate": confidence,
        "reason": reason,
        "signals": signals,
        "net_score": net,
        "retail_bias_raw": retail,
        "oi_price_state": oi_price_state
    }

# ========================== LOAD DATA FROM VARIOUS SOURCES ==========================
def load_sentiment_data(symbol):
    """Read sentiment data from X17's .tmp_x or P09's .tmp_p."""
    path = os.path.join(FEATURES_BASE_DIR, f"{symbol.lower()}_sentiment.tmp_x")
    if not os.path.exists(path):
        path = os.path.join(FEATURES_BASE_DIR, f"{symbol.lower()}_sentiment.tmp_p")
    if not os.path.exists(path):
        log_issue("ERROR", f"Sentiment file not found: {path}")
        return {}
    data = {}
    try:
        with open(path, "r") as f:
            lines = f.readlines()
        if len(lines) < 2:
            return {}
        # The file could be raw X17 (type-based) or processed P09 (header)
        if lines[0].startswith("type\t"):
            # Raw X17 format: type timestamp value1 value2 ...
            for line in lines[1:]:
                parts = line.strip().split('\t')
                if len(parts) < 2:
                    continue
                typ = parts[0]
                if typ == "sentiment_snapshot" and len(parts) >= 4:
                    # Example: sentiment_snapshot timestamp news_score retail_bias social_velocity
                    data['news_score'] = float(parts[2]) if parts[2] else 0.0
                    data['retail_bias'] = parts[3] if len(parts) > 3 else 'Neutral'
                    if len(parts) > 4:
                        data['social_velocity'] = int(parts[4]) if parts[4] else 0
                    else:
                        data['social_velocity'] = 0
        else:
            # Processed P09 format: header line then data line
            header = lines[0].strip().split('\t')
            values = lines[1].strip().split('\t')
            for i, col in enumerate(header):
                if i >= len(values):
                    break
                val = values[i]
                if col == 'news_score':
                    data['news_score'] = float(val) if val else 0.0
                elif col == 'retail_bias':
                    data['retail_bias'] = val
                elif col == 'social_velocity':
                    data['social_velocity'] = int(val) if val else 0
    except Exception as e:
        log_issue("WARNING", f"Failed to load sentiment data: {e}")
    # defaults
    data.setdefault('news_score', 0.0)
    data.setdefault('retail_bias', 'Neutral')
    data.setdefault('social_velocity', 0)
    return data

def load_derivative_data(symbol):
    """Read funding velocity and OI trend from P04's .tmp_p and raw derivative .tmp_x."""
    data = {}
    path = os.path.join(FEATURES_BASE_DIR, f"{symbol.lower()}_derivative.tmp_p")
    if os.path.exists(path):
        try:
            with open(path, "r") as f:
                lines = f.readlines()
            if len(lines) >= 2:
                header = lines[0].strip().split('\t')
                values = lines[1].strip().split('\t')
                for i, col in enumerate(header):
                    if i >= len(values):
                        break
                    val = values[i]
                    if col == 'oi_trend':
                        data['oi_trend'] = val
                    elif col == 'oi_velocity_pct':
                        data['oi_velocity_pct'] = float(val) if val else 0.0
        except Exception as e:
            log_issue("WARNING", f"Failed to load derivative processed data: {e}")
    # Try to read raw derivative .tmp_x for funding_velocity
    raw_path = os.path.join(FEATURES_BASE_DIR, f"{symbol.lower()}_derivative.tmp_x")
    if os.path.exists(raw_path):
        try:
            with open(raw_path, "r") as f:
                funding_rates = []
                for line in f:
                    parts = line.strip().split('\t')
                    if len(parts) >= 3 and parts[0] == "funding_history":
                        funding_rates.append(float(parts[2]))
                if len(funding_rates) >= 2:
                    data['funding_velocity'] = funding_rates[-1] - funding_rates[-2]
                else:
                    data['funding_velocity'] = 0.0
        except:
            pass
    data.setdefault('oi_trend', 'flat')
    data.setdefault('oi_velocity_pct', 0.0)
    data.setdefault('funding_velocity', 0.0)
    return data

def load_price_data(symbol):
    """Read price_change_pct from P01's .tmp_p."""
    path = os.path.join(FEATURES_BASE_DIR, f"{symbol.lower()}.tmp_p")
    if not os.path.exists(path):
        return {}
    data = {}
    try:
        with open(path, "r") as f:
            lines = f.readlines()
        if len(lines) < 2:
            return {}
        header = lines[0].strip().split('\t')
        values = lines[1].strip().split('\t')
        for i, col in enumerate(header):
            if i >= len(values):
                break
            if col == 'price_change_pct':
                data['price_change_pct'] = float(values[i]) if values[i] else 0.0
                break
    except Exception as e:
        log_issue("WARNING", f"Failed to load price data: {e}")
    data.setdefault('price_change_pct', 0.0)
    return data

def combine_data(symbol):
    sent = load_sentiment_data(symbol)
    deriv = load_derivative_data(symbol)
    price = load_price_data(symbol)
    combined = {}
    combined.update(sent)
    combined.update(deriv)
    combined.update(price)
    # Fill any missing fields with defaults
    combined.setdefault('news_score', 0.0)
    combined.setdefault('retail_bias', 'Neutral')
    combined.setdefault('funding_velocity', 0.0)
    combined.setdefault('oi_trend', 'flat')
    combined.setdefault('price_change_pct', 0.0)
    combined.setdefault('social_velocity', 0)
    combined.setdefault('oi_velocity_pct', 0.0)
    return combined

# ========================== MAIN EXPERT FUNCTION ==========================
def run_expert(symbol):
    log_issue("INFO", f"Starting E10 sentiment expert for {symbol}")
    data = combine_data(symbol)
    if not data:
        log_issue("ERROR", "No data available")
        return None
    result = analyze_sentiment(data)
    # Write output TSV
    out_path = os.path.join(FEATURES_BASE_DIR, f"{symbol.lower()}_E10_sentiment.tsv")
    with open(out_path, "w") as f:
        header = ["timestamp", "bias", "confidence", "high_prob_scenario", "probability_estimate",
                  "reason", "signals_json", "net_score", "retail_bias_raw", "oi_price_state"]
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
            result['retail_bias_raw'],
            result['oi_price_state']
        ]
        f.write("\t".join(row) + "\n")
    log_issue("INFO", f"Saved sentiment expert summary to {out_path}")
    return out_path

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python E10_sentiment_expert.py SYMBOL")
        sys.exit(1)
    success = run_expert(sys.argv[1].upper())
    sys.exit(0 if success else 1)