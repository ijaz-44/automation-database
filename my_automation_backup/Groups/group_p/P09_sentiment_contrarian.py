#!/usr/bin/env python3
"""
P09_sentiment_contrarian.py – Process Raw Sentiment Data
- Reads {symbol}_sentiment.tmp_x (TSV from X17)
- Computes: news impact, retail contrarian bias, funding velocity,
  OI+price state, social mania, final contrarian score and conviction
- Outputs TSV {symbol}_sentiment.tmp_p with raw data (commented) + derived features
- Logs to p09_sentiment_contrarian_issues.log (file only, minimal console)
- Input file is NOT deleted.
"""

import os
import sys
import time
import math

# ========== CONFIGURATION ==========
FEATURES_BASE_DIR = os.path.join("market_data", "binance", "symbols")
LOG_FILE = os.path.join(FEATURES_BASE_DIR, "p09_sentiment_contrarian_issues.log")
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
def compute_retail_bias(long_ratio):
    if long_ratio > 0.65:
        return "Bearish_Extreme", 1
    elif long_ratio < 0.35:
        return "Bullish_Extreme", -1
    else:
        return "Neutral", 0

def compute_funding_extreme(funding_rate):
    if funding_rate > 0.0003:
        return "high_positive", -1
    elif funding_rate < -0.0003:
        return "high_negative", 1
    else:
        return "normal", 0

def compute_oi_price_state(price_change, oi_trend):
    if abs(price_change) < 0.1:
        if oi_trend == 'rising':
            return "oi_building_no_move", 0
        elif oi_trend == 'falling':
            return "oi_contracting_no_move", 0
        else:
            return "neutral", 0
    else:
        if price_change > 0.5 and oi_trend == 'rising':
            return "price_up_oi_rising", 1
        elif price_change < -0.5 and oi_trend == 'rising':
            return "price_down_oi_rising", -1
        elif price_change > 0.5 and oi_trend == 'falling':
            return "price_up_oi_falling", 1
        elif price_change < -0.5 and oi_trend == 'falling':
            return "price_down_oi_falling", -1
        else:
            return "ambiguous", 0

# ========== MAIN PROCESSING ==========
def process_sentiment(symbol):
    print(f"[P09] Starting sentiment processing for {symbol}")
    start_time = time.time()
    log_issue("INFO", f"Starting sentiment processing for {symbol}")

    tmp_x_path = os.path.join(FEATURES_BASE_DIR, f"{symbol.lower()}_sentiment.tmp_x")
    if not os.path.exists(tmp_x_path):
        log_issue("ERROR", f"Input file not found: {tmp_x_path}")
        return False

    # ---------- Read raw data and store raw lines ----------
    raw_lines = []
    news_data = {}
    social_data = {}
    funding_history = []
    retail_data = {}
    snapshot_data = {}

    with open(tmp_x_path, "r") as f:
        header = f.readline().strip()
        raw_lines.append(header)  # header line
        for line in f:
            line = line.strip()
            if not line:
                continue
            raw_lines.append(line)
            parts = line.split('\t')
            if len(parts) < 3:
                continue
            row_type = parts[0]
            if row_type == "news" and len(parts) >= 6:
                news_data = {
                    'source': parts[2],
                    'pos': int(parts[3]),
                    'neg': int(parts[4]),
                    'total': int(parts[5])
                }
            elif row_type == "social" and len(parts) >= 5:
                social_data = {
                    'mentions': int(parts[2]),
                    'upvotes': int(parts[3]),
                    'velocity': int(parts[4])
                }
            elif row_type == "funding_history" and len(parts) >= 4:
                try:
                    ts = int(parts[1])
                    rate = float(parts[2])
                    funding_history.append((ts, rate))
                except:
                    pass
            elif row_type == "retail" and len(parts) >= 6:
                retail_data = {
                    'timestamp': int(parts[1]),   # FIXED: missing closing parenthesis
                    'long_pct': float(parts[2]),
                    'short_pct': float(parts[3]),
                    'ratio': float(parts[4])
                }
            elif row_type == "snapshot" and len(parts) >= 7:
                snapshot_data = {
                    'timestamp': int(parts[1]),
                    'price': float(parts[2]),
                    'oi_current': float(parts[3]),
                    'oi_prev': float(parts[4]),
                    'oi_curr_hist': float(parts[5]),
                    'oi_change_pct': float(parts[6])
                }

    if not snapshot_data:
        log_issue("ERROR", "No snapshot data found")
        return False

    # ---------- Compute derived features ----------
    if news_data and news_data['total'] > 0:
        news_score = (news_data['pos'] - news_data['neg']) / news_data['total']
        news_impact = max(-1.0, min(1.0, news_score))
    else:
        news_impact = 0.0

    long_ratio = retail_data.get('long_pct', 50.0) / 100.0 if retail_data else 0.5
    retail_bias_str, retail_contrarian = compute_retail_bias(long_ratio)

    funding_velocity = 0.0
    funding_rate = 0.0
    if len(funding_history) >= 2:
        latest_rate = funding_history[-1][1]
        prev_rate = funding_history[-2][1]
        funding_velocity = latest_rate - prev_rate
        funding_rate = latest_rate
    elif funding_history:
        funding_rate = funding_history[-1][1]

    funding_extreme_str, funding_vel_bias = compute_funding_extreme(funding_rate)
    if funding_velocity > 0.00005:
        funding_vel_bias = -1
    elif funding_velocity < -0.00005:
        funding_vel_bias = 1
    else:
        funding_vel_bias = 0

    oi_change = snapshot_data.get('oi_change_pct', 0.0)
    if oi_change > 2:
        oi_trend = "rising"
    elif oi_change < -2:
        oi_trend = "falling"
    else:
        oi_trend = "flat"

    price_change_pct = 0.0
    oi_price_state, oi_price_bias = compute_oi_price_state(price_change_pct, oi_trend)

    social_vel = social_data.get('velocity', 0)
    social_mania = social_vel > 1000

    sentiment_score = (retail_contrarian * 30) + (news_impact * 20) + (funding_vel_bias * 20) + (oi_price_bias * 30)
    sentiment_score = max(-100, min(100, sentiment_score))
    if social_mania and sentiment_score > 0:
        sentiment_score -= 15
    elif social_mania and sentiment_score < 0:
        sentiment_score += 10

    factors_agree = 0
    if (retail_contrarian == 1 and news_impact > 0.3) or (retail_contrarian == -1 and news_impact < -0.3):
        factors_agree += 1
    if (retail_contrarian == 1 and funding_vel_bias == 1) or (retail_contrarian == -1 and funding_vel_bias == -1):
        factors_agree += 1
    if (retail_contrarian == 1 and oi_price_bias == 1) or (retail_contrarian == -1 and oi_price_bias == -1):
        factors_agree += 1
    conviction = min(95, max(30, 50 + (factors_agree * 15)))

    # ---------- Write output .tmp_p TSV (raw data + derived) ----------
    tmp_p_path = os.path.join(FEATURES_BASE_DIR, f"{symbol.lower()}_sentiment.tmp_p")
    with open(tmp_p_path, "w") as out:
        out.write("# === Raw sentiment data ===\n")
        for raw_line in raw_lines:
            out.write("# " + raw_line + "\n")
        out.write("# === Derived features ===\n")
        header = [
            "timestamp", "sentiment_contrarian_score", "retail_contrarian_signal",
            "retail_bias_raw", "funding_extreme", "funding_vel_bias",
            "oi_price_state", "social_mania", "news_impact", "conviction", "social_velocity"
        ]
        out.write("\t".join(header) + "\n")
        ts = snapshot_data.get('timestamp', int(time.time() * 1000))
        row = [
            str(ts),
            str(sentiment_score),
            str(retail_contrarian),
            retail_bias_str,
            funding_extreme_str,
            str(funding_vel_bias),
            oi_price_state,
            "1" if social_mania else "0",
            f"{news_impact:.2f}",
            str(conviction),
            str(social_vel)
        ]
        out.write("\t".join(row) + "\n")

    # Input file is NOT deleted
    # if os.path.exists(tmp_x_path):
    #     os.remove(tmp_x_path)

    elapsed = time.time() - start_time
    print(f"[P09] Success ({elapsed:.1f}s) -> {os.path.basename(tmp_p_path)}")
    log_issue("INFO", f"Processing complete for {symbol} in {elapsed:.2f}s -> {tmp_p_path}")
    return True

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python P09_sentiment_contrarian.py SYMBOL")
        sys.exit(1)
    success = process_sentiment(sys.argv[1].upper())
    sys.exit(0 if success else 1)