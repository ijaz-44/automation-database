#!/usr/bin/env python3
"""
P13_tick_flow.py – Process Raw Tick Data
- Reads {symbol}_tick.tmp_x (TSV from X25) – raw trades
- Computes: net delta, trade speed, whale delta, micro‑burst delta,
  aggression ratios, trade size skew, urgency score, flow divergence,
  momentum oscillator, confidence, directional bias
- Outputs TSV {symbol}_tick.tmp_p with raw data (commented) + derived features
- Logs to p13_tick_flow_issues.log (file only, minimal console)
- Input file is NOT deleted.
"""

import os
import sys
import time
import math

# ========== CONFIGURATION ==========
FEATURES_BASE_DIR = os.path.join("market_data", "binance", "symbols")
LOG_FILE = os.path.join(FEATURES_BASE_DIR, "p13_tick_flow_issues.log")
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
    # Print only errors to console; other levels go only to log file
    if level == "ERROR":
        print(line)
    rotate_log_if_needed()
    with open(LOG_FILE, "a") as f:
        f.write(line + "\n")

# ========== TICK PROCESSING ==========
def compute_tick_features(trades):
    if not trades or len(trades) < 50:
        return None, "Insufficient tick data"

    buy_vol = 0.0
    sell_vol = 0.0
    buy_trades = 0
    sell_trades = 0
    total_qty = 0.0
    timestamps = []

    for t in trades:
        qty = t['quantity']
        price = t['price']
        total_qty += qty
        timestamps.append(t['timestamp'])
        if t['isBuyerMaker']:
            sell_vol += qty
            sell_trades += 1
        else:
            buy_vol += qty
            buy_trades += 1

    net_delta = buy_vol - sell_vol
    total_vol = buy_vol + sell_vol
    if total_vol == 0:
        return None, "Zero total volume"

    avg_trade_size = total_qty / len(trades)

    time_span_ms = timestamps[-1] - timestamps[0]
    speed_score = len(trades) / (time_span_ms / 1000.0) if time_span_ms > 0 else 0.0

    whale_threshold = avg_trade_size * 5.0
    whale_buy = 0.0
    whale_sell = 0.0
    for t in trades:
        qty = t['quantity']
        if qty >= whale_threshold:
            if t['isBuyerMaker']:
                whale_sell += qty
            else:
                whale_buy += qty
    whale_delta = whale_buy - whale_sell

    last_100 = trades[-100:] if len(trades) >= 100 else trades
    buy_last = 0.0
    sell_last = 0.0
    for t in last_100:
        qty = t['quantity']
        if t['isBuyerMaker']:
            sell_last += qty
        else:
            buy_last += qty
    delta_last_100 = buy_last - sell_last
    total_last_100 = buy_last + sell_last
    micro_burst_ratio = delta_last_100 / total_last_100 if total_last_100 > 0 else 0
    micro_burst_score = min(100, max(0, 50 + int(micro_burst_ratio * 100)))

    buyer_agg = buy_vol / total_vol
    seller_agg = sell_vol / total_vol

    avg_buy_size = buy_vol / buy_trades if buy_trades > 0 else 0.0
    avg_sell_size = sell_vol / sell_trades if sell_trades > 0 else 0.0
    if avg_sell_size > 0:
        trade_skew = avg_buy_size / avg_sell_size
    else:
        trade_skew = 100.0 if avg_buy_size > 0 else 1.0
    trade_skew = max(0.2, min(5.0, trade_skew))

    cumul_ratio = net_delta / total_vol
    recent_ratio = delta_last_100 / total_last_100 if total_last_100 > 0 else 0
    flow_div = 1 if (cumul_ratio > 0.1 and recent_ratio < -0.1) or (cumul_ratio < -0.1 and recent_ratio > 0.1) else 0

    speed_norm = min(1.0, speed_score / 10.0)
    urgency = (max(buyer_agg, seller_agg) + speed_norm) / 2.0

    momentum_osc = (delta_last_100 - net_delta) / total_vol if total_vol > 0 else 0
    momentum_osc = max(-1.0, min(1.0, momentum_osc))

    conf = 50
    if abs(cumul_ratio) > 0.3:
        conf += 15
    if abs(recent_ratio) > 0.3:
        conf += 10
    if abs(whale_delta) / total_vol > 0.2:
        conf += 10
    if speed_score > 5:
        conf += 10
    if flow_div:
        conf -= 20
    conf = max(0, min(100, conf))

    if net_delta > 0 and cumul_ratio > 0.1:
        bias = "bullish"
    elif net_delta < 0 and cumul_ratio < -0.1:
        bias = "bearish"
    else:
        bias = "neutral"

    features = {
        "bias": bias,
        "net_delta": round(net_delta, 2),
        "speed_score": round(speed_score, 2),
        "whale_delta": round(whale_delta, 2),
        "delta_last_100": round(delta_last_100, 2),
        "micro_burst_score": micro_burst_score,
        "buyer_aggression_ratio": round(buyer_agg, 4),
        "seller_aggression_ratio": round(seller_agg, 4),
        "trade_size_skew": round(trade_skew, 3),
        "urgency_score": round(urgency, 3),
        "flow_divergence": flow_div,
        "momentum_oscillator": round(momentum_osc, 4),
        "confidence": conf,
        "total_volume": round(total_vol, 2),
        "avg_trade_size": round(avg_trade_size, 4)
    }
    return features, None

# ========== MAIN PROCESSING ==========
def process_tick(symbol):
    print(f"[P13] Starting tick processing for {symbol}")
    start_time = time.time()
    log_issue("INFO", f"Starting tick processing for {symbol}")

    tmp_x_path = os.path.join(FEATURES_BASE_DIR, f"{symbol.lower()}_tick.tmp_x")
    if not os.path.exists(tmp_x_path):
        log_issue("ERROR", f"Input file not found: {tmp_x_path}")
        return False

    # ---------- Read raw data and store raw lines ----------
    raw_lines = []
    trades = []
    with open(tmp_x_path, "r") as f:
        header = f.readline().strip()
        raw_lines.append(header)  # header line
        for line in f:
            line = line.strip()
            if not line:
                continue
            raw_lines.append(line)
            parts = line.split('\t')
            if len(parts) < 5:
                continue
            try:
                trade = {
                    "timestamp": int(parts[0]),
                    "price": float(parts[1]),
                    "quantity": float(parts[2]),
                    "quoteQty": float(parts[3]),
                    "isBuyerMaker": parts[4].lower() == 'true'
                }
                trades.append(trade)
            except Exception as e:
                log_issue("WARNING", f"Skipping malformed row: {e}", row=line[:50])

    if len(trades) < 50:
        log_issue("ERROR", f"Insufficient trades: {len(trades)} (need at least 50)")
        return False

    # ---------- Compute features ----------
    features, err = compute_tick_features(trades)
    if err:
        log_issue("ERROR", f"Feature computation failed: {err}")
        return False

    # ---------- Write output .tmp_p TSV (raw data + derived) ----------
    tmp_p_path = os.path.join(FEATURES_BASE_DIR, f"{symbol.lower()}_tick.tmp_p")
    with open(tmp_p_path, "w") as out:
        out.write("# === Raw tick data ===\n")
        for raw_line in raw_lines:
            out.write("# " + raw_line + "\n")
        out.write("# === Derived features ===\n")
        header = [
            "timestamp", "bias", "net_delta", "speed_score", "whale_delta",
            "delta_last_100", "micro_burst_score", "buyer_aggression_ratio",
            "seller_aggression_ratio", "trade_size_skew", "urgency_score",
            "flow_divergence", "momentum_oscillator", "confidence",
            "total_volume", "avg_trade_size"
        ]
        out.write("\t".join(header) + "\n")
        ts = int(time.time() * 1000)
        row = [
            str(ts),
            features["bias"],
            str(features["net_delta"]),
            str(features["speed_score"]),
            str(features["whale_delta"]),
            str(features["delta_last_100"]),
            str(features["micro_burst_score"]),
            str(features["buyer_aggression_ratio"]),
            str(features["seller_aggression_ratio"]),
            str(features["trade_size_skew"]),
            str(features["urgency_score"]),
            str(features["flow_divergence"]),
            str(features["momentum_oscillator"]),
            str(features["confidence"]),
            str(features["total_volume"]),
            str(features["avg_trade_size"])
        ]
        out.write("\t".join(row) + "\n")

    # Input file is NOT deleted
    # if os.path.exists(tmp_x_path):
    #     os.remove(tmp_x_path)

    elapsed = time.time() - start_time
    print(f"[P13] Success ({elapsed:.1f}s) -> {os.path.basename(tmp_p_path)}")
    log_issue("INFO", f"Processing complete for {symbol} in {elapsed:.2f}s -> {tmp_p_path}")
    return True

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python P13_tick_flow.py SYMBOL")
        sys.exit(1)
    success = process_tick(sys.argv[1].upper())
    sys.exit(0 if success else 1)