#!/usr/bin/env python3
"""
P03_orderbook_imbalance.py – Order Book Feature Processing
- Reads raw bids/asks from X05 (.tmp_x)
- Computes: imbalance, spread, micro price, depth slopes, iceberg probability, spoofing probability, cumulative depth ratio, hidden liquidity estimate, etc.
- Outputs TSV (.tmp_p) with per‑snapshot features
- Logs issues to p03_orderbook_issues.log (file only, minimal console)
- Input file is NOT deleted.
"""

import os
import sys
import time
import math
from collections import defaultdict

# ========== CONFIGURATION ==========
FEATURES_BASE_DIR = os.path.join("market_data", "binance", "symbols")
LOG_FILE = os.path.join(FEATURES_BASE_DIR, "p03_orderbook_issues.log")
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
    # Print only errors and the start/end messages we'll call manually
    if level == "ERROR" or msg.startswith("[P03] Starting") or msg.startswith("[P03] Success"):
        print(line)
    rotate_log_if_needed()
    with open(LOG_FILE, "a") as f:
        f.write(line + "\n")

# ========== HELPER FUNCTIONS ==========
def linear_slope(prices, volumes):
    n = len(prices)
    if n < 2:
        return 0.0
    mean_p = sum(prices) / n
    mean_v = sum(volumes) / n
    num = sum((p - mean_p) * (v - mean_v) for p, v in zip(prices, volumes))
    den = sum((p - mean_p) ** 2 for p in prices)
    return num / den if den != 0 else 0.0

def detect_liquidity_gaps(levels, mid_price, min_gap_pct=0.0005):
    gaps = []
    for i in range(len(levels)-1):
        price_gap = abs(levels[i+1][0] - levels[i][0])
        pct_gap = price_gap / mid_price if mid_price != 0 else 0
        if pct_gap > min_gap_pct and (levels[i][1] < 0.01 or levels[i+1][1] < 0.01):
            gaps.append({
                "from": levels[i][0],
                "to": levels[i+1][0],
                "gap_pct": round(pct_gap * 100, 4)
            })
    return gaps

def detect_iceberg_candidates(levels, threshold_multiplier=2.0, top_n=15):
    if len(levels) < 5:
        return []
    top_qtys = [q for _, q in levels[:5]]
    avg_qty = sum(top_qtys) / len(top_qtys) if top_qtys else 0
    threshold = avg_qty * threshold_multiplier
    candidates = []
    for i, (price, qty) in enumerate(levels[:top_n]):
        if qty > threshold and (i+1 < len(levels) and qty > levels[i+1][1] * 1.5):
            candidates.append({"price": price, "qty": qty})
    return candidates

# ========== MAIN PROCESSING ==========
def process_depth(symbol):
    log_issue("INFO", f"[P03] Starting order book processing for {symbol}")
    start_time = time.time()

    tmp_x_path = os.path.join(FEATURES_BASE_DIR, f"{symbol.lower()}_depth.tmp_x")
    if not os.path.exists(tmp_x_path):
        log_issue("ERROR", f"Input file not found: {tmp_x_path}")
        return False

    # ---------- Read raw bids/asks ----------
    bids = []
    asks = []
    snapshot_ts = 0

    with open(tmp_x_path, "r") as f:
        header = f.readline().strip()
        for line in f:
            line = line.strip()
            if not line:
                continue
            parts = line.split('\t')
            if len(parts) < 4:
                continue
            side = parts[0]
            price = float(parts[1])
            qty = float(parts[2])
            ts = int(parts[3])
            snapshot_ts = ts
            if side == "bid":
                bids.append([price, qty])
            elif side == "ask":
                asks.append([price, qty])

    if not bids or not asks:
        log_issue("ERROR", "Missing bids or asks in file")
        return False

    bids.sort(key=lambda x: x[0], reverse=True)
    asks.sort(key=lambda x: x[0])

    mid_price = (bids[0][0] + asks[0][0]) / 2

    # ---------- Compute features ----------
    top_n = 10
    bid_vol_top = sum(q for _, q in bids[:top_n])
    ask_vol_top = sum(q for _, q in asks[:top_n])
    total_vol = bid_vol_top + ask_vol_top
    imbalance = (bid_vol_top - ask_vol_top) / total_vol if total_vol > 0 else 0.0

    best_bid = bids[0][0]
    best_ask = asks[0][0]
    spread = best_ask - best_bid
    spread_pct = (spread / mid_price) * 100 if mid_price != 0 else 0.0

    bid_qty = bids[0][1]
    ask_qty = asks[0][1]
    total = bid_qty + ask_qty
    if total > 0:
        micro_price = (best_bid * ask_qty + best_ask * bid_qty) / total
    else:
        micro_price = (best_bid + best_ask) / 2

    def depth_slope(levels):
        if len(levels) < 3:
            return 0.0
        levels = levels[:15]
        prices = [p for p, _ in levels]
        volumes = [v for _, v in levels]
        return linear_slope(prices, volumes)

    bid_slope = depth_slope(bids)
    ask_slope = depth_slope(asks)

    iceberg_bids = detect_iceberg_candidates(bids)
    iceberg_asks = detect_iceberg_candidates(asks)
    iceberg_total = len(iceberg_bids) + len(iceberg_asks)
    iceberg_prob = min(0.95, iceberg_total * 0.2) if iceberg_total > 0 else 0.0
    iceberg_prob = min(1.0, iceberg_prob)

    spoofing_prob = 0.0
    if abs(imbalance) > 0.6 and spread_pct > 0.05:
        spoofing_prob = 0.6
    if iceberg_prob > 0.5:
        spoofing_prob = max(spoofing_prob, 0.4)
    gaps = detect_liquidity_gaps(bids[:50], mid_price) + detect_liquidity_gaps(asks[:50], mid_price)
    for gap in gaps:
        if gap['gap_pct'] > 0.3:
            spoofing_prob = min(1.0, spoofing_prob + 0.2)
            break

    top_n = 5
    bid_vol = sum(q for _, q in bids[:top_n])
    ask_vol = sum(q for _, q in asks[:top_n])
    if bid_vol > 0 and ask_vol > 0:
        bid_price_avg = sum(p*q for p,q in bids[:top_n]) / bid_vol
        ask_price_avg = sum(p*q for p,q in asks[:top_n]) / ask_vol
        weighted_mid = (bid_price_avg * ask_vol + ask_price_avg * bid_vol) / (bid_vol + ask_vol)
    else:
        weighted_mid = mid_price

    cum_bid_20 = sum(q for _, q in bids[:20])
    cum_ask_20 = sum(q for _, q in asks[:20])
    cum_depth_ratio = cum_bid_20 / cum_ask_20 if cum_ask_20 > 0 else 1.0

    hidden_liquidity = sum(item['qty'] for item in iceberg_bids) + sum(item['qty'] for item in iceberg_asks)

    order_arrival_intensity = 0.0
    cancellation_ratio = 0.0

    output_row = {
        "timestamp": snapshot_ts,
        "mid_price": mid_price,
        "order_book_imbalance": round(imbalance, 4),
        "spread_pct": round(spread_pct, 4),
        "micro_price": round(micro_price, 2),
        "depth_slope_bid": round(bid_slope, 4),
        "depth_slope_ask": round(ask_slope, 4),
        "iceberg_probability": round(iceberg_prob, 4),
        "spoofing_probability": round(spoofing_prob, 4),
        "weighted_mid_price": round(weighted_mid, 2),
        "cumulative_depth_ratio": round(cum_depth_ratio, 4),
        "order_arrival_intensity": order_arrival_intensity,
        "cancellation_ratio": cancellation_ratio,
        "hidden_liquidity_estimate": round(hidden_liquidity, 2)
    }

    tmp_p_path = os.path.join(FEATURES_BASE_DIR, f"{symbol.lower()}_depth.tmp_p")
    with open(tmp_p_path, "w") as out:
        header = [
            "timestamp", "mid_price", "order_book_imbalance", "spread_pct",
            "micro_price", "depth_slope_bid", "depth_slope_ask",
            "iceberg_probability", "spoofing_probability", "weighted_mid_price",
            "cumulative_depth_ratio", "order_arrival_intensity", "cancellation_ratio",
            "hidden_liquidity_estimate"
        ]
        out.write("\t".join(header) + "\n")
        row = [
            str(output_row["timestamp"]),
            f"{output_row['mid_price']:.2f}",
            f"{output_row['order_book_imbalance']:.4f}",
            f"{output_row['spread_pct']:.4f}",
            f"{output_row['micro_price']:.2f}",
            f"{output_row['depth_slope_bid']:.4f}",
            f"{output_row['depth_slope_ask']:.4f}",
            f"{output_row['iceberg_probability']:.4f}",
            f"{output_row['spoofing_probability']:.4f}",
            f"{output_row['weighted_mid_price']:.2f}",
            f"{output_row['cumulative_depth_ratio']:.4f}",
            f"{output_row['order_arrival_intensity']:.2f}",
            f"{output_row['cancellation_ratio']:.2f}",
            f"{output_row['hidden_liquidity_estimate']:.2f}"
        ]
        out.write("\t".join(row) + "\n")

    # Input file is NOT deleted
    # if os.path.exists(tmp_x_path):
    #     os.remove(tmp_x_path)

    elapsed = time.time() - start_time
    log_issue("INFO", f"[P03] Success ({elapsed:.1f}s) -> {os.path.basename(tmp_p_path)}")
    return True

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python P03_orderbook_imbalance.py SYMBOL")
        sys.exit(1)
    success = process_depth(sys.argv[1].upper())
    sys.exit(0 if success else 1)