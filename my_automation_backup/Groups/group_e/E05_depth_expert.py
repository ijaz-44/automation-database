#!/usr/bin/env python3
# E05_depth_expert.py – Depth High‑Probability Scenario Detector (≥90% setups)
# Reads raw order book from X05 .tmp_x, computes depth metrics, and outputs TSV summary.

import os
import sys
import time
import math
import json
from collections import defaultdict

# ========================== CONFIGURATION ==========================
FEATURES_BASE_DIR = os.path.join("market_data", "binance", "symbols")
LOG_FILE = os.path.join(FEATURES_BASE_DIR, "E05_depth_expert.log")
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

# ========================== ORIGINAL analyze_depth (UNCHANGED) ==========================
def analyze_depth(depth_data):
    """
    Args:
        depth_data: dict with keys:
            imbalance: float (-1..1)
            current_mid_price: float
            bids_top: list of [price, qty] (top 25)
            asks_top: list of [price, qty] (top 25)
            bids_buckets: dict {bucket_center: volume}
            asks_buckets: dict {bucket_center: volume}
            bids_percentiles: list of {target_pct, price}
            asks_percentiles: list of {target_pct, price}
            bids_tail_stats: dict with total_volume, vwap, std_dev, etc.
            asks_tail_stats: dict
            liquidity_gaps_json: list of gap dicts
            iceberg_bids_json: list of detected icebergs
            iceberg_asks_json: list
    Returns:
        dict with:
            bias: 'bullish'/'bearish'/'neutral'
            confidence: int 0-100 (≥90 indicates high prob)
            high_prob_scenario: 'UP'/'DOWN'/None
            reason: str
            signals: list of str
            net_score: int
    """
    signals = []
    bullish_score = 0
    bearish_score = 0

    # 1. Imbalance ratio (most direct signal)
    imbalance = depth_data.get('imbalance', 0)
    if imbalance > 0.4:
        bullish_score += 30
        signals.append(f"Strong buy imbalance ({imbalance:.2f})")
    elif imbalance > 0.2:
        bullish_score += 15
        signals.append(f"Moderate buy imbalance ({imbalance:.2f})")
    elif imbalance < -0.4:
        bearish_score += 30
        signals.append(f"Strong sell imbalance ({imbalance:.2f})")
    elif imbalance < -0.2:
        bearish_score += 15
        signals.append(f"Moderate sell imbalance ({imbalance:.2f})")

    # 2. Current price relative to volume percentiles (CDF)
    bids_pct = depth_data.get('bids_percentiles', [])
    asks_pct = depth_data.get('asks_percentiles', [])
    mid = depth_data.get('current_mid_price', 0)
    
    bid_price_at_50pct = None
    ask_price_at_50pct = None
    for p in bids_pct:
        if p['target_pct'] == 50:
            bid_price_at_50pct = p['price']
            break
    for p in asks_pct:
        if p['target_pct'] == 50:
            ask_price_at_50pct = p['price']
            break
    
    if bid_price_at_50pct and ask_price_at_50pct:
        if mid < bid_price_at_50pct:
            bullish_score += 20
            signals.append("Price below bid volume concentration → support")
        elif mid > ask_price_at_50pct:
            bearish_score += 20
            signals.append("Price above ask volume concentration → resistance")
        else:
            signals.append("Price inside fair value range")

    # 3. VWAP and deviation
    bids_vwap = depth_data.get('bids_tail_stats', {}).get('vwap', 0)
    asks_vwap = depth_data.get('asks_tail_stats', {}).get('vwap', 0)
    if bids_vwap and asks_vwap:
        if mid < bids_vwap:
            bullish_score += 10
            signals.append("Price below bid VWAP → cheap")
        elif mid > asks_vwap:
            bearish_score += 10
            signals.append("Price above ask VWAP → expensive")

    # 4. Liquidity gaps – large gaps can act as vacuum or barriers
    gaps = depth_data.get('liquidity_gaps_json', [])
    large_gaps = [g for g in gaps if g.get('gap_pct', 0) > 0.5]  # >0.5% gap
    gap_above = any(g.get('from_price', 0) > mid for g in large_gaps)
    gap_below = any(g.get('to_price', 0) < mid for g in large_gaps)
    if gap_above and not gap_below:
        bearish_score += 15
        signals.append("Liquidity gap above → resistance")
    elif gap_below and not gap_above:
        bullish_score += 15
        signals.append("Liquidity gap below → support")
    elif gap_above and gap_below:
        signals.append("Liquidity gaps both sides → pending breakout")

    # 5. Iceberg orders – hidden large orders indicate accumulation/distribution
    iceberg_bids = depth_data.get('iceberg_bids_json', [])
    iceberg_asks = depth_data.get('iceberg_asks_json', [])
    if iceberg_bids:
        bullish_score += 20
        signals.append(f"Detected {len(iceberg_bids)} buy iceberg orders")
    if iceberg_asks:
        bearish_score += 20
        signals.append(f"Detected {len(iceberg_asks)} sell iceberg orders")

    # 6. Volume bucket concentration (no direct score)
    # 7. Spread
    wavg_bid = depth_data.get('wavg_bid', 0)
    wavg_ask = depth_data.get('wavg_ask', 0)
    if wavg_bid and wavg_ask:
        spread_pct = (wavg_ask - wavg_bid) / mid * 100 if mid else 0
        if spread_pct < 0.02:
            signals.append("Very tight spread → high liquidity")
            bullish_score += 5

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

    high_prob_scenario = None
    if confidence >= 90:
        high_prob_scenario = "UP" if bias == "bullish" else "DOWN" if bias == "bearish" else None

    reason = f"Net score {net:+d}, imbalance {imbalance:.2f}" + (f", {len(iceberg_bids)} icebergs" if iceberg_bids or iceberg_asks else "")
    return {
        "bias": bias,
        "confidence": confidence,
        "high_prob_scenario": high_prob_scenario,
        "probability_estimate": confidence,
        "reason": reason,
        "signals": signals,
        "net_score": net
    }

# ========================== DEPTH DATA LOADING & METRICS COMPUTATION ==========================
def load_depth_data(symbol):
    """Read raw order book from X05 .tmp_x file and compute all required depth metrics."""
    path = os.path.join(FEATURES_BASE_DIR, f"{symbol.lower()}_depth.tmp_x")
    if not os.path.exists(path):
        log_issue("ERROR", f"Depth file not found: {path}")
        return None
    bids = []
    asks = []
    try:
        with open(path, "r") as f:
            lines = f.readlines()
        # header line: "side\tprice\tquantity\ttimestamp"
        for line in lines[1:]:
            parts = line.strip().split('\t')
            if len(parts) < 4:
                continue
            side = parts[0]
            price = float(parts[1])
            qty = float(parts[2])
            # timestamp not needed per level
            if side == "bid":
                bids.append((price, qty))
            elif side == "ask":
                asks.append((price, qty))
    except Exception as e:
        log_issue("ERROR", f"Failed to read depth file: {e}")
        return None

    if not bids or not asks:
        log_issue("ERROR", "No bid/ask data found")
        return None

    # sort bids descending (highest price first), asks ascending
    bids.sort(key=lambda x: -x[0])
    asks.sort(key=lambda x: x[0])

    # Keep top 25 for bids and asks
    bids_top = bids[:25]
    asks_top = asks[:25]

    # Compute current mid price (best bid + best ask)/2
    best_bid = bids[0][0]
    best_ask = asks[0][0]
    mid = (best_bid + best_ask) / 2

    # Compute total bid and ask volume
    total_bid_vol = sum(q for _, q in bids)
    total_ask_vol = sum(q for _, q in asks)
    imbalance = (total_bid_vol - total_ask_vol) / (total_bid_vol + total_ask_vol) if (total_bid_vol + total_ask_vol) > 0 else 0

    # Bucket volumes (0.1% price buckets)
    bucket_pct = 0.001  # 0.1%
    bids_buckets = defaultdict(float)
    asks_buckets = defaultdict(float)
    for price, qty in bids:
        bucket = round(price / bucket_pct) * bucket_pct
        bids_buckets[bucket] += qty
    for price, qty in asks:
        bucket = round(price / bucket_pct) * bucket_pct
        asks_buckets[bucket] += qty

    # Percentiles (volume-weighted price levels)
    # For bids: cumulative volume from best bid downward
    # For asks: cumulative volume from best ask upward
    def compute_percentiles(levels, is_bid):
        # levels: list of (price, qty) sorted appropriately (bids descending, asks ascending)
        total_vol = sum(q for _, q in levels)
        if total_vol == 0:
            return []
        cum = 0
        percentiles = []
        targets = [10, 20, 30, 40, 50, 60, 70, 80, 90]
        idx = 0
        for price, qty in levels:
            cum += qty
            pct = (cum / total_vol) * 100
            while idx < len(targets) and pct >= targets[idx]:
                percentiles.append({"target_pct": targets[idx], "price": price})
                idx += 1
            if idx >= len(targets):
                break
        return percentiles

    bids_percentiles = compute_percentiles(bids, is_bid=True)
    asks_percentiles = compute_percentiles(asks, is_bid=False)

    # Compute VWAP and std dev for bids and asks (tail stats)
    def tail_stats(levels):
        if not levels:
            return {}
        total_vol = sum(q for _, q in levels)
        if total_vol == 0:
            return {}
        vwap = sum(p * q for p, q in levels) / total_vol
        # weighted std dev
        variance = sum(((p - vwap) ** 2) * q for p, q in levels) / total_vol
        std_dev = math.sqrt(variance)
        return {"total_volume": total_vol, "vwap": vwap, "std_dev": std_dev}

    bids_tail_stats = tail_stats(bids)
    asks_tail_stats = tail_stats(asks)

    # Detect liquidity gaps (large gaps between consecutive price levels)
    # For bids: gaps between consecutive bid prices (price difference)
    # For asks: gaps between consecutive ask prices
    def find_gaps(levels, is_bid):
        if len(levels) < 2:
            return []
        gaps = []
        for i in range(len(levels)-1):
            if is_bid:
                # bids sorted descending, so next is lower price
                price_diff = levels[i][0] - levels[i+1][0]
            else:
                price_diff = levels[i+1][0] - levels[i][0]
            gap_pct = price_diff / mid * 100 if mid else 0
            if gap_pct > 0.2:  # >0.2% gap
                gaps.append({
                    "gap_pct": gap_pct,
                    "from_price": levels[i][0],
                    "to_price": levels[i+1][0]
                })
        return gaps

    gaps = []
    gaps.extend(find_gaps(bids, is_bid=True))
    gaps.extend(find_gaps(asks, is_bid=False))

    # Iceberg detection: look for repeated same price with alternating large/small quantities? For simplicity, skip.
    iceberg_bids = []
    iceberg_asks = []

    # Weighted average bid/ask (using top 10 levels)
    wavg_bid = 0
    wavg_ask = 0
    if bids_top:
        vol_sum = sum(q for _, q in bids_top)
        if vol_sum > 0:
            wavg_bid = sum(p * q for p, q in bids_top) / vol_sum
    if asks_top:
        vol_sum = sum(q for _, q in asks_top)
        if vol_sum > 0:
            wavg_ask = sum(p * q for p, q in asks_top) / vol_sum

    depth_data = {
        "imbalance": imbalance,
        "current_mid_price": mid,
        "bids_top": bids_top,
        "asks_top": asks_top,
        "bids_buckets": {k: v for k, v in bids_buckets.items()},
        "asks_buckets": {k: v for k, v in asks_buckets.items()},
        "bids_percentiles": bids_percentiles,
        "asks_percentiles": asks_percentiles,
        "bids_tail_stats": bids_tail_stats,
        "asks_tail_stats": asks_tail_stats,
        "liquidity_gaps_json": gaps,
        "iceberg_bids_json": iceberg_bids,
        "iceberg_asks_json": iceberg_asks,
        "wavg_bid": wavg_bid,
        "wavg_ask": wavg_ask
    }
    return depth_data

# ========================== MAIN EXPERT FUNCTION ==========================
def run_expert(symbol):
    log_issue("INFO", f"Starting E05 depth expert for {symbol}")
    depth_data = load_depth_data(symbol)
    if not depth_data:
        log_issue("ERROR", "Failed to load depth data")
        return None
    result = analyze_depth(depth_data)
    # Write output TSV
    out_path = os.path.join(FEATURES_BASE_DIR, f"{symbol.lower()}_E05_depth.tsv")
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
    log_issue("INFO", f"Saved depth expert summary to {out_path}")
    return out_path

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python E05_depth_expert.py SYMBOL")
        sys.exit(1)
    success = run_expert(sys.argv[1].upper())
    sys.exit(0 if success else 1)