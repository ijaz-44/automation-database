#!/usr/bin/env python3
# E03_tick_expert.py – Tick Flow High‑Probability Scenario Detector (Win‑rate focused)
# Reads raw tick data from X25, computes metrics using time‑window normalisation,
# recency weighting, dynamic thresholds, and outputs summary TSV (no JSON).

import os
import sys
import time
import math
from collections import deque

# ========================== CONFIGURATION ==========================
FEATURES_BASE_DIR = os.path.join("market_data", "binance", "symbols")
LOG_FILE = os.path.join(FEATURES_BASE_DIR, "E03_tick_expert.log")
LOG_MAX_SIZE = 5_000_000
LOOKBACK_SECONDS = 300   # 5 minutes window

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

# ========================== ANALYZE TICK FLOW (FIXED) ==========================
def analyze_tick_flow(data):
    """
    Args:
        data: dict with keys:
            - buy_volume: float (weighted)
            - sell_volume: float (weighted)
            - net_delta: float
            - total_volume: float
            - speed_score: float (trades per second)
            - avg_trade_size: float (weighted)
            - whale_delta: float (weighted, using percentile threshold)
            - delta_last_100: float (weighted)
            - burst_ratio: float (abs(delta_last_100) / volume_last_100)
            - price_change_pct: float
            - price_volatility: float (median absolute % change)
            - volatility_regime: str ('low', 'normal', 'high')
            - absorption_score: float (0-100)
    Returns:
        dict with bias, confidence, high_prob_scenario, reason, signals, net_score.
    """
    signals = []
    # Component scores – we will take max, not sum, to avoid inflation
    comp_scores = []

    # 1. Net delta dominance
    net = data.get('net_delta', 0)
    total = data.get('total_volume', 1)
    net_ratio = net / total if total > 0 else 0
    if net_ratio > 0.3:
        comp_scores.append(('bull', 30))
        signals.append(f"Strong buy dominance ({net_ratio*100:.1f}% of volume)")
    elif net_ratio > 0.1:
        comp_scores.append(('bull', 15))
        signals.append(f"Moderate buy dominance ({net_ratio*100:.1f}%)")
    elif net_ratio < -0.3:
        comp_scores.append(('bear', 30))
        signals.append(f"Strong sell dominance ({-net_ratio*100:.1f}%)")
    elif net_ratio < -0.1:
        comp_scores.append(('bear', 15))
        signals.append(f"Moderate sell dominance ({-net_ratio*100:.1f}%)")
    else:
        signals.append(f"Neutral net delta ({net_ratio*100:.1f}%)")

    # 2. Speed (normalised by typical speed for this symbol – we use log scaling)
    speed = data.get('speed_score', 0)
    # Speed normalised between 0 and 1 (capped at 20 tps)
    speed_norm = min(1.0, speed / 20.0) if speed > 0 else 0
    if speed > 10:
        comp_scores.append(('bull' if net > 0 else 'bear', 15 * speed_norm))
        signals.append(f"Very high trade speed ({speed:.1f} tps)")
    elif speed > 3:
        comp_scores.append(('bull' if net > 0 else 'bear', 8 * speed_norm))
        signals.append(f"Elevated speed ({speed:.1f} tps)")

    # 3. Whale delta
    whale_delta = data.get('whale_delta', 0)
    whale_ratio = abs(whale_delta) / total if total > 0 else 0
    if whale_delta > 0:
        score = min(20, int(whale_ratio * 60))
        comp_scores.append(('bull', score))
        signals.append(f"Bullish whale delta (+{whale_delta:.2f}, {whale_ratio*100:.1f}% of volume)")
    elif whale_delta < 0:
        score = min(20, int(whale_ratio * 60))
        comp_scores.append(('bear', score))
        signals.append(f"Bearish whale delta ({whale_delta:.2f})")

    # 4. Micro‑burst (weighted)
    burst_ratio = data.get('burst_ratio', 0)
    delta_last_100 = data.get('delta_last_100', 0)
    if burst_ratio > 0.5:
        if delta_last_100 > 0:
            comp_scores.append(('bull', 20))
            signals.append("Very strong recent buying burst")
        else:
            comp_scores.append(('bear', 20))
            signals.append("Very strong recent selling burst")
    elif burst_ratio > 0.2:
        if delta_last_100 > 0:
            comp_scores.append(('bull', 10))
            signals.append("Recent buying burst")
        else:
            comp_scores.append(('bear', 10))
            signals.append("Recent selling burst")

    # 5. Absorption detection (price response)
    absorption = data.get('absorption_score', 0)
    if absorption > 50:
        # Strong absorption – reduce the dominant side
        signals.append("Strong absorption detected – flow may be fake")
        # We'll reduce final confidence later

    # Volatility regime
    vol_regime = data.get('volatility_regime', 'normal')
    if vol_regime == 'low':
        signals.append("Low volatility regime – flow signals less reliable")
    elif vol_regime == 'high':
        signals.append("High volatility regime – flow amplified")

    # Compute net bias from component scores
    bullish_total = sum(s for direction, s in comp_scores if direction == 'bull')
    bearish_total = sum(s for direction, s in comp_scores if direction == 'bear')
    net_score = bullish_total - bearish_total
    net_score = max(-100, min(100, net_score))

    if net_score >= 30:
        bias = "bullish"
        base_conf = 60 + net_score // 2
    elif net_score <= -30:
        bias = "bearish"
        base_conf = 60 + abs(net_score) // 2
    else:
        bias = "neutral"
        base_conf = 50

    # Adjust confidence for absorption and volatility regime
    confidence = base_conf
    if absorption > 50:
        confidence = max(20, confidence - 25)
    if vol_regime == 'low':
        confidence = max(20, confidence - 10)
    elif vol_regime == 'high':
        confidence = min(95, confidence + 5)   # high vol can amplify real moves
    confidence = min(95, max(20, confidence))

    # High‑probability scenario only at 90+ and strong conviction
    high_prob = None
    if confidence >= 90 and bias != "neutral":
        high_prob = "UP" if bias == "bullish" else "DOWN"

    # FIXED: convert net_score to int before using +d format
    reason = f"Net score {int(net_score):+d}, confidence {confidence:.0f}%"
    return {
        "bias": bias,
        "confidence": confidence,
        "high_prob_scenario": high_prob,
        "probability_estimate": confidence,
        "reason": reason,
        "signals": signals,
        "net_score": net_score
    }

# ========================== TICK DATA LOADER (TIME‑WINDOW NORMALISED) ==========================
def load_tick_data_window(symbol, lookback_sec=LOOKBACK_SECONDS):
    """Load all trades from file, then keep only those within last lookback_sec seconds."""
    path = os.path.join(FEATURES_BASE_DIR, f"{symbol.lower()}_tick.tmp_x")
    if not os.path.exists(path):
        log_issue("ERROR", f"Tick file not found: {path}")
        return None
    trades = []
    try:
        with open(path, "r") as f:
            lines = f.readlines()
        if not lines:
            return None
        # Detect header
        first = lines[0].strip().lower()
        start_idx = 1 if ('timestamp' in first or 'time' in first) else 0
        for line in lines[start_idx:]:
            parts = line.strip().split('\t')
            if len(parts) < 4:
                continue
            try:
                ts = int(parts[0])
                price = float(parts[1])
                qty = float(parts[2])
                side_raw = parts[3].lower()
                is_buy = side_raw in ('buy', '1', 'true')
                trades.append({'ts': ts, 'price': price, 'qty': qty, 'is_buy': is_buy})
            except:
                continue
    except Exception as e:
        log_issue("ERROR", f"Failed to read tick file: {e}")
        return None
    if not trades:
        log_issue("WARNING", "No trade records found")
        return None
    trades.sort(key=lambda x: x['ts'])
    now = trades[-1]['ts']
    cutoff = now - lookback_sec * 1000
    filtered = [t for t in trades if t['ts'] >= cutoff]
    if len(filtered) < 10:
        log_issue("WARNING", f"Only {len(filtered)} trades in last {lookback_sec}s, insufficient")
        return None
    return filtered

def compute_metrics_from_trades(trades):
    """Compute all metrics with recency weighting (exponential decay, half‑life 30s)."""
    n = len(trades)
    if n < 5:
        return None
    now = trades[-1]['ts']
    # weights: half-life 30 seconds
    half_life = 30000  # ms
    alpha = math.exp(-1.0 / (half_life / 1000.0))  # decay per second
    weights = []
    for t in trades:
        age_sec = (now - t['ts']) / 1000.0
        w = alpha ** age_sec
        weights.append(w)
    total_weight = sum(weights)
    if total_weight <= 0:
        total_weight = 1.0

    # Weighted volumes
    buy_vol = sum(t['qty'] * w for t, w in zip(trades, weights) if t['is_buy'])
    sell_vol = sum(t['qty'] * w for t, w in zip(trades, weights) if not t['is_buy'])
    net_delta = buy_vol - sell_vol
    total_vol = buy_vol + sell_vol

    # Weighted average trade size
    avg_trade = sum(t['qty'] * w for t, w in zip(trades, weights)) / total_weight

    # Speed (trades per second)
    time_span = (now - trades[0]['ts']) / 1000.0
    speed = len(trades) / time_span if time_span > 0 else 0

    # Whale detection: use 95th percentile of trade sizes
    sizes = sorted([t['qty'] for t in trades])
    idx95 = int(0.95 * len(sizes))
    whale_threshold = sizes[idx95] if idx95 < len(sizes) else sizes[-1]
    whale_buy = sum(t['qty'] * w for t, w in zip(trades, weights) if t['is_buy'] and t['qty'] >= whale_threshold)
    whale_sell = sum(t['qty'] * w for t, w in zip(trades, weights) if not t['is_buy'] and t['qty'] >= whale_threshold)
    whale_delta = whale_buy - whale_sell

    # Last 100 trades (also weighted, but using same weighting scheme)
    last_100 = trades[-100:] if len(trades) >= 100 else trades
    w_last = [weights[-i] for i in range(1, len(last_100)+1)][::-1]  # align
    total_w_last = sum(w_last) if w_last else 1.0
    delta_last_100 = sum(t['qty'] * (1 if t['is_buy'] else -1) * w for t, w in zip(last_100, w_last))
    vol_last_100 = sum(t['qty'] * w for t, w in zip(last_100, w_last))
    burst_ratio = abs(delta_last_100) / vol_last_100 if vol_last_100 > 0 else 0

    # Price change and volatility
    first_price = trades[0]['price']
    last_price = trades[-1]['price']
    price_change_pct = (last_price - first_price) / first_price * 100 if first_price != 0 else 0
    # Price volatility: median absolute percentage change between consecutive trades
    price_changes = []
    for i in range(1, len(trades)):
        if trades[i-1]['price'] != 0:
            pct = abs((trades[i]['price'] - trades[i-1]['price']) / trades[i-1]['price'] * 100)
            price_changes.append(pct)
    if price_changes:
        price_changes.sort()
        med_idx = len(price_changes)//2
        price_volatility = price_changes[med_idx] if price_changes else 0.2
    else:
        price_volatility = 0.2
    # Absorption: if net delta is strong but price move is small relative to volatility
    if price_volatility > 0:
        move_ratio = abs(price_change_pct) / price_volatility
    else:
        move_ratio = 1.0
    absorption_score = 0
    if (net_delta > 0 and price_change_pct < 0.1 * price_volatility) or (net_delta < 0 and price_change_pct > -0.1 * price_volatility):
        absorption_score = min(100, 50 + abs(net_delta)/total_vol * 100 if total_vol>0 else 0)
    # Volatility regime
    if price_volatility < 0.1:
        vol_regime = 'low'
    elif price_volatility > 0.8:
        vol_regime = 'high'
    else:
        vol_regime = 'normal'

    return {
        "buy_volume": buy_vol,
        "sell_volume": sell_vol,
        "net_delta": net_delta,
        "total_volume": total_vol,
        "speed_score": speed,
        "avg_trade_size": avg_trade,
        "whale_delta": whale_delta,
        "delta_last_100": delta_last_100,
        "burst_ratio": burst_ratio,
        "price_change_pct": price_change_pct,
        "price_volatility": price_volatility,
        "absorption_score": absorption_score,
        "volatility_regime": vol_regime
    }

# ========================== MAIN EXPORT FUNCTION ==========================
def run_expert(symbol):
    log_issue("INFO", f"Starting E03 tick expert for {symbol}")
    trades = load_tick_data_window(symbol, LOOKBACK_SECONDS)
    if not trades:
        log_issue("ERROR", f"Insufficient tick data for {symbol} in last {LOOKBACK_SECONDS}s")
        return None
    metrics = compute_metrics_from_trades(trades)
    if metrics is None:
        log_issue("ERROR", "Failed to compute tick metrics")
        return None
    result = analyze_tick_flow(metrics)
    out_path = os.path.join(FEATURES_BASE_DIR, f"{symbol.lower()}_E03_tick.tsv")
    with open(out_path, "w") as f:
        header = ["timestamp", "bias", "confidence", "high_prob_scenario", "probability_estimate",
                  "reason", "signals", "net_score"]
        f.write("\t".join(header) + "\n")
        ts_now = int(time.time() * 1000)
        signals_str = " | ".join(result['signals'])
        row = [
            str(ts_now),
            result['bias'],
            str(result['confidence']),
            result['high_prob_scenario'] if result['high_prob_scenario'] else "",
            str(result['probability_estimate']),
            result['reason'],
            signals_str,
            str(result['net_score'])
        ]
        f.write("\t".join(row) + "\n")
    log_issue("INFO", f"Saved tick expert summary to {out_path}")
    return out_path

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python E03_tick_expert.py SYMBOL")
        sys.exit(1)
    success = run_expert(sys.argv[1].upper())
    sys.exit(0 if success else 1)