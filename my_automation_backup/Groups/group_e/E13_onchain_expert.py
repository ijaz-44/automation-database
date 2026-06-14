#!/usr/bin/env python3
# E13_onchain_expert.py – On‑Chain High‑Probability Scenario Detector (≥90% setups)
# Reads raw on‑chain data from X23 .tmp_x, computes metrics, outputs TSV summary.

import os
import sys
import time
import json
from collections import defaultdict

# ========================== CONFIGURATION ==========================
FEATURES_BASE_DIR = os.path.join("market_data", "binance", "symbols")
LOG_FILE = os.path.join(FEATURES_BASE_DIR, "E13_onchain_expert.log")
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

# ========================== ORIGINAL analyze_onchain (UNCHANGED) ==========================
def analyze_onchain(data):
    """
    Args:
        data: dict with keys:
            - usdt_netflow: float (USD, positive = inflow)
            - whale_ratio: float (long/short position ratio among whales, typical >1.2 bullish)
            - taker_ratio: float (taker buy/sell ratio, >1.2 bullish)
            - funding_rate: float (positive = longs pay shorts)
            - oi: float (open interest)
            - depth_imbalance: float (-1..1, positive = buy pressure)
            - stablecoin_bullish: bool (True if net inflow positive)
            - liquidations: list of liquidation events (if count >5, increase confidence)
            - exchange_netflow: list of dicts with 'netflow' (optional, latest value)
            - whale_transactions_count: int (number of whale alerts)
            - atr: float (optional, for target)
            - current_price: float (optional)
    Returns:
        dict with bias, confidence, high_prob_scenario, probability_estimate, reason, signals, net_score.
    """
    signals = []
    bullish_score = 0
    bearish_score = 0

    netflow = data.get('usdt_netflow', 0)
    if netflow > 200_000_000:
        bullish_score += 35
        signals.append(f"Strong USDT inflow (+{netflow/1e6:.0f}M) → bullish")
    elif netflow > 50_000_000:
        bullish_score += 20
        signals.append(f"Moderate USDT inflow (+{netflow/1e6:.0f}M) → bullish")
    elif netflow < -200_000_000:
        bearish_score += 35
        signals.append(f"Strong USDT outflow ({netflow/1e6:.0f}M) → bearish")
    elif netflow < -50_000_000:
        bearish_score += 20
        signals.append(f"Moderate USDT outflow ({netflow/1e6:.0f}M) → bearish")
    else:
        signals.append(f"USDT netflow neutral ({netflow/1e6:.0f}M)")

    whale_ratio = data.get('whale_ratio', 1.0)
    if whale_ratio > 1.5:
        bullish_score += 25
        signals.append(f"Very high whale long ratio ({whale_ratio:.2f}) → bullish")
    elif whale_ratio > 1.2:
        bullish_score += 12
        signals.append(f"High whale long ratio ({whale_ratio:.2f})")
    elif whale_ratio < 0.7:
        bearish_score += 25
        signals.append(f"Very low whale long ratio ({whale_ratio:.2f}) → bearish")
    elif whale_ratio < 0.85:
        bearish_score += 12
        signals.append(f"Low whale long ratio ({whale_ratio:.2f})")

    taker_ratio = data.get('taker_ratio', 1.0)
    if taker_ratio > 1.4:
        bullish_score += 20
        signals.append(f"Extreme taker buy ratio ({taker_ratio:.2f}) → aggressive buying")
    elif taker_ratio > 1.2:
        bullish_score += 10
        signals.append(f"High taker buy ratio ({taker_ratio:.2f})")
    elif taker_ratio < 0.6:
        bearish_score += 20
        signals.append(f"Extreme taker sell ratio ({taker_ratio:.2f}) → aggressive selling")
    elif taker_ratio < 0.8:
        bearish_score += 10
        signals.append(f"Low taker buy ratio ({taker_ratio:.2f})")

    funding = data.get('funding_rate', 0)
    if funding > 0.0003:
        bearish_score += 20
        signals.append(f"Very high funding ({funding*100:.3f}%) → overbought, bearish")
    elif funding > 0.0001:
        bearish_score += 10
        signals.append(f"High funding ({funding*100:.3f}%)")
    elif funding < -0.0003:
        bullish_score += 20
        signals.append(f"Very negative funding ({funding*100:.3f}%) → oversold, bullish")
    elif funding < -0.0001:
        bullish_score += 10
        signals.append(f"Negative funding ({funding*100:.3f}%)")

    depth = data.get('depth_imbalance', 0)
    if depth > 0.4:
        bullish_score += 15
        signals.append(f"Strong buy depth imbalance ({depth:.2f})")
    elif depth > 0.2:
        bullish_score += 7
        signals.append(f"Moderate buy depth ({depth:.2f})")
    elif depth < -0.4:
        bearish_score += 15
        signals.append(f"Strong sell depth imbalance ({depth:.2f})")
    elif depth < -0.2:
        bearish_score += 7
        signals.append(f"Moderate sell depth ({depth:.2f})")

    exchange_netflow = data.get('exchange_netflow', [])
    if exchange_netflow:
        latest = exchange_netflow[0]['netflow'] if isinstance(exchange_netflow[0], dict) else 0
        if latest > 0:
            bearish_score += 15
            signals.append(f"Positive exchange netflow ({latest:.0f}) → coins in, bearish")
        elif latest < 0:
            bullish_score += 15
            signals.append(f"Negative exchange netflow ({latest:.0f}) → coins out, bullish")

    whale_tx_count = data.get('whale_transactions_count', 0)
    if whale_tx_count > 50:
        signals.append(f"Very high whale activity ({whale_tx_count} transactions)")
        if bullish_score > bearish_score:
            bullish_score += 10
        elif bearish_score > bullish_score:
            bearish_score += 10
    elif whale_tx_count > 20:
        signals.append(f"Moderate whale activity ({whale_tx_count})")

    liquidations = data.get('liquidations', [])
    liq_count = len(liquidations) if liquidations else 0
    if liq_count > 10:
        signals.append(f"High liquidation count ({liq_count}) → potential trend exhaustion")
        if liq_count > 20:
            bullish_score += 5
            bearish_score += 5
    elif liq_count > 5:
        signals.append(f"Moderate liquidations ({liq_count})")

    if data.get('stablecoin_bullish', False):
        bullish_score += 10
        signals.append("Stablecoin aggregate bullish (net inflow)")
    else:
        bearish_score += 5
        signals.append("Stablecoin aggregate bearish (net outflow)")

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

    reason = f"Net score {net:+d}, dominant: {signals[0] if signals else 'no strong on‑chain signal'}"

    return {
        "bias": bias,
        "confidence": confidence,
        "high_prob_scenario": high_prob,
        "probability_estimate": confidence,
        "reason": reason,
        "signals": signals,
        "net_score": net
    }

# ========================== LOAD ON‑CHAIN DATA FROM X23 ==========================
def load_onchain_data(symbol):
    """Read X23 .tmp_x file and build dict for analyze_onchain."""
    path = os.path.join(FEATURES_BASE_DIR, f"{symbol.lower()}_onchain.tmp_x")
    if not os.path.exists(path):
        log_issue("ERROR", f"On‑chain file not found: {path}")
        return None
    data = {}
    # Defaults
    data['usdt_netflow'] = 0
    data['whale_ratio'] = 1.0
    data['taker_ratio'] = 1.0
    data['funding_rate'] = 0.0
    data['oi'] = 0.0
    data['depth_imbalance'] = 0.0
    data['stablecoin_bullish'] = False
    data['liquidations'] = []
    data['exchange_netflow'] = []
    data['whale_transactions_count'] = 0
    data['atr'] = 0.0
    data['current_price'] = 0.0

    try:
        with open(path, "r") as f:
            lines = f.readlines()
        for line in lines[1:]:  # skip header
            parts = line.strip().split('\t')
            if len(parts) < 2:
                continue
            typ = parts[0]
            if typ == "stablecoin" and len(parts) >= 7:
                symbol = parts[2]
                netflow = float(parts[6]) if parts[6] else 0
                if symbol == "USDT":
                    data['usdt_netflow'] = netflow
                    # stablecoin_bullish: if any stablecoin has positive netflow
                    if netflow > 0:
                        data['stablecoin_bullish'] = True
            elif typ == "binance_snapshot" and len(parts) >= 9:
                # fields: type, timestamp, current_price, oi, funding_rate, whale_ratio, taker_ratio, depth_imbalance, atr
                data['current_price'] = float(parts[2]) if parts[2] else 0.0
                data['oi'] = float(parts[3]) if parts[3] else 0.0
                data['funding_rate'] = float(parts[4]) if parts[4] else 0.0
                data['whale_ratio'] = float(parts[5]) if parts[5] else 1.0
                data['taker_ratio'] = float(parts[6]) if parts[6] else 1.0
                data['depth_imbalance'] = float(parts[7]) if parts[7] else 0.0
                data['atr'] = float(parts[8]) if len(parts) > 8 and parts[8] else 0.0
            elif typ == "whale":
                data['whale_transactions_count'] += 1
                # we also collect whale events as list? Not required for count only
            elif typ == "exchange_netflow" and len(parts) >= 3:
                ts = int(parts[1])
                netflow_val = float(parts[2]) if parts[2] else 0
                data['exchange_netflow'].append({"timestamp": ts, "netflow": netflow_val})
            elif typ == "liquidation":
                # store liquidation events as dict (price, quantity, side)
                if len(parts) >= 5:
                    price = float(parts[2]) if parts[2] else 0
                    qty = float(parts[3]) if parts[3] else 0
                    side = parts[4] if len(parts) > 4 else ""
                    data['liquidations'].append({"price": price, "quantity": qty, "side": side})
    except Exception as e:
        log_issue("ERROR", f"Failed to load on‑chain data: {e}")
        return None
    # Sort exchange netflow by timestamp descending and keep latest
    if data['exchange_netflow']:
        data['exchange_netflow'].sort(key=lambda x: x['timestamp'], reverse=True)
    # Add current_price if missing (maybe not in snapshot)
    # Ensure stablecoin_bullish is set from USDT netflow if not already
    if data['usdt_netflow'] > 0:
        data['stablecoin_bullish'] = True
    # Also we might need `atr` already present
    return data

# ========================== MAIN EXPERT FUNCTION ==========================
def run_expert(symbol):
    log_issue("INFO", f"Starting E13 on‑chain expert for {symbol}")
    onchain_data = load_onchain_data(symbol)
    if not onchain_data:
        log_issue("ERROR", "No on‑chain data found")
        return None
    result = analyze_onchain(onchain_data)
    # Write output TSV
    out_path = os.path.join(FEATURES_BASE_DIR, f"{symbol.lower()}_E13_onchain.tsv")
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
    log_issue("INFO", f"Saved on‑chain expert summary to {out_path}")
    return out_path

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python E13_onchain_expert.py SYMBOL")
        sys.exit(1)
    success = run_expert(sys.argv[1].upper())
    sys.exit(0 if success else 1)