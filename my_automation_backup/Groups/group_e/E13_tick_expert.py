#!/usr/bin/env python3
# E13_tick_expert.py – Tick Flow High‑Probability Scenario Detector (≥90% Setups)
# Input: dict with tick metrics (from X25)
# Output: bias, confidence, high_prob_scenario, signals, net_score, reason.

import math

def analyze_tick_flow(data):
    """
    Args:
        data: dict with keys:
            - buy_volume: float
            - sell_volume: float
            - net_delta: float (buy - sell)
            - speed_score: float (trades per second)
            - avg_trade_size: float
            - whale_buy_volume: float
            - whale_sell_volume: float
            - whale_delta: float (whale_buy - whale_sell)
            - delta_last_100: float (net delta of last 100 trades)
            - total_volume (optional): buy_volume + sell_volume
    Returns:
        dict with:
            'bias' (bullish/bearish/neutral),
            'confidence' (int 0‑100),
            'high_prob_scenario' ('UP'/'DOWN'/None),
            'probability_estimate' (int),
            'reason' (str),
            'signals' (list),
            'net_score' (int)
    """
    signals = []
    bullish_score = 0
    bearish_score = 0

    buy = data.get('buy_volume', 0)
    sell = data.get('sell_volume', 0)
    net = data.get('net_delta', 0)
    total = buy + sell
    speed = data.get('speed_score', 0)
    whale_delta = data.get('whale_delta', 0)
    delta_last_100 = data.get('delta_last_100', 0)
    avg_trade = data.get('avg_trade_size', 1)

    # 1. Net delta (buy - sell) normalized by total volume (if total > 0)
    if total > 0:
        net_ratio = net / total
        if net_ratio > 0.3:
            bullish_score += 30
            signals.append(f"Strong buy dominance ({net_ratio*100:.1f}% of volume)")
        elif net_ratio > 0.1:
            bullish_score += 15
            signals.append(f"Moderate buy dominance ({net_ratio*100:.1f}%)")
        elif net_ratio < -0.3:
            bearish_score += 30
            signals.append(f"Strong sell dominance ({-net_ratio*100:.1f}%)")
        elif net_ratio < -0.1:
            bearish_score += 15
            signals.append(f"Moderate sell dominance ({-net_ratio*100:.1f}%)")
        else:
            signals.append(f"Neutral net delta ({net_ratio*100:.1f}%)")
    else:
        signals.append("No volume data")

    # 2. Speed (activity intensity) – high speed amplifies directional conviction
    if speed > 5.0:
        if net > 0:
            bullish_score += 15
            signals.append(f"Very high trade speed ({speed:.1f} tps) confirming buying pressure")
        elif net < 0:
            bearish_score += 15
            signals.append(f"Very high trade speed ({speed:.1f} tps) confirming selling pressure")
        else:
            signals.append(f"High speed but neutral delta ({speed:.1f} tps)")
    elif speed > 2.0:
        if net > 0:
            bullish_score += 8
            signals.append(f"Elevated speed ({speed:.1f} tps) supports buying")
        elif net < 0:
            bearish_score += 8
            signals.append(f"Elevated speed ({speed:.1f} tps) supports selling")
        else:
            signals.append(f"Moderate speed ({speed:.1f} tps)")
    else:
        signals.append(f"Low speed ({speed:.1f} tps)")

    # 3. Whale delta (large orders)
    # Use absolute whale delta normalized by total volume (if total > 0)
    if total > 0:
        whale_ratio = abs(whale_delta) / total
        # Normalize whale delta to a score (max 20)
        if whale_delta > 0:
            whale_bull = min(20, int(whale_ratio * 60))
            bullish_score += whale_bull
            signals.append(f"Bullish whale delta (+{whale_delta:.2f}, {whale_ratio*100:.1f}% of volume)")
        elif whale_delta < 0:
            whale_bear = min(20, int(whale_ratio * 60))
            bearish_score += whale_bear
            signals.append(f"Bearish whale delta ({whale_delta:.2f}, {whale_ratio*100:.1f}% of volume)")
        else:
            signals.append("No whale imbalance")
    else:
        signals.append("Whale delta not calculated")

    # 4. Micro‑burst delta (last 100 trades) – recent momentum
    if delta_last_100 != 0:
        # Normalize by avg trade size to give a relative magnitude
        # A delta of 10x avg trade size is significant
        burst_strength = abs(delta_last_100) / max(avg_trade, 0.01)
        if delta_last_100 > 0:
            if burst_strength > 5:
                bullish_score += 20
                signals.append(f"Very strong recent buying burst (+{delta_last_100:.2f})")
            elif burst_strength > 2:
                bullish_score += 10
                signals.append(f"Recent buying burst (+{delta_last_100:.2f})")
            else:
                bullish_score += 5
                signals.append(f"Moderate recent buying (+{delta_last_100:.2f})")
        else:
            if burst_strength > 5:
                bearish_score += 20
                signals.append(f"Very strong recent selling burst ({delta_last_100:.2f})")
            elif burst_strength > 2:
                bearish_score += 10
                signals.append(f"Recent selling burst ({delta_last_100:.2f})")
            else:
                bearish_score += 5
                signals.append(f"Moderate recent selling ({delta_last_100:.2f})")

    # 5. Check for divergence between recent momentum and overall delta
    if total > 0 and delta_last_100 != 0 and net != 0:
        # if recent delta is opposite sign to total net delta, could indicate reversal
        if (delta_last_100 > 0 and net < 0) or (delta_last_100 < 0 and net > 0):
            # divergence: recent flow opposite to total
            # this is a potential reversal signal – adjust confidence but not direction
            signals.append("Divergence: recent tick flow opposes cumulative delta")
            # Reduce the dominant score? Actually, it's a warning sign.
            # We'll add a penalty to the side that had been winning.
            if net > 0:
                bullish_score = max(0, bullish_score - 15)
            else:
                bearish_score = max(0, bearish_score - 15)

    # Net score
    net_score = bullish_score - bearish_score
    net_score = max(-100, min(100, net_score))

    # Bias and confidence
    if net_score >= 30:
        bias = "bullish"
        confidence = min(95, 60 + net_score // 2)
    elif net_score <= -30:
        bias = "bearish"
        confidence = min(95, 60 + abs(net_score) // 2)
    else:
        bias = "neutral"
        confidence = 50 + net_score // 2 if net_score else 50

    # High‑probability scenario
    high_prob = None
    if confidence >= 90 and bias != "neutral":
        high_prob = "UP" if bias == "bullish" else "DOWN"

    reason = f"Net score {net_score:+d}, dominant: {signals[0] if signals else 'no clear tick flow'}"

    return {
        "bias": bias,
        "confidence": confidence,
        "high_prob_scenario": high_prob,
        "probability_estimate": confidence,
        "reason": reason,
        "signals": signals,
        "net_score": net_score
    }

# Convenience wrapper for X25's internal data (the metrics computed in collect_and_save)
def from_x25_data_dict(data_dict):
    """Directly use the dict built by X25 (with buy_volume, sell_volume, etc.)."""
    return analyze_tick_flow(data_dict)

# Self-test
if __name__ == "__main__":
    test = {
        "buy_volume": 1250,
        "sell_volume": 800,
        "net_delta": 450,
        "speed_score": 7.2,
        "avg_trade_size": 2.5,
        "whale_buy_volume": 300,
        "whale_sell_volume": 80,
        "whale_delta": 220,
        "delta_last_100": 180,
        "total_volume": 2050
    }
    result = analyze_tick_flow(test)
    print(result)