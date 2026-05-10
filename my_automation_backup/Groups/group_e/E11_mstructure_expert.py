#!/usr/bin/env python3
# E11_mstructure_expert.py – Market Structure High‑Probability Scenario Detector (≥90%)
# Input: dict with keys – trend_score, bos, choch, sr_levels, sd_zones, order_blocks, fakeouts, pivot_zones, swings (optional)
# Output: bias, confidence, high_prob_scenario, signals, net_score, reason.

import math

def analyze_market_structure(data):
    """
    Args:
        data: dict with keys:
            - trend_score: float (-1..1)
            - bos: str ('bullish', 'bearish', 'none')
            - choch: str ('bullish_reversal', 'bearish_reversal', 'none')
            - sr_levels: list of dicts with 'type','price','touches'
            - sd_zones: list of dicts with 'type','strength','status'
            - order_blocks: list of dicts with 'type','strength'
            - fakeouts: list of dicts with 'type','level','rejection','target'
            - pivot_zones: dict with 'prev_day_high','prev_day_low','prev_week_high','prev_week_low','ath','atl'
            - swings: optional dict with timeframes and swing points (not used heavily)
            - current_price: float (optional, if not provided we use later standard)
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

    # 1. Trend score (most important)
    trend_score = data.get('trend_score', 0.0)
    if trend_score > 0.5:
        bullish_score += 30
        signals.append(f"Strong bullish trend score ({trend_score:.2f})")
    elif trend_score > 0.2:
        bullish_score += 15
        signals.append(f"Moderate bullish trend ({trend_score:.2f})")
    elif trend_score < -0.5:
        bearish_score += 30
        signals.append(f"Strong bearish trend score ({trend_score:.2f})")
    elif trend_score < -0.2:
        bearish_score += 15
        signals.append(f"Moderate bearish trend ({trend_score:.2f})")
    else:
        signals.append(f"Neutral trend ({trend_score:.2f})")

    # 2. BOS (break of structure)
    bos = data.get('bos', 'none')
    if bos == 'bullish':
        bullish_score += 20
        signals.append("Bullish BOS")
    elif bos == 'bearish':
        bearish_score += 20
        signals.append("Bearish BOS")

    # 3. CHoCH (change of character)
    choch = data.get('choch', 'none')
    if choch == 'bullish_reversal':
        bullish_score += 25
        signals.append("Bullish CHoCH – reversal")
    elif choch == 'bearish_reversal':
        bearish_score += 25
        signals.append("Bearish CHoCH – reversal")

    # 4. Supply/Demand zones (strength and freshness)
    sd_zones = data.get('sd_zones', [])
    fresh_bullish = 0
    fresh_bearish = 0
    for z in sd_zones:
        if z['type'] == 'demand' and z['status'] == 'fresh':
            fresh_bullish += 1
        elif z['type'] == 'supply' and z['status'] == 'fresh':
            fresh_bearish += 1
    if fresh_bullish > 0:
        bullish_score += min(20, fresh_bullish * 8)
        signals.append(f"{fresh_bullish} fresh demand zones")
    if fresh_bearish > 0:
        bearish_score += min(20, fresh_bearish * 8)
        signals.append(f"{fresh_bearish} fresh supply zones")

    # 5. Order blocks (strength weighted)
    order_blocks = data.get('order_blocks', [])
    for ob in order_blocks:
        strength = ob.get('strength', 0)
        if ob['type'] == 'bullish':
            bullish_score += min(15, strength * 10)
        elif ob['type'] == 'bearish':
            bearish_score += min(15, strength * 10)
    if order_blocks:
        signals.append(f"Order blocks: {len(order_blocks)}")

    # 6. Fakeouts – recent fakeouts near levels indicate direction
    fakeouts = data.get('fakeouts', [])
    fakeout_up = sum(1 for fo in fakeouts if fo['type'] == 'fakeout_low')   # fakeout low → eventually up
    fakeout_down = sum(1 for fo in fakeouts if fo['type'] == 'fakeout_high') # fakeout high → eventually down
    if fakeout_up > fakeout_down:
        bullish_score += min(20, 5 + fakeout_up * 5)
        signals.append(f"{fakeout_up} fakeout lows → bullish signal")
    elif fakeout_down > fakeout_up:
        bearish_score += min(20, 5 + fakeout_down * 5)
        signals.append(f"{fakeout_down} fakeout highs → bearish signal")

    # 7. Pivot zones (proximity)
    current_price = data.get('current_price', 0)
    pivot_zones = data.get('pivot_zones', {})
    if current_price and pivot_zones:
        # prev day high/low
        prev_high = pivot_zones.get('prev_day_high')
        prev_low = pivot_zones.get('prev_day_low')
        if prev_high and current_price > prev_high * 0.99:  # near resistance
            bearish_score += 10
            signals.append("Near previous day high → resistance")
        if prev_low and current_price < prev_low * 1.01:   # near support
            bullish_score += 10
            signals.append("Near previous day low → support")
        # week high/low
        week_high = pivot_zones.get('prev_week_high')
        week_low = pivot_zones.get('prev_week_low')
        if week_high and current_price > week_high * 0.995:
            bearish_score += 15
            signals.append("Near weekly high → strong resistance")
        if week_low and current_price < week_low * 1.005:
            bullish_score += 15
            signals.append("Near weekly low → strong support")

    # 8. Swing points (optional, not used heavily, but we can check last swing direction if available)
    swings = data.get('swings', {})
    for tf, points in swings.items():
        if points:
            last = points[-1]
            if last['type'] == 'swing_high':
                bearish_score += 5
                signals.append(f"Last swing on {tf} is high")
            elif last['type'] == 'swing_low':
                bullish_score += 5
                signals.append(f"Last swing on {tf} is low")
            break  # only consider first timeframe

    # Net score
    net = bullish_score - bearish_score
    net = max(-100, min(100, net))

    # Bias and confidence
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

    reason = f"Net score {net:+d}, dominant: {signals[0] if signals else 'no clear structure'}"

    return {
        "bias": bias,
        "confidence": confidence,
        "high_prob_scenario": high_prob,
        "probability_estimate": confidence,
        "reason": reason,
        "signals": signals,
        "net_score": net
    }

# Convenience wrapper for X21's data_dict (the same as passed to atomic_write_db)
def from_x21_data_dict(data_dict):
    """Directly use the data dict from X21 (already contains all required fields)."""
    return analyze_market_structure(data_dict)

# Self‑test
if __name__ == "__main__":
    test = {
        "trend_score": 0.65,
        "bos": "bullish",
        "choch": "none",
        "sr_levels": [],
        "sd_zones": [{"type": "demand", "strength": 0.8, "status": "fresh"}],
        "order_blocks": [{"type": "bullish", "strength": 0.6}],
        "fakeouts": [{"type": "fakeout_low"}, {"type": "fakeout_low"}],
        "pivot_zones": {"prev_day_high": 64200, "prev_day_low": 63800},
        "current_price": 64150,
        "swings": {"1h": [{"type": "swing_low"}]}
    }
    result = analyze_market_structure(test)
    print(result)