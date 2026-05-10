#!/usr/bin/env python3
# E02_cvd_expert.py – CVD High‑Probability Scenario Detector (≥90% setups)
# Input: either a TOON file path or a dict containing CVD state (from X03)
# Output: structured analysis with bias, confidence, scenarios, reasons.

import os
import re
import math
from datetime import datetime

def parse_cvd_toon(toon_path):
    """Extract CVD summary and signals from X03's TOON file."""
    if not os.path.exists(toon_path):
        return None
    with open(toon_path, 'r', encoding='utf-8') as f:
        content = f.read()
    # extract cvd_signal line
    signal_match = re.search(r'cvd_signal\[1\]\{.*?\}:\s*\n\s+([^,\n]+),([^,\n]+),([^,\n]+),([^,\n]+),([^,\n]+),([^,\n]+),([^,\n]+)', content)
    if not signal_match:
        return None
    groups = signal_match.groups()
    direction = groups[0].strip()
    confidence = int(groups[1].strip())
    slope = float(groups[2].strip())
    acc = float(groups[3].strip())
    divergence = groups[4].strip()
    absorption_net = int(groups[5].strip())
    imbalance_score = float(groups[6].strip())
    # also try to get cumulative CVD from summary
    cvd_match = re.search(r'cvd_summary\[1\]\{.*?\}:\s*\n\s+[^,]+,[^,]+,[^,]+,[^,]+', content)
    cumulative_cvd = None
    if cvd_match:
        # extract the second field (cumulative_cvd)
        parts = cvd_match.group(0).split(',')
        if len(parts) >= 2:
            cumulative_cvd = float(parts[1].split(',')[0])  # crude
    # extract absorption_events (to know recent net)
    absorption_count = content.count('absorption_events')
    # imbalance_events count
    imbalance_count = content.count('imbalance_events')
    return {
        'direction': direction,
        'confidence': confidence,
        'cvd_slope_10': slope,
        'cvd_acceleration': acc,
        'divergence': divergence,
        'absorption_net': absorption_net,
        'imbalance_score': imbalance_score,
        'cumulative_cvd': cumulative_cvd,
        'absorption_events_count': absorption_count,
        'imbalance_events_count': imbalance_count
    }

def analyze_cvd(cvd_data):
    """
    Input: dict with keys (from X03 or parsed TOON):
        direction: 'up'/'down'/'neutral'
        confidence: int 0-100
        cvd_slope_10: float
        cvd_acceleration: float
        divergence: 'bullish'/'bearish'/'none'
        absorption_net: int (bullish - bearish absorptions)
        imbalance_score: float
        cumulative_cvd: float (optional)
    Returns:
        dict with: 
            'bias': 'bullish'/'bearish'/'neutral'
            'confidence': int (0-100)
            'high_prob_scenario': 'UP'/'DOWN'/None
            'probability_estimate': int (0-100) (≥90 for high prob)
            'reason': str
            'signals': list of str
    """
    signals = []
    bullish_score = 0
    bearish_score = 0
    direction = cvd_data.get('direction', 'neutral')
    confidence = cvd_data.get('confidence', 50)
    slope = cvd_data.get('cvd_slope_10', 0)
    acc = cvd_data.get('cvd_acceleration', 0)
    divergence = cvd_data.get('divergence', 'none')
    absorption_net = cvd_data.get('absorption_net', 0)
    imbalance_score = cvd_data.get('imbalance_score', 0)

    # 1. Divergence (most powerful signal)
    if divergence == 'bullish':
        bullish_score += 40
        signals.append("Bullish CVD divergence (price down, CVD up)")
    elif divergence == 'bearish':
        bearish_score += 40
        signals.append("Bearish CVD divergence (price up, CVD down)")

    # 2. Strong slope (>50 units per 10 candles) with acceleration
    if slope > 50:
        bullish_score += 25
        signals.append(f"CVD strong uptrend (slope {slope:.1f})")
    elif slope < -50:
        bearish_score += 25
        signals.append(f"CVD strong downtrend (slope {slope:.1f})")
    # acceleration adds confidence
    if acc > 20 and slope > 0:
        bullish_score += 15
        signals.append("CVD accelerating upward")
    elif acc < -20 and slope < 0:
        bearish_score += 15
        signals.append("CVD accelerating downward")

    # 3. Absorption events (hidden buying/selling)
    if absorption_net >= 2:
        bullish_score += 20
        signals.append(f"Bullish absorption detected (net {absorption_net})")
    elif absorption_net <= -2:
        bearish_score += 20
        signals.append(f"Bearish absorption detected (net {absorption_net})")

    # 4. Imbalance score (>200 strong signal)
    if imbalance_score > 200:
        bullish_score += 15
        signals.append(f"Strong buy imbalance (score {imbalance_score:.0f})")
    elif imbalance_score < -200:
        bearish_score += 15
        signals.append(f"Strong sell imbalance (score {imbalance_score:.0f})")

    # 5. X03's own direction/confidence (moderate weight)
    if direction == 'up':
        bullish_score += confidence / 5  # max 20
    elif direction == 'down':
        bearish_score += confidence / 5

    # Determine net score (-100 to 100)
    net = bullish_score - bearish_score
    net = max(-100, min(100, net))

    # Bias and confidence
    if net >= 30:
        bias = "bullish"
        prob = min(95, 60 + net // 2)
    elif net <= -30:
        bias = "bearish"
        prob = min(95, 60 + abs(net) // 2)
    else:
        bias = "neutral"
        prob = 50

    # High-probability threshold (≥90%)
    high_prob_scenario = None
    if bias == "bullish" and prob >= 90:
        high_prob_scenario = "UP"
    elif bias == "bearish" and prob >= 90:
        high_prob_scenario = "DOWN"
    else:
        high_prob_scenario = None

    # Build reason string
    reason = f"CVD net score {net:+d} – " + ", ".join(signals[:3]) if signals else "No strong signals"

    return {
        "bias": bias,
        "confidence": prob,
        "high_prob_scenario": high_prob_scenario,
        "probability_estimate": prob,
        "reason": reason,
        "signals": signals,
        "net_score": net,
        "cvd_slope": slope,
        "absorption_net": absorption_net,
        "imbalance_score": imbalance_score,
        "divergence": divergence
    }

def from_toon_file(symbol, base_dir=None):
    """Convenience: parse X03's TOON file for a symbol and return analysis."""
    if base_dir is None:
        base_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "market_data", "binance", "symbols")
    toon_path = os.path.join(base_dir, f"{symbol.lower()}_cvd.toon")
    cvd_data = parse_cvd_toon(toon_path)
    if not cvd_data:
        return {"error": "CVD data not found", "bias": "neutral", "confidence": 0}
    return analyze_cvd(cvd_data)

def from_x03_data_dict(data_dict):
    """
    Directly use X03's output dict (when X03 is modified to return data instead of writing TOON).
    Expected keys: direction, confidence, cvd_slope_10, cvd_acceleration, divergence, absorption_net, imbalance_score.
    """
    return analyze_cvd(data_dict)

# Self‑test example
if __name__ == "__main__":
    # Simulate X03 output
    test_data = {
        "direction": "up",
        "confidence": 85,
        "cvd_slope_10": 120.5,
        "cvd_acceleration": 35.2,
        "divergence": "bullish",
        "absorption_net": 3,
        "imbalance_score": 450,
        "cumulative_cvd": 12500
    }
    result = analyze_cvd(test_data)
    print(result)