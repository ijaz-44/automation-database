#!/usr/bin/env python3
# E07_liquidation_expert.py – Liquidation High‑Probability Scenario Detector (≥90% setups)
# Input: either a file path (TOON) or a dict containing liquidation data.
# Output: bias, confidence, high_prob_scenario, reasons, signals.

import os
import re
import math

def analyze_liquidation(data):
    """
    Args:
        data: dict with keys (same as X13's TOON sections):
            - 'liq_1m': list of dicts with 'long', 'short', 'total'
            - 'liq_15m': same
            - 'liq_1h': same
            - 'heatmap': dict {price_bin: volume}
            - 'pools_high': list of (level, count)
            - 'pools_low': list of (level, count)
            - 'stop_levels': dict with 'prev_day_high', 'prev_day_low', 'prev_week_high', 'prev_week_low'
    Returns:
        dict with bias, confidence, high_prob_scenario, signals, net_score, reason.
    """
    signals = []
    bullish_score = 0
    bearish_score = 0

    # 1. Recent liquidation delta (last 5‑10 rows of 1m and 15m)
    for tf in ['1m', '15m']:
        rows = data.get(f'liq_{tf}', [])
        if not rows:
            continue
        # take last 10 rows (or all)
        recent = rows[-10:]
        net_long = sum(r.get('long', 0) - r.get('short', 0) for r in recent)
        total_vol = sum(r.get('total', 0) for r in recent)
        # If net long positive and total volume high → bullish pressure
        if total_vol > 0:
            net_ratio = net_long / total_vol
            if net_ratio > 0.3:
                bullish_score += 20
                signals.append(f"{tf} net long liquidation delta {net_ratio*100:.0f}% → bullish")
            elif net_ratio < -0.3:
                bearish_score += 20
                signals.append(f"{tf} net short liquidation delta {abs(net_ratio)*100:.0f}% → bearish")
        # Also check spikes in total volume (capitulation)
        if len(rows) >= 3:
            recent_avg = sum(r['total'] for r in rows[-5:-1]) / max(1, len(rows[-5:-1]))
            latest = rows[-1]['total']
            if latest > 2 * recent_avg and recent_avg > 0:
                if rows[-1].get('long', 0) > rows[-1].get('short', 0):
                    bullish_score += 15
                    signals.append(f"{tf} spike in long liquidations → possible short squeeze")
                elif rows[-1].get('short', 0) > rows[-1].get('long', 0):
                    bearish_score += 15
                    signals.append(f"{tf} spike in short liquidations → possible long squeeze")

    # 2. Liquidity heatmap: concentration of volume at price levels
    heatmap = data.get('heatmap', {})
    if heatmap:
        # Find the top 3 volume clusters (they act as magnets or barriers)
        sorted_bins = sorted(heatmap.items(), key=lambda x: x[1], reverse=True)
        top_bins = sorted_bins[:3]
        # Not directly directional, but if clusters are far from current price? We don't have current price.
        # We can infer that high volume clusters may act as support/resistance, but without current price skip.
        # Instead, we use pools (below) which give levels with multiple touches.
        pass

    # 3. Liquidity pools (repeated high/low levels)
    high_pools = data.get('pools_high', [])   # list of (level, count)
    low_pools = data.get('pools_low', [])
    # More pools on one side indicates stronger support/resistance.
    if high_pools and low_pools:
        high_count = sum(cnt for _, cnt in high_pools)
        low_count = sum(cnt for _, cnt in low_pools)
        # More high pools suggest resistance above → bearish
        if high_count > low_count * 1.5:
            bearish_score += 15
            signals.append("More high liquidity pools than low → overhead resistance")
        elif low_count > high_count * 1.5:
            bullish_score += 15
            signals.append("More low liquidity pools than high → strong support")

    # 4. Stop hunt levels (recent week/day highs/lows)
    stop_levels = data.get('stop_levels', {})
    prev_day_high = stop_levels.get('prev_day_high', 0)
    prev_day_low = stop_levels.get('prev_day_low', 0)
    prev_week_high = stop_levels.get('prev_week_high', 0)
    prev_week_low = stop_levels.get('prev_week_low', 0)
    # We don't have current price, so we only note the existence of these levels.
    # Typically a break of day/week high triggers buy stops → bullish, break of lows triggers sell stops → bearish.
    # But we can't know price relative to them. We'll just use them as potential volatility catalysts.
    if prev_day_high or prev_week_high:
        signals.append(f"Stop levels detected: day high {prev_day_high}, week high {prev_week_high}")
    if prev_day_low or prev_week_low:
        signals.append(f"Stop levels detected: day low {prev_day_low}, week low {prev_week_low}")

    # 5. Net score
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

    high_prob_scenario = None
    if confidence >= 90:
        high_prob_scenario = "UP" if bias == "bullish" else "DOWN" if bias == "bearish" else None

    reason = f"Net score {net:+d}, signals: {signals[0] if signals else 'no strong signals'}"

    return {
        "bias": bias,
        "confidence": confidence,
        "high_prob_scenario": high_prob_scenario,
        "probability_estimate": confidence,
        "reason": reason,
        "signals": signals,
        "net_score": net
    }

# ---------- Parser for X13's TOON file (optional convenience) ----------
def parse_x13_toon(filepath):
    """Read X13's TOON file and return a dict with the data sections."""
    if not os.path.exists(filepath):
        return None
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    result = {}

    # Helper to extract array sections
    def extract_array(name):
        pattern = rf'{name}\[(\d+)\]\{{([^}}]+)\}}:\s*\n(?:\s+([^\n]+(?:\n\s+[^\n]+)*))?'
        match = re.search(pattern, content, re.DOTALL)
        if not match:
            return None
        rows_text = match.group(3)
        if not rows_text:
            return []
        rows = []
        for line in rows_text.split(' | '):
            parts = line.strip().split(',')
            if len(parts) >= 2:
                if name == 'liquidity_heatmap':
                    # price,volume
                    rows.append((float(parts[0]), float(parts[1])))
                else:
                    # timestamp, long, short, total
                    try:
                        rows.append({
                            'ts': int(parts[0]),
                            'long': float(parts[1]),
                            'short': float(parts[2]),
                            'total': float(parts[3])
                        })
                    except:
                        pass
        return rows

    # Parse liquidation arrays
    for iv in ['1m', '15m', '1h']:
        liq = extract_array(f'liquidation_{iv}')
        if liq:
            result[f'liq_{iv}'] = liq

    # Heatmap
    hm = extract_array('liquidity_heatmap')
    if hm:
        result['heatmap'] = {price: vol for price, vol in hm}

    # Pools high
    high_match = re.search(r'liquidity_pools_high\[\d+\]\{.*?\}:\s*\n(?:\s+([^\n]+(?:\n\s+[^\n]+)*))?', content, re.DOTALL)
    if high_match:
        rows_text = high_match.group(1)
        pools_high = []
        if rows_text:
            for line in rows_text.split(' | '):
                parts = line.strip().split(',')
                if len(parts) >= 2:
                    pools_high.append((float(parts[0]), int(parts[1])))
        result['pools_high'] = pools_high

    # Pools low
    low_match = re.search(r'liquidity_pools_low\[\d+\]\{.*?\}:\s*\n(?:\s+([^\n]+(?:\n\s+[^\n]+)*))?', content, re.DOTALL)
    if low_match:
        rows_text = low_match.group(1)
        pools_low = []
        if rows_text:
            for line in rows_text.split(' | '):
                parts = line.strip().split(',')
                if len(parts) >= 2:
                    pools_low.append((float(parts[0]), int(parts[1])))
        result['pools_low'] = pools_low

    # Stop hunt levels
    stop_match = re.search(r'stop_hunt_levels\[\d+\]\{.*?\}:\s*\n(?:\s+([^\n]+(?:\n\s+[^\n]+)*))?', content, re.DOTALL)
    if stop_match:
        rows_text = stop_match.group(1)
        stop_levels = {}
        if rows_text:
            for line in rows_text.split(' | '):
                parts = line.strip().split(',')
                if len(parts) >= 2:
                    stop_levels[parts[0]] = float(parts[1])
        result['stop_levels'] = stop_levels

    return result

def from_x13_file(symbol, base_dir=None):
    """Convenience: read X13's TOON file and return expert analysis."""
    if base_dir is None:
        base_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "market_data", "binance", "symbols")
    toon_path = os.path.join(base_dir, f"{symbol.lower()}_liquidations.toon")
    data = parse_x13_toon(toon_path)
    if not data:
        return {"error": "Liquidation data not found", "bias": "neutral", "confidence": 0}
    return analyze_liquidation(data)

def from_x13_data_dict(data_dict):
    """Directly accept data dict from X13 (if X13 is modified to return dict)."""
    return analyze_liquidation(data_dict)

# Self-test
if __name__ == "__main__":
    # Simulate X13 data
    test = {
        'liq_1m': [
            {'ts': 1, 'long': 100, 'short': 50, 'total': 150},
            {'ts': 2, 'long': 120, 'short': 40, 'total': 160},
            {'ts': 3, 'long': 200, 'short': 30, 'total': 230}
        ],
        'liq_15m': [
            {'ts': 1, 'long': 500, 'short': 200, 'total': 700},
            {'ts': 2, 'long': 600, 'short': 150, 'total': 750},
            {'ts': 3, 'long': 800, 'short': 100, 'total': 900}
        ],
        'heatmap': {100: 500, 200: 800, 300: 600},
        'pools_high': [(150, 3), (180, 2)],
        'pools_low': [(90, 4), (80, 1)],
        'stop_levels': {'prev_day_high': 200, 'prev_day_low': 180}
    }
    result = analyze_liquidation(test)
    print(result)