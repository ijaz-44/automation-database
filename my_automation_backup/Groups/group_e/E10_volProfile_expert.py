#!/usr/bin/env python3
# E10_volProfile_expert.py – Volume Profile High‑Probability Scenario Detector (≥90%)
# Input: dict with volume profile data from X19 (daily_profiles, intraday_profiles, developing_poc, etc.)
# Output: bias, confidence, high_prob_scenario, signals, net_score, reason.

import math

def analyze_volume_profile(data):
    """
    Args:
        data: dict with keys:
            - last_profile: dict with 'poc', 'vah', 'val', 'shape' (string), 'hvns' (list), 'lvns' (list)
            - daily_profiles: list of dicts (last 7 days) for `poc`, `vah`, `val`, `shape`
            - intraday_profiles: dict with keys '1h', '4h' each containing 'poc', 'vah', 'val', 'shape'
            - developing_poc: float (vPOC) – optional
            - developing_vah_val: dict with 'vvah', 'vval' – optional
            - current_price: float
            - tick_size: float (optional, for threshold scaling)
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

    # Unpack
    last = data.get('last_profile', {})
    shape = last.get('shape', 'D-shape')
    poc = last.get('poc', 0)
    vah = last.get('vah', 0)
    val = last.get('val', 0)
    hvns = last.get('hvns', [])
    lvns = last.get('lvns', [])
    current_price = data.get('current_price', poc)
    intra_1h = data.get('intraday_profiles', {}).get('1h', {})
    intra_4h = data.get('intraday_profiles', {}).get('4h', {})
    vpoc = data.get('developing_poc', None)
    vvah = data.get('developing_vah_val', {}).get('vvah', None)
    vval = data.get('developing_vah_val', {}).get('vval', None)

    # 1. Shape based bias (most important single factor)
    if shape == 'P-shape':
        bullish_score += 30
        signals.append("P‑shape (value area high) → bullish bias")
    elif shape == 'b-shape':
        bearish_score += 30
        signals.append("b‑shape (value area low) → bearish bias")
    else:
        signals.append("D‑shape – neutral, inside value area")

    # 2. Price position relative to value area
    if vah and val and current_price > 0:
        if current_price > vah:
            # above VAH: potential breakout
            bullish_score += 15
            signals.append(f"Price above VAH ({vah:.2f}) → breakout bullish")
        elif current_price < val:
            bearish_score += 15
            signals.append(f"Price below VAL ({val:.2f}) → breakdown bearish")
        else:
            signals.append(f"Price inside value area ({val:.2f} – {vah:.2f}) → neutral")

    # 3. HVN / LVN clusters (support/resistance)
    # HVN *above* price act as resistance, *below* act as support
    if hvns and current_price > 0:
        hvns_above = [h for h in hvns if h > current_price]
        hvns_below = [h for h in hvns if h < current_price]
        if hvns_above:
            bearish_score += 10
            signals.append(f"HVN resistance above at {min(hvns_above):.2f}")
        if hvns_below:
            bullish_score += 10
            signals.append(f"HVN support below at {max(hvns_below):.2f}")
    # LVN – low volume nodes are magnets; price tends to move through them quickly
    if lvns and current_price > 0:
        lvns_above = [l for l in lvns if l > current_price]
        lvns_below = [l for l in lvns if l < current_price]
        if lvns_above:
            bullish_score += 5   # price may move up to fill LVN
            signals.append(f"LVN above at {min(lvns_above):.2f} → potential target")
        if lvns_below:
            bearish_score += 5
            signals.append(f"LVN below at {max(lvns_below):.2f} → potential target")

    # 4. Multi‑timeframe confluence (shape alignment)
    daily_shape = data.get('daily_profiles', [{}])[-1].get('shape', '') if data.get('daily_profiles') else ''
    shape_4h = intra_4h.get('shape', '')
    shape_1h = intra_1h.get('shape', '')
    if daily_shape and shape_4h and shape_1h:
        if daily_shape == 'P-shape' and shape_4h == 'P-shape' and shape_1h == 'P-shape':
            bullish_score += 25
            signals.append("All timeframes P‑shape → strong bullish confluence")
        elif daily_shape == 'b-shape' and shape_4h == 'b-shape' and shape_1h == 'b-shape':
            bearish_score += 25
            signals.append("All timeframes b‑shape → strong bearish confluence")
        else:
            signals.append("Mixed timeframes – lower conviction")

    # 5. Developing POC vs value area
    if vpoc and vah and val:
        if vpoc > vah:
            bullish_score += 15
            signals.append(f"Developing POC ({vpoc:.2f}) above VAH → bullish momentum")
        elif vpoc < val:
            bearish_score += 15
            signals.append(f"Developing POC ({vpoc:.2f}) below VAL → bearish momentum")
        else:
            signals.append(f"Developing POC inside value area")

    # 6. Developing VAH/VAL (current session)
    if vvah and vval and current_price:
        if current_price > vvah:
            bullish_score += 10
            signals.append("Price above developing VAH → current session breakout")
        elif current_price < vval:
            bearish_score += 10
            signals.append("Price below developing VAL → current session breakdown")

    # 7. Trend strength from vPOC vs daily POC (optional)
    daily_poc = data.get('last_profile', {}).get('poc', 0)
    if vpoc and daily_poc:
        diff_pct = (vpoc - daily_poc) / daily_poc * 100 if daily_poc else 0
        if diff_pct > 1:
            bullish_score += 10
            signals.append(f"Developing POC {diff_pct:.1f}% above daily POC → bullish drift")
        elif diff_pct < -1:
            bearish_score += 10
            signals.append(f"Developing POC {abs(diff_pct):.1f}% below daily POC → bearish drift")

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

    reason = f"Net score {net:+d}, dominant signals: {signals[0] if signals else 'volume profile neutral'}"

    return {
        "bias": bias,
        "confidence": confidence,
        "high_prob_scenario": high_prob,
        "probability_estimate": confidence,
        "reason": reason,
        "signals": signals,
        "net_score": net
    }

# Convenience wrapper for X19's data (build dict from its variables)
def from_x19_data(last_profile, daily_profiles, intraday_profiles, developing_poc, developing_vah_val, current_price):
    """Build the input dict from X19's collected data."""
    data = {
        "last_profile": last_profile,
        "daily_profiles": daily_profiles,
        "intraday_profiles": intraday_profiles,
        "developing_poc": developing_poc,
        "developing_vah_val": developing_vah_val,
        "current_price": current_price
    }
    return analyze_volume_profile(data)

# Self-test (example)
if __name__ == "__main__":
    test = {
        "last_profile": {"shape": "P-shape", "poc": 64000, "vah": 64200, "val": 63800,
                         "hvns": [64300, 64150], "lvns": [63700]},
        "daily_profiles": [{"shape": "P-shape"}],
        "intraday_profiles": {"1h": {"shape": "P-shape"}, "4h": {"shape": "P-shape"}},
        "developing_poc": 64350,
        "developing_vah_val": {"vvah": 64400, "vval": 64200},
        "current_price": 64180
    }
    result = analyze_volume_profile(test)
    print(result)