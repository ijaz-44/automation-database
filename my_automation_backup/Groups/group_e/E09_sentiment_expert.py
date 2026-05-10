#!/usr/bin/env python3
# E09_sentiment_expert.py – Sentiment & OI Intelligence (≥90% Setup Detector)
# Input: dict with news_score, retail_bias, funding_velocity, oi_trend, price_change_pct, etc.
# Output: bias, confidence, high_prob_scenario, signals, net_score, reason.

import math

def analyze_sentiment(data):
    """
    Args:
        data: dict with keys:
            - news_score: float (range -1..1, positive = bullish)
            - retail_bias: str ('Bullish_Extreme', 'Bearish_Extreme', or 'Neutral')
            - funding_velocity: float (change in funding rate over last period)
            - oi_trend: str ('rising', 'falling', 'flat')
            - price_change_pct: float (percentage change in price over last hour) – optional
            - social_velocity: int (mentions+upvotes) – optional
            - oi_velocity_pct: float (OI percentage change) – optional (used if oi_trend not provided)
            - symbol: str (optional, for logging)
    Returns:
        dict with:
            'bias' (bullish/bearish/neutral),
            'confidence' (int 0‑100, ≥90 indicates high prob),
            'high_prob_scenario' ('UP'/'DOWN'/None),
            'probability_estimate' (int),
            'reason' (str),
            'signals' (list),
            'net_score' (int),
            'retail_bias_raw' (str),
            'oi_price_state' (str)
    """
    signals = []
    bullish_score = 0
    bearish_score = 0

    # 1. News sentiment
    news = data.get('news_score', 0.0)
    if news > 0.6:
        bullish_score += 25
        signals.append(f"Very bullish news sentiment ({news:.2f})")
    elif news > 0.3:
        bullish_score += 10
        signals.append(f"Moderately bullish news ({news:.2f})")
    elif news < -0.6:
        bearish_score += 25
        signals.append(f"Very bearish news sentiment ({news:.2f})")
    elif news < -0.3:
        bearish_score += 10
        signals.append(f"Moderately bearish news ({news:.2f})")
    else:
        signals.append(f"Neutral news ({news:.2f})")

    # 2. Retail bias (contrarian indicator)
    retail = data.get('retail_bias', 'Neutral')
    if retail == 'Bullish_Extreme':
        bearish_score += 20
        signals.append("Extreme retail bullishness → contrarian bearish")
    elif retail == 'Bearish_Extreme':
        bullish_score += 20
        signals.append("Extreme retail bearishness → contrarian bullish")
    else:
        signals.append("Retail positioning neutral")

    # 3. Funding velocity (rising = more longs, falling = shorts covering)
    funding_vel = data.get('funding_velocity', 0.0)
    if funding_vel > 0.00005:
        bearish_score += 15
        signals.append(f"Funding rate rising ({funding_vel:.6f}) → longs increasing, bearish")
    elif funding_vel < -0.00005:
        bullish_score += 15
        signals.append(f"Funding rate falling ({funding_vel:.6f}) → shorts covering, bullish")
    else:
        signals.append(f"Funding velocity neutral ({funding_vel:.6f})")

    # 4. OI + price dynamics (most powerful)
    oi_trend = data.get('oi_trend', 'flat')
    price_change = data.get('price_change_pct', 0.0)
    oi_vel = data.get('oi_velocity_pct', 0.0)
    # If price_change not provided but oi_vel and oi_trend given, we use oi_trend + a heuristic for direction:
    # We'll rely primarily on oi_trend and a simple mapping if price_change is absent.
    if abs(price_change) < 0.01 and oi_vel != 0:
        # approximate: OI rising alone without price movement may indicate accumulation or distribution
        if oi_vel > 1.5:
            # OI building without price move → potential reversal later, but not immediate; small score
            if oi_vel > 3:
                signals.append(f"OI building strongly ({oi_vel:.1f}%) without price move → possible accumulation")
                # no strong directional score
            else:
                signals.append(f"OI rising ({oi_vel:.1f}%) while price flat")
    else:
        # Use price change and OI trend
        if price_change > 0.5 and oi_trend == 'rising':
            bullish_score += 30
            signals.append("Price up + OI rising → strong bullish buildup")
        elif price_change < -0.5 and oi_trend == 'rising':
            bearish_score += 30
            signals.append("Price down + OI rising → aggressive short buildup, bearish")
        elif price_change > 0.5 and oi_trend == 'falling':
            bullish_score += 20
            signals.append("Price up + OI falling → short covering rally, bullish")
        elif price_change < -0.5 and oi_trend == 'falling':
            bearish_score += 20
            signals.append("Price down + OI falling → long liquidation, bearish")
        else:
            signals.append("OI and price dynamics ambiguous")

    # 5. Social velocity (optional)
    social = data.get('social_velocity', 0)
    if social > 500:
        # High social activity often coincides with tops/bottoms (contrarian)
        if social > 1000:
            bearish_score += 10
            signals.append("Extreme social buzz → potential top")
        else:
            bullish_score += 5
            signals.append("Moderate social activity")
    elif social > 100:
        signals.append("Noticeable social activity")

    # Net score
    net = bullish_score - bearish_score
    net = max(-100, min(100, net))

    # Determine bias and confidence
    if net >= 30:
        bias = "bullish"
        confidence = min(95, 60 + net // 2)
    elif net <= -30:
        bias = "bearish"
        confidence = min(95, 60 + abs(net) // 2)
    else:
        bias = "neutral"
        confidence = 50 + net // 2 if net else 50

    # High‑probability scenario only if confidence ≥ 90 and bias not neutral
    high_prob = None
    if confidence >= 90 and bias != "neutral":
        high_prob = "UP" if bias == "bullish" else "DOWN"

    # Derive a text reason
    reason = f"Net score {net:+d}, signals: {signals[0] if signals else 'no clear signals'}"

    # Additional semantic fields for LLM
    oi_price_state = "unknown"
    if price_change > 0.5:
        oi_price_state = "price_up"
    elif price_change < -0.5:
        oi_price_state = "price_down"
    if oi_trend == "rising":
        oi_price_state += "_oi_rising"
    elif oi_trend == "falling":
        oi_price_state += "_oi_falling"
    else:
        oi_price_state += "_oi_flat"

    return {
        "bias": bias,
        "confidence": confidence,
        "high_prob_scenario": high_prob,
        "probability_estimate": confidence,
        "reason": reason,
        "signals": signals,
        "net_score": net,
        "retail_bias_raw": retail,
        "oi_price_state": oi_price_state
    }

# Convenience wrapper for X17's data (as currently provided)
def from_x17_data_dict(data_dict):
    """Directly use X17's internal data (news_score, retail_bias, funding_velocity, oi_trend, etc.)."""
    return analyze_sentiment(data_dict)

# Self‑test
if __name__ == "__main__":
    test = {
        "news_score": 0.7,
        "retail_bias": "Bearish_Extreme",
        "funding_velocity": -0.00008,
        "oi_trend": "falling",
        "price_change_pct": 1.2,
        "social_velocity": 1200,
        "oi_velocity_pct": -2.5
    }
    result = analyze_sentiment(test)
    print(result)