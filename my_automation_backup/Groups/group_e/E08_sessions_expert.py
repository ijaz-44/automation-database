#!/usr/bin/env python3
# E08_sessions_expert.py – Session Intelligence High‑Probability Scenario Detector (≥90%)
# Input: dict with session data (kill zones, economic calendar, volatility, bias, IB, danger zone)
# Output: bullish/bearish/neutral, confidence (0‑100, >=90 triggers high_prob), signals, etc.

def analyze_sessions(data):
    """
    Args:
        data: dict with keys:
            - session_bias: str ('Strong_Bullish', 'Strong_Bearish', 'Neutral')
            - previous_session_high: float
            - previous_session_low: float
            - news_danger_zone: bool
            - london_kill_zone_active: bool (optional, if current time within London kill zone)
            - ny_kill_zone_active: bool (optional)
            - london_initial_balance: (high, low) tuple or None
            - ny_initial_balance: (high, low) tuple or None
            - volatility_profile: dict {hour: avg_volatility_pct}
            - current_price: float (optional, can be derived from symbol; if missing, skip price‑relative signals)
            - symbol: str (optional, for logging)
            - last_1h_candles: list of dicts with 'high','low','close' (optional)
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

    # 1. Session bias (from price relative to last 8 candles)
    bias = data.get('session_bias', 'Neutral')
    if bias == 'Strong_Bullish':
        bullish_score += 30
        signals.append("Price above last 8‑candle highs → strong bullish bias")
    elif bias == 'Strong_Bearish':
        bearish_score += 30
        signals.append("Price below last 8‑candle lows → strong bearish bias")
    else:
        signals.append("Neutral price position within range")

    # 2. Previous session high/low as liquidity levels (magnets)
    prev_high = data.get('previous_session_high')
    prev_low = data.get('previous_session_low')
    current_price = data.get('current_price')
    if current_price and prev_high and prev_low:
        dist_to_high = (prev_high - current_price) / current_price * 100 if current_price else 100
        dist_to_low = (current_price - prev_low) / current_price * 100 if current_price else 100
        if 0 < dist_to_high < 0.5:
            bullish_score += 15
            signals.append(f"Price within 0.5% of previous session high → potential breakout")
        elif 0 < dist_to_low < 0.5:
            bearish_score += 15
            signals.append(f"Price within 0.5% of previous session low → potential breakdown")
    else:
        signals.append("Previous session levels not used (no current price)")

    # 3. News danger zone (high‑impact event within 15 min)
    if data.get('news_danger_zone', False):
        # Danger zone increases uncertainty; reduce confidence, but not directional
        signals.append("High‑impact news within 15 minutes → high volatility expected")
        # No direct score, but adjust confidence later
        danger_zone_active = True
    else:
        danger_zone_active = False

    # 4. Kill zone active (London or NY)
    london_active = data.get('london_kill_zone_active', False)
    ny_active = data.get('ny_kill_zone_active', False)
    if london_active:
        signals.append("London kill zone active (2‑5 AM EST) → potential momentum")
        bullish_score += 5
        bearish_score += 5   # both sides possible, but small boost to both
    if ny_active:
        signals.append("New York kill zone active (8‑11 AM EST) → potential momentum")
        bullish_score += 5
        bearish_score += 5

    # 5. Initial Balance (IB) levels – breakouts/breakdowns are significant
    london_ib_high, london_ib_low = data.get('london_initial_balance', (None, None))
    ny_ib_high, ny_ib_low = data.get('ny_initial_balance', (None, None))
    if current_price:
        # For London IB (if available)
        if london_ib_high and london_ib_low:
            if current_price > london_ib_high:
                bullish_score += 20
                signals.append(f"Price above London IB high ({london_ib_high}) → breakout bullish")
            elif current_price < london_ib_low:
                bearish_score += 20
                signals.append(f"Price below London IB low ({london_ib_low}) → breakdown bearish")
            else:
                signals.append("Price inside London Initial Balance range → chop")
        # For New York IB
        if ny_ib_high and ny_ib_low:
            if current_price > ny_ib_high:
                bullish_score += 20
                signals.append(f"Price above NY IB high ({ny_ib_high}) → breakout bullish")
            elif current_price < ny_ib_low:
                bearish_score += 20
                signals.append(f"Price below NY IB low ({ny_ib_low}) → breakdown bearish")
            else:
                signals.append("Price inside NY Initial Balance range → chop")

    # 6. Volatility profile (current hour vs average)
    vol_profile = data.get('volatility_profile', {})
    if vol_profile and current_price is not None:
        now_lahore_hour = getattr(data, '_now_hour', None)  # we could pass current hour; if not, skip
        # For simplicity, we'll assume we can get current hour from data if needed
        # If not provided, skip volatility adjustment.
        current_hour = data.get('current_hour_lahore')
        if current_hour is not None and current_hour in vol_profile:
            avg_vol = vol_profile[current_hour]
            # High volatility (>0.5%) suggests strong moves and potential breakouts
            if avg_vol > 0.5:
                signals.append(f"Current hour volatility {avg_vol:.2f}% above threshold → potential breakout")
                # Boost both bullish and bearish scores slightly (momentum can be either direction)
                bullish_score += 5
                bearish_score += 5
            else:
                signals.append(f"Current hour volatility {avg_vol:.2f}% → normal")

    # 7. Net score and bias
    net = bullish_score - bearish_score
    net = max(-100, min(100, net))

    if net >= 30:
        bias_out = "bullish"
        confidence = min(95, 60 + net // 2)
    elif net <= -30:
        bias_out = "bearish"
        confidence = min(95, 60 + abs(net) // 2)
    else:
        bias_out = "neutral"
        confidence = 50 + net // 2 if net else 50

    # Adjust confidence down if danger zone is active and net score is not extreme
    if danger_zone_active and abs(net) < 40:
        confidence = max(30, confidence - 15)

    # High‑probability scenario only if confidence ≥ 90 and not neutral
    high_prob = None
    if confidence >= 90 and bias_out != "neutral":
        high_prob = "UP" if bias_out == "bullish" else "DOWN"

    reason = f"Net score {net:+d}, signals: {signals[0] if signals else 'no clear signals'}"

    return {
        "bias": bias_out,
        "confidence": confidence,
        "high_prob_scenario": high_prob,
        "probability_estimate": confidence,
        "reason": reason,
        "signals": signals,
        "net_score": net
    }

# Convenience wrapper for X15 to convert its TOON data into a dict
def from_x15_toon_data(toon_data_dict):
    """
    Expects a dict built from X15's TOON file (parsed). Keys:
        - session_bias, previous_session_high, previous_session_low, news_danger_zone,
        - london_kill_zone_active, ny_kill_zone_active,
        - london_initial_balance (tuple), ny_initial_balance,
        - volatility_profile (dict), current_price, current_hour_lahore (optional)
    """
    return analyze_sessions(toon_data_dict)

def from_x15_data_dict(data_dict):
    """Directly pass dictionary from X15 (if X15 is modified to return dict)."""
    return analyze_sessions(data_dict)

# Self‑test if run directly
if __name__ == "__main__":
    test = {
        "session_bias": "Strong_Bullish",
        "previous_session_high": 64200,
        "previous_session_low": 63800,
        "news_danger_zone": False,
        "london_kill_zone_active": True,
        "ny_kill_zone_active": False,
        "london_initial_balance": (64050, 63900),
        "ny_initial_balance": None,
        "volatility_profile": {14: 0.45, 15: 0.52},
        "current_price": 64150,
        "current_hour_lahore": 15
    }
    result = analyze_sessions(test)
    print(result)