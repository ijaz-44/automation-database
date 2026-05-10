#!/usr/bin/env python3
# E06_macro_expert.py – Macroeconomic High‑Probability Scenario Detector (≥90% setups)
# Input: current macro data row (dict), optional previous row (dict) to compute changes.
# Output: bias, confidence, high_prob_scenario, signals, reason, net_score.

def analyze_macro(current_row, prev_row=None):
    """
    Args:
        current_row: dict with keys matching macro fields:
            timestamp, treasury_10y, treasury_2y, yield_spread,
            high_impact_count, vix, risk_premium,
            spy, qqq, dia, xauusd, usoil, dxy,
            seconds_to_next_event, next_event_title, is_volatile_zone
        prev_row: optional dict (same structure), used for computing changes.
    Returns:
        dict with:
            'bias': 'bullish'/'bearish'/'neutral'
            'confidence': int 0-100 (≥90 indicates high prob)
            'high_prob_scenario': 'UP'/'DOWN'/None
            'reason': str
            'signals': list of str
            'net_score': int
            'probability_estimate': int
    """
    signals = []
    bullish_score = 0
    bearish_score = 0

    # Helper to safely get value with fallback
    def get_val(row, key, default=0):
        return row.get(key, default) if row else default

    # Helper to compute absolute and relative change
    def get_change(current, previous, is_percent=True):
        if previous is None or previous == 0:
            return 0
        diff = current - previous
        if is_percent:
            return (diff / abs(previous)) * 100
        return diff

    # Extract current values
    yield_spread = get_val(current_row, 'yield_spread', 0)
    vix = get_val(current_row, 'vix', 15)
    risk_premium = get_val(current_row, 'risk_premium', 0)
    spy = get_val(current_row, 'spy', 500)
    qqq = get_val(current_row, 'qqq', 450)
    dia = get_val(current_row, 'dia', 400)
    gold = get_val(current_row, 'xauusd', 2300)
    oil = get_val(current_row, 'usoil', 70)
    dxy = get_val(current_row, 'dxy', 105)
    high_impact_count = get_val(current_row, 'high_impact_count', 0)
    seconds_to_event = get_val(current_row, 'seconds_to_next_event', 0)
    is_volatile = get_val(current_row, 'is_volatile_zone', False)

    # Changes (if previous row available)
    if prev_row:
        spy_change = get_change(spy, get_val(prev_row, 'spy'))
        qqq_change = get_change(qqq, get_val(prev_row, 'qqq'))
        dia_change = get_change(dia, get_val(prev_row, 'dia'))
        gold_change = get_change(gold, get_val(prev_row, 'xauusd'))
        oil_change = get_change(oil, get_val(prev_row, 'usoil'))
        dxy_change = get_change(dxy, get_val(prev_row, 'dxy'))
        vix_change = get_change(vix, get_val(prev_row, 'vix'))
    else:
        spy_change = qqq_change = dia_change = gold_change = oil_change = dxy_change = vix_change = 0

    # 1. Yield spread (inversion = bearish)
    if yield_spread < -0.3:
        bearish_score += 30
        signals.append(f"Deep yield inversion ({yield_spread:.2f}%) → recession fear, bearish")
    elif yield_spread < -0.1:
        bearish_score += 20
        signals.append(f"Yield inverted ({yield_spread:.2f}%) → bearish")
    elif yield_spread > 0.3:
        bullish_score += 20
        signals.append(f"Strong positive yield spread ({yield_spread:.2f}%) → bullish")
    elif yield_spread > 0.1:
        bullish_score += 10
        signals.append(f"Positive yield spread ({yield_spread:.2f}%) → bullish")
    else:
        signals.append(f"Neutral yield spread ({yield_spread:.2f}%)")

    # 2. VIX (fear index)
    if vix > 30:
        bearish_score += 25
        signals.append(f"Very high VIX ({vix:.1f}) → extreme fear, bearish")
    elif vix > 25:
        bearish_score += 15
        signals.append(f"High VIX ({vix:.1f}) → bearish")
    elif vix < 15:
        bullish_score += 15
        signals.append(f"Low VIX ({vix:.1f}) → complacency, bullish")
    elif vix < 20:
        bullish_score += 5
        signals.append(f"Moderate VIX ({vix:.1f}) → neutral-bullish")
    else:
        signals.append(f"Normal VIX ({vix:.1f})")

    # VIX momentum
    if vix_change > 10:
        bearish_score += 10
        signals.append("VIX spiking rapidly → fear rising")
    elif vix_change < -10:
        bullish_score += 10
        signals.append("VIX falling sharply → relief rally")

    # 3. Risk premium
    if risk_premium > 0:
        bullish_score += 10
        signals.append(f"Positive equity risk premium ({risk_premium:.2f}) → bullish")
    elif risk_premium < 0:
        bearish_score += 10
        signals.append(f"Negative risk premium ({risk_premium:.2f}) → bearish")

    # 4. Stock indices (using changes)
    if spy_change > 1:
        bullish_score += 15
        signals.append(f"SPY up {spy_change:.1f}% → bullish")
    elif spy_change < -1:
        bearish_score += 15
        signals.append(f"SPY down {spy_change:.1f}% → bearish")
    else:
        signals.append(f"SPY flat ({spy_change:+.1f}%)")

    if qqq_change > 1:
        bullish_score += 10
        signals.append(f"QQQ up {qqq_change:.1f}% → tech bullish")
    elif qqq_change < -1:
        bearish_score += 10
        signals.append(f"QQQ down {qqq_change:.1f}% → tech bearish")

    if dia_change > 1:
        bullish_score += 10
        signals.append(f"DIA up {dia_change:.1f}% → industrials bullish")
    elif dia_change < -1:
        bearish_score += 10
        signals.append(f"DIA down {dia_change:.1f}% → industrials bearish")

    # 5. Gold (safe haven)
    if gold_change > 1:
        bearish_score += 10
        signals.append(f"Gold up {gold_change:.1f}% → risk-off, bearish")
    elif gold_change < -1:
        bullish_score += 10
        signals.append(f"Gold down {gold_change:.1f}% → risk-on, bullish")
    else:
        signals.append(f"Gold flat ({gold_change:+.1f}%)")

    # 6. Oil (inflation proxy)
    if oil_change > 2:
        bearish_score += 10
        signals.append(f"Oil up {oil_change:.1f}% → inflation fear, bearish")
    elif oil_change < -2:
        bullish_score += 10
        signals.append(f"Oil down {oil_change:.1f}% → inflation easing, bullish")

    # 7. Dollar index (strong dollar = bearish for risk assets)
    if dxy_change > 0.5:
        bearish_score += 10
        signals.append(f"DXY up {dxy_change:.1f}% → strong dollar, bearish")
    elif dxy_change < -0.5:
        bullish_score += 10
        signals.append(f"DXY down {dxy_change:.1f}% → weak dollar, bullish")
    else:
        signals.append(f"DXY flat ({dxy_change:+.1f}%)")

    # 8. High‑impact events
    if high_impact_count > 5:
        # Many events → uncertainty, reduce confidence but don't bias strongly
        signals.append(f"{high_impact_count} high‑impact events ahead → high uncertainty")
        # No direct score, but confidence penalty later
    if is_volatile:
        signals.append("Next high‑impact event within 30min → elevated volatility")
        # Reduce directional confidence
    elif seconds_to_event < 3600 and seconds_to_event > 0:
        signals.append(f"High‑impact event in {seconds_to_event//60} minutes → potential volatility")
    else:
        signals.append("No immediate high‑impact events")

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

    # Adjust confidence downwards for high event count or volatile zone
    if high_impact_count > 5:
        confidence = max(30, confidence - 15)
    if is_volatile:
        confidence = max(30, confidence - 10)

    high_prob_scenario = None
    if confidence >= 90:
        high_prob_scenario = "UP" if bias == "bullish" else "DOWN" if bias == "bearish" else None

    reason = f"Net score {net:+d}, dominant signals: {signals[0] if signals else 'none'}"

    return {
        "bias": bias,
        "confidence": confidence,
        "high_prob_scenario": high_prob_scenario,
        "probability_estimate": confidence,
        "reason": reason,
        "signals": signals,
        "net_score": net
    }

# Convenience wrapper for X11: pass latest macro row and previous row (if exists)
def from_x11_row(current_row, prev_row=None):
    """Directly call analyze_macro with current and optional previous row."""
    return analyze_macro(current_row, prev_row)

# Self‑test example
if __name__ == "__main__":
    # Example current row (simulate)
    current = {
        "yield_spread": -0.25,
        "vix": 28.5,
        "risk_premium": 0.3,
        "spy": 510,
        "qqq": 460,
        "dia": 405,
        "xauusd": 2320,
        "usoil": 72,
        "dxy": 105.5,
        "high_impact_count": 3,
        "seconds_to_next_event": 1200,
        "is_volatile_zone": False
    }
    previous = {
        "spy": 505,
        "qqq": 455,
        "dia": 402,
        "xauusd": 2300,
        "usoil": 70,
        "dxy": 104.8,
        "vix": 25
    }
    result = analyze_macro(current, previous)
    print(result)