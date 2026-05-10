#!/usr/bin/env python3
# E12_onchain_expert.py – On‑Chain High‑Probability Scenario Detector (≥90%)
# Input: dict with on‑chain metrics (from X23)
# Output: bias, confidence, high_prob_scenario, signals, net_score, reason.

import math

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

    # 1. Stablecoin netflow (most important)
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

    # 2. Whale ratio (top trader positioning)
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

    # 3. Taker ratio (aggressive order flow)
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

    # 4. Funding rate (contrarian)
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

    # 5. Depth imbalance
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

    # 6. Exchange netflow (recent)
    exchange_netflow = data.get('exchange_netflow', [])
    if exchange_netflow:
        latest = exchange_netflow[0]['netflow'] if isinstance(exchange_netflow[0], dict) else 0
        if latest > 0:
            bearish_score += 15    # coins moving to exchanges = selling pressure
            signals.append(f"Positive exchange netflow ({latest:.0f}) → coins in, bearish")
        elif latest < 0:
            bullish_score += 15
            signals.append(f"Negative exchange netflow ({latest:.0f}) → coins out, bullish")

    # 7. Whale transactions count
    whale_tx_count = data.get('whale_transactions_count', 0)
    if whale_tx_count > 50:
        signals.append(f"Very high whale activity ({whale_tx_count} transactions)")
        # high whale activity can be both bullish and bearish; we add a small boost to the dominant side
        if bullish_score > bearish_score:
            bullish_score += 10
        elif bearish_score > bullish_score:
            bearish_score += 10
    elif whale_tx_count > 20:
        signals.append(f"Moderate whale activity ({whale_tx_count})")

    # 8. Liquidations count – capitulation signals
    liquidations = data.get('liquidations', [])
    liq_count = len(liquidations) if liquidations else 0
    if liq_count > 10:
        signals.append(f"High liquidation count ({liq_count}) → potential trend exhaustion")
        # after heavy liquidations, price often reverses. So we give bias to the opposite of current trend? But we don't have current trend.
        # We'll just add a small boost to both sides? Actually heavy liquidations can be a sign of panic selling (bearish) or short squeeze (bullish).
        # We'll add 5 to both sides to increase confidence but not directional.
        if liq_count > 20:
            bullish_score += 5
            bearish_score += 5
    elif liq_count > 5:
        signals.append(f"Moderate liquidations ({liq_count})")

    # 9. Stablecoin overall bias (from any stablecoin having positive netflow)
    if data.get('stablecoin_bullish', False):
        bullish_score += 10
        signals.append("Stablecoin aggregate bullish (net inflow)")
    else:
        bearish_score += 5
        signals.append("Stablecoin aggregate bearish (net outflow)")

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

    # High‑probability scenario only if confidence ≥ 90
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

# Convenience wrapper: extract from X23's final data dict (the one used to build TOON)
def from_x23_data_dict(data_dict):
    """Directly use the data dict built by X23 (after computing all values)."""
    return analyze_onchain(data_dict)

# Self-test
if __name__ == "__main__":
    test = {
        "usdt_netflow": 150_000_000,
        "whale_ratio": 1.4,
        "taker_ratio": 1.3,
        "funding_rate": -0.00015,
        "depth_imbalance": 0.35,
        "stablecoin_bullish": True,
        "liquidations": [1]*12,
        "exchange_netflow": [{"netflow": -50_000_000}],
        "whale_transactions_count": 35
    }
    result = analyze_onchain(test)
    print(result)