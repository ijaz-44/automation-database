#!/usr/bin/env python3
# E05_correlation_expert.py – Correlation High‑Probability Scenario Detector (≥90% setups)
# Input: dict with correlation data for a symbol (from X09)
# Output: bias, confidence, high_prob_scenario, signals, reason, etc.

import math

def analyze_correlation(corr_data):
    """
    Args:
        corr_data: dict with keys:
            - 'current_correlations': dict mapping index name to correlation coefficient (e.g., 0.65)
            - 'momentums': dict mapping index name to momentum (percentage change of correlation)
            - 'z_scores': dict mapping index name to z‑score of correlation
            - (Optional) 'primary_bias' from other sources (not used directly)
    Returns:
        dict with:
            'bias': 'bullish'/'bearish'/'neutral'
            'confidence': int 0-100 (≥90 indicates high prob)
            'high_prob_scenario': 'UP'/'DOWN'/None
            'reason': str
            'signals': list of str
            'net_score': int
    """
    signals = []
    bullish_score = 0
    bearish_score = 0

    # Define weights for each index (importance)
    # Positive weight means positive correlation with the index → bullish when index up
    # But here we only have correlation value, not the index's own movement.
    # We need to infer direction from correlation sign: positive correlation means the symbol moves with the index.
    # But we don't know the index's direction. Instead, we rely on the fact that the market might be risk-on/off.
    # Better: Use correlation as an independent signal:
    #   Strong positive correlation with SPY, QQQ, DIA → risk-on, bullish if those indices are up (but we don't have their change).
    #   Strong negative correlation with VIX → risk-off, bullish if VIX down.
    # Since we don't have the change of indices, we rely on the fact that extreme correlation values (z-score) may be unsustainable.
    # So we'll use:
    # - Persistent high positive correlation with SPY/QQQ/DIA → bullish (provided no divergence)
    # - Persistent high negative correlation with VIX → bullish
    # - Extreme z-score ( >2 or < -2) may indicate overextended correlation → potential reversal
    # - Momentum of correlation (if correlation is rapidly increasing) may indicate strengthening trend.

    # We'll treat the correlations as features and compute a net score.

    # Define index groups and their directional impact
    # Positive impact: high positive correlation → bullish, high negative correlation → bearish
    # For VIX: high positive correlation → bearish (since VIX up = market fear)
    # For DXY (dollar): high positive correlation → bearish for risk assets? Generally dollar up = risk assets down.
    # We'll keep it simple: positive correlation with risky assets (SPY, QQQ, DIA) → bullish
    # negative correlation with risky assets → bearish
    # For VIX, positive correlation is bearish; negative correlation is bullish.

    risky_assets = ['SPY', 'QQQ', 'DIA']
    safe_haven = ['GLD', 'USO']   # commodities, ambiguous; we'll treat as neutral
    dollar = ['DXY']              # strong positive dollar = bearish for crypto/stocks
    volatility = ['VIX']          # high positive VIX = bearish
    crypto_peers = ['BTC', 'ETH', 'BTCDOM', 'USDT_BTC']  # BTC/ETH correlation: high positive = bullish for the symbol if it's also crypto? Actually the symbol might be BTC or ETH itself. But we'll treat as: if symbol is not itself, strong correlation with BTC is bullish when BTC is up (but we don't have BTC direction). So we'll use it as a co‑movement signal: high positive correlation means the symbol tends to move with the peer, so if the peer had a recent strong move, we'd need that data. Without it, we'll just use the correlation magnitude as an indication of market alignment, but not directional.

    # To avoid overcomplicating, we'll focus on the correlation values themselves and their z-scores.

    current = corr_data.get('current_correlations', {})
    z_scores = corr_data.get('z_scores', {})
    momentums = corr_data.get('momentums', {})

    # 1. SPY, QQQ, DIA (risk appetite)
    for idx in risky_assets:
        corr = current.get(idx, 0)
        z = z_scores.get(idx, 0)
        mom = momentums.get(idx, 0)
        # Strong positive correlation ( >0.5 ) adds bullish score
        if corr > 0.5:
            bull_bonus = 15
            if z > 2:    # extremely high correlation might be unsustainable → reduce confidence
                bull_bonus -= 5
            if mom > 5:  # correlation increasing rapidly → strengthening trend
                bull_bonus += 5
            bullish_score += bull_bonus
            signals.append(f"{idx} strong positive correlation ({corr:.2f})")
        elif corr < -0.5:
            bear_bonus = 15
            if z < -2:
                bear_bonus -= 5
            if mom < -5:
                bear_bonus += 5
            bearish_score += bear_bonus
            signals.append(f"{idx} strong negative correlation ({corr:.2f})")
        else:
            signals.append(f"{idx} weak correlation ({corr:.2f})")

    # 2. VIX (volatility index)
    vix_corr = current.get('VIX', 0)
    vix_z = z_scores.get('VIX', 0)
    vix_mom = momentums.get('VIX', 0)
    if vix_corr > 0.5:
        bearish_score += 15
        signals.append(f"VIX positive correlation ({vix_corr:.2f}) → bearish")
    elif vix_corr < -0.5:
        bullish_score += 15
        signals.append(f"VIX negative correlation ({vix_corr:.2f}) → bullish")
    else:
        signals.append(f"VIX neutral ({vix_corr:.2f})")

    # 3. DXY (dollar index) – high positive DXY → bearish for risk assets
    dxy_corr = current.get('DXY', 0)
    if dxy_corr > 0.4:
        bearish_score += 10
        signals.append(f"Dollar positive correlation ({dxy_corr:.2f}) → bearish")
    elif dxy_corr < -0.4:
        bullish_score += 10
        signals.append(f"Dollar negative correlation ({dxy_corr:.2f}) → bullish")
    else:
        signals.append(f"Dollar neutral ({dxy_corr:.2f})")

    # 4. Crypto peers (BTC, ETH, BTCDOM, USDT_BTC)
    # If symbol is itself, these might be the same, but we still use them as they represent broader crypto sentiment.
    crypto_peer_corrs = []
    for idx in crypto_peers:
        corr = current.get(idx, 0)
        if corr != 0:
            crypto_peer_corrs.append(corr)
    if crypto_peer_corrs:
        avg_crypto_corr = sum(crypto_peer_corrs) / len(crypto_peer_corrs)
        if avg_crypto_corr > 0.5:
            bullish_score += 15
            signals.append(f"Strong average crypto correlation ({avg_crypto_corr:.2f}) → bullish")
        elif avg_crypto_corr < -0.5:
            bearish_score += 15
            signals.append(f"Strong average crypto correlation ({avg_crypto_corr:.2f}) → bearish")
        else:
            signals.append(f"Moderate crypto correlation ({avg_crypto_corr:.2f})")

    # 5. Extreme z-scores (reversal signals)
    for idx, z in z_scores.items():
        if abs(z) > 2.5:
            if z > 2.5 and current.get(idx, 0) > 0:
                # extremely high positive correlation, may reverse → bearish
                bearish_score += 10
                signals.append(f"Extreme positive z-score for {idx} ({z:.1f}) → possible bearish reversal")
            elif z < -2.5 and current.get(idx, 0) < 0:
                bullish_score += 10
                signals.append(f"Extreme negative z-score for {idx} ({z:.1f}) → possible bullish reversal")

    # Net score
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

# Convenience function to parse from X09's TOON file (optional)
def from_x09_toon_file(symbol, base_dir=None):
    """Parse X09's generated TOON file and return analysis."""
    if base_dir is None:
        base_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "market_data", "binance", "symbols")
    toon_path = os.path.join(base_dir, f"{symbol.lower()}_correlation.toon")
    if not os.path.exists(toon_path):
        return {"error": "Correlation TOON not found", "bias": "neutral", "confidence": 0}
    # Simpler: read the latest row of correlation_data
    import re
    with open(toon_path, 'r') as f:
        content = f.read()
    # Find the correlation_data block
    pattern = r'correlation_data\[\d+\]\{[^}]+\}:\s*\n(?:\s+([^\n]+(?:\n\s+[^\n]+)*))?'
    match = re.search(pattern, content, re.DOTALL)
    if not match:
        return {"error": "No correlation data found", "bias": "neutral", "confidence": 0}
    rows_text = match.group(1)
    if not rows_text:
        return {"error": "Empty correlation data", "bias": "neutral", "confidence": 0}
    # Get the last row (most recent)
    rows = rows_text.split(' | ')
    last_row = rows[-1].strip().split(',')
    # Fields order: timestamp, data_points, BTC_corr_15m, BTC_momentum_15m, BTC_zscore_15m, ...
    # Hardcode field indices (simplify)
    indices = ['BTC','ETH','BTCDOM','USDT_BTC','SPY','QQQ','DIA','GLD','USO','DXY','VIX']
    current_corr = {}
    z_scores = {}
    momentums = {}
    for i, idx in enumerate(indices):
        base = 2 + i*3  # fields start after timestamp, data_points
        if len(last_row) > base+2:
            current_corr[idx] = float(last_row[base])
            momentums[idx] = float(last_row[base+1])
            z_scores[idx] = float(last_row[base+2])
    return analyze_correlation({
        'current_correlations': current_corr,
        'z_scores': z_scores,
        'momentums': momentums
    })

# Self-test
if __name__ == "__main__":
    # Simulate X09 output
    test_corr = {
        'current_correlations': {'SPY': 0.72, 'QQQ': 0.68, 'DIA': 0.55, 'VIX': -0.45, 'DXY': 0.12, 'BTC': 0.85, 'ETH': 0.80},
        'z_scores': {'SPY': 1.2, 'QQQ': 1.1, 'DIA': 0.9, 'VIX': -1.8, 'DXY': 0.2, 'BTC': 2.1, 'ETH': 1.9},
        'momentums': {'SPY': 0.5, 'QQQ': 0.3, 'DIA': 0.1, 'VIX': -0.2, 'DXY': 0.0, 'BTC': 0.8, 'ETH': 0.6}
    }
    result = analyze_correlation(test_corr)
    print(result)