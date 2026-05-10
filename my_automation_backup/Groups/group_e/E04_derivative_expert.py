#!/usr/bin/env python3
# E04_derivative_expert.py – Derivative State Engine (Production‑Ready)
# Provides structured semantic states for X07. No file I/O.

import math
import time
from collections import deque

# ----------------------------- Helper: Timeframe‑aware thresholds -----------------------------
PRICE_THRESHOLDS = {
    '1m': 0.15, '5m': 0.3, '15m': 0.5, '1h': 1.0, '4h': 2.0
}
OI_THRESHOLDS = {
    '1m': 0.5, '5m': 1.0, '15m': 1.5, '1h': 2.0, '4h': 3.0
}

# ----------------------------- Helper: Exponential moving average -----------------------------
def ema(values, alpha=0.3):
    if not values:
        return 0.0
    e = values[0]
    for v in values[1:]:
        e = alpha * v + (1 - alpha) * e
    return e

# ----------------------------- Helper: Funding Z‑score (exponential decay) -----------------------------
def funding_zscore(current_rate, history, window=30, decay_alpha=0.1):
    """Return (zscore, mean, std) using exponentially weighted recent funding rates."""
    if not history or len(history) < 5:
        return 0.0, 0.0, 1.0
    # select last window, but weigh recent more
    recent = list(history[-window:]) if len(history) >= window else history[:]
    # apply exponential decay: weight = (1 - decay_alpha)^(age)
    weights = []
    values = []
    for i, h in enumerate(recent):
        age = len(recent) - 1 - i
        w = (1 - decay_alpha) ** age
        weights.append(w)
        values.append(h['funding_rate'])
    total_w = sum(weights)
    if total_w == 0:
        return 0.0, 0.0, 1.0
    mean = sum(w * v for w, v in zip(weights, values)) / total_w
    var = sum(w * (v - mean)**2 for w, v in zip(weights, values)) / total_w
    std = math.sqrt(var) if var > 0 else 1e-6
    # floor std to avoid extreme z when market is extremely calm
    std = max(std, 1e-5)
    z = (current_rate - mean) / std
    return z, mean, std

# ----------------------------- Helper: Linear regression slope on (timestamp, value) -----------------------------
def time_slope(pairs):
    """pairs: list of (timestamp_ms, value). Returns slope per second."""
    if len(pairs) < 2:
        return 0.0
    # convert timestamps to seconds (relative)
    t0 = pairs[0][0] / 1000.0
    x = [(p[0] / 1000.0 - t0) for p in pairs]
    y = [p[1] for p in pairs]
    n = len(x)
    sum_x = sum(x)
    sum_y = sum(y)
    sum_xy = sum(x[i] * y[i] for i in range(n))
    sum_x2 = sum(xi * xi for xi in x)
    denom = n * sum_x2 - sum_x * sum_x
    if denom == 0:
        return 0.0
    return (n * sum_xy - sum_x * sum_y) / denom

# ----------------------------- Helper: OI percentile (relative to last 30 days) -----------------------------
def oi_percentile(current_oi, oi_history, days=30):
    # assume oi_history sorted ascending, each entry has 'timestamp' and 'oi_value'
    if not oi_history:
        return 50.0
    age_limit = days * 24 * 3600 * 1000  # milliseconds
    now = time.time() * 1000
    recent = [h['oi_value'] for h in oi_history if now - h['timestamp'] <= age_limit]
    if not recent:
        return 50.0
    below = sum(1 for v in recent if v < current_oi)
    return (below / len(recent)) * 100

# ----------------------------- Helper: Detect cascading liquidation clusters -----------------------------
def cascade_risk(liquidation_levels, spot_price, atr_pct):
    """Return risk level: 'NONE', 'LOW', 'MEDIUM', 'HIGH' if multiple clusters within 1 ATR."""
    if not liquidation_levels:
        return "NONE"
    atr_dist = atr_pct * 0.01 * spot_price if atr_pct else spot_price * 0.015
    # count clusters within 2 ATR distance
    clusters_sorted = sorted(liquidation_levels, key=lambda x: x[0])
    cascades = 0
    for i in range(len(clusters_sorted)-1):
        if clusters_sorted[i+1][0] - clusters_sorted[i][0] < atr_dist * 0.5:
            cascades += 1
    if cascades >= 3:
        return "HIGH"
    if cascades >= 2:
        return "MEDIUM"
    if cascades >= 1:
        return "LOW"
    return "NONE"

# ----------------------------- Main analysis (single public entry point) -----------------------------
def analyze_derivative(spot_price, mark_price, funding_rate,
                       oi_current, oi_change_pct, price_change_pct,
                       ls_ratio, ls_history,
                       oi_history, funding_history, liquidation_levels,
                       current_timestamp_ms, atr_pct=None, timeframe='1h'):
    """
    Returns comprehensive derivative state dictionary.
    All timestamps must be in milliseconds since epoch.
    """
    # ----------------------------- 1. Data quality & staleness -----------------------------
    if spot_price <= 0 or oi_current <= 0:
        return {"error": "INVALID_DATA", "conviction": 0, "uncertainty": "INVALID_DATA", "base_state": "UNKNOWN"}

    if oi_history:
        latest_oi_ts = max(h['timestamp'] for h in oi_history)
        age_sec = (current_timestamp_ms - latest_oi_ts) / 1000
        if age_sec > 300:   # older than 5 minutes
            return {"error": "STALE_DATA", "conviction": 0, "uncertainty": "STALE_DATA", "base_state": "UNKNOWN"}

    # ----------------------------- 2. Timeframe‑aware thresholds -----------------------------
    price_th = PRICE_THRESHOLDS.get(timeframe, 0.5)
    oi_th = OI_THRESHOLDS.get(timeframe, 1.0)
    price_up = price_change_pct > price_th
    price_down = price_change_pct < -price_th
    price_flat = not price_up and not price_down
    oi_up = oi_change_pct > oi_th
    oi_down = oi_change_pct < -oi_th

    # ----------------------------- 3. L/S ratio normalization (relative) -----------------------------
    if ls_history and len(ls_history) >= 5:
        recent_ratios = [h['long_short_ratio'] for h in ls_history[-24:]]
        avg_ls = sum(recent_ratios) / len(recent_ratios) if recent_ratios else 0.5
    else:
        avg_ls = 0.5
    ls_ratio_norm = ls_ratio / avg_ls if avg_ls > 0 else 1.0
    if ls_ratio_norm > 1.35:
        crowd_position = "OVERLONG"
    elif ls_ratio_norm < 0.7:
        crowd_position = "OVERSHORT"
    else:
        crowd_position = "NEUTRAL"

    # ----------------------------- 4. Base market state (OI + price) -----------------------------
    if price_up and oi_up:
        base_state = "LONG_BUILDUP"
        continuation_bias = "CONTINUATION_FAVORED"
        reversal_risk = "LOW"
        squeeze_risk = "LOW"
    elif price_down and oi_up:
        base_state = "SHORT_BUILDUP"
        continuation_bias = "CONTINUATION_FAVORED"
        reversal_risk = "LOW"
        squeeze_risk = "LOW"
    elif price_up and oi_down:
        base_state = "SHORT_COVERING_RALLY"
        continuation_bias = "MEAN_REVERSION_RISK"
        reversal_risk = "MEDIUM"
        squeeze_risk = "MEDIUM"
    elif price_down and oi_down:
        base_state = "LONG_LIQUIDATION"
        continuation_bias = "MEAN_REVERSION_RISK"
        reversal_risk = "MEDIUM"
        squeeze_risk = "MEDIUM"
    elif price_flat and oi_up:
        base_state = "OI_BUILDUP_NO_PRICE_MOVE"
        continuation_bias = "EXHAUSTION_RISK"
        reversal_risk = "HIGH"
        squeeze_risk = "LOW"
    else:
        base_state = "NEUTRAL_CHOP"
        continuation_bias = "MEAN_REVERSION_RISK"
        reversal_risk = "MEDIUM"
        squeeze_risk = "LOW"

    # ----------------------------- 5. OI Velocity & Acceleration (time‑normalized) -----------------------------
    oi_velocity_pct = 0.0
    oi_acceleration = 0.0
    trend_strength = 40   # baseline
    if oi_history and len(oi_history) >= 5:
        # use last 12 points with timestamps
        pairs = [(h['timestamp'], h['oi_value']) for h in oi_history[-12:]]
        if len(pairs) >= 3:
            slope = time_slope(pairs)          # OI change per second
            # compute average OI (not strictly needed for normalised rate)
            avg_oi = sum(p[1] for p in pairs) / len(pairs)
            oi_velocity_pct = (slope / avg_oi) * 100 if avg_oi != 0 else 0
            # acceleration: slope of slopes
            if len(pairs) >= 6:
                half = len(pairs) // 2
                slope1 = time_slope(pairs[:half])
                slope2 = time_slope(pairs[half:])
                oi_acceleration = slope2 - slope1
            # trend strength based on velocity absolute value (calibrated)
            trend_strength = min(85, 40 + abs(oi_velocity_pct) * 2)

    # ----------------------------- 6. Funding Z‑score (exponential decay) -----------------------------
    funding_z, funding_mean, funding_std = funding_zscore(funding_rate, funding_history, window=30, decay_alpha=0.1)
    abs_funding_z = abs(funding_z)
    if abs_funding_z > 2.0:
        if base_state in ("LONG_BUILDUP", "SHORT_BUILDUP"):
            squeeze_risk = "HIGH"
            reversal_risk = "HIGH"
        else:
            squeeze_risk = "MEDIUM"
            reversal_risk = "MEDIUM"
    elif abs_funding_z > 1.0:
        if base_state in ("LONG_BUILDUP", "SHORT_BUILDUP"):
            squeeze_risk = "MEDIUM"
            reversal_risk = "MEDIUM"

    # ----------------------------- 7. Basis (mark – spot) -----------------------------
    basis_pct = (mark_price - spot_price) / spot_price * 100 if spot_price != 0 else 0
    basis_state = "NEUTRAL"
    if basis_pct > 0.2:
        basis_state = "PREMIUM"
    elif basis_pct < -0.2:
        basis_state = "DISCOUNT"

    # ----------------------------- 8. OI percentile (30‑day) -----------------------------
    oi_percentile_val = oi_percentile(oi_current, oi_history, days=30)
    oi_crowded = "NORMAL"
    if oi_percentile_val > 90:
        oi_crowded = "EXTREME_HIGH"
    elif oi_percentile_val < 10:
        oi_crowded = "EXTREME_LOW"

    # ----------------------------- 9. Liquidation pressure: magnet, barrier, cascade risk -----------------------------
    liq_magnet_bias = 0.0  # positive = more volume below (potential support)
    liq_barrier = "NONE"
    if liquidation_levels:
        above_vol = 0
        below_vol = 0
        for price, vol in liquidation_levels:
            if price > spot_price:
                above_vol += vol
            else:
                below_vol += vol
        total_liq = above_vol + below_vol
        if total_liq > 0:
            liq_magnet_bias = (below_vol - above_vol) / total_liq   # >0 more volume below
        # proximity (ATR normalized)
        atr = atr_pct if atr_pct is not None else 1.5
        near_liq = False
        for price, vol in liquidation_levels:
            dist_pct = abs((price - spot_price) / spot_price) * 100
            if dist_pct < atr * 0.8:
                near_liq = True
                if price > spot_price:
                    liq_barrier = "RESISTANCE_ABOVE"
                else:
                    liq_barrier = "SUPPORT_BELOW"
                break
        cascade_risk_level = cascade_risk(liquidation_levels, spot_price, atr_pct)
    else:
        liq_magnet_bias = 0.0
        liq_barrier = "NONE"
        cascade_risk_level = "NONE"

    # ----------------------------- 10. Volatility state -----------------------------
    if abs(oi_change_pct) > 3 or abs(funding_z) > 1.5:
        volatility_state = "EXPANDING"
    elif abs(oi_change_pct) < 0.5 and abs(funding_z) < 0.5:
        volatility_state = "CONTRACTING"
    else:
        volatility_state = "NORMAL"

    # ----------------------------- 11. Bias scoring (avoid double counting) -----------------------------
    bias_score = 0
    if base_state in ("LONG_BUILDUP", "SHORT_COVERING_RALLY"):
        bias_score += 30
    elif base_state in ("SHORT_BUILDUP", "LONG_LIQUIDATION"):
        bias_score -= 30
    bias_score += liq_magnet_bias * 20
    if crowd_position == "OVERLONG" and funding_z > 1.5 and reversal_risk == "HIGH":
        bias_score -= 15
    elif crowd_position == "OVERSHORT" and funding_z < -1.5 and reversal_risk == "HIGH":
        bias_score += 15
    bias_score = max(-100, min(100, bias_score))
    if bias_score > 20:
        bias = "bullish"
    elif bias_score < -20:
        bias = "bearish"
    else:
        bias = "neutral"

    # ----------------------------- 12. Weighted agreement & conviction -----------------------------
    max_agreement = 3+2+2+1
    agree_score = 0
    if (price_up and oi_up) or (price_down and oi_down):
        agree_score += 3
    if (bias == "bullish" and funding_z <= 0.5) or (bias == "bearish" and funding_z >= -0.5):
        agree_score += 2
    if (bias == "bullish" and liq_magnet_bias > 0) or (bias == "bearish" and liq_magnet_bias < 0):
        agree_score += 2
    if (crowd_position == "OVERLONG" and bias == "bearish") or (crowd_position == "OVERSHORT" and bias == "bullish"):
        agree_score += 1
    weighted_agreement = (agree_score / max_agreement) * 100

    # Conviction (do NOT include squeeze_risk as positive)
    conviction = 50 + (weighted_agreement / 2) - (10 if reversal_risk == "HIGH" else 0)
    conviction = max(30, min(85, conviction))

    # ----------------------------- 13. Uncertainty decomposition -----------------------------
    data_uncertainty = "LOW"
    if age_sec > 90:
        data_uncertainty = "HIGH"
    elif age_sec > 30:
        data_uncertainty = "MEDIUM"
    signal_conflict = "LOW"
    if (base_state in ("LONG_BUILDUP", "SHORT_COVERING_RALLY") and bias == "bearish") or \
       (base_state in ("SHORT_BUILDUP", "LONG_LIQUIDATION") and bias == "bullish"):
        signal_conflict = "HIGH"
    elif weighted_agreement < 50:
        signal_conflict = "MEDIUM"
    regime_uncertainty = "LOW"
    if volatility_state == "EXPANDING" and squeeze_risk == "HIGH":
        regime_uncertainty = "MEDIUM"

    # ----------------------------- 14. Feature importance (dominant signal) -----------------------------
    dominant_signal = "NEUTRAL"
    if base_state in ("LONG_BUILDUP", "SHORT_BUILDUP"):
        dominant_signal = "OI_PRICE_TREND"
    elif abs(funding_z) > 1.5:
        dominant_signal = "FUNDING_EXTREME"
    elif liq_barrier != "NONE":
        dominant_signal = "LIQUIDATION_BARRIER"
    elif crowd_position != "NEUTRAL":
        dominant_signal = "CROWD_EXTREME"

    # ----------------------------- 15. Expected move (semantic) -----------------------------
    if bias == "bullish":
        exp_dir = "UP"
        exp_strength = min(10, max(1, int(trend_strength // 10)))
    elif bias == "bearish":
        exp_dir = "DOWN"
        exp_strength = min(10, max(1, int(trend_strength // 10)))
    else:
        exp_dir = "NEUTRAL"
        exp_strength = 5
    if timeframe in ('1m','5m'):
        horizon = "1-2 candles"
    elif timeframe in ('15m','1h'):
        horizon = "2-4 candles"
    else:
        horizon = "4-6 candles"

    # ----------------------------- 16. Raw features (for future ML) -----------------------------
    raw_features = {
        "funding_zscore": round(funding_z, 2),
        "ls_ratio_norm": round(ls_ratio_norm, 3),
        "oi_velocity_pct": round(oi_velocity_pct, 2),
        "oi_acceleration": round(oi_acceleration, 4),
        "liquidation_magnet_bias": round(liq_magnet_bias, 2),
        "liquidation_barrier": liq_barrier,
        "weighted_agreement": round(weighted_agreement, 1),
        "oi_percentile": round(oi_percentile_val, 1),
        "basis_pct": round(basis_pct, 2),
        "cascade_risk": cascade_risk_level
    }

    # ----------------------------- 17. Output -----------------------------
    return {
        "base_state": base_state,
        "modifier": liq_barrier if liq_barrier != "NONE" else "NONE",
        "bias": bias,
        "conviction": conviction,
        "uncertainty": "CLEAR_SIGNAL" if weighted_agreement > 40 else "LOW_QUALITY_SIGNAL",
        "data_uncertainty": data_uncertainty,
        "signal_conflict": signal_conflict,
        "regime_uncertainty": regime_uncertainty,
        "expected_move_direction": exp_dir,
        "expected_move_strength": exp_strength,
        "expected_move_horizon": horizon,
        "crowd_position": crowd_position,
        "volatility_state": volatility_state,
        "continuation_bias": continuation_bias,
        "reversal_risk": reversal_risk,
        "squeeze_risk": squeeze_risk,
        "dominant_signal": dominant_signal,
        "basis_state": basis_state,
        "oi_crowded": oi_crowded,
        "raw_features": raw_features
    }

# ----------------------------- Wrapper for X07 (convenience) -----------------------------
def from_x07_data_dict(data_dict, price_change_pct=0, atr_pct=None, timeframe='1h', current_timestamp_ms=None):
    """Convenience function for X07 to call with its data dictionary."""
    ts = current_timestamp_ms if current_timestamp_ms else int(time.time() * 1000)
    return analyze_derivative(
        spot_price=data_dict.get('spot_price', 0),
        mark_price=data_dict.get('mark_price', 0),
        funding_rate=data_dict.get('funding_rate', 0),
        oi_current=data_dict.get('oi_current', 0),
        oi_change_pct=data_dict.get('oi_change_pct', 0),
        price_change_pct=price_change_pct,
        ls_ratio=data_dict.get('ls_ratio', 0.5),
        ls_history=data_dict.get('ls_history', []),
        oi_history=data_dict.get('oi_history', []),
        funding_history=data_dict.get('funding_history', []),
        liquidation_levels=data_dict.get('liquidation_levels', []),
        current_timestamp_ms=ts,
        atr_pct=atr_pct,
        timeframe=timeframe
    )

# ----------------------------- Self‑test -----------------------------
if __name__ == "__main__":
    test_data = {
        "spot_price": 64000,
        "mark_price": 64150,
        "funding_rate": 0.00012,
        "oi_current": 1500000000,
        "oi_change_pct": 2.5,
        "ls_ratio": 1.2,
        "ls_history": [{"timestamp": i*60000, "long_short_ratio": 1.1} for i in range(1, 26)],
        "oi_history": [{"timestamp": i*60000, "oi_value": 1.4e9 + i*1e7} for i in range(1, 31)],
        "funding_history": [{"timestamp": i*3600000, "funding_rate": 0.0001} for i in range(1, 40)],
        "liquidation_levels": [(64200, 100), (63800, 150), (63900, 200)]
    }
    res = from_x07_data_dict(test_data, price_change_pct=1.2, atr_pct=1.2, timeframe='1h', current_timestamp_ms=int(time.time()*1000))
    print(res)