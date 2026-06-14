#!/usr/bin/env python3
# E02_derivative_expert.py – Derivative State Engine (Production‑Ready, All Issues Fixed)
# Reads processed features from P04, P01, P02, P07 and raw derivative file.
# Outputs pure TSV (no JSON) with all raw features as separate columns.

import os
import sys
import time
import math
from collections import deque

# ========================== PATHS & LOGGING ==========================
FEATURES_BASE_DIR = os.path.join("market_data", "binance", "symbols")
LOG_FILE = os.path.join(FEATURES_BASE_DIR, "E02_derivative_expert.log")
LOG_MAX_SIZE = 5_000_000

def rotate_log_if_needed():
    if os.path.exists(LOG_FILE) and os.path.getsize(LOG_FILE) > LOG_MAX_SIZE:
        backup = LOG_FILE + ".old"
        try:
            os.replace(LOG_FILE, backup)
        except:
            pass

def log_issue(level, msg, **kwargs):
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    line = f"{ts} [{level}] {msg}"
    if kwargs:
        line += " " + str(kwargs)
    print(line)
    rotate_log_if_needed()
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(line + "\n")

def safe_float(val, default=0.0):
    try:
        if val is None or str(val).strip() in ('', 'N/A', '--'):
            return default
        return float(val)
    except:
        return default

# ========================== HELPER FUNCTIONS ==========================
PRICE_THRESHOLDS = {
    '1m': 0.15, '5m': 0.3, '15m': 0.5, '1h': 1.0, '4h': 2.0
}
OI_THRESHOLDS = {
    '1m': 0.5, '5m': 1.0, '15m': 1.5, '1h': 2.0, '4h': 3.0
}

def funding_zscore(current_rate, history, window=30, decay_alpha=0.1):
    # Increase minimum history requirement to 10 for stability
    if not history or len(history) < 10:
        return 0.0, 0.0, 1.0
    recent = list(history[-window:]) if len(history) >= window else history[:]
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
    std = max(std, 1e-5)
    z = (current_rate - mean) / std
    return z, mean, std

def time_slope(pairs):
    if len(pairs) < 2:
        return 0.0
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

def oi_percentile(current_oi, oi_history, days=30):
    if not oi_history:
        return 50.0
    age_limit = days * 24 * 3600 * 1000
    now = time.time() * 1000
    # Use hourly data if available, otherwise all points
    recent = [h['oi_value'] for h in oi_history if now - h['timestamp'] <= age_limit]
    if not recent:
        return 50.0
    below = sum(1 for v in recent if v < current_oi)
    return (below / len(recent)) * 100

def cascade_risk(liquidation_levels, spot_price, atr_pct):
    if not liquidation_levels:
        return "NONE"
    atr_dist = atr_pct * 0.01 * spot_price if atr_pct else spot_price * 0.015
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

def analyze_derivative(spot_price, mark_price, funding_rate,
                       oi_current, oi_change_pct, price_change_pct,
                       ls_ratio, ls_history,
                       oi_history, funding_history, liquidation_levels,
                       current_timestamp_ms, atr_pct=None, timeframe='1h'):
    if spot_price <= 0 or oi_current <= 0:
        return {"error": "INVALID_DATA", "conviction": 0, "uncertainty": "INVALID_DATA", "base_state": "UNKNOWN"}
    if oi_history:
        latest_oi_ts = max(h['timestamp'] for h in oi_history)
        age_sec = (current_timestamp_ms - latest_oi_ts) / 1000
        # Increased staleness cutoff to 10 minutes (600 sec) for 1h, adjust by timeframe
        staleness_limit = 600 if timeframe in ('1h','4h') else 300
        if age_sec > staleness_limit:
            return {"error": "STALE_DATA", "conviction": 0, "uncertainty": "STALE_DATA", "base_state": "UNKNOWN"}
    price_th = PRICE_THRESHOLDS.get(timeframe, 0.5)
    oi_th = OI_THRESHOLDS.get(timeframe, 1.0)
    price_up = price_change_pct > price_th
    price_down = price_change_pct < -price_th
    price_flat = not price_up and not price_down
    oi_up = oi_change_pct > oi_th
    oi_down = oi_change_pct < -oi_th
    # LS ratio normalization with floor to avoid explosion
    if ls_history and len(ls_history) >= 5:
        recent_ratios = [h['long_short_ratio'] for h in ls_history[-24:]]
        avg_ls = sum(recent_ratios) / len(recent_ratios) if recent_ratios else 0.5
        # Avoid division by zero or near-zero
        avg_ls = max(avg_ls, 0.1)
        ls_ratio_norm = ls_ratio / avg_ls
        # Clip to reasonable range
        ls_ratio_norm = max(0.2, min(5.0, ls_ratio_norm))
    else:
        avg_ls = 0.5
        ls_ratio_norm = 1.0
    if ls_ratio_norm > 1.35:
        crowd_position = "OVERLONG"
    elif ls_ratio_norm < 0.7:
        crowd_position = "OVERSHORT"
    else:
        crowd_position = "NEUTRAL"
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
    oi_velocity_pct = 0.0
    oi_acceleration = 0.0
    trend_strength = 40
    if oi_history and len(oi_history) >= 5:
        pairs = [(h['timestamp'], h['oi_value']) for h in oi_history[-12:]]
        if len(pairs) >= 3:
            slope = time_slope(pairs)
            avg_oi = sum(p[1] for p in pairs) / len(pairs)
            if avg_oi != 0:
                oi_velocity_pct = (slope / avg_oi) * 100
            else:
                oi_velocity_pct = 0.0
            # Clamp velocity to [-100,100]
            oi_velocity_pct = max(-100.0, min(100.0, oi_velocity_pct))
            # Use log scaling for trend_strength to avoid explosion
            if abs(oi_velocity_pct) > 0:
                log_velocity = math.log1p(abs(oi_velocity_pct) / 10) * 10
                trend_strength = min(85, 40 + log_velocity)
            else:
                trend_strength = 40
            if len(pairs) >= 6:
                half = len(pairs) // 2
                slope1 = time_slope(pairs[:half])
                slope2 = time_slope(pairs[half:])
                oi_acceleration = slope2 - slope1
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
    basis_pct = (mark_price - spot_price) / spot_price * 100 if spot_price != 0 else 0
    basis_state = "NEUTRAL"
    # Increase threshold to 0.5% to reduce noise
    if basis_pct > 0.5:
        basis_state = "PREMIUM"
    elif basis_pct < -0.5:
        basis_state = "DISCOUNT"
    oi_percentile_val = oi_percentile(oi_current, oi_history, days=30)
    oi_crowded = "NORMAL"
    if oi_percentile_val > 90:
        oi_crowded = "EXTREME_HIGH"
    elif oi_percentile_val < 10:
        oi_crowded = "EXTREME_LOW"
    liq_magnet_bias = 0.0
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
            liq_magnet_bias = (below_vol - above_vol) / total_liq
        atr = atr_pct if atr_pct is not None else 1.5
        for price, vol in liquidation_levels:
            dist_pct = abs((price - spot_price) / spot_price) * 100
            if dist_pct < atr * 0.8:
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
    if abs(oi_change_pct) > 3 or abs(funding_z) > 1.5:
        volatility_state = "EXPANDING"
    elif abs(oi_change_pct) < 0.5 and abs(funding_z) < 0.5:
        volatility_state = "CONTRACTING"
    else:
        volatility_state = "NORMAL"
    bias_score = 0
    if base_state in ("LONG_BUILDUP", "SHORT_COVERING_RALLY"):
        bias_score += 30
    elif base_state in ("SHORT_BUILDUP", "LONG_LIQUIDATION"):
        bias_score -= 30
    # Reduce liquidation weight from 20 to 10 to avoid dominance
    bias_score += liq_magnet_bias * 10
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
    max_agreement = 3+2+2+1
    agree_score = 0
    if (price_up and oi_up) or (price_down and oi_down):
        agree_score += 3
    if (bias == "bullish" and funding_z <= 0.5) or (bias == "bearish" and funding_z >= -0.5):
        agree_score += 2
    if (bias == "bullish" and liq_magnet_bias > 0) or (bias == "bearish" and liq_magnet_bias < 0):
        agree_score += 2
    # Improved conflict detection (symmetric)
    if ( (bias == "bearish" and base_state in ("LONG_BUILDUP", "SHORT_COVERING_RALLY")) or (bias == "bullish" and base_state in ("SHORT_BUILDUP", "LONG_LIQUIDATION")) ):
        agree_score += 1
    weighted_agreement = (agree_score / max_agreement) * 100
    conviction = 50 + (weighted_agreement / 2) - (10 if reversal_risk == "HIGH" else 0)
    conviction = max(30, min(85, conviction))
    data_uncertainty = "LOW"
    if oi_history:
        latest_oi_ts = max(h['timestamp'] for h in oi_history)
        age_sec = (current_timestamp_ms - latest_oi_ts) / 1000
        if age_sec > 90:
            data_uncertainty = "HIGH"
        elif age_sec > 30:
            data_uncertainty = "MEDIUM"
    signal_conflict = "LOW"
    if (base_state in ("LONG_BUILDUP", "SHORT_COVERING_RALLY") and bias == "bearish") or (base_state in ("SHORT_BUILDUP", "LONG_LIQUIDATION") and bias == "bullish"):
        signal_conflict = "HIGH"
    elif weighted_agreement < 50:
        signal_conflict = "MEDIUM"
    regime_uncertainty = "LOW"
    if volatility_state == "EXPANDING" and squeeze_risk == "HIGH":
        regime_uncertainty = "MEDIUM"
    dominant_signal = "NEUTRAL"
    if base_state in ("LONG_BUILDUP", "SHORT_BUILDUP"):
        dominant_signal = "OI_PRICE_TREND"
    elif abs(funding_z) > 1.5:
        dominant_signal = "FUNDING_EXTREME"
    elif liq_barrier != "NONE":
        dominant_signal = "LIQUIDATION_BARRIER"
    elif crowd_position != "NEUTRAL":
        dominant_signal = "CROWD_EXTREME"
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

# ========================== LOAD DATA FROM P MODULES AND RAW FILES ==========================
def load_p04_features(symbol):
    path = os.path.join(FEATURES_BASE_DIR, f"{symbol.lower()}_derivative.tmp_p")
    if not os.path.exists(path):
        log_issue("WARNING", f"P04 file not found: {path}")
        return {}
    try:
        with open(path, "r") as f:
            lines = f.readlines()
        if len(lines) < 2:
            return {}
        header = lines[0].strip().split('\t')
        values = lines[1].strip().split('\t')
        data = {}
        for i, col in enumerate(header):
            if i < len(values):
                val = values[i].strip()
                if col in ['oi_change_pct', 'funding_zscore', 'ls_ratio_velocity', 'basis_pct', 'net_score',
                           'oi_velocity_pct', 'oi_percentile', 'liquidation_magnet_bias', 'cascade_risk_value',
                           'long_cascade_risk', 'short_cascade_risk', 'stop_hunt_probability', 'trend_strength',
                           'net_delta_1m', 'net_delta_15m', 'delta_ratio', 'spike_ratio']:
                    data[col] = safe_float(val)
                else:
                    data[col] = val
        return data
    except Exception as e:
        log_issue("WARNING", f"Error reading P04 file: {e}")
        return {}

def load_p01_features(symbol):
    path = os.path.join(FEATURES_BASE_DIR, f"{symbol.lower()}.tmp_p")
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r") as f:
            lines = f.readlines()
        if len(lines) < 2:
            return {}
        header = lines[0].strip().split('\t')
        values = lines[1].strip().split('\t')
        data = {}
        for i, col in enumerate(header):
            if i < len(values):
                val = values[i].strip()
                if col in ['atr_pct', 'volatility_24h', 'volatility_ratio', 'trend_strength', 'price_change_pct']:
                    data[col] = safe_float(val)
                else:
                    data[col] = val
        return data
    except:
        return {}

def load_p02_features(symbol):
    path = os.path.join(FEATURES_BASE_DIR, f"{symbol.lower()}_cvd2.tmp_p")
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r") as f:
            lines = f.readlines()
        if len(lines) < 2:
            return {}
        last = lines[-1].strip().split('\t')
        if len(last) >= 2:
            return {"cvd_net": safe_float(last[0]), "cvd_trend": last[1]}
    except:
        pass
    return {}

def load_p07_features(symbol):
    path = os.path.join(FEATURES_BASE_DIR, f"{symbol.lower()}_liquidations.tmp_p")
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r") as f:
            lines = f.readlines()
        if len(lines) < 2:
            return {}
        header = lines[0].strip().split('\t')
        values = lines[1].strip().split('\t')
        data = {}
        for i, col in enumerate(header):
            if i < len(values):
                val = values[i].strip()
                if col in ['net_delta_1m', 'stop_hunt_probability', 'liquidation_magnet_bias', 'cascade_risk_value',
                           'long_cascade_risk', 'short_cascade_risk', 'liq_volume_above', 'liq_volume_below',
                           'long_dominance', 'short_dominance']:
                    data[col] = safe_float(val)
                else:
                    data[col] = val
        return data
    except:
        return {}

def load_liquidation_levels(symbol):
    path = os.path.join(FEATURES_BASE_DIR, f"{symbol.lower()}_liquidations.tmp_x")
    if not os.path.exists(path):
        return []
    levels = []
    try:
        with open(path, "r") as f:
            for line in f:
                parts = line.strip().split('\t')
                if len(parts) >= 6:
                    raw = parts[5]
                    if raw:
                        for item in raw.split(';'):
                            pv = item.split(':')
                            if len(pv) == 3:
                                levels.append((safe_float(pv[0]), safe_float(pv[1])))
    except:
        pass
    return levels

# ========================== MAIN EXPERT FUNCTION ==========================
def run_expert(symbol, timeframe='1h'):
    log_issue("INFO", f"Starting E02 derivative expert for {symbol} (timeframe={timeframe})")
    # Load data from P modules
    p04 = load_p04_features(symbol)
    p01 = load_p01_features(symbol)
    p02 = load_p02_features(symbol)
    p07 = load_p07_features(symbol)
    liq_levels = load_liquidation_levels(symbol)

    # Read raw derivative file for snapshot and histories
    deriv_raw = os.path.join(FEATURES_BASE_DIR, f"{symbol.lower()}_derivative.tmp_x")
    spot_price = mark_price = funding_rate = oi_current = 0.0
    ls_ratio = 0.5
    ls_history = []
    oi_history = []
    funding_history = []

    if os.path.exists(deriv_raw):
        with open(deriv_raw, "r") as f:
            for line in f:
                parts = line.strip().split('\t')
                if len(parts) < 2:
                    continue
                typ = parts[0]
                if typ == "snapshot" and len(parts) >= 6:
                    spot_price = safe_float(parts[2])
                    mark_price = safe_float(parts[3])
                    funding_rate = safe_float(parts[4])
                    oi_current = safe_float(parts[5])
                elif typ == "oi_history" and len(parts) >= 3:
                    oi_history.append({'timestamp': int(parts[1]), 'oi_value': safe_float(parts[2])})
                elif typ == "funding_history" and len(parts) >= 3:
                    funding_history.append({'timestamp': int(parts[1]), 'funding_rate': safe_float(parts[2])})
                elif typ == "ls_history" and len(parts) >= 3:
                    ls_history.append({'timestamp': int(parts[1]), 'long_short_ratio': safe_float(parts[2])})
        # Sort histories by timestamp
        oi_history.sort(key=lambda x: x['timestamp'])
        funding_history.sort(key=lambda x: x['timestamp'])
        ls_history.sort(key=lambda x: x['timestamp'])
        if ls_history:
            ls_ratio = ls_history[-1]['long_short_ratio']
    else:
        log_issue("WARNING", f"Raw derivative file not found: {deriv_raw}")

    # Use P04 values for derived metrics with safe_float
    oi_change_pct = p04.get('oi_change_pct', 0.0)
    # price_change_pct: prefer P01, else P04, else 0 (with safe conversion)
    price_change_pct = 0.0
    if 'price_change_pct' in p01:
        price_change_pct = p01['price_change_pct']
    elif 'price_change_pct' in p04:
        price_change_pct = p04['price_change_pct']
    # Ensure numeric
    price_change_pct = safe_float(price_change_pct)
    atr_pct = p01.get('atr_pct', 1.5)
    atr_pct = safe_float(atr_pct)
    current_ts = int(time.time() * 1000)

    # Call the analysis function
    result = analyze_derivative(
        spot_price=spot_price,
        mark_price=mark_price,
        funding_rate=funding_rate,
        oi_current=oi_current,
        oi_change_pct=oi_change_pct,
        price_change_pct=price_change_pct,
        ls_ratio=ls_ratio,
        ls_history=ls_history,
        oi_history=oi_history,
        funding_history=funding_history,
        liquidation_levels=liq_levels,
        current_timestamp_ms=current_ts,
        atr_pct=atr_pct,
        timeframe=timeframe
    )
    if "error" in result:
        log_issue("ERROR", f"Derivative analysis failed: {result['error']}")
        return None

    # Flatten raw_features into separate columns (no JSON)
    raw = result['raw_features']
    out_path = os.path.join(FEATURES_BASE_DIR, f"{symbol.lower()}_E02_derivative.tsv")
    with open(out_path, "w") as f:
        header = [
            "timestamp", "timeframe", "base_state", "modifier", "bias", "conviction",
            "uncertainty", "data_uncertainty", "signal_conflict", "regime_uncertainty",
            "expected_move_direction", "expected_move_strength", "expected_move_horizon",
            "crowd_position", "volatility_state", "continuation_bias", "reversal_risk",
            "squeeze_risk", "dominant_signal", "basis_state", "oi_crowded",
            "funding_zscore", "ls_ratio_norm", "oi_velocity_pct", "oi_acceleration",
            "liquidation_magnet_bias", "liquidation_barrier", "weighted_agreement",
            "oi_percentile", "basis_pct", "cascade_risk"
        ]
        f.write("\t".join(header) + "\n")
        row = [
            str(current_ts),
            timeframe,
            result['base_state'],
            result['modifier'],
            result['bias'],
            str(result['conviction']),
            result['uncertainty'],
            result['data_uncertainty'],
            result['signal_conflict'],
            result['regime_uncertainty'],
            result['expected_move_direction'],
            str(result['expected_move_strength']),
            result['expected_move_horizon'],
            result['crowd_position'],
            result['volatility_state'],
            result['continuation_bias'],
            result['reversal_risk'],
            result['squeeze_risk'],
            result['dominant_signal'],
            result['basis_state'],
            result['oi_crowded'],
            str(raw.get('funding_zscore', 0)),
            str(raw.get('ls_ratio_norm', 0)),
            str(raw.get('oi_velocity_pct', 0)),
            str(raw.get('oi_acceleration', 0)),
            str(raw.get('liquidation_magnet_bias', 0)),
            raw.get('liquidation_barrier', 'NONE'),
            str(raw.get('weighted_agreement', 0)),
            str(raw.get('oi_percentile', 0)),
            str(raw.get('basis_pct', 0)),
            raw.get('cascade_risk', 'NONE')
        ]
        f.write("\t".join(row) + "\n")
    log_issue("INFO", f"Saved derivative expert summary to {out_path}")
    return out_path

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python E02_derivative_expert.py SYMBOL [timeframe]")
        sys.exit(1)
    symbol = sys.argv[1].upper()
    timeframe = sys.argv[2] if len(sys.argv) > 2 else "1h"
    success = run_expert(symbol, timeframe)
    sys.exit(0 if success else 1)