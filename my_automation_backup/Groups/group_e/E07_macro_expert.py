#!/usr/bin/env python3
# E07_macro_expert.py – Macroeconomic High‑Probability Scenario Detector (≥90% setups)
# Reads macro data from X11 .tmp_x (or P06 .tmp_p) and outputs TSV summary.

import os
import sys
import time
import json
from collections import defaultdict

# ========================== CONFIGURATION ==========================
FEATURES_BASE_DIR = os.path.join("market_data", "binance", "symbols")
LOG_FILE = os.path.join(FEATURES_BASE_DIR, "E07_macro_expert.log")
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

# ========================== ORIGINAL analyze_macro (UNCHANGED) ==========================
def analyze_macro(current_row, prev_row=None):
    """
    Args:
        current_row: dict with keys:
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

    def get_val(row, key, default=0):
        return row.get(key, default) if row else default

    def get_change(current, previous, is_percent=True):
        if previous is None or previous == 0:
            return 0
        diff = current - previous
        if is_percent:
            return (diff / abs(previous)) * 100
        return diff

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

    # 1. Yield spread
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

    # 2. VIX
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

    # 4. Stock indices
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

    # 5. Gold
    if gold_change > 1:
        bearish_score += 10
        signals.append(f"Gold up {gold_change:.1f}% → risk-off, bearish")
    elif gold_change < -1:
        bullish_score += 10
        signals.append(f"Gold down {gold_change:.1f}% → risk-on, bullish")
    else:
        signals.append(f"Gold flat ({gold_change:+.1f}%)")

    # 6. Oil
    if oil_change > 2:
        bearish_score += 10
        signals.append(f"Oil up {oil_change:.1f}% → inflation fear, bearish")
    elif oil_change < -2:
        bullish_score += 10
        signals.append(f"Oil down {oil_change:.1f}% → inflation easing, bullish")

    # 7. Dollar index
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
        signals.append(f"{high_impact_count} high‑impact events ahead → high uncertainty")
    if is_volatile:
        signals.append("Next high‑impact event within 30min → elevated volatility")
    elif seconds_to_event < 3600 and seconds_to_event > 0:
        signals.append(f"High‑impact event in {seconds_to_event//60} minutes → potential volatility")
    else:
        signals.append("No immediate high‑impact events")

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

# ========================== LOAD MACRO DATA FROM X11 .tmp_x ==========================
def load_macro_rows(symbol):
    """Read macro data from X11's raw file (macro.tmp_x) and return last two rows as dicts."""
    path = os.path.join(FEATURES_BASE_DIR, f"{symbol.lower()}_macro.tmp_x")
    if not os.path.exists(path):
        # fallback to P06 processed file if exists
        path = os.path.join(FEATURES_BASE_DIR, f"{symbol.lower()}_macro.tmp_p")
    if not os.path.exists(path):
        log_issue("ERROR", f"Macro file not found: {path}")
        return None, None
    rows = []
    try:
        with open(path, "r") as f:
            lines = f.readlines()
        if len(lines) < 2:
            log_issue("ERROR", "Macro file has no data rows")
            return None, None
        # Determine header and column mapping
        header = lines[0].strip().split('\t')
        # Expected column names (lowercase)
        # We'll try to map intelligently
        # Typical columns from X11: timestamp, treasury_10y, treasury_2y, yield_spread,
        # high_impact_count, vix, risk_premium, spy, qqq, dia, xauusd, usoil, dxy,
        # seconds_to_next_event, next_event_title, is_volatile_zone
        # We'll hardcode the expected order as a fallback
        expected_cols = [
            'timestamp', 'treasury_10y', 'treasury_2y', 'yield_spread',
            'high_impact_count', 'vix', 'risk_premium', 'spy', 'qqq', 'dia',
            'xauusd', 'usoil', 'dxy', 'seconds_to_next_event', 'next_event_title', 'is_volatile_zone'
        ]
        # Build mapping: index -> key
        idx_map = {}
        for i, h in enumerate(header):
            h_lower = h.strip().lower()
            for ec in expected_cols:
                if ec in h_lower or (ec == 'vix' and 'vix' == h_lower):
                    idx_map[i] = ec
                    break
        # If mapping failed, use fallback order
        if not idx_map:
            # assume columns in expected order
            for i, ec in enumerate(expected_cols):
                if i < len(header):
                    idx_map[i] = ec
                else:
                    break
        # Parse each data line
        for line in lines[1:]:
            parts = line.strip().split('\t')
            if len(parts) < len(idx_map):
                continue
            row = {}
            for i, val in enumerate(parts):
                if i in idx_map:
                    key = idx_map[i]
                    try:
                        if key in ('timestamp', 'seconds_to_next_event', 'high_impact_count'):
                            row[key] = int(val)
                        elif key == 'is_volatile_zone':
                            row[key] = val.lower() in ('true', '1', 'yes')
                        else:
                            row[key] = float(val)
                    except:
                        continue
            if row:
                rows.append(row)
    except Exception as e:
        log_issue("ERROR", f"Failed to load macro data: {e}")
        return None, None
    if not rows:
        return None, None
    # Return last two rows (most recent and previous)
    if len(rows) >= 2:
        return rows[-1], rows[-2]
    else:
        return rows[-1], None

# ========================== MAIN EXPERT FUNCTION ==========================
def run_expert(symbol):
    log_issue("INFO", f"Starting E07 macro expert for {symbol}")
    current, prev = load_macro_rows(symbol)
    if current is None:
        log_issue("ERROR", "No macro data loaded")
        return None
    result = analyze_macro(current, prev)
    # Write output TSV
    out_path = os.path.join(FEATURES_BASE_DIR, f"{symbol.lower()}_E07_macro.tsv")
    with open(out_path, "w") as f:
        header = ["timestamp", "bias", "confidence", "high_prob_scenario", "probability_estimate",
                  "reason", "signals_json", "net_score"]
        f.write("\t".join(header) + "\n")
        ts_now = int(time.time() * 1000)
        row = [
            str(ts_now),
            result['bias'],
            str(result['confidence']),
            str(result['high_prob_scenario']) if result['high_prob_scenario'] else "",
            str(result['probability_estimate']),
            result['reason'],
            json.dumps(result['signals']),
            str(result['net_score'])
        ]
        f.write("\t".join(row) + "\n")
    log_issue("INFO", f"Saved macro expert summary to {out_path}")
    return out_path

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python E07_macro_expert.py SYMBOL")
        sys.exit(1)
    success = run_expert(sys.argv[1].upper())
    sys.exit(0 if success else 1)