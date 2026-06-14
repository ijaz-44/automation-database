#!/usr/bin/env python3
"""
P06_macro_impact.py – Process Macroeconomic Data
- Reads {symbol}_macro.tmp_x (TSV with header, single row)
- Computes directional impact on crypto (bullish/bearish/neutral)
- Outputs {symbol}_macro.tmp_p with raw data + derived features
- Logs to p06_macro_impact_issues.log (file only, minimal console)
- Input file is NOT deleted.
"""

import os
import sys
import time
import math

# ========== CONFIGURATION ==========
FEATURES_BASE_DIR = os.path.join("market_data", "binance", "symbols")
LOG_FILE = os.path.join(FEATURES_BASE_DIR, "p06_macro_impact_issues.log")
LOG_MAX_SIZE = 5_000_000

os.makedirs(FEATURES_BASE_DIR, exist_ok=True)

# ========== LOGGING (minimal console) ==========
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
    # Print only errors on console (INFO and others go only to file)
    if level == "ERROR":
        print(line)
    rotate_log_if_needed()
    with open(LOG_FILE, "a") as f:
        f.write(line + "\n")

# ========== MAIN PROCESSING ==========
def process_macro(symbol):
    print(f"[P06] Starting macro processing for {symbol}")
    start_time = time.time()
    log_issue("INFO", f"Starting macro processing for {symbol}")

    tmp_x_path = os.path.join(FEATURES_BASE_DIR, f"{symbol.lower()}_macro.tmp_x")
    if not os.path.exists(tmp_x_path):
        log_issue("ERROR", f"Input file not found: {tmp_x_path}")
        return False

    # ---------- Read the single row ----------
    with open(tmp_x_path, "r") as f:
        header_line = f.readline().strip()
        if not header_line:
            log_issue("ERROR", "Empty header in tmp_x")
            return False
        headers = header_line.split('\t')
        data_line = f.readline().strip()
        if not data_line:
            log_issue("ERROR", "No data row in tmp_x")
            return False
        values = data_line.split('\t')
        if len(headers) != len(values):
            log_issue("ERROR", f"Column mismatch: {len(headers)} headers, {len(values)} values")
            return False
        row = dict(zip(headers, values))

    # Convert to proper types
    try:
        ts = int(row.get('timestamp', 0))
        t10 = float(row.get('treasury_10y', 4.5))
        t2 = float(row.get('treasury_2y', 4.2))
        spread = float(row.get('yield_spread', t10 - t2))
        high_impact_count = int(float(row.get('high_impact_count', 0)))
        vix = float(row.get('vix', 15.0))
        risk_premium = float(row.get('risk_premium', 0.0))
        spy = float(row.get('spy', 500.0))
        qqq = float(row.get('qqq', 450.0))
        dia = float(row.get('dia', 400.0))
        gold = float(row.get('xauusd', 2300.0))
        oil = float(row.get('usoil', 70.0))
        dxy = float(row.get('dxy', 105.0))
        seconds_to_event = int(float(row.get('seconds_to_next_event', 0)))
        next_event_title = row.get('next_event_title', '')
        is_volatile = int(float(row.get('is_volatile_zone', 0))) == 1
    except Exception as e:
        log_issue("ERROR", f"Data conversion error: {e}", row=data_line[:100])
        return False

    # ---------- Compute macro impact score (positive = bullish for crypto) ----------
    impact_score = 0
    signals = []

    # 1. Yield spread
    if spread < -0.3:
        impact_score -= 25
        signals.append(f"Deep inversion ({spread:.2f}%) → bearish")
    elif spread < -0.1:
        impact_score -= 15
        signals.append(f"Inversion ({spread:.2f}%) → slightly bearish")
    elif spread > 0.3:
        impact_score += 15
        signals.append(f"Steep spread ({spread:.2f}%) → bullish")
    elif spread > 0.1:
        impact_score += 5
        signals.append(f"Positive spread ({spread:.2f}%) → neutral-bullish")
    else:
        signals.append(f"Neutral spread ({spread:.2f}%)")

    # 2. VIX (fear)
    if vix > 30:
        impact_score -= 20
        signals.append(f"Very high VIX ({vix:.1f}) → bearish")
    elif vix > 25:
        impact_score -= 10
        signals.append(f"High VIX ({vix:.1f}) → bearish")
    elif vix < 15:
        impact_score += 10
        signals.append(f"Low VIX ({vix:.1f}) → bullish")
    else:
        signals.append(f"Normal VIX ({vix:.1f})")

    # 3. DXY (dollar)
    if dxy > 108:
        impact_score -= 15
        signals.append(f"Very strong DXY ({dxy:.1f}) → bearish")
    elif dxy > 106:
        impact_score -= 8
        signals.append(f"Strong DXY ({dxy:.1f}) → slightly bearish")
    elif dxy < 102:
        impact_score += 12
        signals.append(f"Weak DXY ({dxy:.1f}) → bullish")
    else:
        signals.append(f"Neutral DXY ({dxy:.1f})")

    # 4. Gold (safe haven)
    if gold > 2500:
        impact_score -= 10
        signals.append(f"Gold very high ({gold:.0f}) → risk‑off, bearish")
    elif gold < 2100:
        impact_score += 10
        signals.append(f"Gold low ({gold:.0f}) → risk‑on, bullish")

    # 5. SPY (risk appetite)
    if spy > 550:
        impact_score += 15
        signals.append(f"SPY very high ({spy:.0f}) → strong risk‑on, bullish")
    elif spy > 520:
        impact_score += 8
        signals.append(f"SPY high ({spy:.0f}) → bullish")
    elif spy < 450:
        impact_score -= 15
        signals.append(f"SPY low ({spy:.0f}) → risk‑off, bearish")
    elif spy < 480:
        impact_score -= 8
        signals.append(f"SPY moderately low ({spy:.0f}) → slightly bearish")
    else:
        signals.append(f"SPY neutral ({spy:.0f})")

    # 6. Oil (inflation proxy)
    if oil > 85:
        impact_score -= 10
        signals.append(f"Oil high ({oil:.1f}) → inflation fear, bearish")
    elif oil > 80:
        impact_score -= 5
        signals.append(f"Oil elevated ({oil:.1f}) → slightly bearish")
    elif oil < 60:
        impact_score += 8
        signals.append(f"Oil low ({oil:.1f}) → inflation easing, bullish")

    # 7. High‑impact events count & proximity
    if high_impact_count > 5:
        impact_score -= 10
        signals.append(f"{high_impact_count} high‑impact events → high uncertainty, bearish")
    elif high_impact_count > 2:
        impact_score -= 5
        signals.append(f"{high_impact_count} high‑impact events → cautious")

    if is_volatile:
        impact_score -= 10
        signals.append("Event within 30min → volatile zone, reduce confidence")
    elif seconds_to_event > 0 and seconds_to_event < 3600:
        impact_score -= 5
        signals.append(f"Event in {seconds_to_event//60} min → cautious")

    # 8. Risk premium
    if risk_premium > 0.5:
        impact_score -= 5
        signals.append(f"High risk premium ({risk_premium:.2f}) → stocks attractive, crypto may suffer")
    elif risk_premium < -0.5:
        impact_score += 5
        signals.append(f"Negative risk premium ({risk_premium:.2f}) → bonds attractive, crypto neutral")

    # Clamp
    impact_score = max(-100, min(100, impact_score))

    # Bias and confidence
    if impact_score >= 25:
        bias = "bullish"
        confidence = min(95, 60 + impact_score // 2)
    elif impact_score <= -25:
        bias = "bearish"
        confidence = min(95, 60 + abs(impact_score) // 2)
    else:
        bias = "neutral"
        confidence = 50 + abs(impact_score) // 2 if impact_score != 0 else 50

    # Derived regimes
    if spread < -0.1:
        spread_cat = "inverted"
    elif spread > 0.1:
        spread_cat = "positive"
    else:
        spread_cat = "flat"

    if vix > 25:
        vix_regime = "high_fear"
    elif vix < 15:
        vix_regime = "low_fear"
    else:
        vix_regime = "normal"

    if dxy > 106:
        dxy_regime = "strong_dollar"
    elif dxy < 103:
        dxy_regime = "weak_dollar"
    else:
        dxy_regime = "neutral_dollar"

    if spy > 520:
        risk_regime = "risk_on"
    elif spy < 480:
        risk_regime = "risk_off"
    else:
        risk_regime = "neutral"

    # ---------- Write output .tmp_p TSV (single row with raw data + derived) ----------
    tmp_p_path = os.path.join(FEATURES_BASE_DIR, f"{symbol.lower()}_macro.tmp_p")
    with open(tmp_p_path, "w") as out:
        # Header includes all raw fields plus derived ones
        header = [
            "timestamp", "treasury_10y", "treasury_2y", "yield_spread", "yield_spread_category",
            "vix", "vix_regime", "risk_premium", "spy", "qqq", "dia", "risk_regime",
            "xauusd", "usoil", "dxy", "dxy_regime", "high_impact_count",
            "seconds_to_next_event", "next_event_title", "is_volatile_zone",
            "impact_score", "bias", "confidence", "macro_signals"
        ]
        out.write("\t".join(header) + "\n")
        signals_str = " | ".join(signals[:5])
        row = [
            str(ts),
            f"{t10:.4f}", f"{t2:.4f}", f"{spread:.4f}", spread_cat,
            f"{vix:.2f}", vix_regime,
            f"{risk_premium:.4f}",
            f"{spy:.2f}", f"{qqq:.2f}", f"{dia:.2f}", risk_regime,
            f"{gold:.2f}", f"{oil:.2f}", f"{dxy:.2f}", dxy_regime,
            str(high_impact_count),
            str(seconds_to_event),
            next_event_title.replace('\t', ' '),
            "1" if is_volatile else "0",
            str(impact_score), bias, str(confidence),
            signals_str
        ]
        out.write("\t".join(row) + "\n")

    # Input file is NOT deleted
    # if os.path.exists(tmp_x_path):
    #     os.remove(tmp_x_path)

    elapsed = time.time() - start_time
    print(f"[P06] Success ({elapsed:.1f}s) -> {os.path.basename(tmp_p_path)}")
    log_issue("INFO", f"Processing complete for {symbol} in {elapsed:.2f}s -> {tmp_p_path}")
    return True

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python P06_macro_impact.py SYMBOL")
        sys.exit(1)
    success = process_macro(sys.argv[1].upper())
    sys.exit(0 if success else 1)