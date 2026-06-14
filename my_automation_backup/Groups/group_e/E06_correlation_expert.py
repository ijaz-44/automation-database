#!/usr/bin/env python3
# E06_correlation_expert.py – Correlation High‑Probability Scenario Detector (No JSON)
# Reads processed correlation data from P05 .tmp_p file and outputs TSV summary.

import os
import sys
import time
import math
from collections import defaultdict

# ========================== CONFIGURATION ==========================
FEATURES_BASE_DIR = os.path.join("market_data", "binance", "symbols")
LOG_FILE = os.path.join(FEATURES_BASE_DIR, "E06_correlation_expert.log")
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

# ========================== ORIGINAL analyze_correlation (UNCHANGED) ==========================
def analyze_correlation(corr_data):
    """
    Args:
        corr_data: dict with keys:
            - 'current_correlations': dict mapping index name to correlation coefficient (e.g., 0.65)
            - 'momentums': dict mapping index name to momentum (percentage change of correlation)
            - 'z_scores': dict mapping index name to z‑score of correlation
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

    current = corr_data.get('current_correlations', {})
    z_scores = corr_data.get('z_scores', {})
    momentums = corr_data.get('momentums', {})

    # 1. SPY, QQQ, DIA (risk appetite)
    risky_assets = ['SPY', 'QQQ', 'DIA']
    for idx in risky_assets:
        corr = current.get(idx, 0)
        z = z_scores.get(idx, 0)
        mom = momentums.get(idx, 0)
        if corr > 0.5:
            bull_bonus = 15
            if z > 2:
                bull_bonus -= 5
            if mom > 5:
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
    if vix_corr > 0.5:
        bearish_score += 15
        signals.append(f"VIX positive correlation ({vix_corr:.2f}) → bearish")
    elif vix_corr < -0.5:
        bullish_score += 15
        signals.append(f"VIX negative correlation ({vix_corr:.2f}) → bullish")
    else:
        signals.append(f"VIX neutral ({vix_corr:.2f})")

    # 3. DXY (dollar index)
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
    crypto_peers = ['BTC', 'ETH', 'BTCDOM', 'USDT_BTC']
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
                bearish_score += 10
                signals.append(f"Extreme positive z-score for {idx} ({z:.1f}) → possible bearish reversal")
            elif z < -2.5 and current.get(idx, 0) < 0:
                bullish_score += 10
                signals.append(f"Extreme negative z-score for {idx} ({z:.1f}) → possible bullish reversal")

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

# ========================== LOAD CORRELATION DATA FROM P05 .tmp_p ==========================
def load_correlation_data(symbol):
    """Read the latest correlation row from P05's .tmp_p file (dynamic column mapping)."""
    path = os.path.join(FEATURES_BASE_DIR, f"{symbol.lower()}_correlation.tmp_p")
    if not os.path.exists(path):
        log_issue("ERROR", f"Correlation file not found: {path}")
        return None
    try:
        with open(path, "r") as f:
            lines = f.readlines()
        if len(lines) < 2:
            log_issue("ERROR", "Correlation file has no data row")
            return None
        # Find the data section (skip comment lines starting with '#')
        data_lines = [l.strip() for l in lines if l.strip() and not l.strip().startswith('#')]
        if len(data_lines) < 2:
            log_issue("ERROR", "No data rows after comments")
            return None
        header = data_lines[0].split('\t')
        # The last data row is the most recent
        last_row = data_lines[-1].split('\t')
        if len(last_row) < len(header):
            log_issue("WARNING", "Last row has fewer columns than header")
            return None
        # Build dictionary of values
        values = {}
        for i, col in enumerate(header):
            if i < len(last_row):
                values[col] = last_row[i]
        # Extract current correlations, momentums, z_scores for known indices
        indices = ['BTC', 'ETH', 'BTCDOM', 'USDT_BTC', 'SPY', 'QQQ', 'DIA', 'GLD', 'USO', 'DXY', 'VIX']
        current_corr = {}
        momentums = {}
        z_scores = {}
        for idx in indices:
            corr_key = f"{idx}_corr"
            mom_key = f"{idx}_mom"
            z_key = f"{idx}_z"
            if corr_key in values:
                try:
                    current_corr[idx] = float(values[corr_key]) if values[corr_key] else 0.0
                except:
                    current_corr[idx] = 0.0
            else:
                current_corr[idx] = 0.0
            if mom_key in values:
                try:
                    momentums[idx] = float(values[mom_key]) if values[mom_key] else 0.0
                except:
                    momentums[idx] = 0.0
            else:
                momentums[idx] = 0.0
            if z_key in values:
                try:
                    z_scores[idx] = float(values[z_key]) if values[z_key] else 0.0
                except:
                    z_scores[idx] = 0.0
            else:
                z_scores[idx] = 0.0
        # Also extract lead asset and bias from the row if available (for reason)
        lead_asset = values.get('lead_asset', 'NONE')
        lag_asset = values.get('lag_asset', 'NONE')
        bias_hint = values.get('bias', 'neutral')
        log_issue("INFO", f"Loaded correlation data: lead={lead_asset}, lag={lag_asset}, bias_hint={bias_hint}")
        return {
            'current_correlations': current_corr,
            'momentums': momentums,
            'z_scores': z_scores,
            'lead_asset': lead_asset,
            'lag_asset': lag_asset,
            'bias_hint': bias_hint
        }
    except Exception as e:
        log_issue("ERROR", f"Failed to load correlation data: {e}")
        return None

# ========================== MAIN EXPERT FUNCTION (NO JSON) ==========================
def run_expert(symbol):
    log_issue("INFO", f"Starting E06 correlation expert for {symbol}")
    corr_data = load_correlation_data(symbol)
    if not corr_data:
        log_issue("ERROR", "No correlation data found, creating minimal output")
        out_path = os.path.join(FEATURES_BASE_DIR, f"{symbol.lower()}_E06_correlation.tsv")
        with open(out_path, "w") as f:
            header = ["timestamp", "bias", "confidence", "high_prob_scenario", "probability_estimate",
                      "reason", "signals", "net_score"]
            f.write("\t".join(header) + "\n")
            ts_now = int(time.time() * 1000)
            row = [str(ts_now), "neutral", "50", "", "50", "No correlation data", "", "0"]
            f.write("\t".join(row) + "\n")
        log_issue("INFO", f"Saved minimal correlation expert summary to {out_path}")
        return out_path

    result = analyze_correlation(corr_data)
    # Convert signals list to pipe‑separated string (no JSON)
    signals_str = "|".join(result['signals']) if result['signals'] else ""

    out_path = os.path.join(FEATURES_BASE_DIR, f"{symbol.lower()}_E06_correlation.tsv")
    with open(out_path, "w") as f:
        header = ["timestamp", "bias", "confidence", "high_prob_scenario", "probability_estimate",
                  "reason", "signals", "net_score"]
        f.write("\t".join(header) + "\n")
        ts_now = int(time.time() * 1000)
        row = [
            str(ts_now),
            result['bias'],
            str(result['confidence']),
            result['high_prob_scenario'] if result['high_prob_scenario'] else "",
            str(result['probability_estimate']),
            result['reason'],
            signals_str,
            str(result['net_score'])
        ]
        f.write("\t".join(row) + "\n")
    log_issue("INFO", f"Saved correlation expert summary to {out_path}")
    return out_path

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python E06_correlation_expert.py SYMBOL")
        sys.exit(1)
    success = run_expert(sys.argv[1].upper())
    sys.exit(0 if success else 1)