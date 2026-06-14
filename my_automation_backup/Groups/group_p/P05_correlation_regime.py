#!/usr/bin/env python3
"""
P05_correlation_regime.py – Process Raw Correlation Data
- Reads {symbol}_correlation.tmp_x (TSV from X09)
- Computes rolling correlation (15‑min window) for each index vs symbol
- Computes momentum (percentage change of correlation over 30 min)
- Computes z‑score of correlation (rolling window of 30 values)
- Outputs {symbol}_correlation.tmp_p with per‑minute features
- Also derives lead‑lag, beta, regime, bias, confidence (from latest row)
- Logs to p05_correlation_issues.log (file only, minimal console)
- Input file is NOT deleted.
"""

import os
import sys
import time
import math
from collections import deque

# ========== CONFIGURATION ==========
FEATURES_BASE_DIR = os.path.join("market_data", "binance", "symbols")
LOG_FILE = os.path.join(FEATURES_BASE_DIR, "p05_correlation_issues.log")
LOG_MAX_SIZE = 5_000_000
WINDOW_CORR = 15
WINDOW_ZSCORE = 30
MOMENTUM_WINDOW = 30

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

# ========== HELPER FUNCTIONS ==========
def pearson_correlation(x, y):
    n = len(x)
    if n < 2:
        return 0.0
    mean_x = sum(x) / n
    mean_y = sum(y) / n
    num = sum((x[i] - mean_x) * (y[i] - mean_y) for i in range(n))
    den_x = sum((xi - mean_x) ** 2 for xi in x)
    den_y = sum((yi - mean_y) ** 2 for yi in y)
    if den_x == 0 or den_y == 0:
        return 0.0
    return num / math.sqrt(den_x * den_y)

def safe_zscore(value, history):
    n = len(history)
    if n < 2:
        return 0.0
    mean = sum(history) / n
    var = sum((v - mean) ** 2 for v in history) / n
    std = math.sqrt(var) if var > 0 else 1e-6
    return (value - mean) / std

# ========== MAIN PROCESSING ==========
def process_correlation(symbol):
    print(f"[P05] Starting correlation processing for {symbol}")
    start_time = time.time()
    log_issue("INFO", f"Starting correlation processing for {symbol}")

    tmp_x_path = os.path.join(FEATURES_BASE_DIR, f"{symbol.lower()}_correlation.tmp_x")
    if not os.path.exists(tmp_x_path):
        log_issue("ERROR", f"Input file not found: {tmp_x_path}")
        return False

    # ---------- Read data ----------
    timestamps = []
    symbol_prices = []
    index_series = {
        'BTC': [], 'ETH': [], 'BTCDOM': [], 'USDT_BTC': [],
        'SPY': [], 'QQQ': [], 'DIA': [], 'GLD': [], 'USO': [], 'DXY': [], 'VIX': []
    }
    
    # Also store raw lines for output (commented)
    raw_lines = []
    
    with open(tmp_x_path, "r") as f:
        header = f.readline().strip()
        raw_lines.append(header)  # keep header
        for line in f:
            line = line.strip()
            if not line:
                continue
            raw_lines.append(line)
            parts = line.split('\t')
            if len(parts) < 13:
                continue
            try:
                ts = int(parts[0])
                sym_price = float(parts[1]) if parts[1] else None
                timestamps.append(ts)
                symbol_prices.append(sym_price)
                idx_names = list(index_series.keys())
                for i, name in enumerate(idx_names):
                    val = parts[2 + i]
                    if val:
                        index_series[name].append(float(val))
                    else:
                        index_series[name].append(None)
            except Exception as e:
                log_issue("WARNING", f"Error parsing row: {e}", row=line[:100])

    n = len(timestamps)
    
    # If no data rows, create a minimal output file with just raw data (commented) and a placeholder row
    if n == 0:
        print(f"[P05] No data rows in {tmp_x_path}, creating minimal output")
        tmp_p_path = os.path.join(FEATURES_BASE_DIR, f"{symbol.lower()}_correlation.tmp_p")
        with open(tmp_p_path, "w") as out:
            out.write("# === Raw data from X09 ===\n")
            for line in raw_lines:
                out.write("# " + line + "\n")
            out.write("# === Derived features ===\n")
            out.write("timestamp\tdata_points\tlead_asset\tlag_asset\tlead_strength\tbeta\tcorrelation_regime_summary\timpact_score\tbias\tconfidence\tnext_direction_hint\n")
            out.write(f"{int(time.time()*1000)}\t0\tNONE\tNONE\t0\t1\tNO_DATA\t0\tneutral\t50\tNEUTRAL\n")
        print(f"[P05] Success (minimal) -> {os.path.basename(tmp_p_path)}")
        return True
    
    if n < WINDOW_CORR + 5:
        log_issue("WARNING", f"Insufficient data points: {n} (need {WINDOW_CORR+5}), creating minimal output")
        tmp_p_path = os.path.join(FEATURES_BASE_DIR, f"{symbol.lower()}_correlation.tmp_p")
        with open(tmp_p_path, "w") as out:
            out.write("# === Raw data from X09 ===\n")
            for line in raw_lines:
                out.write("# " + line + "\n")
            out.write("# === Derived features ===\n")
            out.write("timestamp\tdata_points\tlead_asset\tlag_asset\tlead_strength\tbeta\tcorrelation_regime_summary\timpact_score\tbias\tconfidence\tnext_direction_hint\n")
            out.write(f"{int(time.time()*1000)}\t{n}\tNONE\tNONE\t0\t1\tINSUFFICIENT_DATA\t0\tneutral\t50\tNEUTRAL\n")
        print(f"[P05] Success (minimal) -> {os.path.basename(tmp_p_path)}")
        return True

    # ---------- Compute rolling correlation for each index ----------
    corr_values = {name: [None] * n for name in index_series}
    for name, series in index_series.items():
        for i in range(WINDOW_CORR - 1, n):
            y_window = []
            x_window = []
            for j in range(i - WINDOW_CORR + 1, i + 1):
                if symbol_prices[j] is not None and series[j] is not None:
                    y_window.append(symbol_prices[j])
                    x_window.append(series[j])
            if len(y_window) >= WINDOW_CORR * 0.7:
                corr = pearson_correlation(y_window, x_window)
                corr_values[name][i] = corr
            else:
                corr_values[name][i] = None

    # ---------- Compute momentum ----------
    momentum = {name: [None] * n for name in index_series}
    for name in index_series:
        for i in range(WINDOW_CORR * 2, n):
            if corr_values[name][i] is not None and corr_values[name][i - WINDOW_CORR] is not None:
                prev = corr_values[name][i - WINDOW_CORR]
                curr = corr_values[name][i]
                if prev != 0:
                    mom = (curr - prev) / abs(prev) * 100
                else:
                    mom = 0.0
                momentum[name][i] = mom

    # ---------- Compute z‑score ----------
    zscore = {name: [None] * n for name in index_series}
    for name in index_series:
        history = deque(maxlen=WINDOW_ZSCORE)
        for i in range(n):
            if corr_values[name][i] is not None:
                history.append(corr_values[name][i])
            if len(history) >= 5:
                z = safe_zscore(corr_values[name][i], list(history))
                zscore[name][i] = round(z, 4)
            else:
                zscore[name][i] = None

    # ---------- Write output ----------
    indices = list(index_series.keys())
    tmp_p_path = os.path.join(FEATURES_BASE_DIR, f"{symbol.lower()}_correlation.tmp_p")
    with open(tmp_p_path, "w") as out:
        out.write("# === Raw data from X09 ===\n")
        for line in raw_lines:
            out.write("# " + line + "\n")
        out.write("# === Per‑minute derived features ===\n")
        
        header = ["timestamp", "data_points"]
        for idx in indices:
            header.append(f"{idx}_corr")
            header.append(f"{idx}_mom")
            header.append(f"{idx}_z")
        header.extend(["lead_asset", "lag_asset", "lead_strength", "beta",
                       "correlation_regime_summary", "impact_score", "bias", "confidence", "next_direction_hint"])
        out.write("\t".join(header) + "\n")

        for i in range(n):
            row = [str(timestamps[i]), str(n - i)]
            for idx in indices:
                corr_val = corr_values[idx][i] if corr_values[idx][i] is not None else ""
                mom_val = momentum[idx][i] if momentum[idx][i] is not None else ""
                z_val = zscore[idx][i] if zscore[idx][i] is not None else ""
                row.append(f"{corr_val:.4f}" if corr_val != "" else "")
                row.append(f"{mom_val:.2f}" if mom_val != "" else "")
                row.append(f"{z_val:.4f}" if z_val != "" else "")
            if i == n-1:
                lead_lag = compute_lead_lag(indices, momentum, i)
                btc_corr = corr_values['BTC'][i] if corr_values['BTC'][i] is not None else 0.5
                beta = round(max(0.3, min(3.0, btc_corr * 1.5)), 3)
                regime = compute_regime_summary(indices, corr_values, zscore, i)
                impact = compute_impact_score(indices, corr_values, zscore, i)
                if impact >= 30:
                    bias = "bullish"
                elif impact <= -30:
                    bias = "bearish"
                else:
                    bias = "neutral"
                conf = min(95, max(30, 50 + abs(impact)//2))
                hint = "UP" if bias == "bullish" else "DOWN" if bias == "bearish" else "NEUTRAL"
                row.extend([
                    lead_lag['lead_asset'], lead_lag['lag_asset'],
                    str(lead_lag['strength']), str(beta),
                    regime, str(impact), bias, str(conf), hint
                ])
            else:
                row.extend([""] * 9)
            out.write("\t".join(row) + "\n")

    elapsed = time.time() - start_time
    print(f"[P05] Success ({elapsed:.1f}s) -> {os.path.basename(tmp_p_path)}")
    log_issue("INFO", f"Processing complete for {symbol} in {elapsed:.2f}s -> {tmp_p_path}")
    return True

# ========== Helper functions for aggregated fields ==========
def compute_lead_lag(indices, momentum_dict, idx):
    btc_mom = momentum_dict['BTC'][idx] if momentum_dict['BTC'][idx] is not None else 0
    eth_mom = momentum_dict['ETH'][idx] if momentum_dict['ETH'][idx] is not None else 0
    if abs(btc_mom - eth_mom) > 5:
        if btc_mom > eth_mom:
            lead, lag, strength = "BTC", "ETH", min(10, abs(btc_mom - eth_mom)/2)
        else:
            lead, lag, strength = "ETH", "BTC", min(10, abs(eth_mom - btc_mom)/2)
    else:
        lead, lag, strength = "NONE", "NONE", 0
    return {'lead_asset': lead, 'lag_asset': lag, 'strength': round(strength, 2)}

def compute_regime_summary(indices, corr_values, zscore, idx):
    regimes = []
    for asset in ['BTC', 'ETH', 'SPY', 'DXY', 'VIX']:
        corr = corr_values[asset][idx] if corr_values[asset][idx] is not None else 0
        z = zscore[asset][idx] if zscore[asset][idx] is not None else 0
        if abs(corr) > 0.6:
            if corr > 0:
                regimes.append(f"{asset}:ALIGNED_BULLISH" + ("_EXTREME" if z>1.5 else ""))
            else:
                regimes.append(f"{asset}:ALIGNED_BEARISH" + ("_EXTREME" if z<-1.5 else ""))
        elif abs(corr) < 0.2:
            regimes.append(f"{asset}:DIVERGENT")
        else:
            regimes.append(f"{asset}:NEUTRAL")
    return "|".join(regimes)

def compute_impact_score(indices, corr_values, zscore, idx):
    score = 0
    spy_corr = corr_values['SPY'][idx] if corr_values['SPY'][idx] is not None else 0
    spy_z = zscore['SPY'][idx] if zscore['SPY'][idx] is not None else 0
    if spy_corr > 0.5:
        score += 15
        if spy_z > 1:
            score += 5
    elif spy_corr < -0.5:
        score -= 10
    dxy_corr = corr_values['DXY'][idx] if corr_values['DXY'][idx] is not None else 0
    if dxy_corr < -0.4:
        score += 10
    elif dxy_corr > 0.4:
        score -= 10
    vix_corr = corr_values['VIX'][idx] if corr_values['VIX'][idx] is not None else 0
    if vix_corr < -0.4:
        score += 10
    elif vix_corr > 0.4:
        score -= 10
    btc_eth_corr = corr_values['ETH'][idx] if corr_values['ETH'][idx] is not None else 0
    if btc_eth_corr > 0.7:
        score += 10
    elif btc_eth_corr < 0.3:
        score -= 5
    return max(-100, min(100, score))

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python P05_correlation_regime.py SYMBOL")
        sys.exit(1)
    success = process_correlation(sys.argv[1].upper())
    sys.exit(0 if success else 1)