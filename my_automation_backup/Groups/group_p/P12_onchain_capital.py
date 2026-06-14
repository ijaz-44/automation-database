#!/usr/bin/env python3
"""
P12_onchain_capital.py – Process Raw On‑Chain Data
- Reads {symbol}_onchain.tmp_x (TSV from X23)
- Parses stablecoin, binance snapshot, whale, exchange netflow, liquidation rows
- Computes: total stablecoin netflow, whale ratio, whale transaction count,
  exchange netflow (latest), liquidation count, funding rate, depth imbalance
- Derives direction bias (UP/DOWN/NEUTRAL), confidence (0-100), duration (1h candles),
  target price and stop loss using ATR
- Outputs TSV {symbol}_onchain.tmp_p with raw data (commented) + derived features
- Logs to p12_onchain_capital_issues.log (file only, minimal console)
- Input file is NOT deleted.
"""

import os
import sys
import time
import math
from collections import defaultdict

# ========== CONFIGURATION ==========
FEATURES_BASE_DIR = os.path.join("market_data", "binance", "symbols")
LOG_FILE = os.path.join(FEATURES_BASE_DIR, "p12_onchain_capital_issues.log")
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
def process_onchain(symbol):
    print(f"[P12] Starting on‑chain processing for {symbol}")
    start_time = time.time()
    log_issue("INFO", f"Starting on‑chain processing for {symbol}")

    tmp_x_path = os.path.join(FEATURES_BASE_DIR, f"{symbol.lower()}_onchain.tmp_x")
    if not os.path.exists(tmp_x_path):
        log_issue("ERROR", f"Input file not found: {tmp_x_path}")
        return False

    # ---------- Read raw data and store raw lines ----------
    raw_lines = []
    stablecoins = {}
    binance_snapshot = {}
    whale_tx_count = 0
    exchange_netflow_latest = 0
    liquidation_count = 0

    with open(tmp_x_path, "r") as f:
        header = f.readline().strip()
        raw_lines.append(header)  # header line
        for line in f:
            line = line.strip()
            if not line:
                continue
            raw_lines.append(line)
            parts = line.split('\t')
            if len(parts) < 3:
                continue
            row_type = parts[0]
            if row_type == "stablecoin" and len(parts) >= 7:
                try:
                    symbol_stable = parts[2]
                    netflow = float(parts[6])
                    stablecoins[symbol_stable] = netflow
                except:
                    pass
            elif row_type == "binance_snapshot" and len(parts) >= 9:
                try:
                    binance_snapshot = {
                        "timestamp": int(parts[1]),
                        "price": float(parts[2]),
                        "oi": float(parts[3]),
                        "funding_rate": float(parts[4]),
                        "whale_ratio": float(parts[5]),
                        "taker_ratio": float(parts[6]),
                        "depth_imbalance": float(parts[7]),
                        "atr": float(parts[8])
                    }
                except:
                    pass
            elif row_type == "whale":
                whale_tx_count += 1
            elif row_type == "exchange_netflow" and len(parts) >= 3:
                try:
                    netflow = float(parts[2])
                    # Keep the first (latest) as file order is descending
                    if exchange_netflow_latest == 0:
                        exchange_netflow_latest = netflow
                except:
                    pass
            elif row_type == "liquidation":
                liquidation_count += 1

    if not binance_snapshot:
        log_issue("ERROR", "No binance snapshot data found")
        return False

    # ---------- Compute derived features ----------
    usdt_netflow = stablecoins.get("USDT", 0.0)
    usdc_netflow = stablecoins.get("USDC", 0.0)
    total_stable_netflow = usdt_netflow + usdc_netflow

    whale_ratio = binance_snapshot.get("whale_ratio", 1.0)
    funding_rate = binance_snapshot.get("funding_rate", 0.0)
    depth_imbalance = binance_snapshot.get("depth_imbalance", 0.0)
    atr = binance_snapshot.get("atr", 0.0)
    price = binance_snapshot.get("price", 0.0)
    exchange_netflow = exchange_netflow_latest

    # ---------- Scoring ----------
    bullish_score = 0
    bearish_score = 0
    signals = []

    if total_stable_netflow > 300_000_000:
        bullish_score += 35
        signals.append("Strong stablecoin inflow >300M")
    elif total_stable_netflow > 100_000_000:
        bullish_score += 20
        signals.append("Moderate stablecoin inflow")
    elif total_stable_netflow < -200_000_000:
        bearish_score += 35
        signals.append("Strong stablecoin outflow <-200M")
    elif total_stable_netflow < -50_000_000:
        bearish_score += 20
        signals.append("Moderate stablecoin outflow")

    if whale_ratio > 1.5:
        bullish_score += 25
        signals.append("Whale ratio >1.5 (very bullish)")
    elif whale_ratio > 1.2:
        bullish_score += 12
        signals.append("Whale ratio >1.2 (bullish)")
    elif whale_ratio < 0.65:
        bearish_score += 25
        signals.append("Whale ratio <0.65 (very bearish)")
    elif whale_ratio < 0.85:
        bearish_score += 12
        signals.append("Whale ratio <0.85 (bearish)")

    if exchange_netflow > 100_000_000:
        bearish_score += 20
        signals.append("Exchange net inflow >100M (selling pressure)")
    elif exchange_netflow > 30_000_000:
        bearish_score += 10
        signals.append("Exchange net inflow moderate")
    elif exchange_netflow < -100_000_000:
        bullish_score += 20
        signals.append("Exchange net outflow <-100M (accumulation)")
    elif exchange_netflow < -30_000_000:
        bullish_score += 10
        signals.append("Exchange net outflow moderate")

    if funding_rate > 0.0003:
        bearish_score += 15
        signals.append("High funding rate >0.03% (overbought)")
    elif funding_rate < -0.0003:
        bullish_score += 15
        signals.append("Negative funding rate <-0.03% (oversold)")

    if depth_imbalance > 0.3:
        bullish_score += 15
        signals.append("Depth imbalance >0.3 (strong buy pressure)")
    elif depth_imbalance > 0.15:
        bullish_score += 8
        signals.append("Depth imbalance positive")
    elif depth_imbalance < -0.3:
        bearish_score += 15
        signals.append("Depth imbalance <-0.3 (strong sell pressure)")
    elif depth_imbalance < -0.15:
        bearish_score += 8
        signals.append("Depth imbalance negative")

    if whale_tx_count > 80:
        signals.append(f"Very high whale activity ({whale_tx_count} txns)")
        if bullish_score > bearish_score:
            bullish_score += 10
        elif bearish_score > bullish_score:
            bearish_score += 10
    elif whale_tx_count > 40:
        signals.append(f"High whale activity ({whale_tx_count} txns)")
        if bullish_score > bearish_score:
            bullish_score += 5
        elif bearish_score > bullish_score:
            bearish_score += 5

    if liquidation_count > 20:
        signals.append(f"High liquidation count ({liquidation_count})")

    net_score = bullish_score - bearish_score
    net_score = max(-100, min(100, net_score))

    if net_score >= 30:
        bias = "UP"
        confidence = min(95, 60 + net_score // 2)
    elif net_score <= -30:
        bias = "DOWN"
        confidence = min(95, 60 + abs(net_score) // 2)
    else:
        bias = "NEUTRAL"
        confidence = 50 + abs(net_score) // 2

    if abs(net_score) >= 60:
        duration = 4
    elif abs(net_score) >= 30:
        duration = 3
    else:
        duration = 2
    if whale_tx_count > 80:
        duration = min(6, duration + 1)

    if price > 0 and atr > 0:
        if bias == "UP":
            target = price + atr * 1.2
            stop_loss = price - atr * 0.6
        elif bias == "DOWN":
            target = price - atr * 1.2
            stop_loss = price + atr * 0.6
        else:
            target = price
            stop_loss = price
    else:
        target = price
        stop_loss = price

    # ---------- Write output .tmp_p TSV (raw data + derived) ----------
    tmp_p_path = os.path.join(FEATURES_BASE_DIR, f"{symbol.lower()}_onchain.tmp_p")
    with open(tmp_p_path, "w") as out:
        out.write("# === Raw on‑chain data ===\n")
        for raw_line in raw_lines:
            out.write("# " + raw_line + "\n")
        out.write("# === Derived features ===\n")
        header = [
            "timestamp", "price", "total_stable_netflow", "whale_ratio",
            "exchange_netflow", "funding_rate", "depth_imbalance",
            "whale_tx_count", "liquidation_count", "net_score",
            "bias", "confidence", "duration_candles", "target_price", "stop_loss"
        ]
        out.write("\t".join(header) + "\n")
        ts = int(time.time() * 1000)
        row = [
            str(ts),
            f"{price:.2f}",
            f"{total_stable_netflow:.0f}",
            f"{whale_ratio:.4f}",
            f"{exchange_netflow:.0f}",
            f"{funding_rate:.8f}",
            f"{depth_imbalance:.4f}",
            str(whale_tx_count),
            str(liquidation_count),
            str(net_score),
            bias,
            str(confidence),
            str(duration),
            f"{target:.2f}",
            f"{stop_loss:.2f}"
        ]
        out.write("\t".join(row) + "\n")

    # Input file is NOT deleted
    # if os.path.exists(tmp_x_path):
    #     os.remove(tmp_x_path)

    elapsed = time.time() - start_time
    print(f"[P12] Success ({elapsed:.1f}s) -> {os.path.basename(tmp_p_path)}")
    log_issue("INFO", f"Processing complete for {symbol} in {elapsed:.2f}s -> {tmp_p_path}")
    return True

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python P12_onchain_capital.py SYMBOL")
        sys.exit(1)
    success = process_onchain(sys.argv[1].upper())
    sys.exit(0 if success else 1)