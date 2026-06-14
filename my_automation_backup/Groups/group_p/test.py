#!/usr/bin/env python3
"""
fix_cvd_files.py – Automatically finds all CVD .tmp_x files and creates .tmp_p files.
- Scans market_data/binance/symbols/ for *_cvd1.tmp_x and *_cvd2.tmp_x
- Decodes trades using the same logic as P02_cvd_flow.py
- Writes per‑minute features to {symbol}_cvd1.tmp_p and {symbol}_cvd2.tmp_p
- Appends summary to the second file.
- Works from any subdirectory (automatically finds base folder).
"""

import os
import sys
import time
import math
from collections import defaultdict

# ---------- Find base directory ----------
def find_base_dir():
    current = os.path.dirname(os.path.abspath(__file__))
    while True:
        if os.path.isdir(os.path.join(current, "market_data")):
            return current
        parent = os.path.dirname(current)
        if parent == current:
            break
        current = parent
    return None

BASE_DIR = find_base_dir()
if BASE_DIR is None:
    print("ERROR: Could not find 'market_data' directory. Make sure this script is inside 'My Automation'.")
    sys.exit(1)

SYMBOLS_DIR = os.path.join(BASE_DIR, "market_data", "binance", "symbols")
if not os.path.exists(SYMBOLS_DIR):
    print(f"ERROR: Symbols directory not found: {SYMBOLS_DIR}")
    sys.exit(1)

print(f"Base directory: {BASE_DIR}")
print(f"Symbols directory: {SYMBOLS_DIR}\n")

# ---------- Decoding (same as working test script) ----------
BASE62_ALPH = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

def base62_to_int(s):
    val = 0
    for ch in s:
        val = val * 62 + BASE62_ALPH.index(ch)
    return val

def decode_int(s):
    if not s:
        return 0
    if s.startswith('n'):
        return -base62_to_int(s[1:])
    else:
        return base62_to_int(s)

def decode_trades_from_file(filepath):
    if not os.path.exists(filepath):
        return None, 0, 0

    price_prec = 8
    qty_prec = 8
    price_mult = 10 ** price_prec
    qty_mult = 10 ** qty_prec

    with open(filepath, 'r') as f:
        lines = f.readlines()

    data_lines = []
    for line in lines:
        line = line.strip()
        if not line:
            continue
        if line.startswith('#'):
            if 'price_precision=' in line:
                parts = line.split()
                for p in parts:
                    if p.startswith('price_precision='):
                        price_prec = int(p.split('=')[1])
                        price_mult = 10 ** price_prec
                    elif p.startswith('qty_precision='):
                        qty_prec = int(p.split('=')[1])
                        qty_mult = 10 ** qty_prec
            continue
        data_lines.append(line)

    trades = []
    for line in data_lines:
        parts = line.split('\t')
        if len(parts) != 2:
            continue
        _, row_str = parts
        trade_strings = row_str.split(';')
        if not trade_strings:
            continue
        first = trade_strings[0].split(',')
        if len(first) != 4:
            continue
        try:
            ts_abs = decode_int(first[0])
            price_abs = decode_int(first[1])
            qty_abs = decode_int(first[2])
            sell_abs = int(first[3])
            trades.append({
                'timestamp': ts_abs,
                'price': price_abs / price_mult,
                'qty': qty_abs / qty_mult,
                'is_sell': sell_abs == 1
            })
            prev_ts = ts_abs
            prev_price = price_abs
            prev_qty = qty_abs
            for trade_str in trade_strings[1:]:
                parts2 = trade_str.split(',')
                if len(parts2) != 4:
                    break
                dt = decode_int(parts2[0])
                dp = decode_int(parts2[1])
                dq = decode_int(parts2[2])
                sell = int(parts2[3])
                ts = prev_ts + dt
                price_int = prev_price + dp
                qty_int = prev_qty + dq
                trades.append({
                    'timestamp': ts,
                    'price': price_int / price_mult,
                    'qty': qty_int / qty_mult,
                    'is_sell': sell == 1
                })
                prev_ts, prev_price, prev_qty = ts, price_int, qty_int
        except Exception as e:
            print(f"  Decoding error in {os.path.basename(filepath)}: {e}")
            continue
    return trades, price_mult, qty_mult

def linear_slope(values):
    n = len(values)
    if n < 2:
        return 0.0
    x = list(range(n))
    mean_x = sum(x) / n
    mean_y = sum(values) / n
    num = sum((x[i] - mean_x) * (values[i] - mean_y) for i in range(n))
    den = sum((x[i] - mean_x) ** 2 for i in range(n))
    return num / den if den != 0 else 0.0

def detect_divergence(cvd_series, price_series, lookback=10):
    if len(cvd_series) < lookback * 2 or len(price_series) < lookback * 2:
        return "none"
    recent_cvd = cvd_series[-lookback:]
    recent_price = price_series[-lookback:]
    prev_cvd = cvd_series[-lookback*2:-lookback]
    prev_price = price_series[-lookback*2:-lookback]
    if not prev_price:
        return "none"
    min_price_idx = recent_price.index(min(recent_price))
    min_price = recent_price[min_price_idx]
    cvd_at_min = recent_cvd[min_price_idx]
    prev_min_price = min(prev_price)
    prev_cvd_at_prev_min = prev_cvd[prev_price.index(prev_min_price)] if prev_price else cvd_at_min
    if min_price < prev_min_price and cvd_at_min > prev_cvd_at_prev_min:
        return "bullish"
    max_price_idx = recent_price.index(max(recent_price))
    max_price = recent_price[max_price_idx]
    cvd_at_max = recent_cvd[max_price_idx]
    prev_max_price = max(prev_price)
    prev_cvd_at_prev_max = prev_cvd[prev_price.index(prev_max_price)] if prev_price else cvd_at_max
    if max_price > prev_max_price and cvd_at_max < prev_cvd_at_prev_max:
        return "bearish"
    return "none"

def detect_absorption(cvd_series, window=5, threshold_ratio=0.3):
    if len(cvd_series) < window * 2:
        return 0.0
    recent_cvd = cvd_series[-window:]
    cvd_range = max(recent_cvd) - min(recent_cvd)
    older_range = max(cvd_series[-window*2:-window]) - min(cvd_series[-window*2:-window])
    if older_range == 0:
        return 0.0
    ratio = cvd_range / older_range
    return 1.0 if ratio < threshold_ratio else 0.0

def whale_detection(trades_in_minute, avg_trade_size):
    threshold = avg_trade_size * 5.0
    whale_buy = 0.0
    whale_sell = 0.0
    for t in trades_in_minute:
        if t['qty'] >= threshold:
            if t['is_sell']:
                whale_sell += t['qty']
            else:
                whale_buy += t['qty']
    return whale_buy - whale_sell

def process_trades_chunk(trades):
    if not trades:
        return [], 0, 0

    min_ts = trades[0]['timestamp']
    max_ts = trades[-1]['timestamp']
    start_min = (min_ts // 60000) * 60000
    end_min = (max_ts // 60000) * 60000
    num_buckets = (end_min - start_min) // 60000 + 1
    buckets = [[] for _ in range(num_buckets)]
    bucket_timestamps = [start_min + i*60000 for i in range(num_buckets)]

    for t in trades:
        idx = (t['timestamp'] - start_min) // 60000
        if 0 <= idx < num_buckets:
            buckets[idx].append(t)

    features = []
    total_cvd = 0.0
    cvd_history = []
    price_history = []
    volume_history = []

    for i, bucket_trades in enumerate(buckets):
        if not bucket_trades:
            features.append({
                'timestamp': bucket_timestamps[i],
                'trade_count': 0,
                'buy_vol': 0.0,
                'sell_vol': 0.0,
                'net_delta': 0.0,
                'cumulative_cvd': total_cvd,
                'vwap': None,
                'avg_trade_size': 0.0,
                'whale_delta': 0.0,
                'speed': 0.0,
            })
            cvd_history.append(total_cvd)
            price_history.append(None)
            volume_history.append(0)
            continue

        buy_vol = sum(t['qty'] for t in bucket_trades if not t['is_sell'])
        sell_vol = sum(t['qty'] for t in bucket_trades if t['is_sell'])
        net_delta = buy_vol - sell_vol
        total_cvd += net_delta
        trade_count = len(bucket_trades)
        avg_qty = (buy_vol + sell_vol) / trade_count if trade_count else 0.0
        total_value = sum(t['price'] * t['qty'] for t in bucket_trades)
        total_qty = buy_vol + sell_vol
        vwap = total_value / total_qty if total_qty > 0 else None
        speed = trade_count / ((bucket_trades[-1]['timestamp'] - bucket_trades[0]['timestamp']) / 1000.0) if trade_count > 1 else 0.0
        whale_delta = whale_detection(bucket_trades, avg_qty)

        features.append({
            'timestamp': bucket_timestamps[i],
            'trade_count': trade_count,
            'buy_vol': buy_vol,
            'sell_vol': sell_vol,
            'net_delta': net_delta,
            'cumulative_cvd': total_cvd,
            'vwap': vwap,
            'avg_trade_size': avg_qty,
            'whale_delta': whale_delta,
            'speed': speed,
        })
        cvd_history.append(total_cvd)
        price_history.append(vwap if vwap is not None else (features[i-1]['vwap'] if i>0 else None))
        volume_history.append(total_qty)

    # Micro‑burst delta
    trade_deltas = [t['qty'] if not t['is_sell'] else -t['qty'] for t in trades]
    cum_trade_delta = [0.0]
    for d in trade_deltas:
        cum_trade_delta.append(cum_trade_delta[-1] + d)
    micro_burst = [0.0] * num_buckets
    trade_idx = 0
    for i, bucket in enumerate(buckets):
        end_idx = trade_idx + len(bucket)
        if end_idx >= 100:
            start_idx = max(0, end_idx - 100)
            micro_burst[i] = cum_trade_delta[end_idx] - cum_trade_delta[start_idx]
        else:
            micro_burst[i] = cum_trade_delta[end_idx]
        trade_idx = end_idx

    for i in range(num_buckets):
        if i >= 9:
            features[i]['cvd_slope_10'] = linear_slope(cvd_history[i-9:i+1])
        else:
            features[i]['cvd_slope_10'] = 0.0
        if i >= 19:
            recent_slope = linear_slope(cvd_history[i-4:i+1])
            prev_slope = linear_slope(cvd_history[i-9:i-4])
            features[i]['cvd_acceleration'] = recent_slope - prev_slope
        else:
            features[i]['cvd_acceleration'] = 0.0
        if i >= 19:
            price_vals = [p for p in price_history[i-19:i+1] if p is not None]
            cvd_vals = cvd_history[i-19:i+1]
            if len(price_vals) >= 10 and len(cvd_vals) >= 10:
                features[i]['divergence'] = detect_divergence(cvd_vals, price_vals, lookback=10)
            else:
                features[i]['divergence'] = "none"
        else:
            features[i]['divergence'] = "none"
        if i >= 9:
            features[i]['absorption_score'] = detect_absorption(cvd_history[:i+1], window=5, threshold_ratio=0.3)
        else:
            features[i]['absorption_score'] = 0.0
        buy = features[i]['buy_vol']
        sell = features[i]['sell_vol']
        total = buy + sell
        if total > 0:
            if sell > 0 and buy / sell >= 2.5:
                features[i]['imbalance_score'] = (buy/sell - 2.5) * total
            elif buy > 0 and sell / buy >= 2.5:
                features[i]['imbalance_score'] = -(sell/buy - 2.5) * total
            else:
                features[i]['imbalance_score'] = 0.0
        else:
            features[i]['imbalance_score'] = 0.0
        features[i]['micro_burst_delta'] = micro_burst[i]

    return features, num_buckets, total_cvd

def write_output_file(symbol, suffix, features):
    out_path = os.path.join(SYMBOLS_DIR, f"{symbol.lower()}_cvd{suffix}.tmp_p")
    with open(out_path, "w") as out:
        header = [
            "timestamp", "trade_count", "buy_volume", "sell_volume", "net_delta",
            "cumulative_cvd", "vwap", "avg_trade_size", "whale_delta", "speed_score",
            "cvd_slope_10", "cvd_acceleration", "divergence", "absorption_score",
            "imbalance_score", "micro_burst_delta"
        ]
        out.write("\t".join(header) + "\n")
        for f in features:
            row = [
                str(f['timestamp']),
                str(f['trade_count']),
                f"{f['buy_vol']:.4f}",
                f"{f['sell_vol']:.4f}",
                f"{f['net_delta']:.4f}",
                f"{f['cumulative_cvd']:.4f}",
                f"{f['vwap']:.4f}" if f['vwap'] is not None else "",
                f"{f['avg_trade_size']:.4f}",
                f"{f['whale_delta']:.4f}",
                f"{f['speed']:.2f}",
                f"{f['cvd_slope_10']:.4f}",
                f"{f['cvd_acceleration']:.4f}",
                f['divergence'],
                f"{f['absorption_score']:.2f}",
                f"{f['imbalance_score']:.2f}",
                f"{f['micro_burst_delta']:.4f}"
            ]
            out.write("\t".join(row) + "\n")
    print(f"  Wrote {len(features)} rows to {out_path}")

def write_empty_file(symbol, suffix):
    out_path = os.path.join(SYMBOLS_DIR, f"{symbol.lower()}_cvd{suffix}.tmp_p")
    with open(out_path, "w") as out:
        out.write("# Empty output (no trades or error)\n")
        header = ["timestamp","trade_count","buy_volume","sell_volume","net_delta","cumulative_cvd","vwap","avg_trade_size","whale_delta","speed_score","cvd_slope_10","cvd_acceleration","divergence","absorption_score","imbalance_score","micro_burst_delta"]
        out.write("\t".join(header) + "\n")
        out.write("0\t0\t0\t0\t0\t0\t\t0\t0\t0\t0\t0\tnone\t0\t0\t0\n")
    print(f"  Created empty file: {out_path}")

def process_symbol(symbol):
    print(f"\nProcessing {symbol.upper()}")
    tmp1 = os.path.join(SYMBOLS_DIR, f"{symbol.lower()}_cvd1.tmp_x")
    tmp2 = os.path.join(SYMBOLS_DIR, f"{symbol.lower()}_cvd2.tmp_x")

    # Part 1
    trades1, _, _ = decode_trades_from_file(tmp1)
    if trades1 is None or len(trades1) == 0:
        write_empty_file(symbol, "1")
    else:
        print(f"  Part1: {len(trades1)} trades")
        features1, _, _ = process_trades_chunk(trades1)
        if features1:
            write_output_file(symbol, "1", features1)
        else:
            write_empty_file(symbol, "1")

    # Part 2
    trades2, _, _ = decode_trades_from_file(tmp2)
    if trades2 is None or len(trades2) == 0:
        write_empty_file(symbol, "2")
    else:
        print(f"  Part2: {len(trades2)} trades")
        features2, _, total_cvd2 = process_trades_chunk(trades2)
        if features2:
            write_output_file(symbol, "2", features2)
            # Append summary
            total_buy = sum(t['qty'] for t in trades2 if not t['is_sell'])
            total_sell = sum(t['qty'] for t in trades2 if t['is_sell'])
            total_volume = total_buy + total_sell
            out2 = os.path.join(SYMBOLS_DIR, f"{symbol.lower()}_cvd2.tmp_p")
            with open(out2, "a") as out:
                out.write("\n#=== SUMMARY ===\n")
                out.write(f"# total_trades\t{len(trades2)}\n")
                out.write(f"# total_volume\t{total_volume:.4f}\n")
                out.write(f"# total_buy\t{total_buy:.4f}\n")
                out.write(f"# total_sell\t{total_sell:.4f}\n")
                out.write(f"# net_delta\t{(total_buy - total_sell):.4f}\n")
                out.write(f"# cumulative_cvd\t{total_cvd2:.4f}\n")
                out.write(f"# minutes_processed\t{len(features2)}\n")
            print("  Summary appended to part2 file.")
        else:
            write_empty_file(symbol, "2")

def main():
    # Find all symbols with both _cvd1.tmp_x and _cvd2.tmp_x
    symbols = set()
    for fname in os.listdir(SYMBOLS_DIR):
        if fname.endswith("_cvd1.tmp_x"):
            sym = fname[:-12]
            if os.path.exists(os.path.join(SYMBOLS_DIR, f"{sym}_cvd2.tmp_x")):
                symbols.add(sym)
    if not symbols:
        print("No symbols found with both _cvd1.tmp_x and _cvd2.tmp_x.")
        return
    print(f"Found {len(symbols)} symbol(s): {', '.join(sorted(symbols))}\n")
    for sym in sorted(symbols):
        process_symbol(sym)
    print("\nAll CVD files processed. Check the symbols directory for .tmp_p files.")

if __name__ == "__main__":
    main()