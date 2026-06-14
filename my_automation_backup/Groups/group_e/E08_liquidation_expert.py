#!/usr/bin/env python3
# E08_liquidation_expert.py – Liquidation High‑Probability Scenario Detector (No JSON)
# Reads raw liquidation events from X13 .tmp_x, aggregates, and outputs TSV summary.

import os
import sys
import time
import math
from collections import defaultdict, Counter

# ========================== CONFIGURATION ==========================
FEATURES_BASE_DIR = os.path.join("market_data", "binance", "symbols")
LOG_FILE = os.path.join(FEATURES_BASE_DIR, "E08_liquidation_expert.log")
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

# ========================== ORIGINAL analyze_liquidation (UNCHANGED) ==========================
def analyze_liquidation(data):
    """
    Args:
        data: dict with keys:
            - 'liq_1m': list of dicts with 'long', 'short', 'total'
            - 'liq_15m': same
            - 'liq_1h': same
            - 'heatmap': dict {price_bin: volume}
            - 'pools_high': list of (level, count)
            - 'pools_low': list of (level, count)
            - 'stop_levels': dict with 'prev_day_high', 'prev_day_low', 'prev_week_high', 'prev_week_low'
    Returns:
        dict with bias, confidence, high_prob_scenario, signals, net_score, reason.
    """
    signals = []
    bullish_score = 0
    bearish_score = 0

    # 1. Recent liquidation delta (last 5‑10 rows of 1m and 15m)
    for tf in ['1m', '15m']:
        rows = data.get(f'liq_{tf}', [])
        if not rows:
            continue
        recent = rows[-10:]
        net_long = sum(r.get('long', 0) - r.get('short', 0) for r in recent)
        total_vol = sum(r.get('total', 0) for r in recent)
        if total_vol > 0:
            net_ratio = net_long / total_vol
            if net_ratio > 0.3:
                bullish_score += 20
                signals.append(f"{tf} net long liquidation delta {net_ratio*100:.0f}% → bullish")
            elif net_ratio < -0.3:
                bearish_score += 20
                signals.append(f"{tf} net short liquidation delta {abs(net_ratio)*100:.0f}% → bearish")
        if len(rows) >= 3:
            recent_avg = sum(r['total'] for r in rows[-5:-1]) / max(1, len(rows[-5:-1]))
            latest = rows[-1]['total']
            if latest > 2 * recent_avg and recent_avg > 0:
                if rows[-1].get('long', 0) > rows[-1].get('short', 0):
                    bullish_score += 15
                    signals.append(f"{tf} spike in long liquidations → possible short squeeze")
                elif rows[-1].get('short', 0) > rows[-1].get('long', 0):
                    bearish_score += 15
                    signals.append(f"{tf} spike in short liquidations → possible long squeeze")

    # 2. Heatmap is not directly used for scoring (commented in original)
    # 3. Liquidity pools (repeated high/low levels)
    high_pools = data.get('pools_high', [])
    low_pools = data.get('pools_low', [])
    if high_pools and low_pools:
        high_count = sum(cnt for _, cnt in high_pools)
        low_count = sum(cnt for _, cnt in low_pools)
        if high_count > low_count * 1.5:
            bearish_score += 15
            signals.append("More high liquidity pools than low → overhead resistance")
        elif low_count > high_count * 1.5:
            bullish_score += 15
            signals.append("More low liquidity pools than high → strong support")

    # 4. Stop hunt levels (just note existence)
    stop_levels = data.get('stop_levels', {})
    if stop_levels.get('prev_day_high') or stop_levels.get('prev_week_high'):
        signals.append("Stop levels above present (day/week highs)")
    if stop_levels.get('prev_day_low') or stop_levels.get('prev_week_low'):
        signals.append("Stop levels below present (day/week lows)")

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

    reason = f"Net score {net:+d}, signals: {signals[0] if signals else 'no strong signals'}"
    return {
        "bias": bias,
        "confidence": confidence,
        "high_prob_scenario": high_prob_scenario,
        "probability_estimate": confidence,
        "reason": reason,
        "signals": signals,
        "net_score": net
    }

# ========================== DATA LOADING & AGGREGATION ==========================
def load_raw_liquidation_events(symbol):
    """Read raw events from X13's .tmp_x file (ts, price, usd_volume, side, type, source)."""
    path = os.path.join(FEATURES_BASE_DIR, f"{symbol.lower()}_liquidations.tmp_x")
    if not os.path.exists(path):
        log_issue("WARNING", f"Liquidation file not found: {path}")
        return None
    events = []
    try:
        with open(path, "r") as f:
            lines = f.readlines()
        if not lines:
            return None
        # Check first non-comment line
        first_data = None
        for line in lines:
            line = line.strip()
            if line and not line.startswith('#'):
                first_data = line
                break
        if not first_data:
            log_issue("INFO", "Liquidation file contains only comments – no data")
            return None
        # Determine format by first data line
        if '\t' in first_data:
            parts = first_data.split('\t')
            # Expect 6 columns: ts, price, usd_volume, side, type, source
            if len(parts) >= 4:
                # Raw format (X13 output)
                for line in lines:
                    line = line.strip()
                    if not line or line.startswith('#'):
                        continue
                    parts = line.split('\t')
                    if len(parts) < 4:
                        continue
                    try:
                        ts = int(parts[0])
                        price = float(parts[1])
                        usd_vol = float(parts[2])
                        side = parts[3]
                        ev_type = parts[4] if len(parts) > 4 else 'unknown'
                        source = parts[5] if len(parts) > 5 else 'unknown'
                        events.append({
                            'ts': ts,
                            'price': price,
                            'usd_volume': usd_vol,
                            'side': side,
                            'type': ev_type,
                            'source': source
                        })
                    except Exception:
                        continue
        else:
            # Possibly aggregated format (no tabs) – treat as no data
            log_issue("INFO", "Unrecognized liquidation file format")
            return None
    except Exception as e:
        log_issue("ERROR", f"Failed to read liquidation file: {e}")
        return None
    if not events:
        log_issue("INFO", "No liquidation events found in file")
        return None
    events.sort(key=lambda x: x['ts'])
    return events

def aggregate_buckets(events, interval_ms):
    """Return list of dicts: {ts, long, short, total, levels}"""
    buckets = defaultdict(lambda: {'long': 0.0, 'short': 0.0, 'levels': []})
    for ev in events:
        bucket = (ev['ts'] // interval_ms) * interval_ms
        if ev['side'] == 'long':
            buckets[bucket]['long'] += ev['usd_volume']
        else:
            buckets[bucket]['short'] += ev['usd_volume']
        buckets[bucket]['levels'].append((ev['price'], ev['usd_volume'], ev['side']))
    result = []
    for ts in sorted(buckets.keys()):
        long_v = buckets[ts]['long']
        short_v = buckets[ts]['short']
        total_v = long_v + short_v
        result.append({
            'ts': ts,
            'long': long_v,
            'short': short_v,
            'total': total_v,
            'levels': buckets[ts]['levels']
        })
    return result

def compute_pools_and_stop_levels(events):
    """Compute liquidity pools and stop hunt levels from events."""
    price_counts = Counter()
    volume_by_price = defaultdict(float)
    for ev in events:
        price = round(ev['price'], 2)
        price_counts[price] += 1
        volume_by_price[price] += ev['usd_volume']
    if not events:
        return [], [], {}
    prices = [ev['price'] for ev in events]
    median_price = sorted(prices)[len(prices)//2] if prices else 0
    high_pools = []
    low_pools = []
    for price, cnt in price_counts.items():
        if cnt >= 3:
            vol = volume_by_price[price]
            if price > median_price:
                high_pools.append((price, cnt, vol))
            else:
                low_pools.append((price, cnt, vol))
    high_pools.sort(key=lambda x: -x[1])
    low_pools.sort(key=lambda x: -x[1])
    high_pools = [(p, c) for p, c, _ in high_pools[:5]]
    low_pools = [(p, c) for p, c, _ in low_pools[:5]]

    now_ms = int(time.time() * 1000)
    day_ago = now_ms - 24 * 3600 * 1000
    week_ago = now_ms - 7 * 24 * 3600 * 1000
    day_prices = [ev['price'] for ev in events if ev['ts'] >= day_ago]
    week_prices = [ev['price'] for ev in events if ev['ts'] >= week_ago]
    stop_levels = {}
    if day_prices:
        stop_levels['prev_day_high'] = max(day_prices)
        stop_levels['prev_day_low'] = min(day_prices)
    if week_prices:
        stop_levels['prev_week_high'] = max(week_prices)
        stop_levels['prev_week_low'] = min(week_prices)
    return high_pools, low_pools, stop_levels

def format_density_map(heatmap):
    """Convert heatmap dict to a compact string: price:volume;price:volume"""
    if not heatmap:
        return ""
    items = sorted(heatmap.items(), key=lambda x: -x[1])[:20]  # top 20
    return ";".join(f"{price:.2f}:{vol:.2f}" for price, vol in items)

# ========================== MAIN EXPERT FUNCTION ==========================
def run_expert(symbol):
    log_issue("INFO", f"Starting E08 liquidation expert for {symbol}")
    raw_events = load_raw_liquidation_events(symbol)

    # If no events, produce a neutral output TSV
    if not raw_events:
        log_issue("INFO", "No raw liquidation events – creating neutral output")
        out_path = os.path.join(FEATURES_BASE_DIR, f"{symbol.lower()}_E08_liquidation.tsv")
        with open(out_path, "w") as f:
            header = ["timestamp", "bias", "confidence", "high_prob_scenario", "probability_estimate",
                      "reason", "signals", "net_score"]
            f.write("\t".join(header) + "\n")
            ts_now = int(time.time() * 1000)
            row = [str(ts_now), "neutral", "50", "", "50", "No liquidation events", "", "0"]
            f.write("\t".join(row) + "\n")
        log_issue("INFO", f"Saved liquidation expert summary (neutral) to {out_path}")
        return out_path

    # Aggregate into intervals
    liq_1m = aggregate_buckets(raw_events, 60000)[-120:]
    liq_15m = aggregate_buckets(raw_events, 900000)[-120:]
    liq_1h = aggregate_buckets(raw_events, 3600000)[-120:]

    def to_simple(buckets):
        return [{'long': b['long'], 'short': b['short'], 'total': b['total']} for b in buckets]

    liq_1m_simple = to_simple(liq_1m)
    liq_15m_simple = to_simple(liq_15m)
    liq_1h_simple = to_simple(liq_1h)

    # Build heatmap: price bucket -> volume (using all events)
    heatmap = defaultdict(float)
    for ev in raw_events:
        bucket = round(ev['price'] / 5) * 5
        heatmap[bucket] += ev['usd_volume']
    heatmap = dict(sorted(heatmap.items(), key=lambda x: -x[1])[:20])

    # Compute pools and stop levels
    high_pools, low_pools, stop_levels = compute_pools_and_stop_levels(raw_events)

    data = {
        'liq_1m': liq_1m_simple,
        'liq_15m': liq_15m_simple,
        'liq_1h': liq_1h_simple,
        'heatmap': heatmap,
        'pools_high': high_pools,
        'pools_low': low_pools,
        'stop_levels': stop_levels
    }

    result = analyze_liquidation(data)
    # Convert signals list to pipe‑separated string (no JSON)
    signals_str = "|".join(result['signals']) if result['signals'] else ""
    # Format density map as string (no JSON)
    density_str = format_density_map(heatmap)

    out_path = os.path.join(FEATURES_BASE_DIR, f"{symbol.lower()}_E08_liquidation.tsv")
    with open(out_path, "w") as f:
        header = ["timestamp", "bias", "confidence", "high_prob_scenario", "probability_estimate",
                  "reason", "signals", "net_score", "density_map"]
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
            str(result['net_score']),
            density_str
        ]
        f.write("\t".join(row) + "\n")
    log_issue("INFO", f"Saved liquidation expert summary to {out_path}")
    return out_path

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python E08_liquidation_expert.py SYMBOL")
        sys.exit(1)
    success = run_expert(sys.argv[1].upper())
    sys.exit(0 if success else 1)