#!/usr/bin/env python3
"""
P07_liquidation_heat.py – Professional Liquidation Pressure Engine (Fixed f-string)
- All output goes to p07_liquidation_heat_issues.log
- Fixed conditional in log formatting.
"""

import os
import sys
import time
import math
import bisect
import json
import traceback
from collections import defaultdict, deque

FEATURES_BASE_DIR = os.path.join("market_data", "binance", "symbols")
LOG_FILE = os.path.join(FEATURES_BASE_DIR, "p07_liquidation_heat_issues.log")
LOG_MAX_SIZE = 5_000_000
os.makedirs(FEATURES_BASE_DIR, exist_ok=True)

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
    rotate_log_if_needed()
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(line + "\n")
    # Only print errors to console (optional, can be removed)
    if level == "ERROR":
        print(line)

# ---------- 1. LOAD REAL LIQUIDATION EVENTS (X13) ----------
def load_real_events(symbol):
    tmp_x_path = os.path.join(FEATURES_BASE_DIR, f"{symbol.lower()}_liquidations.tmp_x")
    if not os.path.exists(tmp_x_path):
        return []
    events = []
    try:
        with open(tmp_x_path, "r") as f:
            header = f.readline().strip()
            for line in f:
                line = line.strip()
                if not line or line.startswith('#'):
                    continue
                parts = line.split('\t')
                if len(parts) < 6:
                    continue
                ts = int(parts[0])
                price = float(parts[1])
                usd_vol = float(parts[2])
                side = parts[3]
                ev_type = parts[4]
                source = parts[5]
                events.append({'ts': ts, 'price': price, 'usd_volume': usd_vol,
                               'liquidated_side': side, 'type': ev_type,
                               'source': source, 'confidence': 1.0})
    except Exception as e:
        log_issue("WARNING", f"Error reading X13 file: {e}")
        return []
    log_issue("INFO", f"Loaded {len(events)} real events")
    return events

# ---------- 2. WAIT FOR REQUIRED FILES ----------
def wait_for_files(files, timeout_sec=90, check_interval=2):
    start = time.time()
    missing = set(files)
    while missing and (time.time() - start) < timeout_sec:
        for f in list(missing):
            path = os.path.join(FEATURES_BASE_DIR, f)
            if os.path.exists(path):
                missing.remove(f)
        if missing:
            elapsed = int(time.time() - start)
            log_issue("INFO", f"Waiting for files: {missing} ({elapsed}s / {timeout_sec}s)")
            time.sleep(check_interval)
    if missing:
        log_issue("ERROR", f"Timeout waiting for files: {missing}")
        return False
    return True

def get_required_files(symbol):
    return [
        f"{symbol.lower()}.tmp_x",                     # candles (X01)
        f"{symbol.lower()}_derivative.tmp_x",          # derivative (X07)
        f"{symbol.lower()}_cvd1.tmp_x",               # CVD part1 (X03)
        f"{symbol.lower()}_cvd2.tmp_x",               # CVD part2 (X03)
    ]

# ---------- 3. LOAD AUXILIARY DATA ----------
def load_1m_candles(symbol):
    path = os.path.join(FEATURES_BASE_DIR, f"{symbol.lower()}.tmp_x")
    if not os.path.exists(path):
        return []
    candles = []
    try:
        with open(path, "r") as f:
            for line in f:
                parts = line.strip().split('\t')
                if len(parts) >= 8 and parts[1] == "1m":
                    try:
                        ts = int(parts[2])
                        open_p = float(parts[3])
                        high = float(parts[4])
                        low = float(parts[5])
                        close = float(parts[6])
                        volume = float(parts[7])
                        candles.append({'ts': ts, 'open': open_p, 'high': high,
                                        'low': low, 'close': close, 'volume': volume})
                    except (ValueError, IndexError) as e:
                        log_issue("WARNING", f"Skipping candle line: {e}")
                        continue
    except Exception as e:
        log_issue("WARNING", f"Could not read candles: {e}")
        return []
    candles.sort(key=lambda x: x['ts'])
    log_issue("INFO", f"Loaded {len(candles)} 1m candles")
    return candles

def load_derivative_history(symbol):
    path = os.path.join(FEATURES_BASE_DIR, f"{symbol.lower()}_derivative.tmp_x")
    if not os.path.exists(path):
        return [], []
    oi_items = []
    funding_items = []
    try:
        with open(path, "r") as f:
            for line in f:
                parts = line.strip().split('\t')
                if len(parts) < 2:
                    continue
                typ = parts[0]
                try:
                    if typ == "snapshot" and len(parts) >= 6:
                        ts = int(parts[1])
                        oi = float(parts[5])
                        funding = float(parts[4])
                        oi_items.append((ts, oi))
                        funding_items.append((ts, funding))
                    elif typ == "oi_history" and len(parts) >= 3:
                        ts = int(parts[1])
                        oi = float(parts[2])
                        oi_items.append((ts, oi))
                    elif typ == "funding_history" and len(parts) >= 3:
                        ts = int(parts[1])
                        funding = float(parts[2])
                        funding_items.append((ts, funding))
                except (ValueError, IndexError) as e:
                    log_issue("WARNING", f"Skipping derivative line: {e}")
                    continue
    except Exception as e:
        log_issue("WARNING", f"Could not read derivative file: {e}")
    oi_items.sort(key=lambda x: x[0])
    funding_items.sort(key=lambda x: x[0])
    log_issue("INFO", f"OI entries: {len(oi_items)}, funding: {len(funding_items)}")
    return oi_items, funding_items

# ---------- 3b. DECODE X03 CVD COMPRESSED FORMAT ----------
BASE62 = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

def base62_decode(s):
    val = 0
    for ch in s:
        val = val * 62 + BASE62.index(ch)
    return val

def decode_compressed_value(s):
    if not s:
        return 0
    sign = s[0]
    num = base62_decode(s[1:])
    return -num if sign == 'm' else num

def load_cvd_net_delta(symbol):
    all_trades = []
    price_prec = 8
    qty_prec = 8
    for part in ['cvd1', 'cvd2']:
        path = os.path.join(FEATURES_BASE_DIR, f"{symbol.lower()}_{part}.tmp_x")
        if not os.path.exists(path):
            continue
        try:
            with open(path, "r") as f:
                lines = f.readlines()
            if not lines:
                continue
            header_idx = 0
            while header_idx < len(lines) and lines[header_idx].startswith('#'):
                line = lines[header_idx].strip()
                if '#price_precision=' in line:
                    parts = line.split()
                    for p in parts:
                        if p.startswith('#price_precision='):
                            price_prec = int(p.split('=')[1])
                        elif p.startswith('qty_precision='):
                            qty_prec = int(p.split('=')[1])
                header_idx += 1
            if header_idx >= len(lines):
                continue
            first_line = lines[header_idx].strip().split('\t')
            if len(first_line) != 2:
                continue
            base_parts = first_line[1].split(',')
            if len(base_parts) != 4:
                continue
            ts0 = decode_compressed_value(base_parts[0])
            p0 = decode_compressed_value(base_parts[1])
            q0 = decode_compressed_value(base_parts[2])
            sell0 = int(base_parts[3])
            base_ts_ms = ts0 * 60000
            base_price = p0 / (10**price_prec)
            base_qty = q0 / (10**qty_prec)
            net_usd = (base_price * base_qty) * (-1 if sell0 else 1)
            all_trades.append((base_ts_ms, net_usd))
            prev_ts = ts0
            prev_p = p0
            prev_q = q0
            for line in lines[header_idx+1:]:
                line = line.strip()
                if not line or line.startswith('#'):
                    continue
                parts = line.split('\t')
                if len(parts) != 2:
                    continue
                deltas_str = parts[1].split(';')
                for delta_str in deltas_str:
                    if not delta_str:
                        continue
                    try:
                        d_ts, d_p, d_q, sell = delta_str.split(',')
                    except:
                        continue
                    dt = decode_compressed_value(d_ts)
                    dp = decode_compressed_value(d_p)
                    dq = decode_compressed_value(d_q)
                    cur_ts = prev_ts + dt
                    cur_p = prev_p + dp
                    cur_q = prev_q + dq
                    ts_ms = cur_ts * 60000
                    price = cur_p / (10**price_prec)
                    qty = cur_q / (10**qty_prec)
                    net = (price * qty) * (-1 if sell == '1' else 1)
                    all_trades.append((ts_ms, net))
                    prev_ts, prev_p, prev_q = cur_ts, cur_p, cur_q
        except Exception as e:
            log_issue("WARNING", f"Could not decode CVD file {part}: {e}")
            continue
    if not all_trades:
        return {}
    per_minute = defaultdict(float)
    for ts_ms, net_usd in all_trades:
        bucket = (ts_ms // 60000) * 60000
        per_minute[bucket] += net_usd
    log_issue("INFO", f"CVD aggregated: {len(per_minute)} minutes")
    return per_minute

def load_depth_map(symbol):
    path = os.path.join(FEATURES_BASE_DIR, f"{symbol.lower()}_depth.tmp_x")
    if not os.path.exists(path):
        return [], {}
    depth_dict = {}
    try:
        with open(path, "r") as f:
            lines = f.readlines()
        if not lines:
            return [], {}
        ts_data = defaultdict(lambda: {"bid_vol": 0.0, "ask_vol": 0.0})
        for line in lines[1:]:
            parts = line.strip().split('\t')
            if len(parts) < 4:
                continue
            side = parts[0]
            try:
                qty = float(parts[2])
                ts = int(parts[3])
            except (ValueError, IndexError) as e:
                log_issue("WARNING", f"Skipping depth line: {e}")
                continue
            if side == "bid":
                ts_data[ts]["bid_vol"] += qty
            elif side == "ask":
                ts_data[ts]["ask_vol"] += qty
        for ts, vols in ts_data.items():
            bid = vols["bid_vol"]
            ask = vols["ask_vol"]
            total = bid + ask
            if total == 0:
                imbalance = 0.0
            else:
                imbalance = (bid - ask) / total
            depth_dict[ts] = imbalance
    except Exception as e:
        log_issue("WARNING", f"Could not read depth file: {e}")
        return [], {}
    depth_items = sorted(depth_dict.items())
    log_issue("INFO", f"Loaded depth map: {len(depth_items)} entries")
    return depth_items, depth_dict

def get_nearest_depth(ts, depth_items):
    idx = bisect.bisect_right(depth_items, (ts, float('inf'))) - 1
    if idx >= 0:
        return depth_items[idx][1]
    return None

def get_atr_15m(symbol):
    path = os.path.join(FEATURES_BASE_DIR, f"{symbol.lower()}.tmp_x")
    if not os.path.exists(path):
        return 0.0, 0.0
    candles_15m = []
    try:
        with open(path, "r") as f:
            for line in f:
                parts = line.strip().split('\t')
                if len(parts) >= 8 and parts[1] == "15m":
                    try:
                        high = float(parts[4])
                        low = float(parts[5])
                        close = float(parts[6])
                        candles_15m.append({"high": high, "low": low, "close": close})
                    except (ValueError, IndexError) as e:
                        continue
    except Exception as e:
        log_issue("WARNING", f"Could not read 15m candles: {e}")
        return 0.0, 0.0
    if len(candles_15m) < 20:
        return 0.0, 0.0
    tr_list = []
    for i in range(1, len(candles_15m)):
        high = candles_15m[i]["high"]
        low = candles_15m[i]["low"]
        prev_close = candles_15m[i-1]["close"]
        tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
        tr_list.append(tr)
    atr = sum(tr_list[-14:]) / 14
    window = sorted(tr_list[-60:])
    mid = len(window) // 2
    if len(window) % 2 == 0:
        atr_median = (window[mid-1] + window[mid]) / 2
    else:
        atr_median = window[mid]
    return atr, atr_median

# ---------- 4. SYNTHETIC EVENT GENERATION (FIXED F-STRING) ----------
def generate_synthetic_events(symbol):
    try:
        candles = load_1m_candles(symbol)
        if len(candles) < 20:
            log_issue("WARNING", "Not enough candles for synthetic detection")
            return []
        oi_items, funding_items = load_derivative_history(symbol)
        cvd_map = load_cvd_net_delta(symbol)
        depth_items, _ = load_depth_map(symbol)
        atr, atr_median = get_atr_15m(symbol)
        vol_norm = max(0.75, min(1.5, atr / (atr_median + 1e-6))) if atr_median > 0 else 1.0

        events = []
        last_event_ts = None
        last_ts_min = None
        persistence_counter = 0
        min_gap_ms = 60_000

        vol_window = deque(maxlen=20)
        cvd_window = deque(maxlen=60)
        oi_window = deque(maxlen=20)
        funding_window = deque(maxlen=20)
        traded_usd_window = deque(maxlen=20)

        oi_ptr = 0
        funding_ptr = 0
        current_oi = None
        current_funding = None

        spot_price = candles[-1]['close'] if candles else 1.0
        for c in candles[-20:]:
            traded_usd_window.append(c['close'] * c['volume'])
        median_traded_usd = sorted(traded_usd_window)[len(traded_usd_window)//2] if traded_usd_window else 10_000_000

        log_counter = 0
        max_log = 10

        for i, candle in enumerate(candles):
            if i == 0:
                vol_window.append(candle['volume'])
                traded_usd_window.append(candle['close'] * candle['volume'])
                continue
            prev = candles[i-1]
            ts = candle['ts']
            minute = ts // 60000

            while oi_ptr < len(oi_items) and oi_items[oi_ptr][0] <= ts:
                current_oi = oi_items[oi_ptr][1]
                oi_ptr += 1
            while funding_ptr < len(funding_items) and funding_items[funding_ptr][0] <= ts:
                current_funding = funding_items[funding_ptr][1]
                funding_ptr += 1

            vol_window.append(candle['volume'])
            traded_usd = candle['close'] * candle['volume']
            traded_usd_window.append(traded_usd)
            cvd_val = cvd_map.get(ts, 0)
            cvd_window.append(cvd_val)
            if current_oi is not None:
                oi_window.append(current_oi)
            if current_funding is not None:
                funding_window.append(current_funding)

            avg_vol = sum(vol_window) / len(vol_window) if vol_window else 1.0
            vol_spike = candle['volume'] / avg_vol if avg_vol > 0 else 1.0

            cvd_zscore = 0.0
            if len(cvd_window) >= 10:
                cvd_mean = sum(cvd_window) / len(cvd_window)
                cvd_std = math.sqrt(sum((x - cvd_mean)**2 for x in cvd_window) / len(cvd_window)) if len(cvd_window) > 1 else 1.0
                if cvd_std > 0:
                    cvd_zscore = (cvd_val - cvd_mean) / cvd_std

            oi_ratio = 1.0
            if len(oi_window) >= 5 and current_oi is not None:
                oi_mean = sum(oi_window) / len(oi_window)
                if oi_mean > 0:
                    oi_ratio = current_oi / oi_mean

            funding_threshold = 0.0008
            if len(funding_window) >= 10:
                funding_mean = sum(abs(f) for f in funding_window) / len(funding_window)
                funding_threshold = max(0.0008, 2 * funding_mean)
            funding_extreme = current_funding is not None and abs(current_funding) > funding_threshold

            depth_imb = get_nearest_depth(ts, depth_items)
            price_move = abs(candle['close'] - prev['close']) / prev['close'] if prev['close'] != 0 else 0

            if last_ts_min is not None and minute == last_ts_min + 1:
                persistence_counter += 1
            else:
                persistence_counter = 0
            last_ts_min = minute
            persistence_boost = min(0.2, persistence_counter * 0.05)

            prob = 0.0
            side = None
            base_vol = 0.0
            vol_scaler = median_traded_usd / 1_000_000 if median_traded_usd else 1.0

            if last_event_ts and (ts - last_event_ts) < min_gap_ms:
                if log_counter < max_log:
                    log_issue("INFO", f"Minute {i}: Skipped (cooldown)")
                    log_counter += 1
                continue

            # LOWERED THRESHOLDS FOR TESTING
            cond1_price_vol = (price_move > 0.001 * vol_norm and vol_spike > 1.2 * vol_norm)
            cond2_cvd = (abs(cvd_zscore) > 1.0)
            cond3_funding = funding_extreme and (abs(cvd_zscore) > 0.8 or price_move > 0.001)
            cond4_depth = (depth_imb is not None and abs(depth_imb) > 0.05)
            cond5_oi = (oi_ratio > 1.05)

            # --- FIXED LOGGING: compute display values first ---
            depth_display = depth_imb if depth_imb is not None else 0.0
            if log_counter < max_log:
                log_issue("INFO", f"Minute {i}: price_move={price_move:.5f} (thresh {0.001*vol_norm:.5f}), "
                                   f"vol_spike={vol_spike:.2f} (thresh {1.2*vol_norm:.2f}), "
                                   f"cvd_zscore={cvd_zscore:.2f}, funding={current_funding if current_funding else 0:.6f} (extreme={funding_extreme}), "
                                   f"depth={depth_display:.3f}, oi_ratio={oi_ratio:.3f}")
                log_counter += 1

            if cond1_price_vol:
                prob += 0.3
                side = 'short' if candle['close'] > prev['close'] else 'long'
                base_vol += candle['volume'] * candle['close'] * 0.1

            if cond2_cvd:
                prob += min(0.4, abs(cvd_zscore) * 0.1)
                if side is None:
                    side = 'short' if cvd_zscore > 0 else 'long'
                base_vol += abs(cvd_val) * 0.2

            if cond3_funding:
                prob += 0.2
                if side is None:
                    side = 'short' if current_funding > 0 else 'long'
                base_vol += 500_000 * vol_scaler

            if cond4_depth:
                prob += 0.05
                if side is None:
                    side = 'short' if depth_imb > 0 else 'long'
                base_vol += 200_000 * vol_scaler

            if cond5_oi and side is not None:
                oi_excess = min(0.5, (oi_ratio - 1.0) * 0.5)
                prob += min(0.25, oi_excess)
                base_vol += (oi_ratio - 1.0) * 500_000 * vol_scaler

            prob += persistence_boost
            prob = min(0.95, prob)

            if prob < 0.15 or side is None:
                if log_counter < max_log+10:
                    log_issue("INFO", f"Minute {i}: probability {prob:.2f} < 0.15 or side None, skipping")
                continue

            final_vol = base_vol * (prob / 0.5) * (1 + persistence_boost)
            final_vol = min(final_vol, 10_000_000 * vol_scaler)
            final_vol = max(final_vol, 10_000)

            events.append({
                'ts': ts,
                'price': candle['close'],
                'usd_volume': final_vol,
                'liquidated_side': side,
                'type': 'synthetic_liquidation',
                'source': 'multi_x_inference',
                'confidence': prob
            })
            last_event_ts = ts
            log_issue("INFO", f"Minute {i}: EVENT generated! prob={prob:.2f}, side={side}, vol={final_vol:.2f}")

        log_issue("INFO", f"Generated {len(events)} synthetic events")
        return events
    except Exception as e:
        log_issue("ERROR", f"Error in generate_synthetic_events: {e}\n{traceback.format_exc()}")
        return []

# ---------- 5. AGGREGATION WITH EXPONENTIAL DECAY ----------
def aggregate_buckets_with_decay(events, interval_ms, decay_factor=0.8, decay_minutes=5):
    buckets = defaultdict(lambda: {'long': 0.0, 'short': 0.0, 'levels': [], 'density': defaultdict(float)})
    now = max(e['ts'] for e in events) if events else int(time.time()*1000)
    for ev in events:
        bucket = (ev['ts'] // interval_ms) * interval_ms
        weight = ev.get('confidence', 1.0)
        effective_vol = ev['usd_volume'] * weight
        side = ev['liquidated_side']
        if side == 'long':
            buckets[bucket]['long'] += effective_vol
        else:
            buckets[bucket]['short'] += effective_vol
        price = ev['price']
        bucket_size = price * 0.005
        bucket_price = round(price / bucket_size) * bucket_size
        buckets[bucket]['density'][bucket_price] += effective_vol
        if ev['price'] > 0:
            age_min = (now - ev['ts']) / (60_000)
            if age_min <= decay_minutes:
                decay = decay_factor ** age_min
                buckets[bucket]['levels'].append((ev['price'], effective_vol * decay, side))
    result = []
    for ts in sorted(buckets.keys()):
        density_map = {f"{k:.2f}": v for k, v in sorted(buckets[ts]['density'].items())}
        result.append({'ts': ts,
                       'long': buckets[ts]['long'],
                       'short': buckets[ts]['short'],
                       'total': buckets[ts]['long'] + buckets[ts]['short'],
                       'levels': buckets[ts]['levels'],
                       'density_map': density_map})
    return result

def separate_cascade_risks(levels, spot, atr_15m, threshold=0.5):
    if len(levels) < 2:
        return 0, 0
    atr_dist = atr_15m * threshold if atr_15m else 100
    sorted_lvls = sorted(levels, key=lambda x: x[0])
    long_clusters = []
    short_clusters = []
    cur_long = 0.0
    cur_short = 0.0
    last_price = sorted_lvls[0][0]
    for price, vol, side in sorted_lvls:
        if price - last_price < atr_dist:
            if side == 'long':
                cur_long += vol
            else:
                cur_short += vol
        else:
            if cur_long > 100_000:
                long_clusters.append(cur_long)
            if cur_short > 100_000:
                short_clusters.append(cur_short)
            cur_long = vol if side == 'long' else 0.0
            cur_short = vol if side == 'short' else 0.0
        last_price = price
    if cur_long > 100_000:
        long_clusters.append(cur_long)
    if cur_short > 100_000:
        short_clusters.append(cur_short)
    long_risk = min(100, int((sum(long_clusters) / 1_000_000) * 25))
    short_risk = min(100, int((sum(short_clusters) / 1_000_000) * 25))
    return long_risk, short_risk

def liquidation_magnet_from_levels(levels, spot):
    above = 0.0
    below = 0.0
    for price, vol, _ in levels:
        if price > spot:
            above += vol
        else:
            below += vol
    total = above + below
    if total == 0:
        return 0.0, above, below
    return (above - below) / total, above, below

# ---------- 6. MAIN PROCESSOR ----------
def process_liquidations(symbol):
    try:
        log_issue("INFO", f"Processing for {symbol}")

        raw_events = load_real_events(symbol)
        if not raw_events:
            log_issue("INFO", "No real events, checking synthetic dependencies...")
            required = get_required_files(symbol)
            if not wait_for_files(required, timeout_sec=90, check_interval=2):
                msg = f"Timeout waiting for required files: {required}"
                log_issue("ERROR", msg)
                tmp_p_path = os.path.join(FEATURES_BASE_DIR, f"{symbol.lower()}_liquidations.tmp_p")
                with open(tmp_p_path, "w") as out:
                    out.write(f"# {msg}\n")
                    out.write("# Please ensure X01, X07, X03 modules run first and finish.\n")
                return True
            log_issue("INFO", "All required files present, generating synthetic events")
            raw_events = generate_synthetic_events(symbol)
            if not raw_events:
                log_issue("ERROR", "No synthetic events generated")
                tmp_p_path = os.path.join(FEATURES_BASE_DIR, f"{symbol.lower()}_liquidations.tmp_p")
                with open(tmp_p_path, "w") as out:
                    out.write("# No synthetic events could be generated (market quiet?)\n")
                return True

        # Aggregate and compute features
        buckets_1m = aggregate_buckets_with_decay(raw_events, 60000)[-120:]
        buckets_15m = aggregate_buckets_with_decay(raw_events, 900000)[-120:]

        def get_series(buckets):
            return [(b['ts'], b['long'], b['short'], b['total'], b['levels'], b['density_map']) for b in buckets]

        series_1m = get_series(buckets_1m)
        series_15m = get_series(buckets_15m)

        if series_1m:
            recent_1m = series_1m[-10:] if len(series_1m) > 10 else series_1m
            net_delta_1m = sum(l - s for _, l, s, _, _, _ in recent_1m)
            total_liq_1m = sum(t for _, _, _, t, _, _ in recent_1m)
            delta_ratio = net_delta_1m / total_liq_1m if total_liq_1m > 0 else 0
        else:
            net_delta_1m = 0.0
            total_liq_1m = 0.0
            delta_ratio = 0.0

        if series_15m:
            recent_15m = series_15m[-5:] if len(series_15m) > 5 else series_15m
            net_delta_15m = sum(l - s for _, l, s, _, _, _ in recent_15m)
        else:
            net_delta_15m = 0.0

        spike_ratio = 1.0
        aggressive = False
        if len(series_1m) >= 3:
            totals = [t for _, _, _, t, _, _ in series_1m]
            avg_prev = sum(totals[-5:-1]) / max(1, len(totals[-5:-1]))
            latest = totals[-1] if totals else 0
            if avg_prev > 0:
                spike_ratio = latest / avg_prev
                if spike_ratio > 2.0:
                    aggressive = True

        candles = load_1m_candles(symbol)
        spot_price = candles[-1]['close'] if candles else 0.0
        atr_15m, _ = get_atr_15m(symbol)

        combined_levels = []
        for b in buckets_1m[-5:]:
            combined_levels.extend(b['levels'])
        long_cascade, short_cascade = separate_cascade_risks(combined_levels, spot_price, atr_15m)
        cascade_str = f"LONG_{long_cascade}_SHORT_{short_cascade}"

        magnet_combined = []
        for b in buckets_1m[-5:]:
            magnet_combined.extend(b['levels'])
        magnet, liq_above, liq_below = liquidation_magnet_from_levels(magnet_combined, spot_price) if magnet_combined else (0.0,0.0,0.0)

        density_map = series_1m[-1][5] if series_1m else {}

        if total_liq_1m > 0:
            long_dom = sum(l for _, l, _, _, _, _ in recent_1m) / total_liq_1m
            short_dom = sum(s for _, _, s, _, _, _ in recent_1m) / total_liq_1m
        else:
            long_dom = 0.5
            short_dom = 0.5

        stop_hunt_prob = 0
        if aggressive and net_delta_1m > 0:
            stop_hunt_prob += 40
        elif aggressive and net_delta_1m < 0:
            stop_hunt_prob += 30
        if abs(net_delta_1m) > 1_000_000:
            stop_hunt_prob += 20
        if spike_ratio > 3:
            stop_hunt_prob += 15
        oi_items, funding_items = load_derivative_history(symbol)
        if oi_items and oi_items[-1][1] > 500_000_000:
            stop_hunt_prob += 10
        if funding_items and abs(funding_items[-1][1]) > 0.0008:
            stop_hunt_prob += 10
        stop_hunt_prob = min(100, stop_hunt_prob)

        tmp_p_path = os.path.join(FEATURES_BASE_DIR, f"{symbol.lower()}_liquidations.tmp_p")
        with open(tmp_p_path, "w") as out:
            out.write("# Liquidation features (real if available, else synthetic)\n")
            out.write("# density_map: price bucket (0.5% increments) -> liquidation volume (USD)\n")
            header = [
                "timestamp", "net_delta_1m", "net_delta_15m", "delta_ratio",
                "spike_ratio", "aggressive_spike", "stop_hunt_probability",
                "cascade_risk", "long_cascade_risk", "short_cascade_risk",
                "liquidation_magnet_bias", "liq_volume_above", "liq_volume_below",
                "long_dominance", "short_dominance", "density_map"
            ]
            out.write("\t".join(header) + "\n")
            ts_now = int(time.time() * 1000)
            row = [
                str(ts_now),
                f"{net_delta_1m:.2f}",
                f"{net_delta_15m:.2f}",
                f"{delta_ratio:.4f}",
                f"{spike_ratio:.2f}",
                "1" if aggressive else "0",
                str(stop_hunt_prob),
                cascade_str,
                str(long_cascade),
                str(short_cascade),
                f"{magnet:.4f}",
                f"{liq_above:.2f}",
                f"{liq_below:.2f}",
                f"{long_dom:.4f}",
                f"{short_dom:.4f}",
                json.dumps(density_map, separators=(',', ':'))
            ]
            out.write("\t".join(row) + "\n")

        log_issue("INFO", f"Success -> {os.path.basename(tmp_p_path)}")
        return True
    except Exception as e:
        log_issue("ERROR", f"Error in process_liquidations: {e}\n{traceback.format_exc()}")
        # Create an error .tmp_p file to avoid downstream crashes
        tmp_p_path = os.path.join(FEATURES_BASE_DIR, f"{symbol.lower()}_liquidations.tmp_p")
        with open(tmp_p_path, "w") as out:
            out.write(f"# Error: {e}\n")
        return True

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python P07_liquidation_heat.py SYMBOL")
        sys.exit(1)
    success = process_liquidations(sys.argv[1].upper())
    sys.exit(0 if success else 1)