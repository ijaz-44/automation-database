#!/usr/bin/env python3
"""
P08_session_activity.py – Process Session Data
- Reads {symbol}_sessions.tmp_x (TSV from X15)
- Computes: kill zone activity, news danger, volatility regime,
  session bias, previous session levels, session quality, time score
- Outputs TSV {symbol}_sessions.tmp_p with raw data (commented) + derived features
- Logs to p08_session_activity_issues.log (file only, minimal console)
- Input file is NOT deleted.
"""

import os
import sys
import time
import datetime
import math
from collections import defaultdict

# ========== CONFIGURATION ==========
FEATURES_BASE_DIR = os.path.join("market_data", "binance", "symbols")
LOG_FILE = os.path.join(FEATURES_BASE_DIR, "p08_session_activity_issues.log")
LOG_MAX_SIZE = 5_000_000
LAHORE_OFFSET = 5

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

def utc_to_lahore(utc_dt):
    return utc_dt + datetime.timedelta(hours=LAHORE_OFFSET)

def lahore_now():
    return utc_to_lahore(datetime.datetime.utcnow())

# ========== PARSING X15 OUTPUT (also returns raw lines) ==========
def parse_x15_tmp_x(tmp_path):
    """
    Parse the raw session TSV from X15.
    Returns (raw_lines, result_dict)
    """
    result = {
        'kill_zones': {},
        'economic_events': [],
        'volatility_profile': {},
        'candles_1h': []
    }
    raw_lines = []
    if not os.path.exists(tmp_path):
        return raw_lines, result
    with open(tmp_path, "r") as f:
        header = f.readline().strip()
        raw_lines.append(header)  # keep header
        for line in f:
            line = line.strip()
            if not line:
                continue
            raw_lines.append(line)
            parts = line.split('\t')
            if len(parts) < 3:
                continue
            data_type = parts[0]
            if data_type == 'kill_zone' and len(parts) >= 3:
                zone = parts[1]
                val = parts[2]
                if zone == 'london_start':
                    result['kill_zones'].setdefault('london', {})['start'] = val
                elif zone == 'london_end':
                    result['kill_zones'].setdefault('london', {})['end'] = val
                elif zone == 'ny_start':
                    result['kill_zones'].setdefault('newyork', {})['start'] = val
                elif zone == 'ny_end':
                    result['kill_zones'].setdefault('newyork', {})['end'] = val
            elif data_type == 'economic_event' and len(parts) >= 5:
                result['economic_events'].append({
                    'datetime_lahore': parts[1],
                    'currency': parts[2],
                    'title': parts[3],
                    'impact': parts[4]
                })
            elif data_type == 'volatility_profile' and len(parts) >= 3:
                try:
                    hour = int(parts[1])
                    vol = float(parts[2])
                    result['volatility_profile'][hour] = vol
                except:
                    pass
            elif data_type == 'candle_1h' and len(parts) >= 5:
                try:
                    result['candles_1h'].append({
                        'timestamp': int(parts[1]),
                        'high': float(parts[2]),
                        'low': float(parts[3]),
                        'close': float(parts[4])
                    })
                except:
                    pass
    return raw_lines, result

# ========== MAIN PROCESSING ==========
def process_sessions(symbol):
    print(f"[P08] Starting session processing for {symbol}")
    start_time = time.time()
    log_issue("INFO", f"Starting session processing for {symbol}")

    tmp_x_path = os.path.join(FEATURES_BASE_DIR, f"{symbol.lower()}_sessions.tmp_x")
    if not os.path.exists(tmp_x_path):
        log_issue("ERROR", f"Input file not found: {tmp_x_path}")
        return False

    # Parse raw data and get raw lines
    raw_lines, raw = parse_x15_tmp_x(tmp_x_path)
    candles = raw['candles_1h']
    if len(candles) < 8:
        log_issue("WARNING", f"Only {len(candles)} 1h candles available, need at least 8 for bias calculation")

    # Current price from latest candle
    current_price = candles[-1]['close'] if candles else 0.0
    if current_price == 0:
        log_issue("ERROR", "No valid current price found")
        return False

    current_time = lahore_now()

    # ---------- 1. Kill zone activity ----------
    kill_zones = raw['kill_zones']
    kill_zone_active = 'none'
    seconds_to_london = 0
    seconds_to_ny = 0

    def parse_iso(ts_str):
        try:
            return datetime.datetime.fromisoformat(ts_str.replace('Z', '+00:00'))
        except:
            return None

    london_start = parse_iso(kill_zones.get('london', {}).get('start', ''))
    london_end = parse_iso(kill_zones.get('london', {}).get('end', ''))
    ny_start = parse_iso(kill_zones.get('newyork', {}).get('start', ''))
    ny_end = parse_iso(kill_zones.get('newyork', {}).get('end', ''))

    if london_start and london_end:
        if london_start <= current_time <= london_end:
            kill_zone_active = 'london'
        else:
            if current_time < london_start:
                seconds_to_london = int((london_start - current_time).total_seconds())
            else:
                next_start = london_start + datetime.timedelta(days=1)
                seconds_to_london = int((next_start - current_time).total_seconds())

    if ny_start and ny_end:
        if ny_start <= current_time <= ny_end:
            kill_zone_active = 'newyork'
        else:
            if current_time < ny_start:
                seconds_to_ny = int((ny_start - current_time).total_seconds())
            else:
                next_start = ny_start + datetime.timedelta(days=1)
                seconds_to_ny = int((next_start - current_time).total_seconds())

    # ---------- 2. News danger zone ----------
    events = raw['economic_events']
    news_danger = False
    seconds_to_next_event = 0
    now_ts = current_time.timestamp()
    for ev in events:
        dt_str = ev.get('datetime_lahore', '')
        try:
            ev_time = datetime.datetime.fromisoformat(dt_str.replace('Z', '+00:00'))
            secs = ev_time.timestamp() - now_ts
            if secs > 0:
                seconds_to_next_event = int(secs)
                if secs < 900:
                    news_danger = True
                break
        except:
            continue

    # ---------- 3. Session bias ----------
    if len(candles) >= 8:
        last_8_high = max(c['high'] for c in candles[-8:])
        last_8_low = min(c['low'] for c in candles[-8:])
        if current_price > last_8_high:
            session_bias_score = 1
            bias_str = "Strong_Bullish"
        elif current_price < last_8_low:
            session_bias_score = -1
            bias_str = "Strong_Bearish"
        else:
            session_bias_score = 0
            bias_str = "Neutral"
    else:
        session_bias_score = 0
        bias_str = "Neutral"

    # ---------- 4. Previous session high/low ----------
    if len(candles) >= 24:
        prev_high = max(c['high'] for c in candles[-24:])
        prev_low = min(c['low'] for c in candles[-24:])
    elif len(candles) >= 8:
        prev_high = max(c['high'] for c in candles[-8:])
        prev_low = min(c['low'] for c in candles[-8:])
    else:
        prev_high = prev_low = 0.0

    prev_high_dist = ((current_price - prev_high) / current_price * 100) if prev_high > 0 else 0.0
    prev_low_dist = ((current_price - prev_low) / current_price * 100) if prev_low > 0 else 0.0

    # ---------- 5. Volatility profile ----------
    vol_profile = raw['volatility_profile']
    current_hour = current_time.hour
    current_hour_vol = vol_profile.get(current_hour, 0.25)
    is_high_vol_hour = current_hour_vol > 0.5

    # ---------- 6. Session quality score ----------
    hour = current_hour
    if 18 <= hour <= 21:
        quality = 100
    elif 12 <= hour <= 17:
        quality = 80
    elif 22 <= hour <= 23 or 0 <= hour <= 1:
        quality = 60
    else:
        quality = 40
    if kill_zone_active != 'none':
        quality = min(100, quality + 20)
    if news_danger:
        quality = max(10, quality - 30)

    # 7. Time of day score
    if 17 <= hour <= 21:
        time_score = 100
    elif 22 <= hour <= 23 or 0 <= hour <= 1:
        time_score = 70
    elif 2 <= hour <= 9:
        time_score = 40
    else:
        time_score = 60

    # ---------- Write output .tmp_p TSV (raw data + derived) ----------
    tmp_p_path = os.path.join(FEATURES_BASE_DIR, f"{symbol.lower()}_sessions.tmp_p")
    with open(tmp_p_path, "w") as out:
        # Write raw data as comments
        out.write("# === Raw session data ===\n")
        for raw_line in raw_lines:
            out.write("# " + raw_line + "\n")
        out.write("# === Derived features ===\n")
        header = [
            "timestamp", "kill_zone_active", "seconds_to_london", "seconds_to_ny",
            "news_danger_zone", "seconds_to_next_event", "session_bias_score",
            "session_bias_str", "prev_high_distance_pct", "prev_low_distance_pct",
            "current_hour_volatility", "is_high_vol_hour", "session_quality_score", "time_of_day_score"
        ]
        out.write("\t".join(header) + "\n")
        row = [
            str(int(current_time.timestamp() * 1000)),
            kill_zone_active,
            str(seconds_to_london),
            str(seconds_to_ny),
            "1" if news_danger else "0",
            str(seconds_to_next_event),
            str(session_bias_score),
            bias_str,
            f"{prev_high_dist:.2f}",
            f"{prev_low_dist:.2f}",
            f"{current_hour_vol:.4f}",
            "1" if is_high_vol_hour else "0",
            str(quality),
            str(time_score)
        ]
        out.write("\t".join(row) + "\n")

    # Input file is NOT deleted
    # if os.path.exists(tmp_x_path):
    #     os.remove(tmp_x_path)

    elapsed = time.time() - start_time
    print(f"[P08] Success ({elapsed:.1f}s) -> {os.path.basename(tmp_p_path)}")
    log_issue("INFO", f"Processing complete for {symbol} in {elapsed:.2f}s -> {tmp_p_path}")
    return True

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python P08_session_activity.py SYMBOL")
        sys.exit(1)
    success = process_sessions(sys.argv[1].upper())
    sys.exit(0 if success else 1)