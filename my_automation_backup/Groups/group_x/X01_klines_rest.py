#!/usr/bin/env python3
"""
X01_klines_rest.py – Raw Candle Fetcher + E01 Expert + Aggressive Event Compression
- Fetches candles (limits: 1m:200,5m:180,15m:120,1h:120,4h:80)
- Writes raw candles to temporary TSV
- Calls E01 to get raw rows (limited) and events (identical merged)
- Applies second‑level compression: merges consecutive events with same direction
- Writes final TSV order: RAW rows → HIGH_PROB_SCENARIO → EVENT rows
- Logs to market_data/binance/symbols/candle_issues.log
- Deletes temporary raw TSV
"""

import os
import sys
import time
import requests
import random
import json
import shutil
from typing import List, Dict, Tuple

# ========== Import E01 from group_e ==========
try:
    from Groups.group_e.E01_candles_expert import (
        load_and_prepare,
        compute_all_patterns_and_events,
        compute_high_prob_scenario
    )
    E01_AVAILABLE = True
except ImportError:
    E01_AVAILABLE = False
    print("[X01] ERROR: E01_candles_expert not found in Groups/group_e/. Final TSV will not be generated.")

# ========== CONFIGURATION ==========
TIMEFRAME_SECONDS = {"1m":60,"5m":300,"15m":900,"1h":3600,"4h":14400}
LIMITS = {
    "1m": 200,
    "5m": 180,
    "15m": 120,
    "1h": 120,
    "4h": 80
}
MIN_REQUIRED_CANDLES = {
    "1m": 180,
    "5m": 160,
    "15m": 100,
    "1h": 100,
    "4h": 70
}
MAX_RETRIES = 3
REQUEST_TIMEOUT = 10
RATE_PER_SEC = 10
RATE_BURST = 5
FEATURES_BASE_DIR = os.path.join("market_data", "binance", "symbols")
LOG_FILE = os.path.join(FEATURES_BASE_DIR, "candle_issues.log")
LOG_MAX_SIZE = 5_000_000
TIMEFRAME_FETCH_TIMEOUT = 25

os.makedirs(FEATURES_BASE_DIR, exist_ok=True)

# ========== SYMBOL VALIDATION ==========
def is_valid_symbol(symbol: str) -> bool:
    return bool(symbol) and symbol.isalnum() and len(symbol) <= 15

# ========== DISK SPACE CHECK ==========
def check_disk_space():
    try:
        total, used, free = shutil.disk_usage(".")
        if free < 50 * 1024 * 1024:
            raise Exception("Low disk space: less than 50 MB free")
        if not os.access(FEATURES_BASE_DIR, os.W_OK):
            raise Exception(f"No write permission in {FEATURES_BASE_DIR}")
    except Exception as e:
        log_issue("ERROR", "Pre‑flight check failed", error=str(e))
        raise

# ========== LOGGER ==========
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
        line += " " + json.dumps(kwargs)
    print(line)
    rotate_log_if_needed()
    with open(LOG_FILE, "a") as f:
        f.write(line + "\n")

# ========== RATE LIMITER ==========
class RateLimiter:
    def __init__(self, rate, burst):
        self.rate = rate
        self.cap = rate + burst
        self.tokens = self.cap
        self.last = time.monotonic()
    def wait(self):
        now = time.monotonic()
        self.tokens = min(self.cap, self.tokens + (now - self.last) * self.rate)
        self.last = now
        if self.tokens < 1:
            sleep_time = (1 - self.tokens) / self.rate
            time.sleep(sleep_time + random.uniform(0,0.02))
            self.last = time.monotonic()
            self.tokens = 0
        else:
            self.tokens -= 1
_rate_limiters = {"binance": RateLimiter(RATE_PER_SEC, RATE_BURST)}

# ========== HTTP SESSION ==========
_session = requests.Session()
_session.headers.update({"User-Agent": "X01-ETL/1.0"})
adapter = requests.adapters.HTTPAdapter(max_retries=0)
_session.mount("https://", adapter)
_session.mount("http://", adapter)

# ========== FETCH CANDLES (same as before) ==========
def fetch_candles(symbol: str, interval: str, limit: int, timeout_sec: float) -> Tuple[List[Dict], bool, int, str]:
    fetch_start = time.time()
    symbol = symbol.upper()
    _rate_limiters["binance"].wait()
    url = "https://api.binance.com/api/v3/klines"
    params = {"symbol": symbol, "interval": interval, "limit": limit}
    for attempt in range(MAX_RETRIES):
        if time.time() - fetch_start > timeout_sec:
            log_issue("ERROR", "Fetch timed out", symbol=symbol, interval=interval)
            return [], False, 0, ""
        try:
            r = _session.get(url, params=params, timeout=REQUEST_TIMEOUT)
            if r.status_code == 429:
                time.sleep(2**attempt + random.uniform(0,0.5))
                continue
            r.raise_for_status()
            data = r.json()
            unique = {}
            now_ms = int(time.time() * 1000)
            interval_ms = TIMEFRAME_SECONDS.get(interval, 60) * 1000
            for c in data:
                try:
                    ts_ms = c[0]
                    if ts_ms > now_ms + 60000 or ts_ms < now_ms - (10 * 365 * 86400 * 1000):
                        continue
                    open_p = float(c[1]); high_p = float(c[2]); low_p = float(c[3])
                    close_p = float(c[4]); volume_p = float(c[5])
                    if volume_p < 0 or not (low_p <= open_p <= high_p and low_p <= close_p <= high_p):
                        continue
                except (ValueError, TypeError):
                    continue
                if len(unique) > limit * 1.1:
                    oldest = min(unique.keys())
                    del unique[oldest]
                unique[ts_ms] = {
                    "timestamp_ms": ts_ms,
                    "open": open_p, "high": high_p, "low": low_p,
                    "close": close_p, "volume": volume_p
                }
            candles = sorted(unique.values(), key=lambda x: x['timestamp_ms'])
            min_needed = MIN_REQUIRED_CANDLES.get(interval, limit * 0.8)
            if len(candles) < min_needed:
                log_issue("ERROR", "Too few candles", tf=interval, got=len(candles), needed=min_needed)
                return [], False, 0, ""
            max_gap_found = 0
            gap_detected = False
            gap_type = "none"
            for i in range(1, len(candles)):
                diff = candles[i]['timestamp_ms'] - candles[i-1]['timestamp_ms']
                if diff != interval_ms:
                    gap_detected = True
                    if diff > max_gap_found: max_gap_found = diff
                    if diff > interval_ms * 3: gap_type = "large_missing_candle"
                    elif diff > interval_ms * 1.5: gap_type = "missed_candle"
                    else: gap_type = "latency_jitter"
                    log_issue("WARNING", "Gap detected", symbol=symbol, tf=interval, gap_ms=diff, type=gap_type)
            last_ts = candles[-1]['timestamp_ms']
            min_stale_ms = max(2 * interval_ms, 120000)
            age = now_ms - last_ts
            if age > min_stale_ms:
                log_issue("ERROR", "Dataset too stale", tf=interval, age_sec=age//1000)
                return [], False, 0, ""
            return candles, gap_detected, max_gap_found, gap_type
        except Exception as e:
            if attempt == MAX_RETRIES-1:
                log_issue("ERROR", f"Fetch failed {symbol} {interval}", error=str(e))
                return [], False, 0, ""
            time.sleep((1.5**attempt) + random.uniform(0,0.3))
    return [], False, 0, ""

def write_raw_tsv(symbol: str, candles_by_tf: Dict[str, List[Dict]], tmp_tsv_path: str):
    with open(tmp_tsv_path, "w") as f:
        for tf in sorted(candles_by_tf.keys()):
            for c in candles_by_tf[tf]:
                f.write(f"{symbol.upper()}\t{tf}\t{c['timestamp_ms']}\t{c['open']}\t{c['high']}\t{c['low']}\t{c['close']}\t{c['volume']}\n")
    log_issue("INFO", f"Raw TSV written: {tmp_tsv_path}", rows=sum(len(v) for v in candles_by_tf.values()))

# ========== FUNCTION TO GET DIRECTION FROM PATTERN CODE ==========
def pattern_direction(pat_code):
    bullish = {'3WS','4WS','MRS','BUE','HAB','HBR','PIE','TZB','HAF','WHM','LWC','HAM','INV','BUH','RUP'}
    bearish = {'3BC','4BC','EVS','BRE','HBB','HBE','DCC','TZT','HBF','BLM','LBC','BRH','RDN'}
    if pat_code in bullish: return 1
    if pat_code in bearish: return -1
    return 0

def compress_events_by_direction(events):
    if not events:
        return []
    dir_events = []
    for ev in events:
        d = pattern_direction(ev['pattern'])
        if d == 0:
            continue
        dir_events.append({
            'start_ts': ev['start_ts'],
            'end_ts': ev['end_ts'],
            'duration': ev['duration'],
            'strength': ev['strength'],
            'pattern': ev['pattern'],
            'direction': d
        })
    if not dir_events:
        return []
    merged = []
    curr = dir_events[0].copy()
    curr['first_pattern'] = curr['pattern']
    curr['last_pattern'] = curr['pattern']
    curr['duration_sum'] = curr['duration']
    curr['strength_sum'] = curr['strength']
    for ev in dir_events[1:]:
        if ev['direction'] == curr['direction']:
            curr['end_ts'] = ev['end_ts']
            curr['duration_sum'] += ev['duration']
            curr['strength_sum'] += ev['strength']
            curr['last_pattern'] = ev['pattern']
        else:
            merged.append({
                'start_ts': curr['start_ts'],
                'end_ts': curr['end_ts'],
                'duration': curr['duration_sum'],
                'strength': curr['strength_sum'],
                'first_pattern': curr['first_pattern'],
                'last_pattern': curr['last_pattern'],
                'direction': 'BULL' if curr['direction'] == 1 else 'BEAR'
            })
            curr = ev.copy()
            curr['first_pattern'] = curr['pattern']
            curr['last_pattern'] = curr['pattern']
            curr['duration_sum'] = curr['duration']
            curr['strength_sum'] = curr['strength']
    merged.append({
        'start_ts': curr['start_ts'],
        'end_ts': curr['end_ts'],
        'duration': curr['duration_sum'],
        'strength': curr['strength_sum'],
        'first_pattern': curr['first_pattern'],
        'last_pattern': curr['last_pattern'],
        'direction': 'BULL' if curr['direction'] == 1 else 'BEAR'
    })
    return merged

# ========== MAIN PIPELINE ==========
def run_overwrite(symbol: str):
    if not is_valid_symbol(symbol):
        log_issue("ERROR", "Invalid symbol", symbol=symbol)
        return False

    raw_tsv_path = os.path.join(FEATURES_BASE_DIR, f"{symbol.lower()}_raw.tmp.tsv")
    try:
        check_disk_space()
        timeframes = ["1m","5m","15m","1h","4h"]
        fetched = {}
        for tf in timeframes:
            limit = LIMITS.get(tf, 500)
            candles, _, max_gap, gap_type = fetch_candles(symbol, tf, limit, TIMEFRAME_FETCH_TIMEOUT)
            if not candles:
                time.sleep(2)
                candles, _, max_gap, gap_type = fetch_candles(symbol, tf, limit, TIMEFRAME_FETCH_TIMEOUT)
            if not candles:
                log_issue("ERROR", f"Missing timeframe {tf} after retry", symbol=symbol)
                return False
            fetched[tf] = candles
            if max_gap:
                log_issue("WARNING", f"Gap in {tf}", max_gap_ms=max_gap, type=gap_type)

        write_raw_tsv(symbol, fetched, raw_tsv_path)

        if not E01_AVAILABLE:
            log_issue("ERROR", "E01_candles_expert not available. Cannot generate final TSV.")
            return False

        data_by_tf, _ = load_and_prepare(raw_tsv_path)
        if data_by_tf.get('4h') is None or data_by_tf.get('1h') is None:
            log_issue("ERROR", "Missing 4H or 1H data")
            return False

        raw_1m, events_by_tf, raw_rows_by_tf = compute_all_patterns_and_events(data_by_tf)

        # Compress events by direction
        compressed_events_by_tf = {}
        for tf in events_by_tf:
            compressed_events_by_tf[tf] = compress_events_by_direction(events_by_tf[tf])

        # Get high‑probability scenario
        direction, prob, reason = compute_high_prob_scenario(data_by_tf, raw_1m, events_by_tf)

        final_tsv_path = os.path.join(FEATURES_BASE_DIR, f"{symbol.lower()}.tsv")
        with open(final_tsv_path, 'w') as out:
            out.write("timestamp\ttype\ttimeframe\tpattern\tduration\tstrength\tfirst_pattern\tlast_pattern\tdirection\tstart_ts\tend_ts\n")
            # 1. Write raw rows
            for tf in ['1m','5m','15m','1h','4h']:
                for r in raw_rows_by_tf.get(tf, []):
                    out.write(f"{r['timestamp']}\tRAW\t{tf}\t{r['patterns']}\t\t\t\t\t\t\t\n")
            # 2. Write HIGH_PROB_SCENARIO row (conclusion) right after raw candles
            out.write(f"HIGH_PROB_SCENARIO\t{direction}\t{prob}\t{reason}\t\t\t\t\t\t\t\n")
            # 3. Write event rows (compressed directional blocks)
            for tf in ['1m','5m','15m','1h','4h']:
                for ev in compressed_events_by_tf.get(tf, []):
                    out.write(f"{ev['start_ts']}\tEVENT\t{tf}\t{ev['first_pattern']}\t{ev['duration']}\t{ev['strength']}\t{ev['first_pattern']}\t{ev['last_pattern']}\t{ev['direction']}\t{ev['start_ts']}\t{ev['end_ts']}\n")

        log_issue("INFO", f"Final TSV written: {final_tsv_path}")

        return True
    except Exception as e:
        log_issue("ERROR", f"Overwrite failed for {symbol}", error=str(e))
        return False
    finally:
        if os.path.exists(raw_tsv_path):
            try:
                os.remove(raw_tsv_path)
                log_issue("INFO", f"Removed temporary raw file: {raw_tsv_path}")
            except:
                pass

def fetch_and_update_ws(symbol: str, ws_instance=None):
    return run_overwrite(symbol)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python X01_klines_rest.py SYMBOL")
        sys.exit(1)
    success = run_overwrite(sys.argv[1].upper())
    sys.exit(0 if success else 1)