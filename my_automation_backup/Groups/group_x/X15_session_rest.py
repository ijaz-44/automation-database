#!/usr/bin/env python3
"""
X15_session_rest.py – Raw Session Data Downloader (Only .tmp_x)
- Fetches session kill zones (London, NY) in Lahore time
- Fetches economic calendar (next 24h high‑impact events)
- Fetches hourly volatility profile (30 days of 1h candles, averages per hour)
- Fetches current price and last 48h 1h candles (for bias and levels)
- Writes raw TSV: {symbol}_sessions.tmp_x
- Logs to global file: market_data/binance/symbols/X15_sessions.log
- No processing, no TOON, no derived calculations.
"""

import os
import sys
import time
import datetime
import requests
from collections import defaultdict

# ========== CONFIGURATION ==========
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SYMBOLS_DIR = os.path.join(BASE_DIR, "market_data", "binance", "symbols")
os.makedirs(SYMBOLS_DIR, exist_ok=True)

# Global log file for this module (not per symbol)
LOG_FILE = os.path.join(SYMBOLS_DIR, "X15_sessions.log")
LOG_MAX_SIZE = 5_000_000

BINANCE_PRICE_URL = "https://api.binance.com/api/v3/klines"
LAHORE_OFFSET = 5

# ========== GLOBAL LOGGING ==========
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
    try:
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except:
        pass

def utc_to_lahore(utc_dt):
    return utc_dt + datetime.timedelta(hours=LAHORE_OFFSET)

def lahore_now():
    return utc_to_lahore(datetime.datetime.utcnow())

# ---------- 1. Session Kill Zones (raw) ----------
def get_kill_zones():
    now_lahore = lahore_now()
    today = now_lahore.date()
    utc_now = datetime.datetime.utcnow()
    is_edt = (utc_now.month > 3 and utc_now.month < 11)
    est_offset = -4 if is_edt else -5
    london_start_est, london_end_est = 2, 5
    ny_start_est, ny_end_est = 8, 11
    london_start_utc = (london_start_est - est_offset) % 24
    london_end_utc = (london_end_est - est_offset) % 24
    ny_start_utc = (ny_start_est - est_offset) % 24
    ny_end_utc = (ny_end_est - est_offset) % 24
    london_start_lah = (london_start_utc + LAHORE_OFFSET) % 24
    london_end_lah = (london_end_utc + LAHORE_OFFSET) % 24
    ny_start_lah = (ny_start_utc + LAHORE_OFFSET) % 24
    ny_end_lah = (ny_end_utc + LAHORE_OFFSET) % 24

    today_start = datetime.datetime(today.year, today.month, today.day, 0, 0, 0)
    london_start = today_start + datetime.timedelta(hours=london_start_lah)
    london_end = today_start + datetime.timedelta(hours=london_end_lah)
    ny_start = today_start + datetime.timedelta(hours=ny_start_lah)
    ny_end = today_start + datetime.timedelta(hours=ny_end_lah)

    if london_end_lah < london_start_lah:
        london_end += datetime.timedelta(days=1)
    if ny_end_lah < ny_start_lah:
        ny_end += datetime.timedelta(days=1)

    return {
        "london": {"start": london_start.isoformat(), "end": london_end.isoformat()},
        "newyork": {"start": ny_start.isoformat(), "end": ny_end.isoformat()}
    }

# ---------- 2. Economic Calendar (raw events) ----------
def _parse_timestamp(ts):
    if ts is None:
        return None
    if isinstance(ts, (int, float)):
        return int(ts)
    if isinstance(ts, str):
        try:
            if ts.endswith('Z'):
                ts = ts[:-1] + '+00:00'
            dt = datetime.datetime.fromisoformat(ts)
            return int(dt.timestamp())
        except:
            try:
                return int(ts)
            except:
                return None
    return None

def fetch_economic_calendar():
    events = []
    # Primary: ForexFactory
    try:
        r = requests.get("https://nfs.faireconomy.media/ff_calendar_thisweek.json", timeout=8)
        if r.status_code == 200:
            data = r.json()
            now_utc = int(time.time())
            tomorrow_utc = now_utc + 86400
            for item in data:
                raw_ts = item.get('date')
                ts = _parse_timestamp(raw_ts)
                if ts is None or ts < now_utc or ts > tomorrow_utc:
                    continue
                if str(item.get('impact', '')) == '3':
                    dt_lahore = utc_to_lahore(datetime.datetime.utcfromtimestamp(ts))
                    events.append({
                        "datetime_lahore": dt_lahore.isoformat(),
                        "currency": item.get('country', ''),
                        "title": item.get('title', ''),
                        "impact": "High"
                    })
            if events:
                log_issue("INFO", f"ForexFactory: found {len(events)} high-impact events")
                return events
    except Exception as e:
        log_issue("WARNING", f"ForexFactory error: {e}")

    # Fallback: economic-calendar-api
    try:
        r = requests.get("https://economic-calendar-api.herokuapp.com/events", timeout=8)
        if r.status_code == 200:
            data = r.json()
            now_utc = int(time.time())
            tomorrow_utc = now_utc + 86400
            for item in data:
                raw_ts = item.get('timestamp') or item.get('date')
                ts = _parse_timestamp(raw_ts)
                if ts is None or ts < now_utc or ts > tomorrow_utc:
                    continue
                impact = str(item.get('impact', '')).lower()
                if impact in ['high', '3']:
                    dt_lahore = utc_to_lahore(datetime.datetime.utcfromtimestamp(ts))
                    events.append({
                        "datetime_lahore": dt_lahore.isoformat(),
                        "currency": item.get('currency', item.get('country', '')),
                        "title": item.get('event', item.get('title', '')),
                        "impact": "High"
                    })
            if events:
                log_issue("INFO", f"Fallback calendar: found {len(events)} events")
                return events
    except Exception as e:
        log_issue("WARNING", f"Fallback calendar error: {e}")

    # Static fallback (example)
    log_issue("WARNING", "No live economic calendar data, using static example")
    now_lahore = lahore_now()
    return [
        {"datetime_lahore": (now_lahore + datetime.timedelta(hours=2)).strftime("%Y-%m-%dT%H:%M:%S"), "currency": "USD", "title": "FOMC Statement (example)", "impact": "High"},
        {"datetime_lahore": (now_lahore + datetime.timedelta(hours=6)).strftime("%Y-%m-%dT%H:%M:%S"), "currency": "EUR", "title": "ECB Press Conference (example)", "impact": "High"}
    ]

# ---------- 3. Volatility Profile (raw hourly averages from Binance) ----------
def get_hourly_volatility_raw(symbol):
    limit = 24 * 30  # 30 days of 1h candles
    params = {"symbol": symbol.upper(), "interval": "1h", "limit": limit}
    try:
        r = requests.get(BINANCE_PRICE_URL, params=params, timeout=15)
        if r.status_code != 200:
            log_issue("WARNING", f"Volatility fetch HTTP {r.status_code}")
            return {}
        data = r.json()
        hour_vol = defaultdict(list)
        for candle in data:
            ts_ms = candle[0]
            dt_utc = datetime.datetime.utcfromtimestamp(ts_ms / 1000)
            dt_lahore = utc_to_lahore(dt_utc)
            hour = dt_lahore.hour
            high = float(candle[2])
            low = float(candle[3])
            open_price = float(candle[1])
            if open_price > 0:
                vol_pct = (high - low) / open_price * 100
                hour_vol[hour].append(vol_pct)
        result = {}
        for hour, vols in hour_vol.items():
            result[hour] = round(sum(vols) / len(vols), 4)
        log_issue("INFO", f"Volatility profile: computed {len(result)} hours")
        return result
    except Exception as e:
        log_issue("ERROR", f"Volatility fetch error: {e}")
        return {}

# ---------- 4. Candles for bias (last 48h 1h candles) ----------
def fetch_1h_candles(symbol, limit=48):
    params = {"symbol": symbol.upper(), "interval": "1h", "limit": limit}
    try:
        r = requests.get(BINANCE_PRICE_URL, params=params, timeout=10)
        if r.status_code == 200:
            data = r.json()
            candles = []
            for c in data:
                candles.append({
                    'timestamp': int(c[0]),
                    'high': float(c[2]),
                    'low': float(c[3]),
                    'close': float(c[4])
                })
            return candles
        else:
            log_issue("WARNING", f"Candles fetch HTTP {r.status_code}")
            return []
    except Exception as e:
        log_issue("ERROR", f"Candles fetch error: {e}")
        return []

# ---------- 5. Helper for current price (from latest 1h candle) ----------
def get_current_price(candles):
    if candles:
        return candles[-1]['close']
    return 0.0

# ---------- 6. Main Downloader (raw output) ----------
def run_download(symbol):
    log_issue("INFO", f"Starting session raw download for {symbol}")
    start = time.time()

    # 1. Kill zones
    kill_zones = get_kill_zones()

    # 2. Economic calendar events
    econ_events = fetch_economic_calendar()

    # 3. Volatility profile (hourly averages)
    vol_profile = get_hourly_volatility_raw(symbol)

    # 4. 1h candles for bias and levels
    candles = fetch_1h_candles(symbol, limit=48)
    if not candles or len(candles) < 8:
        log_issue("WARNING", f"Insufficient candles for {symbol} (need 8), will use empty values")

    # Write to .tmp_x TSV (plural name: sessions)
    tmp_x_path = os.path.join(SYMBOLS_DIR, f"{symbol.lower()}_sessions.tmp_x")
    with open(tmp_x_path, "w", encoding="utf-8") as f:
        # Header
        f.write("data_type\tkey\tvalue1\tvalue2\tvalue3\tvalue4\tvalue5\n")

        # Kill zones
        f.write("kill_zone\tlondon_start\t{}\t\t\t\t\n".format(kill_zones['london']['start']))
        f.write("kill_zone\tlondon_end\t{}\t\t\t\t\n".format(kill_zones['london']['end']))
        f.write("kill_zone\tny_start\t{}\t\t\t\t\n".format(kill_zones['newyork']['start']))
        f.write("kill_zone\tny_end\t{}\t\t\t\t\n".format(kill_zones['newyork']['end']))

        # Economic events (each event as a row)
        for ev in econ_events:
            f.write("economic_event\t{}\t{}\t{}\t{}\t\t\n".format(
                ev['datetime_lahore'], ev['currency'], ev['title'], ev['impact']))

        # Volatility profile (each hour as a row)
        for hour, avg_vol in sorted(vol_profile.items()):
            f.write("volatility_profile\t{}\t{}\t\t\t\t\n".format(hour, avg_vol))

        # 1h candles (each candle as a row)
        for c in candles:
            f.write("candle_1h\t{}\t{}\t{}\t{}\t\t\n".format(
                c['timestamp'], c['high'], c['low'], c['close']))

    log_issue("INFO", f"Raw session data saved to {tmp_x_path}")
    elapsed = time.time() - start
    log_issue("INFO", f"Download complete for {symbol} in {elapsed:.2f}s")
    return True

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python X15_session_rest.py SYMBOL")
        sys.exit(1)
    success = run_download(sys.argv[1].upper())
    sys.exit(0 if success else 1)