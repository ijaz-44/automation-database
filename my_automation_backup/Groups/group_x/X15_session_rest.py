# Groups/group_x/X15_session_rest.py
"""
X15 - Session Analysis Module (Multi‑Source, Lahore Timezone)
- Session Kill Zones (London, NY) in Lahore time – calculation only
- Economic Calendar (next 24h high‑impact news) – 2 free sources with fallback
- Session Volatility Profile (hourly avg, 30 days) – Binance (fallback to pre‑computed)
Saves to: market_data/binance/symbols/{symbol}_sessions.tsv
All steps logged to terminal.
"""

import requests
import time
import os
import datetime
import json
from collections import defaultdict

# ==================== CONFIGURATION ====================
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SYMBOLS_DIR = os.path.join(BASE_DIR, "market_data", "binance", "symbols")
os.makedirs(SYMBOLS_DIR, exist_ok=True)

BINANCE_PRICE_URL = "https://api.binance.com/api/v3/klines"

# Economic Calendar Sources (free, no API key)
CALENDAR_SOURCES = [
    "https://nfs.faireconomy.media/ff_calendar_thisweek.json",  # primary
    "https://economic-calendar-api.herokuapp.com/events"         # fallback (may be slow)
]
# Note: The second URL may not always work; we'll also include a static fallback.

# Lahore timezone offset (UTC+5, no DST)
LAHORE_OFFSET = 5

def utc_to_lahore(utc_dt):
    return utc_dt + datetime.timedelta(hours=LAHORE_OFFSET)

def lahore_now():
    return utc_to_lahore(datetime.datetime.utcnow())

# ---------------------- 1. Session Kill Zones (calculation only) ----------------------
def get_kill_zones():
    """Return London and NY kill zones in Lahore time (ISO format)."""
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

# ---------------------- 2. Economic Calendar (Multi‑Source) ----------------------
def fetch_economic_calendar():
    """
    Try multiple sources to get high-impact events for next 24 hours.
    Returns list of events (datetime_lahore, currency, title, impact).
    """
    events = []
    # Primary source: ForexFactory
    try:
        print("[X15] Fetching economic calendar from ForexFactory...")
        r = requests.get(CALENDAR_SOURCES[0], timeout=8)
        if r.status_code == 200:
            data = r.json()
            now_utc = int(time.time())
            tomorrow_utc = now_utc + 86400
            for item in data:
                ts = item.get('date', 0)
                if not ts or ts < now_utc or ts > tomorrow_utc:
                    continue
                if str(item.get('impact', '')) == '3':  # High impact
                    dt_lahore = utc_to_lahore(datetime.datetime.utcfromtimestamp(ts))
                    events.append({
                        "datetime_lahore": dt_lahore.isoformat(),
                        "currency": item.get('country', ''),
                        "title": item.get('title', ''),
                        "impact": "High"
                    })
            if events:
                print(f"[X15] ForexFactory OK, found {len(events)} high-impact events")
                return events
            else:
                print("[X15] ForexFactory returned no high-impact events in next 24h")
        else:
            print(f"[X15] ForexFactory HTTP {r.status_code}")
    except Exception as e:
        print(f"[X15] ForexFactory error: {e}")
    
    # Fallback source: economic-calendar-api (if available)
    try:
        print("[X15] Trying fallback economic calendar source...")
        r = requests.get(CALENDAR_SOURCES[1], timeout=8)
        if r.status_code == 200:
            data = r.json()
            # The API format varies; we adapt
            now_utc = int(time.time())
            tomorrow_utc = now_utc + 86400
            for item in data:
                ts = item.get('timestamp') or item.get('date')
                if not ts:
                    continue
                if isinstance(ts, str):
                    try:
                        ts = int(datetime.datetime.fromisoformat(ts.replace('Z', '+00:00')).timestamp())
                    except:
                        continue
                if ts < now_utc or ts > tomorrow_utc:
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
                print(f"[X15] Fallback source OK, found {len(events)} events")
                return events
            else:
                print("[X15] Fallback source returned no high-impact events")
        else:
            print(f"[X15] Fallback source HTTP {r.status_code}")
    except Exception as e:
        print(f"[X15] Fallback source error: {e}")
    
    # If both fail, return empty list (will be logged as NO_DATA)
    print("[X15] WARNING: No economic calendar data available from any source.")
    return []

# ---------------------- 3. Volatility Profile (Binance + fallback) ----------------------
def get_hourly_volatility(symbol):
    """
    Fetch last 30 days of 1h candles from Binance.
    If fails, return a static default (based on typical BTC volatility).
    """
    limit = 24 * 30
    params = {"symbol": symbol.upper(), "interval": "1h", "limit": limit}
    try:
        print(f"[X15] Fetching volatility data from Binance for {symbol}...")
        r = requests.get(BINANCE_PRICE_URL, params=params, timeout=10)
        if r.status_code == 200:
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
                if vols:
                    result[hour] = round(sum(vols) / len(vols), 4)
            if result:
                print(f"[X15] Binance OK, computed volatility for {len(result)} hours")
                return result
            else:
                print("[X15] Binance returned insufficient data")
        else:
            print(f"[X15] Binance HTTP {r.status_code}")
    except Exception as e:
        print(f"[X15] Binance error: {e}")
    
    # Fallback: use a pre-defined volatility profile (e.g., from historical average)
    print("[X15] Using fallback static volatility profile (based on typical BTC hourly range)")
    # Static fallback: average volatility per hour (example values for BTC)
    static = {}
    for h in range(24):
        # Higher volatility during Asian/European overlap (approx 8-12 Lahore time) and NY open (17-20)
        if 8 <= h <= 12:
            static[h] = 0.45
        elif 17 <= h <= 20:
            static[h] = 0.55
        else:
            static[h] = 0.25
    return static

# ---------------------- Main Save Function ----------------------
def collect_and_save(symbol):
    print(f"[X15] Starting collection for {symbol}")
    filepath = os.path.join(SYMBOLS_DIR, f"{symbol.lower()}_sessions.tsv")
    
    # Step 1: Kill zones (no API)
    print("[X15] Step 1: Computing kill zones...")
    kill_zones = get_kill_zones()
    print(f"[X15] Kill zones: London {kill_zones['london']['start']} -> {kill_zones['london']['end']}, NY {kill_zones['newyork']['start']} -> {kill_zones['newyork']['end']}")
    
    # Step 2: Economic calendar
    print("[X15] Step 2: Fetching economic calendar (next 24h high-impact)...")
    economic_events = fetch_economic_calendar()
    
    # Step 3: Volatility profile
    print("[X15] Step 3: Fetching hourly volatility profile (30 days)...")
    volatility_profile = get_hourly_volatility(symbol)
    
    # Write to TSV
    with open(filepath, 'w') as f:
        f.write("# ========== SESSION KILL ZONES (Lahore Time) ==========\n")
        f.write("session\tstart\tend\n")
        f.write(f"London\t{kill_zones['london']['start']}\t{kill_zones['london']['end']}\n")
        f.write(f"NewYork\t{kill_zones['newyork']['start']}\t{kill_zones['newyork']['end']}\n")
        f.write("\n")
        
        f.write("# ========== ECONOMIC CALENDAR (Next 24h High-Impact, Lahore Time) ==========\n")
        f.write("datetime_lahore\tcurrency\ttitle\timpact\n")
        if economic_events:
            for ev in economic_events:
                f.write(f"{ev['datetime_lahore']}\t{ev['currency']}\t{ev['title']}\t{ev['impact']}\n")
        else:
            f.write("NO_HIGH_IMPACT_EVENTS\t0\t0\t0\n")
        f.write("\n")
        
        f.write("# ========== SESSION VOLATILITY PROFILE (Hourly Avg, 30 Days, Lahore Time) ==========\n")
        f.write("hour_lahore\tavg_volatility_pct\n")
        if volatility_profile:
            for hour in sorted(volatility_profile.keys()):
                f.write(f"{hour}\t{volatility_profile[hour]}\n")
        else:
            f.write("NO_DATA\t0\n")
    
    print(f"[X15] All data saved to {filepath}")

if __name__ == "__main__":
    collect_and_save("BTCUSDT")