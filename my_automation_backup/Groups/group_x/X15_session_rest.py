# Groups/group_x/X15_session_rest.py (Enhanced with Session Range, Liquidity Levels, News Risk)
"""
X15 - Session Analysis Module (TOON format, Multi‑Source, Lahore Timezone)
- Session Kill Zones (London, NY) in Lahore time
- Initial Balance (first 60 minutes) High/Low for each session
- Previous Session High/Low (liquidity targets)
- Economic Calendar (next 24h high‑impact news) with danger zone flag (next 15 min)
- Session Volatility Profile (hourly avg, 30 days) – Binance (fallback)
- Session Bias (based on price relative to last 8 candles)
- Atomic rename + full logging
- Saves to {symbol}_sessions.toon
"""

import requests
import time
import os
import datetime
import re
from collections import defaultdict

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SYMBOLS_DIR = os.path.join(BASE_DIR, "market_data", "binance", "symbols")
os.makedirs(SYMBOLS_DIR, exist_ok=True)

LOG_FILE = os.path.join(SYMBOLS_DIR, "session_issues.log")

def _log(msg, level="INFO"):
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"{timestamp} [{level}] {msg}"
    print(line)
    try:
        with open(LOG_FILE, 'a', encoding='utf-8') as f:
            f.write(line + "\n")
    except:
        pass

def atomic_write(path, content):
    tmp = path + ".tmp"
    try:
        with open(tmp, 'w', encoding='utf-8') as f:
            f.write(content)
            f.flush()
            os.fsync(f.fileno())
        os.rename(tmp, path)
        _log(f"[ATOMIC] OK -> {os.path.basename(path)}")
        return True
    except Exception as e:
        if os.path.exists(tmp):
            os.remove(tmp)
        _log(f"[ATOMIC] FAIL {path}: {e}", "ERROR")
        return False

BINANCE_PRICE_URL = "https://api.binance.com/api/v3/klines"
LAHORE_OFFSET = 5

def utc_to_lahore(utc_dt):
    return utc_dt + datetime.timedelta(hours=LAHORE_OFFSET)

def lahore_now():
    return utc_to_lahore(datetime.datetime.utcnow())

# ---------------------- 1. Session Kill Zones (unchanged) ----------------------
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

# ---------------------- 2. Economic Calendar (with danger zone) ----------------------
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
    # Source 1: ForexFactory
    try:
        _log("[X15] Fetching economic calendar from ForexFactory...")
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
                _log(f"[X15] ForexFactory OK, found {len(events)} high-impact events")
                return events
            else:
                _log("[X15] ForexFactory no high-impact events in next 24h")
        else:
            _log(f"[X15] ForexFactory HTTP {r.status_code}")
    except Exception as e:
        _log(f"[X15] ForexFactory error: {e}")

    # Source 2: economic-calendar-api
    try:
        _log("[X15] Trying fallback economic calendar source...")
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
                _log(f"[X15] Fallback source OK, found {len(events)} events")
                return events
            else:
                _log("[X15] Fallback source no high-impact events")
        else:
            _log(f"[X15] Fallback source HTTP {r.status_code}")
    except Exception as e:
        _log(f"[X15] Fallback source error: {e}")

    # Source 3: static fallback
    _log("[X15] Using static fallback (typical high-impact events)")
    now_lahore = lahore_now()
    static_events = [
        {"datetime_lahore": (now_lahore + datetime.timedelta(hours=2)).strftime("%Y-%m-%dT%H:%M:%S"), "currency": "USD", "title": "FOMC Statement (static example)", "impact": "High"},
        {"datetime_lahore": (now_lahore + datetime.timedelta(hours=6)).strftime("%Y-%m-%dT%H:%M:%S"), "currency": "EUR", "title": "ECB Press Conference (static example)", "impact": "High"},
    ]
    return static_events

# ---------------------- 3. Volatility Profile (unchanged) ----------------------
def get_hourly_volatility(symbol):
    limit = 24 * 30
    params = {"symbol": symbol.upper(), "interval": "1h", "limit": limit}
    try:
        _log(f"[X15] Fetching volatility from Binance for {symbol}...")
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
                _log(f"[X15] Binance OK, computed volatility for {len(result)} hours")
                return result
            else:
                _log("[X15] Binance insufficient data")
        else:
            _log(f"[X15] Binance HTTP {r.status_code}")
    except Exception as e:
        _log(f"[X15] Binance error: {e}")
    static = {}
    for h in range(24):
        if 8 <= h <= 12:
            static[h] = 0.45
        elif 17 <= h <= 20:
            static[h] = 0.55
        else:
            static[h] = 0.25
    return static

# ---------------------- 4. Helper: Read 1h candles from local .toon (or fallback to API) ----------------------
def read_1h_candles(symbol, limit=48):
    """Return list of dicts each with 'high', 'low', 'close' (last 'limit' candles)."""
    filepath = os.path.join(SYMBOLS_DIR, f"{symbol.lower()}.toon")
    if not os.path.exists(filepath):
        return fetch_1h_candles_api(symbol, limit)
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()
        pattern = r'candles_1h\[\d+\]\{ts,dt,o,h,l,c,v\}:\s*\n(?:\s+([^\n]+(?:\n\s+[^\n]+)*))?'
        match = re.search(pattern, content, re.DOTALL)
        if not match:
            return fetch_1h_candles_api(symbol, limit)
        rows_text = match.group(1)
        if not rows_text:
            return []
        candles = []
        for row in rows_text.split(' | '):
            parts = row.strip().split(',')
            if len(parts) >= 7:
                candles.append({
                    'high': float(parts[3]),
                    'low': float(parts[4]),
                    'close': float(parts[5])
                })
        if len(candles) > limit:
            candles = candles[-limit:]
        return candles
    except Exception as e:
        _log(f"[X15] Error reading 1h candles: {e}", "WARNING")
        return fetch_1h_candles_api(symbol, limit)

def fetch_1h_candles_api(symbol, limit=48):
    params = {"symbol": symbol.upper(), "interval": "1h", "limit": limit}
    try:
        r = requests.get(BINANCE_PRICE_URL, params=params, timeout=10)
        if r.status_code == 200:
            data = r.json()
            candles = []
            for c in data:
                candles.append({
                    'high': float(c[2]),
                    'low': float(c[3]),
                    'close': float(c[4])
                })
            return candles
        else:
            _log(f"[X15] API fetch failed: HTTP {r.status_code}", "WARNING")
    except Exception as e:
        _log(f"[X15] API fetch error: {e}", "WARNING")
    return []

# ---------------------- 5. Session Range (Initial Balance) ----------------------
def get_initial_balance(symbol, session_start, session_end):
    """
    For a given session (start/end datetime in Lahore time), fetch 1‑minute candles
    within that session and compute high/low of the first 60 minutes.
    Returns (ib_high, ib_low) or (None, None) if insufficient data.
    """
    # Convert Lahore datetime to UTC timestamp (ms)
    start_utc = session_start - datetime.timedelta(hours=LAHORE_OFFSET)
    end_utc = session_end - datetime.timedelta(hours=LAHORE_OFFSET)
    ib_end = start_utc + datetime.timedelta(hours=1)
    if ib_end > end_utc:
        ib_end = end_utc
    start_ms = int(start_utc.timestamp() * 1000)
    end_ms = int(ib_end.timestamp() * 1000)
    url = "https://api.binance.com/api/v3/klines"
    params = {"symbol": symbol.upper(), "interval": "1m", "startTime": start_ms, "endTime": end_ms, "limit": 60}
    try:
        r = requests.get(url, params=params, timeout=10)
        if r.status_code != 200:
            _log(f"[X15] IB fetch failed: HTTP {r.status_code}", "WARNING")
            return None, None
        data = r.json()
        if not data:
            return None, None
        high = max(float(c[2]) for c in data)
        low = min(float(c[3]) for c in data)
        return high, low
    except Exception as e:
        _log(f"[X15] IB error: {e}", "WARNING")
        return None, None

# ---------------------- 6. Session Bias & Previous Session High/Low ----------------------
def get_session_bias_and_liquidity(symbol):
    candles = read_1h_candles(symbol, limit=48)   # last 48 hours (2 days)
    if not candles or len(candles) < 8:
        return "Neutral", None, None
    last_8_high = max(c['high'] for c in candles[-8:])
    last_8_low = min(c['low'] for c in candles[-8:])
    current_price = candles[-1]['close']
    if current_price > last_8_high:
        bias = "Strong_Bullish"
    elif current_price < last_8_low:
        bias = "Strong_Bearish"
    else:
        bias = "Neutral"
    # Previous session high/low (last 24 hours)
    last_24_high = max(c['high'] for c in candles[-24:]) if len(candles) >= 24 else last_8_high
    last_24_low = min(c['low'] for c in candles[-24:]) if len(candles) >= 24 else last_8_low
    return bias, last_24_high, last_24_low

# ---------------------- 7. Main Save Function (enhanced) ----------------------
def collect_and_save(symbol):
    _log(f"[COLLECT] Starting for {symbol} (TOON format, enhanced session)")
    start_time = time.time()

    kill_zones = get_kill_zones()
    _log(f"[X15] Kill zones: London {kill_zones['london']['start']} -> {kill_zones['london']['end']}, NY {kill_zones['newyork']['start']} -> {kill_zones['newyork']['end']}")

    economic_events = fetch_economic_calendar()
    volatility_profile = get_hourly_volatility(symbol)

    # Compute session bias and previous session high/low
    bias, prev_high, prev_low = get_session_bias_and_liquidity(symbol)

    # Compute Initial Balance for London and NY sessions (if within time bounds)
    now_lahore = lahore_now()
    london_start = datetime.datetime.fromisoformat(kill_zones['london']['start'])
    london_end = datetime.datetime.fromisoformat(kill_zones['london']['end'])
    ny_start = datetime.datetime.fromisoformat(kill_zones['newyork']['start'])
    ny_end = datetime.datetime.fromisoformat(kill_zones['newyork']['end'])
    london_ib_high = london_ib_low = None
    ny_ib_high = ny_ib_low = None
    if now_lahore < london_end and now_lahore > london_start - datetime.timedelta(hours=1):
        london_ib_high, london_ib_low = get_initial_balance(symbol, london_start, london_end)
    if now_lahore < ny_end and now_lahore > ny_start - datetime.timedelta(hours=1):
        ny_ib_high, ny_ib_low = get_initial_balance(symbol, ny_start, ny_end)

    # Determine news danger zone (if any high‑impact event within next 15 minutes)
    danger_zone = False
    now_utc = int(time.time())
    for ev in economic_events:
        dt_str = ev.get('datetime_lahore', '')
        try:
            dt = datetime.datetime.fromisoformat(dt_str.replace('Z', '+00:00'))
            event_utc = int(dt.timestamp())
            if 0 < event_utc - now_utc < 900:   # within 15 minutes
                danger_zone = True
                break
        except:
            continue

    # Build TOON content
    lines = []
    lines.append(f"# Session analysis for {symbol.upper()} – TOON format (enhanced)")
    lines.append(f"generated: {datetime.datetime.now().isoformat()}")
    lines.append(f"symbol: {symbol}")
    lines.append("")

    # 1. Kill zones
    kill_fields = ["session", "start", "end"]
    kill_rows = [
        ["London", kill_zones['london']['start'], kill_zones['london']['end']],
        ["NewYork", kill_zones['newyork']['start'], kill_zones['newyork']['end']]
    ]
    lines.append(f"session_kill_zones[{len(kill_rows)}]{{{','.join(kill_fields)}}}:")
    lines.append("  " + " |\n  ".join([','.join(row) for row in kill_rows]))
    lines.append("")

    # 2. Economic calendar
    econ_fields = ["datetime_lahore", "currency", "title", "impact"]
    econ_rows = []
    for ev in economic_events:
        econ_rows.append([ev['datetime_lahore'], ev['currency'], ev['title'], ev['impact']])
    if not econ_rows:
        econ_rows.append(["NO_HIGH_IMPACT_EVENTS", "0", "0", "0"])
    lines.append(f"economic_calendar[{len(econ_rows)}]{{{','.join(econ_fields)}}}:")
    lines.append("  " + " |\n  ".join([','.join(row) for row in econ_rows]))
    lines.append("")

    # 3. Volatility profile
    vol_fields = ["hour_lahore", "avg_volatility_pct"]
    if volatility_profile:
        sorted_hours = sorted(volatility_profile.keys())
        vol_rows = [[str(h), str(volatility_profile[h])] for h in sorted_hours]
    else:
        vol_rows = [["NO_DATA", "0"]]
    lines.append(f"volatility_profile[{len(vol_rows)}]{{{','.join(vol_fields)}}}:")
    lines.append("  " + " |\n  ".join([','.join(row) for row in vol_rows]))
    lines.append("")

    # 4. Session bias and liquidity levels (simple key‑value lines for easy parsing)
    lines.append("session_bias: " + bias)
    lines.append(f"previous_session_high: {prev_high if prev_high else 'N/A'}")
    lines.append(f"previous_session_low: {prev_low if prev_low else 'N/A'}")
    lines.append(f"news_danger_zone: {'YES' if danger_zone else 'NO'}")
    lines.append("")

    # 5. Initial Balance arrays (if available)
    if london_ib_high and london_ib_low:
        lines.append("london_initial_balance[1]{high,low}:")
        lines.append(f"  {london_ib_high},{london_ib_low}")
        lines.append("")
    if ny_ib_high and ny_ib_low:
        lines.append("newyork_initial_balance[1]{high,low}:")
        lines.append(f"  {ny_ib_high},{ny_ib_low}")
        lines.append("")

    lines.append("# ========== END OF TOON DATA ==========")

    filepath = os.path.join(SYMBOLS_DIR, f"{symbol.lower()}_sessions.toon")
    content = "\n".join(lines) + "\n"
    if atomic_write(filepath, content):
        elapsed = time.time() - start_time
        _log(f"[SAVE_SUCCESS] {symbol} saved to TOON in {elapsed:.2f}s -> {filepath}")
        return True
    else:
        _log(f"[SAVE_FAIL] {symbol} could not save file", "ERROR")
        return False

session_collect = collect_and_save

if __name__ == "__main__":
    collect_and_save("BTCUSDT")