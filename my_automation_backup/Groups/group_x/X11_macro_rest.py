#!/usr/bin/env python3
"""
X11_macro_rest.py – Raw Macroeconomic Data Downloader (Only .tmp_x)
- Fetches treasury yields, risk premium, stock indices, gold, oil, DXY, VIX, economic events.
- Uses fallback APIs.
- Global cache for 5 minutes.
- Writes a single row TSV: {symbol}_macro.tmp_x
- Logs issues to global log file (X11_macro.log) – NO terminal spam (only success/error lines).
"""

import os
import sys
import time
import datetime
import threading
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ========== CONFIGURATION ==========
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SYMBOLS_DIR = os.path.join(BASE_DIR, "market_data", "binance", "symbols")
os.makedirs(SYMBOLS_DIR, exist_ok=True)

CACHE_TTL = 300   # 5 minutes

# Global log file (single file for all symbols)
GLOBAL_LOG_FILE = os.path.join(SYMBOLS_DIR, "X11_macro.log")
LOG_MAX_SIZE = 5_000_000

# API Keys (user must replace if needed)
FMP_API_KEY = "YHCwaJeBO1VM4HSes37u9jpLJ0evFAq4"
POLYGON_API_KEY = "KorjOw9PlvhL4TcBf7Duixw0j2p7dtFp"
ALPHA_VANTAGE_KEY = "VR0IMR1DIAAIIOOW"
FINNHUB_KEY = "d6u7u8pr01qp1k9bjte0d6u7u8pr01qp1k9bjteg"
ALPACA_KEY = "PKV4ZTQRNABQTOKGZDMDIVGSWO"
ALPACA_SECRET = "4RwFhBkpp9a3aXHghRe3KNW932EwLHw3LLNStFAEutSv"
TIINGO_TOKEN = "83c2c98a0d132e441720c1788ea9bc3bcd51b852"

# Endpoints
FMP_TREASURY_URL = "https://financialmodelingprep.com/api/v3/treasury"
FMP_RISK_PREMIUM_URL = "https://financialmodelingprep.com/api/v3/market_risk_premium"
FMP_ECONOMIC_CALENDAR_URL = "https://financialmodelingprep.com/api/v3/economic_calendar"
POLYGON_TREASURY_URL = "https://api.polygon.io/v1/indicators/treasury"
POLYGON_ECONOMIC_URL = "https://api.polygon.io/v2/reference/economic"
ALPHA_VANTAGE_TREASURY_URL = "https://www.alphavantage.co/query"
FINNHUB_ECONOMIC_CALENDAR_URL = "https://finnhub.io/api/v1/calendar/economic"
ALPACA_BARS_URL = "https://data.alpaca.markets/v2/stocks/bars/latest"
TIINGO_FX_TOP_URL = "https://api.tiingo.com/tiingo/fx/top"
TIINGO_STOCKS_URL = "https://api.tiingo.com/tiingo/daily"

# Fallback constants (used when APIs fail)
FALLBACK_TREASURY_10Y = 4.5
FALLBACK_TREASURY_2Y = 4.2
FALLBACK_GOLD = 2300.0
FALLBACK_OIL = 70.0
FALLBACK_DXY = 105.0
FALLBACK_SPY = 500.0
FALLBACK_QQQ = 450.0
FALLBACK_DIA = 400.0
FALLBACK_RISK_PREMIUM = 0.0
FALLBACK_VIX = 15.0

# ========== LOGGING (global log file, no terminal prints) ==========
def rotate_log_if_needed():
    if os.path.exists(GLOBAL_LOG_FILE) and os.path.getsize(GLOBAL_LOG_FILE) > LOG_MAX_SIZE:
        backup = GLOBAL_LOG_FILE + ".old"
        try:
            os.replace(GLOBAL_LOG_FILE, backup)
        except:
            pass

def log_issue(level, msg, **kwargs):
    """Write to global log file only (no terminal)."""
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    line = f"{ts} [{level}] {msg}"
    if kwargs:
        line += " " + str(kwargs)
    rotate_log_if_needed()
    with open(GLOBAL_LOG_FILE, "a", encoding="utf-8") as f:
        f.write(line + "\n")

# ========== SESSION & RATE LIMITING ==========
session = requests.Session()
retry_strategy = Retry(total=2, backoff_factor=1, status_forcelist=[429,500,502,503,504])
adapter = HTTPAdapter(max_retries=retry_strategy)
session.mount("https://", adapter)

_last_fmp_call = 0
_last_polygon_call = 0
_fmp_lock = threading.Lock()
_polygon_lock = threading.Lock()

def rate_limit_fmp():
    global _last_fmp_call
    with _fmp_lock:
        now = time.time()
        if now - _last_fmp_call < 1.0:
            time.sleep(1.0 - (now - _last_fmp_call))
        _last_fmp_call = time.time()

def rate_limit_polygon():
    global _last_polygon_call
    with _polygon_lock:
        now = time.time()
        if now - _last_polygon_call < 12:
            time.sleep(12 - (now - _last_polygon_call))
        _last_polygon_call = time.time()

def get(url, params=None, headers=None, timeout=12):
    try:
        return session.get(url, params=params, headers=headers, timeout=timeout, verify=False)
    except Exception as e:
        return None

# ========== API FETCHERS (all logs go to file) – same as original ==========
def fetch_treasury_rates():
    """Return (ten_year, two_year) using fallback chain."""
    # FMP
    rate_limit_fmp()
    resp = get(FMP_TREASURY_URL, params={"apikey": FMP_API_KEY})
    if resp and resp.status_code == 200:
        try:
            data = resp.json()
            if isinstance(data, list) and len(data) > 0:
                t10 = data[0].get("10y") or data[0].get("10Y")
                t2 = data[0].get("2y") or data[0].get("2Y")
                if t10 and t2:
                    return float(t10), float(t2)
        except:
            pass
    # Polygon
    rate_limit_polygon()
    today = datetime.datetime.now().strftime("%Y-%m-%d")
    ten, two = None, None
    for ticker, name in [("DGS10", "ten"), ("DGS2", "two")]:
        url = f"{POLYGON_TREASURY_URL}/{ticker}/{today}"
        resp = get(url, params={"apiKey": POLYGON_API_KEY})
        if resp and resp.status_code == 200:
            data = resp.json()
            vals = data.get("results", {}).get("values", [])
            if vals:
                val = vals[0].get("value")
                if val:
                    if name == "ten":
                        ten = float(val)
                    else:
                        two = float(val)
        if not (ten and two):
            # try previous day
            prev = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
            url_prev = f"{POLYGON_TREASURY_URL}/{ticker}/{prev}"
            resp2 = get(url_prev, params={"apiKey": POLYGON_API_KEY})
            if resp2 and resp2.status_code == 200:
                data2 = resp2.json()
                vals2 = data2.get("results", {}).get("values", [])
                if vals2:
                    val = vals2[0].get("value")
                    if val:
                        if name == "ten":
                            ten = float(val)
                        else:
                            two = float(val)
    if ten and two:
        return ten, two
    # Alpha Vantage
    params = {"function": "TREASURY_YIELD", "interval": "daily", "apikey": ALPHA_VANTAGE_KEY}
    t10, t2 = None, None
    params["maturity"] = "10year"
    resp10 = get(ALPHA_VANTAGE_TREASURY_URL, params=params)
    if resp10 and resp10.status_code == 200:
        try:
            data = resp10.json()
            values = data.get("data", [])
            if values:
                t10 = float(values[0].get("value", FALLBACK_TREASURY_10Y))
        except:
            pass
    params["maturity"] = "2year"
    resp2 = get(ALPHA_VANTAGE_TREASURY_URL, params=params)
    if resp2 and resp2.status_code == 200:
        try:
            data = resp2.json()
            values = data.get("data", [])
            if values:
                t2 = float(values[0].get("value", FALLBACK_TREASURY_2Y))
        except:
            pass
    if t10 and t2:
        return t10, t2
    # Yahoo fallback
    try:
        url = "https://query1.finance.yahoo.com/v8/finance/chart/^TNX"
        r = get(url, headers={"User-Agent": "Mozilla/5.0"})
        t10 = FALLBACK_TREASURY_10Y
        if r and r.status_code == 200:
            data = r.json()
            price = data.get("chart", {}).get("result", [{}])[0].get("meta", {}).get("regularMarketPrice")
            if price:
                t10 = price
        url2 = "https://query1.finance.yahoo.com/v8/finance/chart/^IRX"
        r2 = get(url2, headers={"User-Agent": "Mozilla/5.0"})
        t2 = FALLBACK_TREASURY_2Y
        if r2 and r2.status_code == 200:
            data2 = r2.json()
            price2 = data2.get("chart", {}).get("result", [{}])[0].get("meta", {}).get("regularMarketPrice")
            if price2:
                t2 = price2
        return t10, t2
    except:
        pass
    log_issue("WARNING", "All treasury sources failed, using fallbacks")
    return FALLBACK_TREASURY_10Y, FALLBACK_TREASURY_2Y

def fetch_risk_premium():
    rate_limit_fmp()
    resp = get(FMP_RISK_PREMIUM_URL, params={"apikey": FMP_API_KEY})
    if resp and resp.status_code == 200:
        try:
            data = resp.json()
            if isinstance(data, list) and len(data) > 0:
                val = data[0].get("value")
                if val:
                    return float(val)
        except:
            pass
    log_issue("WARNING", "Risk premium fetch failed, using fallback 0")
    return FALLBACK_RISK_PREMIUM

def fetch_high_impact_events():
    """Return (count, next_timestamp_ms, next_title)."""
    # FMP
    rate_limit_fmp()
    resp = get(FMP_ECONOMIC_CALENDAR_URL, params={"apikey": FMP_API_KEY})
    if resp and resp.status_code == 200:
        try:
            data = resp.json()
            high_events = []
            if isinstance(data, list):
                for e in data:
                    impact = str(e.get("impact", "")).lower()
                    if impact in ("high", "3", "3/3"):
                        raw_ts = e.get("date") or e.get("timestamp")
                        ts = parse_timestamp(raw_ts)
                        if ts:
                            title = (e.get("event") or "High Impact Event").replace(",", "|")
                            high_events.append((ts, title))
            now_utc = int(time.time())
            future = [(ts, title) for ts, title in high_events if ts > now_utc]
            future.sort(key=lambda x: x[0])
            count = len(high_events)
            if future:
                return count, future[0][0], future[0][1]
            else:
                return count, 0, ""
        except:
            pass
    # Polygon
    rate_limit_polygon()
    now = datetime.datetime.now()
    params = {
        "apiKey": POLYGON_API_KEY,
        "from": now.strftime("%Y-%m-%d"),
        "to": (now + datetime.timedelta(days=2)).strftime("%Y-%m-%d"),
        "limit": 100
    }
    resp = get(POLYGON_ECONOMIC_URL, params=params)
    if resp and resp.status_code == 200:
        try:
            data = resp.json()
            results = data.get("results", [])
            high_events = []
            for e in results:
                impact = str(e.get("impact", "")).lower()
                if impact in ("high", "3", "3/3"):
                    raw_ts = e.get("timestamp") or e.get("date")
                    ts = parse_timestamp(raw_ts)
                    if ts:
                        title = (e.get("title") or "High Impact Event").replace(",", "|")
                        high_events.append((ts, title))
            now_utc = int(time.time())
            future = [(ts, title) for ts, title in high_events if ts > now_utc]
            future.sort(key=lambda x: x[0])
            count = len(high_events)
            if future:
                return count, future[0][0], future[0][1]
            else:
                return count, 0, ""
        except:
            pass
    # Finnhub
    params = {
        "token": FINNHUB_KEY,
        "from": now.strftime("%Y-%m-%d"),
        "to": (now + datetime.timedelta(days=2)).strftime("%Y-%m-%d")
    }
    resp = get(FINNHUB_ECONOMIC_CALENDAR_URL, params=params)
    if resp and resp.status_code == 200:
        try:
            data = resp.json()
            events = data.get("economicCalendar", [])
            high_events = []
            for e in events:
                impact = str(e.get("impact", "")).lower()
                if impact in ("high", "3", "3/3"):
                    raw_ts = e.get("timestamp") or e.get("date")
                    ts = parse_timestamp(raw_ts)
                    if ts:
                        title = (e.get("event") or "High Impact Event").replace(",", "|")
                        high_events.append((ts, title))
            now_utc = int(time.time())
            future = [(ts, title) for ts, title in high_events if ts > now_utc]
            future.sort(key=lambda x: x[0])
            count = len(high_events)
            if future:
                return count, future[0][0], future[0][1]
            else:
                return count, 0, ""
        except:
            pass
    log_issue("WARNING", "All economic calendar sources failed, returning zeros")
    return 0, 0, ""

def fetch_stock_price(stock_sym, fallback):
    headers = {"APCA-API-KEY-ID": ALPACA_KEY, "APCA-API-SECRET-KEY": ALPACA_SECRET}
    resp = get(ALPACA_BARS_URL, headers=headers, params={"symbols": stock_sym})
    if resp and resp.status_code == 200:
        try:
            data = resp.json()
            bars = data.get("bars", {}).get(stock_sym, [])
            if bars:
                return float(bars[0].get("c", fallback))
        except:
            pass
    resp2 = get(f"{TIINGO_STOCKS_URL}/{stock_sym}/prices", params={"token": TIINGO_TOKEN})
    if resp2 and resp2.status_code == 200:
        try:
            data2 = resp2.json()
            if data2:
                return float(data2[0].get("close", fallback))
        except:
            pass
    try:
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{stock_sym}"
        r = get(url, headers={"User-Agent": "Mozilla/5.0"})
        if r and r.status_code == 200:
            data = r.json()
            price = data.get("chart", {}).get("result", [{}])[0].get("meta", {}).get("regularMarketPrice")
            if price:
                return float(price)
    except:
        pass
    log_issue("WARNING", f"Failed to fetch {stock_sym}, using fallback {fallback}")
    return fallback

def fetch_gold():
    url = "https://api.tiingo.com/tiingo/fx/top"
    params = {"tickers": "xauusd", "token": TIINGO_TOKEN}
    resp = get(url, params=params)
    if resp and resp.status_code == 200:
        try:
            data = resp.json()
            if data and isinstance(data, list):
                price = data[0].get('midPrice') or data[0].get('bidPrice')
                if price and price > 0:
                    return float(price)
        except:
            pass
    log_issue("WARNING", "Gold fetch failed, using fallback")
    return FALLBACK_GOLD

def fetch_dxy():
    url = "https://api.tiingo.com/tiingo/fx/top"
    params = {"tickers": "eurusd", "token": TIINGO_TOKEN}
    resp = get(url, params=params)
    if resp and resp.status_code == 200:
        try:
            data = resp.json()
            if data and isinstance(data, list):
                eur = data[0].get('midPrice') or data[0].get('bidPrice')
                if eur and eur > 0:
                    return round(100 / eur, 2)
        except:
            pass
    log_issue("WARNING", "DXY fetch failed, using fallback")
    return FALLBACK_DXY

def fetch_oil():
    return FALLBACK_OIL

def parse_timestamp(ts):
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

# ========== GLOBAL CACHE ==========
_cached_row = None
_cached_time = 0

def get_current_macro_row():
    global _cached_row, _cached_time
    now = time.time()
    if _cached_row is not None and (now - _cached_time) < CACHE_TTL:
        return _cached_row

    log_issue("INFO", "Fetching fresh macro data")
    t10, t2 = fetch_treasury_rates()
    yield_spread = t10 - t2
    risk_premium = fetch_risk_premium()
    spy = fetch_stock_price("SPY", FALLBACK_SPY)
    qqq = fetch_stock_price("QQQ", FALLBACK_QQQ)
    dia = fetch_stock_price("DIA", FALLBACK_DIA)
    vix = fetch_stock_price("^VIX", FALLBACK_VIX)
    gold = fetch_gold()
    dxy = fetch_dxy()
    oil = fetch_oil()
    high_cnt, next_ts, next_title = fetch_high_impact_events()
    seconds_to_next = max(0, next_ts - int(now)) if next_ts > 0 else 0
    is_volatile = 1 if seconds_to_next < 1800 else 0

    fields = [
        "timestamp", "treasury_10y", "treasury_2y", "yield_spread",
        "high_impact_count", "vix", "risk_premium",
        "spy", "qqq", "dia", "xauusd", "usoil", "dxy",
        "seconds_to_next_event", "next_event_title", "is_volatile_zone"
    ]
    row = [
        int(now * 1000), t10, t2, yield_spread,
        high_cnt, vix, risk_premium,
        spy, qqq, dia, gold, oil, dxy,
        seconds_to_next, next_title if next_title else "NONE", is_volatile
    ]
    _cached_row = (fields, row)
    _cached_time = now
    return _cached_row

# ========== MAIN DOWNLOADER (minimal terminal output) ==========
def run_download(symbol):
    start_time = time.time()
    fields, row = get_current_macro_row()
    if not fields:
        print(f"[X11] ERROR: Failed to obtain macro data for {symbol}")
        log_issue("ERROR", f"Failed to obtain macro data for {symbol}")
        return False

    tmp_path = os.path.join(SYMBOLS_DIR, f"{symbol.lower()}_macro.tmp_x")
    try:
        with open(tmp_path, "w", encoding="utf-8") as f:
            f.write("\t".join(fields) + "\n")
            f.write("\t".join(str(v) for v in row) + "\n")
        elapsed = time.time() - start_time
        # Only success line printed to terminal
        print(f"[X11] SUCCESS: {symbol} done in {elapsed:.2f}s")
        log_issue("INFO", f"Download complete for {symbol} in {elapsed:.2f}s")
        return True
    except Exception as e:
        print(f"[X11] ERROR: Failed to write file for {symbol}: {e}")
        log_issue("ERROR", f"File write error for {symbol}: {e}")
        return False

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python X11_macro_rest.py SYMBOL")
        sys.exit(1)
    success = run_download(sys.argv[1].upper())
    sys.exit(0 if success else 1)