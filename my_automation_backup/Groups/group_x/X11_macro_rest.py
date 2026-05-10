"""
Macro data fetcher – on‑demand only (triggered by fill button).
Includes: Treasury yields, risk premium, SPY, QQQ, DIA, gold, oil, DXY,
VIX, yield spread, next high‑impact event countdown.
Saves to TOON format (macro_data array) with atomic rename.
GLOBAL CACHE: API calls only once per 5 minutes, but every symbol’s file gets updated.
"""

import requests
import time
import os
import datetime
import threading
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SYMBOLS_DIR = os.path.join(BASE_DIR, "market_data", "binance", "symbols")
os.makedirs(SYMBOLS_DIR, exist_ok=True)

# API Keys
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

# Fallback constants
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

# Max rows to keep in TOON array
MAX_MACRO_ROWS = 500
# Cache TTL (seconds) – 5 minutes
CACHE_TTL = 300

ISSUE_LOG = os.path.join(SYMBOLS_DIR, "macro_issues.log")
_FILE_LOCK = threading.Lock()

def log_issue(level, issue_type, message, details=None):
    timestamp = datetime.datetime.now().isoformat()
    log_line = f"{timestamp} [{level}] {issue_type}: {message}"
    if details:
        log_line += f" | Details: {details}"
    with open(ISSUE_LOG, 'a', encoding='utf-8') as f:
        f.write(log_line + "\n")
    print(f"[X11_macro] {level}: {message}")

class MacroDataFetcher:
    def __init__(self):
        self.session = requests.Session()
        retry_strategy = Retry(total=2, backoff_factor=1, status_forcelist=[429,500,502,503,504])
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("https://", adapter)
        self._tiingo_cache = {}
        self._tiingo_cache_time = 0
        self._cache_ttl = 60
        self._last_fmp_call = 0
        self._last_polygon_call = 0
        # Global cache for macro data
        self._last_macro_fetch = 0
        self._cached_row_vals = None
        self._cached_fields = None
        print("[X11_macro] TOON version with 5‑minute global cache, VIX, yield spread, event countdown")

    def _get(self, url, params=None, headers=None, timeout=12, verify=False):
        try:
            return self.session.get(url, params=params, headers=headers, timeout=timeout, verify=verify)
        except Exception as e:
            log_issue("ERROR", "NETWORK", str(e), details=url)
            return None

    def _rate_limit_fmp(self):
        now = time.time()
        if now - self._last_fmp_call < 1.0:
            time.sleep(1.0 - (now - self._last_fmp_call))
        self._last_fmp_call = time.time()

    def _rate_limit_polygon(self):
        now = time.time()
        if now - self._last_polygon_call < 12:
            time.sleep(12 - (now - self._last_polygon_call))
        self._last_polygon_call = time.time()

    # ---------- Treasury yields (unchanged) ----------
    def _get_treasury_rates(self):
        self._rate_limit_fmp()
        fmp = self._fetch_fmp_treasury()
        if fmp:
            return fmp
        poly = self._fetch_polygon_treasury()
        if poly:
            return poly
        av = self._fetch_alpha_treasury()
        if av:
            return av
        yahoo = self._fetch_yahoo_treasury()
        if yahoo:
            return yahoo
        log_issue("WARNING", "TREASURY_ALL_FAIL", "Using constant fallback")
        return FALLBACK_TREASURY_10Y, FALLBACK_TREASURY_2Y

    def _fetch_fmp_treasury(self):
        resp = self._get(FMP_TREASURY_URL, params={"apikey": FMP_API_KEY})
        if resp and resp.status_code == 200:
            try:
                data = resp.json()
                if isinstance(data, list) and len(data) > 0:
                    latest = data[0]
                    t10 = latest.get("10y") or latest.get("10Y")
                    t2 = latest.get("2y") or latest.get("2Y")
                    if t10 and t2:
                        return float(t10), float(t2)
            except Exception as e:
                log_issue("ERROR", "FMP_TREASURY_PARSE", str(e), details=resp.text[:200])
        else:
            status = resp.status_code if resp else "no response"
            log_issue("WARNING", "FMP_TREASURY_FAIL", f"HTTP {status}")
        return None

    def _fetch_polygon_treasury(self):
        self._rate_limit_polygon()
        today = datetime.datetime.now().strftime("%Y-%m-%d")
        ten, two = None, None
        for ticker, name in [("DGS10", "ten"), ("DGS2", "two")]:
            url = f"{POLYGON_TREASURY_URL}/{ticker}/{today}"
            resp = self._get(url, params={"apiKey": POLYGON_API_KEY})
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
            else:
                prev = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
                url_prev = f"{POLYGON_TREASURY_URL}/{ticker}/{prev}"
                resp2 = self._get(url_prev, params={"apiKey": POLYGON_API_KEY})
                if resp2 and resp2.status_code == 200:
                    data2 = resp2.json()
                    vals2 = data2.get("results", {}).get("values", [])
                    if vals2:
                        val = vals2[0].get("value")
                        if name == "ten":
                            ten = float(val)
                        else:
                            two = float(val)
        if ten and two:
            return ten, two
        return None

    def _fetch_alpha_treasury(self):
        params = {"function": "TREASURY_YIELD", "interval": "daily", "apikey": ALPHA_VANTAGE_KEY}
        t10, t2 = None, None
        params["maturity"] = "10year"
        resp10 = self._get(ALPHA_VANTAGE_TREASURY_URL, params=params)
        if resp10 and resp10.status_code == 200:
            try:
                data = resp10.json()
                values = data.get("data", [])
                if values:
                    t10 = float(values[0].get("value", FALLBACK_TREASURY_10Y))
            except:
                pass
        params["maturity"] = "2year"
        resp2 = self._get(ALPHA_VANTAGE_TREASURY_URL, params=params)
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
        return None

    def _fetch_yahoo_treasury(self):
        try:
            url = "https://query1.finance.yahoo.com/v8/finance/chart/^TNX"
            resp = self._get(url, headers={"User-Agent": "Mozilla/5.0"})
            t10 = FALLBACK_TREASURY_10Y
            if resp and resp.status_code == 200:
                data = resp.json()
                price = data.get("chart", {}).get("result", [{}])[0].get("meta", {}).get("regularMarketPrice")
                if price:
                    t10 = price
            url2 = "https://query1.finance.yahoo.com/v8/finance/chart/^IRX"
            resp2 = self._get(url2, headers={"User-Agent": "Mozilla/5.0"})
            t2 = FALLBACK_TREASURY_2Y
            if resp2 and resp2.status_code == 200:
                data2 = resp2.json()
                price2 = data2.get("chart", {}).get("result", [{}])[0].get("meta", {}).get("regularMarketPrice")
                if price2:
                    t2 = price2
            return t10, t2
        except Exception as e:
            log_issue("WARNING", "YAHOO_SCRAPE_FAIL", str(e))
            return None

    # ---------- Economic Calendar (robust impact, safe title) ----------
    def _get_high_impact_events(self):
        self._rate_limit_fmp()
        fmp = self._fetch_fmp_economic()
        if fmp is not None:
            count, next_ts, next_title = fmp
            return count, next_ts, next_title
        poly = self._fetch_polygon_economic()
        if poly is not None:
            count, next_ts, next_title = poly
            return count, next_ts, next_title
        finn = self._fetch_finnhub_economic()
        if finn is not None:
            count, next_ts, next_title = finn
            return count, next_ts, next_title
        return 0, 0, ""

    def _fetch_fmp_economic(self):
        resp = self._get(FMP_ECONOMIC_CALENDAR_URL, params={"apikey": FMP_API_KEY})
        if resp and resp.status_code == 200:
            try:
                data = resp.json()
                high_events = []
                if isinstance(data, list):
                    for e in data:
                        impact = str(e.get("impact", "")).lower()
                        if impact in ["high", "3", "3/3"]:
                            raw_ts = e.get("date") or e.get("timestamp")
                            ts = self._parse_timestamp(raw_ts)
                            if ts:
                                title = (e.get("event") or "High Impact Event").replace(",", "|")
                                high_events.append((ts, title))
                now_utc = int(time.time())
                future = [(ts, title) for ts, title in high_events if ts > now_utc]
                future.sort(key=lambda x: x[0])
                count = len([e for e in data if str(e.get("impact", "")).lower() in ["high", "3", "3/3"]])
                if future:
                    next_ts, next_title = future[0]
                    return count, next_ts, next_title
                else:
                    return count, 0, ""
            except Exception as e:
                log_issue("ERROR", "FMP_CALENDAR_PARSE", str(e), details=resp.text[:200])
        else:
            status = resp.status_code if resp else "no response"
            log_issue("WARNING", "FMP_CALENDAR_FAIL", f"HTTP {status}")
        return None

    def _fetch_polygon_economic(self):
        self._rate_limit_polygon()
        now = datetime.datetime.now()
        params = {
            "apiKey": POLYGON_API_KEY,
            "from": now.strftime("%Y-%m-%d"),
            "to": (now + datetime.timedelta(days=2)).strftime("%Y-%m-%d"),
            "limit": 100
        }
        resp = self._get(POLYGON_ECONOMIC_URL, params=params)
        if resp and resp.status_code == 200:
            data = resp.json()
            results = data.get("results", [])
            high_events = []
            for e in results:
                impact = str(e.get("impact", "")).lower()
                if impact in ["high", "3", "3/3"]:
                    raw_ts = e.get("timestamp") or e.get("date")
                    ts = self._parse_timestamp(raw_ts)
                    if ts:
                        title = (e.get("title") or "High Impact Event").replace(",", "|")
                        high_events.append((ts, title))
            now_utc = int(time.time())
            future = [(ts, title) for ts, title in high_events if ts > now_utc]
            future.sort(key=lambda x: x[0])
            count = len([e for e in results if str(e.get("impact", "")).lower() in ["high", "3", "3/3"]])
            if future:
                next_ts, next_title = future[0]
                return count, next_ts, next_title
            else:
                return count, 0, ""
        return None

    def _fetch_finnhub_economic(self):
        now = datetime.datetime.now()
        params = {
            "token": FINNHUB_KEY,
            "from": now.strftime("%Y-%m-%d"),
            "to": (now + datetime.timedelta(days=2)).strftime("%Y-%m-%d")
        }
        resp = self._get(FINNHUB_ECONOMIC_CALENDAR_URL, params=params)
        if resp and resp.status_code == 200:
            data = resp.json()
            events = data.get("economicCalendar", [])
            high_events = []
            for e in events:
                impact = str(e.get("impact", "")).lower()
                if impact in ["high", "3", "3/3"]:
                    raw_ts = e.get("timestamp") or e.get("date")
                    ts = self._parse_timestamp(raw_ts)
                    if ts:
                        title = (e.get("event") or "High Impact Event").replace(",", "|")
                        high_events.append((ts, title))
            now_utc = int(time.time())
            future = [(ts, title) for ts, title in high_events if ts > now_utc]
            future.sort(key=lambda x: x[0])
            count = len([e for e in events if str(e.get("impact", "")).lower() in ["high", "3", "3/3"]])
            if future:
                next_ts, next_title = future[0]
                return count, next_ts, next_title
            else:
                return count, 0, ""
        return None

    # ---------- Risk Premium (unchanged) ----------
    def _get_risk_premium(self):
        self._rate_limit_fmp()
        resp = self._get(FMP_RISK_PREMIUM_URL, params={"apikey": FMP_API_KEY})
        if resp and resp.status_code == 200:
            try:
                data = resp.json()
                if isinstance(data, list) and len(data) > 0:
                    val = data[0].get("value")
                    if val:
                        return float(val)
            except:
                pass
        log_issue("WARNING", "FMP_RISK_PREMIUM_FAIL", "Using fallback 0")
        return FALLBACK_RISK_PREMIUM

    # ---------- Stock Indices (including VIX) ----------
    def _get_stock_price(self, symbol, fallback):
        # Alpaca (for regular stocks)
        headers = {"APCA-API-KEY-ID": ALPACA_KEY, "APCA-API-SECRET-KEY": ALPACA_SECRET}
        resp = self._get(ALPACA_BARS_URL, headers=headers, params={"symbols": symbol})
        if resp and resp.status_code == 200:
            try:
                data = resp.json()
                bars = data.get("bars", {}).get(symbol, [])
                if bars:
                    return float(bars[0].get("c", fallback))
            except:
                pass
        # Tiingo
        resp2 = self._get(f"{TIINGO_STOCKS_URL}/{symbol}/prices", params={"token": TIINGO_TOKEN})
        if resp2 and resp2.status_code == 200:
            try:
                data2 = resp2.json()
                if data2:
                    return float(data2[0].get("close", fallback))
            except:
                pass
        # Yahoo Finance fallback (works for ^VIX)
        try:
            url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
            resp = self._get(url, headers={"User-Agent": "Mozilla/5.0"})
            if resp and resp.status_code == 200:
                data = resp.json()
                price = data.get("chart", {}).get("result", [{}])[0].get("meta", {}).get("regularMarketPrice")
                if price:
                    return float(price)
        except:
            pass
        log_issue("WARNING", f"STOCK_{symbol}_FAIL", f"Using fallback {fallback}")
        return fallback

    def _get_spy(self):
        return self._get_stock_price("SPY", FALLBACK_SPY)

    def _get_qqq(self):
        return self._get_stock_price("QQQ", FALLBACK_QQQ)

    def _get_dia(self):
        return self._get_stock_price("DIA", FALLBACK_DIA)

    def _get_vix(self):
        return self._get_stock_price("^VIX", FALLBACK_VIX)

    # ---------- Gold, Oil, DXY (unchanged) ----------
    def _fetch_tiingo_fx(self, ticker):
        now = time.time()
        if (now - self._tiingo_cache_time) < self._cache_ttl and ticker in self._tiingo_cache:
            return self._tiingo_cache[ticker]
        params = {"tickers": ticker, "token": TIINGO_TOKEN}
        resp = self._get(TIINGO_FX_TOP_URL, params=params)
        if resp and resp.status_code == 200:
            data = resp.json()
            if data and isinstance(data, list):
                price = data[0].get('midPrice') or data[0].get('bidPrice')
                if price and price > 0:
                    self._tiingo_cache[ticker] = price
                    self._tiingo_cache_time = now
                    return price
        return None

    def _get_gold(self):
        val = self._fetch_tiingo_fx("xauusd")
        return val if val is not None else FALLBACK_GOLD

    def _get_dxy(self):
        eur = self._fetch_tiingo_fx("eurusd")
        return round(100 / eur, 2) if eur else FALLBACK_DXY

    def _get_oil(self):
        return FALLBACK_OIL

    @staticmethod
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

    # ---------- TOON helpers (unchanged) ----------
    def _read_macro_rows(self, filepath):
        if not os.path.exists(filepath):
            return None, []
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                content = f.read()
            import re
            pattern = r'macro_data\[(\d+)\]\{([^}]+)\}:\s*\n(?:\s+([^\n]+(?:\n\s+[^\n]+)*))?'
            match = re.search(pattern, content, re.DOTALL)
            if not match:
                return None, []
            fields = match.group(2).split(',')
            rows_text = match.group(3)
            rows = []
            if rows_text:
                for line in rows_text.split(' | '):
                    parts = line.strip().split(',')
                    if len(parts) == len(fields):
                        rows.append(parts)
            return fields, rows
        except Exception as e:
            log_issue("ERROR", "READ_TOON", str(e), filepath)
            return None, []

    def _atomic_write_toon(self, filepath, fields, rows):
        dirname = os.path.dirname(filepath)
        os.makedirs(dirname, exist_ok=True)
        temp_path = filepath + ".tmp"
        try:
            with open(temp_path, 'w', encoding='utf-8') as f:
                f.write(f"# Macroeconomic data – TOON format\n")
                f.write(f"generated: {datetime.datetime.now().isoformat()}\n")
                f.write(f"\nmacro_data[{len(rows)}]{{{','.join(fields)}}}:\n")
                if rows:
                    row_strings = []
                    for row_vals in rows:
                        row_strings.append(','.join(str(v) for v in row_vals))
                    f.write("  " + " |\n  ".join(row_strings) + "\n")
                else:
                    f.write("  \n")
                f.write("\n# ========== END OF TOON DATA ==========\n")
                f.flush()
                os.fsync(f.fileno())
            os.rename(temp_path, filepath)
        except Exception as e:
            if os.path.exists(temp_path):
                try:
                    os.remove(temp_path)
                except:
                    pass
            log_issue("ERROR", "TOON_WRITE", f"{filepath}: {e}", details=str(e))
            raise

    # ---------- Core method: refresh cache every 5 min ----------
    def _refresh_cache_if_needed(self):
        now = time.time()
        if self._cached_row_vals is not None and (now - self._last_macro_fetch) < CACHE_TTL:
            log_issue("INFO", "CACHE_HIT", f"Using cached macro data (age {int(now - self._last_macro_fetch)}s)", None)
            return True
        log_issue("INFO", "CACHE_MISS", "Fetching fresh macro data...", None)

        # Fetch all values
        treasury_10y, treasury_2y = self._get_treasury_rates()
        high_impact_count, next_event_ts, next_event_title = self._get_high_impact_events()
        risk_premium = self._get_risk_premium()
        spy = self._get_spy()
        qqq = self._get_qqq()
        dia = self._get_dia()
        xauusd = self._get_gold()
        oil = self._get_oil()
        dxy = self._get_dxy()
        vix = self._get_vix()

        # Derived values
        yield_spread = treasury_10y - treasury_2y
        seconds_to_next_event = max(0, next_event_ts - int(now)) if next_event_ts > 0 else 0
        is_volatile_zone = seconds_to_next_event < 1800  # 30 minutes

        def validate(val, fallback, name):
            try:
                if float(val) > 0:
                    return float(val)
            except:
                pass
            log_issue("WARNING", "VALIDATION", f"{name} invalid, using fallback")
            return fallback

        treasury_10y = validate(treasury_10y, FALLBACK_TREASURY_10Y, "10Y")
        treasury_2y = validate(treasury_2y, FALLBACK_TREASURY_2Y, "2Y")
        high_impact_count = validate(high_impact_count, 0, "HighImpactCount")
        risk_premium = validate(risk_premium, 0, "RiskPremium")
        spy = validate(spy, FALLBACK_SPY, "SPY")
        qqq = validate(qqq, FALLBACK_QQQ, "QQQ")
        dia = validate(dia, FALLBACK_DIA, "DIA")
        xauusd = validate(xauusd, FALLBACK_GOLD, "Gold")
        oil = validate(oil, FALLBACK_OIL, "Oil")
        dxy = validate(dxy, FALLBACK_DXY, "DXY")
        vix = validate(vix, FALLBACK_VIX, "VIX")
        yield_spread = validate(yield_spread, 0.0, "YieldSpread")
        seconds_to_next_event = int(validate(seconds_to_next_event, 0, "SecondsToEvent"))
        # next_event_title already safe (commas replaced)
        if not next_event_title:
            next_event_title = "NONE"

        ts = int(now * 1000)
        fields = [
            "timestamp", "treasury_10y", "treasury_2y", "yield_spread",
            "high_impact_count", "vix", "risk_premium",
            "spy", "qqq", "dia", "xauusd", "usoil", "dxy",
            "seconds_to_next_event", "next_event_title", "is_volatile_zone"
        ]
        row_vals = [ts, treasury_10y, treasury_2y, yield_spread,
                    high_impact_count, vix, risk_premium,
                    spy, qqq, dia, xauusd, oil, dxy,
                    seconds_to_next_event, next_event_title, 1 if is_volatile_zone else 0]
        self._cached_fields = fields
        self._cached_row_vals = row_vals
        self._last_macro_fetch = now
        log_issue("INFO", "CACHE_UPDATED", "Macro data cached for next 5 minutes", None)
        return True

    def fetch_and_save_all(self, symbol, minutes=120):
        if not self._refresh_cache_if_needed():
            log_issue("ERROR", "CACHE_FAIL", "Could not obtain macro data", None)
            return

        filepath = os.path.join(SYMBOLS_DIR, f"{symbol.lower()}_macro.toon")
        fields = self._cached_fields
        new_row_str = [str(v) for v in self._cached_row_vals]

        with _FILE_LOCK:
            existing_fields, existing_rows = self._read_macro_rows(filepath)
            if existing_fields is None or not existing_rows:
                all_rows = [new_row_str]
            else:
                all_rows = existing_rows.copy()
                all_rows.append(new_row_str)
                if len(all_rows) > MAX_MACRO_ROWS:
                    all_rows = all_rows[-MAX_MACRO_ROWS:]

            try:
                self._atomic_write_toon(filepath, fields, all_rows)
                log_issue("INFO", "SAVE_SUCCESS", f"Appended macro row for {symbol}, now {len(all_rows)} rows", None)
            except Exception as e:
                log_issue("ERROR", "SAVE_FAIL", f"Could not save macro data for {symbol}", details=str(e))
                return

if __name__ == "__main__":
    fetcher = MacroDataFetcher()
    fetcher.fetch_and_save_all("BTCUSDT")
    print("Test done.")