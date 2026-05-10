# X09_correlation_rest.py – Fixed: direct API fetch for symbol candles, robust reading
"""
KALI PORT - Correlation Module (Crypto + Traditional Assets with 1‑minute history)
- Crypto: BTC, ETH, BTCDOM, USDT_BTC (Binance, 2‑min cache)
- Traditional: SPY, QQQ, DIA, GLD, USO, DXY, VIX
- Primary: Yahoo Finance (free, unlimited)
- Fallback: Tiingo (1000 calls/day)
- Fallback: Alpaca (200 calls/min)
- Symbol price history: reads from local .toon (X01) with fallback to Binance API
- All data stored in TOON array correlation_data[...]
- Parallel fetching, rate‑limited fallbacks, atomic writes
"""

import requests
import time
import os
import threading
import math
import logging
import traceback
import glob
from collections import deque
import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

FUTURES_BASE_URL = "https://fapi.binance.com"
SPOT_BASE_URL = "https://api.binance.com/api/v3"

# API Keys for fallbacks (user must replace with their own)
TIINGO_TOKEN = "83c2c98a0d132e441720c1788ea9bc3bcd51b852"   # 1000 calls/day
ALPACA_API_KEY = "PKV4ZTQRNABQTOKGZDMDIVGSWO"              # 200 calls/min
ALPACA_SECRET_KEY = "4RwFhBkpp9a3aXHghRe3KNW932EwLHw3LLNStFAEutSv"

# Other API keys preserved but not used for traditional assets
FMP_API_KEY = "YHCwaJeBO1VM4HSes37u9jpLJ0evFAq4"           # kept for crypto fallback
FINNHUB_KEY = "d6u7u8pr01qp1k9bjte0d6u7u8pr01qp1k9bjteg"   # kept for macro module
TWELVE_DATA_KEY = "YOUR_TWELVE_DATA_KEY"                    # not used, kept for compatibility

REFRESH_INTERVAL = 30
MIN_POINTS = 30
WINDOW_SIZES = [15]
MAX_ROWS = 100
ROLLING_WINDOW = 30
RATE_LIMIT_SEC = 0.5
MAX_ZSCORE = 5.0
API_TIMEOUT = 8
CACHE_TTL_SEC = 120   # 2 minutes
ZSCORE_MIN_POINTS = 15

class CorrelationRest:
    def __init__(self, base_dir):
        self.base_dir = base_dir
        self.symbols_dir = os.path.join(base_dir, "symbols")
        os.makedirs(self.symbols_dir, exist_ok=True)
        self._file_lock = threading.Lock()
        self._running = True
        self._last_api_call = 0

        # Crypto indices
        self._btc_history = deque(maxlen=120)
        self._eth_history = deque(maxlen=120)
        self._btc_dom_history = deque(maxlen=120)
        self._usdt_btc_history = deque(maxlen=120)

        # Traditional asset deques
        self._spy_history = deque(maxlen=120)
        self._qqq_history = deque(maxlen=120)
        self._dia_history = deque(maxlen=120)
        self._gld_history = deque(maxlen=120)
        self._uso_history = deque(maxlen=120)
        self._dxy_history = deque(maxlen=120)
        self._vix_history = deque(maxlen=120)

        self._timestamp_history = deque(maxlen=120)

        # Cache storage
        self._cached_data = {}
        self._last_fetch_time = 0

        self._corr_history = {}
        self._session = requests.Session()
        self._session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        })

        self.issue_log_path = os.path.join(self.symbols_dir, "correlation_issues.log")

        # Cleanup orphaned .tmp files
        for ext in [".tsv.tmp", ".toon.tmp"]:
            for tmp_file in glob.glob(os.path.join(self.symbols_dir, f"*_correlation{ext}")):
                try:
                    os.remove(tmp_file)
                    self._log_issue("CLEANUP", f"Removed orphaned {os.path.basename(tmp_file)}", level=logging.INFO)
                except Exception as e:
                    self._log_issue("CLEANUP_ERROR", f"Could not remove {tmp_file}: {e}", level=logging.WARNING)

        self._pre_fill_history()
        logger.info("X09_correlation_rest initialized (TOON, on-demand, 2min cache, parallel traditional fetch)")
        self._log_issue("INFO", "Correlation module started (TOON, on-demand)", level=logging.INFO)

    def _pre_fill_history(self):
        """Initial history fill (called from __init__)."""
        self._refresh_indices_data()

    def __del__(self):
        self._session.close()

    def _log_issue(self, issue_type, message, level=logging.WARNING):
        timestamp = datetime.datetime.now().isoformat()
        log_line = f"{timestamp} [{issue_type}] {message}\n"
        try:
            with open(self.issue_log_path, 'a', encoding='utf-8') as f:
                f.write(log_line)
        except Exception as e:
            logger.error(f"Failed to write issue log: {e}")
        if level == logging.ERROR:
            logger.error(message)
        elif level == logging.WARNING:
            logger.warning(message)
        else:
            logger.info(message)

    def _rate_limited_fetch(self, url, params=None, timeout=API_TIMEOUT):
        now = time.time()
        elapsed = now - self._last_api_call
        if elapsed < RATE_LIMIT_SEC:
            time.sleep(RATE_LIMIT_SEC - elapsed)
        self._last_api_call = time.time()
        try:
            r = self._session.get(url, params=params, timeout=timeout)
            if r.status_code != 200:
                logger.error(f"API request failed: {r.status_code} for {url[:60]}")
                return None
            return r
        except Exception as e:
            logger.error(f"Request error: {e}")
            return None

    # ---------- Crypto data (Binance) – with 2‑min cache ----------
    def _refresh_crypto_data(self):
        now = time.time()
        if 'btc' in self._cached_data and (now - self._last_fetch_time) < CACHE_TTL_SEC:
            with self._file_lock:
                self._btc_history.clear()
                self._btc_history.extend(self._cached_data['btc'])
                self._eth_history.clear()
                self._eth_history.extend(self._cached_data['eth'])
                self._btc_dom_history.clear()
                self._btc_dom_history.extend(self._cached_data['dom'])
                self._usdt_btc_history.clear()
                self._usdt_btc_history.extend(self._cached_data['usdt_btc'])
                self._timestamp_history.clear()
                self._timestamp_history.extend(self._cached_data['ts'])
            logger.info(f"Crypto cache hit (age {now - self._last_fetch_time:.0f}s)")
            return True
        try:
            url_btc = f"{SPOT_BASE_URL}/klines"
            url_eth = f"{SPOT_BASE_URL}/klines"
            url_dom = f"{FUTURES_BASE_URL}/fapi/v1/klines"
            r_btc = self._rate_limited_fetch(url_btc, params={"symbol": "BTCUSDT", "interval": "1m", "limit": 120})
            r_eth = self._rate_limited_fetch(url_eth, params={"symbol": "ETHUSDT", "interval": "1m", "limit": 120})
            r_dom = self._rate_limited_fetch(url_dom, params={"symbol": "BTCDOMUSDT", "interval": "1m", "limit": 120})
            if not (r_btc and r_eth and r_dom) or not all(r.status_code == 200 for r in [r_btc, r_eth, r_dom]):
                return False
            btc_data = r_btc.json()
            eth_data = r_eth.json()
            dom_data = r_dom.json()
            btc_prices = []
            eth_prices = []
            dom_prices = []
            ts_list = []
            min_len = min(len(btc_data), len(eth_data), len(dom_data))
            for i in range(min_len):
                try:
                    btc = float(btc_data[i][4])
                    eth = float(eth_data[i][4])
                    dom = float(dom_data[i][4])
                    if btc <= 0: continue
                    btc_prices.append(btc)
                    eth_prices.append(eth)
                    dom_prices.append(dom)
                    ts_list.append(btc_data[i][0])
                except:
                    continue
            usdt_btc = [1.0/p for p in btc_prices]
            with self._file_lock:
                self._btc_history = deque(btc_prices, maxlen=120)
                self._eth_history = deque(eth_prices, maxlen=120)
                self._btc_dom_history = deque(dom_prices, maxlen=120)
                self._usdt_btc_history = deque(usdt_btc, maxlen=120)
                self._timestamp_history = deque(ts_list, maxlen=120)
            self._cached_data.update({
                'btc': btc_prices, 'eth': eth_prices, 'dom': dom_prices,
                'usdt_btc': usdt_btc, 'ts': ts_list
            })
            self._last_fetch_time = now
            return True
        except Exception as e:
            logger.error(f"Crypto refresh error: {e}")
            return False

    # ---------- Traditional data – fallback chain: Yahoo → Tiingo → Alpaca ----------
    def _fetch_yahoo_intraday(self, symbol, limit=120):
        """Fetch last 'limit' minutes of 1‑minute data from Yahoo Finance (UTC)."""
        mapping = {
            'SPY': 'SPY', 'QQQ': 'QQQ', 'DIA': 'DIA', 'GLD': 'GLD',
            'USO': 'USO', 'DXY': 'DX-Y.NYB', 'VIX': '^VIX'
        }
        yahoo_sym = mapping.get(symbol, symbol)
        try:
            url = f"https://query1.finance.yahoo.com/v8/finance/chart/{yahoo_sym}"
            params = {'interval': '1m', 'range': '2d'}
            resp = self._rate_limited_fetch(url, params=params)
            if resp and resp.status_code == 200:
                data = resp.json()
                result = data.get('chart', {}).get('result', [])
                if result:
                    timestamps = result[0].get('timestamp', [])
                    closes = result[0].get('indicators', {}).get('quote', [{}])[0].get('close', [])
                    if len(timestamps) > 0 and len(closes) > 0:
                        pairs = [(ts, c) for ts, c in zip(timestamps, closes) if c is not None]
                        if len(pairs) < limit/2:
                            logger.warning(f"Yahoo returned only {len(pairs)} points for {symbol}")
                            return None
                        pairs = pairs[-limit:]
                        return [[ts*1000, c] for ts, c in pairs]
            return None
        except Exception as e:
            logger.error(f"Yahoo error for {symbol}: {e}")
            return None

    def _fetch_tiingo_intraday(self, symbol, limit=120):
        """Fetch 1‑minute data from Tiingo (free tier: 1000 calls/day)."""
        if not TIINGO_TOKEN:
            return None
        try:
            # Tiingo's IEX endpoint for 1-minute bars (only for stocks/ETFs, not indices)
            if symbol in ['DXY', 'VIX']:
                return None
            url = f"https://api.tiingo.com/iex/{symbol}/prices"
            params = {'token': TIINGO_TOKEN, 'format': 'json', 'resampleFreq': '1min', 'startDate': '5d'}
            resp = self._rate_limited_fetch(url, params=params)
            if resp and resp.status_code == 200:
                data = resp.json()
                if data and len(data) > 0:
                    result = []
                    for item in data[-limit:]:
                        ts = int(datetime.datetime.fromisoformat(item['date'].replace('Z', '+00:00')).timestamp() * 1000)
                        price = item.get('close', 0)
                        if price:
                            result.append([ts, price])
                    return result
            return None
        except Exception as e:
            logger.error(f"Tiingo error for {symbol}: {e}")
            return None

    def _fetch_alpaca_intraday(self, symbol, limit=120):
        """Fetch 1‑minute data from Alpaca Markets (free tier: 200 calls/minute)."""
        if not ALPACA_API_KEY or not ALPACA_SECRET_KEY:
            return None
        if symbol in ['DXY', 'VIX']:
            return None
        headers = {
            'APCA-API-KEY-ID': ALPACA_API_KEY,
            'APCA-API-SECRET-KEY': ALPACA_SECRET_KEY
        }
        url = "https://data.alpaca.markets/v2/stocks/bars"
        params = {
            'symbols': symbol,
            'timeframe': '1Min',
            'limit': limit,
            'sort': 'asc'
        }
        try:
            resp = self._rate_limited_fetch(url, params=params, headers=headers)
            if resp and resp.status_code == 200:
                data = resp.json()
                bars = data.get('bars', {}).get(symbol, [])
                if bars:
                    result = []
                    for bar in bars:
                        ts = int(datetime.datetime.fromisoformat(bar['t'].replace('Z', '+00:00')).timestamp() * 1000)
                        result.append([ts, bar['c']])
                    return result[-limit:]
            return None
        except Exception as e:
            logger.error(f"Alpaca error for {symbol}: {e}")
            return None

    def _fetch_traditional_prices(self, symbol, limit=120):
        """Try multiple sources sequentially to get historical 1‑minute data."""
        # 1. Yahoo (primary)
        prices = self._fetch_yahoo_intraday(symbol, limit)
        if prices and len(prices) >= limit*0.8:
            return prices
        # 2. Tiingo (fallback) – only for stocks/ETFs
        if symbol not in ['DXY', 'VIX']:
            prices = self._fetch_tiingo_intraday(symbol, limit)
            if prices and len(prices) >= limit*0.8:
                return prices
        # 3. Alpaca (fallback) – only for stocks/ETFs
        if symbol not in ['DXY', 'VIX']:
            prices = self._fetch_alpaca_intraday(symbol, limit)
            if prices and len(prices) >= limit*0.8:
                return prices
        return None

    def _refresh_traditional_data(self):
        """Fetch 1‑minute history for all traditional assets in parallel."""
        trad_symbols = ['SPY', 'QQQ', 'DIA', 'GLD', 'USO', 'DXY', 'VIX']
        now = time.time()
        cache_ok = all(sym in self._cached_data for sym in ['spy','qqq','dia','gld','uso','dxy','vix']) and (now - self._last_fetch_time) < CACHE_TTL_SEC
        if cache_ok:
            with self._file_lock:
                self._spy_history.clear()
                self._spy_history.extend(self._cached_data['spy'])
                self._qqq_history.clear()
                self._qqq_history.extend(self._cached_data['qqq'])
                self._dia_history.clear()
                self._dia_history.extend(self._cached_data['dia'])
                self._gld_history.clear()
                self._gld_history.extend(self._cached_data['gld'])
                self._uso_history.clear()
                self._uso_history.extend(self._cached_data['uso'])
                self._dxy_history.clear()
                self._dxy_history.extend(self._cached_data['dxy'])
                self._vix_history.clear()
                self._vix_history.extend(self._cached_data['vix'])
            logger.info("Traditional data cache hit")
            return True

        logger.info("Fetching fresh traditional data in parallel...")
        new_data = {}
        with ThreadPoolExecutor(max_workers=len(trad_symbols)) as executor:
            future_to_sym = {executor.submit(self._fetch_traditional_prices, sym): sym for sym in trad_symbols}
            for future in as_completed(future_to_sym):
                sym = future_to_sym[future]
                try:
                    prices = future.result()
                    if prices:
                        new_data[sym.lower()] = [p[1] for p in prices]
                        if 'ts' not in new_data:
                            new_data['ts'] = [p[0] for p in prices]
                    else:
                        old = self._cached_data.get(sym.lower(), [])
                        new_data[sym.lower()] = old if old else []
                except Exception as e:
                    logger.error(f"Error fetching {sym}: {e}")

        if not new_data.get('ts'):
            now_ms = int(time.time()*1000)
            new_data['ts'] = [now_ms - i*60000 for i in range(120)]

        min_len = min(len(new_data.get('spy', [])), len(new_data.get('qqq', [])),
                      len(new_data.get('dia', [])), len(new_data.get('gld', [])),
                      len(new_data.get('uso', [])), len(new_data.get('dxy', [])),
                      len(new_data.get('vix', [])))
        if min_len < MIN_POINTS:
            logger.error("Not enough traditional data points")
            return False

        for k in ['spy','qqq','dia','gld','uso','dxy','vix']:
            if k in new_data:
                new_data[k] = new_data[k][-min_len:]

        with self._file_lock:
            self._spy_history = deque(new_data['spy'], maxlen=120)
            self._qqq_history = deque(new_data['qqq'], maxlen=120)
            self._dia_history = deque(new_data['dia'], maxlen=120)
            self._gld_history = deque(new_data['gld'], maxlen=120)
            self._uso_history = deque(new_data['uso'], maxlen=120)
            self._dxy_history = deque(new_data['dxy'], maxlen=120)
            self._vix_history = deque(new_data['vix'], maxlen=120)

        for k in ['spy','qqq','dia','gld','uso','dxy','vix']:
            self._cached_data[k] = list(new_data[k])
        self._cached_data['trad_ts'] = new_data.get('ts', [])
        logger.info(f"Traditional data refreshed: {min_len} points")
        return True

    # ---------- Combined indices refresh ----------
    def _refresh_indices_data(self):
        crypto_ok = self._refresh_crypto_data()
        if not crypto_ok:
            return False
        self._refresh_traditional_data()   # optional, don't fail
        self._last_fetch_time = time.time()
        return True

    # ========== FIXED: Get symbol prices – fallback to Binance API ==========
    def _get_symbol_prices(self, symbol, minutes=120, max_wait=30):
        sym = symbol.lower()
        # First try to read from local .toon file
        toon_path = os.path.join(self.symbols_dir, f"{sym}.toon")
        start_time = time.time()
        prices = []
        while time.time() - start_time < max_wait:
            if not os.path.exists(toon_path):
                time.sleep(1)
                continue
            try:
                with open(toon_path, 'r', encoding='utf-8') as f:
                    content = f.read()
                # Parse candles_1m array
                import re
                pattern = r'candles_1m\[\d+\]\{ts,dt,o,h,l,c,v\}:\s*\n(?:\s+([^\n]+(?:\n\s+[^\n]+)*))?'
                match = re.search(pattern, content, re.DOTALL)
                if not match:
                    time.sleep(1)
                    continue
                rows_text = match.group(1)
                if not rows_text:
                    time.sleep(1)
                    continue
                prices = []
                for row in rows_text.split(' | '):
                    parts = row.strip().split(',')
                    if len(parts) >= 6:
                        try:
                            prices.append(float(parts[5]))  # close price
                        except:
                            continue
                if len(prices) >= minutes:
                    return prices[-minutes:]
                time.sleep(1)
            except Exception as e:
                logger.error(f"Error reading {toon_path}: {e}")
                time.sleep(1)
        # If local read fails or insufficient, fallback to Binance API
        logger.warning(f"Local read insufficient for {symbol}, falling back to Binance API")
        return self._fetch_symbol_candles_from_api(symbol, minutes)

    def _fetch_symbol_candles_from_api(self, symbol, limit=120):
        """Fetch 1‑minute candles directly from Binance API."""
        url = f"{SPOT_BASE_URL}/klines"
        params = {"symbol": symbol.upper(), "interval": "1m", "limit": limit}
        data = self._rate_limited_fetch(url, params=params)
        if not data:
            return []
        candles = []
        for c in data:
            try:
                candles.append(float(c[4]))  # close price
            except:
                continue
        return candles

    # ---------- Other helper methods (unchanged) ----------
    @staticmethod
    def linear_interpolate(data):
        if not data or len(data) < 5:
            return data
        indices = [i for i, v in enumerate(data) if v != 0]
        if len(indices) < 2:
            return data
        interpolated = data[:]
        for i in range(len(data)):
            if data[i] == 0:
                left = max([idx for idx in indices if idx < i], default=None)
                right = min([idx for idx in indices if idx > i], default=None)
                if left is not None and right is not None:
                    ratio = (i - left) / (right - left)
                    interpolated[i] = data[left] + ratio * (data[right] - data[left])
                elif left is not None:
                    interpolated[i] = data[left]
                elif right is not None:
                    interpolated[i] = data[right]
        return interpolated

    @staticmethod
    def compute_correlation(x, y):
        n = min(len(x), len(y))
        if n < 2:
            return 0
        x = x[-n:]; y = y[-n:]
        mean_x = sum(x)/n; mean_y = sum(y)/n
        num = sum((x[i]-mean_x)*(y[i]-mean_y) for i in range(n))
        den = math.sqrt(sum((xi-mean_x)**2 for xi in x) * sum((yi-mean_y)**2 for yi in y))
        return round(num/den,4) if den != 0 else 0

    def _calc_corr_mom(self, price_slice, idx_prices, w):
        if len(price_slice) < w or len(idx_prices) < w:
            return 0,0
        corr = self.compute_correlation(price_slice[-w:], idx_prices[-w:])
        if len(price_slice) >= 2*w and len(idx_prices) >= 2*w:
            prev_prices = price_slice[-2*w:-w]
            prev_idx = idx_prices[-2*w:-w]
            corr_prev = self.compute_correlation(prev_prices, prev_idx)
            momentum = round((corr - corr_prev)/corr_prev*100,4) if corr_prev != 0 else 0
        else:
            momentum = 0
        return corr, momentum

    def _generate_fields(self, indices):
        fields = ["timestamp", "data_points"]
        for idx in indices.keys():
            for w in WINDOW_SIZES:
                fields.append(f"{idx}_corr_{w}m")
                fields.append(f"{idx}_momentum_{w}m")
                fields.append(f"{idx}_zscore_{w}m")
        return fields

    def _generate_row_values(self, price_slice, indices, timestamp, n_points, history_corrs=None):
        row = [str(timestamp), str(n_points)]
        for idx_name, idx_prices in indices.items():
            for w in WINDOW_SIZES:
                corr, momentum = self._calc_corr_mom(price_slice, idx_prices, w)
                if history_corrs and idx_name in history_corrs and w in history_corrs[idx_name]:
                    hist_vals = history_corrs[idx_name][w]
                    zscore = self.safe_zscore(corr, hist_vals, min_points=ZSCORE_MIN_POINTS)
                    if math.isnan(zscore):
                        zscore = 0.0
                    zscore = round(zscore, 4)
                else:
                    zscore = 0.0
                row.append(str(corr))
                row.append(str(momentum))
                row.append(str(zscore))
        return row

    @staticmethod
    def safe_zscore(corr, history, min_points=5):
        n = len(history)
        if n < min_points:
            return 0.0
        mean = sum(history)/n
        variance = sum((x-mean)**2 for x in history)/n
        if variance < 1e-8:
            return 0.0
        stddev = math.sqrt(variance)
        z = (corr - mean)/stddev
        return max(min(z, MAX_ZSCORE), -MAX_ZSCORE)

    def _load_historical_correlations(self, sym):
        if sym in self._corr_history:
            return
        filepath = os.path.join(self.symbols_dir, f"{sym}_correlation.toon")
        if not os.path.exists(filepath):
            self._corr_history[sym] = {}
            return
        try:
            with open(filepath, 'r') as f:
                content = f.read()
            import re
            pattern = r'correlation_data\[(\d+)\]\{([^}]+)\}:\s*\n(?:\s+([^\n]+(?:\n\s+[^\n]+)*))?'
            match = re.search(pattern, content, re.DOTALL)
            if not match:
                self._corr_history[sym] = {}
                return
            fields = match.group(2).split(',')
            rows_text = match.group(3)
            if not rows_text:
                self._corr_history[sym] = {}
                return
            indices = ["BTC", "ETH", "BTCDOM", "USDT_BTC", "SPY", "QQQ", "DIA", "GLD", "USO", "DXY", "VIX"]
            self._corr_history[sym] = {}
            for idx in indices:
                self._corr_history[sym][idx] = {}
                for w in WINDOW_SIZES:
                    self._corr_history[sym][idx][w] = deque(maxlen=ROLLING_WINDOW)
            for i, field in enumerate(fields):
                if field.endswith('_zscore_15m'):
                    prefix = field.replace('_zscore_15m', '')
                    if prefix in indices:
                        idx = prefix
                        w = 15
                        for row in rows_text.split(' | '):
                            parts = row.strip().split(',')
                            if len(parts) > i:
                                try:
                                    z = float(parts[i])
                                    self._corr_history[sym][idx][w].append(z)
                                except:
                                    pass
            logger.info(f"Loaded history for {sym} from TOON")
        except Exception as e:
            logger.error(f"Load history error: {e}")
            self._corr_history[sym] = {}

    def _atomic_write_toon(self, filepath, fields, rows):
        dirname = os.path.dirname(filepath)
        os.makedirs(dirname, exist_ok=True)
        temp_path = filepath + ".tmp"
        try:
            with open(temp_path, 'w', encoding='utf-8') as f:
                f.write(f"# Correlation data – TOON format (crypto + traditional assets)\n")
                f.write(f"generated: {datetime.datetime.now().isoformat()}\n")
                f.write(f"\ncorrelation_data[{len(rows)}]{{{','.join(fields)}}}:\n")
                if rows:
                    line_rows = []
                    for row_vals in rows:
                        line_rows.append(','.join(str(v) for v in row_vals))
                    f.write("  " + " |\n  ".join(line_rows) + "\n")
                else:
                    f.write("  \n")
                f.write("\n# ========== END OF TOON DATA ==========\n")
                f.flush()
                os.fsync(f.fileno())
            os.rename(temp_path, filepath)
            return True
        except Exception as e:
            if os.path.exists(temp_path):
                try:
                    os.remove(temp_path)
                except:
                    pass
            self._log_issue("TOON_WRITE_ERROR", f"{filepath}: {e}", level=logging.ERROR)
            return False

    def _read_toon_rows(self, filepath):
        if not os.path.exists(filepath):
            return None, []
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                content = f.read()
            import re
            pattern = r'correlation_data\[(\d+)\]\{([^}]+)\}:\s*\n(?:\s+([^\n]+(?:\n\s+[^\n]+)*))?'
            match = re.search(pattern, content, re.DOTALL)
            if not match:
                return None, []
            fields = match.group(2).split(',')
            rows_text = match.group(3)
            rows = []
            if rows_text:
                for row_str in rows_text.split(' | '):
                    parts = row_str.strip().split(',')
                    if len(parts) == len(fields):
                        rows.append(parts)
            return fields, rows
        except Exception as e:
            self._log_issue("READ_TOON_ERROR", f"{filepath}: {e}", level=logging.WARNING)
            return None, []

    def collect_and_save(self, symbol):
        overall_start = time.time()
        print(f"[TRACE] collect_and_save started for {symbol} (TOON mode with traditional assets)")

        if not self._refresh_indices_data():
            self._save_partial(symbol, "Indices fetch failed")
            return False

        # Get symbol prices – this now has fallback to API
        prices = self._get_symbol_prices(symbol, minutes=120)
        if not prices or len(prices) < MIN_POINTS:
            self._save_partial(symbol, f"Only {len(prices)} points, need {MIN_POINTS}")
            return False

        with self._file_lock:
            n = min(len(prices), len(self._btc_history), len(self._eth_history),
                    len(self._btc_dom_history), len(self._usdt_btc_history),
                    len(self._spy_history), len(self._qqq_history), len(self._dia_history),
                    len(self._gld_history), len(self._uso_history), len(self._dxy_history),
                    len(self._vix_history))
            if n < MIN_POINTS:
                self._save_partial(symbol, f"Only {n} points, need {MIN_POINTS}")
                return False

            price_last = prices[-n:]
            indices = {
                "BTC": list(self._btc_history)[-n:],
                "ETH": list(self._eth_history)[-n:],
                "BTCDOM": list(self._btc_dom_history)[-n:],
                "USDT_BTC": list(self._usdt_btc_history)[-n:],
                "SPY": list(self._spy_history)[-n:],
                "QQQ": list(self._qqq_history)[-n:],
                "DIA": list(self._dia_history)[-n:],
                "GLD": list(self._gld_history)[-n:],
                "USO": list(self._uso_history)[-n:],
                "DXY": list(self._dxy_history)[-n:],
                "VIX": list(self._vix_history)[-n:]
            }
            timestamp = int(time.time() * 1000)
            filepath = os.path.join(self.symbols_dir, f"{symbol.lower()}_correlation.toon")
            fields = self._generate_fields(indices)

            existing_fields, existing_rows = self._read_toon_rows(filepath)
            save_success = False

            if not existing_rows or len(existing_rows) == 0:
                max_rows = min(30, n)
                if max_rows < 1:
                    self._save_partial(symbol, "Not enough data for 1 row")
                    return False
                print(f"[TRACE] First fill: generating {max_rows} rows")
                rows = []
                temp_history = {idx: {w: deque(maxlen=ROLLING_WINDOW) for w in WINDOW_SIZES} for idx in indices}
                for i in range(max_rows-1, -1, -1):
                    slice_end = n - i
                    if slice_end < 1:
                        break
                    price_slice = price_last[:slice_end]
                    indices_slice = {k: v[:slice_end] for k, v in indices.items()}
                    ts = timestamp - i * 60000
                    history_corrs = {}
                    for idx_name in indices_slice:
                        history_corrs[idx_name] = {}
                        for w in WINDOW_SIZES:
                            hist_vals = list(temp_history[idx_name][w])
                            history_corrs[idx_name][w] = hist_vals if len(hist_vals) >= 5 else []
                    row_vals = self._generate_row_values(price_slice, indices_slice, ts, slice_end, history_corrs)
                    rows.append(row_vals)
                    for idx_name, idx_prices in indices_slice.items():
                        for w in WINDOW_SIZES:
                            corr, _ = self._calc_corr_mom(price_slice, idx_prices, w)
                            temp_history[idx_name][w].append(corr)
                rows.reverse()
                if self._atomic_write_toon(filepath, fields, rows):
                    _, new_rows = self._read_toon_rows(filepath)
                    if new_rows and len(new_rows) >= 2:
                        save_success = True
                        print(f"[TIMING] Wrote {len(rows)} rows to TOON")
                    else:
                        self._log_issue("VERIFY_FAIL", f"After write, got {len(new_rows) if new_rows else 0} rows", level=logging.WARNING)
                else:
                    self._log_issue("SAVE_ERROR", f"First fill for {symbol}: atomic write failed", level=logging.ERROR)
                    return False
            else:
                print(f"[TRACE] Append mode: adding 1 row")
                self._load_historical_correlations(symbol)
                for idx_name in indices:
                    if idx_name not in self._corr_history.get(symbol, {}):
                        if symbol not in self._corr_history:
                            self._corr_history[symbol] = {}
                        self._corr_history[symbol][idx_name] = {}
                    for w in WINDOW_SIZES:
                        if w not in self._corr_history[symbol][idx_name]:
                            self._corr_history[symbol][idx_name][w] = deque(maxlen=ROLLING_WINDOW)
                current_corrs = {}
                for idx_name, idx_prices in indices.items():
                    current_corrs[idx_name] = {}
                    for w in WINDOW_SIZES:
                        corr, _ = self._calc_corr_mom(price_last, idx_prices, w)
                        current_corrs[idx_name][w] = corr
                history_corrs = {}
                for idx_name in indices:
                    history_corrs[idx_name] = {}
                    for w in WINDOW_SIZES:
                        hist = list(self._corr_history[symbol][idx_name][w])
                        history_corrs[idx_name][w] = hist if len(hist) >= 5 else []
                new_row_vals = self._generate_row_values(price_last, indices, timestamp, n, history_corrs)
                for idx_name in indices:
                    for w in WINDOW_SIZES:
                        self._corr_history[symbol][idx_name][w].append(current_corrs[idx_name][w])
                new_rows_list = existing_rows.copy()
                new_rows_list.append(new_row_vals)
                if len(new_rows_list) > MAX_ROWS:
                    new_rows_list = new_rows_list[-MAX_ROWS:]
                if self._atomic_write_toon(filepath, fields, new_rows_list):
                    save_success = True
                    print(f"[TIMING] Appended row to TOON, now {len(new_rows_list)} rows")
                else:
                    self._log_issue("APPEND_ERROR", f"Append for {symbol}: atomic write failed", level=logging.ERROR)
                    return False

        if save_success:
            try:
                _, rows = self._read_toon_rows(filepath)
                if len(rows) >= 2:
                    overall_elapsed = time.time() - overall_start
                    print(f"[TIMING] Total time: {overall_elapsed:.2f}s")
                    self._log_issue("SAVE_SUCCESS", f"{symbol}: {len(rows)} rows saved in TOON", level=logging.INFO)
                    return True
                else:
                    self._log_issue("VERIFY_FAIL", f"After save, only {len(rows)} rows", level=logging.WARNING)
            except Exception as e:
                self._log_issue("VERIFY_ERROR", f"Could not verify file: {e}", level=logging.WARNING)
        return False

    def _save_partial(self, symbol, error):
        filepath = os.path.join(self.symbols_dir, f"{symbol}_correlation_errors.tsv")
        timestamp = int(time.time()*1000)
        new_line = f"{timestamp}\t{error}\n"
        if not os.path.exists(filepath):
            with open(filepath, 'w') as f:
                f.write("timestamp\terror\n" + new_line)
        else:
            with open(filepath, 'a') as f:
                f.write(new_line)
        self._log_issue("PARTIAL_SAVE", f"{symbol}: {error}", level=logging.WARNING)

    def stop(self):
        self._running = False
        self._session.close()