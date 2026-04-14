# Groups/group_x/X09_correlation_rest.py
"""
KALI PORT - Correlation Module (Fixed for File Save)
- Crypto: BTC, ETH, BTCDOM, USDT_BTC
- Macro: XAUUSD (Gold), USOIL (Crude Oil)
- 15m windows only
"""

import requests
import time
import os
import threading
import math
import logging
import traceback
from collections import deque
import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

FUTURES_BASE_URL = "https://fapi.binance.com"
SPOT_BASE_URL = "https://api.binance.com/api/v3"

REFRESH_INTERVAL = 30
MIN_POINTS = 30
WINDOW_SIZES = [15]
MAX_ROWS = 100
ROLLING_WINDOW = 30
RATE_LIMIT_SEC = 0.5
MAX_ZSCORE = 5.0
API_TIMEOUT = 8

TWELVEDATA_API_KEY = os.getenv("TWELVEDATA_API_KEY", "")
TWELVEDATA_BASE_URL = "https://api.twelvedata.com"

MACRO_MAP = {
    "XAUUSD": "XAU/USD",
    "USOIL": "WTI",
}
MACRO_SYMBOLS = list(MACRO_MAP.keys())


class CorrelationRest:
    def __init__(self, base_dir):
        self.base_dir = base_dir
        self.symbols_dir = os.path.join(base_dir, "symbols")
        os.makedirs(self.symbols_dir, exist_ok=True)
        self._file_lock = threading.Lock()
        self._running = True
        self._last_api_call = 0

        self._btc_history = deque(maxlen=120)
        self._eth_history = deque(maxlen=120)
        self._btc_dom_history = deque(maxlen=120)
        self._usdt_btc_history = deque(maxlen=120)
        self._timestamp_history = deque(maxlen=120)

        self._macro_histories = {}
        self._macro_last_fetch = 0
        self._macro_cache_ttl = 300
        self.macro_enabled = False

        self._corr_history = {}

        self._session = requests.Session()
        
        self._pre_fill_history()
        self._refresh_thread = threading.Thread(target=self._refresh_loop, daemon=True)
        self._refresh_thread.start()
        logger.info("X09_correlation_rest initialized")

    def __del__(self):
        self._session.close()

    @staticmethod
    def detect_outliers(data, threshold=2):
        if len(data) < 2:
            return []
        mean = sum(data) / len(data)
        variance = sum((x - mean) ** 2 for x in data) / len(data)
        stddev = math.sqrt(variance) if variance > 0 else 1e-9
        return [x for x in data if abs(x - mean) > threshold * stddev]

    @staticmethod
    def replace_outliers_with_median(data, threshold=2):
        if len(data) < 2:
            return data
        mean = sum(data) / len(data)
        variance = sum((x - mean) ** 2 for x in data) / len(data)
        stddev = math.sqrt(variance) if variance > 0 else 1e-9
        non_outliers = [x for x in data if abs(x - mean) <= threshold * stddev]
        if not non_outliers:
            return data
        median = sorted(non_outliers)[len(non_outliers)//2]
        return [median if abs(x - mean) > threshold * stddev else x for x in data]

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
    def safe_zscore(corr, history, min_points=5):
        n = len(history)
        if n < min_points:
            return 0.0
        mean = sum(history) / n
        variance = sum((x - mean) ** 2 for x in history) / n
        if variance < 1e-8:
            if abs(corr - mean) > 1e-6:
                return MAX_ZSCORE if corr > mean else -MAX_ZSCORE
            else:
                return 0.0
        stddev = math.sqrt(variance)
        z = (corr - mean) / stddev
        z = max(min(z, MAX_ZSCORE), -MAX_ZSCORE)
        return round(z, 4)

    def _rate_limited_fetch(self, url, params=None, timeout=API_TIMEOUT):
        now = time.time()
        elapsed = now - self._last_api_call
        if elapsed < RATE_LIMIT_SEC:
            time.sleep(RATE_LIMIT_SEC - elapsed)
        self._last_api_call = time.time()
        try:
            r = self._session.get(url, params=params, timeout=timeout)
            if r.status_code != 200:
                logger.error(f"API request failed: {r.status_code}")
                return None
            return r
        except requests.exceptions.Timeout:
            logger.error(f"API timeout: {url}")
            return None
        except Exception as e:
            logger.error(f"Request error: {e}")
            return None

    def _fetch_price(self, symbol, spot=True):
        url = f"{SPOT_BASE_URL}/ticker/price" if spot else f"{FUTURES_BASE_URL}/fapi/v1/ticker/price"
        r = self._rate_limited_fetch(url, params={"symbol": symbol}, timeout=5)
        if r and r.status_code == 200:
            try:
                return float(r.json().get('price', 0))
            except:
                pass
        with self._file_lock:
            if symbol == "BTCUSDT" and self._btc_history:
                return self._btc_history[-1]
            elif symbol == "ETHUSDT" and self._eth_history:
                return self._eth_history[-1]
            elif symbol == "BTCDOMUSDT" and self._btc_dom_history:
                return self._btc_dom_history[-1]
        return 0.0

    def _pre_fill_history(self):
        self._refresh_crypto_data()
        self._fetch_macro_data_cached()

    def _fetch_macro_data_cached(self):
        now = time.time()
        
        if self._macro_histories and (now - self._macro_last_fetch) < self._macro_cache_ttl:
            logger.info("[TRACE] Using cached macro data")
            return
        
        print("[TRACE] Fetching fresh macro data...")
        self.macro_enabled = False
        self._macro_histories = {}
        
        for sym in MACRO_SYMBOLS:
            try:
                print(f"[TRACE] Fetching {sym}...")
                timestamps, closes = self._fetch_twelvedata_macro(sym, minutes=120)
                if closes and len(closes) >= 10:
                    self._macro_histories[sym] = deque(closes, maxlen=120)
                    print(f"[TRACE] {sym} got {len(closes)} points")
                    self.macro_enabled = True
                else:
                    print(f"[TRACE] {sym} insufficient data ({len(closes)} points)")
            except Exception as e:
                print(f"[TRACE] ERROR fetching {sym}: {e}")
        
        self._macro_last_fetch = now
        if self.macro_enabled:
            print("[TRACE] Macro enabled")
        else:
            print("[TRACE] Macro disabled")

    def _fetch_twelvedata_macro(self, symbol, minutes=120):
        if not TWELVEDATA_API_KEY:
            return [], []
        
        ticker = MACRO_MAP.get(symbol.upper())
        if not ticker:
            return [], []
        
        url = f"{TWELVEDATA_BASE_URL}/time_series"
        params = {
            "symbol": ticker,
            "interval": "1min",
            "outputsize": minutes,
            "apikey": TWELVEDATA_API_KEY,
        }
        
        try:
            r = self._session.get(url, params=params, timeout=API_TIMEOUT)
            if r.status_code != 200:
                print(f"Twelve Data error for {symbol}: HTTP {r.status_code}")
                return [], []
            
            data = r.json()
            if "values" not in data:
                msg = data.get('message', 'no values')
                print(f"Twelve Data error for {symbol}: {msg}")
                return [], []
            
            values = data["values"]
            if not values:
                return [], []
            
            timestamps = []
            closes = []
            for v in reversed(values):
                try:
                    dt = datetime.datetime.fromisoformat(v["datetime"])
                    ts_ms = int(dt.timestamp() * 1000)
                    close = float(v["close"])
                    if close > 0:
                        timestamps.append(ts_ms)
                        closes.append(close)
                except (KeyError, ValueError):
                    continue
            
            timestamps = timestamps[-minutes:]
            closes = closes[-minutes:]
            return timestamps, closes
            
        except requests.exceptions.Timeout:
            print(f"Twelve Data TIMEOUT for {symbol}")
            return [], []
        except Exception as e:
            print(f"Twelve Data exception for {symbol}: {e}")
            return [], []

    def _refresh_loop(self):
        while self._running:
            try:
                self.update_global_data()
            except Exception as e:
                logger.error(f"Refresh loop error: {e}")
            time.sleep(REFRESH_INTERVAL)

    def update_global_data(self):
        btc = self._fetch_price("BTCUSDT", spot=True)
        eth = self._fetch_price("ETHUSDT", spot=True)
        btc_dom = self._fetch_price("BTCDOMUSDT", spot=False)
        usdt_btc = 1.0 / btc if btc > 0 else 0
        now_ms = int(time.time() * 1000)
        
        with self._file_lock:
            self._timestamp_history.append(now_ms)
            self._btc_history.append(btc)
            self._eth_history.append(eth)
            self._btc_dom_history.append(btc_dom)
            self._usdt_btc_history.append(usdt_btc)
        
        logger.debug(f"Updated: BTC={btc:.0f} ETH={eth:.0f} DOM={btc_dom:.2f}%")

    def _get_symbol_prices(self, symbol, minutes=120):
        sym = symbol.lower()
        filepath = os.path.join(self.symbols_dir, f"{sym}.tsv")
        if not os.path.exists(filepath):
            return []
        try:
            prices = []
            with open(filepath, 'r') as f:
                for line in f:
                    parts = line.strip().split('\t')
                    if len(parts) >= 5:
                        try:
                            prices.append(float(parts[4]))
                        except:
                            continue
            if len(prices) < 5:
                return prices
            prices = self.linear_interpolate(prices)
            if len(prices) >= minutes:
                return prices[-minutes:]
            return prices
        except Exception as e:
            logger.error(f"Error reading {sym}: {e}")
            return []

    def compute_correlation(self, x, y):
        n = min(len(x), len(y))
        if n < 2:
            return 0
        x = x[-n:]
        y = y[-n:]
        mean_x = sum(x) / n
        mean_y = sum(y) / n
        num = sum((x[i] - mean_x) * (y[i] - mean_y) for i in range(n))
        den = math.sqrt(sum((xi - mean_x)**2 for xi in x) * sum((yi - mean_y)**2 for yi in y))
        return round(num / den, 4) if den != 0 else 0

    def _calc_corr_mom(self, price_slice, idx_prices, w):
        if len(price_slice) < w or len(idx_prices) < w:
            return 0, 0
        corr = self.compute_correlation(price_slice[-w:], idx_prices[-w:])
        if len(price_slice) >= 2*w and len(idx_prices) >= 2*w:
            prev_prices = price_slice[-2*w:-w]
            prev_idx = idx_prices[-2*w:-w]
            corr_prev = self.compute_correlation(prev_prices, prev_idx)
            momentum = round((corr - corr_prev) / corr_prev * 100, 4) if corr_prev != 0 else 0
        else:
            momentum = 0
        return corr, momentum

    def _load_historical_correlations(self, sym):
        if sym in self._corr_history:
            return
        filepath = os.path.join(self.symbols_dir, f"{sym}_correlation.tsv")
        if not os.path.exists(filepath):
            self._corr_history[sym] = {}
            return
        
        try:
            with open(filepath, 'r') as f:
                lines = f.readlines()
            if len(lines) < 2:
                self._corr_history[sym] = {}
                return
            
            header = lines[0].strip().split('\t')
            indices = ["BTC", "ETH", "BTCDOM", "USDT_BTC"]
            if self.macro_enabled:
                indices += MACRO_SYMBOLS
            
            col_map = {}
            for idx in indices:
                for w in WINDOW_SIZES:
                    col_name = f"{idx}_corr_{w}m"
                    if col_name in header:
                        col_map[(idx, w)] = header.index(col_name)
            
            self._corr_history[sym] = {}
            for idx in indices:
                self._corr_history[sym][idx] = {}
                for w in WINDOW_SIZES:
                    self._corr_history[sym][idx][w] = deque(maxlen=ROLLING_WINDOW)
            
            recent_lines = lines[-50:] if len(lines) > 50 else lines[1:]
            for line in recent_lines:
                parts = line.strip().split('\t')
                for (idx, w), col_idx in col_map.items():
                    if len(parts) > col_idx:
                        try:
                            val = float(parts[col_idx])
                            self._corr_history[sym][idx][w].append(val)
                        except:
                            pass
            
            logger.info(f"Loaded history for {sym} ({len(recent_lines)} rows)")
        except Exception as e:
            logger.error(f"Load history error: {e}")
            self._corr_history[sym] = {}

    def _generate_row(self, price_slice, indices, timestamp, n_points, history_corrs=None):
        row = [str(timestamp), str(n_points)]
        for idx_name, idx_prices in indices.items():
            for w in WINDOW_SIZES:
                corr, momentum = self._calc_corr_mom(price_slice, idx_prices, w)
                if history_corrs and idx_name in history_corrs and w in history_corrs[idx_name]:
                    hist_vals = history_corrs[idx_name][w]
                    zscore = self.safe_zscore(corr, hist_vals, min_points=2)
                    if math.isnan(zscore):
                        zscore = 0.0
                    zscore = round(zscore, 4)
                else:
                    zscore = 0.0
                row.append(str(corr))
                row.append(str(momentum))
                row.append(str(zscore))
        return row

    def _generate_headers(self, indices):
        headers = ["timestamp", "data_points"]
        for idx in indices.keys():
            for w in WINDOW_SIZES:
                headers.append(f"{idx}_corr_{w}m")
                headers.append(f"{idx}_momentum_{w}m")
                headers.append(f"{idx}_zscore_{w}m")
        return headers

    def _trim_file(self, filepath, max_rows=MAX_ROWS):
        try:
            with self._file_lock:
                with open(filepath, 'r') as f:
                    lines = f.readlines()
                if len(lines) > max_rows + 1:
                    with open(filepath, 'w') as f:
                        f.write(lines[0])
                        f.writelines(lines[-max_rows:])
        except Exception as e:
            logger.error(f"Trim error: {e}")

    def _refresh_crypto_data(self):
        try:
            url_btc = f"{SPOT_BASE_URL}/klines"
            url_eth = f"{SPOT_BASE_URL}/klines"
            url_dom = f"{FUTURES_BASE_URL}/fapi/v1/klines"
            
            r_btc = self._rate_limited_fetch(url_btc, params={"symbol": "BTCUSDT", "interval": "1m", "limit": 120})
            r_eth = self._rate_limited_fetch(url_eth, params={"symbol": "ETHUSDT", "interval": "1m", "limit": 120})
            r_dom = self._rate_limited_fetch(url_dom, params={"symbol": "BTCDOMUSDT", "interval": "1m", "limit": 120})
            
            if not (r_btc and r_eth and r_dom):
                logger.error("Crypto fetch failed")
                return False
            
            if not all(r.status_code == 200 for r in [r_btc, r_eth, r_dom]):
                logger.error("Crypto API returned non-200 status")
                return False
            
            try:
                btc_data = r_btc.json()
                eth_data = r_eth.json()
                dom_data = r_dom.json()
            except ValueError as e:
                logger.error(f"Error parsing crypto JSON: {e}")
                return False
            
            with self._file_lock:
                self._btc_history.clear()
                self._eth_history.clear()
                self._btc_dom_history.clear()
                self._usdt_btc_history.clear()
                self._timestamp_history.clear()
                
                min_len = min(len(btc_data), len(eth_data), len(dom_data))
                
                for i in range(min_len):
                    try:
                        btc_price = float(btc_data[i][4])
                        eth_price = float(eth_data[i][4])
                        dom_price = float(dom_data[i][4])
                        
                        if btc_price <= 0:
                            continue
                        
                        self._btc_history.append(btc_price)
                        self._eth_history.append(eth_price)
                        self._btc_dom_history.append(dom_price)
                        self._usdt_btc_history.append(1.0 / btc_price)
                        self._timestamp_history.append(btc_data[i][0])
                    except (IndexError, ValueError):
                        continue
            
            logger.info(f"Crypto refreshed: {len(self._btc_history)} points")
            return True
            
        except Exception as e:
            logger.error(f"Crypto refresh error: {e}")
            return False

    def _save_partial(self, symbol, error):
        filepath = os.path.join(self.symbols_dir, f"{symbol}_correlation.tsv")
        try:
            with self._file_lock:
                with open(filepath, 'w') as f:
                    f.write("timestamp\terror\tnote\n")
                    f.write(f"{int(time.time()*1000)}\t{error}\tInsufficient data\n")
            logger.info(f"Saved partial for {symbol}")
        except Exception as e:
            logger.error(f"Partial save error: {e}")

    def collect_and_save(self, symbol):
        """
        Main method with PROPER FILE HANDLING.
        """
        print(f"[TRACE] collect_and_save started for {symbol}")
        start_time = time.time()
        
        # Step A: Fetch crypto
        print("[TRACE] Fetching crypto data...")
        if not self._refresh_crypto_data():
            self._save_partial(symbol, "Crypto fetch failed")
            return
        
        # Step B: Fetch macro (cached)
        print("[TRACE] Checking macro data...")
        self._fetch_macro_data_cached()
        
        # Step C: Get symbol prices
        sym = symbol.lower()
        prices = self._get_symbol_prices(sym, minutes=120)
        
        if not prices:
            self._save_partial(sym, "No price data")
            return
        
        # Step D: Calculate and save
        with self._file_lock:
            n = min(len(prices), len(self._btc_history), len(self._eth_history),
                    len(self._btc_dom_history), len(self._usdt_btc_history))
            
            active_macros = []
            if self.macro_enabled:
                for m in MACRO_SYMBOLS:
                    if m in self._macro_histories and len(self._macro_histories[m]) >= MIN_POINTS:
                        n = min(n, len(self._macro_histories[m]))
                        active_macros.append(m)
            
            print(f"[TRACE] Using n={n} points (macros: {active_macros})")
            
            if n < MIN_POINTS:
                self._save_partial(sym, f"Only {n} points, need {MIN_POINTS}")
                return
            
            # Prepare data
            price_last = prices[-n:]
            indices = {
                "BTC": list(self._btc_history)[-n:],
                "ETH": list(self._eth_history)[-n:],
                "BTCDOM": list(self._btc_dom_history)[-n:],
                "USDT_BTC": list(self._usdt_btc_history)[-n:],
            }
            
            for m in active_macros:
                indices[m] = list(self._macro_histories[m])[-n:]
            
            timestamp = int(time.time() * 1000)
            filepath = os.path.join(self.symbols_dir, f"{sym}_correlation.tsv")
            headers = self._generate_headers(indices)
            
            # ===== CRITICAL FIX: Proper file existence check =====
            file_exists = os.path.exists(filepath)
            file_has_data = False
            
            if file_exists:
                try:
                    size = os.path.getsize(filepath)
                    file_has_data = size > 100  # At least header + some data
                except:
                    file_has_data = False
            
            print(f"[TRACE] File exists: {file_exists}, has_data: {file_has_data}")
            
            if not file_has_data:
                # ========== FIRST FILL ==========
                max_rows = min(30, n)
                if max_rows < 1:
                    self._save_partial(sym, "Not enough data for 1 row")
                    return
                
                print(f"[TRACE] First fill: generating {max_rows} rows")
                rows = []
                temp_history = {idx: {w: deque(maxlen=ROLLING_WINDOW) for w in WINDOW_SIZES} 
                              for idx in indices.keys()}
                
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
                    
                    row = self._generate_row(price_slice, indices_slice, ts, slice_end, history_corrs)
                    rows.append(row)
                    
                    for idx_name, idx_prices in indices_slice.items():
                        for w in WINDOW_SIZES:
                            corr, _ = self._calc_corr_mom(price_slice, idx_prices, w)
                            temp_history[idx_name][w].append(corr)
                
                # WRITE FILE (with proper error handling)
                try:
                    with open(filepath, 'w') as f:
                        f.write("\t".join(headers) + "\n")
                        for row in rows:
                            f.write("\t".join(row) + "\n")
                    
                    elapsed = time.time() - start_time
                    print(f"[TRACE] ✅ SAVED {len(rows)} rows to {filepath} in {elapsed:.2f}s")
                    
                    # Verify
                    if os.path.exists(filepath):
                        verify_size = os.path.getsize(filepath)
                        print(f"[TRACE] Verified: file exists, size={verify_size} bytes")
                    
                    self._load_historical_correlations(sym)
                    
                except Exception as e:
                    print(f"[TRACE] ❌ SAVE ERROR: {e}")
                    traceback.print_exc()
                    return
                
            else:
                # ========== APPEND MODE ==========
                print(f"[TRACE] Append mode: adding 1 row")
                self._load_historical_correlations(sym)
                
                for idx_name in indices:
                    if idx_name not in self._corr_history.get(sym, {}):
                        if sym not in self._corr_history:
                            self._corr_history[sym] = {}
                        self._corr_history[sym][idx_name] = {}
                    for w in WINDOW_SIZES:
                        if w not in self._corr_history[sym][idx_name]:
                            self._corr_history[sym][idx_name][w] = deque(maxlen=ROLLING_WINDOW)
                
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
                        hist = list(self._corr_history[sym][idx_name][w])
                        history_corrs[idx_name][w] = hist if len(hist) >= 5 else []
                
                row = self._generate_row(price_last, indices, timestamp, n, history_corrs)
                
                for idx_name in indices:
                    for w in WINDOW_SIZES:
                        self._corr_history[sym][idx_name][w].append(current_corrs[idx_name][w])
                
                try:
                    with open(filepath, 'a') as f:
                        f.write("\t".join(row) + "\n")
                    
                    self._trim_file(filepath, MAX_ROWS)
                    elapsed = time.time() - start_time
                    print(f"[TRACE] ✅ APPENDED row to {filepath} in {elapsed:.2f}s")
                    
                except Exception as e:
                    print(f"[TRACE] ❌ APPEND ERROR: {e}")
                    traceback.print_exc()
                    return

    def stop(self):
        self._running = False
        self._session.close()
