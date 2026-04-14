# Groups/group_x/X07_derivative_rest.py
import requests
import json
import time
import os
import threading
import math
from collections import deque, defaultdict

FUTURES_BASE_URL = "https://fapi.binance.com"
SPOT_BASE_URL = "https://api.binance.com/api/v3"

class DerivativeRest:
    def __init__(self, base_dir):
        self.base_dir = base_dir
        self.symbols_dir = os.path.join(base_dir, "symbols")
        os.makedirs(self.symbols_dir, exist_ok=True)
        self._file_lock = threading.Lock()
        self._running = True
        self._liquidation_history = {}
        self._oi_history = {}
        self._ls_history = {}
        self._price_history = {}
        self._max_history = 1000
        print("[X07_derivative_rest] Fixed version: better liquidation fetching")

    # ---------- API calls ----------
    def fetch_open_interest(self, symbol):
        try:
            r = requests.get(f"{FUTURES_BASE_URL}/fapi/v1/openInterest",
                             params={"symbol": symbol.upper()}, timeout=10)
            r.raise_for_status()
            return float(r.json().get('openInterest', 0))
        except Exception as e:
            print(f"[X07_derivative_rest] OI error: {e}")
            return 0

    def fetch_oi_history(self, symbol, period="5m", limit=24):
        try:
            r = requests.get(f"{FUTURES_BASE_URL}/futures/data/openInterestHist",
                             params={"symbol": symbol.upper(), "period": period, "limit": limit}, timeout=10)
            r.raise_for_status()
            data = r.json()
            history = []
            for item in data:
                history.append({
                    'timestamp': item.get('timestamp', 0),
                    'value': float(item.get('sumOpenInterest', 0))
                })
            return history
        except Exception as e:
            print(f"[X07_derivative_rest] OI history error: {e}")
            return []

    def fetch_long_short_ratio_history(self, symbol, period="5m", limit=24):
        try:
            r = requests.get(f"{FUTURES_BASE_URL}/futures/data/topLongShortAccountRatio",
                             params={"symbol": symbol.upper(), "period": period, "limit": limit}, timeout=10)
            r.raise_for_status()
            data = r.json()
            history = []
            for item in data:
                history.append({
                    'timestamp': item.get('timestamp', 0),
                    'long_short_ratio': float(item.get('longShortRatio', 0)),
                    'long_account': float(item.get('longAccount', 0)),
                    'short_account': float(item.get('shortAccount', 0))
                })
            return history
        except Exception as e:
            print(f"[X07_derivative_rest] L/S history error: {e}")
            return []

    def fetch_funding_rate_history(self, symbol, limit=24):
        try:
            r = requests.get(f"{FUTURES_BASE_URL}/fapi/v1/fundingRate",
                             params={"symbol": symbol.upper(), "limit": limit}, timeout=10)
            r.raise_for_status()
            data = r.json()
            history = []
            for item in data:
                history.append({
                    'timestamp': item.get('fundingTime', 0),
                    'funding_rate': float(item.get('fundingRate', 0))
                })
            return history
        except Exception as e:
            print(f"[X07_derivative_rest] Funding history error: {e}")
            return []

    def fetch_spot_price(self, symbol):
        try:
            r = requests.get(f"{SPOT_BASE_URL}/ticker/price",
                             params={"symbol": symbol.upper()}, timeout=10)
            r.raise_for_status()
            return float(r.json().get('price', 0))
        except Exception as e:
            print(f"[X07_derivative_rest] Spot price error: {e}")
            return 0

    def fetch_current_funding_and_mark(self, symbol):
        try:
            r = requests.get(f"{FUTURES_BASE_URL}/fapi/v1/premiumIndex",
                             params={"symbol": symbol.upper()}, timeout=10)
            r.raise_for_status()
            data = r.json()
            return {
                'funding_rate': float(data.get('lastFundingRate', 0)),
                'mark_price': float(data.get('markPrice', 0)),
                'index_price': float(data.get('indexPrice', 0))
            }
        except Exception as e:
            print(f"[X07_derivative_rest] Funding error: {e}")
            return {'funding_rate': 0, 'mark_price': 0, 'index_price': 0}

    def fetch_liquidations_history(self, symbol, minutes=60, limit=100, min_quantity=1.0, retries=3):
        """
        Fetch liquidations with reduced default minutes and limit to avoid 400 errors.
        """
        for attempt in range(retries):
            try:
                now_ms = int(time.time() * 1000)
                start_ms = now_ms - minutes * 60 * 1000
                url = f"{FUTURES_BASE_URL}/fapi/v1/allForceOrders"
                params = {"symbol": symbol.upper(), "limit": min(limit, 100)}
                print(f"[X07_derivative_rest] Fetching liquidations for {symbol} (attempt {attempt+1})")
                r = requests.get(url, params=params, timeout=10)
                if r.status_code == 200:
                    data = r.json()
                    recent = []
                    for item in data:
                        ts = item.get('time', 0)
                        qty = float(item.get('origQty', 0))
                        if ts >= start_ms and qty >= min_quantity:
                            liquidation = {
                                'timestamp': ts,
                                'price': float(item.get('price', 0)),
                                'quantity': qty,
                                'side': item.get('side', '')
                            }
                            recent.append(liquidation)
                    recent.sort(key=lambda x: x['timestamp'])
                    print(f"[X07_derivative_rest] Fetched {len(recent)} liquidation events for {symbol} (last {minutes} min)")
                    return recent
                else:
                    print(f"[X07_derivative_rest] HTTP {r.status_code} for liquidations: {r.text[:200]}")
            except Exception as e:
                print(f"[X07_derivative_rest] Liquidations fetch error (attempt {attempt+1}): {e}")
            if attempt < retries - 1:
                time.sleep(2)
        print(f"[X07_derivative_rest] Failed to fetch liquidations after {retries} attempts")
        return []

    # ---------- History management ----------
    def update_liquidation_history(self, symbol, liquidation):
        sym = symbol.lower()
        if sym not in self._liquidation_history:
            self._liquidation_history[sym] = deque(maxlen=self._max_history)
        self._liquidation_history[sym].append(liquidation)

    def update_oi_and_price(self, symbol, oi, price, timestamp):
        sym = symbol.lower()
        if sym not in self._oi_history:
            self._oi_history[sym] = deque(maxlen=self._max_history)
            self._price_history[sym] = deque(maxlen=self._max_history)
        self._oi_history[sym].append((timestamp, oi))
        self._price_history[sym].append((timestamp, price))

    def update_ls_ratio(self, symbol, ls_ratio, long_pct, short_pct, timestamp):
        sym = symbol.lower()
        if sym not in self._ls_history:
            self._ls_history[sym] = deque(maxlen=self._max_history)
        self._ls_history[sym].append((timestamp, ls_ratio, long_pct, short_pct))

    # ---------- Analysis methods (same as before) ----------
    def compute_liquidation_levels(self, symbol, bins=50):
        liq_list = list(self._liquidation_history.get(symbol, []))
        if not liq_list:
            return []
        prices = [l['price'] for l in liq_list if l['price'] > 0]
        if not prices:
            return []
        min_p = min(prices)
        max_p = max(prices)
        bucket_size = (max_p - min_p) / bins if bins > 0 else 1
        buckets = defaultdict(float)
        for liq in liq_list:
            price = liq['price']
            qty = liq['quantity']
            bucket = int((price - min_p) / bucket_size) if bucket_size > 0 else 0
            buckets[bucket] += qty
        levels = []
        for b, vol in buckets.items():
            level_price = min_p + (b + 0.5) * bucket_size
            levels.append((level_price, vol))
        levels.sort(key=lambda x: x[1], reverse=True)
        return levels[:5]

    def compute_delta_liquidations(self, symbol, window_minutes=5):
        liq_list = list(self._liquidation_history.get(symbol, []))
        if not liq_list:
            return 0
        now_ms = int(time.time() * 1000)
        cutoff = now_ms - window_minutes * 60 * 1000
        net = 0.0
        for liq in liq_list:
            if liq['timestamp'] >= cutoff:
                if liq['side'] == 'BUY':
                    net -= liq['quantity']
                elif liq['side'] == 'SELL':
                    net += liq['quantity']
        return net

    def compute_market_reversal_signal(self, symbol, threshold_volume=100.0):
        liq_list = list(self._liquidation_history.get(symbol, []))
        price_hist = list(self._price_history.get(symbol, []))
        if len(liq_list) < 2 or len(price_hist) < 5:
            return 'NONE'
        large_liq = None
        for liq in reversed(liq_list):
            if liq['quantity'] >= threshold_volume:
                large_liq = liq
                break
        if not large_liq:
            return 'NONE'
        liq_ts = large_liq['timestamp']
        price_at_liq = None
        for ts, p in price_hist:
            if ts >= liq_ts:
                price_at_liq = p
                break
        if not price_at_liq:
            return 'NONE'
        later_ts = liq_ts + 5 * 60 * 1000
        price_later = None
        for ts, p in price_hist:
            if ts >= later_ts:
                price_later = p
                break
        if not price_later:
            return 'NONE'
        pct_change = (price_later - price_at_liq) / price_at_liq * 100 if price_at_liq != 0 else 0
        if large_liq['side'] == 'SELL' and pct_change > 1.0:
            return 'BULLISH'
        elif large_liq['side'] == 'BUY' and pct_change < -1.0:
            return 'BEARISH'
        return 'NONE'

    def compute_oi_liquidation_relation(self, symbol):
        oi_hist = list(self._oi_history.get(symbol, []))
        liq_list = list(self._liquidation_history.get(symbol, []))
        if len(oi_hist) < 2 or not liq_list:
            return 'NO_DATA'
        recent_oi = [oi for _, oi in oi_hist[-12:]]
        if len(recent_oi) < 2:
            return 'NO_DATA'
        oi_change_pct = (recent_oi[-1] - recent_oi[0]) / recent_oi[0] * 100 if recent_oi[0] != 0 else 0
        now_ms = int(time.time() * 1000)
        cutoff = now_ms - 60 * 60 * 1000
        total_liq = sum(l['quantity'] for l in liq_list if l['timestamp'] >= cutoff)
        high_liq_threshold = 1000.0
        if oi_change_pct > 5 and total_liq > high_liq_threshold:
            return 'OI_RISING_HIGH_LIQ'
        elif oi_change_pct < -5 and total_liq > high_liq_threshold:
            return 'OI_FALLING_HIGH_LIQ'
        elif oi_change_pct > 5:
            return 'OI_RISING'
        elif oi_change_pct < -5:
            return 'OI_FALLING'
        else:
            return 'OI_STABLE'

    def compute_retail_sentiment(self, symbol):
        ls_hist = list(self._ls_history.get(symbol, []))
        if not ls_hist:
            return 'NEUTRAL'
        latest = ls_hist[-1][1]
        if latest > 1.5:
            return 'EXTREME_LONG'
        elif latest < 0.67:
            return 'EXTREME_SHORT'
        elif latest > 1.2:
            return 'BULLISH'
        elif latest < 0.83:
            return 'BEARISH'
        else:
            return 'NEUTRAL'

    def _write_raw_row(self, symbol, liq):
        sym = symbol.lower()
        filepath = os.path.join(self.symbols_dir, f"{sym}_liquidations.tsv")
        try:
            with open(filepath, 'a') as f:
                f.write(f"raw\t{liq['timestamp']}\t{liq['price']:.2f}\t{liq['quantity']:.2f}\t{liq['side']}\t0\t0\t0\t0\t0\t\t\t\t\t\t\n")
        except Exception as e:
            print(f"[X07_derivative_rest] Raw write error: {e}")

    def _write_analysis_row(self, symbol):
        sym = symbol.lower()
        timestamp = int(time.time() * 1000)
        liq_list = list(self._liquidation_history.get(sym, []))
        
        if not liq_list:
            total_liq_vol = 0.0
            recent_liq_vol = 0.0
            long_liq = 0.0
            short_liq = 0.0
            delta_liq = 0.0
            magnet_str = ''
            reversal = 'NONE'
            oi_relation = 'NO_DATA'
            sentiment = 'NEUTRAL'
        else:
            total_liq_vol = sum(l['quantity'] for l in liq_list)
            recent_liq_vol = sum(l['quantity'] for l in liq_list[-20:])
            long_liq = sum(l['quantity'] for l in liq_list if l['side'] == 'BUY')
            short_liq = sum(l['quantity'] for l in liq_list if l['side'] == 'SELL')
            delta_liq = self.compute_delta_liquidations(sym)
            levels = self.compute_liquidation_levels(sym)
            magnet_str = ';'.join([f"{p:.2f}:{v:.0f}" for p, v in levels[:3]])
            reversal = self.compute_market_reversal_signal(sym)
            oi_relation = self.compute_oi_liquidation_relation(sym)
            sentiment = self.compute_retail_sentiment(sym)

        oi_hist = list(self._oi_history.get(sym, []))
        price_hist = list(self._price_history.get(sym, []))
        oi_trend = 'FLAT'
        price_trend = 'FLAT'
        if len(oi_hist) >= 2:
            oi_change = (oi_hist[-1][1] - oi_hist[-2][1]) / oi_hist[-2][1] * 100 if oi_hist[-2][1] != 0 else 0
            oi_trend = 'RISING' if oi_change > 0.5 else 'FALLING' if oi_change < -0.5 else 'FLAT'
        if len(price_hist) >= 2:
            price_change = (price_hist[-1][1] - price_hist[-2][1]) / price_hist[-2][1] * 100 if price_hist[-2][1] != 0 else 0
            price_trend = 'UP' if price_change > 0.1 else 'DOWN' if price_change < -0.1 else 'FLAT'

        filepath = os.path.join(self.symbols_dir, f"{sym}_liquidations.tsv")
        if not os.path.exists(filepath):
            try:
                with open(filepath, 'w') as f:
                    f.write("event_type\ttimestamp\tprice\tquantity\tside\ttotal_liq_vol\trecent_liq_vol\tlong_liq\tshort_liq\tdelta_liq_5m\tmagnet_levels\treversal_signal\toi_relation\tretail_sentiment\toi_trend\tprice_trend\n")
                print(f"[X07_derivative_rest] Created liquidation file for {sym}")
            except Exception as e:
                print(f"[X07_derivative_rest] Failed to create file: {e}")
                return
        try:
            with open(filepath, 'a') as f:
                f.write(f"analysis\t{timestamp}\t0\t0\t0\t{total_liq_vol:.2f}\t{recent_liq_vol:.2f}\t{long_liq:.2f}\t{short_liq:.2f}\t{delta_liq:.2f}\t{magnet_str}\t{reversal}\t{oi_relation}\t{sentiment}\t{oi_trend}\t{price_trend}\n")
            print(f"[X07_derivative_rest] Appended analysis row for {sym}")
        except Exception as e:
            print(f"[X07_derivative_rest] Analysis write error: {e}")

    # ---------- Main collect and save ----------
    def collect_and_save(self, symbol):
        sym = symbol.lower()
        print(f"[X07_derivative_rest] Collecting derivative data for {sym}")
        
        spot = self.fetch_spot_price(sym)
        current_funding = self.fetch_current_funding_and_mark(sym)
        oi_history = self.fetch_oi_history(sym, period="5m", limit=24)
        ls_history = self.fetch_long_short_ratio_history(sym, period="5m", limit=24)
        funding_history = self.fetch_funding_rate_history(sym, limit=24)
        # Use smaller minutes and limit to avoid 400
        liquidations = self.fetch_liquidations_history(sym, minutes=60, limit=100, min_quantity=1.0)
        
        now_ms = int(time.time() * 1000)
        current_oi = self.fetch_open_interest(sym)
        self.update_oi_and_price(sym, current_oi, spot, now_ms)
        for ls in ls_history:
            self.update_ls_ratio(sym, ls['long_short_ratio'], ls['long_account'], ls['short_account'], ls['timestamp'])
        
        filepath = os.path.join(self.symbols_dir, f"{sym}_liquidations.tsv")
        if not os.path.exists(filepath):
            try:
                with open(filepath, 'w') as f:
                    f.write("event_type\ttimestamp\tprice\tquantity\tside\ttotal_liq_vol\trecent_liq_vol\tlong_liq\tshort_liq\tdelta_liq_5m\tmagnet_levels\treversal_signal\toi_relation\tretail_sentiment\toi_trend\tprice_trend\n")
                print(f"[X07_derivative_rest] Created liquidation file for {sym}")
            except Exception as e:
                print(f"[X07_derivative_rest] Failed to create file: {e}")
                return
        
        if liquidations:
            for liq in liquidations:
                self.update_liquidation_history(sym, liq)
                self._write_raw_row(sym, liq)
            print(f"[X07_derivative_rest] Wrote {len(liquidations)} raw liquidation rows")
        else:
            print(f"[X07_derivative_rest] No real liquidations found for {sym} (raw rows skipped)")
        
        self._write_analysis_row(sym)
        
        oi_change = self.compute_oi_change(oi_history)
        oi_momentum = self.compute_oi_momentum(oi_history)
        latest_ls = ls_history[-1] if ls_history else {'long_short_ratio': 0, 'long_account': 0, 'short_account': 0}
        arbitrage = self.compute_arbitrage(spot, current_funding['mark_price'], current_funding['funding_rate'])
        
        deriv_file = os.path.join(self.symbols_dir, f"{sym}_derivative.tsv")
        try:
            write_header = not os.path.exists(deriv_file)
            with open(deriv_file, 'a') as f:
                if write_header:
                    f.write("timestamp\tspot_price\tmark_price\tfunding_rate\toi_current\toi_change_2h_pct\toi_trend\tls_ratio\tlong_pct\tshort_pct\tarb_basis_pct\tannualized_return\tliquidations_count\tlatest_liq_price\tlatest_liq_qty\n")
                f.write(f"{now_ms}\t{spot}\t{current_funding['mark_price']}\t{current_funding['funding_rate']}\t"
                        f"{oi_change['current']}\t{oi_change['change_pct_2h']}\t{oi_momentum['trend']}\t"
                        f"{latest_ls['long_short_ratio']}\t{latest_ls['long_account']}\t{latest_ls['short_account']}\t"
                        f"{arbitrage['basis_pct']}\t{arbitrage['annualized_return']}\t"
                        f"{len(liquidations)}\t{liquidations[-1]['price'] if liquidations else 0}\t{liquidations[-1]['quantity'] if liquidations else 0}\n")
            print(f"[X07_derivative_rest] Saved derivative summary for {sym}")
        except Exception as e:
            print(f"[X07_derivative_rest] Derivative save error: {e}")

    # ---------- Helper methods (unchanged) ----------
    def compute_oi_change(self, oi_history):
        if len(oi_history) >= 2:
            current = oi_history[-1]['value']
            previous = oi_history[0]['value']
            if previous > 0:
                change = current - previous
                change_pct = (change / previous) * 100
            else:
                change = 0
                change_pct = 0
            return {'current': current, 'change_2h': change, 'change_pct_2h': round(change_pct, 4)}
        return {'current': 0, 'change_2h': 0, 'change_pct_2h': 0}

    def compute_oi_momentum(self, oi_history):
        if len(oi_history) < 2:
            return {'momentum': 0, 'trend': 'neutral'}
        recent = oi_history[-5:] if len(oi_history) >= 5 else oi_history
        first = recent[0]['value']
        last = recent[-1]['value']
        if first == 0:
            return {'momentum': 0, 'trend': 'neutral'}
        momentum = (last - first) / first
        trend = 'bullish' if momentum > 0.03 else 'bearish' if momentum < -0.03 else 'neutral'
        return {'momentum': round(momentum, 4), 'trend': trend}

    def compute_arbitrage(self, spot_price, futures_mark, funding_rate):
        if spot_price == 0:
            return {'basis': 0, 'basis_pct': 0, 'annualized_return': 0}
        basis = futures_mark - spot_price
        basis_pct = (basis / spot_price) * 100
        annualized = funding_rate * 3 * 365 * 100
        return {
            'basis': round(basis, 2),
            'basis_pct': round(basis_pct, 4),
            'annualized_return': round(annualized, 2)
        }

    def stop(self):
        self._running = False