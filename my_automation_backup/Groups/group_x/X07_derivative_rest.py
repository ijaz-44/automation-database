import requests
import json
import time
import os
import sqlite3
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
        self.min_quantity_map = {}
        self.KEEP_RUNS = 5   # keep last 5 snapshots per symbol
        print("[X07_derivative_rest] SQLite version – atomic write, auto cleanup")

    def _atomic_write_db(self, final_db_path, data_dict):
        tmp_db = final_db_path + ".tmp"
        if os.path.exists(tmp_db):
            os.remove(tmp_db)
        conn = sqlite3.connect(tmp_db)
        cursor = conn.cursor()

        # Create tables
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS derivative_summary (
                run_id INTEGER PRIMARY KEY,
                symbol TEXT,
                timestamp INTEGER,
                spot_price REAL,
                mark_price REAL,
                funding_rate REAL,
                oi_current REAL,
                oi_change_pct REAL,
                oi_trend TEXT,
                ls_ratio REAL,
                long_pct REAL,
                short_pct REAL,
                arb_basis_pct REAL,
                annualized_return REAL,
                liquidations_count INTEGER,
                latest_liq_price REAL,
                latest_liq_qty REAL,
                created_at INTEGER
            ) WITHOUT ROWID
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS derivative_oi_history (
                run_id INTEGER,
                timestamp INTEGER,
                oi_value REAL,
                PRIMARY KEY (run_id, timestamp)
            ) WITHOUT ROWID
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS derivative_ls_history (
                run_id INTEGER,
                timestamp INTEGER,
                long_short_ratio REAL,
                long_account REAL,
                short_account REAL,
                PRIMARY KEY (run_id, timestamp)
            ) WITHOUT ROWID
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS derivative_funding_history (
                run_id INTEGER,
                timestamp INTEGER,
                funding_rate REAL,
                PRIMARY KEY (run_id, timestamp)
            ) WITHOUT ROWID
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS derivative_liquidation_levels (
                run_id INTEGER,
                price REAL,
                volume REAL,
                seq INTEGER,
                PRIMARY KEY (run_id, seq)
            ) WITHOUT ROWID
        """)
        cursor.execute("CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT) WITHOUT ROWID")

        # Insert summary
        cursor.execute("""
            INSERT INTO derivative_summary (
                run_id, symbol, timestamp, spot_price, mark_price, funding_rate,
                oi_current, oi_change_pct, oi_trend, ls_ratio, long_pct, short_pct,
                arb_basis_pct, annualized_return, liquidations_count, latest_liq_price, latest_liq_qty, created_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            data_dict['run_id'], data_dict['symbol'], data_dict['timestamp'],
            data_dict['spot_price'], data_dict['mark_price'], data_dict['funding_rate'],
            data_dict['oi_current'], data_dict['oi_change_pct'], data_dict['oi_trend'],
            data_dict['ls_ratio'], data_dict['long_pct'], data_dict['short_pct'],
            data_dict['arb_basis_pct'], data_dict['annualized_return'],
            data_dict['liquidations_count'], data_dict['latest_liq_price'], data_dict['latest_liq_qty'],
            int(time.time())
        ))

        # OI history
        for h in data_dict['oi_history']:
            cursor.execute("INSERT INTO derivative_oi_history (run_id, timestamp, oi_value) VALUES (?,?,?)",
                           (data_dict['run_id'], h['timestamp'], h['value']))

        # LS history
        for h in data_dict['ls_history']:
            cursor.execute("INSERT INTO derivative_ls_history (run_id, timestamp, long_short_ratio, long_account, short_account) VALUES (?,?,?,?,?)",
                           (data_dict['run_id'], h['timestamp'], h['long_short_ratio'], h['long_account'], h['short_account']))

        # Funding history
        for h in data_dict['funding_history']:
            cursor.execute("INSERT INTO derivative_funding_history (run_id, timestamp, funding_rate) VALUES (?,?,?)",
                           (data_dict['run_id'], h['timestamp'], h['funding_rate']))

        # Liquidation levels
        seq = 0
        for price, vol in data_dict['liquidation_levels']:
            cursor.execute("INSERT INTO derivative_liquidation_levels (run_id, price, volume, seq) VALUES (?,?,?,?)",
                           (data_dict['run_id'], price, vol, seq))
            seq += 1

        # Meta
        cursor.execute("INSERT OR REPLACE INTO meta (key, value) VALUES (?,?)", ("last_update", str(time.time())))
        cursor.execute("INSERT OR REPLACE INTO meta (key, value) VALUES (?,?)", ("run_id", str(data_dict['run_id'])))

        conn.commit()

        # Cleanup: keep only last KEEP_RUNS runs
        cursor.execute("SELECT run_id FROM derivative_summary ORDER BY run_id DESC")
        all_runs = [row[0] for row in cursor.fetchall()]
        if len(all_runs) > self.KEEP_RUNS:
            old_runs = all_runs[self.KEEP_RUNS:]
            placeholders = ','.join(['?'] * len(old_runs))
            for table in ['derivative_summary', 'derivative_oi_history', 'derivative_ls_history',
                          'derivative_funding_history', 'derivative_liquidation_levels']:
                cursor.execute(f"DELETE FROM {table} WHERE run_id IN ({placeholders})", old_runs)
            print(f"[X07] Cleaned up {len(old_runs)} old runs")

        conn.commit()
        conn.close()

        if os.path.exists(final_db_path):
            os.remove(final_db_path)
        os.rename(tmp_db, final_db_path)
        print(f"[X07] Atomic DB write successful -> {os.path.basename(final_db_path)}")

    # ---------- API methods (same as before) ----------
    def fetch_open_interest(self, symbol):
        try:
            r = requests.get(f"{FUTURES_BASE_URL}/fapi/v1/openInterest",
                             params={"symbol": symbol.upper()}, timeout=10)
            r.raise_for_status()
            return float(r.json().get('openInterest', 0))
        except Exception as e:
            print(f"[X07] OI error: {e}")
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
            print(f"[X07] OI history error: {e}")
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
            print(f"[X07] L/S history error: {e}")
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
            print(f"[X07] Funding history error: {e}")
            return []

    def fetch_spot_price(self, symbol):
        try:
            r = requests.get(f"{SPOT_BASE_URL}/ticker/price",
                             params={"symbol": symbol.upper()}, timeout=10)
            r.raise_for_status()
            return float(r.json().get('price', 0))
        except Exception as e:
            print(f"[X07] Spot price error: {e}")
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
            print(f"[X07] Funding error: {e}")
            return {'funding_rate': 0, 'mark_price': 0, 'index_price': 0}

    def fetch_liquidations_history(self, symbol, minutes=60, limit=100, retries=3):
        min_quantity = self.min_quantity_map.get(symbol.upper(), 1.0)
        if min_quantity == 1.0:
            oi = self.fetch_open_interest(symbol)
            if oi > 0:
                min_quantity = max(0.1, oi * 0.0001)
        for attempt in range(retries):
            try:
                now_ms = int(time.time() * 1000)
                start_ms = now_ms - minutes * 60 * 1000
                url = f"{FUTURES_BASE_URL}/fapi/v1/allForceOrders"
                params = {"symbol": symbol.upper(), "limit": min(limit, 100)}
                r = requests.get(url, params=params, timeout=10)
                if r.status_code == 200:
                    data = r.json()
                    recent = []
                    for item in data:
                        ts = item.get('time', 0)
                        qty = float(item.get('origQty', 0))
                        if ts >= start_ms and qty >= min_quantity:
                            recent.append({
                                'timestamp': ts,
                                'price': float(item.get('price', 0)),
                                'quantity': qty,
                                'side': item.get('side', '')
                            })
                    recent.sort(key=lambda x: x['timestamp'])
                    return recent
                else:
                    print(f"[X07] HTTP {r.status_code} for liquidations")
            except Exception as e:
                print(f"[X07] Liquidations fetch error (attempt {attempt+1}): {e}")
            if attempt < retries - 1:
                time.sleep(2)
        return []

    # History management (same as before)
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

    # Analysis methods
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

    # Main collect and save – writes to SQLite DB
    def collect_and_save(self, symbol):
        sym = symbol.lower()
        print(f"[X07] Collecting derivative data for {sym}")

        spot = self.fetch_spot_price(sym)
        current_funding = self.fetch_current_funding_and_mark(sym)
        oi_history = self.fetch_oi_history(sym, period="5m", limit=24)
        ls_history = self.fetch_long_short_ratio_history(sym, period="5m", limit=24)
        funding_history = self.fetch_funding_rate_history(sym, limit=24)
        liquidations = self.fetch_liquidations_history(sym, minutes=60, limit=100)

        now_ms = int(time.time() * 1000)
        current_oi = self.fetch_open_interest(sym)
        self.update_oi_and_price(sym, current_oi, spot, now_ms)
        for ls in ls_history:
            self.update_ls_ratio(sym, ls['long_short_ratio'], ls['long_account'], ls['short_account'], ls['timestamp'])
        for liq in liquidations:
            self.update_liquidation_history(sym, liq)

        oi_change = self.compute_oi_change(oi_history)
        oi_momentum = self.compute_oi_momentum(oi_history)
        latest_ls = ls_history[-1] if ls_history else {'long_short_ratio': 0, 'long_account': 0, 'short_account': 0}
        arbitrage = self.compute_arbitrage(spot, current_funding['mark_price'], current_funding['funding_rate'])
        total_liq_vol = sum(l['quantity'] for l in liquidations) if liquidations else 0.0
        latest_liq_price = liquidations[-1]['price'] if liquidations else 0
        latest_liq_qty = liquidations[-1]['quantity'] if liquidations else 0
        liq_levels = self.compute_liquidation_levels(sym, bins=50)

        data_dict = {
            "run_id": int(time.time() * 1000),
            "symbol": sym.upper(),
            "timestamp": now_ms,
            "spot_price": spot,
            "mark_price": current_funding['mark_price'],
            "funding_rate": current_funding['funding_rate'],
            "oi_current": oi_change['current'],
            "oi_change_pct": oi_change['change_pct_2h'],
            "oi_trend": oi_momentum['trend'],
            "ls_ratio": latest_ls['long_short_ratio'],
            "long_pct": latest_ls['long_account'],
            "short_pct": latest_ls['short_account'],
            "arb_basis_pct": arbitrage['basis_pct'],
            "annualized_return": arbitrage['annualized_return'],
            "liquidations_count": len(liquidations),
            "latest_liq_price": latest_liq_price,
            "latest_liq_qty": latest_liq_qty,
            "oi_history": oi_history,
            "ls_history": ls_history,
            "funding_history": funding_history,
            "liquidation_levels": liq_levels
        }

        final_db_path = os.path.join(self.symbols_dir, f"{sym}_derivative.db")
        self._atomic_write_db(final_db_path, data_dict)
        print(f"[X07] Derivative data saved to {final_db_path}")

    def stop(self):
        self._running = False