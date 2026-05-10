"""
X13 - Liquidation Data Module (TOON format, Multi‑Resource, Atomic Rename)
- Tries 6 public APIs in sequence (Coinalyze, CryptoCompare, Binance Futures, Bybit, Deribit, Hyblock)
- Falls back to next on failure
- Caches results per symbol+interval for 120 seconds (2 min freshness)
- Caches daily candles (refreshed once per day)
- Rate‑limited API calls (1.2 sec between calls)
- Designed to be instantiated ONCE and reused for all symbols
- Saves to: market_data/binance/symbols/{symbol}_liquidations.toon
"""

import os
import time
import glob
import requests
from collections import defaultdict
from datetime import datetime

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SYMBOLS_DIR = os.path.join(BASE_DIR, "market_data", "binance", "symbols")
os.makedirs(SYMBOLS_DIR, exist_ok=True)

LOG_FILE = os.path.join(SYMBOLS_DIR, "liquidation_log.txt")

class LiquidationDataREST:
    # Singleton instance
    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        if hasattr(self, '_initialized'):
            return
        self._initialized = True
        self._last_call = 0
        self._rate_limit_sec = 1.2          # renamed to avoid conflict with method
        # Cache for liquidation data: key = (symbol, interval) -> (timestamp, rows)
        self._liq_cache = {}
        # Cache for daily candles
        self._daily_candles_cache = None
        self._daily_candles_time = 0
        # Cache for 1m candles (per symbol)
        self._1m_candles_cache = {}
        # TTL: 2 minutes for liquidation and 1m candles
        self._ttl = 120

    def _rate_limit(self):
        now = time.time()
        elapsed = now - self._last_call
        if elapsed < self._rate_limit_sec:
            time.sleep(self._rate_limit_sec - elapsed)
        self._last_call = time.time()

    def _log(self, msg):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        line = f"{timestamp} {msg}"
        print(line)
        try:
            with open(LOG_FILE, 'a', encoding='utf-8') as f:
                f.write(line + "\n")
        except:
            pass

    def _atomic_write(self, path, content):
        temp = path + ".tmp"
        try:
            with open(temp, 'w', encoding='utf-8') as f:
                f.write(content)
                f.flush()
                os.fsync(f.fileno())
            os.rename(temp, path)
            self._log(f"[ATOMIC] OK -> {os.path.basename(path)}")
            return True
        except Exception as e:
            if os.path.exists(temp):
                os.remove(temp)
            self._log(f"[ATOMIC] FAIL {path}: {e}")
            return False

    # ---------- Helper ----------
    def _to_coin(self, symbol):
        for suffix in ['USDT', 'BUSD', 'USDC', 'PERP']:
            if symbol.upper().endswith(suffix):
                return symbol.upper()[:-len(suffix)]
        return symbol.upper()

    # ---------- API calls with rate limit ----------
    def _fetch_coinalyze(self, symbol, interval):
        self._rate_limit()
        coin = self._to_coin(symbol)
        url = "https://api.coinalyze.net/v1/liquidation"
        headers = {"api_key": "8d7838f9-7111-4d4f-bffd-b83e8d468b60"}
        for sym_try in [coin, f"Binance:{symbol.upper()}", symbol.upper()]:
            params = {"symbol": sym_try, "interval": interval, "limit": 120,
                      "fields": "timestamp,longVolume,shortVolume"}
            try:
                r = requests.get(url, headers=headers, params=params, timeout=10)
                if r.status_code == 200:
                    data = r.json()
                    if data.get("data"):
                        rows = []
                        for item in data["data"]:
                            rows.append({
                                "ts": item["timestamp"],
                                "long": float(item.get("longVolume", 0)),
                                "short": float(item.get("shortVolume", 0))
                            })
                        self._log(f"[COINALYZE] {symbol} {interval} -> {len(rows)} rows (using {sym_try})")
                        return rows
                else:
                    self._log(f"[COINALYZE] {sym_try} HTTP {r.status_code}")
            except Exception as e:
                self._log(f"[COINALYZE] {sym_try} error: {e}")
        return []

    def _fetch_cryptocompare(self, symbol, interval):
        self._rate_limit()
        coin = self._to_coin(symbol)
        url = "https://min-api.cryptocompare.com/data/v1/liquidations"
        params = {"symbol": coin, "limit": 200, "api_key": "2f8a33bc64db22db858d7962112cead0ccc07035f9806adb031ad4ce71743d75"}
        try:
            r = requests.get(url, params=params, timeout=10)
            if r.status_code == 200:
                data = r.json()
                events = []
                for item in data.get("Data", []):
                    events.append({
                        "ts": item.get("timestamp", 0),
                        "long": float(item.get("longLiquidatedUsd", 0)),
                        "short": float(item.get("shortLiquidatedUsd", 0))
                    })
                interval_ms = {"1m": 60000, "15m": 900000, "1h": 3600000}[interval]
                bins = defaultdict(lambda: {"long": 0.0, "short": 0.0})
                for e in events:
                    bucket = (e["ts"] // interval_ms) * interval_ms
                    bins[bucket]["long"] += e["long"]
                    bins[bucket]["short"] += e["short"]
                rows = [{"ts": b, "long": v["long"], "short": v["short"]} for b, v in sorted(bins.items())]
                if rows:
                    self._log(f"[CRYPTOCOMPARE] {symbol} {interval} -> {len(rows)} rows")
                    return rows[-120:]
            else:
                self._log(f"[CRYPTOCOMPARE] HTTP {r.status_code}")
        except Exception as e:
            self._log(f"[CRYPTOCOMPARE] error: {e}")
        return []

    def _fetch_binance_futures(self, symbol):
        self._rate_limit()
        url = "https://fapi.binance.com/fapi/v1/liquidationOrders"
        params = {"symbol": symbol.upper(), "limit": 100}
        try:
            r = requests.get(url, params=params, timeout=10)
            if r.status_code == 200:
                data = r.json()
                if data:
                    rows = []
                    for item in data:
                        price = float(item.get("price", 0))
                        qty = float(item.get("origQty", 0))
                        side = item.get("side", "")
                        if side == "BUY":
                            rows.append({"ts": item.get("time", 0), "long": price * qty, "short": 0})
                        elif side == "SELL":
                            rows.append({"ts": item.get("time", 0), "long": 0, "short": price * qty})
                    self._log(f"[BINANCE_FUTURES] {symbol} -> {len(rows)} events")
                    return rows
            else:
                self._log(f"[BINANCE_FUTURES] HTTP {r.status_code}")
        except Exception as e:
            self._log(f"[BINANCE_FUTURES] error: {e}")
        return []

    def _fetch_bybit(self, symbol):
        self._rate_limit()
        url = "https://api.bybit.com/v5/market/liquidations"
        params = {"category": "linear", "symbol": symbol.upper(), "limit": 100}
        try:
            r = requests.get(url, params=params, timeout=10)
            if r.status_code == 200:
                data = r.json()
                if data.get("retCode") == 0:
                    items = data.get("result", {}).get("list", [])
                    rows = []
                    for item in items:
                        price = float(item.get("price", 0))
                        size = float(item.get("size", 0))
                        side = item.get("side", "")
                        if side == "Buy":
                            rows.append({"ts": int(item.get("updatedTime", 0)), "long": price * size, "short": 0})
                        elif side == "Sell":
                            rows.append({"ts": int(item.get("updatedTime", 0)), "long": 0, "short": price * size})
                    self._log(f"[BYBIT] {symbol} -> {len(rows)} events")
                    return rows
            else:
                self._log(f"[BYBIT] HTTP {r.status_code}")
        except Exception as e:
            self._log(f"[BYBIT] error: {e}")     # fixed parentheses
        return []

    def _fetch_deribit(self, symbol):
        self._rate_limit()
        coin = self._to_coin(symbol)
        if coin not in ["BTC", "ETH"]:
            return []
        instrument = f"{coin}-PERPETUAL"
        url = "https://www.deribit.com/api/v2/public/get_last_settlements_by_instrument"
        params = {"instrument_name": instrument, "type": "liquidation", "count": 100}
        try:
            r = requests.get(url, params=params, timeout=10)
            if r.status_code == 200:
                data = r.json()
                settlements = data.get("result", {}).get("settlements", [])
                rows = []
                for s in settlements:
                    price = float(s.get("price", 0))
                    qty = abs(float(s.get("position", 0)))
                    direction = s.get("direction", "")
                    if direction == "buy":
                        rows.append({"ts": s.get("timestamp", 0), "long": price * qty, "short": 0})
                    elif direction == "sell":
                        rows.append({"ts": s.get("timestamp", 0), "long": 0, "short": price * qty})
                self._log(f"[DERIBIT] {symbol} -> {len(rows)} events")
                return rows
            else:
                self._log(f"[DERIBIT] HTTP {r.status_code}")
        except Exception as e:
            self._log(f"[DERIBIT] error: {e}")
        return []

    def _fetch_hyblock(self, symbol):
        self._rate_limit()
        coin = self._to_coin(symbol).lower()
        url = f"https://api.hyblockcapital.com/liquidation/levels?symbol={coin}"
        try:
            r = requests.get(url, timeout=10)
            if r.status_code == 200:
                self._log(f"[HYBLOCK] {symbol} aggregated data available (skipping events)")
                return []
            else:
                self._log(f"[HYBLOCK] HTTP {r.status_code}")
        except Exception as e:
            self._log(f"[HYBLOCK] error: {e}")
        return []

    # ---------- Master fetch with 2‑minute TTL ----------
    def _get_liquidations(self, symbol, interval):
        cache_key = (symbol.lower(), interval)
        now = time.time()
        if cache_key in self._liq_cache:
            cached_time, cached_rows = self._liq_cache[cache_key]
            if now - cached_time < self._ttl:
                self._log(f"[CACHE_HIT] {symbol} {interval} (age={now-cached_time:.0f}s)")
                return cached_rows
        # Not in cache or expired
        rows = self._fetch_coinalyze(symbol, interval)
        if rows:
            self._liq_cache[cache_key] = (now, rows)
            return rows
        rows = self._fetch_cryptocompare(symbol, interval)
        if rows:
            self._liq_cache[cache_key] = (now, rows)
            return rows

        events = self._fetch_binance_futures(symbol)
        if not events:
            events = self._fetch_bybit(symbol)
        if not events:
            events = self._fetch_deribit(symbol)
        if events:
            interval_ms = {"1m": 60000, "15m": 900000, "1h": 3600000}[interval]
            bins = defaultdict(lambda: {"long": 0.0, "short": 0.0})
            for e in events:
                bucket = (e["ts"] // interval_ms) * interval_ms
                bins[bucket]["long"] += e["long"]
                bins[bucket]["short"] += e["short"]
            rows = [{"ts": b, "long": v["long"], "short": v["short"]} for b, v in sorted(bins.items())]
            if rows:
                rows = rows[-120:]
                self._liq_cache[cache_key] = (now, rows)
                self._log(f"[BINNED] {symbol} {interval} -> {len(rows)} rows from raw events")
                return rows
        self._log(f"[WARNING] No data for {symbol} {interval}")
        empty_row = [{"ts": int(now*1000), "long": 0.0, "short": 0.0}]
        self._liq_cache[cache_key] = (now, empty_row)
        return empty_row

    # ---------- Price candles with TTL=2m for 1m, 24h for daily ----------
    def _fetch_candles(self, symbol, interval="1m", limit=120):
        if interval == "1d":
            now = time.time()
            if self._daily_candles_cache is not None and (now - self._daily_candles_time) < 86400:
                return self._daily_candles_cache
        elif interval == "1m":
            now = time.time()
            if symbol in self._1m_candles_cache:
                cached_time, cached_candles = self._1m_candles_cache[symbol]
                if now - cached_time < self._ttl:  # 2 minutes freshness for 1m candles
                    return cached_candles

        self._rate_limit()          # fixed: now calls the method, not a float
        url = "https://api.binance.com/api/v3/klines"
        params = {"symbol": symbol.upper(), "interval": interval, "limit": limit}
        try:
            r = requests.get(url, params=params, timeout=10)
            if r.status_code == 200:
                data = r.json()
                candles = []
                for c in data:
                    candles.append({
                        'ts': c[0],
                        'high': float(c[2]),
                        'low': float(c[3]),
                        'volume': float(c[5])
                    })
                if interval == "1d":
                    self._daily_candles_cache = candles
                    self._daily_candles_time = time.time()
                elif interval == "1m":
                    self._1m_candles_cache[symbol] = (time.time(), candles)
                return candles
        except Exception as e:
            self._log(f"[CANDLES] error: {e}")
        return []

    # ---------- Derived data (unchanged) ----------
    def _heatmap(self, candles, bins=20):
        if not candles:
            return {}
        min_p = min(c['low'] for c in candles)
        max_p = max(c['high'] for c in candles)
        if min_p >= max_p:
            return {}
        step = (max_p - min_p) / bins
        hm = defaultdict(float)
        for c in candles:
            low, high, vol = c['low'], c['high'], c['volume']
            start = int((low - min_p) / step)
            end = int((high - min_p) / step)
            span = max(1, end - start + 1)
            per_bin = vol / span
            for b in range(start, end+1):
                if 0 <= b < bins:
                    center = min_p + (b + 0.5) * step
                    hm[center] += per_bin
        return dict(hm)

    def _cluster(self, values, tol=0.001):
        clusters = []
        for v in sorted(values):
            found = False
            for cl in clusters:
                if abs(v - cl[0]) / cl[0] <= tol:
                    cl.append(v)
                    found = True
                    break
            if not found:
                clusters.append([v])
        return [(round(sum(c)/len(c), 4), len(c)) for c in clusters if len(c) >= 2]

    def _pools(self, candles):
        highs = [c['high'] for c in candles]
        lows = [c['low'] for c in candles]
        return self._cluster(highs), self._cluster(lows)

    def _stop_levels(self, candles_daily):
        if not candles_daily or len(candles_daily) < 7:
            return {}
        weekly_high = max(c['high'] for c in candles_daily[-7:])
        weekly_low = min(c['low'] for c in candles_daily[-7:])
        prev_day = candles_daily[-2] if len(candles_daily) >= 2 else candles_daily[-1]
        return {
            'prev_day_high': prev_day['high'],
            'prev_day_low': prev_day['low'],
            'prev_week_high': weekly_high,
            'prev_week_low': weekly_low
        }

    # ---------- Public method ----------
    def collect_and_save(self, symbol):
        self._log(f"[COLLECT] Starting for {symbol} (TOON format)")
        start_time = time.time()

        # Clean old files
        for ext in ['.tsv', '.toon']:
            old_file = os.path.join(SYMBOLS_DIR, f"{symbol.lower()}_liquidations{ext}")
            if os.path.exists(old_file):
                try:
                    os.remove(old_file)
                    self._log(f"[CLEANUP] Removed old {ext} file")
                except:
                    pass

        candles = self._fetch_candles(symbol, "1m", 120)
        candles_daily = self._fetch_candles(symbol, "1d", 7)
        now_ts = int(time.time() * 1000)

        lines = []
        lines.append(f"# Liquidation data for {symbol.upper()} – TOON format")
        lines.append(f"generated: {datetime.now().isoformat()}")
        lines.append(f"symbol: {symbol}")
        lines.append("")

        # 1. AGGREGATES (1m, 15m, 1h)
        for iv in ["1m", "15m", "1h"]:
            rows = self._get_liquidations(symbol, iv)
            rows = rows[-10:] if len(rows) > 10 else rows
            fields = ["timestamp", "long_volume", "short_volume", "total_volume"]
            lines.append(f"liquidation_{iv}[{len(rows)}]{{{','.join(fields)}}}:")
            if rows:
                row_strings = [f"{r['ts']},{r['long']:.2f},{r['short']:.2f},{r['long']+r['short']:.2f}" for r in rows]
                lines.append("  " + " |\n  ".join(row_strings))
            else:
                lines.append("  ")
            lines.append("")

        # 2. HEATMAP
        hm = self._heatmap(candles, 20) if candles else {}
        hm_items = sorted(hm.items()) if hm else []
        lines.append(f"liquidity_heatmap[{len(hm_items)}]{{price_bin,volume}}:")
        if hm_items:
            row_strings = [f"{p:.2f},{v:.2f}" for p, v in hm_items]
            lines.append("  " + " |\n  ".join(row_strings))
        else:
            lines.append("  ")
        lines.append("")

        # 3. POOLS
        high_p, low_p = self._pools(candles) if candles else ([], [])
        lines.append(f"liquidity_pools_high[{len(high_p)}]{{level,count}}:")
        lines.append("  " + (" |\n  ".join([f"{l},{c}" for l,c in high_p]) if high_p else " "))
        lines.append("")
        lines.append(f"liquidity_pools_low[{len(low_p)}]{{level,count}}:")
        lines.append("  " + (" |\n  ".join([f"{l},{c}" for l,c in low_p]) if low_p else " "))
        lines.append("")

        # 4. STOP HUNT LEVELS
        lvls = self._stop_levels(candles_daily) if candles_daily else {}
        lines.append(f"stop_hunt_levels[{len(lvls)}]{{level_name,price}}:")
        if lvls:
            lines.append("  " + " |\n  ".join([f"{k},{v:.4f}" for k,v in lvls.items()]))
        else:
            lines.append("  ")
        lines.append("")

        lines.append("# ========== END OF TOON DATA ==========")

        filepath = os.path.join(SYMBOLS_DIR, f"{symbol.lower()}_liquidations.toon")
        content = "\n".join(lines) + "\n"
        if self._atomic_write(filepath, content):
            elapsed = time.time() - start_time
            self._log(f"[SAVE_SUCCESS] {symbol} saved to TOON in {elapsed:.2f}s -> {filepath}")
            return True
        else:
            self._log(f"[SAVE_FAIL] {symbol} could not save file")
            return False

if __name__ == "__main__":
    liq = LiquidationDataREST()
    liq.collect_and_save("BTCUSDT")