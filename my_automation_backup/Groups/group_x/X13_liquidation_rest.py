# Groups/group_x/X13_liquidations_rest.py
"""
X13 - Liquidation Data Module (REST, multi‑API fallback)
- Saves ALL data (aggregates, pools, stop hunts, heatmap) into ONE TSV file:
  market_data/binance/symbols/{symbol}_liquidation.tsv
- Step‑by‑step sections with clear markers
- Uses Coinalyze (primary) + CryptoCompare (fallback)
"""

import requests
import time
import os
from collections import defaultdict

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SYMBOLS_DIR = os.path.join(BASE_DIR, "market_data", "binance", "symbols")
os.makedirs(SYMBOLS_DIR, exist_ok=True)

# ---------- API Keys ----------
COINALYZE_API_KEY = "8d7838f9-7111-4d4f-bffd-b83e8d468b60"
CRYPTOCOMPARE_API_KEY = "2f8a33bc64db22db858d7962112cead0ccc07035f9806adb031ad4ce71743d75"

# ---------- URLs ----------
COINALYZE_BASE = "https://api.coinalyze.net/v1"
CRYPTOCOMPARE_BASE = "https://min-api.cryptocompare.com"
BINANCE_PRICE_URL = "https://api.binance.com/api/v3/klines"

class LiquidationDataREST:
    def __init__(self):
        self._last_call = 0
        self._rate_limit_sec = 1.2

    def _rate_limited_fetch(self, url, headers=None, params=None):
        now = time.time()
        elapsed = now - self._last_call
        if elapsed < self._rate_limit_sec:
            time.sleep(self._rate_limit_sec - elapsed)
        self._last_call = time.time()
        try:
            r = requests.get(url, headers=headers, params=params, timeout=15)
            if r.status_code == 200:
                return r.json()
            else:
                print(f"[X13] HTTP {r.status_code} from {url[:60]}: {r.text[:100]}")
                return None
        except Exception as e:
            print(f"[X13] Request error: {e}")
            return None

    # ---------- Coinalyze ----------
    def _fetch_coinalyze(self, symbol, interval, limit=120):
        url = f"{COINALYZE_BASE}/liquidation"
        headers = {"api_key": COINALYZE_API_KEY}
        params = {"symbol": symbol.upper(), "interval": interval, "limit": limit,
                  "fields": "timestamp,longVolume,shortVolume"}
        data = self._rate_limited_fetch(url, headers=headers, params=params)
        if not data or "data" not in data:
            return []
        rows = []
        for item in data["data"]:
            rows.append({
                "timestamp": item["timestamp"],
                "long_volume": float(item.get("longVolume", 0)),
                "short_volume": float(item.get("shortVolume", 0)),
                "total_volume": float(item.get("longVolume", 0)) + float(item.get("shortVolume", 0))
            })
        return rows

    # ---------- CryptoCompare (fallback) ----------
    def _fetch_cryptocompare(self, symbol, interval, limit=120):
        # Map interval: "1m", "15m", "1h" -> CryptoCompare uses "histominute", "histohour", etc.
        # But for liquidations, they have /data/v1/liquidations endpoint
        url = f"{CRYPTOCOMPARE_BASE}/data/v1/liquidations"
        params = {
            "symbol": symbol.upper(),
            "limit": limit,
            "api_key": CRYPTOCOMPARE_API_KEY
        }
        # CryptoCompare liquidation endpoint returns list of liquidation events
        data = self._rate_limited_fetch(url, params=params)
        if not data or "Data" not in data:
            return []
        # We need to aggregate by time interval (1m, 15m, 1h) ourselves
        # For simplicity, we use the raw events and then bin them.
        events = []
        for item in data["Data"]:
            events.append({
                "timestamp": item.get("timestamp", 0),
                "long_volume": float(item.get("longLiquidatedUsd", 0)),
                "short_volume": float(item.get("shortLiquidatedUsd", 0))
            })
        # Bin events into requested interval
        interval_ms = {"1m": 60*1000, "15m": 15*60*1000, "1h": 60*60*1000}[interval]
        now = int(time.time() * 1000)
        bins = {}
        for e in events:
            ts = e["timestamp"]
            # Align to interval start
            bucket = (ts // interval_ms) * interval_ms
            if bucket not in bins:
                bins[bucket] = {"long": 0.0, "short": 0.0}
            bins[bucket]["long"] += e["long_volume"]
            bins[bucket]["short"] += e["short_volume"]
        # Take last 'limit' buckets
        sorted_buckets = sorted(bins.items(), key=lambda x: x[0])[-limit:]
        rows = []
        for bucket, vols in sorted_buckets:
            rows.append({
                "timestamp": bucket,
                "long_volume": vols["long"],
                "short_volume": vols["short"],
                "total_volume": vols["long"] + vols["short"]
            })
        return rows

    # ---------- Master fetch (try Coinalyze, fallback CryptoCompare) ----------
    def _fetch_liquidations(self, symbol, interval, limit=120):
        rows = self._fetch_coinalyze(symbol, interval, limit)
        if rows:
            print(f"[X13] Coinalyze OK for {symbol} {interval}")
            return rows
        rows = self._fetch_cryptocompare(symbol, interval, limit)
        if rows:
            print(f"[X13] CryptoCompare OK for {symbol} {interval}")
            return rows
        print(f"[X13] No liquidation data for {symbol} {interval}")
        return []

    # ---------- Price candles (from Binance) ----------
    def _fetch_price_candles(self, symbol, interval="1m", limit=120):
        try:
            r = requests.get(BINANCE_PRICE_URL,
                             params={"symbol": symbol.upper(), "interval": interval, "limit": limit},
                             timeout=10)
            if r.status_code == 200:
                data = r.json()
                candles = []
                for c in data:
                    candles.append({
                        'timestamp': c[0],
                        'high': float(c[2]),
                        'low': float(c[3]),
                        'volume': float(c[5])
                    })
                return candles
        except Exception as e:
            print(f"[X13] Price fetch error: {e}")
        return []

    # ---------- Save everything into ONE file ----------
    def collect_and_save(self, symbol):
        print(f"[X13] Collecting liquidation data for {symbol}")
        filepath = os.path.join(SYMBOLS_DIR, f"{symbol.lower()}_liquidation.tsv")

        # We'll write sections with markers
        with open(filepath, 'w') as f:
            # ========== SECTION 1: LIQUIDATION AGGREGATES (1m, 15m, 1h) ==========
            f.write("# ========== LIQUIDATION AGGREGATES (1m, 15m, 1h) ==========\n")
            f.write("interval\ttimestamp\tlong_volume\tshort_volume\ttotal_volume\n")

            for interval in ["1m", "15m", "1h"]:
                rows = self._fetch_liquidations(symbol, interval, limit=120)
                for r in rows:
                    f.write(f"{interval}\t{r['timestamp']}\t{r['long_volume']}\t{r['short_volume']}\t{r['total_volume']}\n")
                if rows:
                    print(f"[X13] Wrote {len(rows)} rows for {interval}")
                else:
                    f.write(f"{interval}\t0\t0\t0\t0\n")
            f.write("\n")

            # ========== SECTION 2: LIQUIDITY HEATMAP (price bins) ==========
            f.write("# ========== LIQUIDITY HEATMAP (price bins from volume) ==========\n")
            f.write("price_bin\tvolume\n")
            heatmap = self._compute_heatmap(symbol)
            for price_bin, vol in sorted(heatmap.items()):
                f.write(f"{price_bin}\t{vol}\n")
            if not heatmap:
                f.write("NO_DATA\t0\n")
            f.write("\n")

            # ========== SECTION 3: LIQUIDITY POOLS (equal highs/lows) ==========
            f.write("# ========== LIQUIDITY POOLS (equal highs/lows) ==========\n")
            f.write("type\tlevel\tcount\n")
            high_pools, low_pools = self._compute_equal_highs_lows(symbol)
            for level, cnt in high_pools:
                f.write(f"high\t{level}\t{cnt}\n")
            for level, cnt in low_pools:
                f.write(f"low\t{level}\t{cnt}\n")
            if not high_pools and not low_pools:
                f.write("NO_DATA\t0\t0\n")
            f.write("\n")

            # ========== SECTION 4: STOP HUNT LEVELS (previous day/week) ==========
            f.write("# ========== STOP HUNT LEVELS (previous day/week highs/lows) ==========\n")
            f.write("level_name\tprice\n")
            levels = self._compute_stop_hunt_levels(symbol)
            for name, price in levels.items():
                f.write(f"{name}\t{price}\n")
            if not levels:
                f.write("NO_DATA\t0\n")

        print(f"[X13] All data saved to {filepath}")

    # ---------- Helper methods for derived data ----------
    def _compute_heatmap(self, symbol, price_bin_size=100):
        candles = self._fetch_price_candles(symbol, "1m", 120)
        if not candles:
            return {}
        heatmap = defaultdict(float)
        for c in candles:
            low, high, vol = c['low'], c['high'], c['volume']
            if high <= low:
                continue
            steps = max(5, int((high - low) / price_bin_size) + 1)
            step_price = (high - low) / steps
            for i in range(steps):
                price = low + i * step_price
                bin_key = int(price // price_bin_size) * price_bin_size
                heatmap[bin_key] += vol / steps
        return dict(heatmap)

    def _compute_equal_highs_lows(self, symbol, lookback_minutes=120, tolerance=0.001):
        candles = self._fetch_price_candles(symbol, "1m", limit=lookback_minutes)
        if not candles:
            return [], []
        highs = [c['high'] for c in candles]
        lows = [c['low'] for c in candles]
        def cluster(values):
            clusters = []
            for v in sorted(values):
                found = False
                for cl in clusters:
                    if abs(v - cl[0]) / cl[0] <= tolerance:
                        cl.append(v)
                        found = True
                        break
                if not found:
                    clusters.append([v])
            return [(round(sum(c)/len(c), 4), len(c)) for c in clusters if len(c) >= 2]
        return cluster(highs), cluster(lows)

    def _compute_stop_hunt_levels(self, symbol):
        daily = self._fetch_price_candles(symbol, "1d", limit=7)
        if len(daily) < 2:
            return {}
        prev_day = daily[-2]
        prev_high = prev_day['high']
        prev_low = prev_day['low']
        weekly_high = max(c['high'] for c in daily)
        weekly_low = min(c['low'] for c in daily)
        return {
            'prev_day_high': prev_high,
            'prev_day_low': prev_low,
            'prev_week_high': weekly_high,
            'prev_week_low': weekly_low,
        }

if __name__ == "__main__":
    liq = LiquidationDataREST()
    liq.collect_and_save("BTCUSDT")