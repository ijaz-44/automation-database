# Groups/group_x/X19_volProfile_rest.py
"""
X19 - Volume Profile Module
- Daily Point of Control (POC) – price level with max volume
- Daily Value Area High/Low (70% of volume)
- Untested POC levels from last 7 days (prices not revisited)
Saves to: market_data/binance/symbols/{symbol}_volProfile.tsv
Minimum API calls: 1 per day (fetch 1d candles) + historical if needed.
"""

import requests
import time
import os
import datetime
from collections import defaultdict

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SYMBOLS_DIR = os.path.join(BASE_DIR, "market_data", "binance", "symbols")
os.makedirs(SYMBOLS_DIR, exist_ok=True)

BINANCE_KLINES_URL = "https://api.binance.com/api/v3/klines"

class VolumeProfile:
    def __init__(self):
        self._last_call = 0
        self._rate_limit_sec = 1

    def _rate_limited_fetch(self, url, params=None):
        now = time.time()
        elapsed = now - self._last_call
        if elapsed < self._rate_limit_sec:
            time.sleep(self._rate_limit_sec - elapsed)
        self._last_call = time.time()
        try:
            r = requests.get(url, params=params, timeout=10)
            if r.status_code == 200:
                return r.json()
            else:
                print(f"[X19] HTTP {r.status_code} from {url[:60]}: {r.text[:100]}")
                return None
        except Exception as e:
            print(f"[X19] Request error: {e}")
            return None

    # ---------- Fetch Daily Candles (last 8 days to have 7 complete days) ----------
    def fetch_daily_candles(self, symbol, days=8):
        """Fetch daily OHLCV candles for last 'days' days."""
        params = {"symbol": symbol.upper(), "interval": "1d", "limit": days}
        data = self._rate_limited_fetch(BINANCE_KLINES_URL, params=params)
        if not data:
            return []
        candles = []
        for c in data:
            candles.append({
                "timestamp": c[0],      # ms
                "open": float(c[1]),
                "high": float(c[2]),
                "low": float(c[3]),
                "close": float(c[4]),
                "volume": float(c[5])
            })
        return candles

    # ---------- Compute Volume Profile for a single day (using 1m candles internally) ----------
    def compute_daily_volume_profile(self, symbol, day_timestamp_ms):
        """
        Fetch 1-minute candles for the given day and compute:
        - Point of Control (POC) – price with highest total volume.
        - Value Area High/Low – price range covering 70% of total volume.
        Returns dict with 'poc', 'vah', 'val', 'total_volume'.
        """
        # Convert day timestamp to start of day (UTC)
        day_dt = datetime.datetime.utcfromtimestamp(day_timestamp_ms / 1000)
        start_of_day = int(datetime.datetime(day_dt.year, day_dt.month, day_dt.day, 0, 0, 0).timestamp() * 1000)
        end_of_day = start_of_day + 86400000  # next day

        # Fetch 1-minute candles for that day (Binance limit up to 1440, but we can fetch exactly 1440)
        params = {
            "symbol": symbol.upper(),
            "interval": "1m",
            "startTime": start_of_day,
            "endTime": end_of_day,
            "limit": 1440
        }
        data = self._rate_limited_fetch(BINANCE_KLINES_URL, params=params)
        if not data:
            return None

        # Build volume profile: price -> total volume (using close price as reference)
        vol_profile = defaultdict(float)
        total_vol = 0.0
        for c in data:
            price = float(c[4])  # close price
            vol = float(c[5])
            # Round to 1 decimal place for grouping (avoid too many bins)
            price_rounded = round(price, 1)
            vol_profile[price_rounded] += vol
            total_vol += vol

        if not vol_profile:
            return None

        # Find POC (price with max volume)
        poc = max(vol_profile.items(), key=lambda x: x[1])[0]

        # Compute Value Area (70% of total volume)
        target_vol = total_vol * 0.70
        # Sort price levels by volume descending
        sorted_levels = sorted(vol_profile.items(), key=lambda x: x[1], reverse=True)
        cum_vol = 0
        value_prices = []
        for price, v in sorted_levels:
            cum_vol += v
            value_prices.append(price)
            if cum_vol >= target_vol:
                break
        vah = max(value_prices)
        val = min(value_prices)

        return {
            "poc": poc,
            "vah": vah,
            "val": val,
            "total_volume": total_vol,
            "timestamp": start_of_day
        }

    # ---------- Detect Untested POC Levels (last 7 days) ----------
    def get_untested_pocs(self, symbol, daily_profiles):
        """
        Given list of daily profiles (each with poc, vah, val), return POC levels
        from previous days that were not touched in the last 7 days.
        """
        if not daily_profiles or len(daily_profiles) < 2:
            return []

        # Get price range of last 7 days (lowest low, highest high)
        last_7_days = daily_profiles[:7]  # most recent 7 days
        all_lows = [p['val'] for p in last_7_days]
        all_highs = [p['vah'] for p in last_7_days]
        recent_low = min(all_lows)
        recent_high = max(all_highs)

        # Older profiles (before last 7 days)
        older_profiles = daily_profiles[7:]
        untested = []
        for profile in older_profiles:
            poc = profile['poc']
            # If poc is outside recent price range, it's untested
            if poc < recent_low or poc > recent_high:
                untested.append({
                    "date": datetime.datetime.utcfromtimestamp(profile['timestamp']/1000).strftime("%Y-%m-%d"),
                    "poc": poc,
                    "vah": profile['vah'],
                    "val": profile['val']
                })
        return untested

    # ---------- Main Save Function ----------
    def collect_and_save(self, symbol):
        print(f"[X19] Starting volume profile collection for {symbol}")
        filepath = os.path.join(SYMBOLS_DIR, f"{symbol.lower()}_volProfile.tsv")

        # Fetch daily candles for last 15 days (to have at least 7 days of history + older for untested)
        daily_candles = self.fetch_daily_candles(symbol, days=15)
        if not daily_candles:
            print("[X19] ERROR: No daily candles fetched")
            with open(filepath, 'w') as f:
                f.write("# ERROR: No daily data available\n")
            return

        # Compute volume profile for each day (using 1m candles)
        daily_profiles = []
        print(f"[X19] Computing volume profiles for {len(daily_candles)} days...")
        for i, candle in enumerate(daily_candles):
            day_ts = candle['timestamp']
            print(f"[X19] Processing day {i+1}/{len(daily_candles)} (timestamp {day_ts})...")
            profile = self.compute_daily_volume_profile(symbol, day_ts)
            if profile:
                daily_profiles.append(profile)
            else:
                print(f"[X19] WARNING: Failed to compute profile for day {day_ts}")

        if not daily_profiles:
            print("[X19] ERROR: No volume profiles computed")
            with open(filepath, 'w') as f:
                f.write("# ERROR: No volume profiles\n")
            return

        # Separate recent 7 days and older
        recent_7 = daily_profiles[:7]
        older = daily_profiles[7:]

        # Compute untested POCs from older profiles
        untested = self.get_untested_pocs(symbol, daily_profiles)

        # Write to TSV
        with open(filepath, 'w') as f:
            # Section 1: Daily Snapshots (last 7 days)
            f.write("# ========== DAILY VOLUME PROFILE SNAPSHOTS (Last 7 Days) ==========\n")
            f.write("date\tpoc\tvah\tval\ttotal_volume\n")
            for prof in recent_7:
                date_str = datetime.datetime.utcfromtimestamp(prof['timestamp']/1000).strftime("%Y-%m-%d")
                f.write(f"{date_str}\t{prof['poc']}\t{prof['vah']}\t{prof['val']}\t{prof['total_volume']:.2f}\n")
            f.write("\n")

            # Section 2: Untested POC Levels (from older days, not touched in last 7 days)
            f.write("# ========== UNTESTED POC LEVELS (Last 7 days not touched) ==========\n")
            f.write("date\tpoc\tvah\tval\n")
            if untested:
                for u in untested:
                    f.write(f"{u['date']}\t{u['poc']}\t{u['vah']}\t{u['val']}\n")
            else:
                f.write("NO_UNTESTED_POC\t0\t0\t0\n")

        print(f"[X19] Volume profile data saved to {filepath}")

if __name__ == "__main__":
    vp = VolumeProfile()
    vp.collect_and_save("BTCUSDT")