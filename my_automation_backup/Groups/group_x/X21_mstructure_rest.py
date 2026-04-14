# Groups/group_x/X21_mstructure_rest.py
"""
X21 - Market Structure Module
- Swing Highs/Lows (last 48 hours, 15m candles)
- Key Support/Resistance Levels (last 120 1h candles)
- Institutional Supply/Demand Zones (last 120 1h candles)
- Fair Value Gaps (FVG) – 15m candles, last 120
- Institutional Order Blocks (1h, last 120)
- Fakeout / Trap detection (price rejection + volume)
Saves to: market_data/binance/symbols/{symbol}_mstructure.tsv
"""

import requests
import time
import os
import math
from collections import defaultdict

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SYMBOLS_DIR = os.path.join(BASE_DIR, "market_data", "binance", "symbols")
os.makedirs(SYMBOLS_DIR, exist_ok=True)

BINANCE_KLINES_URL = "https://api.binance.com/api/v3/klines"

class MarketStructure:
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
                print(f"[X21] HTTP {r.status_code} from {url[:60]}: {r.text[:100]}")
                return None
        except Exception as e:
            print(f"[X21] Request error: {e}")
            return None

    # ---------- Fetch Candles ----------
    def fetch_candles(self, symbol, interval, limit):
        params = {"symbol": symbol.upper(), "interval": interval, "limit": limit}
        data = self._rate_limited_fetch(BINANCE_KLINES_URL, params=params)
        if not data:
            return []
        candles = []
        for c in data:
            candles.append({
                "timestamp": c[0],
                "open": float(c[1]),
                "high": float(c[2]),
                "low": float(c[3]),
                "close": float(c[4]),
                "volume": float(c[5])
            })
        return candles

    # ---------- 1. Swing Highs/Lows (15m, last 48h = 192 candles) ----------
    def find_swing_points(self, candles, lookback=2):
        """
        Detect swing highs and lows.
        A swing high: high is higher than 'lookback' candles on both sides.
        A swing low: low is lower than 'lookback' candles on both sides.
        """
        if len(candles) < 2*lookback + 1:
            return [], []
        highs = []
        lows = []
        for i in range(lookback, len(candles) - lookback):
            is_high = True
            is_low = True
            for j in range(1, lookback+1):
                if candles[i]['high'] <= candles[i-j]['high'] or candles[i]['high'] <= candles[i+j]['high']:
                    is_high = False
                if candles[i]['low'] >= candles[i-j]['low'] or candles[i]['low'] >= candles[i+j]['low']:
                    is_low = False
            if is_high:
                highs.append({
                    "timestamp": candles[i]['timestamp'],
                    "price": candles[i]['high'],
                    "type": "swing_high"
                })
            if is_low:
                lows.append({
                    "timestamp": candles[i]['timestamp'],
                    "price": candles[i]['low'],
                    "type": "swing_low"
                })
        return highs, lows

    # ---------- 2. Key S/R Levels (1h, last 120 candles) ----------
    def find_sr_levels(self, candles, tolerance=0.002):
        """
        Identify support/resistance levels where price has reversed multiple times.
        Group nearby prices (within tolerance %).
        """
        # Extract all swing points from 1h candles
        highs, lows = self.find_swing_points(candles, lookback=2)
        all_points = highs + lows
        if not all_points:
            return []
        # Group by price
        groups = defaultdict(list)
        for p in all_points:
            price = p['price']
            # Find group within tolerance
            found = False
            for key in list(groups.keys()):
                if abs(price - key) / key <= tolerance:
                    groups[key].append(p)
                    found = True
                    break
            if not found:
                groups[price].append(p)
        # Keep only levels with at least 2 touches
        levels = []
        for price, touches in groups.items():
            if len(touches) >= 2:
                levels.append({
                    "price": round(price, 2),
                    "touches": len(touches),
                    "type": "resistance" if touches[0]['type'] == "swing_high" else "support"
                })
        return levels

    # ---------- 3. Institutional Supply/Demand Zones (1h, last 120) ----------
    def find_supply_demand_zones(self, candles, lookback=3, zone_width=0.005):
        """
        Supply zone: area where price dropped sharply after a rally (sell pressure).
        Demand zone: area where price rose sharply after a drop (buy pressure).
        Using 1h candles.
        """
        zones = []
        for i in range(lookback, len(candles) - lookback):
            # Look for a strong move (momentum)
            if candles[i]['close'] > candles[i-1]['close'] * 1.005:  # bullish move
                # Demand zone: the low of the candle before the move
                zone_high = candles[i-1]['high']
                zone_low = candles[i-1]['low']
                # Expand zone width
                range_zone = (zone_high - zone_low) * zone_width
                zones.append({
                    "type": "demand",
                    "start_time": candles[i-1]['timestamp'],
                    "end_time": candles[i]['timestamp'],
                    "high": zone_high + range_zone,
                    "low": zone_low - range_zone,
                    "strength": (candles[i]['close'] - candles[i-1]['close']) / candles[i-1]['close']
                })
            elif candles[i-1]['close'] > candles[i]['close'] * 1.005:  # bearish move
                # Supply zone: the high of the candle before the drop
                zone_high = candles[i-1]['high']
                zone_low = candles[i-1]['low']
                range_zone = (zone_high - zone_low) * zone_width
                zones.append({
                    "type": "supply",
                    "start_time": candles[i-1]['timestamp'],
                    "end_time": candles[i]['timestamp'],
                    "high": zone_high + range_zone,
                    "low": zone_low - range_zone,
                    "strength": (candles[i-1]['close'] - candles[i]['close']) / candles[i]['close']
                })
        # Keep last 10 most recent zones
        zones = zones[-10:]
        return zones

    # ---------- 4. Fair Value Gaps (FVG) – 15m, last 120 candles ----------
    def find_fvg(self, candles):
        """
        FVG occurs when the high of one candle is below the low of the next candle (bullish)
        or the low of one candle is above the high of the next candle (bearish).
        Using 15m candles.
        """
        fvgs = []
        for i in range(1, len(candles) - 1):
            # Bullish FVG: current low > previous high
            if candles[i]['low'] > candles[i-1]['high']:
                fvgs.append({
                    "type": "bullish",
                    "time": candles[i]['timestamp'],
                    "gap_top": candles[i]['low'],
                    "gap_bottom": candles[i-1]['high']
                })
            # Bearish FVG: current high < previous low
            elif candles[i]['high'] < candles[i-1]['low']:
                fvgs.append({
                    "type": "bearish",
                    "time": candles[i]['timestamp'],
                    "gap_top": candles[i-1]['low'],
                    "gap_bottom": candles[i]['high']
                })
        return fvgs

    # ---------- 5. Institutional Order Blocks (1h, last 120) ----------
    def find_order_blocks(self, candles, lookback=3):
        """
        Order block: last candle before a strong move (imbalance).
        Bullish OB: last bearish candle before a bullish breakout.
        Bearish OB: last bullish candle before a bearish breakdown.
        """
        blocks = []
        for i in range(lookback, len(candles) - lookback):
            # Bullish order block: previous candle is bearish, then a strong bullish candle
            if candles[i-1]['close'] < candles[i-1]['open'] and candles[i]['close'] > candles[i]['open'] * 1.005:
                blocks.append({
                    "type": "bullish",
                    "timestamp": candles[i-1]['timestamp'],
                    "high": candles[i-1]['high'],
                    "low": candles[i-1]['low'],
                    "strength": (candles[i]['close'] - candles[i-1]['close']) / candles[i-1]['close']
                })
            # Bearish order block: previous candle is bullish, then a strong bearish candle
            elif candles[i-1]['close'] > candles[i-1]['open'] and candles[i]['close'] < candles[i]['open'] * 0.995:
                blocks.append({
                    "type": "bearish",
                    "timestamp": candles[i-1]['timestamp'],
                    "high": candles[i-1]['high'],
                    "low": candles[i-1]['low'],
                    "strength": (candles[i-1]['close'] - candles[i]['close']) / candles[i]['close']
                })
        return blocks[-10:]

    # ---------- 6. Fakeout / Trap Detection ----------
    def detect_fakeouts(self, candles_15m, candles_1h):
        """
        Fakeout: price breaks a swing high/low but immediately reverses (wick rejection).
        Using 15m candles for precision.
        """
        fakeouts = []
        # First get swing points from 15m
        highs, lows = self.find_swing_points(candles_15m, lookback=2)
        all_swings = highs + lows
        # For each swing, check if price moved beyond it but then closed back inside
        for i in range(1, len(candles_15m) - 1):
            for swing in all_swings:
                if abs(swing['timestamp'] - candles_15m[i]['timestamp']) < 15*60*1000:  # within 15 min
                    continue
                # Check for fakeout above swing high
                if swing['type'] == 'swing_high':
                    if candles_15m[i]['high'] > swing['price'] and candles_15m[i]['close'] < swing['price']:
                        fakeouts.append({
                            "type": "fakeout_high",
                            "timestamp": candles_15m[i]['timestamp'],
                            "level": swing['price'],
                            "rejection": candles_15m[i]['high'] - swing['price']
                        })
                # Check for fakeout below swing low
                elif swing['type'] == 'swing_low':
                    if candles_15m[i]['low'] < swing['price'] and candles_15m[i]['close'] > swing['price']:
                        fakeouts.append({
                            "type": "fakeout_low",
                            "timestamp": candles_15m[i]['timestamp'],
                            "level": swing['price'],
                            "rejection": swing['price'] - candles_15m[i]['low']
                        })
        return fakeouts[-20:]

    # ---------- Main Save Function ----------
    def collect_and_save(self, symbol):
        print(f"[X21] Starting market structure analysis for {symbol}")
        filepath = os.path.join(SYMBOLS_DIR, f"{symbol.lower()}_mstructure.tsv")

        # Fetch data
        print("[X21] Fetching 15m candles (48 hours = 192 candles)...")
        candles_15m = self.fetch_candles(symbol, "15m", 192)
        if not candles_15m:
            print("[X21] ERROR: No 15m data")
            return

        print("[X21] Fetching 1h candles (5 days = 120 candles)...")
        candles_1h = self.fetch_candles(symbol, "1h", 120)
        if not candles_1h:
            print("[X21] ERROR: No 1h data")
            return

        # Compute all structures
        print("[X21] Computing swing highs/lows (15m)...")
        swing_highs, swing_lows = self.find_swing_points(candles_15m, lookback=2)

        print("[X21] Computing key S/R levels (1h)...")
        sr_levels = self.find_sr_levels(candles_1h, tolerance=0.002)

        print("[X21] Computing supply/demand zones (1h)...")
        sd_zones = self.find_supply_demand_zones(candles_1h, lookback=3)

        print("[X21] Computing Fair Value Gaps (15m)...")
        fvgs = self.find_fvg(candles_15m)

        print("[X21] Computing order blocks (1h)...")
        order_blocks = self.find_order_blocks(candles_1h)

        print("[X21] Detecting fakeouts/traps (15m)...")
        fakeouts = self.detect_fakeouts(candles_15m, candles_1h)

        # Write to TSV
        with open(filepath, 'w') as f:
            # Section 1: Swing Highs/Lows (last 48h)
            f.write("# ========== SWING HIGHS & LOWS (15m, last 48 hours) ==========\n")
            f.write("type\ttimestamp\tprice\n")
            for sh in swing_highs:
                f.write(f"swing_high\t{sh['timestamp']}\t{sh['price']}\n")
            for sl in swing_lows:
                f.write(f"swing_low\t{sl['timestamp']}\t{sl['price']}\n")
            if not swing_highs and not swing_lows:
                f.write("NO_SWING_POINTS\t0\t0\n")
            f.write("\n")

            # Section 2: Key S/R Levels (1h, last 120)
            f.write("# ========== KEY SUPPORT/RESISTANCE LEVELS (1h, last 120 candles) ==========\n")
            f.write("type\tprice\ttouches\n")
            for lvl in sr_levels:
                f.write(f"{lvl['type']}\t{lvl['price']}\t{lvl['touches']}\n")
            if not sr_levels:
                f.write("NO_SR_LEVELS\t0\t0\n")
            f.write("\n")

            # Section 3: Supply/Demand Zones (1h)
            f.write("# ========== INSTITUTIONAL SUPPLY/DEMAND ZONES (1h) ==========\n")
            f.write("type\ttimestamp\thigh\tlow\tstrength\n")
            for z in sd_zones:
                f.write(f"{z['type']}\t{z['start_time']}\t{z['high']}\t{z['low']}\t{z['strength']:.4f}\n")
            if not sd_zones:
                f.write("NO_ZONES\t0\t0\t0\t0\n")
            f.write("\n")

            # Section 4: Fair Value Gaps (15m)
            f.write("# ========== FAIR VALUE GAPS (FVG) – 15m, last 120 candles ==========\n")
            f.write("type\ttimestamp\tgap_top\tgap_bottom\n")
            for fvg in fvgs:
                f.write(f"{fvg['type']}\t{fvg['time']}\t{fvg['gap_top']}\t{fvg['gap_bottom']}\n")
            if not fvgs:
                f.write("NO_FVG\t0\t0\t0\n")
            f.write("\n")

            # Section 5: Institutional Order Blocks (1h)
            f.write("# ========== INSTITUTIONAL ORDER BLOCKS (1h, last 120) ==========\n")
            f.write("type\ttimestamp\thigh\tlow\tstrength\n")
            for ob in order_blocks:
                f.write(f"{ob['type']}\t{ob['timestamp']}\t{ob['high']}\t{ob['low']}\t{ob['strength']:.4f}\n")
            if not order_blocks:
                f.write("NO_ORDER_BLOCKS\t0\t0\t0\t0\n")
            f.write("\n")

            # Section 6: Fakeouts / Traps
            f.write("# ========== FAKEOUTS & TRAPS (price rejection at swing levels) ==========\n")
            f.write("type\ttimestamp\tlevel\trejection\n")
            for fo in fakeouts:
                f.write(f"{fo['type']}\t{fo['timestamp']}\t{fo['level']}\t{fo['rejection']}\n")
            if not fakeouts:
                f.write("NO_FAKEOUTS\t0\t0\t0\n")

        print(f"[X21] Market structure data saved to {filepath}")

if __name__ == "__main__":
    ms = MarketStructure()
    ms.collect_and_save("BTCUSDT")