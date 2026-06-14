#!/usr/bin/env python3
# E14_regime_expert.py – Complete Market Regime Detection (No Indicators, No Duplication)
# Detects 14+ regime types using price action, volatility, volume, and structure.
# Outputs TSV: {symbol}_E14_regime.tsv

import os
import sys
import time
import math
from collections import defaultdict

# ---------- CONFIGURATION ----------
FEATURES_BASE_DIR = os.path.join("market_data", "binance", "symbols")
LOG_FILE = os.path.join(FEATURES_BASE_DIR, "E14_regime_expert.log")
LOG_MAX_SIZE = 5_000_000

def rotate_log_if_needed():
    if os.path.exists(LOG_FILE) and os.path.getsize(LOG_FILE) > LOG_MAX_SIZE:
        backup = LOG_FILE + ".old"
        try:
            os.replace(LOG_FILE, backup)
        except:
            pass

def log_issue(level, msg, **kwargs):
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    line = f"{ts} [{level}] {msg}"
    if kwargs:
        line += " " + str(kwargs)
    print(line)
    rotate_log_if_needed()
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(line + "\n")

# ---------- Pure Python helpers ----------
def mean(arr):
    return sum(arr) / len(arr) if arr else 0.0

def stdev(arr):
    if len(arr) < 2:
        return 0.0
    avg = mean(arr)
    var = sum((x - avg) ** 2 for x in arr) / len(arr)
    return math.sqrt(var)

def find_swing_points(candles, lookback=2):
    highs = []
    lows = []
    n = len(candles)
    for i in range(lookback, n - lookback):
        is_high = all(candles[i]['high'] > candles[i-j]['high'] for j in range(1, lookback+1)) and \
                  all(candles[i]['high'] > candles[i+j]['high'] for j in range(1, lookback+1))
        if is_high:
            highs.append((candles[i]['timestamp'], candles[i]['high']))
        is_low = all(candles[i]['low'] < candles[i-j]['low'] for j in range(1, lookback+1)) and \
                 all(candles[i]['low'] < candles[i+j]['low'] for j in range(1, lookback+1))
        if is_low:
            lows.append((candles[i]['timestamp'], candles[i]['low']))
    return highs, lows

def compute_atr_wilder(candles, period=14):
    if len(candles) < period + 1:
        return 0.0
    tr = []
    for i in range(1, len(candles)):
        hl = candles[i]['high'] - candles[i]['low']
        hc = abs(candles[i]['high'] - candles[i-1]['close'])
        lc = abs(candles[i]['low'] - candles[i-1]['close'])
        tr.append(max(hl, hc, lc))
    atr = sum(tr[:period]) / period
    for i in range(period, len(tr)):
        atr = (atr * (period-1) + tr[i]) / period
    return atr

def compute_bollinger_bandwidth(candles, period=20, num_std=2):
    if len(candles) < period:
        return 0.0
    closes = [c['close'] for c in candles[-period:]]
    mid = mean(closes)
    if mid == 0:
        return 0.0
    std = stdev(closes)
    upper = mid + num_std * std
    lower = mid - num_std * std
    return (upper - lower) / mid

def compute_obv_slope(candles, period=20):
    if len(candles) < period:
        return 0.0
    obv = 0
    obv_list = []
    for i, c in enumerate(candles):
        if i == 0:
            obv = 0
        else:
            if c['close'] > candles[i-1]['close']:
                obv += c['volume']
            elif c['close'] < candles[i-1]['close']:
                obv -= c['volume']
        obv_list.append(obv)
    if len(obv_list) < period:
        return 0.0
    x = list(range(period))
    y = obv_list[-period:]
    n = period
    sum_x = sum(x)
    sum_y = sum(y)
    sum_xy = sum(x[i]*y[i] for i in range(n))
    sum_x2 = sum(xi*xi for xi in x)
    denom = n * sum_x2 - sum_x * sum_x
    if denom == 0:
        return 0.0
    slope = (n * sum_xy - sum_x * sum_y) / denom
    avg_obv = mean(y) if mean(y) != 0 else 1
    return slope / avg_obv

def compute_chop_index(candles, period=14):
    """Choppiness Index (higher = ranging, lower = trending). Range 0-100."""
    if len(candles) < period + 1:
        return 50.0
    highest_high = max(c['high'] for c in candles[-period:])
    lowest_low = min(c['low'] for c in candles[-period:])
    if highest_high == lowest_low:
        return 100.0
    sum_atr = 0
    for i in range(1, period+1):
        tr = max(candles[-i]['high'] - candles[-i]['low'],
                 abs(candles[-i]['high'] - candles[-i-1]['close']),
                 abs(candles[-i]['low'] - candles[-i-1]['close']))
        sum_atr += tr
    ci = 100 * math.log10(sum_atr / (highest_high - lowest_low)) / math.log10(period)
    return max(0, min(100, ci))

def detect_market_structure(candles):
    """Return (structure, trend_score, trend_direction)"""
    if len(candles) < 20:
        return "RANGE", 0, 0
    highs, lows = find_swing_points(candles, lookback=3)
    if len(highs) < 2 or len(lows) < 2:
        return "RANGE", 0, 0
    recent_highs = highs[-3:] if len(highs) >= 3 else highs
    recent_lows = lows[-3:] if len(lows) >= 3 else lows
    def slope(points):
        if len(points) < 2:
            return 0
        x = list(range(len(points)))
        y = [p[1] for p in points]
        n = len(x)
        sum_x = sum(x)
        sum_y = sum(y)
        sum_xy = sum(x[i]*y[i] for i in range(n))
        sum_x2 = sum(xi*xi for xi in x)
        denom = n * sum_x2 - sum_x * sum_x
        if denom == 0:
            return 0
        return (n * sum_xy - sum_x * sum_y) / denom
    high_slope = slope(recent_highs)
    low_slope = slope(recent_lows)
    trend_score = (high_slope + low_slope) / 2
    if trend_score > 0.1:
        direction = 1
    elif trend_score < -0.1:
        direction = -1
    else:
        direction = 0
    if high_slope > 0 and low_slope > 0:
        return "HH/HL", trend_score, direction
    if high_slope < 0 and low_slope < 0:
        return "LH/LL", trend_score, direction
    return "RANGE", trend_score, direction

def detect_accumulation_distribution(candles):
    """Return ('ACCUMULATION', score) or ('DISTRIBUTION', score) or (None, 0)"""
    if len(candles) < 20:
        return None, 0
    recent_range = max(c['high'] for c in candles[-5:]) - min(c['low'] for c in candles[-5:])
    prev_range = max(c['high'] for c in candles[-10:-5]) - min(c['low'] for c in candles[-10:-5])
    recent_vol = sum(c['volume'] for c in candles[-5:])
    prev_vol = sum(c['volume'] for c in candles[-10:-5])
    obv_slope = compute_obv_slope(candles, 20)
    if prev_range > 0:
        range_ratio = recent_range / prev_range
        vol_ratio = recent_vol / prev_vol if prev_vol > 0 else 1.0
        if range_ratio < 0.7 and vol_ratio < 0.8 and obv_slope > 0.02:
            return "ACCUMULATION", 20
        if range_ratio < 0.7 and vol_ratio > 1.2 and obv_slope < -0.02:
            return "DISTRIBUTION", 20
    return None, 0

def detect_basic_manipulation(candles):
    """Basic wick/fakeout detection – full manipulation handled by E16"""
    if len(candles) < 5:
        return None, 0
    last = candles[-1]
    body = abs(last['close'] - last['open'])
    upper_wick = last['high'] - max(last['open'], last['close'])
    lower_wick = min(last['open'], last['close']) - last['low']
    range_total = last['high'] - last['low']
    if range_total == 0:
        return None, 0
    # Bull trap: long upper wick with close near low
    if upper_wick > 2 * body and upper_wick > 0.5 * range_total:
        return "bull_trap", 15
    # Bear trap: long lower wick with close near high
    if lower_wick > 2 * body and lower_wick > 0.5 * range_total:
        return "bear_trap", 15
    return None, 0

def compute_volume_trend(candles, period=10):
    """Volume trend: rising, falling, neutral"""
    if len(candles) < period:
        return "neutral"
    recent_vol = sum(c['volume'] for c in candles[-period:]) / period
    prev_vol = sum(c['volume'] for c in candles[-period*2:-period]) / period if len(candles) >= period*2 else recent_vol
    if recent_vol > prev_vol * 1.1:
        return "rising"
    elif recent_vol < prev_vol * 0.9:
        return "falling"
    return "neutral"

# ---------- Main Expert ----------
class E14RegimeExpert:
    def __init__(self):
        self.market_type = None

    def detect_market_type(self, symbol):
        sym_upper = symbol.upper()
        if sym_upper.startswith(('BTC', 'ETH', 'BNB', 'SOL', 'XRP', 'ADA', 'DOGE', 'DOT')):
            return 'crypto'
        elif sym_upper.endswith(('USD', 'EUR', 'GBP', 'JPY', 'AUD', 'CAD', 'CHF', 'NZD')):
            return 'forex'
        elif sym_upper.startswith(('XAU', 'XAG', 'GOLD')) or sym_upper in ('OIL', 'BRENT', 'WTI', 'NGAS'):
            return 'commodity'
        return 'crypto'

    def analyze(self, candles, symbol):
        if len(candles) < 50:
            return {"error": "Insufficient data (need at least 50 candles)"}

        # Basic metrics
        price = candles[-1]['close']
        atr = compute_atr_wilder(candles, 14)
        atr_ratio = atr / price if price != 0 else 0
        bb_width = compute_bollinger_bandwidth(candles, 20, 2)
        obv_slope = compute_obv_slope(candles, 20)
        chop = compute_chop_index(candles, 14)
        volume_trend = compute_volume_trend(candles, 10)

        # Structure and trend
        structure, trend_score, trend_dir = detect_market_structure(candles)
        acc_dist_type, acc_dist_score = detect_accumulation_distribution(candles)
        manip_type, manip_score = detect_basic_manipulation(candles)

        # Volatility regime
        avg_atr_50 = compute_atr_wilder(candles[-50:], 14) if len(candles) >= 50 else atr
        vol_ratio = atr / avg_atr_50 if avg_atr_50 != 0 else 1.0
        if vol_ratio > 1.5:
            vol_regime = "high_volatility"
        elif vol_ratio < 0.7:
            vol_regime = "low_volatility"
        else:
            vol_regime = "normal_volatility"

        # Momentum (price change over last 5 candles)
        roc = (candles[-1]['close'] - candles[-5]['close']) / candles[-5]['close'] * 100 if candles[-5]['close'] != 0 else 0
        if roc > 1:
            momentum = "positive"
            mom_strength = min(10, roc)
        elif roc < -1:
            momentum = "negative"
            mom_strength = min(10, abs(roc))
        else:
            momentum = "neutral"
            mom_strength = 0

        # Mean reversion (distance from 20-period SMA)
        sma20 = mean([c['close'] for c in candles[-20:]])
        mr_deviation = (price - sma20) / sma20 * 100 if sma20 != 0 else 0
        if abs(mr_deviation) > 2:
            mean_rev_regime = "active"
        else:
            mean_rev_regime = "inactive"

        # Market phase
        if chop > 60:
            market_phase = "choppy"
        elif bb_width < 0.05:
            market_phase = "ranging"
        elif bb_width > 0.15 and vol_regime == "high_volatility":
            market_phase = "breakout"
        else:
            market_phase = "neutral"

        # Determine regime type (prioritize)
        if acc_dist_type:
            regime = acc_dist_type.lower()
            confidence = 75
            win_prob = 65 if regime == "accumulation" else 35
            loss_prob = 30 if regime == "accumulation" else 60
        elif trend_dir == 1 and trend_score > 0.2:
            if momentum == "positive" and roc > 2:
                regime = "momentum_bull"
            else:
                regime = "bull_market"
            confidence = min(90, 70 + int(abs(trend_score)*50))
            win_prob = 75
            loss_prob = 20
        elif trend_dir == -1 and trend_score < -0.2:
            if momentum == "negative" and roc < -2:
                regime = "momentum_bear"
            else:
                regime = "bear_market"
            confidence = min(90, 70 + int(abs(trend_score)*50))
            win_prob = 20
            loss_prob = 75
        elif chop > 70:
            regime = "chop"
            confidence = 60
            win_prob = 45
            loss_prob = 50
        elif market_phase == "ranging":
            regime = "range_market"
            confidence = 70
            win_prob = 50
            loss_prob = 45
        elif vol_regime == "high_volatility" and market_phase == "breakout":
            regime = "breakout_market"
            confidence = 70
            win_prob = 55
            loss_prob = 40
        elif vol_regime == "low_volatility":
            regime = "calm_market"
            confidence = 60
            win_prob = 55
            loss_prob = 40
        else:
            regime = "neutral"
            confidence = 50
            win_prob = 50
            loss_prob = 45

        # Adjust for mean reversion
        if mean_rev_regime == "active" and regime in ("bull_market", "bear_market", "momentum_bull", "momentum_bear"):
            win_prob = 55
            confidence = max(confidence, 60)

        # Trend continuation detection (pullback in trend)
        if len(candles) >= 10 and abs(trend_score) > 0.15 and abs(roc) < 1:
            regime = "trend_continuation"
            confidence = 65
            win_prob = 60 if trend_dir == 1 else 35
            loss_prob = 35 if trend_dir == 1 else 60

        # Reversal detection (CHoCH style – swing high/lows cross)
        if len(candles) >= 30:
            highs, lows = find_swing_points(candles, lookback=3)
            if len(highs) >= 2 and len(lows) >= 2:
                last_high = highs[-1][1]
                prev_high = highs[-2][1]
                last_low = lows[-1][1]
                prev_low = lows[-2][1]
                if last_high > prev_high and last_low < prev_low:
                    regime = "reversal_up"
                    confidence = 70
                    win_prob = 60
                    loss_prob = 35
                elif last_high < prev_high and last_low > prev_low:
                    regime = "reversal_down"
                    confidence = 70
                    win_prob = 35
                    loss_prob = 60

        # Ensure bounds
        confidence = max(30, min(95, confidence))
        win_prob = max(5, min(95, win_prob))
        loss_prob = max(5, min(95, loss_prob))

        # Trend strength label
        if abs(trend_score) > 0.25:
            trend_strength = "strong"
        elif abs(trend_score) > 0.1:
            trend_strength = "moderate"
        else:
            trend_strength = "weak"

        details = {
            "trend_direction": trend_dir,
            "trend_strength": trend_strength,
            "vol_regime": vol_regime,
            "market_phase": market_phase,
            "manipulation_type": manip_type if manip_type else "none",
            "momentum_regime": momentum + "_momentum" if momentum != "neutral" else "neutral_momentum",
            "mean_rev_regime": mean_rev_regime,
            "adx": abs(trend_score) * 100,
            "bb_width": round(bb_width, 4),
            "atr": round(atr, 2),
            "roc": round(roc, 2),
            "obv_slope": round(obv_slope, 4),
            "distance_from_ma_pct": round(mr_deviation, 2),
            "structure": structure,
            "chop_index": round(chop, 1),
            "volume_trend": volume_trend,
            "total_score": trend_score * 100
        }

        return {
            "market_type": self.detect_market_type(symbol),
            "regime_type": regime,
            "regime_confidence": confidence,
            "win_probability": win_prob,
            "loss_probability": loss_prob,
            "regime_score": int(trend_score * 100),
            "details": details
        }

# ---------- Load candles ----------
def load_candles(symbol, timeframe="1h", limit=300):
    path = os.path.join(FEATURES_BASE_DIR, f"{symbol.lower()}.tmp_x")
    if not os.path.exists(path):
        log_issue("ERROR", f"Candle file not found: {path}")
        return []
    candles = []
    try:
        with open(path, "r") as f:
            for line in f:
                parts = line.strip().split('\t')
                if len(parts) >= 8 and parts[1] == timeframe:
                    ts = int(parts[2])
                    open_p = float(parts[3])
                    high = float(parts[4])
                    low = float(parts[5])
                    close = float(parts[6])
                    volume = float(parts[7])
                    candles.append({'timestamp': ts, 'open': open_p, 'high': high,
                                    'low': low, 'close': close, 'volume': volume})
    except Exception as e:
        log_issue("ERROR", f"Failed to read candles: {e}")
        return []
    candles.sort(key=lambda x: x['timestamp'])
    return candles[-limit:]

# ---------- Run export ----------
def run_expert(symbol):
    log_issue("INFO", f"Starting E14 regime expert for {symbol}")
    candles = load_candles(symbol, timeframe="1h", limit=300)
    if len(candles) < 50:
        log_issue("ERROR", f"Insufficient candles: {len(candles)}")
        out_path = os.path.join(FEATURES_BASE_DIR, f"{symbol.lower()}_E14_regime.tsv")
        with open(out_path, "w") as f:
            header = ["timestamp", "market_type", "regime_type", "regime_confidence", "win_probability",
                      "loss_probability", "regime_score", "trend_direction", "trend_strength", "vol_regime",
                      "market_phase", "manipulation_type", "momentum_regime", "mean_rev_regime", "adx",
                      "bb_width", "atr", "roc", "obv_slope", "distance_from_ma_pct", "structure", "chop_index", "volume_trend"]
            f.write("\t".join(header) + "\n")
            f.write(f"{int(time.time()*1000)}\tunknown\tinsufficient_data\t0\t0\t0\t0\t0\tweak\tnormal\tneutral\tnone\tneutral\tinactive\t0\t0\t0\t0\t0\t0\tunknown\t0\tneutral\n")
        log_issue("INFO", "Saved minimal TSV (no data)")
        return out_path

    expert = E14RegimeExpert()
    result = expert.analyze(candles, symbol)
    if "error" in result:
        log_issue("ERROR", result["error"])
        return None

    details = result["details"]
    out_path = os.path.join(FEATURES_BASE_DIR, f"{symbol.lower()}_E14_regime.tsv")
    with open(out_path, "w") as f:
        header = ["timestamp", "market_type", "regime_type", "regime_confidence", "win_probability",
                  "loss_probability", "regime_score", "trend_direction", "trend_strength", "vol_regime",
                  "market_phase", "manipulation_type", "momentum_regime", "mean_rev_regime", "adx",
                  "bb_width", "atr", "roc", "obv_slope", "distance_from_ma_pct", "structure", "chop_index", "volume_trend"]
        f.write("\t".join(header) + "\n")
        ts_now = int(time.time() * 1000)
        row = [
            str(ts_now),
            result["market_type"],
            result["regime_type"],
            str(result["regime_confidence"]),
            str(result["win_probability"]),
            str(result["loss_probability"]),
            str(result["regime_score"]),
            str(details.get("trend_direction", 0)),
            details.get("trend_strength", "weak"),
            details.get("vol_regime", "normal"),
            details.get("market_phase", "neutral"),
            details.get("manipulation_type", "none"),
            details.get("momentum_regime", "neutral_momentum"),
            details.get("mean_rev_regime", "inactive"),
            f"{details.get('adx', 0):.1f}",
            f"{details.get('bb_width', 0):.4f}",
            f"{details.get('atr', 0):.2f}",
            f"{details.get('roc', 0):.2f}",
            f"{details.get('obv_slope', 0):.4f}",
            f"{details.get('distance_from_ma_pct', 0):.2f}",
            details.get("structure", "unknown"),
            f"{details.get('chop_index', 0):.1f}",
            details.get("volume_trend", "neutral")
        ]
        f.write("\t".join(row) + "\n")
    log_issue("INFO", f"Saved regime expert summary to {out_path}")
    return out_path

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python E14_regime_expert.py SYMBOL")
        sys.exit(1)
    success = run_expert(sys.argv[1].upper())
    sys.exit(0 if success else 1)