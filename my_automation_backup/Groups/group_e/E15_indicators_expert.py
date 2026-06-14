#!/usr/bin/env python3
# E15_indicators_expert.py – Pure Python High‑Win‑Rate Indicator Expert (No JSON)
# Reads real candles from X01 .tmp_x, computes indicators, outputs TSV summary.

import os
import sys
import time
import math

# ========================== CONFIGURATION ==========================
FEATURES_BASE_DIR = os.path.join("market_data", "binance", "symbols")
LOG_FILE = os.path.join(FEATURES_BASE_DIR, "E15_indicators_expert.log")
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

# ========================== INDICATORS EXPERT CLASS ==========================
class IndicatorsExpert:
    """
    High‑confidence directional signal generator (target win rate >85% in strong trends).
    Uses multiple indicators and a consensus‑based scoring system.
    """

    @staticmethod
    def ema(prices, period):
        if not prices:
            return 0.0
        alpha = 2.0 / (period + 1)
        ema_val = prices[0]
        for i in range(1, len(prices)):
            ema_val = (prices[i] - ema_val) * alpha + ema_val
        return ema_val

    @staticmethod
    def sma(prices, period):
        if len(prices) < period:
            return sum(prices) / len(prices) if prices else 0.0
        return sum(prices[-period:]) / period

    @staticmethod
    def rsi(prices, period=14):
        if len(prices) < period + 1:
            return 50.0
        gains = 0.0
        losses = 0.0
        for i in range(-period, 0):
            change = prices[i] - prices[i-1]
            if change > 0:
                gains += change
            else:
                losses -= change
        if losses == 0:
            return 100.0
        rs = gains / losses
        rsi = 100 - (100 / (1 + rs))
        return rsi

    @staticmethod
    def stochastic(high, low, close, k_period=14, d_period=3):
        if len(close) < k_period:
            return 50.0, 50.0
        highest = max(high[-k_period:])
        lowest = min(low[-k_period:])
        if highest == lowest:
            k = 50.0
        else:
            k = 100 * (close[-1] - lowest) / (highest - lowest)
        return k, k

    @staticmethod
    def macd(prices, fast=12, slow=26, signal=9):
        if len(prices) < slow + signal:
            return 0.0, 0.0, 0.0
        ema_fast = IndicatorsExpert.ema(prices, fast)
        ema_slow = IndicatorsExpert.ema(prices, slow)
        macd_line = ema_fast - ema_slow
        return macd_line, 0.0, 0.0

    @staticmethod
    def bollinger_bands(prices, period=20, num_std=2):
        if len(prices) < period:
            mid = prices[-1] if prices else 0
            return mid, mid, mid, 0.0
        mid = sum(prices[-period:]) / period
        variance = sum((x - mid) ** 2 for x in prices[-period:]) / period
        std = math.sqrt(variance)
        upper = mid + num_std * std
        lower = mid - num_std * std
        bandwidth = (upper - lower) / mid if mid != 0 else 0
        return upper, mid, lower, bandwidth

    @staticmethod
    def atr(high, low, close, period=14):
        if len(close) < period + 1:
            return 0.0
        tr_values = []
        for i in range(1, len(close)):
            hl = high[i] - low[i]
            hc = abs(high[i] - close[i-1])
            lc = abs(low[i] - close[i-1])
            tr = max(hl, hc, lc)
            tr_values.append(tr)
        if period > len(tr_values):
            return sum(tr_values) / len(tr_values)
        return sum(tr_values[-period:]) / period

    @staticmethod
    def obv(close, volume):
        if len(close) < 2:
            return 0, 0
        obv = [0.0] * len(close)
        obv[0] = volume[0]
        for i in range(1, len(close)):
            if close[i] > close[i-1]:
                obv[i] = obv[i-1] + volume[i]
            elif close[i] < close[i-1]:
                obv[i] = obv[i-1] - volume[i]
            else:
                obv[i] = obv[i-1]
        if len(obv) >= 5:
            slope = (obv[-1] - obv[-5]) / max(1, abs(obv[-5]))
        else:
            slope = 0
        return obv[-1], slope

    @staticmethod
    def vwap(high, low, close, volume):
        if not volume:
            return 0
        return (high[-1] + low[-1] + close[-1]) / 3

    @staticmethod
    def rate_of_change(prices, period=5):
        if len(prices) < period + 1:
            return 0.0
        return (prices[-1] - prices[-1-period]) / prices[-1-period] * 100

    @staticmethod
    def trend_strength(high, low, close, period=20):
        if len(close) < period * 2:
            return 0
        up_moves = []
        down_moves = []
        for i in range(1, len(close)):
            up = high[i] - high[i-1]
            down = low[i-1] - low[i]
            up_moves.append(up if up > 0 else 0)
            down_moves.append(down if down > 0 else 0)
        tr = [max(high[i]-low[i], abs(high[i]-close[i-1]), abs(low[i]-close[i-1])) for i in range(1, len(close))]
        def wilder_smooth(series, period):
            if not series:
                return []
            smoothed = [series[0]]
            for val in series[1:]:
                smoothed.append(smoothed[-1] * (1 - 1/period) + val / period)
            return smoothed
        tr_smooth = wilder_smooth(tr, period)
        up_smooth = wilder_smooth(up_moves, period)
        down_smooth = wilder_smooth(down_moves, period)
        plus_di = [100 * u / t if t != 0 else 0 for u, t in zip(up_smooth, tr_smooth)]
        minus_di = [100 * d / t if t != 0 else 0 for d, t in zip(down_smooth, tr_smooth)]
        dx = [100 * abs(p - m) / max(p + m, 0.0001) for p, m in zip(plus_di, minus_di)]
        adx = wilder_smooth(dx, period)
        return adx[-1] if adx else 0

    @staticmethod
    def get_signal(high, low, close, volume, symbol=None, timeframe=None):
        if len(close) < 30:
            return {"error": "Insufficient data", "direction": "NEUTRAL", "confidence": 0}

        ema20 = IndicatorsExpert.ema(close, 20)
        ema50 = IndicatorsExpert.ema(close, 50)
        sma20 = IndicatorsExpert.sma(close, 20)
        rsi_val = IndicatorsExpert.rsi(close, 14)
        k_val, d_val = IndicatorsExpert.stochastic(high, low, close, 14, 3)
        macd_line, signal_line, hist = IndicatorsExpert.macd(close, 12, 26, 9)
        bb_upper, bb_mid, bb_lower, bb_width = IndicatorsExpert.bollinger_bands(close, 20, 2)
        atr_val = IndicatorsExpert.atr(high, low, close, 14)
        obv_val, obv_slope = IndicatorsExpert.obv(close, volume)
        roc = IndicatorsExpert.rate_of_change(close, 5)
        adx_val = IndicatorsExpert.trend_strength(high, low, close, 14)

        last_close = close[-1]

        bullish = 0
        bearish = 0
        trig_signals = []

        if ema20 > ema50:
            bullish += 20
            trig_signals.append("EMA20>50")
        elif ema20 < ema50:
            bearish += 20
            trig_signals.append("EMA20<50")
        if last_close > ema20:
            bullish += 15
            trig_signals.append("price>EMA20")
        elif last_close < ema20:
            bearish += 15
            trig_signals.append("price<EMA20")

        if rsi_val > 70:
            bearish += 15
            trig_signals.append(f"RSI overbought ({rsi_val:.1f})")
        elif rsi_val < 30:
            bullish += 15
            trig_signals.append(f"RSI oversold ({rsi_val:.1f})")

        if k_val > 80:
            bearish += 10
            trig_signals.append(f"Stoch overbought (K={k_val:.0f})")
        elif k_val < 20:
            bullish += 10
            trig_signals.append(f"Stoch oversold (K={k_val:.0f})")

        if macd_line > 0 and macd_line > signal_line:
            bullish += 10
            trig_signals.append("MACD bullish crossover")
        elif macd_line < 0 and macd_line < signal_line:
            bearish += 10
            trig_signals.append("MACD bearish crossover")

        if last_close > bb_upper:
            bearish += 10
            trig_signals.append("price above upper band")
        elif last_close < bb_lower:
            bullish += 10
            trig_signals.append("price below lower band")
        if bb_width < 0.05:
            trig_signals.append("squeeze (low volatility)")

        if obv_slope > 0.01:
            bullish += 10
            trig_signals.append("OBV rising")
        elif obv_slope < -0.01:
            bearish += 10
            trig_signals.append("OBV falling")

        if roc > 1:
            bullish += 5
            trig_signals.append(f"ROC positive ({roc:.1f}%)")
        elif roc < -1:
            bearish += 5
            trig_signals.append(f"ROC negative ({roc:.1f}%)")

        strong_trend = adx_val > 25
        if strong_trend:
            if bullish > bearish:
                bull_adjust = min(20, bullish // 2)
                bullish += bull_adjust
                trig_signals.append(f"strong uptrend (ADX={adx_val:.1f})")
            elif bearish > bullish:
                bear_adjust = min(20, bearish // 2)
                bearish += bear_adjust
                trig_signals.append(f"strong downtrend (ADX={adx_val:.1f})")

        net_score = bullish - bearish
        net_score = max(-100, min(100, net_score))

        if net_score >= 25:
            direction = "UP"
            confidence = min(95, 60 + net_score // 2)
            high_prob = "UP" if confidence >= 85 else None
        elif net_score <= -25:
            direction = "DOWN"
            confidence = min(95, 60 + abs(net_score) // 2)
            high_prob = "DOWN" if confidence >= 85 else None
        else:
            direction = "NEUTRAL"
            confidence = 50 + abs(net_score) // 2 if net_score else 50
            high_prob = None

        reason = f"Net score {net_score:+d}, signals: {', '.join(trig_signals[:3])}"

        return {
            "direction": direction,
            "bias": direction,
            "confidence": confidence,
            "high_prob_scenario": high_prob,
            "probability_estimate": confidence,
            "reason": reason,
            "signals": trig_signals,
            "net_score": net_score,
            "indicator_values": {
                "rsi": round(rsi_val, 1),
                "macd_line": round(macd_line, 4),
                "bb_width": round(bb_width, 4),
                "atr": round(atr_val, 4),
                "adx": round(adx_val, 1),
                "roc": round(roc, 1)
            }
        }

# ========================== LOAD REAL CANDLES ==========================
def load_candles(symbol, timeframe="1h", limit=300):
    path = os.path.join(FEATURES_BASE_DIR, f"{symbol.lower()}.tmp_x")
    if not os.path.exists(path):
        log_issue("ERROR", f"Candle file not found: {path}")
        return [], [], [], []
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
                    candles.append({'ts': ts, 'open': open_p, 'high': high,
                                    'low': low, 'close': close, 'volume': volume})
    except Exception as e:
        log_issue("ERROR", f"Failed to read candle file: {e}")
        return [], [], [], []
    if len(candles) < 30:
        log_issue("WARNING", f"Only {len(candles)} {timeframe} candles, may be insufficient")
    candles.sort(key=lambda x: x['ts'])
    candles = candles[-limit:]
    high = [c['high'] for c in candles]
    low = [c['low'] for c in candles]
    close = [c['close'] for c in candles]
    volume = [c['volume'] for c in candles]
    return high, low, close, volume

# ========================== MAIN EXPERT FUNCTION ==========================
def run_expert(symbol, timeframe="1h"):
    log_issue("INFO", f"Starting E15 indicator expert for {symbol} ({timeframe})")
    high, low, close, volume = load_candles(symbol, timeframe, limit=300)
    if not close or len(close) < 30:
        log_issue("ERROR", "Insufficient candle data")
        # Create minimal TSV with neutral signal
        out_path = os.path.join(FEATURES_BASE_DIR, f"{symbol.lower()}_E15_indicators.tsv")
        with open(out_path, "w") as f:
            header = ["timestamp", "direction", "confidence", "high_prob_scenario", "probability_estimate",
                      "reason", "signals", "net_score", "rsi", "macd_line", "bb_width", "atr", "adx", "roc"]
            f.write("\t".join(header) + "\n")
            ts_now = int(time.time() * 1000)
            row = [str(ts_now), "NEUTRAL", "50", "", "50", "Insufficient data", "", "0", "50", "0", "0", "0", "0", "0"]
            f.write("\t".join(row) + "\n")
        log_issue("INFO", f"Saved neutral indicator summary to {out_path}")
        return out_path

    result = IndicatorsExpert.get_signal(high, low, close, volume, symbol, timeframe)
    if "error" in result:
        log_issue("ERROR", f"Indicator error: {result['error']}")
        return None

    signals_str = "|".join(result['signals']) if result['signals'] else ""
    ind = result['indicator_values']
    out_path = os.path.join(FEATURES_BASE_DIR, f"{symbol.lower()}_E15_indicators.tsv")
    with open(out_path, "w") as f:
        header = ["timestamp", "direction", "confidence", "high_prob_scenario", "probability_estimate",
                  "reason", "signals", "net_score", "rsi", "macd_line", "bb_width", "atr", "adx", "roc"]
        f.write("\t".join(header) + "\n")
        ts_now = int(time.time() * 1000)
        row = [
            str(ts_now),
            result['direction'],
            str(result['confidence']),
            result['high_prob_scenario'] if result['high_prob_scenario'] else "",
            str(result['probability_estimate']),
            result['reason'],
            signals_str,
            str(result['net_score']),
            str(ind.get('rsi', 50)),
            str(ind.get('macd_line', 0)),
            str(ind.get('bb_width', 0)),
            str(ind.get('atr', 0)),
            str(ind.get('adx', 0)),
            str(ind.get('roc', 0))
        ]
        f.write("\t".join(row) + "\n")
    log_issue("INFO", f"Saved indicator expert summary to {out_path}")
    return out_path

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python E15_indicators_expert.py SYMBOL [timeframe]")
        sys.exit(1)
    symbol = sys.argv[1].upper()
    timeframe = sys.argv[2] if len(sys.argv) > 2 else "1h"
    success = run_expert(symbol, timeframe)
    sys.exit(0 if success else 1)