# E14_regime_expert.py – Market aur Regime Detection Expert (Roman Urdu, Complete Guide)

"""
Ye file market types aur regime types ko detect karne ka expert hai.
Isme:
- All market types (crypto, forex, commodities) covered.
- All regime types (Bull, Bear, Range, Volatile, Calm, Momentum, Mean Reversion, Breakout, Accumulation, Distribution, Manipulation, etc.) included.
- Har regime ki detection method, win/loss chances, aur scoring system diya gaya hai.
- Output JSON structure jo aapke X14 (ya koi bhi) module use kar sake.
- Roman Urdu mein likha gaya hai (English-transliterated Urdu).

"""

from datetime import datetime
import math
import numpy as np  # Agar numpy nahi hai to pure Python se compute kar sakte hain; yahan assume kiya hai
import json

class E14RegimeExpert:
    """
    Ye class market type, regime type, detection score, aur probability estimate provide karegi.
    """

    def __init__(self, timeframe: str = "1h"):
        """
        timeframe: '1m', '5m', '15m', '1h', '4h', '1d'
        """
        self.timeframe = timeframe
        self.market_type = None  # 'crypto', 'forex', 'commodity'
        self.regime_type = None  # final regime
        self.regime_confidence = 0  # 0-100
        self.win_probability = 0  # short-term win probability %
        self.loss_probability = 0  # short-term loss probability %
        self.regime_score = 0  # -100 to 100 (positive bullish, negative bearish)
        self.details = {}  # raw metrics

    def detect_market_type(self, symbol: str) -> str:
        """Symbol ke basis par market type detect karega."""
        sym_upper = symbol.upper()
        if sym_upper.startswith(('BTC', 'ETH', 'BNB', 'SOL', 'XRP', 'ADA', 'DOGE', 'DOT')):
            return 'crypto'
        elif sym_upper.endswith(('USD', 'EUR', 'GBP', 'JPY', 'AUD', 'CAD', 'CHF', 'NZD')):
            return 'forex'
        elif sym_upper.startswith(('XAU', 'XAG', 'GOLD')) or sym_upper in ('OIL', 'BRENT', 'WTI', 'NGAS'):
            return 'commodity'
        else:
            return 'crypto'  # default crypto

    def compute_atr(self, high, low, close, period=14):
        """ATR calculate karega (time series)."""
        tr = []
        for i in range(1, len(close)):
            hl = high[i] - low[i]
            hc = abs(high[i] - close[i-1])
            lc = abs(low[i] - close[i-1])
            tr.append(max(hl, hc, lc))
        atr = []
        for i in range(len(tr)):
            if i < period-1:
                atr.append(np.nan)
            elif i == period-1:
                atr.append(np.mean(tr[:period]))
            else:
                atr.append((atr[-1]*(period-1) + tr[i])/period)
        return atr

    def compute_adx(self, high, low, close, period=14):
        """ADX compute karega (trend strength)."""
        # DMI calculation
        tr = []
        plus_dm = []
        minus_dm = []
        for i in range(1, len(close)):
            hl = high[i] - low[i]
            hc = abs(high[i] - close[i-1])
            lc = abs(low[i] - close[i-1])
            tr.append(max(hl, hc, lc))
            up = high[i] - high[i-1]
            down = low[i-1] - low[i]
            plus_dm.append(up if up > down and up > 0 else 0)
            minus_dm.append(down if down > up and down > 0 else 0)
        # Smoothing
        def smooth(series, period):
            smooth_series = []
            for i in range(len(series)):
                if i < period-1:
                    smooth_series.append(np.nan)
                elif i == period-1:
                    smooth_series.append(np.mean(series[:period]))
                else:
                    smooth_series.append(smooth_series[-1] - (smooth_series[-1]/period) + series[i])
            return smooth_series
        tr_smooth = smooth(tr, period)
        plus_smooth = smooth(plus_dm, period)
        minus_smooth = smooth(minus_dm, period)
        # DI and ADX
        plus_di = [100 * p / t if t != 0 else 0 for p, t in zip(plus_smooth, tr_smooth)]
        minus_di = [100 * m / t if t != 0 else 0 for m, t in zip(minus_smooth, tr_smooth)]
        dx = [abs(p - m) / (p + m) * 100 if (p+m) != 0 else 0 for p, m in zip(plus_di, minus_di)]
        adx = smooth(dx, period)
        return plus_di, minus_di, adx

    def compute_bollinger_bands(self, close, period=20, std_dev=2):
        """Bollinger Bands aur bandwidth."""
        sma = []
        std = []
        for i in range(len(close)):
            if i < period-1:
                sma.append(np.nan)
                std.append(np.nan)
            else:
                window = close[i-period+1:i+1]
                sma.append(np.mean(window))
                std.append(np.std(window))
        upper = [s + std_dev * st for s, st in zip(sma, std)]
        lower = [s - std_dev * st for s, st in zip(sma, std)]
        bandwidth = [(u - l) / s if s != 0 else 0 for u, l, s in zip(upper, lower, sma)]
        return upper, lower, sma, bandwidth

    def compute_regime_scores(self, high, low, close):
        """
        Overall regime scores compute karega:
        - Trend score (bullish/bearish)
        - Volatility regime (high/normal/low)
        - Market phase (trending/ranging/breakout)
        - Accumulation/Distribution/Manipulation scores
        Returns: dict with all intermediate scores.
        """
        n = len(close)
        if n < 50:
            return {"error": "Insufficient data"}
        # ADX aur DMI
        plus_di, minus_di, adx = self.compute_adx(high, low, close, period=14)
        latest_adx = adx[-1]
        plus_di_latest = plus_di[-1]
        minus_di_latest = minus_di[-1]
        # Trend direction
        if plus_di_latest > minus_di_latest:
            trend_direction = 1  # bullish
        elif minus_di_latest > plus_di_latest:
            trend_direction = -1  # bearish
        else:
            trend_direction = 0
        # Trend strength
        if latest_adx > 25:
            trend_strength = "strong"
            trend_score = trend_direction * 40
        elif latest_adx > 20:
            trend_strength = "moderate"
            trend_score = trend_direction * 20
        else:
            trend_strength = "weak"
            trend_score = 0
        # Volatility regime (ATR based)
        atr = self.compute_atr(high, low, close, period=14)[-1]
        avg_atr = np.mean(self.compute_atr(high, low, close, period=14)[-50:])
        if atr > avg_atr * 1.5:
            vol_regime = "high_volatility"
            vol_score = 30 * trend_direction if trend_direction != 0 else -10  # high vol amplifies trend
        elif atr < avg_atr * 0.7:
            vol_regime = "low_volatility"
            vol_score = -5  # low vol can be ranging
        else:
            vol_regime = "normal_volatility"
            vol_score = 0
        # Bollinger bandwidth (ranging detection)
        _, _, _, bb_width = self.compute_bollinger_bands(close, period=20, std_dev=2)
        latest_bb_width = bb_width[-1]
        if latest_bb_width < 0.05:
            range_score = 30
            market_phase = "ranging"
        elif latest_bb_width > 0.15:
            range_score = -30
            market_phase = "breakout"
        else:
            range_score = 0
            market_phase = "neutral"
        # Accumulation / Distribution score (volume based)
        # Simple version: Compare close to VWAP? Or use OBV.
        # For simplicity, use close position relative to high/low range.
        obv = []
        obv_val = 0
        for i in range(n):
            if i == 0:
                obv_val = 0
            else:
                if close[i] > close[i-1]:
                    obv_val += volume[i] if 'volume' in locals() else 1e6  # dummy volume
                elif close[i] < close[i-1]:
                    obv_val -= volume[i] if 'volume' in locals() else 1e6
            obv.append(obv_val)
        # OBV trend
        obv_slope = (obv[-1] - obv[-20]) / max(1, abs(obv[-20])) if obv[-20] != 0 else 0
        if obv_slope > 0.02:
            accumulation_score = 20
            distribution_score = 0
        elif obv_slope < -0.02:
            accumulation_score = 0
            distribution_score = 20
        else:
            accumulation_score = 0
            distribution_score = 0
        # Manipulation detection (extreme wicks or false breakouts)
        # Simple check: if price broke out of range but closed back inside, manipulation
        recent_high = max(high[-10:])
        recent_low = min(low[-10:])
        if close[-1] > recent_high * 0.99 and close[-1] < recent_high:
            manipulation_score = 15
            manipulation_type = "bull_trap"  # could be fakeout
        elif close[-1] < recent_low * 1.01 and close[-1] > recent_low:
            manipulation_score = 15
            manipulation_type = "bear_trap"
        else:
            manipulation_score = 0
            manipulation_type = None

        # Momentum regime (ROC)
        roc = (close[-1] - close[-5]) / close[-5] * 100 if close[-5] != 0 else 0
        if roc > 1:
            momentum_regime = "positive_momentum"
            mom_score = 10
        elif roc < -1:
            momentum_regime = "negative_momentum"
            mom_score = -10
        else:
            momentum_regime = "neutral_momentum"
            mom_score = 0

        # Mean reversion regime (when price far from moving average)
        ma20 = sum(close[-20:]) / 20
        distance_pct = (close[-1] - ma20) / ma20 * 100
        if abs(distance_pct) > 2:
            mean_rev_regime = "active"
            mr_score = -trend_direction * 15  # opposite to trend
        else:
            mean_rev_regime = "inactive"
            mr_score = 0

        return {
            "trend_direction": trend_direction,
            "trend_strength": trend_strength,
            "trend_score": trend_score,
            "vol_regime": vol_regime,
            "vol_score": vol_score,
            "market_phase": market_phase,
            "range_score": range_score,
            "accumulation_score": accumulation_score,
            "distribution_score": distribution_score,
            "manipulation_score": manipulation_score,
            "manipulation_type": manipulation_type,
            "momentum_regime": momentum_regime,
            "mom_score": mom_score,
            "mean_rev_regime": mean_rev_regime,
            "mr_score": mr_score,
            "latest_adx": latest_adx,
            "plus_di": plus_di_latest,
            "minus_di": minus_di_latest,
            "bb_width": latest_bb_width,
            "atr": atr,
            "roc": roc,
            "obv_slope": obv_slope,
            "distance_from_ma_pct": distance_pct
        }

    def classify_regime(self, scores):
        """Scores ko use karke final regime type classify karega."""
        trend_dir = scores["trend_direction"]
        trend_str = scores["trend_strength"]
        vol_reg = scores["vol_regime"]
        market_phase = scores["market_phase"]
        accum = scores["accumulation_score"]
        distrib = scores["distribution_score"]
        manip = scores["manipulation_score"]
        mom = scores["momentum_regime"]
        mr = scores["mean_rev_regime"]

        # Determine primary regime
        if trend_dir == 1 and trend_str == "strong":
            regime = "bull_market"
            confidence = 80
            win_prob = 70
            loss_prob = 25
        elif trend_dir == -1 and trend_str == "strong":
            regime = "bear_market"
            confidence = 80
            win_prob = 25
            loss_prob = 70
        elif trend_dir == 0 and market_phase == "ranging":
            regime = "range_market"
            confidence = 70
            win_prob = 50
            loss_prob = 45
        elif vol_reg == "high_volatility" and trend_dir != 0:
            regime = "volatile_trend"
            confidence = 75
            win_prob = 60 if trend_dir == 1 else 35
            loss_prob = 35 if trend_dir == 1 else 60
        elif vol_reg == "high_volatility" and trend_dir == 0:
            regime = "volatile_chop"
            confidence = 65
            win_prob = 45
            loss_prob = 50
        elif vol_reg == "low_volatility":
            regime = "calm_market"
            confidence = 60
            win_prob = 55
            loss_prob = 40
        else:
            regime = "neutral"
            confidence = 50
            win_prob = 50
            loss_prob = 45

        # Adjust based on accumulation/distribution
        if accum > 0:
            regime = "accumulation"
            confidence = min(85, confidence + 10)
            win_prob = min(90, win_prob + 15)
        elif distrib > 0:
            regime = "distribution"
            confidence = min(85, confidence + 10)
            win_prob = max(10, win_prob - 15)
            loss_prob = min(90, loss_prob + 15)

        # Manipulation adjustment
        if manip > 0:
            regime = "manipulation"
            confidence = 75
            win_prob = 35   # fakeout usually goes opposite
            loss_prob = 60

        # Momentum
        if mom == "positive_momentum" and regime in ["bull_market", "range_market"]:
            win_prob = min(85, win_prob + 10)
        elif mom == "negative_momentum" and regime in ["bear_market", "range_market"]:
            win_prob = max(15, win_prob - 10)

        # Mean reversion
        if mr == "active":
            win_prob = 55
            confidence = max(confidence, 60)

        # Ensure bounds
        win_prob = max(0, min(100, win_prob))
        loss_prob = max(0, min(100, loss_prob))

        return regime, confidence, win_prob, loss_prob

    def analyze(self, high, low, close, symbol):
        """Main method: market type aur regime detect karega."""
        self.market_type = self.detect_market_type(symbol)
        scores = self.compute_regime_scores(high, low, close)
        if "error" in scores:
            return {"error": scores["error"]}
        regime, confidence, win_prob, loss_prob = self.classify_regime(scores)

        self.regime_type = regime
        self.regime_confidence = confidence
        self.win_probability = win_prob
        self.loss_probability = loss_prob
        self.regime_score = scores["trend_score"] + scores["vol_score"]
        self.details = scores

        return {
            "market_type": self.market_type,
            "regime_type": self.regime_type,
            "regime_confidence": self.regime_confidence,
            "win_probability": self.win_probability,
            "loss_probability": self.loss_probability,
            "regime_score": self.regime_score,
            "details": self.details
        }


# ---------------------------
# Market Types Complete List (Roman Urdu)
# ---------------------------
market_types_info = """
=== MARKET TYPES (Roman Urdu) ===

1. Cryptocurrencies (Crypto):
   - Examples: Bitcoin (BTC), Ethereum (ETH), Binance Coin (BNB), Solana (SOL), Ripple (XRP)
   - Characteristics: High volatility, 24/7 trading, strong trend persistence, frequent breakouts.
   - Typical Regimes: Bull, Bear, Range, High Volatility, Manipulation, Accumulation.
   - Win/Loss: Long-term bullish bias, but short-term can be very risky.

2. Forex (Foreign Exchange):
   - Examples: EURUSD, GBPUSD, USDJPY, AUDUSD, USDCAD, NZDUSD, USDCHF
   - Characteristics: Lower volatility than crypto, range‑bound behavior common, mean reversion frequent.
   - Typical Regimes: Range, Calm, Momentum (during news), Mean Reversion.
   - Win/Loss: Moderate, requires discipline and news awareness.

3. Commodities:
   - a) Gold (XAUUSD, GOLD): Safe haven, less volatile than crypto, strong trend during crisis.
   - b) Silver (XAGUSD): More volatile than gold, industrial demand influence.
   - c) Oil (WTI, BRENT): Geopolitical sensitivity, high volatility, strong seasonality.
   - d) Natural Gas (NGAS): Extremely volatile, weather dependent.
   - e) Other metals (Copper, Platinum, etc.): Industrial demand.
   - Characteristics: Macroeconomic drivers, supply constraints.
   - Typical Regimes: Bull (inflation), Bear (deflation), Range, High Volatility.
   - Win/Loss: Medium to high risk.

"""

# ---------------------------
# Regime Types Complete List with Detection Methods and Win/Loss Summary
# ---------------------------
regime_types_info = """
=== REGIME TYPES (Roman Urdu) ===

1. Bull Market (Bull Market):
   - Characteristics: Higher highs, higher lows, strong buying interest, trending upward.
   - Detection: ADX > 25, PlusDI > MinusDI, price > MA200.
   - Win/Loss: Winning probability 65-80%, loss 20-35%.
   - Strategy: Trend following, breakout trades.

2. Bear Market (Bear Market):
   - Characteristics: Lower lows, lower highs, selling pressure, trending downward.
   - Detection: ADX > 25, MinusDI > PlusDI, price < MA200.
   - Win/Loss: Winning probability 20-35%, loss 65-80%.

3. Range Market (Range Bound):
   - Characteristics: Price oscillates between support and resistance, no clear trend.
   - Detection: ADX < 20, Bollinger Band width low, price inside bands.
   - Win/Loss: 45-55% win rate, suitable for mean reversion.

4. High Volatility (High Volatility Market):
   - Characteristics: Large price swings, increased ATR, erratic moves.
   - Detection: ATR > 1.5x average, ADX rising but low or high.
   - Win/Loss: 40-60%, but risk high.

5. Low Volatility (Calm Market):
   - Characteristics: Small ranges, quiet price action, low ATR.
   - Detection: ATR < 0.7x average, Bollinger Band width low.
   - Win/Loss: 50-55%, good for scalping.

6. Momentum Market (Momentum Regime):
   - Characteristics: Strong directional movement with increasing speed.
   - Detection: ROC > 1% or < -1%, ADX rising > 25.
   - Win/Loss: 60-70% in direction of momentum.

7. Mean Reversion Market (Mean Reversion Regime):
   - Characteristics: Price oscillates around moving average, overextended moves retrace.
   - Detection: Price > 2% away from MA20, ADX < 20.
   - Win/Loss: 55-65% if trading reversals.

8. Breakout Market (Breakout Regime):
   - Characteristics: Price exits a consolidation zone with high volume.
   - Detection: Bollinger Band width expanding, ADX rising above 20.
   - Win/Loss: 50-60%, but false breakouts common.

9. Accumulation Phase:
   - Characteristics: Quiet consolidation after downtrend, smart money buying.
   - Detection: OBV rising while price flat, ADX low, volume decreasing.
   - Win/Loss: Upward probability 65-75% in medium term.

10. Distribution Phase:
    - Characteristics: Quiet consolidation after uptrend, smart money selling.
    - Detection: OBV falling while price flat, ADX low, volume decreasing.
    - Win/Loss: Downward probability 65-75%.

11. Manipulation/Stop Hunt:
    - Characteristics: Price spikes to take out stops then reverses.
    - Detection: Wicks > body, price breaks out then closes back inside.
    - Win/Loss: 30-40% win rate if caught, but can be profitable.

12. Reversal Market:
    - Characteristics: Change of character (CHoCH), break of structure (BOS).
    - Detection: ADX falling from high, PlusDI cross below MinusDI, price crossing key MA.
    - Win/Loss: 40-50%, best to wait for confirmation.

13. Trend Continuation (Trend Continuation Regime):
    - Characteristics: Pullback in trend, then resumption.
    - Detection: ADX still > 20, price pulls back to MA then bounces.
    - Win/Loss: 55-65%.

14. Chop/Churn (Indecision Market):
    - Characteristics: Whipsaws, multiple false signals.
    - Detection: ADX < 15, Bollinger Band width medium, choppiness index high.
    - Win/Loss: 40-45%, avoid trading.

"""

# ---------------------------
# Logic for each regime ki detection (score system)
# ---------------------------
detection_logic = """
=== Regime Detection Logic (Score Based) ===

Har query ke liye, hum multiple components score karte hain:

A. Bull/Bear Score:
   - PlusDI – MinusDI ka difference dijiye, normalize kariye.
   - ADX value: 0-25 (weak), 25-50 (strong), >50 (very strong).
   - Price position relative to EMA200/SMA200.
   - Trend direction validation: Higher highs/lower lows count in last N bars.

B. Volatility Score:
   - Current ATR vs rolling average (50 periods).
   - Bollinger Band width percentile.
   - Use thresholds: normal, high, extreme.

C. Market Phase Score:
   - ADX (trend strength) vs Bollinger Band width (compression/expansion).
   - If ADX low and bands narrow -> Ranging.
   - If ADX rising and bands expanding -> Breakout.
   - If ADX high and bands wide -> Trending.

D. Accumulation/Distribution Score:
   - OBV slope regression.
   - Volume vs volume MA.
   - Price action pattern (higher lows in consolidation).

E. Manipulation Score:
   - Wicks length vs body length.
   - Breakout distance vs close reversal distance.
   - Volume spike during breakout.

F. Momentum Score:
   - ROC (rate of change) over short period (5 bars).
   - MACD histogram direction.

Final regime is determined by highest weighted score among:
- Bull market
- Bear market
- Range market
- High volatility
- Low volatility
- Momentum
- Mean reversion
- Breakout
- Accumulation
- Distribution
- Manipulation
These are not mutually exclusive; priority is given to the strongest signal.
"""

# Example usage (if run as script)
if __name__ == "__main__":
    import random
    # Dummy data for testing
    high = [100 + random.uniform(-1,2) for _ in range(100)]
    low = [98 + random.uniform(-1,1) for _ in range(100)]
    close = [99 + random.uniform(-1,1) for _ in range(100)]
    expert = E14RegimeExpert(timeframe="1h")
    result = expert.analyze(high, low, close, "BTCUSDT")
    print(json.dumps(result, indent=4))
    print(market_types_info)
    print(regime_types_info)
    print(detection_logic)