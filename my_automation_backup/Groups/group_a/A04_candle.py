# A04_candle.py — Layer 2: Candlestick + Forecast hint
# NO API CALLS — sirf rows se kaam karta hai


def confirm(symbol, interval="15m", rows=None):
    try:
        if not rows or len(rows) < 3:
            return _r("WAIT", 0, "None", "Not enough candles", {})
        
        c = rows[-1]
        p = rows[-2]
        pp = rows[-3]
        ppp = rows[-4] if len(rows) >= 4 else None
        
        co, cc = c["open"], c["close"]
        po, pc = p["open"], p["close"]
        ppo, ppc = pp["open"], pp["close"]
        
        body_c = abs(cc - co)
        rng_c = c["high"] - c["low"] if c["high"] > c["low"] else 0.0001
        uw = c["high"] - max(co, cc)
        lw = min(co, cc) - c["low"]
        
        pattern = "Normal"
        sig = "WAIT"
        mod = 0
        
        # ========== 3-candle patterns ==========
        # Morning Star
        if (pc < po and abs(pc - po) < abs(ppo - ppc) * 0.35 and cc > co and cc > (ppo + ppc) / 2):
            pattern = "Morning Star"
            sig = "BUY"
            mod = 18
        
        # Evening Star
        elif (pc > po and abs(pc - po) < abs(ppo - ppc) * 0.35 and cc < co and cc < (ppo + ppc) / 2):
            pattern = "Evening Star"
            sig = "SELL"
            mod = 18
        
        # 3 White Soldiers
        elif cc > co and pc > po and ppc > ppo and cc > pc > ppc:
            pattern = "3 White Soldiers"
            sig = "BUY"
            mod = 16
        
        # 3 Black Crows
        elif cc < co and pc < po and ppc < ppo and cc < pc < ppc:
            pattern = "3 Black Crows"
            sig = "SELL"
            mod = 16
        
        # ========== 2-candle patterns ==========
        # Bullish Engulfing
        elif cc > co and pc < po and co < pc and cc > po:
            pattern = "Bullish Engulfing"
            sig = "BUY"
            mod = 16
        
        # Bearish Engulfing
        elif cc < co and pc > po and co > pc and cc < po:
            pattern = "Bearish Engulfing"
            sig = "SELL"
            mod = 16
        
        # Bullish Harami (small body inside previous)
        elif cc > co and pc < po and co < pc and cc < po:
            pattern = "Bullish Harami"
            sig = "BUY"
            mod = 12
        
        # Bearish Harami
        elif cc < co and pc > po and co > pc and cc > po:
            pattern = "Bearish Harami"
            sig = "SELL"
            mod = 12
        
        # Piercing Line
        elif cc > co and pc < po and cc > (po + pc) / 2 and co < pc:
            pattern = "Piercing Line"
            sig = "BUY"
            mod = 14
        
        # Dark Cloud Cover
        elif cc < co and pc > po and cc < (po + pc) / 2 and co > pc:
            pattern = "Dark Cloud Cover"
            sig = "SELL"
            mod = 14
        
        # ========== Single candle ==========
        # Hammer / Pin Bar Bullish (long lower wick)
        elif lw > rng_c * 0.62 and body_c < rng_c * 0.3:
            pattern = "Hammer / Pin Bar Bull"
            sig = "BUY"
            mod = 12
        
        # Shooting Star / Pin Bar Bearish (long upper wick)
        elif uw > rng_c * 0.62 and body_c < rng_c * 0.3:
            pattern = "Shooting Star / Pin Bar Bear"
            sig = "SELL"
            mod = 12
        
        # Strong bull body (close > open, body > 70% of range)
        elif cc > co and body_c > rng_c * 0.7:
            pattern = "Strong Bull"
            sig = "BUY"
            mod = 9
        
        # Strong bear body
        elif cc < co and body_c > rng_c * 0.7:
            pattern = "Strong Bear"
            sig = "SELL"
            mod = 9
        
        # Doji
        elif body_c < rng_c * 0.08:
            pattern = "Doji"
            sig = "WAIT"
            mod = -3
        
        # Spinning Top (small body, moderate wicks)
        elif body_c < rng_c * 0.3 and lw > rng_c * 0.2 and uw > rng_c * 0.2:
            pattern = "Spinning Top"
            sig = "WAIT"
            mod = -1
        
        # ========== Forecast ==========
        forecast = _forecast(rows, sig, pattern, mod)
        
        # Reason string
        if sig == "BUY":
            reason = f"{pattern} bullish"
        elif sig == "SELL":
            reason = f"{pattern} bearish"
        else:
            reason = f"{pattern} — indecision"
        
        return {
            "signal": sig,
            "score_mod": mod,
            "pattern": pattern,
            "reason": reason,
            "forecast": forecast,
        }
    
    except Exception as e:
        return _r("WAIT", 0, "Error", str(e)[:40], {})


def _forecast(rows, current_signal, pattern, mod):
    """
    Next candle probability using more data.
    - uses last 10 candles for trend
    - adjusts by pattern strength
    """
    if len(rows) < 10:
        return {"up": 50, "down": 50, "flat": 0}
    
    closes = [r["close"] for r in rows[-10:]]
    ups = sum(1 for i in range(1, len(closes)) if closes[i] > closes[i-1])
    downs = sum(1 for i in range(1, len(closes)) if closes[i] < closes[i-1])
    total = len(closes) - 1
    
    if total == 0:
        base_up = 50
        base_down = 50
    else:
        base_up = int(ups / total * 100)
        base_down = int(downs / total * 100)
    
    # Adjust by pattern strength (mod)
    if current_signal == "BUY":
        # Positive mod adds bias
        base_up = min(base_up + (mod // 3), 85)
        base_down = max(base_down - (mod // 3), 10)
    elif current_signal == "SELL":
        base_down = min(base_down + (mod // 3), 85)
        base_up = max(base_up - (mod // 3), 10)
    
    # Ensure total sum is 100
    total_bias = base_up + base_down
    if total_bias > 100:
        ratio = 100 / total_bias
        base_up = int(base_up * ratio)
        base_down = int(base_down * ratio)
    elif total_bias < 100:
        base_up += (100 - total_bias) // 2
        base_down += (100 - total_bias) - (100 - total_bias)//2
    
    flat = 0
    return {"up": base_up, "down": base_down, "flat": flat}


def _r(sig, mod, pattern, reason, forecast):
    return {
        "signal": sig,
        "score_mod": mod,
        "pattern": pattern,
        "reason": reason,
        "forecast": forecast
    }