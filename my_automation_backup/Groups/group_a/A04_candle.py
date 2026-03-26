# A04_candle.py — Layer 2: Candlestick + Forecast hint
# NO API CALLS — sirf rows se kaam karta hai


def confirm(symbol, interval="15m", rows=None):
    try:
        if not rows or len(rows) < 3:
            return _r("WAIT", 0, "None", "Not enough candles", {})
        
        c = rows[-1]
        p = rows[-2]
        pp = rows[-3]
        
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
        
        # ── 3-candle patterns (most reliable) ─────────
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
        
        # ── 2-candle patterns ─────────────────────────
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
        
        # ── Single candle ─────────────────────────────
        # Pin Bar Bullish (long lower wick)
        elif lw > rng_c * 0.62 and body_c < rng_c * 0.3:
            pattern = "Pin Bar Bull"
            sig = "BUY"
            mod = 12
        
        # Pin Bar Bearish (long upper wick)
        elif uw > rng_c * 0.62 and body_c < rng_c * 0.3:
            pattern = "Pin Bar Bear"
            sig = "SELL"
            mod = 12
        
        # Strong bull body
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
        
        # ── Next candle forecast ──────────────────────
        forecast = _forecast(rows, sig)
        
        return {
            "signal": sig,
            "score_mod": mod,
            "pattern": pattern,
            "reason": pattern + (" bullish" if sig == "BUY" else " bearish" if sig == "SELL" else " — indecision"),
            "forecast": forecast,
        }
    
    except Exception as e:
        return _r("WAIT", 0, "Error", str(e)[:40], {})


def _forecast(rows, current_signal):
    """Next candle probability estimate"""
    if len(rows) < 5:
        return {"up": 50, "down": 50, "flat": 0}
    
    closes = [r["close"] for r in rows[-5:]]
    ups = sum(1 for i in range(1, len(closes)) if closes[i] > closes[i - 1])
    downs = sum(1 for i in range(1, len(closes)) if closes[i] < closes[i - 1])
    total = len(closes) - 1
    
    base_up = int(ups / total * 100) if total > 0 else 50
    base_down = int(downs / total * 100) if total > 0 else 50
    
    # Signal bias
    if current_signal == "BUY":
        base_up = min(base_up + 18, 80)
        base_down = max(base_down - 18, 10)
    elif current_signal == "SELL":
        base_down = min(base_down + 18, 80)
        base_up = max(base_up - 18, 10)
    
    flat = max(0, 100 - base_up - base_down)
    return {"up": base_up, "down": base_down, "flat": flat}


def _r(sig, mod, pattern, reason, forecast):
    return {"signal": sig, "score_mod": mod, "pattern": pattern, "reason": reason, "forecast": forecast}