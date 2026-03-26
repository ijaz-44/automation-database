# Z01_trend.py — Layer 1: Trend detection
# NO API CALLS — sirf rows se kaam karta hai

def score(symbol, interval="15m", rows=None):
    """
    Returns trend direction and signal.
    rows: already fetched data (no API call)
    """
    try:
        if not rows or len(rows) < 25:
            return _r("WAIT", "FLAT", 40, "Not enough candles")
        
        closes = [r["close"] for r in rows]
        highs = [r["high"] for r in rows]
        lows = [r["low"] for r in rows]
        
        # EMA 10 and EMA 20
        ema10 = _ema(closes, 10)
        ema20 = _ema(closes, 20)
        
        if len(ema10) < 6 or len(ema20) < 6:
            return _r("WAIT", "FLAT", 40, "EMA calc failed")
        
        price = closes[-1]
        e10 = ema10[-1]
        e20 = ema20[-1]
        
        # Slope calculation
        slope10 = (ema10[-1] - ema10[-5]) / ema10[-5] * 100 if ema10[-5] > 0 else 0
        slope20 = (ema20[-1] - ema20[-5]) / ema20[-5] * 100 if ema20[-5] > 0 else 0
        
        # Swing structure (last 60 candles)
        N = 3
        sh = []
        sl = []
        for i in range(N, len(highs) - N):
            if highs[i] == max(highs[i-N:i+N+1]):
                sh.append(highs[i])
            if lows[i] == min(lows[i-N:i+N+1]):
                sl.append(lows[i])
        
        sh = sh[-4:]
        sl = sl[-4:]
        
        hh = len(sh) >= 2 and sh[-1] > sh[-2]
        hl = len(sl) >= 2 and sl[-1] > sl[-2]
        lh = len(sh) >= 2 and sh[-1] < sh[-2]
        ll = len(sl) >= 2 and sl[-1] < sl[-2]
        
        # Score components
        bull = 0
        bear = 0
        
        if price > e10:
            bull += 2
        else:
            bear += 2
        
        if price > e20:
            bull += 2
        else:
            bear += 2
        
        if e10 > e20:
            bull += 3
        else:
            bear += 3
        
        if slope10 > 0.03:
            bull += 2
        elif slope10 < -0.03:
            bear += 2
        
        if slope20 > 0.02:
            bull += 1
        elif slope20 < -0.02:
            bear += 1
        
        if hh and hl:
            bull += 3
        if lh and ll:
            bear += 3
        
        total = bull + bear
        if total == 0:
            return _r("WAIT", "FLAT", 40, "No momentum")
        
        if bull > bear:
            score_val = int(50 + (bull / total) * 45)
            sig = "BUY" if score_val >= 55 else "WAIT"
            trend = "UP"
            reason = "EMA bull"
            if hh and hl:
                reason += "+HH/HL"
            return _r(sig, trend, score_val, reason)
        
        elif bear > bull:
            score_val = int(50 + (bear / total) * 45)
            sig = "SELL" if score_val >= 55 else "WAIT"
            trend = "DOWN"
            reason = "EMA bear"
            if lh and ll:
                reason += "+LH/LL"
            return _r(sig, trend, score_val, reason)
        
        else:
            return _r("WAIT", "RANGING", 38, "EMA mixed")
    
    except Exception as e:
        return _r("WAIT", "FLAT", 40, str(e)[:40])


def _ema(data, period):
    if len(data) < period:
        return []
    k = 2 / (period + 1)
    ema = [sum(data[:period]) / period]
    for v in data[period:]:
        ema.append(v * k + ema[-1] * (1 - k))
    return ema


def _r(sig, trend, score, reason):
    return {"signal": sig, "trend": trend, "score": score, "reason": reason}