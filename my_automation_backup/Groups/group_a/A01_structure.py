# A01_structure.py — Layer 2: Market Structure
# NO API CALLS — sirf rows se kaam karta hai

N = 3  # Swing lookback


def confirm(symbol, interval="15m", rows=None):
    """
    Returns market structure signal.
    rows: already fetched data (no API call)
    """
    try:
        if not rows or len(rows) < N * 4 + 5:
            return _r("WAIT", 0, "UNKNOWN", "Not enough data")
        
        highs = [r["high"] for r in rows]
        lows = [r["low"] for r in rows]
        
        # Swing points
        sh = []
        sl = []
        
        for i in range(N, len(highs) - N):
            if highs[i] == max(highs[i - N:i + N + 1]):
                sh.append({"p": highs[i], "i": i})
            if lows[i] == min(lows[i - N:i + N + 1]):
                sl.append({"p": lows[i], "i": i})
        
        sh = sh[-6:]
        sl = sl[-6:]
        
        if len(sh) < 2 or len(sl) < 2:
            return _r("WAIT", 0, "UNCLEAR", "Few swing points")
        
        hh = sh[-1]["p"] > sh[-2]["p"]
        hl = sl[-1]["p"] > sl[-2]["p"]
        lh = sh[-1]["p"] < sh[-2]["p"]
        ll = sl[-1]["p"] < sl[-2]["p"]
        
        # CHoCH (Change of Character)
        choch = "NONE"
        if len(sh) >= 3 and len(sl) >= 3:
            if sh[-2]["p"] < sh[-3]["p"] and sl[-1]["p"] > sl[-2]["p"]:
                choch = "BULLISH"
            elif sh[-2]["p"] > sh[-3]["p"] and sh[-1]["p"] < sh[-2]["p"]:
                choch = "BEARISH"
        
        # BOS (Break of Structure)
        bos = "BULLISH" if hh else "BEARISH" if ll else "NONE"
        
        # Decision
        if hh and hl:
            mod = 22
            if choch == "BULLISH":
                mod = 28
            return _r("BUY", mod, "UPTREND", f"HH+HL confirmed + CHoCH" if choch == "BULLISH" else "HH+HL confirmed")
        
        if lh and ll:
            mod = 22
            if choch == "BEARISH":
                mod = 28
            return _r("SELL", mod, "DOWNTREND", f"LH+LL confirmed + CHoCH" if choch == "BEARISH" else "LH+LL confirmed")
        
        if choch == "BULLISH":
            return _r("BUY", 18, "REVERSAL", "CHoCH bullish reversal")
        if choch == "BEARISH":
            return _r("SELL", 18, "REVERSAL", "CHoCH bearish reversal")
        
        if hh and ll:
            return _r("WAIT", -5, "EXPANDING", "Expanding range")
        if lh and hl:
            return _r("WAIT", 0, "CONTRACTING", "Contracting — breakout soon")
        
        return _r("WAIT", 0, "RANGING", "No clear structure")
    
    except Exception as e:
        return _r("WAIT", 0, "ERROR", str(e)[:40])


def _r(sig, mod, structure, reason):
    return {"signal": sig, "score_mod": mod, "structure": structure, "reason": reason}