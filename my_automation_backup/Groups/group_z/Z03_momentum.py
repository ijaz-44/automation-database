# Z03_momentum.py — Layer 1: Momentum
# NO API CALLS — sirf rows se kaam karta hai

def score(symbol, interval="15m", rows=None):
    """
    Returns momentum signal and score modifier.
    rows: already fetched data (no API call)
    """
    try:
        if not rows or len(rows) < 8:
            return _m("WAIT", 0, "UNKNOWN", "Not enough data")
        
        closes = [r["close"] for r in rows]
        
        # Rate of change
        roc5 = (closes[-1] - closes[-5]) / closes[-5] * 100 if closes[-5] > 0 else 0
        roc10 = (closes[-1] - closes[-10]) / closes[-10] * 100 if len(closes) >= 10 and closes[-10] > 0 else roc5
        
        # Consecutive candle streak
        bull = 0
        bear = 0
        for i in range(len(closes) - 1, 0, -1):
            if closes[i] > closes[i - 1]:
                if bear > 0:
                    break
                bull += 1
            elif closes[i] < closes[i - 1]:
                if bull > 0:
                    break
                bear += 1
            else:
                break
        
        # RSI simplified (7 period)
        gains = [max(closes[i] - closes[i - 1], 0) for i in range(1, len(closes))]
        losses = [max(closes[i - 1] - closes[i], 0) for i in range(1, len(closes))]
        ag = sum(gains[-7:]) / 7 if len(gains) >= 7 else sum(gains) / len(gains) if gains else 0
        al = sum(losses[-7:]) / 7 if len(losses) >= 7 else sum(losses) / len(losses) if losses else 0
        rsi = 100 - (100 / (1 + (ag / al))) if al > 0 else 50
        
        # Strong bull momentum
        if roc5 > 0.2 and bull >= 3 and rsi < 75:
            return _m("BUY", 18, "STRONG", f"ROC=+{round(roc5,2)}% streak={bull}")
        
        # Strong bear momentum
        if roc5 < -0.2 and bear >= 3 and rsi > 25:
            return _m("SELL", 18, "STRONG", f"ROC={round(roc5,2)}% streak={bear}")
        
        # Moderate bull
        if roc5 > 0.08 and bull >= 2 and rsi < 72:
            return _m("BUY", 10, "MODERATE", f"ROC=+{round(roc5,2)}% streak={bull}")
        
        # Moderate bear
        if roc5 < -0.08 and bear >= 2 and rsi > 28:
            return _m("SELL", 10, "MODERATE", f"ROC={round(roc5,2)}% streak={bear}")
        
        # RSI extremes — reversal
        if rsi < 28 and roc5 > 0:
            return _m("BUY", 12, "REVERSAL", f"RSI oversold {round(rsi,1)} + recovering")
        if rsi > 72 and roc5 < 0:
            return _m("SELL", 12, "REVERSAL", f"RSI overbought {round(rsi,1)} + fading")
        
        # Flat
        if abs(roc5) < 0.02:
            return _m("WAIT", -5, "FLAT", "No momentum — flat price")
        
        # Weak momentum
        if abs(roc5) < 0.05:
            return _m("WAIT", -3, "WEAK", f"Weak momentum ROC={round(roc5,2)}%")
        
        return _m("WAIT", 0, "NEUTRAL", f"ROC={round(roc5,2)}%")
    
    except Exception as e:
        return _m("WAIT", 0, "UNKNOWN", str(e)[:40])


def _m(sig, mod, strength, reason):
    return {"signal": sig, "score_mod": mod, "strength": strength, "reason": reason}