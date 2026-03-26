# A02_indicators.py — Layer 2: RSI + MACD + Bollinger
# NO API CALLS — sirf rows se kaam karta hai

import math


def confirm(symbol, interval="15m", rows=None):
    """
    Returns indicators signal.
    rows: already fetched data (no API call)
    """
    try:
        if not rows or len(rows) < 35:
            return _r("WAIT", 0, "Not enough data for indicators")
        
        closes = [r["close"] for r in rows]
        
        # ── RSI (14) ──────────────────────────────────
        rsi = _rsi(closes, 14)
        rsi_v = rsi[-1] if rsi else 50
        
        # RSI divergence (simple)
        rsi_div = "NONE"
        if len(rsi) >= 5 and len(closes) >= 5:
            if closes[-1] < closes[-4] and rsi[-1] > rsi[-4] and rsi_v < 35:
                rsi_div = "BULLISH"
            elif closes[-1] > closes[-4] and rsi[-1] < rsi[-4] and rsi_v > 65:
                rsi_div = "BEARISH"
        
        # ── MACD (12,26,9) ────────────────────────────
        e12 = _ema(closes, 12)
        e26 = _ema(closes, 26)
        if len(e12) == 0 or len(e26) == 0:
            macd = []
        else:
            diff = len(e12) - len(e26)
            start_idx = max(0, diff)
            macd = [a - b for a, b in zip(e12[start_idx:], e26)]
        
        sig9 = _ema(macd, 9) if len(macd) > 0 else []
        if len(macd) == 0 or len(sig9) == 0:
            hist = []
        else:
            d2 = len(macd) - len(sig9)
            start_idx = max(0, d2)
            hist = [m - s for m, s in zip(macd[start_idx:], sig9)]
        
        macd_signal = "WAIT"
        if len(hist) >= 2:
            if hist[-1] > 0 and hist[-1] > hist[-2]:
                macd_signal = "BUY"
            elif hist[-1] < 0 and hist[-1] < hist[-2]:
                macd_signal = "SELL"
        
        # Crossover
        if len(macd) >= 2 and len(sig9) >= 2:
            d2 = len(macd) - len(sig9)
            start_idx = max(0, d2)
            if len(macd[start_idx:]) >= 2 and len(sig9) >= 2:
                macd_vals = macd[start_idx:]
                if macd_vals[-2] <= sig9[-2] and macd_vals[-1] > sig9[-1]:
                    macd_signal = "BUY"
                elif macd_vals[-2] >= sig9[-2] and macd_vals[-1] < sig9[-1]:
                    macd_signal = "SELL"
        
        # ── Bollinger Bands (20, 2σ) ──────────────────
        bb_sig = "WAIT"
        if len(closes) >= 22:
            window = closes[-20:]
            sma = sum(window) / 20
            std = math.sqrt(sum((x - sma) ** 2 for x in window) / 20) if len(window) > 0 else 0
            lower = sma - 2 * std
            pct = (closes[-1] - lower) / (4 * std) * 100 if std > 0 else 50
            
            if pct < 15:
                bb_sig = "BUY"
            elif pct > 85:
                bb_sig = "SELL"
        
        # ── Combine ───────────────────────────────────
        signals = []
        if rsi_v < 30 or rsi_div == "BULLISH":
            signals.append("BUY")
        elif rsi_v > 70 or rsi_div == "BEARISH":
            signals.append("SELL")
        else:
            signals.append("WAIT")
        
        signals.append(macd_signal)
        signals.append(bb_sig)
        
        buy_c = signals.count("BUY")
        sell_c = signals.count("SELL")
        
        if buy_c >= 2:
            mod = 18 if buy_c == 3 else 10
            reason = "RSI oversold" if rsi_v < 30 else ""
            reason += " + MACD bull" if macd_signal == "BUY" else ""
            reason += " + BB low" if bb_sig == "BUY" else ""
            if rsi_div == "BULLISH":
                reason += " + RSI div"
            return _r("BUY", mod, reason.strip("+").strip())
        
        if sell_c >= 2:
            mod = 18 if sell_c == 3 else 10
            reason = "RSI overbought" if rsi_v > 70 else ""
            reason += " + MACD bear" if macd_signal == "SELL" else ""
            reason += " + BB high" if bb_sig == "SELL" else ""
            if rsi_div == "BEARISH":
                reason += " + RSI div"
            return _r("SELL", mod, reason.strip("+").strip())
        
        # Single indicator
        if macd_signal != "WAIT":
            return _r(macd_signal, 6, f"MACD {macd_signal} only")
        
        return _r("WAIT", 0, f"Mixed: RSI={round(rsi_v,1)} MACD={macd_signal} BB={bb_sig}")
    
    except Exception as e:
        return _r("WAIT", 0, str(e)[:40])


def _rsi(closes, period=14):
    if len(closes) < period + 1:
        return []
    g = [max(closes[i] - closes[i - 1], 0) for i in range(1, len(closes))]
    l = [max(closes[i - 1] - closes[i], 0) for i in range(1, len(closes))]
    if len(g) < period or len(l) < period:
        return []
    ag = sum(g[:period]) / period
    al = sum(l[:period]) / period
    out = []
    for i in range(period, len(g)):
        ag = (ag * (period - 1) + g[i]) / period
        al = (al * (period - 1) + l[i]) / period
        rs = ag / al if al > 0 else 100
        out.append(100 - (100 / (1 + rs)))
    return out


def _ema(data, period):
    if len(data) < period:
        return []
    k = 2 / (period + 1)
    e = sum(data[:period]) / period
    out = [e]
    for v in data[period:]:
        e = v * k + e * (1 - k)
        out.append(e)
    return out


def _r(sig, mod, reason):
    return {"signal": sig, "score_mod": mod, "reason": reason}