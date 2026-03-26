# D03_deep_indicators.py — Layer 3: Advanced Indicators
# Stochastic + Williams%R + ADX + multi-timeframe bias
# DATA: fetcher cache — no extra API call

import os, sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", ".."))
from fetcher import get_rows

def analyze(symbol, interval="15m"):
    try:
        rows = get_rows(symbol, interval, 100)
        if len(rows) < 20:
            return _r("WAIT", 45, "Not enough data", {})

        highs  = [r["high"]  for r in rows]
        lows   = [r["low"]   for r in rows]
        closes = [r["close"] for r in rows]

        # ── Stochastic (14,3) ─────────────────────────
        k_vals = []
        for i in range(14, len(closes)):
            h14 = max(highs[i-14:i])
            l14 = min(lows[i-14:i])
            k   = (closes[i]-l14)/(h14-l14)*100 if (h14-l14)>0 else 50
            k_vals.append(k)
        stoch_k = round(k_vals[-1],1) if k_vals else 50
        stoch_d = round(sum(k_vals[-3:])/3,1) if len(k_vals)>=3 else stoch_k
        stoch_sig = "WAIT"
        if stoch_k < 20 and stoch_d < 20: stoch_sig="BUY"
        elif stoch_k > 80 and stoch_d > 80: stoch_sig="SELL"
        elif stoch_k > stoch_d and stoch_k < 50: stoch_sig="BUY"
        elif stoch_k < stoch_d and stoch_k > 50: stoch_sig="SELL"

        # ── Williams %R (14) ──────────────────────────
        h14 = max(highs[-14:]); l14 = min(lows[-14:])
        wr  = (h14-closes[-1])/(h14-l14)*-100 if (h14-l14)>0 else -50
        wr  = round(wr,1)
        wr_sig = "BUY" if wr<-80 else "SELL" if wr>-20 else "WAIT"

        # ── ADX (14) — trend strength ─────────────────
        adx_val = _adx(highs, lows, closes, 14)
        trend_strong = adx_val > 25  # ADX>25 = strong trend

        # ── Higher timeframe bias (use 1h if possible) ─
        # Use same data but look at last 20 candles slope
        # as proxy for higher TF bias
        htf_closes = closes[-20:]
        ema5_htf   = _ema(htf_closes, 5)
        ema20_htf  = _ema(htf_closes, 20)
        htf_bias   = "BULL" if (ema5_htf and ema20_htf and
                                 ema5_htf[-1]>ema20_htf[-1]) else "BEAR"

        # ── Combine ───────────────────────────────────
        bull=0; bear=0
        if stoch_sig=="BUY":   bull+=2
        elif stoch_sig=="SELL":bear+=2
        if wr_sig=="BUY":      bull+=2
        elif wr_sig=="SELL":   bear+=2
        if htf_bias=="BULL":   bull+=2
        else:                  bear+=2
        if trend_strong:       # Strong trend boost direction
            if bull>bear: bull+=1
            else: bear+=1

        total = bull+bear
        if total==0: total=1

        if bull>bear:
            score  = int(50+(bull/total)*42)
            signal = "BUY"
        elif bear>bull:
            score  = int(50+(bear/total)*42)
            signal = "SELL"
        else:
            score=45; signal="WAIT"

        summary = ("Stoch K:"+str(stoch_k)+" D:"+str(stoch_d)
                   +" | WR:"+str(wr)
                   +" | ADX:"+str(adx_val)
                   +(" STRONG" if trend_strong else " WEAK")
                   +" | HTF:"+htf_bias)

        return _r(signal, score, summary, {
            "stoch_k":      stoch_k,
            "stoch_d":      stoch_d,
            "stoch_signal": stoch_sig,
            "williams_r":   wr,
            "wr_signal":    wr_sig,
            "adx":          adx_val,
            "trend_strong": trend_strong,
            "htf_bias":     htf_bias,
        })

    except Exception as e:
        return _r("WAIT", 45, str(e)[:50], {})


def _adx(highs, lows, closes, period=14):
    try:
        if len(closes) < period+2: return 20
        dm_plus=[]; dm_minus=[]; tr_list=[]
        for i in range(1,len(closes)):
            h_diff = highs[i]-highs[i-1]
            l_diff = lows[i-1]-lows[i]
            dm_plus.append(max(h_diff,0) if h_diff>l_diff else 0)
            dm_minus.append(max(l_diff,0) if l_diff>h_diff else 0)
            tr = max(highs[i]-lows[i],
                     abs(highs[i]-closes[i-1]),
                     abs(lows[i]-closes[i-1]))
            tr_list.append(tr)
        atr=sum(tr_list[:period])/period
        dmp=sum(dm_plus[:period])/period
        dmm=sum(dm_minus[:period])/period
        for i in range(period,len(tr_list)):
            atr=atr*(period-1)/period+tr_list[i]
            dmp=dmp*(period-1)/period+dm_plus[i]
            dmm=dmm*(period-1)/period+dm_minus[i]
        di_plus  = dmp/atr*100 if atr>0 else 0
        di_minus = dmm/atr*100 if atr>0 else 0
        dx = abs(di_plus-di_minus)/(di_plus+di_minus)*100 if (di_plus+di_minus)>0 else 0
        return round(dx,1)
    except Exception:
        return 20

def _ema(data, period):
    if len(data)<period: return []
    k=2/(period+1); e=sum(data[:period])/period; out=[e]
    for v in data[period:]: e=v*k+e*(1-k); out.append(e)
    return out

def _r(sig, score, summary, details):
    return {"signal":sig,"score":score,"summary":summary,"details":details}