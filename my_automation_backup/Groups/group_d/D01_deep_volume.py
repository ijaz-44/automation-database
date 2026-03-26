# D01_deep_volume.py — Layer 3: Deep Volume Analysis
# DATA: fetcher.get_rows() — uses cache, no extra API call
# INTERFACE: analyze(symbol, interval) → dict

import os, sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", ".."))
from fetcher import get_rows

def analyze(symbol, interval="15m"):
    try:
        rows = get_rows(symbol, interval, 100)
        if len(rows) < 10:
            return _r("WAIT", 50, "Not enough data", {})

        closes  = [r["close"]  for r in rows]
        volumes = [r["volume"] for r in rows]
        opens   = [r["open"]   for r in rows]

        # ── OBV (On Balance Volume) ───────────────────
        obv = 0; obvs = []
        for i in range(1, len(closes)):
            if closes[i] > closes[i-1]:   obv += volumes[i]
            elif closes[i] < closes[i-1]: obv -= volumes[i]
            obvs.append(obv)

        obv_trend = "UP" if len(obvs)>=5 and obvs[-1]>obvs[-5] else "DOWN"

        # ── CVD (Cumulative Volume Delta) ─────────────
        # Estimate: bullish candle = buy vol, bearish = sell vol
        cvd = 0; cvds = []
        for i in range(len(rows)):
            c,o,v = closes[i],opens[i],volumes[i]
            if c >= o:
                cvd += v * ((c-o)/(rows[i]["high"]-rows[i]["low"]+0.0001))
            else:
                cvd -= v * ((o-c)/(rows[i]["high"]-rows[i]["low"]+0.0001))
            cvds.append(cvd)

        cvd_trend = "UP" if len(cvds)>=5 and cvds[-1]>cvds[-5] else "DOWN"

        # ── Volume Profile (POC — Point of Control) ───
        price_min = min(r["low"]  for r in rows[-50:])
        price_max = max(r["high"] for r in rows[-50:])
        bins = 10
        step = (price_max - price_min) / bins if price_max > price_min else 0.001
        vol_by_level = {}
        for r in rows[-50:]:
            lvl = round((r["close"]-price_min) / step)
            lvl = max(0, min(bins-1, lvl))
            vol_by_level[lvl] = vol_by_level.get(lvl,0) + r["volume"]
        poc_lvl   = max(vol_by_level, key=vol_by_level.get) if vol_by_level else 5
        poc_price = round(price_min + poc_lvl * step, 6)
        current   = closes[-1]
        above_poc = current > poc_price

        # ── Buy/Sell pressure (last 10 candles) ───────
        buy_vol  = sum(volumes[i] for i in range(len(rows)-10, len(rows))
                       if i>=0 and closes[i]>=opens[i])
        sell_vol = sum(volumes[i] for i in range(len(rows)-10, len(rows))
                       if i>=0 and closes[i]<opens[i])
        total_vol= buy_vol + sell_vol
        buy_pct  = int(buy_vol/total_vol*100) if total_vol>0 else 50

        # ── Volume spike recent ───────────────────────
        recent_vols = [r["volume"] for r in rows[-5:] if r["volume"]>0]
        avg_vol     = sum(volumes[:-5])/len(volumes[:-5]) if len(volumes)>5 else 1
        spike = recent_vols[-1] > avg_vol*2.0 if recent_vols and avg_vol>0 else False

        # ── Signal ────────────────────────────────────
        bull_pts = 0; bear_pts = 0
        if obv_trend=="UP":   bull_pts+=2
        else:                 bear_pts+=2
        if cvd_trend=="UP":   bull_pts+=2
        else:                 bear_pts+=2
        if above_poc:         bull_pts+=1
        else:                 bear_pts+=1
        if buy_pct > 55:      bull_pts+=2
        elif buy_pct < 45:    bear_pts+=2

        total_pts = bull_pts + bear_pts
        if total_pts == 0: total_pts = 1

        if bull_pts > bear_pts:
            score = int(50 + (bull_pts/total_pts)*40)
            signal= "BUY"
        elif bear_pts > bull_pts:
            score = int(50 + (bear_pts/total_pts)*40)
            signal= "SELL"
        else:
            score = 45; signal="WAIT"

        summary = ("OBV "+obv_trend+" | CVD "+cvd_trend
                   +" | BuyPres:"+str(buy_pct)+"%"
                   +" | POC:"+str(poc_price)
                   +(" | Vol SPIKE" if spike else ""))

        return _r(signal, score, summary, {
            "obv_trend":  obv_trend,
            "cvd_trend":  cvd_trend,
            "poc_price":  poc_price,
            "above_poc":  above_poc,
            "buy_pct":    buy_pct,
            "spike":      spike,
        })

    except Exception as e:
        return _r("WAIT", 45, str(e)[:50], {})


def _r(sig, score, summary, details):
    return {
        "signal":  sig,
        "score":   score,
        "summary": summary,
        "details": details,
    }