# D02_deep_structure.py — Layer 3: Deep Structure Analysis
# Demand/Supply zones + Fair Value Gaps + Order Blocks
# DATA: fetcher cache — no extra API call

import os, sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", ".."))
from fetcher import get_rows

MERGE = 0.002  # 0.2% = same zone

def analyze(symbol, interval="15m"):
    try:
        rows = get_rows(symbol, interval, 150)
        if len(rows) < 20:
            return _r("WAIT", 45, "Not enough data", {})

        highs   = [r["high"]  for r in rows]
        lows    = [r["low"]   for r in rows]
        opens   = [r["open"]  for r in rows]
        closes  = [r["close"] for r in rows]
        current = closes[-1]

        # ── Demand Zones (strong bullish candles) ─────
        demand_zones = []
        for i in range(2, len(rows)-1):
            move = (closes[i]-opens[i])/opens[i] if opens[i]>0 else 0
            if move > 0.004:  # 0.4%+ bullish candle
                z_low  = min(opens[i], closes[i]) * 0.9998
                z_high = max(opens[i], closes[i]) * 1.0002
                dist   = abs(current - z_low)/current*100
                demand_zones.append({
                    "low": round(z_low,6), "high": round(z_high,6),
                    "mid": round((z_low+z_high)/2,6),
                    "strength": round(move*100,2), "dist_pct": round(dist,3)
                })

        # ── Supply Zones (strong bearish candles) ─────
        supply_zones = []
        for i in range(2, len(rows)-1):
            move = (opens[i]-closes[i])/opens[i] if opens[i]>0 else 0
            if move > 0.004:
                z_low  = min(opens[i], closes[i]) * 0.9998
                z_high = max(opens[i], closes[i]) * 1.0002
                dist   = abs(current - z_high)/current*100
                supply_zones.append({
                    "low": round(z_low,6), "high": round(z_high,6),
                    "mid": round((z_low+z_high)/2,6),
                    "strength": round(move*100,2), "dist_pct": round(dist,3)
                })

        # Nearest zones
        nearest_dem = sorted(
            [z for z in demand_zones if z["mid"] < current],
            key=lambda x: x["dist_pct"]
        )[:3]
        nearest_sup = sorted(
            [z for z in supply_zones if z["mid"] > current],
            key=lambda x: x["dist_pct"]
        )[:3]

        # ── Fair Value Gaps (FVG) ─────────────────────
        fvgs = []
        for i in range(1, len(rows)-1):
            # Bullish FVG: candle[i-1].high < candle[i+1].low
            if highs[i-1] < lows[i+1]:
                fvg_low  = highs[i-1]
                fvg_high = lows[i+1]
                dist = abs(current - (fvg_low+fvg_high)/2)/current*100
                fvgs.append({"type":"BULL","low":round(fvg_low,6),
                             "high":round(fvg_high,6),"dist_pct":round(dist,3)})
            # Bearish FVG: candle[i-1].low > candle[i+1].high
            elif lows[i-1] > highs[i+1]:
                fvg_low  = highs[i+1]
                fvg_high = lows[i-1]
                dist = abs(current - (fvg_low+fvg_high)/2)/current*100
                fvgs.append({"type":"BEAR","low":round(fvg_low,6),
                             "high":round(fvg_high,6),"dist_pct":round(dist,3)})

        nearest_fvg = sorted(fvgs, key=lambda x: x["dist_pct"])[:2]

        # ── Signal decision ───────────────────────────
        signal = "WAIT"; score = 48

        if nearest_dem and nearest_dem[0]["dist_pct"] < 0.3:
            signal = "BUY"
            score  = int(65 + min(nearest_dem[0]["strength"]*3, 20))
        elif nearest_sup and nearest_sup[0]["dist_pct"] < 0.3:
            signal = "SELL"
            score  = int(65 + min(nearest_sup[0]["strength"]*3, 20))
        elif nearest_fvg:
            fvg = nearest_fvg[0]
            if fvg["type"]=="BULL" and current < fvg["high"]:
                signal="BUY";  score=58
            elif fvg["type"]=="BEAR" and current > fvg["low"]:
                signal="SELL"; score=58

        nd_str = str(nearest_dem[0]["dist_pct"])+"%" if nearest_dem else "—"
        ns_str = str(nearest_sup[0]["dist_pct"])+"%" if nearest_sup else "—"
        summary = ("Demand zones:"+str(len(demand_zones))
                   +" | Supply:"+str(len(supply_zones))
                   +" | Nearest dem:"+nd_str
                   +" | Nearest sup:"+ns_str
                   +" | FVGs:"+str(len(fvgs)))

        return _r(signal, score, summary, {
            "nearest_demand":  nearest_dem,
            "nearest_supply":  nearest_sup,
            "nearest_fvg":     nearest_fvg,
            "demand_count":    len(demand_zones),
            "supply_count":    len(supply_zones),
        })

    except Exception as e:
        return _r("WAIT", 45, str(e)[:50], {})


def _r(sig, score, summary, details):
    return {"signal":sig,"score":score,"summary":summary,"details":details}