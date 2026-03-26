# A03_sr.py — Layer 2: Support & Resistance
# NO API CALLS — sirf rows se kaam karta hai

N = 3
MERGE = 0.0015
MIN_T = 2


def confirm(symbol, interval="15m", rows=None):
    try:
        if not rows or len(rows) < 20:
            return _r("WAIT", 0, "Mid", "Not enough data", 0, 0)
        
        highs = [r["high"] for r in rows]
        lows = [r["low"] for r in rows]
        current = rows[-1]["close"]
        
        # Find swing highs and lows
        sh = []
        sl = []
        for i in range(N, len(highs) - N):
            if highs[i] == max(highs[i - N:i + N + 1]):
                sh.append(highs[i])
            if lows[i] == min(lows[i - N:i + N + 1]):
                sl.append(lows[i])
        
        res = _cluster(sh)
        sup = _cluster(sl)
        
        above = sorted([l for l in res if l["price"] > current], key=lambda x: x["price"])
        below = sorted([l for l in sup if l["price"] < current], key=lambda x: x["price"], reverse=True)
        
        if not above and not below:
            return _r("WAIT", 0, "Mid", "No levels found", 0, 0)
        
        ns = below[0]["price"] if below else current * 0.995
        nr = above[0]["price"] if above else current * 1.005
        rng = nr - ns
        
        if rng <= 0:
            return _r("WAIT", 0, "Mid", "Invalid range", ns, nr)
        
        pos = (current - ns) / rng
        ds = round((current - ns) / current * 100, 3)
        dr = round((nr - current) / current * 100, 3)
        
        if pos < 0.18:
            touches = below[0]["touches"] if below else 1
            mod = 15 + min(touches, 3) * 2
            return _r("BUY", mod, "Near Support", f"Support {round(ns,5)} ({touches}x) {ds}%", ns, nr)
        
        if pos > 0.82:
            touches = above[0]["touches"] if above else 1
            mod = 15 + min(touches, 3) * 2
            return _r("SELL", mod, "Near Resistance", f"Resist {round(nr,5)} ({touches}x) {dr}%", ns, nr)
        
        if pos < 0.35:
            return _r("BUY", 6, "Lower Half", f"Lower half — sup:{ds}%", ns, nr)
        if pos > 0.65:
            return _r("SELL", 6, "Upper Half", f"Upper half — res:{dr}%", ns, nr)
        
        return _r("WAIT", 0, "Mid Range", f"Mid — sup:{ds}% res:{dr}%", ns, nr)
    
    except Exception as e:
        return _r("WAIT", 0, "Error", str(e)[:40], 0, 0)


def _cluster(points):
    if not points:
        return []
    srt = sorted(set(points))
    out = []
    used = set()
    for i, p in enumerate(srt):
        if i in used:
            continue
        grp = [p]
        gi = [i]
        for j, q in enumerate(srt):
            if j != i and j not in used and p > 0 and abs(p - q) / p < MERGE:
                grp.append(q)
                gi.append(j)
        for x in gi:
            used.add(x)
        if len(grp) >= MIN_T:
            out.append({"price": round(sum(grp) / len(grp), 6), "touches": len(grp)})
    return out


def _r(sig, mod, pos, reason, ns, nr):
    return {"signal": sig, "score_mod": mod, "position": pos, "reason": reason, "nearest_sup": ns, "nearest_res": nr}