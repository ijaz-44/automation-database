# L01_loss_finder.py — Loss Auditor
import os, sys, json, datetime
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", ".."))
from fetcher import get_rows

BASE_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "..")
LOG_PATH = os.path.join(BASE_DIR, "temp", "audit_log.json")


def audit(symbol, interval, signal_was, outcome, entry_price=0, exit_price=0):
    sym     = symbol.upper().strip()
    rows    = get_rows(sym, interval, 50)
    reasons = []; hints = []; severity = 0

    if len(rows) < 10:
        return _r(sym, signal_was, outcome, ["No data to analyze"], [], 0)

    closes  = [r["close"]  for r in rows]
    highs   = [r["high"]   for r in rows]
    lows    = [r["low"]    for r in rows]
    volumes = [r["volume"] for r in rows]

    # Check 1: Volume dead?
    vols = [v for v in volumes[-10:] if v>0]
    if vols:
        avg = sum(vols[:-5])/len(vols[:-5]) if len(vols)>5 else vols[-1]
        if avg>0 and vols[-1]/avg < 0.3:
            reasons.append("Volume dead ("+str(round(vols[-1]/avg,2))+"x) — no participation")
            hints.append("Increase Z02 dead volume penalty in rules.py")
            severity += 2

    # Check 2: ATR extreme?
    trs = []
    for i in range(1,len(rows)):
        h,l,pc = rows[i]["high"],rows[i]["low"],rows[i-1]["close"]
        trs.append(max(h-l,abs(h-pc),abs(l-pc)))
    if trs:
        atr = sum(trs[-14:])/14
        hist_atr = sum(trs[:20])/20 if len(trs)>=20 else atr
        if hist_atr>0:
            ratio = atr/hist_atr
            if ratio>3.0:
                reasons.append("Extreme volatility ("+str(round(ratio,1))+"x) — unpredictable")
                hints.append("Add ATR check in rules.py — block if ratio > 2.5")
                severity += 3
            elif ratio<0.3:
                reasons.append("Market frozen ("+str(round(ratio,2))+"x ATR)")
                hints.append("Increase Z03 frozen penalty in rules.py")
                severity += 2

    # Check 3: EMA against signal?
    def ema(data, p):
        if len(data)<p: return []
        k=2/(p+1); e=sum(data[:p])/p; out=[e]
        for v in data[p:]: e=v*k+e*(1-k); out.append(e)
        return out
    e10=ema(closes,10); e20=ema(closes,20)
    if e10 and e20:
        if signal_was=="BUY" and e10[-1]<e20[-1]:
            reasons.append("EMA bearish when BUY signal given")
            hints.append("Increase Z01 trend weight in rules.py (0.30 → 0.35)")
            severity += 3
        elif signal_was=="SELL" and e10[-1]>e20[-1]:
            reasons.append("EMA bullish when SELL signal given")
            hints.append("Increase Z01 trend weight in rules.py (0.30 → 0.35)")
            severity += 3

    # Check 4: Near resistance (BUY)?
    if signal_was=="BUY":
        res = max(highs[-20:])
        dist = (res-closes[-1])/closes[-1]*100 if closes[-1]>0 else 1
        if dist < 0.15:
            reasons.append("BUY too close to resistance ("+str(round(dist,3))+"%)")
            hints.append("Tighten A03 near_resistance threshold")
            severity += 2

    # Check 5: Near support (SELL)?
    if signal_was=="SELL":
        sup = min(lows[-20:])
        dist = (closes[-1]-sup)/closes[-1]*100 if closes[-1]>0 else 1
        if dist < 0.15:
            reasons.append("SELL too close to support ("+str(round(dist,3))+"%)")
            hints.append("Tighten A03 near_support threshold")
            severity += 2

    if not reasons:
        reasons.append("No clear reason — market noise or random outcome")
        hints.append("Accept some losses — focus on win rate over 20+ trades")
        severity = 1

    result = _r(sym, signal_was, outcome, reasons, hints, severity)
    _log(result)
    return result


def get_table(symbol, interval, signal_was="BUY", outcome="LOSS",
              entry_price=0, exit_price=0):
    r   = audit(symbol, interval, signal_was, outcome, entry_price, exit_price)
    sev = r.get("severity",0)
    sc  = "#ff4444" if sev>=7 else "#ff8866" if sev>=4 else "#ffcc44"

    html = (
        "<div style='background:rgba(255,60,60,0.04);"
        "border:1px solid rgba(255,60,60,0.2);"
        "border-radius:8px;padding:10px 14px;margin-bottom:10px;'>"
        "<span style='color:#ff6666;font-weight:bold;'>Loss Audit</span>"
        "<span style='color:"+sc+";font-size:11px;margin-left:8px;'>"
        "Severity: "+str(sev)+"/10</span></div>"
        "<table><tr><th>#</th><th>Reason</th><th>Fix</th></tr>"
    )
    reasons = r.get("reasons",[])
    hints   = r.get("hints",[])
    for i,(reason,hint) in enumerate(zip(reasons,hints),1):
        html += ("<tr><td style='color:#555;'>"+str(i)+"</td>"
                 "<td class='down' style='font-size:10px;'>"+reason+"</td>"
                 "<td style='color:#4da8ff;font-size:10px;'>"+hint+"</td></tr>")
    html += "</table>"
    return html


def get_log():
    try:
        with open(LOG_PATH) as f: return json.load(f)
    except Exception: return []


def _log(result):
    try:
        os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)
        log = []
        try:
            with open(LOG_PATH) as f: log = json.load(f)
        except Exception: pass
        log.append({
            "ts":      datetime.datetime.utcnow().isoformat(),
            "symbol":  result.get("symbol",""),
            "signal":  result.get("signal_was",""),
            "outcome": result.get("outcome",""),
            "severity":result.get("severity",0),
            "reasons": result.get("reasons",[]),
        })
        with open(LOG_PATH,"w") as f: json.dump(log[-100:], f)
    except Exception: pass


def _r(sym, signal_was, outcome, reasons, hints, severity):
    return {
        "symbol":     sym,
        "signal_was": signal_was,
        "outcome":    outcome,
        "reasons":    reasons,
        "hints":      hints,
        "severity":   severity,
        "ts":         datetime.datetime.utcnow().isoformat(),
    }