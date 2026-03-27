# F01_next_candle.py — Next candle probability forecast
import os, sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", ".."))

# Fixed import - use data_manager instead of fetcher
try:
    from data_manager import get_rows, stage_d, stage_z, stage_a
except ImportError:
    # Fallback if data_manager not available
    def get_rows(*args, **kwargs):
        return []

def forecast(symbol, interval="15m"):
    try:
        rows = get_rows(symbol, interval, 50)
        if len(rows) < 10:
            return _neutral("Not enough data")

        closes  = [r["close"]  for r in rows]
        highs   = [r["high"]   for r in rows]
        lows    = [r["low"]    for r in rows]
        volumes = [r["volume"] for r in rows]
        opens   = [r["open"]   for r in rows]

        # ROC
        roc3 = (closes[-1]-closes[-3])/closes[-3]*100 if closes[-3]>0 else 0
        roc5 = (closes[-1]-closes[-5])/closes[-5]*100 if len(closes)>=5 and closes[-5]>0 else 0

        # Momentum bias
        mom = 0
        if roc5 >  0.10: mom += 20
        elif roc5 < -0.10: mom -= 20
        if roc3 >  0.05: mom += 10
        elif roc3 < -0.05: mom -= 10

        # Last candle
        c = rows[-1]; p = rows[-2]
        co,cc = c["open"],c["close"]
        po,pc = p["open"],p["close"]
        body_c = abs(cc-co)
        rng_c  = c["high"]-c["low"] if c["high"]>c["low"] else 0.0001
        uw = c["high"]-max(co,cc); lw = min(co,cc)-c["low"]

        cand = 0
        if cc>co and pc<po and co<pc and cc>po: cand=+15  # Bull engulf
        elif cc<co and pc>po and co>pc and cc<po: cand=-15  # Bear engulf
        elif cc>co and body_c>rng_c*0.7: cand=+10
        elif cc<co and body_c>rng_c*0.7: cand=-10
        elif lw>rng_c*0.6 and body_c<rng_c*0.3: cand=+8
        elif uw>rng_c*0.6 and body_c<rng_c*0.3: cand=-8

        # S/R position
        hi50 = max(highs[-50:]); lo50 = min(lows[-50:])
        rng50 = hi50-lo50
        pos   = (closes[-1]-lo50)/rng50*100 if rng50>0 else 50
        sr = +12 if pos<20 else -12 if pos>80 else 0

        # Volume
        vols = [v for v in volumes[-5:] if v>0]
        vol_bias = 0
        if len(vols)>=2:
            avg = sum(vols[:-1])/len(vols[:-1]) if len(vols)>1 else vols[-1]
            ratio = vols[-1]/avg if avg>0 else 1
            if ratio>1.8 and cc>co:  vol_bias=+8
            elif ratio>1.8 and cc<co: vol_bias=-8

        # Historical
        sim_up=0; sim_dn=0
        for i in range(5, len(closes)-1):
            hr = (closes[i]-closes[i-5])/closes[i-5]*100 if closes[i-5]>0 else 0
            same = (hr>0.05 and roc5>0.05) or (hr<-0.05 and roc5<-0.05)
            if same:
                if closes[i+1]>closes[i]: sim_up+=1
                else: sim_dn+=1
        hist = 0
        ht = sim_up+sim_dn
        if ht>=5: hist = int((sim_up/ht*100-50)*0.5)

        total = max(-60, min(60, mom+cand+sr+vol_bias+hist))

        up1   = max(5, min(85, 50+total))
        down1 = max(5, min(85, 50-total))
        flat1 = max(0, 100-up1-down1)

        b2    = int(total*0.7)
        up2   = max(10, min(80, 50+b2)); down2=max(10,min(80,50-b2)); flat2=max(0,100-up2-down2)

        b3    = int(total*0.5)
        up3   = max(15, min(75, 50+b3)); down3=max(15,min(75,50-b3)); flat3=max(0,100-up3-down3)

        conf  = min(85, int(30+abs(total)*0.8))
        dirn  = "UP" if total>5 else "DOWN" if total<-5 else "FLAT"

        return {
            "symbol":    symbol,
            "interval":  interval,
            "direction": dirn,
            "confidence":conf,
            "next_1":    {"up":up1,"down":down1,"flat":flat1},
            "next_2":    {"up":up2,"down":down2,"flat":flat2},
            "next_3":    {"up":up3,"down":down3,"flat":flat3},
            "factors":   {"momentum":mom,"candle":cand,"sr":sr,"volume":vol_bias,"history":hist,"total":total},
        }
    except Exception as e:
        return _neutral(str(e)[:50])


def get_table(symbol, interval="15m"):
    r    = forecast(symbol, interval)
    dirn = r.get("direction","FLAT")
    conf = r.get("confidence",30)
    n1   = r.get("next_1",{}); n2=r.get("next_2",{}); n3=r.get("next_3",{})
    fc   = r.get("factors",{})

    dc = {"UP":"#44cc88","DOWN":"#ff8866","FLAT":"#ffcc44"}.get(dirn,"#ffcc44")

    html = (
        "<div style='background:rgba(0,0,0,0.18);border:1px solid "
        +dc+"33;border-radius:8px;padding:10px 14px;margin-bottom:10px;'>"
        "<span style='color:"+dc+";font-size:13px;font-weight:bold;'>"+dirn+"</span>"
        "<span style='color:#888;font-size:11px;margin-left:8px;'>Confidence: "+str(conf)+"%</span>"
        "</div>"
        "<table><tr><th>Candle</th><th>UP</th><th>DOWN</th><th>FLAT</th></tr>"
    )
    for label,nc in [("Next 1",n1),("Next 2",n2),("Next 3",n3)]:
        html += ("<tr><td>"+label+"</td>"
                 "<td class='up'>"+str(nc.get("up",50))+"%</td>"
                 "<td class='down'>"+str(nc.get("down",50))+"%</td>"
                 "<td style='color:#888;'>"+str(nc.get("flat",0))+"%</td></tr>")
    html += "</table>"

    html += "<br><table><tr><th>Factor</th><th>Bias</th></tr>"
    for k,v in fc.items():
        if k=="total": continue
        cls = "up" if v>0 else "down" if v<0 else ""
        html += ("<tr><td>"+k.title()+"</td>"
                 "<td class='"+cls+"'>"
                 +("+" if v>0 else "")+str(v)+"</td></tr>")
    html += "</table>"
    return html


def _neutral(reason):
    return {
        "symbol":"","interval":"","direction":"FLAT","confidence":30,
        "next_1":{"up":50,"down":50,"flat":0},
        "next_2":{"up":50,"down":50,"flat":0},
        "next_3":{"up":50,"down":50,"flat":0},
        "factors":{},"reason":reason
    }

# Export the function for sys_data
__all__ = ['forecast', 'get_table']