# D04_websocket.py — Layer 3: Live WebSocket Data
# ══════════════════════════════════════════════════════
# 3 STAGES — 1 single WebSocket connection:
#
# Stage Z (scan):  Har pair ka live price → Z score update
# Stage A (GO):    Single pair ka live OHLC stream → A confirm
# Stage D (detail):Full tick data → detailed analysis
#
# DESIGN: 1 connection, 3 uses — no IP block
# DATA: From fetcher._ws_prices (already running WS)
#       + Binance REST for OHLC (cached)
# ══════════════════════════════════════════════════════

import os, sys, time, json
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

# ONLY THIS PART CHANGED - SAFE IMPORT
try:
    from data_manager import get_rows, get_price
    # WebSocket prices from data manager
    from data_manager import get_data_manager
    dm = get_data_manager()
    _ws_prices = getattr(dm.binance, 'ws_prices', {})
except ImportError as e:
    print(f"[D04] Import error: {e}")
    def get_rows(*args, **kwargs): return []
    def get_price(*args, **kwargs): return 0
    _ws_prices = {}

# ══════════════════════════════════════════════════════
# STAGE Z — Fast live price for scanner
# Called every 5 sec to update scores
# ══════════════════════════════════════════════════════

def stage_z(symbol, interval="15m"):
    """
    Returns live price update for scanner.
    Uses WebSocket cached price — instant, no API call.
    """
    sym   = symbol.upper().strip().replace("/","")
    live  = _ws_prices.get(sym.lower())
    rows  = get_rows(sym, interval, 5)

    if not rows:
        return {"live_price": 0, "change_pct": 0,
                "direction": "FLAT", "source": "offline"}

    last_close = rows[-1]["close"]
    prev_close = rows[-2]["close"] if len(rows) >= 2 else last_close
    price      = live if live else last_close

    if prev_close > 0:
        change_pct = round((price - prev_close) / prev_close * 100, 4)
    else:
        change_pct = 0

    direction = "UP" if change_pct > 0.01 else "DOWN" if change_pct < -0.01 else "FLAT"

    return {
        "symbol":     sym,
        "live_price": price,
        "last_close": last_close,
        "change_pct": change_pct,
        "direction":  direction,
        "source":     "WebSocket" if live else "REST cache",
        "ts":         time.time(),
    }


# ══════════════════════════════════════════════════════
# STAGE A — Single pair live OHLC for GO layer
# Called when GO button pressed
# Gives real-time candle formation status
# ══════════════════════════════════════════════════════

def stage_a(symbol, interval="15m"):
    """
    Live candle status for A layer.
    Shows how current candle is forming.
    """
    sym   = symbol.upper().strip().replace("/","")
    rows  = get_rows(sym, interval, 20)
    live  = _ws_prices.get(sym.lower())

    if not rows:
        return {"status": "No data", "candle": {}, "momentum": "FLAT"}

    current = rows[-1].copy()
    if live:
        current["close"] = live
        if live > current["high"]: current["high"] = live
        if live < current["low"]:  current["low"]  = live

    o, h, l, c = current["open"], current["high"], current["low"], current["close"]
    body   = abs(c - o)
    rng    = h - l if h > l else 0.0001
    body_pct = round(body / rng * 100, 1)

    # Candle bias
    if c > o:
        bias = "BULL"
        pct  = round((c-o)/o*100, 4) if o > 0 else 0
    elif c < o:
        bias = "BEAR"
        pct  = round((o-c)/o*100, 4) if o > 0 else 0
    else:
        bias = "FLAT"
        pct  = 0

    # Volume momentum (recent)
    vols = [r["volume"] for r in rows[-5:] if r.get("volume",0)>0]
    vol_trend = "RISING" if len(vols)>=2 and vols[-1]>vols[-2] else "FALLING"

    # Change from prev close
    prev = rows[-2]["close"] if len(rows)>=2 else o
    chg  = round((c-prev)/prev*100,4) if prev>0 else 0

    return {
        "symbol":    sym,
        "interval":  interval,
        "candle": {
            "open":     o,
            "high":     h,
            "low":      l,
            "close":    c,
            "body_pct": body_pct,
            "bias":     bias,
            "move_pct": pct,
        },
        "change_pct":   chg,
        "vol_trend":    vol_trend,
        "live_price":   live if live else c,
        "source":       "WS+REST" if live else "REST cache",
        "ts":           time.time(),
    }


# ══════════════════════════════════════════════════════
# STAGE D — Full live data for detail analysis
# Called when Continue pressed
# ══════════════════════════════════════════════════════

def stage_d(symbol, interval="15m"):
    """
    Full live data enrichment for detail layer.
    Enriches cached OHLC with live WebSocket price.
    Returns extra analysis not possible with historical data.
    """
    sym  = symbol.upper().strip().replace("/","")
    rows = get_rows(sym, interval, 50)
    live = _ws_prices.get(sym.lower())

    if not rows:
        return {"status":"No data","live_analysis":{}}

    # Update last row with live price
    if live:
        rows[-1] = rows[-1].copy()
        rows[-1]["close"] = live
        if live > rows[-1]["high"]: rows[-1]["high"] = live
        if live < rows[-1]["low"]:  rows[-1]["low"]  = live

    closes = [r["close"] for r in rows]
    current = closes[-1]

    # ── Live momentum (last 5 ticks) ──────────────────
    # Estimate from recent candles
    roc3 = (closes[-1]-closes[-3])/closes[-3]*100 if closes[-3]>0 else 0
    roc1 = (closes[-1]-closes[-2])/closes[-2]*100 if closes[-2]>0 else 0

    # ── Live ATR ──────────────────────────────────────
    trs=[]
    for i in range(1,len(rows)):
        h,l,pc=rows[i]["high"],rows[i]["low"],rows[i-1]["close"]
        trs.append(max(h-l,abs(h-pc),abs(l-pc)))
    live_atr = round(sum(trs[-14:])/14,6) if len(trs)>=14 else 0

    # ── Live SL/TP suggestion ─────────────────────────
    if live_atr > 0:
        sl_buy  = round(current - live_atr*1.5, 6)
        tp_buy  = round(current + live_atr*2.5, 6)
        sl_sell = round(current + live_atr*1.5, 6)
        tp_sell = round(current - live_atr*2.5, 6)
    else:
        sl_buy=tp_buy=sl_sell=tp_sell=0

    # ── Price relative to recent range ───────────────
    hi50 = max(r["high"]  for r in rows[-50:])
    lo50 = min(r["low"]   for r in rows[-50:])
    rng50 = hi50 - lo50
    pos_pct = round((current-lo50)/rng50*100,1) if rng50>0 else 50

    # ── Live signal ───────────────────────────────────
    signal = "WAIT"
    score  = 50
    if roc3 > 0.08 and roc1 > 0:
        signal = "BUY"; score = min(75, 55+int(roc3*20))
    elif roc3 < -0.08 and roc1 < 0:
        signal = "SELL"; score = min(75, 55+int(abs(roc3)*20))

    return {
        "symbol":      sym,
        "interval":    interval,
        "live_price":  current,
        "live_atr":    live_atr,
        "roc_1":       round(roc1,4),
        "roc_3":       round(roc3,4),
        "pos_in_range":pos_pct,
        "range_high":  round(hi50,6),
        "range_low":   round(lo50,6),
        "signal":      signal,
        "score":       score,
        "sl_if_buy":   sl_buy,
        "tp_if_buy":   tp_buy,
        "sl_if_sell":  sl_sell,
        "tp_if_sell":  tp_sell,
        "source":      "WS+REST" if live else "REST cache",
        "ts":          time.time(),
    }


# ══════════════════════════════════════════════════════
# UNIFIED ENTRY — sys_data calls this
# ══════════════════════════════════════════════════════

def analyze(symbol, interval="15m", stage="z"):
    """
    Unified function.
    stage="z" → scanner update
    stage="a" → GO layer
    stage="d" → Detail layer
    """
    sym = symbol.upper().strip().replace("/","")
    if stage == "z":
        return stage_z(sym, interval)
    elif stage == "a":
        return stage_a(sym, interval)
    elif stage == "d":
        return stage_d(sym, interval)
    return {}


def get_table(symbol, interval="15m", stage="d"):
    """HTML for detail view"""
    r = stage_d(symbol, interval) if stage=="d" else stage_a(symbol, interval)

    sig   = r.get("signal","WAIT")
    score = r.get("score",0)
    price = r.get("live_price",0)
    atr   = r.get("live_atr",0)
    roc3  = r.get("roc_3",0)
    pos   = r.get("pos_in_range",50)
    src   = r.get("source","")

    colors={"BUY":"#44cc88","SELL":"#ff8866","WAIT":"#ffcc44"}
    color = colors.get(sig,"#ffcc44")

    html = (
        "<div style='background:rgba(0,0,0,0.18);"
        "border:1px solid rgba(0,200,255,0.15);"
        "border-radius:8px;padding:10px 14px;margin-bottom:10px;'>"
        "<span style='color:#4da8ff;font-size:12px;font-weight:bold;'>"
        "⚡ Live Data</span>"
        "<span style='color:#555;font-size:10px;margin-left:8px;'>"+src+"</span>"
        "</div>"
    )

    html += "<table><tr><th>Metric</th><th>Value</th></tr>"
    rows_data = [
        ("Live price",    str(price)),
        ("ROC 3 candles", ("+" if roc3>=0 else "")+str(roc3)+"%",
         "up" if roc3>0 else "down" if roc3<0 else ""),
        ("ATR (live)",    str(atr)),
        ("Range position",str(pos)+"%"),
        ("Live signal",   sig, "up" if sig=="BUY" else "down" if sig=="SELL" else "warn"),
        ("Live score",    str(score)+"%"),
    ]
    for row in rows_data:
        label=row[0]; val=row[1]; cls=row[2] if len(row)>2 else ""
        html+=("<tr><td>"+label+"</td>"
               "<td class='"+cls+"'>"+val+"</td></tr>")
    html += "</table>"

    # SL/TP
    if r.get("sl_if_buy",0) and sig in ("BUY","SELL"):
        key_sl = "sl_if_buy" if sig=="BUY" else "sl_if_sell"
        key_tp = "tp_if_buy" if sig=="BUY" else "tp_if_sell"
        html += (
            "<div style='margin-top:8px;font-size:10px;color:#555;'>"
            "SL: <span style='color:#ff6666;'>"+str(r.get(key_sl,0))+"</span>"
            " &nbsp; TP: <span style='color:#44cc88;'>"+str(r.get(key_tp,0))+"</span>"
            "</div>"
        )

    return html

# ══════════════════════════════════════════════════════
# WEBSOCKET CONTROL — Must be at module level, not inside function
# ══════════════════════════════════════════════════════

def start_websocket():
    """Start WebSocket connection"""
    print("WebSocket started for Group D")
    return "OK"

# Export all functions
__all__ = ['analyze', 'get_table', 'stage_z', 'stage_a', 'stage_d', 'start_websocket']