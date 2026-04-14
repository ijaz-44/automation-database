import sys, json, os, gc
from flask import Flask, request, jsonify, render_template_string, send_file
from sys_data import SysData
from pairs import get_pairs_by_market
from config import HTML_TEMPLATE, MARKET_TIMEFRAMES

import logging
log = logging.getLogger('werkzeug')
log.setLevel(logging.ERROR)   # ya WARNING

print("[Main] Starting...")
app = Flask(__name__)

try:
    sys_engine = SysData()
    print("✅ [Main] SysData initialized")
except Exception as e:
    print(f"❌ [Main] SysData init error: {e}")
    sys.exit(1)

print("✅ [Main] Ready — http://0.0.0.0:5000")

# ── Helper ────────────────────────────────────────────────────────────────────
def _feel_html(feel: dict) -> str:
    steps = feel.get("steps", 0)
    color = feel.get("color", "red")
    pct   = feel.get("pct", 0)
    color_map = {"green": "#00cc66", "orange": "#ffaa00", "red": "#ff4444"}
    bar_color = color_map.get(color, "#ff4444")
    blocks = ""
    for i in range(20):
        if i < steps:
            bc = bar_color
        else:
            bc = "#2a2a2a"
        blocks += (f"<div style='width:4px;height:10px;background:{bc};"
                   f"border-radius:1px;margin-right:1px;display:inline-block;'></div>")
    return (f"<div style='display:flex;flex-direction:column;align-items:center;gap:2px;'>"
            f"<div style='display:flex;align-items:center;' class='feel-blocks'>{blocks}</div>"
            f"<span class='feel-pct' style='font-size:9px;color:{bar_color};'>{pct}%</span>"
            f"</div>")

def _score_badge(score, signal) -> str:
    if score == "NA":
        return "<span class='sb sl'>NA</span>"
    bc = "sh" if score >= 65 else "sm" if score >= 40 else "sl"
    return f"<span class='sb {bc}'>{score}%</span>"

def _trend_icon(trend) -> str:
    return {"UP":"▲","DOWN":"▼","RANGING":"↔","FLAT":"—"}.get(trend, "—")

def _signal_style(signal) -> str:
    return {"STRONG BUY":"#00ff88","BUY":"#00ff88","SELL":"#ff6666",
            "STRONG SELL":"#ff4444","WAIT":"#ffcc44"}.get(signal, "#ffcc44")

def _quality_badge(quality) -> str:
    c = {"HIGH":"#00cc66","MED":"#ffaa00","LOW":"#ff4444"}.get(quality,"#888")
    return (f"<span style='font-size:9px;padding:1px 4px;border-radius:3px;"
            f"background:{c}22;border:1px solid {c}66;color:{c};'>{quality}</span>")

# ── Routes ────────────────────────────────────────────────────────────────────
@app.route('/')
def index():
    try:
        mkt_opts = "".join(f"<option value='{m}'>{m}</option>" for m in MARKET_TIMEFRAMES.keys())
        html = HTML_TEMPLATE.replace("<!-- MARKET_OPTIONS -->", mkt_opts)
        return render_template_string(html)
    except Exception as e:
        return f"<p style='color:red'>Page error: {e}</p>", 500

@app.route('/scan')
def scan():
    market   = request.args.get('market',   '')
    tf       = request.args.get('tf',       '5m')
    src      = request.args.get('src',      'real')
    iq_email = request.args.get('iq_email', '')
    iq_pwd   = request.args.get('iq_pwd',   '')
    pairs = get_pairs_by_market(market)
    if not pairs:
        return "<div class='err-msg'>No pairs for this market.</div>", 200
    try:
        results = sys_engine.scan(market, pairs, src=src, interval=tf,
                                  iq_email=iq_email, iq_pwd=iq_pwd)
        rows_html = ""
        for r in results:
            pair    = r.get("pair", "")
            score   = r.get("score", "NA")
            trend   = r.get("trend", "FLAT")
            sr_pos  = r.get("sr_position", "—")
            signal  = r.get("signal", "WAIT")
            reason  = r.get("reason", "")[:30]
            quality = r.get("quality", "LOW")
            feel    = r.get("feel", {"steps":0,"pct":0,"color":"red"})
            feel_pct = feel.get("pct", 0)
            if feel_pct == 0: status_icon = "🔴"
            elif feel_pct < 100: status_icon = "🟡"
            else: status_icon = "🟢"
            status_html = f"<span class='status-light' onclick='fillSymbol(\"{pair}\")' style='cursor:pointer; font-size:14px;' title='Click to fill missing data'>{status_icon}</span>"
            otc_badge = ""
            if src == "iqoption" or "(OTC)" in pair:
                otc_badge = ("<span style='font-size:8px;color:#ffcc44;background:rgba(255,200,0,0.07);"
                             "border:1px solid rgba(255,200,0,0.2);padding:1px 4px;border-radius:3px;"
                             "margin-left:4px;'>OTC</span>")
            if score == "NA":
                score_badge = "<span class='sb sl'>NA</span>"
            else:
                bc = "sh" if score >= 65 else "sm" if score >= 40 else "sl"
                score_badge = f"<span class='sb {bc}'>{score}%</span>"
            trend_icon = {"UP":"▲","DOWN":"▼","RANGING":"↔","FLAT":"—"}.get(trend, "—")
            signal_color = {"STRONG BUY":"#00ff88","BUY":"#00ff88","SELL":"#ff6666",
                            "STRONG SELL":"#ff4444","WAIT":"#ffcc44"}.get(signal, "#ffcc44")
            qc = {"HIGH":"#00cc66","MED":"#ffaa00","LOW":"#ff4444"}.get(quality,"#888")
            quality_badge = f"<span style='font-size:9px;padding:1px 4px;border-radius:3px;background:{qc}22;border:1px solid {qc}66;color:{qc};'>{quality}</span>"
            bar_color = {"green":"#00cc66","orange":"#ffaa00","red":"#ff4444"}.get(feel.get("color","red"), "#ff4444")
            blocks = ""
            for i in range(20):
                if i < feel.get("steps",0):
                    blocks += f"<div style='width:4px;height:10px;background:{bar_color};border-radius:1px;margin-right:1px;display:inline-block;'></div>"
                else:
                    blocks += "<div style='width:4px;height:10px;background:#2a2a2a;border-radius:1px;margin-right:1px;display:inline-block;'></div>"
            feel_html = f"""
            <div style='display:flex;flex-direction:column;align-items:center;gap:2px;'>
                <div style='display:flex;align-items:center;' class='feel-blocks'>{blocks}</div>
                <span class='feel-pct' style='font-size:9px;color:{bar_color};'>{feel_pct}%</span>
            </div>
            """
            rows_html += f"""
<tr data-pair='{pair}'>
  <td class='drag-handle' style='cursor:grab; text-align:center;'>☰</td>
  <td style='text-align:center;'>{status_html}</td>
  <td style='white-space:nowrap;'><b>{pair}</b>{otc_badge}</td>
  <td class='score-cell'>{score_badge}</td>
  <td>{trend_icon} {trend}</td>
  <td style='color:#777;font-size:10px;'>{sr_pos}</td>
  <td class='signal-cell' style='color:{signal_color};font-weight:bold;'>{signal}</td>
  <td>{quality_badge}</td>
  <td style='font-size:9px;color:#555;max-width:120px;overflow:hidden;'>{reason}</td>
  <td class='feel-cell' style='min-width:100px;'>{feel_html}</td>
  <td><button class='go-btn' onclick="doGO('{market}','{pair}','{tf}')">→</button></td>
</tr>"""
        return f"""
<table id='stbl'>
  <thead>
    <tr>
      <th style='width:20px;'></th>
      <th>STATUS</th><th>PAIR</th><th>SCORE</th><th>TREND</th>
      <th>S/R</th><th>SIGNAL</th><th>QUAL</th><th>REASON</th><th>FEEL ({tf})</th><th>GO</th>
    </tr>
  </thead>
  <tbody>{rows_html}</tbody>
</table>
<div style='color:#444;font-size:10px;padding:6px 2px;'>
  {len(pairs)} pairs · 65%+ strong · 40-65% mid · &lt;40% weak · auto-refresh 0.5s · Click 🔴/🟡 to fetch missing data · Drag ☰ to reorder rows
</div>"""
    except Exception as e:
        print(f"❌ [Main] Scan error: {e}")
        return f"<p class='err-msg'>Scan error: {e}</p>", 500

@app.route('/go')
def go():
    market = request.args.get('market', '')
    pair   = request.args.get('pair',   '')
    tf     = request.args.get('tf',     '5m')
    clean  = pair.replace(" (OTC)", "").strip()
    try:
        result = sys_engine.go(clean, market, interval=tf)
        if "error" in result:
            return f"<div class='err-msg'>{result['error']}</div>"
        return _render_a_result(result, pair, market, tf)
    except Exception as e:
        print(f"❌ [Main] GO error: {e}")
        return f"<div class='err-msg'>GO error: {e}</div>", 500

@app.route('/deep')
def deep():
    market   = request.args.get('market', '')
    pair     = request.args.get('pair',   '')
    tf       = request.args.get('tf',     '5m')
    a_result = request.args.get('a_result', '{}')
    clean    = pair.replace(" (OTC)", "").strip()
    try:
        a_data = json.loads(a_result)
        result = sys_engine.deep(clean, market, a_data, interval=tf)
        if "error" in result:
            return f"<div class='err-msg'>{result['error']}</div>"
        return _render_d_result(result)
    except Exception as e:
        print(f"❌ [Main] Deep error: {e}")
        return f"<div class='err-msg'>Deep error: {e}</div>", 500

@app.route('/scores')
def scores():
    try:
        return jsonify(sys_engine.get_current_scores())
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/refresh')
def refresh():
    try:
        new_scores = sys_engine.refresh_scores()
        return jsonify(new_scores)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/calls')
def calls():
    try:
        stats = sys_engine.get_call_stats()
        total = sum(stats.values())
        html  = (f"<div style='color:#888;font-size:11px;padding:4px 8px;'>"
                 f"REST calls — " + " | ".join(f"{k}: {v}" for k,v in stats.items())
                 + f" | <b>Total: {total}</b></div>")
        return html
    except Exception as e:
        return f"<span style='color:red'>{e}</span>", 500

@app.route('/fill')
def fill():
    symbol = request.args.get('symbol', '')
    if not symbol:
        return jsonify({"error": "No symbol"}), 400
    try:
        sys_engine.fill_single(symbol)
        return jsonify({"status": "started", "symbol": symbol})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/fill_status')
def fill_status():
    symbol = request.args.get('symbol', '')
    if not symbol:
        return jsonify({"error": "No symbol"}), 400
    status = sys_engine.get_fill_status(symbol)
    return jsonify(status)

@app.route('/check_file')
def check_file():
    symbol = request.args.get('symbol', '')
    file_type = request.args.get('type', '')
    if not symbol or not file_type:
        return jsonify({"exists": False}), 400
    clean = symbol.upper().replace("/", "").replace(" (OTC)", "")
    base_dir = os.path.join(os.path.dirname(__file__), "market_data", "binance", "symbols")
    if file_type == 'candles':
        filename = f"{clean.lower()}.tsv"
    else:
        filename = f"{clean.lower()}_{file_type}.tsv"
    filepath = os.path.join(base_dir, filename)
    exists = os.path.exists(filepath)
    return jsonify({"exists": exists})

@app.route('/data/<symbol>/<data_type>')
def view_data(symbol, data_type):
    clean = symbol.upper().replace("/", "").replace(" (OTC)", "")
    base_dir = os.path.join(os.path.dirname(__file__), "market_data", "binance", "symbols")
    if data_type == 'candles':
        filename = f"{clean.lower()}.tsv"
    else:
        filename = f"{clean.lower()}_{data_type}.tsv"
    filepath = os.path.join(base_dir, filename)
    if not os.path.exists(filepath):
        return f"File not found: {filename}", 404
    return send_file(filepath, as_attachment=False, mimetype='text/plain')

@app.route('/file_info')
def file_info():
    symbol = request.args.get('symbol', '')
    if not symbol:
        return jsonify({"error": "No symbol"}), 400
    clean = symbol.upper().replace("/", "").replace(" (OTC)", "")
    base_dir = os.path.join(os.path.dirname(__file__), "market_data", "binance", "symbols")
    
    def count_rows(filename):
        filepath = os.path.join(base_dir, filename)
        if not os.path.exists(filepath):
            return 0
        try:
            with open(filepath, 'r') as f:
                lines = f.readlines()
            return max(0, len(lines) - 1)  # subtract header
        except:
            return 0
    
    # Existing file types
    candles_rows = count_rows(f"{clean.lower()}.tsv")
    cvd_rows = count_rows(f"{clean.lower()}_cvd.tsv")
    depth_rows = count_rows(f"{clean.lower()}_depth.tsv")
    derivative_rows = count_rows(f"{clean.lower()}_derivative.tsv")
    correlation_rows = count_rows(f"{clean.lower()}_correlation.tsv")
    liquidations_rows = count_rows(f"{clean.lower()}_liquidations.tsv")
    
    # New file types (added for UI buttons)
    macro_rows = count_rows(f"{clean.lower()}_macro.tsv")
    sessions_rows = count_rows(f"{clean.lower()}_sessions.tsv")
    sentiment_rows = count_rows(f"{clean.lower()}_sentiment.tsv")
    volProfile_rows = count_rows(f"{clean.lower()}_volProfile.tsv")
    mstructure_rows = count_rows(f"{clean.lower()}_mstructure.tsv")
    onchain_rows = count_rows(f"{clean.lower()}_onchain.tsv")
    tick_rows = count_rows(f"{clean.lower()}_tick.tsv")
    
    return jsonify({
        "candles": candles_rows,
        "cvd": cvd_rows,
        "depth": depth_rows,
        "derivative": derivative_rows,
        "correlation": correlation_rows,
        "liquidations": liquidations_rows,
        "macro": macro_rows,
        "sessions": sessions_rows,
        "sentiment": sentiment_rows,
        "volProfile": volProfile_rows,
        "mstructure": mstructure_rows,
        "onchain": onchain_rows,
        "tick": tick_rows
    })

@app.route('/mem')
def mem():
    try:
        gc.collect()
        try:
            import ctypes
            ctypes.CDLL("libc.so.6").malloc_trim(0)
        except Exception:
            pass
        return jsonify({"status": "ok"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/favicon.ico')
def favicon():
    return '', 204

# ── Result renderers ──────────────────────────────────────────────────────────
def _render_a_result(r: dict, pair: str, market: str, tf: str) -> str:
    a_score  = r.get("a_score",  "NA")
    a_signal = r.get("a_signal", "WAIT")
    reason   = r.get("reason",   "—")
    sl       = r.get("sl",  0)
    tp       = r.get("tp",  0)
    forecast = r.get("forecast", {})
    quality  = r.get("quality",  "LOW")
    regime   = r.get("regime",   "—")
    z_score  = r.get("z_score",  "—")
    req_deep = r.get("requires_deep", False)
    colors = {"STRONG BUY":"#00ff88","BUY":"#44cc88","STRONG SELL":"#ff4444","SELL":"#ff8866","WAIT":"#ffcc44"}
    col = colors.get(a_signal, "#ffcc44")
    a_json = json.dumps({"a_score": a_score, "a_signal": a_signal,
                         "min_deep_score": r.get("min_deep_score", 60)}).replace('"', '&quot;')
    forecast_html = ""
    if forecast:
        up, down, flat = forecast.get("up",50), forecast.get("down",50), forecast.get("flat",0)
        forecast_html = f"""
<div style='padding:8px 12px;background:rgba(255,255,255,0.02);border-radius:6px;border:1px solid rgba(255,255,255,0.06);margin-top:8px;font-size:11px;'>
  <span style='color:#888;margin-right:8px;'>Next candle:</span>
  <span style='color:#44cc88;'>▲ {up}%</span> &nbsp;
  <span style='color:#ff8866;'>▼ {down}%</span> &nbsp;
  <span style='color:#888;'>→ {flat}%</span>
</div>"""
    continue_btn = ""
    if req_deep and a_signal not in ["WAIT"]:
        continue_btn = f"""
<div style='margin-top:10px;display:flex;gap:10px;'>
  <button class='go-btn' style='background:#1a4a2a;border-color:#00cc66;' onclick="doDeep('{market}','{pair}','{tf}','{a_json}')">✓ Continue (Deep Analysis)</button>
  <button class='go-btn' style='background:#4a1a1a;border-color:#cc3333;' onclick="closePanel()">✗ Cancel</button>
</div>"""
    elif a_signal not in ["WAIT"]:
        continue_btn = f"<div style='margin-top:10px;display:flex;gap:10px;'><button class='go-btn' style='background:#1a3a4a;border-color:#3399cc;' onclick='closePanel()'>✓ Done</button><button class='go-btn' style='background:#4a1a1a;border-color:#cc3333;' onclick='closePanel()'>✗ Cancel</button></div>"
    return f"""
<div style='background:rgba(0,0,0,0.3);border:1px solid {col}44;border-radius:10px;padding:14px 18px;'>
  <div style='display:flex;align-items:center;gap:12px;margin-bottom:10px;'>
    <span style='color:{col};font-size:17px;font-weight:bold;'>{a_signal}</span>
    <span style='color:{col};font-size:15px;'>{a_score}%</span>
    {_quality_badge(quality)}
    <span style='color:#555;font-size:10px;margin-left:auto;'>Z:{z_score} · Regime:{regime}</span>
  </div>
  <div style='color:#777;font-size:11px;margin-bottom:6px;'>Reason: {reason}</div>
  <div style='color:#555;font-size:11px;'>SL: <span style='color:#ff8866;'>{round(sl,6)}</span> &nbsp;|&nbsp; TP: <span style='color:#44cc88;'>{round(tp,6)}</span></div>
  {forecast_html}
  {continue_btn}
</div>"""

def _render_d_result(r: dict) -> str:
    d_score   = r.get("d_score",   "NA")
    d_signal  = r.get("d_signal",  "WAIT")
    confirmed = r.get("confirmed", False)
    reason    = r.get("reason",    "—")
    col = "#00ff88" if confirmed and "BUY" in d_signal else "#ff4444" if confirmed and "SELL" in d_signal else "#ffcc44"
    verdict = "✅ CONFIRMED" if confirmed else "⛔ NOT CONFIRMED"
    return f"""
<div style='background:rgba(0,0,0,0.3);border:1px solid {col}44;border-radius:10px;padding:14px 18px;margin-top:10px;'>
  <div style='color:{col};font-size:15px;font-weight:bold;margin-bottom:8px;'>{verdict} — D Score: {d_score}%</div>
  <div style='color:#777;font-size:11px;'>Signal: {d_signal}</div>
  <div style='color:#555;font-size:10px;margin-top:4px;'>{reason}</div>
  <div style='margin-top:10px;'><button class='go-btn' style='background:#2a2a2a;' onclick='closePanel()'>Close</button></div>
</div>"""

if __name__ == "__main__":
    print("[Main] Starting Flask server on port 5000...")
    app.run(host="0.0.0.0", port=5000, debug=False, threaded=True)