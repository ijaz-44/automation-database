import sys, json, os, gc, time, sqlite3, traceback
from flask import Flask, request, jsonify, render_template_string, send_file
from sys_data import SysData
from pairs import get_pairs_by_market
from config import HTML_TEMPLATE, MARKET_TIMEFRAMES

# Import brain prediction module
try:
    from brain import predict as brain_predict
    BRAIN_AVAILABLE = True
except ImportError:
    BRAIN_AVAILABLE = False
    print("[Main] Brain module not found – AI predictions disabled")

import logging
log = logging.getLogger('werkzeug')
log.setLevel(logging.ERROR)

print("[Main] Starting...")
app = Flask(__name__)

try:
    sys_engine = SysData()
    print("✅ [Main] SysData initialized")
except Exception as e:
    print(f"❌ [Main] SysData init error: {e}")
    sys.exit(1)

print("✅ [Main] Ready — http://0.0.0.0:5000")

# ── Helper functions (unchanged) ──────────────────────────────────────────
def _feel_html(feel: dict) -> str:
    return '<div class="feel-cell-placeholder" style="min-width:100px;"></div>'

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

# ---------- Helper: check if all X modules DB files exist ----------
def _all_modules_complete(symbol: str) -> bool:
    clean = symbol.upper().replace("/", "").replace(" (OTC)", "")
    base_dir = os.path.join(os.path.dirname(__file__), "market_data", "binance", "symbols")
    
    db_path = os.path.join(base_dir, f"{clean.lower()}.db")
    if not os.path.exists(db_path):
        return False
    try:
        conn = sqlite3.connect(db_path)
        cur = conn.execute("SELECT COUNT(*) FROM candles_summary")
        count = cur.fetchone()[0]
        conn.close()
        if count < 200:
            return False
    except:
        return False
    
    vol_db = os.path.join(base_dir, f"{clean.lower()}_volProfile.db")
    if not os.path.exists(vol_db):
        return False
    
    return True
# ----------------------------------------------------------------

# ── Routes ────────────────────────────────────────────────────────────────
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
            score_val = r.get("score", "NA")
            gatekeeper = r.get("gatekeeper", {})
            preflight = gatekeeper.get("preflight_score", 0)
            if score_val == "NA" and preflight > 0:
                score_val = preflight
            trend   = r.get("trend", "FLAT")
            sr_pos  = r.get("sr_position", "—")
            signal  = r.get("signal", "WAIT")
            reason  = r.get("reason", "")[:30]
            quality = r.get("quality", "LOW")
            feel    = r.get("feel", {"steps":0,"pct":0})
            feel_pct = feel.get("pct", 0)

            all_done = _all_modules_complete(pair)
            if all_done:
                status_icon = "🟢"
            elif feel_pct > 0:
                status_icon = "🟡"
            else:
                status_icon = "🔴"

            status_html = f"<span class='status-light' onclick='fillSymbol(\"{pair}\")' style='cursor:pointer; font-size:14px;' title='Click to fetch missing data'>{status_icon}</span>"
            otc_badge = ""
            if src == "iqoption" or "(OTC)" in pair:
                otc_badge = ("<span style='font-size:8px;color:#ffcc44;background:rgba(255,200,0,0.07);"
                             "border:1px solid rgba(255,200,0,0.2);padding:1px 4px;border-radius:3px;"
                             "margin-left:4px;'>OTC</span>")
            if score_val == "NA":
                score_badge = "<span class='sb sl'>NA</span>"
            else:
                bc = "sh" if score_val >= 65 else "sm" if score_val >= 40 else "sl"
                score_badge = f"<span class='sb {bc}'>{score_val}%</span>"
            trend_icon = {"UP":"▲","DOWN":"▼","RANGING":"↔","FLAT":"—"}.get(trend, "—")
            signal_color = {"STRONG BUY":"#00ff88","BUY":"#00ff88","SELL":"#ff6666",
                            "STRONG SELL":"#ff4444","WAIT":"#ffcc44"}.get(signal, "#ffcc44")
            qc = {"HIGH":"#00cc66","MED":"#ffaa00","LOW":"#ff4444"}.get(quality,"#888")
            quality_badge = f"<span style='font-size:9px;padding:1px 4px;border-radius:3px;background:{qc}22;border:1px solid {qc}66;color:{qc};'>{quality}</span>"
            feel_html = _feel_html(feel)
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
  <td><button class='go-btn' onclick="doPredict('{market}','{pair}','{tf}', event)">→</button></td>
</tr>"""
        # JavaScript for expandable row and prediction
        js = """
<script>
function doPredict(market, pair, tf, event) {
    let btn = event.target;
    let row = btn.closest('tr');
    // Remove existing expanded row if any
    let nextRow = row.nextElementSibling;
    if (nextRow && nextRow.classList && nextRow.classList.contains('expand-row')) {
        nextRow.remove();
        return;
    }
    // Create new row for expansion
    let expandRow = document.createElement('tr');
    expandRow.classList.add('expand-row');
    let td = document.createElement('td');
    td.colSpan = row.cells.length;
    td.style.padding = '10px';
    td.style.background = '#1a1a2e';
    td.style.borderTop = '1px solid #333';
    td.innerHTML = '<div class="prediction-loader">Loading prediction...</div>';
    expandRow.appendChild(td);
    row.insertAdjacentElement('afterend', expandRow);
    // Fetch prediction
    fetch(`/predict/${encodeURIComponent(pair)}`)
        .then(response => response.json())
        .then(data => {
            if (data.error) {
                td.innerHTML = `<div style="color:red;">Error: ${data.error}</div><button class="ok-btn" onclick="this.closest('.expand-row').remove()">Close</button>`;
                return;
            }
            let html = `<div style="display:flex; justify-content:space-around; text-align:center; font-size:14px;">`;
            for (let tf of ['30m', '45m', '1h']) {
                let dir = data[tf].direction;
                let conf = data[tf].confidence;
                let color = dir === 'UP' ? '#00ff88' : (dir === 'DOWN' ? '#ff4444' : '#ffcc44');
                html += `<div style="flex:1;">
                            <div style="font-size:12px; color:#aaa;">${tf}</div>
                            <div style="font-size:20px; font-weight:bold; color:${color};">${dir}</div>
                            <div style="font-size:12px;">${conf}% conf</div>
                         </div>`;
            }
            html += `</div><div style="text-align:center; margin-top:8px;"><button class="ok-btn" onclick="this.closest('.expand-row').remove()">Close</button></div>`;
            td.innerHTML = html;
        })
        .catch(err => {
            td.innerHTML = `<div style="color:red;">Error: ${err.message}</div><button class="ok-btn" onclick="this.closest('.expand-row').remove()">Close</button>`;
        });
}
</script>
"""
        return f"""
<table id='stbl'>
  <thead>
    <tr><th style='width:20px;'></th><th>STATUS</th><th>PAIR</th><th>SCORE</th><th>TREND</th>
      <th>S/R</th><th>SIGNAL</th><th>QUAL</th><th>REASON</th><th>FEEL ({tf})</th><th>GO</th></tr>
  </thead>
  <tbody>{rows_html}</tbody>
</table>
<div style='color:#444;font-size:10px;padding:6px 2px;'>
  {len(pairs)} pairs · 65%+ strong · 40-65% mid · &lt;40% weak · auto-refresh 0.5s · 🟢 = all data ready · 🟡 = live candles (WS) but some modules missing · 🔴 = no live candles · Click 🔴/🟡 to fetch missing data · Drag ☰ to reorder rows
</div>
{js}"""
    except Exception as e:
        print(f"❌ [Main] Scan error: {e}")
        return f"<p class='err-msg'>Scan error: {e}</p>", 500

@app.route('/predict/<symbol>')
def predict_route(symbol):
    """Return JSON prediction for the symbol using brain module."""
    if not BRAIN_AVAILABLE:
        return jsonify({"error": "Brain module not available"}), 500
    try:
        clean = symbol.upper().replace("/", "").replace(" (OTC)", "")
        pred = brain_predict(clean)
        return jsonify(pred)
    except Exception as e:
        print(f"[Predict] Error for {symbol}: {e}")
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

# Keep old /go route unchanged (for compatibility)
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

# ==================== HEADER CALLS ROUTE ====================
@app.route('/calls')
def calls():
    try:
        rest_stats = sys_engine.get_call_stats()
        try:
            from Groups.group_z.Z01_news import get_api_stats
            news_stats = get_api_stats()
        except Exception as e:
            news_stats = {}
            print(f"[Main] Could not import news API stats: {e}")
        all_stats = {**rest_stats, **news_stats}
        total = sum(all_stats.values())
        items = list(all_stats.items())
        mid = (len(items) + 1) // 2
        left_items = items[:mid]
        right_items = items[mid:]
        rows_html = ""
        for i in range(max(len(left_items), len(right_items))):
            left = left_items[i] if i < len(left_items) else None
            right = right_items[i] if i < len(right_items) else None
            rows_html += "<tr>"
            if left:
                rows_html += f"<td style='padding:1px 6px; font-size:10px;'>{left[0]}</td><td style='padding:1px 6px; text-align:right; font-size:10px;'>{left[1]}</td>"
            else:
                rows_html += "<td style='padding:1px 6px;'></td><td style='padding:1px 6px;'></td>"
            if right:
                rows_html += f"<td style='padding:1px 6px; font-size:10px;'>{right[0]}</td><td style='padding:1px 6px; text-align:right; font-size:10px;'>{right[1]}</td>"
            else:
                rows_html += "<td style='padding:1px 6px;'></td><td style='padding:1px 6px;'></td>"
            rows_html += "</tr>"
        rows_html += f"<tr style='border-top:1px solid rgba(180,160,255,0.3);'><td colspan='2' style='padding:1px 6px; font-size:10px;'><b>Total</b></td><td colspan='2' style='padding:1px 6px; text-align:right; font-size:10px;'><b>{total}</b></td></tr>"
        html = f"""
        <div style='color:#ccc; font-size:10px; padding:2px 6px;'>
            <table style='border-collapse:collapse; background:transparent; border-radius:4px; width:auto;'>
                {rows_html}
              </table>
        </div>
        """
        return html
    except Exception as e:
        return f"<span style='color:red'>{e}</span>", 500

@app.route('/fill')
def fill():
    symbol = request.args.get('symbol', '')
    if not symbol:
        return jsonify({"error": "No symbol"}), 400
    try:
        sys_engine.call_single(symbol)
        return jsonify({"status": "started", "symbol": symbol})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/fill_status')
def fill_status():
    symbol = request.args.get('symbol', '')
    if not symbol:
        return jsonify({"error": "No symbol"}), 400
    clean = symbol.upper().replace("/", "").replace(" (OTC)", "")
    status = sys_engine.get_fill_status(clean)
    return jsonify(status)

# ==================== MASTER ANALYSIS ROUTE ====================
@app.route('/master')
def master():
    symbol = request.args.get('symbol', '')
    if not symbol:
        return jsonify({"error": "No symbol"}), 400
    try:
        from Groups.group_x.X50_master import generate_master
        content = generate_master(symbol)
        if content.startswith("Error"):
            return jsonify({"error": content}), 500
        return jsonify({"analysis": content})
    except ImportError:
        return jsonify({"error": "Master module not available"}), 500
    except Exception as e:
        print(f"[Master] Error: {e}")
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500
# ========================================================================

@app.route('/check_file')
def check_file():
    symbol = request.args.get('symbol', '')
    file_type = request.args.get('type', '')
    if not symbol or not file_type:
        return jsonify({"exists": False}), 400
    clean = symbol.upper().replace("/", "").replace(" (OTC)", "")
    base_dir = os.path.join(os.path.dirname(__file__), "market_data", "binance", "symbols")
    
    if file_type == 'candles':
        db_path = os.path.join(base_dir, f"{clean.lower()}.db")
    elif file_type == 'volprofile':
        db_path = os.path.join(base_dir, f"{clean.lower()}_volProfile.db")
    elif file_type == 'depth':
        db_path = os.path.join(base_dir, f"{clean.lower()}_depth.db")
    else:
        db_path = os.path.join(base_dir, f"{clean.lower()}_{file_type}.db")
    
    exists = os.path.exists(db_path)
    modified = None
    if exists:
        modified = int(os.path.getmtime(db_path) * 1000)
    return jsonify({"exists": exists, "modified": modified})

@app.route('/data/<symbol>/<data_type>')
def view_data(symbol, data_type):
    clean = symbol.upper().replace("/", "").replace(" (OTC)", "")
    base_dir = os.path.join(os.path.dirname(__file__), "market_data", "binance", "symbols")
    
    if data_type == 'candles':
        db_path = os.path.join(base_dir, f"{clean.lower()}.db")
    elif data_type == 'volprofile':
        db_path = os.path.join(base_dir, f"{clean.lower()}_volProfile.db")
    elif data_type == 'depth':
        db_path = os.path.join(base_dir, f"{clean.lower()}_depth.db")
    elif data_type == 'derivative':
        db_path = os.path.join(base_dir, f"{clean.lower()}_derivative.db")
    elif data_type == 'liquidations':
        db_path = os.path.join(base_dir, f"{clean.lower()}_liquidations.db")
    elif data_type == 'mstructure':
        db_path = os.path.join(base_dir, f"{clean.lower()}_mstructure.db")
    else:
        db_path = os.path.join(base_dir, f"{clean.lower()}_{data_type}.db")
    
    if not os.path.exists(db_path):
        return f"File not found: {os.path.basename(db_path)}", 404
    
    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        
        if data_type == 'candles':
            cur = conn.execute("SELECT * FROM candles_summary ORDER BY timeframe, timestamp_ms")
            lines = [json.dumps(dict(row)) for row in cur]
        elif data_type == 'volprofile':
            lines = []
            tables = [
                'developing_poc', 'daily_profiles', 'untested_pocs', 'prediction_context',
                'intraday_profiles', 'developing_vah_val', 'shape_interpretation', 'multi_tf_confluence'
            ]
            for table in tables:
                try:
                    cur = conn.execute(f"SELECT * FROM {table}")
                    for row in cur:
                        lines.append(json.dumps(dict(row)))
                except sqlite3.OperationalError:
                    pass
            if not lines:
                lines = ['{"info": "No volProfile data available"}']
        elif data_type == 'depth':
            lines = []
            cur = conn.execute("SELECT * FROM depth_summary ORDER BY run_id DESC LIMIT 1")
            for row in cur:
                lines.append(json.dumps(dict(row)))
            cur = conn.execute("SELECT side, target_pct, price FROM depth_tail_percentiles ORDER BY side, target_pct")
            for row in cur:
                lines.append(json.dumps(dict(row)))
            cur = conn.execute("SELECT side, levels_json FROM depth_top")
            for row in cur:
                lines.append(json.dumps({"side": row['side'], "top25": json.loads(row['levels_json'])}))
            cur = conn.execute("SELECT side, min_price, max_price, avg_price, total_volume, avg_volume, count, vwap, std_dev FROM depth_tail_stats")
            for row in cur:
                lines.append(json.dumps(dict(row)))
            cur = conn.execute("SELECT * FROM meta")
            for row in cur:
                lines.append(json.dumps(dict(row)))
            if not lines:
                lines = ['{"info": "No depth data available"}']
        elif data_type == 'derivative':
            lines = []
            cur = conn.execute("SELECT * FROM derivative_summary ORDER BY run_id DESC LIMIT 1")
            for row in cur:
                lines.append(json.dumps(dict(row)))
            cur = conn.execute("SELECT timestamp, oi_value FROM derivative_oi_history ORDER BY timestamp")
            for row in cur:
                lines.append(json.dumps(dict(row)))
            cur = conn.execute("SELECT timestamp, long_short_ratio, long_account, short_account FROM derivative_ls_history ORDER BY timestamp")
            for row in cur:
                lines.append(json.dumps(dict(row)))
            cur = conn.execute("SELECT timestamp, funding_rate FROM derivative_funding_history ORDER BY timestamp")
            for row in cur:
                lines.append(json.dumps(dict(row)))
            cur = conn.execute("SELECT price, volume FROM derivative_liquidation_levels ORDER BY seq")
            for row in cur:
                lines.append(json.dumps(dict(row)))
            cur = conn.execute("SELECT * FROM meta")
            for row in cur:
                lines.append(json.dumps(dict(row)))
            if not lines:
                lines = ['{"info": "No derivative data available"}']
        elif data_type == 'liquidations':
            lines = []
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = cursor.fetchall()
            for table in tables:
                table_name = table[0]
                cur = conn.execute(f"SELECT * FROM {table_name}")
                for row in cur:
                    lines.append(json.dumps(dict(row)))
            if not lines:
                lines = ['{"info": "No liquidations data available"}']
        elif data_type == 'mstructure':
            lines = []
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = cursor.fetchall()
            for table in tables:
                table_name = table[0]
                cur = conn.execute(f"SELECT * FROM {table_name}")
                for row in cur:
                    lines.append(json.dumps(dict(row)))
            if not lines:
                lines = ['{"info": "No market structure data available"}']
        else:
            try:
                cur = conn.execute("SELECT * FROM main")
                lines = [json.dumps(dict(row)) for row in cur]
            except sqlite3.OperationalError:
                lines = []
        
        conn.close()
        content = "\n".join(lines)
        return content, 200, {'Content-Type': 'text/plain; charset=utf-8'}
    except Exception as e:
        print(f"[ERROR] /data/{symbol}/{data_type}: {e}")
        import traceback
        traceback.print_exc()
        return f"Error reading DB: {e}", 500

@app.route('/file_info')
def file_info():
    symbol = request.args.get('symbol', '')
    if not symbol:
        return jsonify({"error": "No symbol"}), 400
    clean = symbol.upper().replace("/", "").replace(" (OTC)", "")
    base_dir = os.path.join(os.path.dirname(__file__), "market_data", "binance", "symbols")
    
    def get_db_row_count(db_name, table='main'):
        if db_name == 'candles':
            db_path = os.path.join(base_dir, f"{clean.lower()}.db")
        else:
            db_path = os.path.join(base_dir, f"{clean.lower()}_{db_name}.db")
        if not os.path.exists(db_path):
            return 0
        try:
            conn = sqlite3.connect(db_path)
            if db_name == 'candles':
                cur = conn.execute("SELECT COUNT(*) FROM candles_summary")
            elif db_name == 'volprofile':
                cur = conn.execute("SELECT COUNT(*) FROM daily_profiles")
            elif db_name == 'depth':
                cur = conn.execute("SELECT COUNT(*) FROM depth_summary")
            else:
                cur = conn.execute(f"SELECT COUNT(*) FROM {table}")
            count = cur.fetchone()[0]
            conn.close()
            return count
        except:
            return 0
    
    return jsonify({
        "candles": get_db_row_count('candles'),
        "volProfile": get_db_row_count('volprofile'),
        "depth": get_db_row_count('depth'),
        "cvd": get_db_row_count('cvd'),
        "derivative": get_db_row_count('derivative'),
        "correlation": get_db_row_count('correlation'),
        "liquidations": get_db_row_count('liquidations'),
        "macro": get_db_row_count('macro'),
        "sessions": get_db_row_count('sessions'),
        "sentiment": get_db_row_count('sentiment'),
        "mstructure": get_db_row_count('mstructure'),
        "onchain": get_db_row_count('onchain'),
        "tick": get_db_row_count('tick')
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

# ── Result renderers (unchanged) ──────────────────────────────────────────
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