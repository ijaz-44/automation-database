# main.py — Frontend only. Sirf dikhata hai, logic nahi.
import sys, json, os, gc
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from flask    import Flask, request, jsonify
from sys_data import SysData
from data_manager  import cache_info

app      = Flask(__name__)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

with open(os.path.join(BASE_DIR,"config.json")) as f:
    _CFG = json.load(f)

MARKET_PAIRS = _CFG.get("pairs",{})
# Remove 1min and 2min scalping markets
if "Scalp (1min)" in MARKET_PAIRS:
    del MARKET_PAIRS["Scalp (1min)"]
if "Scalp (2min)" in MARKET_PAIRS:
    del MARKET_PAIRS["Scalp (2min)"]

ALL_PAIRS    = list(set(
    p.replace(" (OTC)","").strip()
    for v in MARKET_PAIRS.values() for p in v
))

print("[App] Starting...")
sys_engine = SysData()
sys_engine.warm_up(ALL_PAIRS)
print("[App] Ready!")

# ══════════════════════════════════════════════════════
# HTML
# ══════════════════════════════════════════════════════

def build_page():
    ci   = cache_info()
    cs   = str(ci.get("valid",0))+"/"+str(ci.get("total",0))

    mkt_opts = "".join(
        "<option value='"+m+"'>"+m+"</option>"
        for m in MARKET_PAIRS if m not in ["Scalp (1min)", "Scalp (2min)"]
    )

    return ("""<!DOCTYPE html><html><head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Trading Scanner</title>
<style>
@import url('https://fonts.googleapis.com/css2?family=Share+Tech+Mono&display=swap');
*{box-sizing:border-box;margin:0;padding:0;}
body{background:#060a12;color:#c8d8e8;font-family:'Share Tech Mono','Courier New',monospace;min-height:100vh;}
#topbar{position:sticky;top:0;z-index:100;background:rgba(6,10,18,0.96);backdrop-filter:blur(14px);border-bottom:1px solid rgba(0,255,136,0.14);padding:9px 16px;display:flex;align-items:center;gap:8px;flex-wrap:wrap;}
#topbar h1{color:#00ff88;font-size:12px;letter-spacing:3px;white-space:nowrap;margin-right:4px;}
.gsel{appearance:none;padding:6px 10px;font-family:inherit;font-size:11px;color:#e0f0ff;background:rgba(255,255,255,0.07);border:1px solid rgba(0,200,255,0.2);border-radius:8px;cursor:pointer;outline:none;}
.gsel:hover,.gsel:focus{border-color:rgba(0,255,136,0.45);}
.gsel option{background:#0d1520;color:#e0f0ff;}
.tf-group{display:flex;gap:4px;}
.tf-btn{padding:5px 9px;font-family:inherit;font-size:10px;color:#778899;cursor:pointer;border:1px solid rgba(255,255,255,0.1);border-radius:6px;background:rgba(255,255,255,0.04);transition:all 0.13s;}
.tf-btn.on{color:#00ff88;border-color:rgba(0,255,136,0.45);background:rgba(0,255,136,0.07);}
.tf-btn:hover{border-color:rgba(0,200,255,0.4);color:#88ffcc;}
.scan-btn{padding:6px 16px;font-family:inherit;font-size:11px;font-weight:bold;color:#fff;cursor:pointer;border:none;border-radius:8px;background:linear-gradient(135deg,rgba(0,200,120,0.32),rgba(0,120,255,0.26));border:1px solid rgba(255,255,255,0.14);transition:all 0.16s;}
.scan-btn:hover{transform:translateY(-1px);}
#rbadge{font-size:10px;color:#88ffaa;padding:2px 8px;border:1px solid rgba(0,255,136,0.18);border-radius:10px;}
#cbadge{font-size:10px;color:#88aaff;}
#mbtn{padding:4px 9px;font-family:inherit;font-size:10px;color:#ffcc44;cursor:pointer;border:1px solid rgba(255,200,0,0.22);border-radius:6px;background:rgba(255,200,0,0.05);}
#mbtn:hover{background:rgba(255,200,0,0.14);}
#page{padding:12px 16px 50px;}
#stbl{width:100%;border-collapse:collapse;font-size:11px;}
#stbl th{background:rgba(0,50,100,0.4);color:#00ff88;padding:8px 11px;text-align:left;border-bottom:2px solid rgba(0,255,136,0.15);white-space:nowrap;position:sticky;top:46px;font-size:10px;letter-spacing:1px;}
#stbl td{padding:7px 11px;border-bottom:1px solid rgba(255,255,255,0.04);white-space:nowrap;vertical-align:top;}
.sb{display:inline-block;padding:2px 8px;border-radius:12px;font-size:11px;font-weight:bold;min-width:46px;text-align:center;transition:background 0.3s,color 0.3s;}
.sh{background:rgba(0,255,136,0.14);color:#00ff88;border:1px solid rgba(0,255,136,0.28);}
.sm{background:rgba(255,200,0,0.1);color:#ffcc44;border:1px solid rgba(255,200,0,0.26);}
.sl{background:rgba(255,60,60,0.1);color:#ff6666;border:1px solid rgba(255,60,60,0.26);}
.go-btn{padding:4px 10px;font-family:inherit;font-size:10px;font-weight:bold;color:#fff;cursor:pointer;border:1px solid rgba(255,255,255,0.15);border-radius:5px;background:rgba(0,200,120,0.18);transition:all 0.12s;}
.go-btn:hover{background:rgba(0,200,120,0.32);}
.go-btn.on{background:rgba(0,200,255,0.18);border-color:rgba(0,200,255,0.38);}
.exp-row td{padding:0 0 0 14px;background:rgba(0,80,160,0.05);border-bottom:1px solid rgba(0,200,255,0.07);}
.exp-inner{padding:10px 12px;border-left:2px solid rgba(0,200,255,0.22);}
.det-row td{padding:0 0 0 28px;background:rgba(80,0,160,0.04);border-bottom:1px solid rgba(180,0,255,0.07);}
.det-inner{padding:10px 12px;border-left:2px solid rgba(180,0,255,0.22);}
.section{margin-bottom:12px;}
.section-title{color:#4da8ff;font-size:10px;letter-spacing:1.4px;text-transform:uppercase;margin-bottom:6px;border-left:2px solid rgba(0,180,255,0.28);padding-left:6px;}
table{width:100%;border-collapse:collapse;font-size:11px;}
th{background:rgba(0,70,140,0.16);color:#00ff88;padding:5px 8px;text-align:left;border-bottom:1px solid rgba(0,255,136,0.1);}
td{padding:4px 8px;border-bottom:1px solid rgba(255,255,255,0.03);}
td.up{color:#00ff88;}td.down{color:#ff6666;}td.warn{color:#ffcc44;}
.err-msg{color:#ff8888;font-size:11px;padding:6px 10px;background:rgba(255,60,60,0.06);border-radius:6px;border:1px solid rgba(255,60,60,0.16);}
.btn-group{display:flex;gap:6px;margin-top:8px;}
.btn-group button{padding:4px 10px;font-family:inherit;font-size:9px;color:#fff;cursor:pointer;border:1px solid rgba(255,255,255,0.2);border-radius:4px;transition:all 0.12s;}
.btn-cont{background:rgba(0,200,255,0.15);border-color:rgba(0,200,255,0.3);}
.btn-cont:hover{background:rgba(0,200,255,0.25);}
.btn-canc{background:rgba(255,100,100,0.15);border-color:rgba(255,100,100,0.3);}
.btn-canc:hover{background:rgba(255,100,100,0.25);}
#placeholder{text-align:center;padding:60px;color:rgba(200,220,240,0.15);font-size:12px;letter-spacing:2px;}
#scan-loading{display:none;padding:50px;text-align:center;color:#4da8ff;letter-spacing:2px;}
.changed{animation:chg 0.45s ease;}
@keyframes chg{0%{opacity:0.3;transform:scale(0.94)}100%{opacity:1;transform:scale(1)}}
</style>
</head><body>
<div id="topbar">
  <h1>SCANNER</h1>
  <select class="gsel" id="sel-mkt">
    <option value="">-- Market --</option>"""+mkt_opts+"""
  </select>
  <div class="tf-group" id="tf-row">
    <div class="tf-btn" onclick="setTF('1m')">1m</div>
    <div class="tf-btn" onclick="setTF('2m')">2m</div>
    <div class="tf-btn on"  onclick="setTF('5m')">5m</div>
    <div class="tf-btn" onclick="setTF('10m')">10m</div>
    <div class="tf-btn" onclick="setTF('15m')">15m</div>
  </div>
  <button class="scan-btn" onclick="doScan()">SCAN</button>
  <span id="rbadge">idle</span>
  <span id="cbadge">"""+cs+"""</span>
  <button id="mbtn" onclick="doMem()">MEM</button>
</div>

<div id="page">
  <div id="placeholder">Select market + timeframe then SCAN</div>
  <div id="scan-loading">Scanning...</div>
  <div id="wrap" style="display:none"></div>
</div>

<script>
var _mkt="",_tf="5m",_ok=false,_timer=null,_prev={};
var THR=3; // % change threshold

function setTF(tf){
  _tf=tf;
  document.querySelectorAll('.tf-btn').forEach(function(b){
    b.classList.toggle('on',b.textContent===tf);
  });
}

function doScan(){
  _mkt=document.getElementById('sel-mkt').value;
  if(!_mkt){alert('Market select karo!');return;}
  document.getElementById('placeholder').style.display='none';
  document.getElementById('wrap').style.display='none';
  document.getElementById('scan-loading').style.display='block';
  if(_timer) clearInterval(_timer);
  _prev={};
  fetch('/scan?market='+encodeURIComponent(_mkt)+'&tf='+encodeURIComponent(_tf))
    .then(function(r){return r.text();})
    .then(function(html){
      document.getElementById('scan-loading').style.display='none';
      document.getElementById('wrap').style.display='block';
      document.getElementById('wrap').innerHTML=html;
      _ok=true;
      _timer=setInterval(doRefresh,5000);
      setBadge('LIVE');
    }).catch(function(e){
      document.getElementById('scan-loading').style.display='none';
      document.getElementById('wrap').innerHTML="<p class='err-msg'>"+e+"</p>";
      document.getElementById('wrap').style.display='block';
    });
}

function doRefresh(){
  if(!_ok||!_mkt) return;
  fetch('/scores?market='+encodeURIComponent(_mkt)+'&tf='+encodeURIComponent(_tf))
    .then(function(r){return r.json();})
    .then(function(data){
      var changed=0;
      Object.keys(data).forEach(function(pair){
        var s=data[pair], prev=_prev[pair];
        var sc = !prev||Math.abs((prev.score||0)-(s.score||0))>=THR;
        var ss = !prev||prev.signal!==s.signal;
        if(!sc&&!ss) return;
        changed++;
        _prev[pair]=s;
        var safe=pair.replace(/[^a-zA-Z0-9]/g,'');
        var el=document.getElementById('sc-'+safe);
        var se=document.getElementById('si-'+safe);
        if(!el||!se) return;
        var bc=s.score>=65?'sh':s.score>=40?'sm':'sl';
        el.innerHTML="<span class='sb "+bc+"'>"+s.score+"%</span>";
        el.classList.add('changed');
        setTimeout(function(){el.classList.remove('changed');},500);
        var css={'STRONG BUY':'color:#00ff88;font-weight:bold',
                 'BUY':'color:#00ff88','SELL':'color:#ff6666',
                 'STRONG SELL':'color:#ff6666;font-weight:bold',
                 'WAIT':'color:#ffcc44'}[s.signal]||'color:#ffcc44';
        se.style.cssText=css;
        se.textContent=s.signal;
      });
      var n=new Date();
      setBadge(n.getHours().toString().padStart(2,'0')+':'+
               n.getMinutes().toString().padStart(2,'0')+':'+
               n.getSeconds().toString().padStart(2,'0')+
               (changed?' +'+changed:''));
    }).catch(function(){});
}

function setBadge(t){document.getElementById('rbadge').textContent=t;}

function doGO(mkt,pair){
  var safe=pair.replace(/[^a-zA-Z0-9]/g,'');
  var rowId='exp-'+safe;
  var row=document.getElementById(rowId);
  var btn=document.getElementById('gb-'+safe);
  if(row&&row.style.display!=='none'){
    row.style.display='none';
    if(btn)btn.classList.remove('on');
    return;
  }
  if(btn)btn.classList.add('on');
  window._cp=pair;window._cm=mkt;window._ct=_tf;
  if(!row){
    var mr=document.getElementById('r-'+safe);
    if(!mr)return;
    var tr=document.createElement('tr');
    tr.id=rowId;tr.className='exp-row';
    tr.innerHTML="<td colspan='7'><div class='exp-inner'><span style='color:#4da8ff'>Loading A layer...</span></div></td>";
    mr.parentNode.insertBefore(tr,mr.nextSibling);
  }else{
    row.style.display='';
    row.querySelector('.exp-inner').innerHTML="<span style='color:#4da8ff'>Loading...</span>";
  }
  fetch('/go?market='+encodeURIComponent(mkt)+'&pair='+encodeURIComponent(pair)+'&tf='+encodeURIComponent(_tf))
    .then(function(r){return r.text();})
    .then(function(html){
      var r2=document.getElementById(rowId);
      if(r2)r2.querySelector('.exp-inner').innerHTML=html + '<div class="btn-group"><button class="btn-cont" onclick="doContinue()">CONTINUE</button><button class="btn-canc" onclick="closeDetail()">CANCEL</button></div>';
    }).catch(function(e){
      var r2=document.getElementById(rowId);
      if(r2)r2.querySelector('.exp-inner').innerHTML="<span class='err-msg'>"+e+"</span>";
    });
}

function doContinue(){
  var pair=window._cp,mkt=window._cm,tf=window._ct;
  if(!pair)return;
  var safe=pair.replace(/[^a-zA-Z0-9]/g,'');
  var expRow=document.getElementById('exp-'+safe);
  if(!expRow)return;
  var detId='det-'+safe;
  var ex=document.getElementById(detId);
  if(!ex){
    var tr=document.createElement('tr');
    tr.id=detId;tr.className='det-row';
    tr.innerHTML="<td colspan='7'><div class='det-inner'><span style='color:#b088ff'>Deep analysis...</span></div></td>";
    expRow.parentNode.insertBefore(tr,expRow.nextSibling);
  }else{
    ex.style.display='';
    ex.querySelector('.det-inner').innerHTML="<span style='color:#b088ff'>Loading...</span>";
  }
  fetch('/detail?market='+encodeURIComponent(mkt)+'&pair='+encodeURIComponent(pair)+'&tf='+encodeURIComponent(tf))
    .then(function(r){return r.text();})
    .then(function(html){
      var dr=document.getElementById(detId);
      if(dr)dr.querySelector('.det-inner').innerHTML=html;
    }).catch(function(e){
      var dr=document.getElementById(detId);
      if(dr)dr.querySelector('.det-inner').innerHTML="<span class='err-msg'>"+e+"</span>";
    });
}

function closeDetail(){
  if(window._cp){
    var expRow=document.getElementById('exp-'+window._cp.replace(/[^a-zA-Z0-9]/g,''));
    var detRow=document.getElementById('det-'+window._cp.replace(/[^a-zA-Z0-9]/g,''));
    if(detRow)detRow.style.display='none';
    if(expRow)expRow.style.display='none';
  }
}

function doMem(){
  fetch('/mem').then(function(r){return r.json();})
    .then(function(d){
      document.getElementById('cbadge').textContent='MEM: '+d.freed+'MB freed';
    }).catch(function(){});
}
</script>
</body></html>""")


# ══════════════════════════════════════════════════════
# ROUTES
# ══════════════════════════════════════════════════════

@app.route('/')
def index():
    return build_page()


@app.route('/scan')
def scan():
    market  = request.args.get('market','')
    tf      = request.args.get('tf','5m')
    pairs   = MARKET_PAIRS.get(market,[])
    is_otc  = "OTC" in market
    tf      = _CFG.get("market_timeframes",{}).get(market, tf)
    
    try:
        results = sys_engine.scan(market, pairs)
    except Exception as e:
        return f"<p class='err-msg'>Scan error: {str(e)}</p>", 500

    rows_html = ""
    for r in results:
        pair   = r.get("pair", r.get("symbol",""))
        score  = r.get("score",0)
        trend  = r.get("trend","—")
        sr_pos = r.get("sr_position","—")
        signal = r.get("signal","WAIT")
        reason = r.get("reason","")[:32]
        safe   = pair.replace(" ","").replace("(","").replace(")","")

        bc = "sh" if score>=65 else "sm" if score>=40 else "sl"
        td = {"UP":"<span style='color:#00ff88'>▲</span>",
              "DOWN":"<span style='color:#ff6666'>▼</span>",
              "RANGING":"<span style='color:#aaaaff'>↔</span>"
              }.get(trend,"<span style='color:#ffcc44'>—</span>")
        sc = {"STRONG BUY":"color:#00ff88;font-weight:bold",
              "BUY":"color:#00ff88","SELL":"color:#ff6666",
              "STRONG SELL":"color:#ff6666;font-weight:bold",
              "WAIT":"color:#ffcc44"}.get(signal,"color:#ffcc44")

        badge = ""
        if is_otc:
            badge = "<span style='font-size:8px;color:#999;background:rgba(255,200,0,0.07);border:1px solid rgba(255,200,0,0.15);padding:1px 3px;border-radius:3px;margin-left:4px;'>OTC</span>"

        go_b = ("<button id='gb-"+safe+"' class='go-btn' onclick=\""
                "doGO('"+market.replace("'","")+"','"+pair.replace("'","")+"')\">"
                "GO</button>")

        rows_html += (
            "<tr id='r-"+safe+"'>"
            "<td><b>"+pair+"</b>"+badge+"</td>"
            "<td id='sc-"+safe+"'><span class='sb "+bc+"'>"+str(score)+"%</span></td>"
            "<td>"+td+" "+trend+"</td>"
            "<td style='color:#888;font-size:10px;'>"+sr_pos+"</td>"
            "<td id='si-"+safe+"' style='"+sc+"'>"+signal+"</td>"
            "<td style='color:#666;font-size:10px;'>"+reason+"</td>"
            "<td>"+go_b+"</td>"
            "</tr>"
        )

    return (
        "<table id='stbl'><thead><tr>"
        "<th>PAIR</th><th>SCORE</th><th>TREND</th>"
        "<th>S/R</th><th>SIGNAL</th><th>REASON</th><th>GO</th>"
        "</tr></thead><tbody>"+rows_html+"</tbody></table>"
        "<div style='color:#555;font-size:10px;padding:6px 2px;'>"
        +str(len(pairs))+" pairs | 65%+ strong | 40-65% mid | &lt;40% weak | live 5sec"
        "</div>"
    )


@app.route('/scores')
def scores():
    """5sec refresh — get latest scores"""
    try:
        return jsonify(sys_engine.get_current_scores())
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/go')
def go():
    market = request.args.get('market','')
    pair   = request.args.get('pair','')
    tf     = request.args.get('tf','5m')
    tf     = _CFG.get("market_timeframes",{}).get(market, tf)
    clean  = pair.replace(" (OTC)","").strip()
    
    try:
        html = sys_engine.go(clean, market)
    except Exception as e:
        html = f"<div class='err-msg'>GO error: {str(e)[:80]}</div>"
        
    if "(OTC)" in pair:
        html = ("<div style='font-size:10px;color:#ffcc88;padding:3px 8px;"
                "background:rgba(255,200,0,0.06);border-radius:5px;"
                "border:1px solid rgba(255,200,0,0.18);display:inline-block;"
                "margin-bottom:7px;'>OTC: Real "+clean+" data</div>")+html
    return html


@app.route('/detail')
def detail():
    market = request.args.get('market','')
    pair   = request.args.get('pair','')
    clean  = pair.replace(" (OTC)","").strip()
    
    try:
        return sys_engine.detail(clean, market)
    except Exception as e:
        return f"<div class='err-msg'>Detail error: {str(e)[:80]}</div>"


@app.route('/mem')
def mem():
    gc.collect()
    try:
        import ctypes
        ctypes.CDLL("libc.so.6").malloc_trim(0)
    except Exception:
        pass
    return jsonify({"status":"ok","freed":0})


@app.route('/favicon.ico')
def favicon():
    return '', 204


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False, threaded=True)