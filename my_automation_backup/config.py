# config.py
MARKET_TIMEFRAMES = {
    "Binary OTC": "5m",
    "CFD":        "5m",
    "Spot":       "5m",
    "Future":     "5m",
}

HTML_TEMPLATE = """<!DOCTYPE html>
<html lang='en'>
<head>
<meta charset='UTF-8'>
<meta name='viewport' content='width=device-width,initial-scale=1'>
<title>Signal Bot</title>
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body {
    background: #0d0d0d;
    color: #ccc;
    font-family: 'Segoe UI', monospace, sans-serif;
    font-size: 12px;
    min-height: 100vh;
  }

  /* ── Header ── */
  .hdr {
    background: linear-gradient(135deg,#111,#1a1a2e);
    padding: 8px 16px;
    display: flex;
    align-items: center;
    gap: 10px;
    border-bottom: 1px solid #222;
    flex-wrap: wrap;
  }
  .hdr h1 {
    color: #00cc66;
    font-size: 15px;
    letter-spacing: 1px;
    margin-right: 8px;
  }
  .time-stats {
    margin-left: auto;
    display: flex;
    flex-direction: column;
    align-items: flex-end;
    gap: 2px;
    background: rgba(0,0,0,0.3);
    padding: 4px 8px;
    border-radius: 6px;
    font-size: 11px;
  }
  .time-stats div {
    display: flex;
    gap: 6px;
    line-height: 1.4;
  }
  .time-stats span {
    color: #00cc66;
    font-weight: bold;
    font-family: monospace;
    font-size: 11px;
  }

  /* ── Controls ── */
  select, input[type=text], input[type=password] {
    background: #1a1a1a;
    color: #ccc;
    border: 1px solid #333;
    border-radius: 5px;
    padding: 5px 8px;
    font-size: 11px;
    outline: none;
  }
  select:focus, input:focus { border-color: #00cc66; }

  .scan-btn {
    background: linear-gradient(135deg,#004422,#006633);
    color: #00ff88;
    border: 1px solid #00cc66;
    border-radius: 6px;
    padding: 5px 14px;
    font-size: 12px;
    font-weight: bold;
    cursor: pointer;
    transition: all 0.2s;
  }
  .scan-btn:hover { background: #006633; }
  .scan-btn:disabled { opacity: 0.5; cursor: not-allowed; }

  /* ── Status bar ── */
  #status-bar {
    font-size: 10px;
    color: #555;
    padding: 4px 16px;
    border-bottom: 1px solid #181818;
    display: flex;
    gap: 16px;
    align-items: center;
  }
  #status-bar .dot {
    width: 7px; height: 7px;
    border-radius: 50%;
    display: inline-block;
    margin-right: 4px;
  }
  .dot-green  { background: #00cc66; }
  .dot-red    { background: #ff4444; }
  .dot-orange { background: #ffaa00; }

  /* ── IQ credentials panel ── */
  #iq-creds {
    display: none;
    background: #111;
    border: 1px solid #333;
    border-radius: 6px;
    padding: 6px 10px;
    gap: 6px;
    align-items: center;
    flex-wrap: wrap;
  }
  #iq-creds.visible { display: flex; }

  /* ── Main layout ── */
  #main-wrap {
    display: flex;
    height: calc(100vh - 85px);
  }
  #pairs-panel {
    flex: 1;
    overflow-y: auto;
    padding: 12px;
  }
  #detail-panel {
    width: 340px;
    min-width: 280px;
    background: #111;
    border-left: 1px solid #1e1e1e;
    overflow-y: auto;
    padding: 12px;
    display: none;
  }
  #detail-panel.open { display: block; }

  /* ── Card container ── */
  .cards-container {
    display: flex;
    flex-direction: column;
    gap: 8px;
  }
  .pair-card {
    background: #121212;
    border: 1px solid #252525;
    border-radius: 8px;
    padding: 8px 12px;
    cursor: grab;
    transition: all 0.1s;
  }
  .pair-card:active { cursor: grabbing; }
  .pair-card.dragging { opacity: 0.5; }

  /* Card header (top row) */
  .card-header {
    display: flex;
    align-items: center;
    gap: 12px;
    margin-bottom: 6px;
    flex-wrap: wrap;
  }
  .drag-handle {
    font-size: 16px;
    color: #777;
    cursor: grab;
  }
  .drag-handle:active { cursor: grabbing; }
  .status-light {
    font-size: 14px;
    cursor: pointer;
  }
  .pair-name {
    font-weight: bold;
    font-size: 13px;
    color: #00cc66;
  }
  .score-badge {
    display: inline-block;
    padding: 1px 5px;
    border-radius: 3px;
    font-weight: bold;
    font-size: 11px;
  }
  .score-high { background: rgba(0,204,102,0.15); color: #00cc66; }
  .score-mid  { background: rgba(255,170,0,0.15);  color: #ffaa00; }
  .score-low  { background: rgba(255,68,68,0.15);  color: #ff4444; }
  .go-btn {
    background: #1a2a1a;
    color: #00cc66;
    border: 1px solid #2a4a2a;
    border-radius: 4px;
    padding: 2px 8px;
    cursor: pointer;
    font-size: 10px;
    transition: all 0.15s;
    margin-left: auto;
  }
  .go-btn:hover { background: #1e3a1e; border-color: #00cc66; }

  /* Action buttons row */
  .action-buttons {
    display: flex;
    gap: 8px;
    margin-top: 6px;
    justify-content: flex-end;
  }
  .data-btn {
    background: #1a1a1a;
    border: 1px solid #333;
    border-radius: 4px;
    padding: 2px 8px;
    font-size: 9px;
    cursor: pointer;
    color: #aaa;
    transition: all 0.1s;
  }
  .data-btn.available {
    background: #1a3a1a;
    border-color: #00cc66;
    color: #00cc66;
  }
  .data-btn:hover {
    background: #2a2a2a;
  }

  /* Card body (second row) */
  .card-body {
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    gap: 12px;
    font-size: 10px;
    color: #aaa;
    margin-top: 4px;
  }
  .trend, .sr, .signal, .quality, .reason {
    display: inline-flex;
    align-items: center;
    gap: 4px;
  }
  .signal {
    font-weight: bold;
  }
  .quality {
    padding: 1px 4px;
    border-radius: 3px;
    background: rgba(255,255,255,0.05);
  }
  .reason {
    color: #888;
    max-width: 200px;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }
  .feel-bar {
    display: inline-flex;
    align-items: center;
    gap: 4px;
    margin-left: auto;
  }
  .feel-blocks {
    display: flex;
    gap: 1px;
  }
  .feel-block {
    width: 4px;
    height: 10px;
    background: #2a2a2a;
    border-radius: 1px;
  }
  .feel-pct {
    font-size: 9px;
    color: #ffaa00;
  }

  /* Detail panel */
  #detail-title {
    font-size: 13px;
    font-weight: bold;
    color: #00cc66;
    margin-bottom: 10px;
    display: flex;
    align-items: center;
    gap: 8px;
  }
  #close-panel {
    margin-left: auto;
    cursor: pointer;
    color: #555;
    font-size: 14px;
  }
  #close-panel:hover { color: #ccc; }

  /* Loading & placeholder */
  #loading {
    display: none;
    text-align: center;
    padding: 30px;
    color: #444;
    font-size: 13px;
  }
  #loading.visible { display: block; }
  .spinner {
    display: inline-block;
    width: 20px; height: 20px;
    border: 2px solid #333;
    border-top-color: #00cc66;
    border-radius: 50%;
    animation: spin 0.8s linear infinite;
    margin-right: 8px;
    vertical-align: middle;
  }
  @keyframes spin { to { transform: rotate(360deg); } }
  .err-msg { color: #ff4444; padding: 8px; font-size: 11px; }
  #placeholder {
    color: #333;
    text-align: center;
    padding: 40px;
    font-size: 13px;
  }
  ::-webkit-scrollbar { width: 5px; }
  ::-webkit-scrollbar-track { background: #0d0d0d; }
  ::-webkit-scrollbar-thumb { background: #2a2a2a; border-radius: 3px; }
</style>
<script src="https://cdn.jsdelivr.net/npm/sortablejs@latest/Sortable.min.js"></script>
</head>
<body>

<!-- Header -->
<div class='hdr'>
  <h1>⚡ Signal Bot</h1>
  <select id='src-sel' onchange='onSrcChange()' title='Data Platform'>
    <option value='real'>🌐 Real</option>
    <option value='iqoption'>📊 IQ Option</option>
    <option value='quotex'>📈 Quotex</option>
  </select>
  <div id='iq-creds'>
    <input type='text'     id='iq-email' placeholder='IQ Email'    style='width:140px;'>
    <input type='password' id='iq-pwd'   placeholder='IQ Password' style='width:110px;'>
  </div>
  <select id='mkt-sel' title='Market Type'>
    <!-- MARKET_OPTIONS -->
  </select>
  <select id='tf-sel' title='Timeframe'>
    <option value='1m'>1m</option>
    <option value='2m'>2m</option>
    <option value='5m' selected>5m</option>
    <option value='10m'>10m</option>
    <option value='15m'>15m</option>
    <option value='1h'>1h</option>
    <option value='4h'>4h</option>
  </select>
  <button class='scan-btn' id='scan-btn' onclick='doScan()'>⟳ SCAN</button>
  <div class='time-stats'>
    <div>🕒 Clock: <span id='current-time'>--:--:--</span></div>
    <div>⏱️ Uptime: <span id='stopwatch'>00:00:00.000</span></div>
  </div>
</div>

<!-- Status bar -->
<div id='status-bar'>
  <span><span class='dot dot-red' id='ws-dot'></span><span id='ws-status'>Not connected</span></span>
  <span id='call-stats'></span>
  <span id='pair-count'></span>
</div>

<!-- Main content -->
<div id='main-wrap'>
  <div id='pairs-panel'>
    <div id='loading'><span class='spinner'></span> Scanning pairs…</div>
    <div id='placeholder'>Select platform + market + timeframe → SCAN</div>
    <div id='result-wrap'></div>
  </div>
  <div id='detail-panel'>
    <div id='detail-title'>
      <span id='detail-pair'>—</span>
      <span id='close-panel' onclick='closePanel()' title='Close'>✕</span>
    </div>
    <div id='detail-body'></div>
  </div>
</div>

<script>
// ── State ────────────────────────────────────────────────────────────────────
var _mkt = '', _tf = '5m', _src = 'real';
var _iqEmail = '', _iqPwd = '';
var _sortable = null;
var refreshInterval = null;

// Time display
var startTime = Date.now();
function updateStopwatch() {
  var elapsed = Date.now() - startTime;
  var hours = Math.floor(elapsed / 3600000);
  var minutes = Math.floor((elapsed % 3600000) / 60000);
  var seconds = Math.floor((elapsed % 60000) / 1000);
  var ms = elapsed % 1000;
  document.getElementById('stopwatch').innerText = 
    String(hours).padStart(2,'0') + ':' + 
    String(minutes).padStart(2,'0') + ':' + 
    String(seconds).padStart(2,'0') + '.' + 
    String(ms).padStart(3,'0');
}
function updateCurrentTime() {
  var now = new Date();
  document.getElementById('current-time').innerText = 
    String(now.getHours()).padStart(2,'0') + ':' + 
    String(now.getMinutes()).padStart(2,'0') + ':' + 
    String(now.getSeconds()).padStart(2,'0');
}
setInterval(updateStopwatch, 100);
setInterval(updateCurrentTime, 1000);
updateStopwatch();
updateCurrentTime();

function onSrcChange() {
  _src = document.getElementById('src-sel').value;
  var iqDiv = document.getElementById('iq-creds');
  iqDiv.classList.toggle('visible', _src === 'iqoption');
}

// Helper to copy data to clipboard (no new tab)
async function copyData(symbol, type) {
  const url = `/data/${symbol}/${type}`;
  try {
    const response = await fetch(url);
    if (!response.ok) {
      alert(`File not found: ${symbol}_${type}.tsv\\nRun fill first.`);
      return;
    }
    const text = await response.text();
    await navigator.clipboard.writeText(text);
    alert(`✅ Copied ${type} data for ${symbol} to clipboard!`);
  } catch (err) {
    console.error("Copy failed:", err);
    alert("Failed to copy data. Check console.");
  }
}

function escapeHtml(str) {
  if (!str) return '';
  return str.replace(/[&<>]/g, function(m) {
    if (m === '&') return '&amp;';
    if (m === '<') return '&lt;';
    if (m === '>') return '&gt;';
    return m;
  });
}

// Build cards from initial scan HTML
function buildCards(originalTbody) {
  var container = document.createElement('div');
  container.className = 'cards-container';
  var rows = Array.from(originalTbody.children);
  for (var i = 0; i < rows.length; i++) {
    var row = rows[i];
    var cells = row.cells;
    if (cells.length < 11) continue;
    
    var dragHandleHtml = cells[0].innerHTML;
    var statusHtml = cells[1].innerHTML;
    var pairName = cells[2].innerText.trim();
    var scoreHtml = cells[3].innerHTML;
    var trendHtml = cells[4].innerHTML;
    var srHtml = cells[5].innerHTML;
    var signalHtml = cells[6].innerHTML;
    var qualityHtml = cells[7].innerHTML;
    var reasonHtml = cells[8].innerHTML;
    var feelHtml = cells[9].innerHTML;
    
    var card = document.createElement('div');
    card.className = 'pair-card';
    card.setAttribute('data-pair', pairName);
    
    // Header
    var header = document.createElement('div');
    header.className = 'card-header';
    header.innerHTML = `
      <span class='drag-handle'>${dragHandleHtml}</span>
      <span class='status-light' onclick="fillSymbol('${escapeHtml(pairName)}')">${statusHtml}</span>
      <span class='pair-name'>${escapeHtml(pairName)}</span>
      <span class='score-badge'>${scoreHtml}</span>
      <button class='go-btn' onclick="doGO('${_mkt}','${escapeHtml(pairName)}','${_tf}')">→</button>
    `;
    
    // Action buttons row - using copyData instead of viewData
    var actionRow = document.createElement('div');
    actionRow.className = 'action-buttons';
    actionRow.innerHTML = `
      <button class='data-btn cvd' onclick="copyData('${escapeHtml(pairName)}','cvd')">CVD</button>
      <button class='data-btn correlation' onclick="copyData('${escapeHtml(pairName)}','correlation')">CORR</button>
      <button class='data-btn derivative' onclick="copyData('${escapeHtml(pairName)}','derivative')">DERIV</button>
      <button class='data-btn liquidations' onclick="copyData('${escapeHtml(pairName)}','liquidations')">LIQ</button>
      <button class='data-btn depth' onclick="copyData('${escapeHtml(pairName)}','depth')">DEPTH</button>
      <button class='data-btn candles' onclick="copyData('${escapeHtml(pairName)}','candles')">CANDLES</button>
    `;
    
    // Body
    var body = document.createElement('div');
    body.className = 'card-body';
    body.innerHTML = `
      <div class='trend'>${trendHtml}</div>
      <div class='sr'>${srHtml}</div>
      <div class='signal'>${signalHtml}</div>
      <div class='quality'>${qualityHtml}</div>
      <div class='reason' title='${escapeHtml(reasonHtml.replace(/<[^>]*>/g, ''))}'>${reasonHtml}</div>
      <div class='feel-bar'>${feelHtml}</div>
    `;
    
    card.appendChild(header);
    card.appendChild(actionRow);
    card.appendChild(body);
    container.appendChild(card);
  }
  return container;
}

// ── Refresh cards using lightweight /refresh endpoint ─────────────────────
async function refreshCards() {
  if (!_mkt) return;
  try {
    var resp = await fetch('/refresh');
    var scores = await resp.json();
    var cards = document.querySelectorAll('.pair-card');
    for (var i = 0; i < cards.length; i++) {
      var card = cards[i];
      var pair = card.getAttribute('data-pair');
      var data = scores[pair];
      if (!data) continue;
      
      // Update score badge
      var scoreSpan = card.querySelector('.score-badge');
      if (scoreSpan) {
        var score = data.score;
        if (score === "NA") {
          scoreSpan.innerHTML = "<span class='sb sl'>NA</span>";
        } else {
          var bc = (score >= 65) ? "sh" : (score >= 40) ? "sm" : "sl";
          scoreSpan.innerHTML = `<span class='sb ${bc}'>${score}%</span>`;
        }
      }
      
      // Update trend
      var trendDiv = card.querySelector('.trend');
      if (trendDiv) {
        var trend = data.trend || "FLAT";
        var trendIcon = {"UP":"▲","DOWN":"▼","RANGING":"↔","FLAT":"—"}[trend] || "—";
        trendDiv.innerHTML = `${trendIcon} ${trend}`;
      }
      
      // Update SR position
      var srDiv = card.querySelector('.sr');
      if (srDiv) srDiv.innerHTML = data.sr_position || "—";
      
      // Update signal
      var signalDiv = card.querySelector('.signal');
      if (signalDiv) {
        var signal = data.signal || "WAIT";
        var signalColor = {"STRONG BUY":"#00ff88","BUY":"#00ff88","SELL":"#ff6666","STRONG SELL":"#ff4444","WAIT":"#ffcc44"}[signal] || "#ffcc44";
        signalDiv.innerHTML = `<span style="color:${signalColor};">${signal}</span>`;
      }
      
      // Update quality
      var qualityDiv = card.querySelector('.quality');
      if (qualityDiv) {
        var quality = data.quality || "LOW";
        var qc = {"HIGH":"#00cc66","MED":"#ffaa00","LOW":"#ff4444"}[quality] || "#888";
        qualityDiv.innerHTML = `<span style="font-size:9px;padding:1px 4px;border-radius:3px;background:${qc}22;border:1px solid ${qc}66;color:${qc};">${quality}</span>`;
      }
      
      // Update reason
      var reasonDiv = card.querySelector('.reason');
      if (reasonDiv) {
        var reason = data.reason || "";
        reasonDiv.innerText = reason;
        reasonDiv.setAttribute('title', reason);
      }
      
      // Update feel bar and status light
      var feelPct = data.feel_pct || 0;
      var feelBarDiv = card.querySelector('.feel-bar');
      if (feelBarDiv) {
        var steps = Math.round(feelPct / 5);
        var color = feelPct >= 40 ? '#00cc66' : (feelPct >= 5 ? '#ffaa00' : '#ff4444');
        var blocks = "";
        for (var j = 0; j < 20; j++) {
          var bg = j < steps ? color : '#2a2a2a';
          blocks += `<div style='width:4px;height:10px;background:${bg};border-radius:1px;margin-right:1px;display:inline-block;'></div>`;
        }
        feelBarDiv.innerHTML = `<div style='display:flex;flex-direction:column;align-items:center;gap:2px;'>
          <div style='display:flex;align-items:center;' class='feel-blocks'>${blocks}</div>
          <span class='feel-pct' style='font-size:9px;color:${color};'>${feelPct}%</span>
        </div>`;
      }
      
      // Update status light emoji (🔴/🟡/🟢)
      var statusSpan = card.querySelector('.status-light');
      if (statusSpan) {
        var emoji = feelPct == 0 ? '🔴' : (feelPct < 100 ? '🟡' : '🟢');
        statusSpan.innerHTML = emoji;
      }
    }
  } catch(e) {
    console.warn("Refresh failed:", e);
  }
}

function doScan() {
  _mkt = document.getElementById('mkt-sel').value;
  _tf  = document.getElementById('tf-sel').value;
  _src = document.getElementById('src-sel').value;
  if (!_mkt) { alert('Select a market first'); return; }
  
  if (_src === 'iqoption') {
    _iqEmail = document.getElementById('iq-email').value;
    _iqPwd = document.getElementById('iq-pwd').value;
  }
  
  document.getElementById('placeholder').style.display = 'none';
  document.getElementById('loading').classList.add('visible');
  document.getElementById('result-wrap').innerHTML = '';
  document.getElementById('scan-btn').disabled = true;
  
  var url = '/scan?market=' + encodeURIComponent(_mkt) + '&tf=' + encodeURIComponent(_tf) + '&src=' + encodeURIComponent(_src);
  if (_src === 'iqoption') {
    url += '&iq_email=' + encodeURIComponent(_iqEmail) + '&iq_pwd=' + encodeURIComponent(_iqPwd);
  }
  
  fetch(url)
    .then(r => r.text())
    .then(html => {
      document.getElementById('loading').classList.remove('visible');
      var tempDiv = document.createElement('div');
      tempDiv.innerHTML = html;
      var originalTbody = tempDiv.querySelector('#stbl tbody');
      if (originalTbody) {
        var cardsContainer = buildCards(originalTbody);
        document.getElementById('result-wrap').innerHTML = '';
        document.getElementById('result-wrap').appendChild(cardsContainer);
        if (_sortable) _sortable.destroy();
        _sortable = new Sortable(cardsContainer, {
          animation: 150,
          handle: '.drag-handle',
          onEnd: function() {
            var order = [];
            document.querySelectorAll('.pair-card').forEach(card => {
              var pair = card.getAttribute('data-pair');
              if (pair) order.push(pair);
            });
            localStorage.setItem('cardOrder_' + _mkt + '_' + _src + '_' + _tf, JSON.stringify(order));
          }
        });
        restoreOrder();
        
        // Start auto-refresh every 500ms
        if (refreshInterval) clearInterval(refreshInterval);
        refreshInterval = setInterval(refreshCards, 500);
        
      } else {
        document.getElementById('result-wrap').innerHTML = html;
      }
      document.getElementById('scan-btn').disabled = false;
      updateWsDot(true);
      updateCallStats();
      setInterval(updateCallStats, 10000);
    })
    .catch(e => {
      document.getElementById('loading').classList.remove('visible');
      document.getElementById('result-wrap').innerHTML = "<p class='err-msg'>Scan error: " + e + "</p>";
      document.getElementById('scan-btn').disabled = false;
    });
}

function restoreOrder() {
  var key = 'cardOrder_' + _mkt + '_' + _src + '_' + _tf;
  var order = localStorage.getItem(key);
  if (!order) return;
  order = JSON.parse(order);
  var container = document.querySelector('.cards-container');
  if (!container) return;
  var cards = Array.from(container.children);
  var sorted = [];
  order.forEach(pair => {
    var card = cards.find(c => c.getAttribute('data-pair') === pair);
    if (card) sorted.push(card);
  });
  cards.forEach(card => { if (!sorted.includes(card)) sorted.push(card); });
  sorted.forEach(card => container.appendChild(card));
}

function doGO(market, pair, tf) {
  document.getElementById('detail-pair').textContent = pair;
  document.getElementById('detail-body').innerHTML = "<div style='text-align:center;padding:20px;color:#444;'><span class='spinner'></span> Running A-layer…</div>";
  document.getElementById('detail-panel').classList.add('open');
  fetch('/go?market=' + encodeURIComponent(market) + '&pair=' + encodeURIComponent(pair) + '&tf=' + encodeURIComponent(tf))
    .then(r => r.text())
    .then(html => document.getElementById('detail-body').innerHTML = html)
    .catch(e => document.getElementById('detail-body').innerHTML = "<p class='err-msg'>GO error: " + e + "</p>");
}

function doDeep(market, pair, tf, aResultJson) {
  document.getElementById('detail-body').innerHTML += "<div style='text-align:center;padding:12px;color:#444;'><span class='spinner'></span> Running D-layer…</div>";
  fetch('/deep?market=' + encodeURIComponent(market) + '&pair=' + encodeURIComponent(pair) + '&tf=' + encodeURIComponent(tf) + '&a_result=' + encodeURIComponent(aResultJson))
    .then(r => r.text())
    .then(html => {
      var body = document.getElementById('detail-body');
      var spinner = body.querySelector('.spinner');
      if (spinner) spinner.parentNode.remove();
      body.innerHTML += html;
    })
    .catch(e => document.getElementById('detail-body').innerHTML += "<p class='err-msg'>Deep error: " + e + "</p>");
}

function closePanel() {
  document.getElementById('detail-panel').classList.remove('open');
  document.getElementById('detail-body').innerHTML = '';
}

function updateWsDot(connected) {
  var dot = document.getElementById('ws-dot');
  var stat = document.getElementById('ws-status');
  if (connected) {
    dot.className = 'dot dot-green';
    stat.textContent = 'Connected (' + _src + ')';
  } else {
    dot.className = 'dot dot-red';
    stat.textContent = 'Not connected';
  }
}

function updateCallStats() {
  fetch('/calls')
    .then(r => r.text())
    .then(html => document.getElementById('call-stats').innerHTML = html)
    .catch(() => {});
}

// fillSymbol – polls /fill_status every second and updates buttons
function fillSymbol(pair) {
  var card = document.querySelector('.pair-card[data-pair="' + pair.replace(/"/g, '&quot;') + '"]');
  if (!card) return;
  var statusSpan = card.querySelector('.status-light');
  if (statusSpan) {
    statusSpan.innerHTML = '🟡';
    statusSpan.style.cursor = 'wait';
  }
  
  // Disable all data buttons and show loading state
  var btnTypes = ['cvd', 'correlation', 'derivative', 'depth', 'candles', 'liquidations'];
  for (var i = 0; i < btnTypes.length; i++) {
    var btn = card.querySelector('.data-btn.' + btnTypes[i]);
    if (btn) {
      btn.classList.remove('available');
      btn.style.opacity = '0.5';
    }
  }
  
  fetch('/fill?symbol=' + encodeURIComponent(pair))
    .then(r => r.json())
    .then(data => {
      if (data.status === 'started') {
        var pollInterval = setInterval(function() {
          fetch('/fill_status?symbol=' + encodeURIComponent(pair))
            .then(r => r.json())
            .then(status => {
              var allDone = true;
              for (var i = 0; i < btnTypes.length; i++) {
                var comp = btnTypes[i];
                var btn = card.querySelector('.data-btn.' + comp);
                if (btn) {
                  if (comp === 'liquidations') {
                    if (status['derivative'] === true) {
                      btn.classList.add('available');
                      btn.style.opacity = '1';
                    } else {
                      allDone = false;
                    }
                  } else {
                    if (status[comp] === true) {
                      btn.classList.add('available');
                      btn.style.opacity = '1';
                    } else {
                      allDone = false;
                    }
                  }
                }
              }
              if (allDone) {
                clearInterval(pollInterval);
                if (statusSpan) statusSpan.style.cursor = 'pointer';
                refreshCards();
              }
            })
            .catch(e => console.warn("Status poll error", e));
        }, 1000);
      } else {
        alert('Fill failed: ' + (data.error || 'Unknown error'));
        if (statusSpan) statusSpan.innerHTML = '🔴';
      }
    })
    .catch(e => {
      alert('Error: ' + e);
      if (statusSpan) statusSpan.innerHTML = '🔴';
    });
}

document.getElementById('mkt-sel').addEventListener('change', function() {
  var mktTf = {'Binary OTC':'1m','CFD':'5m','Spot':'5m','Future':'5m'};
  document.getElementById('tf-sel').value = mktTf[this.value] || '5m';
});
</script>
</body>
</html>"""

print("✅ [config] Loaded OK")