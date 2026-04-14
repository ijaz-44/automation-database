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
<title>KALI PORT</title>
<link href="https://fonts.googleapis.com/css2?family=Orbitron:wght@400;700;900&display=swap" rel="stylesheet">
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body {
    background: #000000;
    color: #ccc;
    font-family: 'Segoe UI', monospace, sans-serif;
    font-size: 12px;
    min-height: 100vh;
  }

  /* ── Header container ── */
  .header-container {
    background: linear-gradient(135deg,#0a0a0a,#1a1a2e);
    border-bottom: 1px solid #9AFFAB;
    padding: 0 20px;
  }
  .logo-row {
    padding: 10px 0 5px 0;
    margin: 0;
    text-align: left;
  }
  .logo-row h1 {
    font-family: 'Orbitron', monospace;
    font-weight: 900;
    letter-spacing: 2px;
    color: #FF6600;
    font-size: 20px;
    margin: 0;
  }
  .controls-row {
    display: flex;
    justify-content: space-between;
    align-items: stretch;
    gap: 20px;
    padding: 5px 0 10px 0;
    flex-wrap: wrap;
  }
  .left-controls {
    display: flex;
    flex-direction: column;
    gap: 8px;
    background: rgba(0,0,0,0.3);
    padding: 6px 12px;
    border-radius: 0;
    flex: 1;
  }
  .top-controls {
    display: flex;
    align-items: center;
    gap: 10px;
    flex-wrap: wrap;
  }
  .bottom-controls {
    display: flex;
    align-items: center;
    gap: 10px;
    flex-wrap: wrap;
  }
  .right-clocks {
    display: flex;
    flex-direction: column;
    justify-content: center;
    background: rgba(0,0,0,0.3);
    padding: 4px 12px;
    border-radius: 0;
    border-left: 2px solid #9AFFAB;
  }
  .time-stats {
    display: flex;
    flex-direction: column;
    gap: 4px;
    font-size: 11px;
  }
  .time-stats div {
    display: flex;
    gap: 8px;
    justify-content: space-between;
  }
  .time-stats span {
    color: #9AFFAB;
    font-weight: bold;
    font-family: monospace;
    font-size: 12px;
  }

  /* ── Controls (form elements) ── */
  select, input[type=text], input[type=password] {
    background: #1a1a1a;
    color: #ccc;
    border: 1px solid #333;
    border-radius: 0;
    padding: 4px 8px;
    font-size: 11px;
    outline: none;
  }
  select:focus, input:focus { border-color: #9AFFAB; }

  .scan-btn {
    background: linear-gradient(135deg,#1a3a1a,#2a5a2a);
    color: #9AFFAB;
    border: 1px solid #9AFFAB;
    border-radius: 0;
    padding: 2px 10px;
    font-size: 10px;
    font-weight: bold;
    cursor: pointer;
    transition: all 0.2s;
  }
  .scan-btn:hover { background: #2a6a2a; transform: scale(1.02); }
  .scan-btn:disabled { opacity: 0.5; cursor: not-allowed; }

  /* ── IQ credentials panel ── */
  #iq-creds {
    display: none;
    background: #111;
    border: 1px solid #333;
    border-radius: 0;
    padding: 4px 8px;
    gap: 6px;
    align-items: center;
    flex-wrap: wrap;
  }
  #iq-creds.visible { display: flex; }

  /* ── Status bar ── */
  #status-bar {
    font-size: 10px;
    color: #555;
    padding: 4px 20px;
    border-bottom: 1px solid #181818;
    display: flex;
    gap: 16px;
    align-items: center;
    background: #0a0a0a;
  }
  #status-bar .dot {
    width: 7px; height: 7px;
    border-radius: 0;
    display: inline-block;
    margin-right: 4px;
  }
  .dot-green  { background: #9AFFAB; }
  .dot-red    { background: #FF6352; }
  .dot-orange { background: #FCDB66; }

  /* ── Main layout ── */
  #main-wrap {
    display: flex;
    height: calc(100vh - 115px);
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
    border-radius: 0;
    padding: 8px 12px;
    cursor: grab;
    transition: all 0.1s;
  }
  .pair-card:active { cursor: grabbing; }
  .pair-card.dragging { opacity: 0.5; }

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
  .status-light {
    width: 14px;
    height: 14px;
    display: inline-block;
    background: #FF6352;
    cursor: pointer;
  }
  .pair-name {
    font-weight: bold;
    font-size: 13px;
    color: #9AFFAB;
  }
  .score-badge {
    display: inline-block;
    padding: 1px 5px;
    border-radius: 0;
    font-weight: bold;
    font-size: 11px;
  }
  .score-high { background: rgba(154,255,171,0.15); color: #9AFFAB; }
  .score-mid  { background: rgba(252,219,102,0.15); color: #FCDB66; }
  .score-low  { background: rgba(255,99,82,0.15); color: #FF6352; }
  .go-btn {
    background: #1a2a1a;
    color: #9AFFAB;
    border: 1px solid #2a4a2a;
    border-radius: 0;
    padding: 2px 8px;
    cursor: pointer;
    font-size: 10px;
    margin-left: auto;
  }
  .go-btn:hover { background: #1e3a1e; border-color: #9AFFAB; }

  .action-buttons {
    display: flex;
    gap: 8px;
    margin-top: 6px;
    justify-content: flex-start;
    flex-wrap: wrap;
  }
  .action-row {
    margin-bottom: 4px;
    display: flex;
    gap: 8px;
    flex-wrap: wrap;
  }
  .data-btn {
    background: #1a1a1a;
    border: 1px solid #333;
    border-radius: 0;
    padding: 2px 8px;
    font-size: 9px;
    cursor: pointer;
    color: #aaa;
    transition: all 0.1s;
  }
  .data-btn.available {
    background: #1a3a1a;
    border-color: #9AFFAB;
    color: #9AFFAB;
  }
  .data-btn:hover {
    background: #2a2a2a;
  }
  .btn-percent {
    font-size: 8px;
    margin-left: 4px;
    opacity: 0.9;
    color: #FCDB66;
  }

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
  .signal { font-weight: bold; }
  .quality {
    padding: 1px 4px;
    border-radius: 0;
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
    border-radius: 0;
  }
  .feel-pct {
    font-size: 9px;
    color: #FCDB66;
  }

  #detail-title {
    font-size: 13px;
    font-weight: bold;
    color: #9AFFAB;
    margin-bottom: 10px;
    display: flex;
    align-items: center;
    gap: 8px;
  }
  #close-panel {
    margin-left: auto;
    cursor: pointer;
    color: #FF6352;
    font-size: 14px;
  }
  #close-panel:hover { color: #FCDB66; }

  #loading {
    display: none;
    text-align: center;
    padding: 30px;
    color: #9AFFAB;
    font-size: 13px;
  }
  #loading.visible { display: block; }
  .spinner {
    display: inline-block;
    width: 20px; height: 20px;
    border: 2px solid #2a2a2a;
    border-top-color: #9AFFAB;
    border-radius: 50%;
    animation: spin 0.8s linear infinite;
    margin-right: 8px;
    vertical-align: middle;
  }
  @keyframes spin { to { transform: rotate(360deg); } }
  .err-msg { color: #FF6352; padding: 8px; font-size: 11px; }
  #placeholder {
    color: #9AFFAB;
    text-align: center;
    padding: 40px;
    font-size: 13px;
    border: 1px dashed #9AFFAB;
  }
  ::-webkit-scrollbar { width: 5px; }
  ::-webkit-scrollbar-track { background: #0d0d0d; }
  ::-webkit-scrollbar-thumb { background: #9AFFAB; border-radius: 0; }
</style>
<script src="https://cdn.jsdelivr.net/npm/sortablejs@latest/Sortable.min.js"></script>
</head>
<body>

<div class="header-container">
  <div class="logo-row">
    <h1>⚡ KALI PORT 💱</h1>
  </div>
  <div class="controls-row">
    <div class="left-controls">
      <div class="top-controls">
        <select id='src-sel' onchange='onSrcChange()' title='Data Platform'>
          <option value='real'>🌐 Real</option>
          <option value='iqoption'>📊 IQ Option</option>
          <option value='quotex'>📈 Quotex</option>
        </select>
        <div id='iq-creds'>
          <input type='text' id='iq-email' placeholder='IQ Email' style='width:120px;'>
          <input type='password' id='iq-pwd' placeholder='IQ Password' style='width:100px;'>
        </div>
        <select id='tf-sel' title='Timeframe'>
          <option value='1m'>1m</option>
          <option value='2m'>2m</option>
          <option value='5m' selected>5m</option>
          <option value='10m'>10m</option>
          <option value='15m'>15m</option>
          <option value='1h'>1h</option>
          <option value='4h'>4h</option>
        </select>
      </div>
      <div class="bottom-controls">
        <select id='mkt-sel' title='Market Type'>
          <!-- MARKET_OPTIONS -->
        </select>
        <button class='scan-btn' id='scan-btn' onclick='doScan()'>⟳ SCAN</button>
      </div>
    </div>
    <div class="right-clocks">
      <div class='time-stats'>
        <div>🕒 <span id='current-time'>--:--:--</span></div>
        <div>⏱️ <span id='stopwatch'>00:00:00.000</span></div>
      </div>
    </div>
  </div>
</div>

<div id='status-bar'>
  <span><span class='dot dot-red' id='ws-dot'></span><span id='ws-status'>Not connected</span></span>
  <span id='call-stats'></span>
  <span id='pair-count'></span>
</div>

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

// Copy single data file to clipboard
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

// Master copy: copy all available data files for a symbol
async function copyAllData(symbol) {
  const types = ['candles', 'cvd', 'correlation', 'derivative', 'depth', 'liquidations', 
                 'macro', 'sessions', 'sentiment', 'volProfile', 'mstructure', 'onchain', 'tick'];
  let allContent = '';
  for (const type of types) {
    const url = `/data/${symbol}/${type}`;
    try {
      const response = await fetch(url);
      if (response.ok) {
        const text = await response.text();
        allContent += `========== ${type.toUpperCase()} ==========\\n${text}\\n\\n`;
      } else {
        allContent += `========== ${type.toUpperCase()} ==========\\nFile not available\\n\\n`;
      }
    } catch(e) {
      allContent += `========== ${type.toUpperCase()} ==========\\nError fetching\\n\\n`;
    }
  }
  if (allContent) {
    await navigator.clipboard.writeText(allContent);
    alert(`✅ Master copy for ${symbol} copied to clipboard!`);
  } else {
    alert(`❌ No data available for ${symbol}`);
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

// Update button percentages based on fill status (existing modules) and file checks for new modules
async function updatePercentages(symbol, card) {
  try {
    // First get fill_status for existing modules
    const statusResp = await fetch(`/fill_status?symbol=${encodeURIComponent(symbol)}`);
    const status = await statusResp.json();
    const existingTypes = ['cvd', 'correlation', 'derivative', 'depth', 'candles', 'liquidations'];
    for (const type of existingTypes) {
      const btn = card.querySelector(`.data-btn.${type}`);
      if (btn) {
        let percent = 0;
        if (type === 'liquidations') {
          if (status['derivative'] === true) percent = 100;
        } else {
          if (status[type] === true) percent = 100;
        }
        const label = type === 'liquidations' ? 'LIQ' : type.toUpperCase();
        btn.innerHTML = `${label} <span class='btn-percent'>${percent}%</span>`;
      }
    }
    // For new modules, check file existence via /check_file endpoint
    const newTypes = ['macro', 'sessions', 'sentiment', 'volProfile', 'mstructure', 'onchain', 'tick'];
    for (const type of newTypes) {
      const btn = card.querySelector(`.data-btn.${type}`);
      if (btn) {
        const checkResp = await fetch(`/check_file?symbol=${encodeURIComponent(symbol)}&type=${type}`);
        const check = await checkResp.json();
        const exists = check.exists === true;
        btn.classList.toggle('available', exists);
        const percent = exists ? 100 : 0;
        const label = type === 'volProfile' ? 'VOLP' : type.toUpperCase();
        btn.innerHTML = `${label} <span class='btn-percent'>${percent}%</span>`;
      }
    }
  } catch(e) {
    console.warn("Failed to fetch file status:", e);
  }
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
    
    var statusPlaceholder = `<span class='status-light' style='background:#FF6352;' onclick="fillSymbol('${escapeHtml(pairName)}')"></span>`;
    
    var header = document.createElement('div');
    header.className = 'card-header';
    header.innerHTML = `
      <span class='drag-handle'>${dragHandleHtml}</span>
      ${statusPlaceholder}
      <span class='pair-name'>${escapeHtml(pairName)}</span>
      <span class='score-badge'>${scoreHtml}</span>
      <button class='go-btn' onclick="doGO('${_mkt}','${escapeHtml(pairName)}','${_tf}')">→</button>
    `;
    
    // First action row: existing modules
    var actionRow1 = document.createElement('div');
    actionRow1.className = 'action-row';
    actionRow1.innerHTML = `
      <button class='data-btn cvd' onclick="copyData('${escapeHtml(pairName)}','cvd')">CVD <span class='btn-percent'>0%</span></button>
      <button class='data-btn correlation' onclick="copyData('${escapeHtml(pairName)}','correlation')">CORR <span class='btn-percent'>0%</span></button>
      <button class='data-btn derivative' onclick="copyData('${escapeHtml(pairName)}','derivative')">DERIV <span class='btn-percent'>0%</span></button>
      <button class='data-btn liquidations' onclick="copyData('${escapeHtml(pairName)}','liquidations')">LIQ <span class='btn-percent'>0%</span></button>
      <button class='data-btn depth' onclick="copyData('${escapeHtml(pairName)}','depth')">DEPTH <span class='btn-percent'>0%</span></button>
      <button class='data-btn candles' onclick="copyData('${escapeHtml(pairName)}','candles')">CANDLES <span class='btn-percent'>0%</span></button>
      <button class='data-btn master' onclick="copyAllData('${escapeHtml(pairName)}')" style="background:#2a0a2a; border-color:#FF6600; color:#FF6600;">MASTER</button>
    `;
    
    // Second action row: new modules (macro, sessions, sentiment, volProfile, mstructure, onchain, tick)
    var actionRow2 = document.createElement('div');
    actionRow2.className = 'action-row';
    actionRow2.innerHTML = `
      <button class='data-btn macro' onclick="copyData('${escapeHtml(pairName)}','macro')">MACRO <span class='btn-percent'>0%</span></button>
      <button class='data-btn sessions' onclick="copyData('${escapeHtml(pairName)}','sessions')">SESS <span class='btn-percent'>0%</span></button>
      <button class='data-btn sentiment' onclick="copyData('${escapeHtml(pairName)}','sentiment')">SENT <span class='btn-percent'>0%</span></button>
      <button class='data-btn volProfile' onclick="copyData('${escapeHtml(pairName)}','volProfile')">VOLP <span class='btn-percent'>0%</span></button>
      <button class='data-btn mstructure' onclick="copyData('${escapeHtml(pairName)}','mstructure')">MSTR <span class='btn-percent'>0%</span></button>
      <button class='data-btn onchain' onclick="copyData('${escapeHtml(pairName)}','onchain')">ONCH <span class='btn-percent'>0%</span></button>
      <button class='data-btn tick' onclick="copyData('${escapeHtml(pairName)}','tick')">TICK <span class='btn-percent'>0%</span></button>
    `;
    
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
    card.appendChild(actionRow1);
    card.appendChild(actionRow2);
    card.appendChild(body);
    container.appendChild(card);
    
    updatePercentages(pairName, card);
  }
  return container;
}

// Refresh cards using /refresh endpoint and update percentages
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
      
      var scoreSpan = card.querySelector('.score-badge');
      if (scoreSpan) {
        var score = data.score;
        if (score === "NA") {
          scoreSpan.innerHTML = "<span class='score-low'>NA</span>";
        } else {
          var cls = (score >= 65) ? "score-high" : (score >= 40) ? "score-mid" : "score-low";
          scoreSpan.innerHTML = `<span class='${cls}'>${score}%</span>`;
        }
      }
      
      var trendDiv = card.querySelector('.trend');
      if (trendDiv) {
        var trend = data.trend || "FLAT";
        var trendIcon = {"UP":"▲","DOWN":"▼","RANGING":"↔","FLAT":"—"}[trend] || "—";
        trendDiv.innerHTML = `${trendIcon} ${trend}`;
      }
      
      var srDiv = card.querySelector('.sr');
      if (srDiv) srDiv.innerHTML = data.sr_position || "—";
      
      var signalDiv = card.querySelector('.signal');
      if (signalDiv) {
        var signal = data.signal || "WAIT";
        var signalColor = {"STRONG BUY":"#9AFFAB","BUY":"#9AFFAB","SELL":"#FF6352","STRONG SELL":"#FF6352","WAIT":"#FCDB66"}[signal] || "#FCDB66";
        signalDiv.innerHTML = `<span style="color:${signalColor};">${signal}</span>`;
      }
      
      var qualityDiv = card.querySelector('.quality');
      if (qualityDiv) {
        var quality = data.quality || "LOW";
        var qc = {"HIGH":"#9AFFAB","MED":"#FCDB66","LOW":"#FF6352"}[quality] || "#888";
        qualityDiv.innerHTML = `<span style="font-size:9px;padding:1px 4px;border-radius:3px;background:${qc}22;border:1px solid ${qc}66;color:${qc};">${quality}</span>`;
      }
      
      var reasonDiv = card.querySelector('.reason');
      if (reasonDiv) {
        var reason = data.reason || "";
        reasonDiv.innerText = reason;
        reasonDiv.setAttribute('title', reason);
      }
      
      var feelPct = data.feel_pct || 0;
      var feelBarDiv = card.querySelector('.feel-bar');
      if (feelBarDiv) {
        var steps = Math.round(feelPct / 5);
        var color = feelPct >= 40 ? '#9AFFAB' : (feelPct >= 5 ? '#FCDB66' : '#FF6352');
        var blocks = "";
        for (var j = 0; j < 20; j++) {
          var bg = j < steps ? color : '#2a2a2a';
          blocks += `<div style='width:4px;height:10px;background:${bg};border-radius:0;margin-right:1px;display:inline-block;'></div>`;
        }
        feelBarDiv.innerHTML = `<div style='display:flex;flex-direction:column;align-items:center;gap:2px;'>
          <div style='display:flex;align-items:center;' class='feel-blocks'>${blocks}</div>
          <span class='feel-pct' style='font-size:9px;color:${color};'>${feelPct}%</span>
        </div>`;
      }
      
      var statusSpan = card.querySelector('.status-light');
      if (statusSpan) {
        var bgColor = feelPct == 0 ? '#FF6352' : (feelPct < 100 ? '#FCDB66' : '#9AFFAB');
        statusSpan.style.background = bgColor;
      }
      
      updatePercentages(pair, card);
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
  document.getElementById('detail-body').innerHTML = "<div style='text-align:center;padding:20px;color:#9AFFAB;'><span class='spinner'></span> Running A-layer…</div>";
  document.getElementById('detail-panel').classList.add('open');
  fetch('/go?market=' + encodeURIComponent(market) + '&pair=' + encodeURIComponent(pair) + '&tf=' + encodeURIComponent(tf))
    .then(r => r.text())
    .then(html => document.getElementById('detail-body').innerHTML = html)
    .catch(e => document.getElementById('detail-body').innerHTML = "<p class='err-msg'>GO error: " + e + "</p>");
}

function doDeep(market, pair, tf, aResultJson) {
  document.getElementById('detail-body').innerHTML += "<div style='text-align:center;padding:12px;color:#9AFFAB;'><span class='spinner'></span> Running D-layer…</div>";
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

function fillSymbol(pair) {
  var card = document.querySelector('.pair-card[data-pair="' + pair.replace(/"/g, '&quot;') + '"]');
  if (!card) return;
  var statusSpan = card.querySelector('.status-light');
  if (statusSpan) {
    statusSpan.style.background = '#FCDB66';
    statusSpan.style.cursor = 'wait';
  }
  
  var btnTypes = ['cvd', 'correlation', 'derivative', 'depth', 'candles', 'liquidations', 
                  'macro', 'sessions', 'sentiment', 'volProfile', 'mstructure', 'onchain', 'tick', 'master'];
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
                  } else if (comp === 'master') {
                    btn.style.opacity = '1';
                  } else if (comp === 'macro' || comp === 'sessions' || comp === 'sentiment' ||
                             comp === 'volProfile' || comp === 'mstructure' || comp === 'onchain' || comp === 'tick') {
                    // For new modules, check file existence via /check_file
                    fetch(`/check_file?symbol=${encodeURIComponent(pair)}&type=${comp}`)
                      .then(r => r.json())
                      .then(check => {
                        if (check.exists) {
                          btn.classList.add('available');
                          btn.style.opacity = '1';
                        } else {
                          allDone = false;
                        }
                      });
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
              updatePercentages(pair, card);
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
        if (statusSpan) statusSpan.style.background = '#FF6352';
      }
    })
    .catch(e => {
      alert('Error: ' + e);
      if (statusSpan) statusSpan.style.background = '#FF6352';
    });
}

document.getElementById('mkt-sel').addEventListener('change', function() {
  var mktTf = {'Binary OTC':'1m','CFD':'5m','Spot':'5m','Future':'5m'};
  document.getElementById('tf-sel').value = mktTf[this.value] || '5m';
});
</script>
</body>
</html>"""

print("✅ [config] Loaded OK - KALI PORT with #9AFFAB green, Master Copy button added")