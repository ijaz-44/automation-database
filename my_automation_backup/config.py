# config.py – with GO button linking to full analysis page (no brain.py)
# NOTE: TOON, DB, JSON formats are removed. X‑buttons light up when .tmp_p file exists.

MARKET_TIMEFRAMES = {
    "Binary OTC": "1h",
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
    color: #00FFFF;
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

  /* ── Toggle bar & collapsible API panel ── */
  .toggle-bar {
    background: #0a0a0a;
    border-bottom: 1px solid #1e1e1e;
    text-align: center;
    cursor: pointer;
    padding: 2px 0 0 0;
    line-height: 12px;
    font-size: 12px;
    color: #9AFFAB;
    transition: background 0.2s;
  }
  .toggle-bar:hover {
    background: #1a1a1a;
  }
  .toggle-icon {
    display: inline-block;
    transition: transform 0.2s;
  }
  .toggle-icon.rotated {
    transform: rotate(180deg);
  }
  .api-panel {
    background: #111;
    border-bottom: 1px solid #333;
    overflow: hidden;
    transition: max-height 0.3s ease;
    max-height: 0;
    padding: 0 20px;
  }
  .api-panel.open {
    max-height: 300px;
    padding: 8px 20px;
  }
  .api-content {
    font-size: 10px;
    color: #aaa;
    display: flex;
    flex-direction: column;
    gap: 12px;
  }
  .api-stats {
    display: flex;
    flex-wrap: wrap;
    gap: 16px;
  }
  .api-stats span {
    color: #9AFFAB;
    font-weight: bold;
  }
  .api-total {
    color: #FCDB66;
  }
  /* Global buttons row */
  .global-buttons {
    display: flex;
    flex-wrap: wrap;
    gap: 12px;
    border-top: 1px solid rgba(255,255,255,0.1);
    padding-top: 8px;
    margin-top: 4px;
  }
  .global-btn {
    background: #1a1a1a;
    border: 1px solid #333;
    border-radius: 0;
    padding: 4px 10px;
    font-size: 10px;
    cursor: pointer;
    color: #ccc;
    transition: all 0.1s;
    display: inline-flex;
    align-items: center;
    gap: 6px;
  }
  .global-btn .label {
    font-weight: bold;
    color: #9AFFAB;
  }
  .global-btn .value {
    font-family: monospace;
    font-weight: bold;
  }
  .global-btn.yellow { background: #332d00; border-color: #FCDB66; color: #FCDB66; }
  .global-btn.red    { background: #330000; border-color: #FF6352; color: #FF6352; }
  .global-btn.green  { background: #003300; border-color: #9AFFAB; color: #9AFFAB; }
  .global-btn:hover { filter: brightness(1.2); border-color: #9AFFAB; }

  /* ── Status bar (remaining) ── */
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
  .call-light {
    width: 14px;
    height: 14px;
    display: inline-block;
    background: #FF6352;
    cursor: pointer;
    border-radius: 2px;
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

  /* Z‑group (8 buttons) */
  .z-group {
    display: flex;
    flex-wrap: wrap;
    gap: 6px;
    margin: 6px 0 4px 0;
    padding: 4px 0;
    border-top: 1px solid rgba(255,255,255,0.08);
    border-bottom: 1px solid rgba(255,255,255,0.08);
  }
  .z-button {
    display: inline-flex;
    flex-direction: column;
    align-items: center;
    background: rgba(0,0,0,0.3);
    padding: 2px 6px;
    border-radius: 0;
    font-size: 8px;
    min-width: 50px;
    cursor: pointer;
    transition: all 0.1s;
    color: #ddd;
    border: 1px solid #333;
  }
  .z-button .label {
    font-size: 6px;
    color: #aaa;
    text-transform: uppercase;
    margin-bottom: 1px;
  }
  .z-button .value {
    font-weight: bold;
    font-size: 9px;
  }
  .z-button.yellow { background: #332d00; border-color: #FCDB66; color: #FCDB66; }
  .z-button.red    { background: #330000; border-color: #FF6352; color: #FF6352; }
  .z-button:hover { filter: brightness(1.2); border-color: #9AFFAB; }

  /* Existing action rows */
  .action-buttons {
    display: flex;
    gap: 8px;
    margin-top: 6px;
    justify-content: flex-start;
    flex-wrap: wrap;
  }
  .action-row {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(80px, auto));
    gap: 6px;
    margin-bottom: 4px;
    align-items: center;
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
    white-space: nowrap;
    text-align: center;
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
  .master-btn {
    background: #3a1a1a;
    border-color: #cc6666;
    color: #ff8866;
  }
  .master-btn:hover {
    background: #4a2a2a;
  }

  /* Card body: flex wrap, feel bar inline (right side) - MED removed */
  .card-body {
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    gap: 12px;
    font-size: 10px;
    color: #aaa;
    margin-top: 4px;
  }
  .trend, .sr, .signal {
    display: inline-flex;
    align-items: center;
    gap: 4px;
  }
  .signal { font-weight: bold; }
  .feel-cell {
    display: inline-flex;
    align-items: center;
    margin-left: auto;
  }
  .feel-blocks {
    display: flex;
    gap: 1px;
  }
  .feel-pct {
    font-size: 9px;
    color: #888;
    margin-left: 4px;
  }
  .reason {
    width: 100%;
    margin-top: 4px;
    color: #888;
    max-width: none;
    overflow: visible;
    white-space: normal;
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

<!-- Toggle Bar -->
<div class="toggle-bar" onclick="toggleApiPanel()">
  <span class="toggle-icon" id="toggleIcon">^</span>
</div>

<!-- Collapsible API Panel -->
<div id="apiPanel" class="api-panel">
  <div class="api-content">
    <div id="call-stats" class="api-stats">Loading...</div>
    <div class="api-total" id="call-total"></div>
    <!-- Global buttons row (added CANDLES) -->
    <div class="global-buttons">
      <button id="global-ticker" class="global-btn"><span class="label">TICKER</span> <span class="value">--</span></button>
      <button id="global-news" class="global-btn"><span class="label">NEWS</span> <span class="value">--</span></button>
      <button id="global-macro" class="global-btn"><span class="label">MACRO</span> <span class="value">--</span></button>
      <button id="global-mood" class="global-btn"><span class="label">MARKET MOOD</span> <span class="value">--</span></button>
      <button id="global-candles" class="global-btn"><span class="label">CANDLES</span> <span class="value">--</span></button>
    </div>
  </div>
</div>

<div id='status-bar'>
  <span><span class='dot dot-red' id='ws-dot'></span><span id='ws-status'>Not connected</span></span>
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

// Copy data functions (unchanged)
async function copyData(symbol, type) {
  const url = `/data/${symbol}/${type}`;
  try {
    const response = await fetch(url);
    if (!response.ok) {
      alert(`Data not available for ${symbol} - ${type}`);
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

async function masterAnalysis(symbol) {
  const url = `/master?symbol=${encodeURIComponent(symbol)}`;
  try {
    const response = await fetch(url);
    const data = await response.json();
    if (data.error) {
      alert("Master analysis error: " + data.error);
      return;
    }
    await navigator.clipboard.writeText(data.analysis);
    alert(`✅ Master analysis for ${symbol} copied to clipboard!`);
  } catch (err) {
    console.error("Master analysis failed:", err);
    alert("Failed to get master analysis. Check console.");
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

// ================== Update percentages (uses .tmp_p completion status from backend) ==================
async function updatePercentages(symbol, card) {
  try {
    const statusResp = await fetch(`/fill_status?symbol=${encodeURIComponent(symbol)}`);
    const status = await statusResp.json();
    const allComponents = ['cvd', 'correlation', 'derivative', 'depth', 'candles', 'liquidations',
                           'macro', 'sessions', 'sentiment', 'volProfile', 'mstructure', 'onchain', 'tick'];
    for (const comp of allComponents) {
      const btn = card.querySelector(`.data-btn.${comp}`);
      if (btn) {
        let percent = 0;
        // FIX: liquidations button now uses status['liquidations'] directly
        if (status[comp] === true) percent = 100;
        let label = comp.toUpperCase();
        if (comp === 'liquidations') label = 'LIQ';
        if (comp === 'volProfile') label = 'VOLP';
        if (comp === 'mstructure') label = 'MSTR';
        if (comp === 'onchain') label = 'ONCH';
        btn.innerHTML = `${label} <span class='btn-percent'>${percent}%</span>`;
        if (percent === 100) {
          btn.classList.add('available');
          btn.style.opacity = '1';
        } else {
          btn.classList.remove('available');
          btn.style.opacity = '0.5';
        }
      }
    }
  } catch(e) {
    console.warn("Failed to fetch fill_status:", e);
  }
}

// ================== BUILD CARDS ==================
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
    var reasonHtml = cells[8].innerHTML;
    var feelHtml = cells[9].innerHTML;
    
    var card = document.createElement('div');
    card.className = 'pair-card';
    card.setAttribute('data-pair', pairName);
    
    var callPlaceholder = `<span class='call-light' style='background:#FF6352;' onclick="fillSymbol('${escapeHtml(pairName)}')"></span>`;
    
    var header = document.createElement('div');
    header.className = 'card-header';
    header.innerHTML = `
      <span class='drag-handle'>${dragHandleHtml}</span>
      ${callPlaceholder}
      <span class='pair-name'>${escapeHtml(pairName)}</span>
      <span class='score-badge'>${scoreHtml}</span>
      <button class='go-btn' onclick="window.open('/go_analysis/' + encodeURIComponent('${escapeHtml(pairName)}'), '_blank')">→</button>
    `;
    
    // Z‑group (8 buttons) – initial values set to "No data"
    var zGroup = document.createElement('div');
    zGroup.className = 'z-group';
    zGroup.innerHTML = `
      <div class='z-button' data-factor='sr'><span class='label'>S&R</span><span class='value'>No data</span></div>
      <div class='z-button' data-factor='regime'><span class='label'>REGIME</span><span class='value'>No data</span></div>
      <div class='z-button' data-factor='news'><span class='label'>NEWS</span><span class='value'>No data</span></div>
      <div class='z-button' data-factor='volatility'><span class='label'>VOL</span><span class='value'>No data</span></div>
      <div class='z-button' data-factor='correlation'><span class='label'>CORR</span><span class='value'>No data</span></div>
      <div class='z-button' data-factor='spread'><span class='label'>SPREAD</span><span class='value'>No data</span></div>
      <div class='z-button' data-factor='orderflow'><span class='label'>ORDERFLOW</span><span class='value'>No data</span></div>
      <div class='z-button' data-factor='adr'><span class='label'>ADR EXH</span><span class='value'>No data</span></div>
    `;
    
    // Row 1
    var actionRow1 = document.createElement('div');
    actionRow1.className = 'action-row';
    actionRow1.innerHTML = `
      <button class='data-btn cvd' onclick="copyData('${escapeHtml(pairName)}','cvd')">CVD <span class='btn-percent'>0%</span></button>
      <button class='data-btn correlation' onclick="copyData('${escapeHtml(pairName)}','correlation')">CORR <span class='btn-percent'>0%</span></button>
      <button class='data-btn derivative' onclick="copyData('${escapeHtml(pairName)}','derivative')">DERIV <span class='btn-percent'>0%</span></button>
      <button class='data-btn liquidations' onclick="copyData('${escapeHtml(pairName)}','liquidations')">LIQ <span class='btn-percent'>0%</span></button>
      <button class='data-btn sessions' onclick="copyData('${escapeHtml(pairName)}','sessions')">SESSIONS <span class='btn-percent'>0%</span></button>
      <button class='data-btn depth' onclick="copyData('${escapeHtml(pairName)}','depth')">DEPTH <span class='btn-percent'>0%</span></button>
      <button class='data-btn candles' onclick="copyData('${escapeHtml(pairName)}','candles')">CANDLES <span class='btn-percent'>0%</span></button>
      <button class='data-btn macro' onclick="copyData('${escapeHtml(pairName)}','macro')">MACRO <span class='btn-percent'>0%</span></button>
    `;
    
    // Row 2 – MASTER button now uses masterAnalysis
    var actionRow2 = document.createElement('div');
    actionRow2.className = 'action-row';
    actionRow2.innerHTML = `
      <button class='data-btn sentiment' onclick="copyData('${escapeHtml(pairName)}','sentiment')">SENTIMENT <span class='btn-percent'>0%</span></button>
      <button class='data-btn volProfile' onclick="copyData('${escapeHtml(pairName)}','volprofile')">VOLP <span class='btn-percent'>0%</span></button>
      <button class='data-btn mstructure' onclick="copyData('${escapeHtml(pairName)}','mstructure')">MSTR <span class='btn-percent'>0%</span></button>
      <button class='data-btn onchain' onclick="copyData('${escapeHtml(pairName)}','onchain')">ONCH <span class='btn-percent'>0%</span></button>
      <button class='data-btn tick' onclick="copyData('${escapeHtml(pairName)}','tick')">TICK <span class='btn-percent'>0%</span></button>
      <button class='data-btn master-btn' onclick="masterAnalysis('${escapeHtml(pairName)}')">MASTER</button>
    `;
    
    var body = document.createElement('div');
    body.className = 'card-body';
    body.innerHTML = `
      <div class='trend'>${trendHtml}</div>
      <div class='sr'>${srHtml}</div>
      <div class='signal'>${signalHtml}</div>
      <div class='feel-cell' style='min-width:100px;'>${feelHtml}</div>
      <div class='reason' title='${escapeHtml(reasonHtml.replace(/<[^>]*>/g, ''))}'>${reasonHtml}</div>
    `;
    
    card.appendChild(header);
    card.appendChild(zGroup);
    card.appendChild(actionRow1);
    card.appendChild(actionRow2);
    card.appendChild(body);
    container.appendChild(card);
    
    updatePercentages(pairName, card);
  }
  return container;
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

// ================== TOGGLE API PANEL ==================
function toggleApiPanel() {
  var panel = document.getElementById('apiPanel');
  var icon = document.getElementById('toggleIcon');
  panel.classList.toggle('open');
  icon.classList.toggle('rotated');
}

// ================== CALL STATS ==================
function updateCallStats() {
  fetch('/calls')
    .then(r => r.text())
    .then(html => {
      document.getElementById('call-stats').innerHTML = html;
      const totalSpan = document.querySelector('#call-stats .api-total');
      if (totalSpan) {
        document.getElementById('call-total').innerHTML = totalSpan.innerHTML;
      } else {
        let total = 0;
        document.querySelectorAll('#call-stats td:last-child').forEach(td => {
          let val = parseInt(td.innerText);
          if (!isNaN(val)) total += val;
        });
        document.getElementById('call-total').innerHTML = `Total: ${total}`;
      }
    })
    .catch(() => {
      document.getElementById('call-stats').innerHTML = 'Call stats unavailable';
    });
}

// ================== GLOBAL BUTTONS ==================
let globalUpdateInterval = null;

async function fetchGlobalTicker() {
  try {
    const resp = await fetch('https://api.binance.com/api/v3/ticker/24hr');
    if (!resp.ok) throw new Error('Ticker API error');
    const data = await resp.json();
    const btcItem = data.find(item => item.symbol === 'BTCUSDT');
    if (btcItem) {
      const change = parseFloat(btcItem.priceChangePercent).toFixed(2);
      const btn = document.getElementById('global-ticker');
      const valueSpan = btn.querySelector('.value');
      valueSpan.innerText = `${change}%`;
      if (change > 1) {
        btn.classList.add('green');
        btn.classList.remove('yellow', 'red');
      } else if (change < -1) {
        btn.classList.add('red');
        btn.classList.remove('green', 'yellow');
      } else {
        btn.classList.add('yellow');
        btn.classList.remove('green', 'red');
      }
    } else {
      document.getElementById('global-ticker').querySelector('.value').innerText = 'N/A';
    }
  } catch (e) {
    console.warn('Ticker fetch failed:', e);
    document.getElementById('global-ticker').querySelector('.value').innerText = 'ERR';
  }
}

async function fetchGlobalNews() {
  try {
    document.getElementById('global-news').querySelector('.value').innerText = 'Neutral';
    document.getElementById('global-news').classList.add('yellow');
  } catch (e) {
    document.getElementById('global-news').querySelector('.value').innerText = 'ERR';
  }
}

async function fetchGlobalMacro() {
  try {
    document.getElementById('global-macro').querySelector('.value').innerText = 'Normal';
    document.getElementById('global-macro').classList.add('yellow');
  } catch (e) {
    document.getElementById('global-macro').querySelector('.value').innerText = 'ERR';
  }
}

async function fetchMarketMood() {
  try {
    const resp = await fetch('https://api.binance.com/api/v3/ticker/24hr?symbol=BTCUSDT');
    if (!resp.ok) throw new Error();
    const data = await resp.json();
    const change = parseFloat(data.priceChangePercent);
    let mood = 'Neutral';
    let colorClass = 'yellow';
    if (change > 2) {
      mood = 'Bullish';
      colorClass = 'green';
    } else if (change < -2) {
      mood = 'Bearish';
      colorClass = 'red';
    }
    const btn = document.getElementById('global-mood');
    btn.querySelector('.value').innerText = mood;
    btn.classList.add(colorClass);
    btn.classList.remove(colorClass === 'green' ? 'yellow' : 'green');
  } catch (e) {
    document.getElementById('global-mood').querySelector('.value').innerText = 'ERR';
  }
}

// CANDLES button – shows "Active" (green) if any symbol has live WebSocket data
function fetchGlobalCandles() {
  const btn = document.getElementById('global-candles');
  const valueSpan = btn.querySelector('.value');
  let isActive = false;
  if (cachedData && Object.keys(cachedData).length > 0) {
    for (let sym in cachedData) {
      if (cachedData[sym].ws_alive === true) {
        isActive = true;
        break;
      }
    }
  }
  if (isActive) {
    valueSpan.innerText = 'Active';
    btn.classList.add('green');
    btn.classList.remove('red', 'yellow');
  } else {
    valueSpan.innerText = 'Inactive';
    btn.classList.add('red');
    btn.classList.remove('green', 'yellow');
  }
}

function updateGlobalButtons() {
  fetchGlobalTicker();
  fetchGlobalNews();
  fetchGlobalMacro();
  fetchMarketMood();
  fetchGlobalCandles();
}

function startGlobalUpdater() {
  if (globalUpdateInterval) clearInterval(globalUpdateInterval);
  updateGlobalButtons();
  globalUpdateInterval = setInterval(updateGlobalButtons, 60000);
}

// ================== FILL SYMBOL ==================
function fillSymbol(pair) {
  var card = document.querySelector('.pair-card[data-pair="' + pair.replace(/"/g, '&quot;') + '"]');
  if (!card) return;
  var callSpan = card.querySelector('.call-light');
  if (callSpan) {
    callSpan.style.background = '#FCDB66';
    callSpan.style.cursor = 'wait';
  }
  var allComponents = ['cvd', 'correlation', 'derivative', 'depth', 'candles', 'liquidations',
                       'macro', 'sessions', 'sentiment', 'volProfile', 'mstructure', 'onchain', 'tick', 'master'];
  for (var i = 0; i < allComponents.length; i++) {
    var btn = card.querySelector('.data-btn.' + allComponents[i]);
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
              for (var i = 0; i < allComponents.length; i++) {
                var comp = allComponents[i];
                var btn = card.querySelector('.data-btn.' + comp);
                if (btn) {
                  // FIX: liquidations now uses status['liquidations'] directly
                  if (comp === 'master') {
                    btn.style.opacity = '1';
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
              updatePercentages(pair, card);  // update percentages & class again
              if (allDone) {
                clearInterval(pollInterval);
                if (callSpan) {
                  callSpan.style.background = '#9AFFAB';
                  callSpan.style.cursor = 'pointer';
                  callSpan.title = 'Data filled via API';
                }
                sessionStorage.setItem('filled_' + pair, 'true');
                fetchDataFromBackend();
              }
            })
            .catch(e => console.warn("Status poll error", e));
        }, 1000);
      } else {
        alert('Fill failed: ' + (data.error || 'Unknown error'));
        if (callSpan) callSpan.style.background = '#FF6352';
      }
    })
    .catch(e => {
      alert('Error: ' + e);
      if (callSpan) callSpan.style.background = '#FF6352';
    });
}

function setZButtonColor(btn, value) {
  if (!btn) return;
  const val = (value || "").trim();
  if (val === "Error") {
    btn.classList.add('red');
    btn.classList.remove('yellow');
  } 
  else if (val !== "" && val !== "No data") {
    btn.classList.add('yellow');
    btn.classList.remove('red');
  } 
  else {
    btn.classList.remove('yellow', 'red');
  }
}

var cachedData = {};
var renderTimer = null;
var fetchTimer = null;
var backgroundFetchInterval = 3000;
var uiRenderInterval = 500;

function renderCardsFromCache() {
  if (!cachedData || Object.keys(cachedData).length === 0) return;
  const cards = document.querySelectorAll('.pair-card');
  for (let card of cards) {
    const pair = card.getAttribute('data-pair');
    const info = cachedData[pair];
    if (!info) continue;
    
    const scoreSpan = card.querySelector('.score-badge');
    if (scoreSpan) {
      let score = info.score;
      if (score === "NA" && info.preflight_score) score = info.preflight_score;
      if (score === "NA") scoreSpan.innerHTML = "<span class='score-low'>NA</span>";
      else {
        const cls = (score >= 65) ? "score-high" : (score >= 40) ? "score-mid" : "score-low";
        scoreSpan.innerHTML = `<span class='${cls}'>${score}%</span>`;
      }
    }
    const trendDiv = card.querySelector('.trend');
    if (trendDiv) {
      const trend = info.trend || "FLAT";
      const trendIcon = {"UP":"▲","DOWN":"▼","RANGING":"↔","FLAT":"—"}[trend] || "—";
      trendDiv.innerHTML = `${trendIcon} ${trend}`;
    }
    const srDiv = card.querySelector('.sr');
    if (srDiv) srDiv.innerHTML = info.sr_position || "—";
    const signalDiv = card.querySelector('.signal');
    if (signalDiv) {
      const signal = info.signal || "WAIT";
      const signalColor = {"STRONG BUY":"#9AFFAB","BUY":"#9AFFAB","SELL":"#FF6352","STRONG SELL":"#FF6352","WAIT":"#FCDB66"}[signal] || "#FCDB66";
      signalDiv.innerHTML = `<span style="color:${signalColor};">${signal}</span>`;
    }
    const reasonDiv = card.querySelector('.reason');
    if (reasonDiv) {
      const reason = info.reason || "";
      reasonDiv.innerText = reason;
      reasonDiv.setAttribute('title', reason);
    }
    
    const feelPct = info.feel_pct || 0;
    const getColor = (pct) => {
      if (pct <= 50) {
        const t = pct / 50;
        const r = 0xFF - Math.round(0x66 * t);
        const g = 0x66 + Math.round(0x77 * t);
        const b = 0x00 + Math.round(0x66 * t);
        return `rgb(${r}, ${g}, ${b})`;
      } else {
        const t = (pct - 50) / 50;
        const r = 0x99 - Math.round(0x33 * t);
        const g = 0xCC + Math.round(0x33 * t);
        const b = 0x66 + Math.round(0x55 * t);
        return `rgb(${r}, ${g}, ${b})`;
      }
    };
    let blocks = '';
    for (let i = 0; i < 20; i++) {
      const blockPct = (i + 1) * 5;
      let bg;
      if (blockPct <= feelPct) {
        bg = getColor(blockPct);
      } else {
        bg = '#2a2a2a';
      }
      blocks += `<div style='width:4px;height:10px;background:${bg};border-radius:1px;margin-right:1px;display:inline-block;'></div>`;
    }
    const feelCell = card.querySelector('.feel-cell');
    if (feelCell) {
      feelCell.innerHTML = `<div style='display:inline-flex;align-items:center;gap:2px;'>
        <div style='display:flex;align-items:center;' class='feel-blocks'>${blocks}</div>
        <span class='feel-pct' style='font-size:9px;color:#888;margin-left:4px;'>${feelPct}%</span>
      </div>`;
    }
    
    const callSpan = card.querySelector('.call-light');
    if (callSpan) {
      const filledFlag = sessionStorage.getItem('filled_' + pair);
      if (filledFlag === 'true') {
        callSpan.style.background = '#9AFFAB';
        callSpan.title = 'Data filled via API';
      } else {
        const wsActive = (info.ws_alive === true);
        if (wsActive) {
          callSpan.style.background = '#FCDB66';
          callSpan.title = 'Live WebSocket data';
        } else {
          callSpan.style.background = '#FF6352';
          callSpan.title = 'No data (click to fill)';
        }
      }
    }
    
    const zButtons = card.querySelectorAll('.z-button');
    if (zButtons.length === 8 && info.statuses) {
      const statuses = info.statuses;
      const factors = ['sr', 'trend', 'news', 'volatility', 'correlation', 'spread', 'orderflow', 'adr'];
      for (let i = 0; i < factors.length; i++) {
        const btn = zButtons[i];
        const factor = factors[i];
        let rawValue = statuses[factor] || "";
        let displayValue = (rawValue === "" || rawValue === undefined) ? "No data" : rawValue;
        
        if (factor === 'news') {
          let newsWord = info.news_word || "";
          let newsSentiment = info.news_sentiment || 0;
          let newsDisplay = "";
          if (newsWord === "Error") {
            newsDisplay = "Error";
          } else if (newsWord === "No news") {
            newsDisplay = "No news";
          } else if (newsWord === "Bullish" || newsWord === "Bearish") {
            let sign = newsSentiment > 0 ? '+' : (newsSentiment < 0 ? '-' : '');
            let score = Math.abs(newsSentiment).toFixed(1);
            newsDisplay = `${newsWord} ${sign}${score}`;
          } else if (newsWord && newsWord !== "") {
            newsDisplay = newsWord;
          } else {
            newsDisplay = "No data";
          }
          const valueSpan = btn.querySelector('.value');
          if (valueSpan) valueSpan.innerText = newsDisplay;
          if (newsDisplay === "Error") {
            btn.classList.add('red');
            btn.classList.remove('yellow');
          } else if (newsDisplay === "No news" || (newsDisplay !== "No data" && newsDisplay !== "Error")) {
            btn.classList.add('yellow');
            btn.classList.remove('red');
          } else {
            btn.classList.remove('yellow', 'red');
          }
        } else {
          const valueSpan = btn.querySelector('.value');
          if (valueSpan) valueSpan.innerText = displayValue;
          setZButtonColor(btn, displayValue);
        }
      }
    }
  }
  document.getElementById('pair-count').innerHTML = `${Object.keys(cachedData).length} pairs active`;
}

async function fetchDataFromBackend() {
  try {
    const resp = await fetch('/refresh');
    const newData = await resp.json();
    cachedData = newData;
  } catch(e) {
    console.warn("Background fetch error:", e);
  }
}

function startBackgroundFetcher() {
  if (fetchTimer) clearInterval(fetchTimer);
  fetchTimer = setInterval(fetchDataFromBackend, backgroundFetchInterval);
  fetchDataFromBackend();
}

function startUIRenderer() {
  if (renderTimer) clearInterval(renderTimer);
  renderTimer = setInterval(renderCardsFromCache, uiRenderInterval);
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
        startBackgroundFetcher();
        startUIRenderer();
        startGlobalUpdater();
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

document.getElementById('mkt-sel').addEventListener('change', function() {
  var mktTf = {'Binary OTC':'1m','CFD':'5m','Spot':'5m','Future':'5m'};
  document.getElementById('tf-sel').value = mktTf[this.value] || '5m';
});
</script>
</body>
</html>"""