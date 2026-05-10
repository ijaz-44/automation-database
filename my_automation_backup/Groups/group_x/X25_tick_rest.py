# Groups/group_x/X25_tick_rest.py
"""
X25 - Tick Data Module (TOON format, REST, atomic rename, 1H ready with speed & whale metrics)
- Fetches last 1000 trades from Binance public endpoint
- Computes buy_volume, sell_volume, net_delta (total)
- Computes speed (trades/sec), whale delta (large orders), micro‑burst delta (last 100 trades)
- Saves summary + full trade list to: {symbol}_tick.toon
- Logs to: tick_issues.log (append mode)
"""

import os
import time
import requests
from datetime import datetime

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SYMBOLS_DIR = os.path.join(BASE_DIR, "market_data", "binance", "symbols")
os.makedirs(SYMBOLS_DIR, exist_ok=True)

LOG_FILE = os.path.join(SYMBOLS_DIR, "tick_issues.log")

def log(msg, level="INFO"):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"{timestamp} [{level}] {msg}"
    print(line)
    try:
        with open(LOG_FILE, 'a', encoding='utf-8') as f:
            f.write(line + "\n")
    except:
        pass

def atomic_write(path, content):
    """Write content to .tmp then rename atomically."""
    tmp = path + ".tmp"
    try:
        with open(tmp, 'w', encoding='utf-8') as f:
            f.write(content)
            f.flush()
            os.fsync(f.fileno())
        os.rename(tmp, path)
        log(f"[ATOMIC] OK -> {os.path.basename(path)}")
        return True
    except Exception as e:
        if os.path.exists(tmp):
            os.remove(tmp)
        log(f"[ATOMIC] FAIL: {e}", "ERROR")
        return False

def fetch_trades(symbol, limit=1000):
    """Fetch recent trades from Binance public endpoint (max 1000)."""
    url = "https://api.binance.com/api/v3/trades"
    params = {"symbol": symbol.upper(), "limit": min(limit, 1000)}
    try:
        r = requests.get(url, params=params, timeout=10)
        if r.status_code == 200:
            trades = r.json()
            log(f"[BINANCE] Fetched {len(trades)} trades for {symbol}")
            return trades
        else:
            log(f"[BINANCE] HTTP {r.status_code}", "WARNING")
            return None
    except Exception as e:
        log(f"[BINANCE] Error: {e}", "ERROR")
        return None

def collect_and_save(symbol):
    """Main entry point. Fetches 1000 trades, computes all metrics, saves TOON."""
    log(f"[COLLECT] Starting for {symbol} (TOON format, 1H ready with speed/whale)")
    start_time = time.time()

    trades = fetch_trades(symbol, limit=1000)
    if not trades:
        log(f"[COLLECT] No trades data for {symbol}", "WARNING")
        return False

    # ----- Basic totals -----
    buy_vol = 0.0
    sell_vol = 0.0
    for t in trades:
        qty = float(t['qty'])
        if t['isBuyerMaker']:   # True = sell (aggressive seller)
            sell_vol += qty
        else:                   # False = buy (aggressive buyer)
            buy_vol += qty
    net_delta = buy_vol - sell_vol

    # ----- Speed (trades per second) -----
    first_ts = int(trades[0]['time'])
    last_ts = int(trades[-1]['time'])
    time_span_sec = (last_ts - first_ts) / 1000.0
    if time_span_sec > 0:
        speed_score = len(trades) / time_span_sec
    else:
        speed_score = 0.0

    # ----- Whale detection (large trades ≥ 5× avg trade size) -----
    total_qty = sum(float(t['qty']) for t in trades)
    avg_trade_size = total_qty / len(trades) if trades else 0
    whale_threshold = avg_trade_size * 5.0
    whale_buy_vol = 0.0
    whale_sell_vol = 0.0
    for t in trades:
        qty = float(t['qty'])
        if qty >= whale_threshold:
            if t['isBuyerMaker']:
                whale_sell_vol += qty
            else:
                whale_buy_vol += qty
    whale_delta = whale_buy_vol - whale_sell_vol

    # ----- Micro‑burst delta (last 100 trades) -----
    last_100 = trades[-100:] if len(trades) >= 100 else trades
    buy_last100 = 0.0
    sell_last100 = 0.0
    for t in last_100:
        qty = float(t['qty'])
        if t['isBuyerMaker']:
            sell_last100 += qty
        else:
            buy_last100 += qty
    delta_last100 = buy_last100 - sell_last100

    # ----- Build TOON content -----
    lines = []
    lines.append(f"# Tick trades for {symbol.upper()} – TOON format (1H ready)")
    lines.append(f"generated: {datetime.now().isoformat()}")
    lines.append(f"symbol: {symbol}")
    lines.append("")
    # Summary metrics
    lines.append(f"buy_volume: {buy_vol:.4f}")
    lines.append(f"sell_volume: {sell_vol:.4f}")
    lines.append(f"net_delta: {net_delta:.4f}")
    lines.append(f"speed_score: {speed_score:.2f} trades/sec")
    lines.append(f"avg_trade_size: {avg_trade_size:.4f}")
    lines.append(f"whale_threshold: {whale_threshold:.4f}")
    lines.append(f"whale_buy_volume: {whale_buy_vol:.4f}")
    lines.append(f"whale_sell_volume: {whale_sell_vol:.4f}")
    lines.append(f"whale_delta: {whale_delta:.4f}")
    lines.append(f"delta_last_100: {delta_last100:.4f}")
    lines.append("")

    fields = ["timestamp", "price", "quantity", "quoteQty", "isBuyerMaker"]
    rows = []
    for t in trades:
        row = f"{t['time']},{t['price']},{t['qty']},{t['quoteQty']},{t['isBuyerMaker']}"
        rows.append(row)

    lines.append(f"trades[{len(rows)}]{{{','.join(fields)}}}:")
    if rows:
        lines.append("  " + " |\n  ".join(rows))
    else:
        lines.append("  ")
    lines.append("")
    lines.append("# ========== END OF TOON DATA ==========")

    content = "\n".join(lines) + "\n"

    filepath = os.path.join(SYMBOLS_DIR, f"{symbol.lower()}_tick.toon")
    success = atomic_write(filepath, content)

    if success:
        elapsed = time.time() - start_time
        log(f"[SAVE_SUCCESS] {symbol} saved in {elapsed:.2f}s -> {os.path.basename(filepath)}"
            f" (speed={speed_score:.1f} tps, whale_delta={whale_delta:.2f}, micro_delta={delta_last100:.2f})")
        return True
    else:
        log(f"[SAVE_FAIL] {symbol} could not save file", "ERROR")
        return False

# Alias for sys_data import
tick_collect = collect_and_save

if __name__ == "__main__":
    tick_collect("BTCUSDT")