# Groups/group_x/X03_cvd_rest.py
"""
CVD Module – 50k trades processed, only TOON output, dynamic thresholds, issue logging.
- Fetches 50k most recent trades.
- Stores aggregated metrics: footprint (top 800), signals, events, summary.
- Automatically adapts thresholds to any coin's price and volume.
- Logs warnings/errors to {symbol}_cvd_issues.log.
"""

import requests
import time
import os
import math
from collections import defaultdict, deque

BASE_URL = "https://api.binance.com/api/v3"

MAX_TRADES_FETCH = 50000
MAX_HISTORY_LEN = 100
ABSORPTION_WINDOW = 30
MAX_FOOTPRINT_LEVELS = 800        # top price clusters
RATE_LIMIT_SEC = 0.2

# Default tick size per coin (optional, can be fetched dynamically)
TICK_SIZE = {"BTCUSDT": 0.5, "ETHUSDT": 0.1, "BNBUSDT": 0.1, "SOLUSDT": 0.01}
DEFAULT_TICK = 0.01

def get_tick_size(symbol):
    for key in TICK_SIZE:
        if symbol.upper().startswith(key.replace("USDT","")):
            return TICK_SIZE[key]
    return DEFAULT_TICK

def get_data_dir():
    base = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    data_dir = os.path.join(base, "market_data", "binance", "symbols")
    os.makedirs(data_dir, exist_ok=True)
    return data_dir

# ---------- LOGGING ----------
def log_issue(symbol, issue_type, message, level="WARNING"):
    data_dir = get_data_dir()
    log_file = os.path.join(data_dir, f"{symbol.lower()}_cvd_issues.log")
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    with open(log_file, 'a', encoding='utf-8') as f:
        f.write(f"{timestamp} [{level}] [{issue_type}] {message}\n")
    print(f"[X03_cvd] {level}: {message}")

# ---------- API FETCH ----------
_last_call_time = 0
def rate_limited_fetch(url, params=None):
    global _last_call_time
    now = time.time()
    if now - _last_call_time < RATE_LIMIT_SEC:
        time.sleep(RATE_LIMIT_SEC - (now - _last_call_time))
    _last_call_time = time.time()
    try:
        r = requests.get(url, params=params, timeout=10)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        log_issue("unknown", "NETWORK_ERROR", f"Fetch failed: {e}", "ERROR")
        return None

def fetch_agg_trades(symbol, limit=1000, from_id=None):
    params = {"symbol": symbol.upper(), "limit": limit}
    if from_id is not None:
        params["fromId"] = from_id
    data = rate_limited_fetch(f"{BASE_URL}/aggTrades", params)
    if not data:
        return []
    trades = []
    for t in data:
        trades.append({
            'id': t['a'],
            'price': float(t['p']),
            'qty': float(t['q']),
            'is_sell': t['m'],
            'timestamp': t['T']
        })
    return trades

def fetch_initial_trades(symbol):
    latest = fetch_agg_trades(symbol, limit=1)
    if not latest:
        log_issue(symbol, "NO_TRADES", "No trades fetched at all")
        return []
    target_id = latest[0]['id']
    print(f"[X03_cvd] Backfilling from ID {target_id}")
    all_trades = []
    while len(all_trades) < MAX_TRADES_FETCH:
        fetch_id = max(1, target_id - 1000)
        trades = fetch_agg_trades(symbol, limit=1000, from_id=fetch_id)
        if not trades:
            break
        valid_batch = [t for t in trades if t['id'] <= target_id]
        if not valid_batch:
            break
        all_trades = valid_batch + all_trades
        target_id = valid_batch[0]['id'] - 1
        if len(all_trades) % 5000 == 0:
            print(f"[X03_cvd] Progress: {len(all_trades)}/{MAX_TRADES_FETCH}")
        if target_id < 1:
            break
    if len(all_trades) > MAX_TRADES_FETCH:
        all_trades = all_trades[-MAX_TRADES_FETCH:]
    print(f"[X03_cvd] Fetched {len(all_trades)} trades")
    if not all_trades:
        log_issue(symbol, "NO_TRADES", "No trades after backfill")
    return all_trades

# ---------- METRICS UPDATE WITH DYNAMIC THRESHOLDS ----------
def update_metrics(trades, symbol, tick_size):
    clusters = defaultdict(lambda: {'buy': 0.0, 'sell': 0.0})
    cvd_history = deque(maxlen=MAX_HISTORY_LEN)
    price_history = deque(maxlen=MAX_HISTORY_LEN)
    total_trade_count = 0
    cumulative_cvd = 0.0
    imbalance_events = []
    absorption_events = []

    # First pass: build clusters and gather basic stats
    total_notional = 0.0
    total_volume = 0.0
    for t in trades:
        price = round(t['price'] / tick_size) * tick_size
        qty = t['qty']
        is_sell = t['is_sell']
        total_notional += price * qty
        total_volume += qty
        if is_sell:
            cumulative_cvd -= qty
            clusters[price]['sell'] += qty
        else:
            cumulative_cvd += qty
            clusters[price]['buy'] += qty
        cvd_history.append(cumulative_cvd)
        price_history.append(price)
        total_trade_count += 1

    if total_trade_count == 0:
        log_issue(symbol, "NO_TRADES", "Zero trades in update_metrics")
        return {
            'clusters': {},
            'cvd_history': [],
            'price_history': [],
            'total_trade_count': 0,
            'cumulative_cvd': 0.0,
            'imbalance_events': [],
            'absorption_events': []
        }

    avg_trade_qty = total_volume / total_trade_count
    avg_price = total_notional / total_volume if total_volume > 0 else 0

    # Dynamic thresholds (ensuring they are never too small or too large)
    # For extremely cheap coins, avg_trade_qty may be huge; we cap at 100000 to avoid insane numbers.
    # For expensive coins, avg_trade_qty may be tiny; we floor at 0.1.
    safe_avg_qty = max(0.1, min(100000, avg_trade_qty))
    min_vol_threshold = max(10.0, safe_avg_qty * 5)
    min_diff_threshold = max(5.0, safe_avg_qty * 2.5)
    # Also require minimum notional (price * volume) to filter dust
    min_notional = max(50.0, avg_price * 5)  # at least $5 worth

    # Imbalance events
    last_imbalance_price = None
    for price, vols in clusters.items():
        b = vols['buy']
        s = vols['sell']
        total = b + s
        notional = price * total
        if total > min_vol_threshold and notional > min_notional:
            if s > 0 and b / s >= 2.5:
                diff = b - s
                if diff > min_diff_threshold and price != last_imbalance_price:
                    imbalance_events.append({
                        'timestamp': int(time.time()*1000),
                        'price': price,
                        'ratio': round(b / s, 2),
                        'type': 'buy',
                        'volume': round(total, 2),
                        'diff': round(diff, 2)
                    })
                    last_imbalance_price = price
            elif b > 0 and s / b >= 2.5:
                diff = s - b
                if diff > min_diff_threshold and price != last_imbalance_price:
                    imbalance_events.append({
                        'timestamp': int(time.time()*1000),
                        'price': price,
                        'ratio': round(s / b, 2),
                        'type': 'sell',
                        'volume': round(total, 2),
                        'diff': round(diff, 2)
                    })
                    last_imbalance_price = price
    imbalance_events = imbalance_events[-20:]

    # Absorption events
    if len(cvd_history) >= ABSORPTION_WINDOW:
        start = max(ABSORPTION_WINDOW, len(cvd_history)-200)
        new_abs = []
        for i in range(start, len(cvd_history)):
            if i < ABSORPTION_WINDOW:
                continue
            window_cvd = list(cvd_history)[i-ABSORPTION_WINDOW:i+1]
            window_price = list(price_history)[i-ABSORPTION_WINDOW:i+1]
            cvd_delta = cvd_history[i] - cvd_history[i-ABSORPTION_WINDOW]
            price_delta = price_history[i] - price_history[i-ABSORPTION_WINDOW]
            mean = sum(window_cvd) / len(window_cvd)
            variance = sum((x-mean)**2 for x in window_cvd) / len(window_cvd)
            std = math.sqrt(variance) if variance > 0 else 1e-9
            threshold = std * 2
            if abs(cvd_delta) > threshold:
                if price_delta <= 0 and cvd_delta > 0:
                    new_abs.append({
                        'timestamp': int(time.time()*1000),
                        'price': price_history[i],
                        'cvd_delta': round(cvd_delta, 2),
                        'price_delta': round(price_delta, 6),
                        'type': 'bullish'
                    })
                elif price_delta >= 0 and cvd_delta < 0:
                    new_abs.append({
                        'timestamp': int(time.time()*1000),
                        'price': price_history[i],
                        'cvd_delta': round(cvd_delta, 2),
                        'price_delta': round(price_delta, 6),
                        'type': 'bearish'
                    })
        absorption_events = (absorption_events + new_abs)[-15:]

    # Log if no imbalance events (but only once per run)
    if not imbalance_events:
        log_issue(symbol, "NO_IMBALANCE", f"No imbalance events: thresholds vol={min_vol_threshold:.1f}, diff={min_diff_threshold:.1f}, notional={min_notional:.1f}", "INFO")

    return {
        'clusters': clusters,
        'cvd_history': list(cvd_history),
        'price_history': list(price_history),
        'total_trade_count': total_trade_count,
        'cumulative_cvd': cumulative_cvd,
        'imbalance_events': imbalance_events,
        'absorption_events': absorption_events
    }

def compute_signals(state):
    cvd_hist = state['cvd_history']
    price_hist = state['price_history']
    if len(cvd_hist) < 30:
        return {'direction': 'neutral', 'confidence': 0, 'cvd_slope_10': 0, 'cvd_acceleration': 0, 'divergence': 'none', 'absorption_net': 0, 'imbalance_score': 0}

    cvd_slope_10 = cvd_hist[-1] - cvd_hist[-11] if len(cvd_hist) >= 11 else 0
    cvd_acc = (cvd_hist[-1] - cvd_hist[-6]) - (cvd_hist[-6] - cvd_hist[-11]) if len(cvd_hist) >= 11 else 0
    price_slope_10 = price_hist[-1] - price_hist[-11] if len(price_hist) >= 11 else 0
    divergence = 'none'
    if price_slope_10 > 0 and cvd_slope_10 < 0:
        divergence = 'bearish'
    elif price_slope_10 < 0 and cvd_slope_10 > 0:
        divergence = 'bullish'

    abs_events = state['absorption_events']
    bullish_abs = sum(1 for e in abs_events if e['type'] == 'bullish')
    bearish_abs = sum(1 for e in abs_events if e['type'] == 'bearish')
    net_abs = bullish_abs - bearish_abs

    imb_events = state['imbalance_events'][-10:]
    imbalance_score = sum((e['ratio'] - 2.5) * e['volume'] for e in imb_events if e['type'] == 'buy')
    imbalance_score -= sum((e['ratio'] - 2.5) * e['volume'] for e in imb_events if e['type'] == 'sell')

    direction = 'neutral'
    confidence = 50
    if divergence == 'bullish' and cvd_slope_10 > 0:
        direction = 'up'
        confidence = 70 + min(20, abs(cvd_slope_10)/100)
    elif divergence == 'bearish' and cvd_slope_10 < 0:
        direction = 'down'
        confidence = 70 + min(20, abs(cvd_slope_10)/100)
    elif net_abs > 2 and cvd_slope_10 > 0:
        direction = 'up'
        confidence = 65 + net_abs * 5
    elif net_abs < -2 and cvd_slope_10 < 0:
        direction = 'down'
        confidence = 65 + abs(net_abs) * 5
    elif imbalance_score > 100:
        direction = 'up'
        confidence = 60 + min(30, imbalance_score/100)
    elif imbalance_score < -100:
        direction = 'down'
        confidence = 60 + min(30, abs(imbalance_score)/100)
    else:
        if cvd_slope_10 > 5:
            direction = 'up'
            confidence = 55
        elif cvd_slope_10 < -5:
            direction = 'down'
            confidence = 55
        else:
            direction = 'neutral'
            confidence = 50
    confidence = min(95, max(5, int(confidence)))
    return {
        'direction': direction,
        'confidence': confidence,
        'cvd_slope_10': round(cvd_slope_10, 2),
        'cvd_acceleration': round(cvd_acc, 2),
        'divergence': divergence,
        'absorption_net': net_abs,
        'imbalance_score': round(imbalance_score, 2)
    }

def compress_footprint(clusters, max_levels=MAX_FOOTPRINT_LEVELS):
    items = [(p, d['buy']+d['sell']) for p, d in clusters.items()]
    items.sort(key=lambda x: x[1], reverse=True)
    top = items[:max_levels]
    footprint = []
    for price, total in top:
        footprint.append({
            'price': price,
            'buy': clusters[price]['buy'],
            'sell': clusters[price]['sell']
        })
    return footprint

def save_toon(filepath, state, signals, tick_size, symbol):
    footprint = compress_footprint(state['clusters'], max_levels=MAX_FOOTPRINT_LEVELS)
    lines = []
    lines.append(f"# CVD intelligence for {symbol.upper()} (binned tick={tick_size})")
    lines.append(f"generated: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append(f"symbol: {symbol}")
    lines.append("")
    lines.append("cvd_summary[1]{timestamp,cumulative_cvd,total_trade_count,last_trade_id}:")
    lines.append(f"  {int(time.time()*1000)},{state['cumulative_cvd']:.2f},{state['total_trade_count']},0")
    lines.append("")
    lines.append("cvd_signal[1]{direction,confidence,cvd_slope_10,cvd_acceleration,divergence,absorption_net,imbalance_score}:")
    lines.append(f"  {signals['direction']},{signals['confidence']},{signals['cvd_slope_10']},{signals['cvd_acceleration']},{signals['divergence']},{signals['absorption_net']},{signals['imbalance_score']}")
    lines.append("")
    lines.append(f"price_clusters[{len(footprint)}]{{price,buy_vol,sell_vol,delta}}:")
    for p in footprint:
        delta = p['buy'] - p['sell']
        lines.append(f"  {p['price']},{p['buy']:.2f},{p['sell']:.2f},{delta:.2f}")
    lines.append("")
    imb = state['imbalance_events'][-10:]
    lines.append(f"imbalance_events[{len(imb)}]{{timestamp,price,ratio,type,volume,diff}}:")
    if imb:
        rows = [f"{e['timestamp']},{e['price']},{e['ratio']},{e['type']},{e['volume']},{e['diff']}" for e in imb]
        lines.append("  " + " |\n  ".join(rows))
    else:
        lines.append("  ")
    lines.append("")
    abs_ev = state['absorption_events'][-10:]
    lines.append(f"absorption_events[{len(abs_ev)}]{{timestamp,price,cvd_delta,price_delta,type}}:")
    if abs_ev:
        rows = [f"{e['timestamp']},{e['price']},{e['cvd_delta']},{e['price_delta']},{e['type']}" for e in abs_ev]
        lines.append("  " + " |\n  ".join(rows))
    else:
        lines.append("  ")
    lines.append("")
    lines.append("# ========== END ==========")

    tmp = filepath + ".tmp"
    try:
        with open(tmp, 'w', encoding='utf-8') as f:
            f.write("\n".join(lines))
            f.flush()
            os.fsync(f.fileno())
        os.rename(tmp, filepath)
        print(f"[X03_cvd] TOON written: {filepath}")
    except Exception as e:
        if os.path.exists(tmp):
            os.remove(tmp)
        print(f"[X03_cvd] TOON write failed: {e}")
        log_issue(symbol, "WRITE_ERROR", f"Failed to write TOON: {e}", "ERROR")

def backfill_cvd_advanced(symbol, config=None, max_total_trades=50000, max_cluster_levels=500, min_vol=5.0):
    print(f"[X03_cvd] Processing {MAX_TRADES_FETCH} most recent trades (no raw TSV)")
    data_dir = get_data_dir()
    toon_path = os.path.join(data_dir, f"{symbol.lower()}_cvd.toon")
    tick_size = get_tick_size(symbol)

    trades = fetch_initial_trades(symbol)
    if not trades:
        return {"error": "No trades fetched"}

    state = update_metrics(trades, symbol, tick_size)
    signals = compute_signals(state)
    save_toon(toon_path, state, signals, tick_size, symbol)

    print(f"[X03_cvd] Done. Trades: {state['total_trade_count']}, CVD={state['cumulative_cvd']:.2f}, Signal={signals['direction']} ({signals['confidence']}%)")
    return {"cvd": state['cumulative_cvd'], "trade_count": state['total_trade_count'], "signal": signals}

if __name__ == "__main__":
    backfill_cvd_advanced("BTCUSDT")