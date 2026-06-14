#!/usr/bin/env python3
"""
X03_cvd_rest.py – Split 50k trades into two files (25k each), ultra‑compact, opens instantly
- Fetches 50,000 Binance aggregate trades.
- First 25k trades → {symbol}_cvd1.tmp_x
- Next 25k trades → {symbol}_cvd2.tmp_x
- Timestamp in minutes, base62 encoding, 20 trades per row.
- Each file independent (contains its own first trade absolute).
- Logs to X03_cvd.log (no console spam).
"""

import requests
import time
import os
import sys

BASE_URL = "https://api.binance.com/api/v3"
TRADES_PER_FILE = 25000
TRADES_PER_ROW = 20
PAGE_LIMIT = 1000
RATE_LIMIT_SEC = 0.2
MAX_RETRIES = 3
RETRY_BACKOFF = 1.5
LOG_GAPS = False          # Set True to log ID gaps (for debugging only)

# ---------- Global log file ----------
LOG_FILE = None
LOG_MAX_SIZE = 5 * 1024 * 1024  # 5 MB

def get_log_file():
    global LOG_FILE
    if LOG_FILE is None:
        data_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "market_data", "binance", "symbols")
        os.makedirs(data_dir, exist_ok=True)
        LOG_FILE = os.path.join(data_dir, "X03_cvd.log")
    return LOG_FILE

def rotate_log_if_needed():
    log_path = get_log_file()
    if os.path.exists(log_path) and os.path.getsize(log_path) > LOG_MAX_SIZE:
        backup = log_path + ".old"
        try:
            os.replace(log_path, backup)
        except:
            pass

def log_msg(level, msg):
    rotate_log_if_needed()
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    line = f"{timestamp} [{level}] {msg}"
    # Write to log file
    with open(get_log_file(), "a", encoding="utf-8") as f:
        f.write(line + "\n")
    # Print only errors and essential start/end to console
    if level == "ERROR" or msg.startswith("Starting") or msg.startswith("Wrote") or msg.startswith("Total"):
        print(line)

# ----- Base62 encoding -----
BASE62 = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

def to_base62(n):
    if n == 0:
        return "0"
    digits = []
    while n:
        n, r = divmod(n, 62)
        digits.append(BASE62[r])
    return ''.join(reversed(digits))

def encode(n):
    if n == 0:
        return "p0"
    sign = 'm' if n < 0 else 'p'
    return sign + to_base62(abs(n))

def decode(s):
    sign = s[0]
    val = 0
    for ch in s[1:]:
        val = val * 62 + BASE62.index(ch)
    return -val if sign == 'm' else val

# ----- Precision helpers -----
def decimal_length(s):
    return len(s.split('.')[1]) if '.' in s else 0

def str_to_int(s, prec):
    s = s.strip()
    if '.' in s:
        ip, fp = s.split('.')
        if len(fp) >= prec:
            fp = fp[:prec]
        else:
            fp = fp + '0' * (prec - len(fp))
        return int(ip + fp)
    else:
        return int(s + '0' * prec)

def get_precision(symbol):
    session = requests.Session()
    for attempt in range(MAX_RETRIES):
        try:
            r = session.get(f"{BASE_URL}/exchangeInfo", timeout=15)
            r.raise_for_status()
            data = r.json()
            for s in data['symbols']:
                if s['symbol'] == symbol.upper():
                    price_prec = 8
                    qty_prec = 8
                    for f in s['filters']:
                        if f['filterType'] == 'PRICE_FILTER':
                            price_prec = decimal_length(f['tickSize'])
                        elif f['filterType'] == 'LOT_SIZE':
                            qty_prec = decimal_length(f['stepSize'])
                    return price_prec, qty_prec
            break
        except:
            if attempt == MAX_RETRIES - 1:
                pass
            time.sleep(RETRY_BACKOFF ** attempt)
    return 8, 8

# ----- API client with rate limiting -----
class APIClient:
    def __init__(self):
        self.session = requests.Session()
        self.last_call = 0

    def get(self, url, params):
        now = time.time()
        if now - self.last_call < RATE_LIMIT_SEC:
            time.sleep(RATE_LIMIT_SEC - (now - self.last_call))
        self.last_call = time.time()
        for attempt in range(MAX_RETRIES):
            try:
                r = self.session.get(url, params=params, timeout=10)
                if r.status_code == 200:
                    data = r.json()
                    if isinstance(data, list):
                        return data
                elif r.status_code == 429:
                    wait = RETRY_BACKOFF ** attempt * 2
                    time.sleep(wait)
                    continue
                else:
                    time.sleep(1)
            except:
                pass
            time.sleep(RETRY_BACKOFF ** attempt)
        return None

def fetch_all_trades(symbol, max_trades):
    """Generator that yields trades up to max_trades, with duplicate skipping."""
    client = APIClient()
    from_id = 1
    fetched = 0
    last_id = 0
    while fetched < max_trades:
        params = {"symbol": symbol.upper(), "limit": PAGE_LIMIT, "fromId": from_id}
        batch = client.get(f"{BASE_URL}/aggTrades", params)
        if not batch:
            break
        max_id_in_batch = max(t['a'] for t in batch)
        if max_id_in_batch <= last_id:
            log_msg("WARNING", f"No ID progress (last={last_id}, max={max_id_in_batch}), stopping")
            break
        for t in batch:
            if fetched >= max_trades:
                break
            if t['a'] <= last_id:
                continue
            if LOG_GAPS and last_id and t['a'] > last_id + 1:
                log_msg("WARNING", f"Gap: expected {last_id+1}, got {t['a']} (skip of {t['a']-last_id-1})")
            yield {
                'ts': t['T'],
                'price': t['p'],
                'qty': t['q'],
                'sell': t['m']
            }
            last_id = t['a']
            fetched += 1
            if fetched % 5000 == 0:
                log_msg("INFO", f"{fetched}/{max_trades}")
        if batch:
            from_id = last_id + 1
        else:
            break
    log_msg("INFO", f"Fetched {fetched} trades for this chunk")
    return fetched

def write_file(symbol, trade_gen, file_suffix, price_prec, qty_prec):
    data_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "market_data", "binance", "symbols")
    os.makedirs(data_dir, exist_ok=True)
    out_path = os.path.join(data_dir, f"{symbol.lower()}_{file_suffix}.tmp_x")
    with open(out_path, 'w', encoding='utf-8') as f:
        f.write(f"#version=2\n")
        f.write(f"#exchange=binance\n")
        f.write(f"#symbol={symbol.upper()}\n")
        f.write(f"#price_precision={price_prec} qty_precision={qty_prec}\n")
        first = next(trade_gen, None)
        if not first:
            return 0
        ts0 = first['ts'] // 60000
        p0 = str_to_int(first['price'], price_prec)
        q0 = str_to_int(first['qty'], qty_prec)
        s0 = 1 if first['sell'] else 0
        f.write(f"0\t{encode(ts0)},{encode(p0)},{encode(q0)},{s0}\n")
        row = []
        row_idx = 1
        prev_ts, prev_p, prev_q = ts0, p0, q0
        total = 1
        for t in trade_gen:
            ts = t['ts'] // 60000
            p = str_to_int(t['price'], price_prec)
            q = str_to_int(t['qty'], qty_prec)
            sell = 1 if t['sell'] else 0
            dt = ts - prev_ts
            dp = p - prev_p
            dq = q - prev_q
            row.append(f"{encode(dt)},{encode(dp)},{encode(dq)},{sell}")
            total += 1
            if len(row) == TRADES_PER_ROW:
                f.write(f"{row_idx}\t{';'.join(row)}\n")
                row_idx += 1
                row.clear()
            prev_ts, prev_p, prev_q = ts, p, q
        if row:
            f.write(f"{row_idx}\t{';'.join(row)}\n")
    return total

def run_download(symbol):
    log_msg("INFO", f"Starting {symbol}")
    try:
        price_prec, qty_prec = get_precision(symbol)
        log_msg("INFO", f"Precision: price={price_prec}, qty={qty_prec}")

        gen1 = fetch_all_trades(symbol, TRADES_PER_FILE)
        cnt1 = write_file(symbol, gen1, "cvd1", price_prec, qty_prec)
        log_msg("INFO", f"Wrote {cnt1} trades to {symbol}_cvd1.tmp_x")

        gen2 = fetch_all_trades(symbol, TRADES_PER_FILE)
        cnt2 = write_file(symbol, gen2, "cvd2", price_prec, qty_prec)
        log_msg("INFO", f"Wrote {cnt2} trades to {symbol}_cvd2.tmp_x")

        log_msg("INFO", f"Total trades: {cnt1 + cnt2}")
        return True
    except Exception as e:
        log_msg("ERROR", f"FAIL: {e}")
        import traceback
        traceback.print_exc(file=sys.stdout)
        return False

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python X03_cvd_rest.py SYMBOL")
        sys.exit(1)
    success = run_download(sys.argv[1].upper())
    sys.exit(0 if success else 1)