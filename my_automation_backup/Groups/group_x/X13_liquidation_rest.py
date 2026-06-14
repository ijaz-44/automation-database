#!/usr/bin/env python3
"""
X13_liquidation_rest.py – Raw Liquidation & Trade Collector (No Data Fallback)
- If no data, creates .tmp_x file with comment "# No data downloaded"
- Fast fail (2 retries, 3 sec timeout)
- All sources kept
"""

import os
import sys
import time
import requests
import hashlib
import random
from datetime import datetime, timezone
from collections import defaultdict

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SYMBOLS_DIR = os.path.join(BASE_DIR, "market_data", "binance", "symbols")
os.makedirs(SYMBOLS_DIR, exist_ok=True)
GLOBAL_LOG = os.path.join(SYMBOLS_DIR, "X13_liquidation.log")
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "Mozilla/5.0"})

SOURCE_LAST_CALL = defaultdict(float)
SOURCE_MIN_INTERVAL = { "Hyperliquid": 0.5, "dYdX": 0.5, "Bybit": 0.5, "Binance": 0.5, "Bitget": 0.5 }

def rate_limit(source):
    now = time.time()
    last = SOURCE_LAST_CALL[source]
    if now - last < SOURCE_MIN_INTERVAL.get(source, 0.5):
        time.sleep(SOURCE_MIN_INTERVAL.get(source, 0.5) - (now - last))
    SOURCE_LAST_CALL[source] = time.time()

def log_issue(symbol, level, msg):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"{ts} [{level}] [{symbol}] {msg}"
    print(line)
    with open(GLOBAL_LOG, "a", encoding="utf-8") as f:
        f.write(line + "\n")

def normalize_symbol(sym):
    sym = sym.upper()
    if sym.endswith("PERP"):
        sym = sym[:-4]
    if not sym.endswith("USDT"):
        sym = sym + "USDT"
    return sym

def normalize_ts(ts):
    if ts is None:
        return int(time.time() * 1000)
    ts = int(ts)
    if ts > 10**15:
        ts = ts // 1000
    if len(str(ts)) <= 10:
        ts = ts * 1000
    return (ts // 1000) * 1000

def safe_iso_parse(ts_str):
    if not ts_str:
        return None
    try:
        dt = datetime.fromisoformat(ts_str.replace('Z', '+00:00'))
        return normalize_ts(int(dt.timestamp() * 1000))
    except:
        try:
            cleaned = ts_str.replace('Z', '').replace('+00:00', '').split('.')[0]
            dt = datetime.fromisoformat(cleaned).replace(tzinfo=timezone.utc)
            return normalize_ts(int(dt.timestamp() * 1000))
        except:
            return None

def fetch_with_retries(source, url, params=None, headers=None, max_retries=2, backoff=0.5):
    for attempt in range(max_retries):
        rate_limit(source)
        try:
            time.sleep(random.uniform(0.1, 0.3) * attempt)
            resp = SESSION.get(url, params=params, headers=headers, timeout=3)
            ct = resp.headers.get('Content-Type', '').lower()
            if resp.status_code == 200 and ('json' in ct or resp.text.startswith('{')):
                return resp
            else:
                log_issue(source, "WARNING", f"HTTP {resp.status_code} (attempt {attempt+1})")
        except Exception as e:
            log_issue(source, "WARNING", f"Error: {type(e).__name__}: {e} (attempt {attempt+1})")
        if attempt < max_retries - 1:
            time.sleep(backoff * (2 ** attempt) + random.uniform(0, 0.3))
    return None

def quick_post(source, url, json_data=None, max_retries=2):
    for attempt in range(max_retries):
        rate_limit(source)
        try:
            resp = SESSION.post(url, json=json_data, timeout=3)
            ct = resp.headers.get('Content-Type', '').lower()
            if resp.status_code == 200 and ('json' in ct or resp.text.startswith('{')):
                return resp
        except Exception as e:
            log_issue(source, "WARNING", f"POST error: {e} (attempt {attempt+1})")
        time.sleep(0.5 * (attempt+1))
    return None

NOW_MS = int(time.time() * 1000)
CUTOFF_MS = NOW_MS - 6 * 3600 * 1000
def is_within_cutoff(ts): return ts >= CUTOFF_MS

# ---------- Hyperliquid ----------
def fetch_hyperliquid_true(symbol):
    source = "Hyperliquid"
    coin = symbol.upper().replace("USDT", "")
    payload = {"type": "clearingEvents", "user": "0x0000000000000000000000000000000000000000"}
    resp = quick_post(source, "https://api.hyperliquid.xyz/info", json_data=payload)
    if not resp:
        return []
    try:
        raw = resp.json()
    except:
        return []
    if isinstance(raw, list):
        data = raw
    elif isinstance(raw, dict):
        data = raw.get("clearingEvents") or raw.get("data")
        if isinstance(data, dict):
            data = data.get("clearingEvents") or data.get("data", [])
    else:
        return []
    if not isinstance(data, list):
        return []
    events = []
    for item in data:
        if not isinstance(item, dict) or item.get("type") != "liquidation":
            continue
        liq = item.get("liquidation", {})
        if liq.get("coin", "").upper() != coin:
            continue
        price = float(liq.get("price", 0))
        size = float(liq.get("size", 0))
        side = "short" if liq.get("isBuy") else "long"
        ts = normalize_ts(int(item.get("time", 0)))
        if not is_within_cutoff(ts):
            continue
        events.append({"ts": ts, "price": price, "usd_volume": price * size,
                       "side": side, "type": "true_liquidation", "source": "hyperliquid"})
    log_issue(symbol, "INFO", f"Hyperliquid: {len(events)} events")
    return events

# ---------- dYdX ----------
def fetch_dydx_true(symbol):
    source = "dYdX"
    coin = symbol.upper().replace("USDT", "")
    url = f"https://indexer.dydx.trade/v4/perpetualMarkets/{coin}-USD/liquidations"
    resp = fetch_with_retries(source, url, params={"limit": 200})
    if not resp:
        return []
    try:
        data = resp.json()
    except:
        return []
    liq_list = data.get("liquidations")
    if not isinstance(liq_list, list):
        return []
    events = []
    for liq in liq_list:
        try:
            price = float(liq.get("price", 0))
            size = float(liq.get("size", 0))
            side_raw = liq.get("side", "")
            side = "long" if side_raw == "LONG" else "short" if side_raw == "SHORT" else None
            if not side:
                continue
            ts = safe_iso_parse(liq.get("createdAt", ""))
            if ts is None or not is_within_cutoff(ts):
                continue
            events.append({"ts": ts, "price": price, "usd_volume": price * size,
                           "side": side, "type": "true_liquidation", "source": "dydx"})
        except:
            continue
    log_issue(symbol, "INFO", f"dYdX: {len(events)} events")
    return events

# ---------- Bybit ----------
def fetch_bybit_true(symbol):
    source = "Bybit"
    sym = normalize_symbol(symbol)
    endpoints = [
        ("force-orders", "https://api.bybit.com/v5/market/force-orders", {"category": "linear", "symbol": sym, "limit": 200}),
        ("liquidations", "https://api.bybit.com/v5/market/liquidations", {"category": "linear", "symbol": sym, "limit": 200})
    ]
    all_events = []
    for name, url, params in endpoints:
        resp = fetch_with_retries(source, url, params)
        if not resp:
            continue
        try:
            data = resp.json()
        except:
            continue
        if data.get("retCode") != 0 or "result" not in data:
            continue
        items = data["result"].get("list", [])
        if not isinstance(items, list):
            continue
        for item in items:
            try:
                price = float(item.get("price", 0))
                size = float(item.get("size", 0))
                side_raw = item.get("side", "")
                if not side_raw:
                    continue
                side = "short" if side_raw == "Buy" else "long" if side_raw == "Sell" else None
                if not side:
                    continue
                ts_raw = int(item.get("updatedTime", 0)) if "updatedTime" in item else int(item.get("time", 0))
                ts = normalize_ts(ts_raw)
                if not is_within_cutoff(ts):
                    continue
                all_events.append({"ts": ts, "price": price, "usd_volume": price * size,
                                   "side": side, "type": "true_liquidation", "source": "bybit"})
            except:
                continue
    log_issue(symbol, "INFO", f"Bybit: {len(all_events)} events")
    return all_events

# ---------- Binance trades ----------
def parse_binance_trades(data):
    if not isinstance(data, list):
        return []
    trades = []
    for item in data:
        try:
            trades.append({
                "ts": normalize_ts(int(item.get("T", 0))),
                "price": float(item.get("p", 0)),
                "usd_vol": float(item.get("p", 0)) * float(item.get("q", 0)),
                "side": "long" if not item.get("m", False) else "short"
            })
        except:
            continue
    return trades

def fetch_binance_trades(symbol):
    source = "Binance"
    sym = normalize_symbol(symbol)
    resp = fetch_with_retries(source, "https://api.binance.com/api/v3/trades",
                              params={"symbol": sym, "limit": 1000})
    if not resp:
        return []
    try:
        data = resp.json()
    except:
        return []
    trades = parse_binance_trades(data)
    events = []
    for t in trades:
        if not is_within_cutoff(t['ts']):
            continue
        events.append({"ts": t['ts'], "price": t['price'], "usd_volume": t['usd_vol'],
                       "side": t['side'], "type": "raw_trade", "source": "binance"})
    log_issue(symbol, "INFO", f"Binance raw trades: {len(events)}")
    return events

# ---------- Bitget trades ----------
def parse_bitget_trades(data):
    if not isinstance(data, dict):
        return []
    trades_data = data.get("data", [])
    if isinstance(trades_data, dict):
        trades_data = trades_data.get("list", [])
    if isinstance(trades_data, dict):
        trades_data = trades_data.get("data", [])
    if not isinstance(trades_data, list):
        return []
    trades = []
    for item in trades_data:
        try:
            trades.append({
                "ts": normalize_ts(int(item.get("ts", 0))),
                "price": float(item.get("price", 0)),
                "usd_vol": float(item.get("price", 0)) * float(item.get("size", 0)),
                "side": "long" if item.get("side", "") == "buy" else "short"
            })
        except:
            continue
    return trades

def fetch_bitget_trades(symbol):
    source = "Bitget"
    sym = normalize_symbol(symbol)
    resp = fetch_with_retries(source, "https://api.bitget.com/api/v2/mix/market/trades",
                              params={"symbol": sym, "limit": 1000})
    if not resp:
        return []
    try:
        data = resp.json()
    except:
        return []
    trades = parse_bitget_trades(data)
    events = []
    for t in trades:
        if not is_within_cutoff(t['ts']):
            continue
        events.append({"ts": t['ts'], "price": t['price'], "usd_volume": t['usd_vol'],
                       "side": t['side'], "type": "raw_trade", "source": "bitget"})
    log_issue(symbol, "INFO", f"Bitget raw trades: {len(events)}")
    return events

# ---------- Deduplication ----------
def deduplicate_events(events, symbol):
    seen = set()
    unique = []
    price_round = 1 if "BTC" in symbol.upper() or "ETH" in symbol.upper() else 2
    for e in events:
        price_r = round(e['price'], price_round)
        vol_bucket = int(e['usd_volume'] / 50) * 50
        key = hashlib.md5(f"{e['source']}|{e['ts']}|{price_r}|{vol_bucket}|{e['side']}".encode()).hexdigest()
        if key not in seen:
            seen.add(key)
            unique.append(e)
    return unique

# ---------- Main ----------
def collect_raw_data(symbol):
    log_issue(symbol, "INFO", "=== RAW DATA COLLECTOR (Fast Fail) ===")
    all_events = []
    all_events.extend(fetch_hyperliquid_true(symbol))
    all_events.extend(fetch_dydx_true(symbol))
    all_events.extend(fetch_bybit_true(symbol))
    all_events.extend(fetch_binance_trades(symbol))
    all_events.extend(fetch_bitget_trades(symbol))
    all_events = deduplicate_events(all_events, symbol)
    output_path = os.path.join(SYMBOLS_DIR, f"{symbol.lower()}_liquidations.tmp_x")
    with open(output_path, "w", encoding="utf-8") as f:
        f.write("ts\tprice\tusd_volume\tside\ttype\tsource\n")
        if not all_events:
            f.write("# No data downloaded from any source\n")
        else:
            for e in all_events:
                f.write(f"{e['ts']}\t{e['price']:.2f}\t{e['usd_volume']:.2f}\t{e['side']}\t{e['type']}\t{e['source']}\n")
    log_issue(symbol, "INFO", f"Saved {len(all_events)} events to {output_path}")
    return True

def run_download(symbol):
    return collect_raw_data(symbol)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python X13_liquidation_rest.py SYMBOL")
        sys.exit(1)
    success = run_download(sys.argv[1].upper())
    sys.exit(0 if success else 1)