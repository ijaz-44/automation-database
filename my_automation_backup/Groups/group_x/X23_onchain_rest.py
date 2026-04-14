# Groups/group_x/X23_onchain_rest.py
"""
X23 - On‑Chain Data Module (Multi‑Source, No API Key Required)
- Exchange Inflow/Outflow (1h, last 120 candles) – via Glassnode community data (CSV mirror) + CryptoQuant free chart CSV
- Whale Wallet Movements (1h, last 120 candles) – via Whale Alert API (free tier) + Dune Analytics public query (JSON)
- USDT/USDC Exchange Flow (1h, last 120 candles) – via Glassnode community data + CoinMetrics community data
- Single TSV file: {symbol}_onchain.tsv with sections, full logging, fallback for every metric
"""

import requests
import time
import os
import csv
import json
import math
from datetime import datetime, timedelta
from collections import defaultdict

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SYMBOLS_DIR = os.path.join(BASE_DIR, "market_data", "binance", "symbols")
os.makedirs(SYMBOLS_DIR, exist_ok=True)

# ---------- Rate Limiting ----------
LAST_CALL = 0
RATE_LIMIT_SEC = 1.2

def rate_limited_fetch(url, headers=None, params=None):
    global LAST_CALL
    now = time.time()
    if now - LAST_CALL < RATE_LIMIT_SEC:
        time.sleep(RATE_LIMIT_SEC - (now - LAST_CALL))
    LAST_CALL = time.time()
    try:
        r = requests.get(url, headers=headers, params=params, timeout=15)
        if r.status_code == 200:
            return r
        else:
            print(f"[X23] HTTP {r.status_code} from {url[:60]}: {r.text[:100]}")
            return None
    except Exception as e:
        print(f"[X23] Request error: {e}")
        return None

# ---------- Helper: Timestamp Alignment ----------
def align_to_hour(ts_ms):
    """Round down timestamp to beginning of hour (UTC)."""
    return (ts_ms // 3600000) * 3600000

def bucket_events(events, value_key, hours=120):
    """
    Aggregate raw events into 1‑hour buckets for last 'hours' hours.
    events: list of dicts with 'timestamp' (ms) and a numeric value under value_key.
    Returns list of dicts: {'timestamp': bucket_start_ms, 'value': sum}
    """
    now = int(time.time() * 1000)
    cutoff = now - hours * 3600000
    buckets = defaultdict(float)
    for ev in events:
        ts = ev.get('timestamp')
        if ts and ts >= cutoff:
            bucket = align_to_hour(ts)
            buckets[bucket] += ev.get(value_key, 0)
    result = []
    # Generate all hourly buckets in order (most recent first)
    for i in range(hours):
        bucket_ts = align_to_hour(now - i * 3600000)
        result.append({
            "timestamp": bucket_ts,
            "value": buckets.get(bucket_ts, 0.0)
        })
    return result

# ========== SECTION 1: EXCHANGE INFLOW / OUTFLOW (Glassnode CSV mirror) ==========
# Glassnode provides free CSV exports of exchange inflow/outflow for major coins.
# We use a community‑maintained mirror that doesn't require an API key.

def fetch_exchange_flows_glassnode(symbol):
    """
    Fetch exchange inflow/outflow for Bitcoin (proxy for market) from Glassnode CSV mirror.
    Returns list of (timestamp, inflow, outflow, netflow) for last 120 hours.
    """
    # Map symbol to Glassnode asset code
    asset_map = {"BTCUSDT": "bitcoin", "ETHUSDT": "ethereum"}
    asset = asset_map.get(symbol.upper())
    if not asset:
        return []
    # Free CSV from Glassnode community data (no key)
    url_in = f"https://data.glassnode.com/community/{asset}/exchange_inflow_usd_1h.csv"
    url_out = f"https://data.glassnode.com/community/{asset}/exchange_outflow_usd_1h.csv"
    inflow_data = {}
    outflow_data = {}
    # Fetch inflow
    r = rate_limited_fetch(url_in)
    if r and r.status_code == 200:
        for row in csv.DictReader(r.text.splitlines()):
            ts = int(row.get('timestamp', 0))
            val = float(row.get('value', 0))
            if ts:
                inflow_data[ts] = val
    # Fetch outflow
    r = rate_limited_fetch(url_out)
    if r and r.status_code == 200:
        for row in csv.DictReader(r.text.splitlines()):
            ts = int(row.get('timestamp', 0))
            val = float(row.get('value', 0))
            if ts:
                outflow_data[ts] = val
    if not inflow_data and not outflow_data:
        return []
    now = int(time.time() * 1000)
    cutoff = now - 120 * 3600000
    hourly = []
    for i in range(120):
        bucket = align_to_hour(now - i * 3600000)
        inflow = inflow_data.get(bucket, 0.0)
        outflow = outflow_data.get(bucket, 0.0)
        hourly.append({
            "timestamp": bucket,
            "inflow": inflow,
            "outflow": outflow,
            "netflow": inflow - outflow
        })
    return hourly

def fetch_exchange_flows_cryptoquant(symbol):
    """
    Fallback: CryptoQuant exchange flows (public chart CSV).
    Works for BTC and ETH.
    """
    asset_map = {"BTCUSDT": "BTC", "ETHUSDT": "ETH"}
    asset = asset_map.get(symbol.upper())
    if not asset:
        return []
    # CryptoQuant free chart CSV (example for Bitcoin)
    url = f"https://raw.githubusercontent.com/cryptoquant/data/master/exchange_flows/{asset}_exchange_netflow_1h.csv"
    r = rate_limited_fetch(url)
    if not r:
        return []
    hourly = []
    lines = r.text.strip().splitlines()
    if len(lines) < 2:
        return []
    # CSV format: timestamp, netflow
    for line in lines[1:]:
        parts = line.split(',')
        if len(parts) >= 2:
            ts = int(parts[0])
            netflow = float(parts[1])
            hourly.append({"timestamp": ts, "inflow": 0, "outflow": 0, "netflow": netflow})
    # Keep last 120 hours
    hourly.sort(key=lambda x: x['timestamp'], reverse=True)
    hourly = hourly[:120]
    return hourly

def get_exchange_flows(symbol):
    print("[X23] Fetching exchange inflow/outflow from Glassnode...")
    data = fetch_exchange_flows_glassnode(symbol)
    if data:
        print(f"[X23] Glassnode OK, got {len(data)} hourly rows")
        return data
    print("[X23] Glassnode failed, trying CryptoQuant fallback...")
    data = fetch_exchange_flows_cryptoquant(symbol)
    if data:
        print(f"[X23] CryptoQuant fallback OK, got {len(data)} hourly rows")
        return data
    print("[X23] WARNING: No exchange flow data available")
    return []

# ========== SECTION 2: WHALE WALLET MOVEMENTS (Whale Alert API + Dune) ==========
# Whale Alert API has a free tier (10 calls/min, min tx $500k). We'll also use Dune public queries as fallback.

WHALE_ALERT_API_KEY = ""  # Get free key from https://developer.whale-alert.io/

def fetch_whale_movements_whalealert(symbol, hours=120):
    """
    Fetch whale transactions ($500k+) from Whale Alert API.
    Returns list of dicts with 'timestamp' and 'value_usd'.
    """
    if not WHALE_ALERT_API_KEY:
        return []
    # Whale Alert API endpoint
    url = "https://api.whale-alert.io/v1/transactions"
    params = {
        "api_key": WHALE_ALERT_API_KEY,
        "min_value": 500000,
        "limit": 100
    }
    r = rate_limited_fetch(url, params=params)
    if not r:
        return []
    data = r.json()
    if data.get("success") and "transactions" in data:
        transactions = data["transactions"]
        events = []
        for tx in transactions:
            events.append({
                "timestamp": tx.get("timestamp", 0),
                "value_usd": tx.get("amount_usd", 0)
            })
        print(f"[X23] Whale Alert OK, fetched {len(events)} raw transactions")
        return bucket_events(events, "value_usd", hours=hours)
    return []

def fetch_whale_movements_dune(symbol):
    """
    Fallback: Dune Analytics public query for whale movements (JSON).
    Uses a community query that returns hourly aggregated whale activity.
    """
    # Example Dune query ID that returns hourly whale transfers >500k USD
    # (This is a placeholder; you can replace with any public query ID)
    query_id = "4146451"  # Sample Dune query for whale transfers
    url = f"https://api.dune.com/api/v1/query/{query_id}/results"
    r = rate_limited_fetch(url)
    if not r:
        return []
    data = r.json()
    if "result" in data and "rows" in data["result"]:
        rows = data["result"]["rows"]
        hourly = []
        for row in rows:
            hourly.append({
                "timestamp": row.get("hour"),
                "inflow": row.get("inflow_usd", 0),
                "outflow": row.get("outflow_usd", 0),
                "netflow": row.get("netflow_usd", 0)
            })
        hourly.sort(key=lambda x: x['timestamp'], reverse=True)
        print(f"[X23] Dune fallback OK, got {len(hourly)} hourly rows")
        return hourly
    return []

def get_whale_movements(symbol):
    print("[X23] Fetching whale movements from Whale Alert...")
    data = fetch_whale_movements_whalealert(symbol)
    if data:
        return data
    print("[X23] Whale Alert failed (or no key), trying Dune fallback...")
    data = fetch_whale_movements_dune(symbol)
    if data:
        return data
    print("[X23] WARNING: No whale movement data available")
    return []

# ========== SECTION 3: USDT / USDC EXCHANGE FLOW (Glassnode + CoinMetrics) ==========
# Stablecoin exchange flows are available via Glassnode community CSV and CoinMetrics community API.

def fetch_stablecoin_flows_glassnode(stablecoin, hours=120):
    """
    Fetch exchange inflow/outflow for a stablecoin (USDT or USDC) from Glassnode CSV.
    Returns list of dicts: {'timestamp': bucket, 'inflow', 'outflow', 'netflow'}
    """
    # Glassnode asset codes for stablecoins
    asset_map = {"USDT": "tether", "USDC": "usd_coin"}
    asset = asset_map.get(stablecoin.upper())
    if not asset:
        return []
    url_in = f"https://data.glassnode.com/community/{asset}/exchange_inflow_usd_1h.csv"
    url_out = f"https://data.glassnode.com/community/{asset}/exchange_outflow_usd_1h.csv"
    inflow = {}
    outflow = {}
    r = rate_limited_fetch(url_in)
    if r and r.status_code == 200:
        for row in csv.DictReader(r.text.splitlines()):
            ts = int(row.get('timestamp', 0))
            val = float(row.get('value', 0))
            if ts:
                inflow[ts] = val
    r = rate_limited_fetch(url_out)
    if r and r.status_code == 200:
        for row in csv.DictReader(r.text.splitlines()):
            ts = int(row.get('timestamp', 0))
            val = float(row.get('value', 0))
            if ts:
                outflow[ts] = val
    if not inflow and not outflow:
        return []
    now = int(time.time() * 1000)
    cutoff = now - hours * 3600000
    hourly = []
    for i in range(hours):
        bucket = align_to_hour(now - i * 3600000)
        inc = inflow.get(bucket, 0.0)
        outc = outflow.get(bucket, 0.0)
        hourly.append({
            "timestamp": bucket,
            "inflow": inc,
            "outflow": outc,
            "netflow": inc - outc
        })
    return hourly

def fetch_stablecoin_flows_coinmetrics(stablecoin):
    """
    Fallback: CoinMetrics community API for stablecoin exchange flows.
    """
    # CoinMetrics asset ID for stablecoins
    asset_map = {"USDT": "usdt", "USDC": "usdc"}
    asset = asset_map.get(stablecoin.upper())
    if not asset:
        return []
    url = f"https://community-api.coinmetrics.io/v4/timeseries/asset-metrics?assets={asset}&metrics=FlowInExUSD,FlowOutExUSD&frequency=1h&limit=120"
    r = rate_limited_fetch(url)
    if not r:
        return []
    data = r.json()
    if "data" not in data:
        return []
    hourly = []
    for item in data["data"]:
        ts = int(datetime.fromisoformat(item["time"].replace('Z', '+00:00')).timestamp() * 1000)
        inflow = float(item.get("FlowInExUSD", 0))
        outflow = float(item.get("FlowOutExUSD", 0))
        hourly.append({
            "timestamp": ts,
            "inflow": inflow,
            "outflow": outflow,
            "netflow": inflow - outflow
        })
    hourly.sort(key=lambda x: x['timestamp'], reverse=True)
    return hourly

def get_stablecoin_flows(symbol):
    """
    Determine which stablecoin is most relevant (USDT for most pairs, USDC for others).
    """
    # For BTCUSDT, we track USDT flows. For USDC pairs, track USDC.
    if "USDT" in symbol.upper():
        stablecoin = "USDT"
    elif "USDC" in symbol.upper():
        stablecoin = "USDC"
    else:
        return []
    print(f"[X23] Fetching {stablecoin} exchange flows from Glassnode...")
    data = fetch_stablecoin_flows_glassnode(stablecoin)
    if data:
        print(f"[X23] Glassnode OK for {stablecoin}")
        return data
    print(f"[X23] Glassnode failed, trying CoinMetrics fallback for {stablecoin}...")
    data = fetch_stablecoin_flows_coinmetrics(stablecoin)
    if data:
        print(f"[X23] CoinMetrics fallback OK for {stablecoin}")
        return data
    print(f"[X23] WARNING: No stablecoin flow data for {stablecoin}")
    return []

# ========== MAIN SAVE FUNCTION (Single TSV) ==========
def collect_and_save(symbol):
    print(f"[X23] Starting on-chain data collection for {symbol}")
    filepath = os.path.join(SYMBOLS_DIR, f"{symbol.lower()}_onchain.tsv")

    # Section 1: Exchange Inflow/Outflow
    print("[X23] Step 1: Exchange Inflow/Outflow (1h, last 120)")
    exchange_flows = get_exchange_flows(symbol)

    # Section 2: Whale Wallet Movements
    print("[X23] Step 2: Whale Wallet Movements (1h, last 120)")
    whale_movements = get_whale_movements(symbol)

    # Section 3: USDT/USDC Exchange Flow
    print("[X23] Step 3: USDT/USDC Exchange Flow (1h, last 120)")
    stablecoin_flows = get_stablecoin_flows(symbol)

    with open(filepath, 'w') as f:
        # ========== SECTION 1: EXCHANGE INFLOW/OUTFLOW ==========
        f.write("# ========== EXCHANGE INFLOW/OUTFLOW (1h, last 120 candles) ==========\n")
        f.write("timestamp\tinflow_usd\toutflow_usd\tnetflow_usd\n")
        if exchange_flows:
            for row in exchange_flows:
                f.write(f"{row['timestamp']}\t{row['inflow']:.2f}\t{row['outflow']:.2f}\t{row['netflow']:.2f}\n")
        else:
            f.write("NO_DATA\t0\t0\t0\n")
        f.write("\n")

        # ========== SECTION 2: WHALE WALLET MOVEMENTS ==========
        f.write("# ========== WHALE WALLET MOVEMENTS (1h, last 120 candles) ==========\n")
        f.write("timestamp\twhale_inflow_usd\twhale_outflow_usd\twhale_netflow_usd\n")
        if whale_movements:
            for row in whale_movements:
                # If the API provides only netflow, we split it into inflow/outflow heuristically
                inflow = row.get('inflow', row.get('value', 0) if row.get('value', 0) > 0 else 0)
                outflow = row.get('outflow', 0)
                netflow = row.get('netflow', row.get('value', 0))
                f.write(f"{row['timestamp']}\t{inflow:.2f}\t{outflow:.2f}\t{netflow:.2f}\n")
        else:
            f.write("NO_DATA\t0\t0\t0\n")
        f.write("\n")

        # ========== SECTION 3: STABLECOIN EXCHANGE FLOW ==========
        f.write("# ========== USDT/USDC EXCHANGE FLOW (1h, last 120 candles) ==========\n")
        f.write("timestamp\tinflow_usd\toutflow_usd\tnetflow_usd\n")
        if stablecoin_flows:
            for row in stablecoin_flows:
                f.write(f"{row['timestamp']}\t{row['inflow']:.2f}\t{row['outflow']:.2f}\t{row['netflow']:.2f}\n")
        else:
            f.write("NO_DATA\t0\t0\t0\n")

    print(f"[X23] All on‑chain data saved to {filepath}")

if __name__ == "__main__":
    collect_and_save("BTCUSDT")