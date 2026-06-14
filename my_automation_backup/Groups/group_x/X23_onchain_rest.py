#!/usr/bin/env python3
"""
X23_onchain_rest.py – Raw On‑Chain Data Downloader (Only .tmp_x)
- Fetches stablecoin inflow (USDT, USDC) from DeFiLlama (cached 5 min)
- Fetches Binance futures metrics (liquidations, whale ratio, taker ratio, funding rate, OI) in parallel
- Fetches spot depth imbalance, ATR, current price
- Fetches exchange netflow (BTC/ETH only)
- Fetches whale alerts from 10+ public APIs
- Writes raw TSV: {symbol}_onchain.tmp_x (overwrites each call)
- Logs to market_data/binance/symbols/X23_onchain.log
- No prediction, no scoring, no TOON.
"""

import os
import sys
import time
import json
import glob
import requests
import math
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# ========== CONFIGURATION ==========
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SYMBOLS_DIR = os.path.join(BASE_DIR, "market_data", "binance", "symbols")
os.makedirs(SYMBOLS_DIR, exist_ok=True)

# Log file name changed to X23_onchain.log
LOG_FILE = os.path.join(SYMBOLS_DIR, "X23_onchain.log")
LOG_MAX_SIZE = 5 * 1024 * 1024

CMC_API_KEY = "36a9dba86c4d49c7b74a0ca49728d7d2"
BINANCE_FUTURES_BASE = "https://fapi.binance.com"
BINANCE_SPOT_BASE = "https://api.binance.com/api/v3"

ETHERSCAN_API_KEY = "YOUR_ETHERSCAN_API_KEY"
BSCSCAN_API_KEY = "YOUR_BSCSCAN_API_KEY"
WHALE_ALERT_API_KEY = "YOUR_WHALE_ALERT_API_KEY"

# Global cache for stablecoin inflow (5 minutes)
_stable_cache = {"data": None, "timestamp": 0}
_STABLE_TTL = 300

# ========== LOGGING ==========
def rotate_log_if_needed():
    if not os.path.exists(LOG_FILE):
        return
    if os.path.getsize(LOG_FILE) > LOG_MAX_SIZE:
        try:
            with open(LOG_FILE, 'r', encoding='utf-8') as f:
                lines = f.readlines()
            keep_lines = lines[-5000:] if len(lines) > 5000 else lines
            with open(LOG_FILE, 'w', encoding='utf-8') as f:
                f.writelines(keep_lines)
        except:
            pass

def log_issue(level, msg, **kwargs):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"{timestamp} [{level}] {msg}"
    if kwargs:
        line += " " + str(kwargs)
    print(line)
    rotate_log_if_needed()
    with open(LOG_FILE, 'a', encoding='utf-8') as f:
        f.write(line + "\n")

# ========== STABLECOIN INFLOW (cached) ==========
def get_stablecoin_inflow():
    now = time.time()
    if _stable_cache["data"] is not None and (now - _stable_cache["timestamp"]) < _STABLE_TTL:
        return _stable_cache["data"]
    result = []
    # USDT via DeFiLlama
    try:
        url = "https://stablecoins.llama.fi/stablecoincharts/all?asset=1"
        resp = requests.get(url, timeout=15)
        if resp.status_code == 200:
            data = resp.json()
            if data and len(data) >= 2:
                curr = data[-1]
                prev = data[-2]
                curr_circ = float(curr.get('totalCirculating', {}).get('peggedUSD', 0))
                prev_circ = float(prev.get('totalCirculating', {}).get('peggedUSD', 0))
                netflow = curr_circ - prev_circ
                result.append({
                    "symbol": "USDT",
                    "circulating": curr_circ,
                    "inflow_24h": netflow if netflow > 0 else 0,
                    "outflow_24h": -netflow if netflow < 0 else 0,
                    "netflow": netflow
                })
                log_issue("INFO", f"USDT: circ={curr_circ:.0f}, netflow={netflow:.0f}")
    except Exception as e:
        log_issue("WARNING", f"DeFiLlama USDT error: {e}")
    # USDC
    try:
        url = "https://stablecoins.llama.fi/stablecoincharts/all?asset=2"
        resp = requests.get(url, timeout=15)
        if resp.status_code == 200:
            data = resp.json()
            if data and len(data) >= 2:
                curr = data[-1]
                prev = data[-2]
                curr_circ = float(curr.get('totalCirculating', {}).get('peggedUSD', 0))
                if curr_circ > 200_000_000_000:
                    raise ValueError("Suspicious USDC value")
                prev_circ = float(prev.get('totalCirculating', {}).get('peggedUSD', 0))
                netflow = curr_circ - prev_circ
                result.append({
                    "symbol": "USDC",
                    "circulating": curr_circ,
                    "inflow_24h": netflow if netflow > 0 else 0,
                    "outflow_24h": -netflow if netflow < 0 else 0,
                    "netflow": netflow
                })
                log_issue("INFO", f"USDC: circ={curr_circ:.0f}, netflow={netflow:.0f}")
    except Exception:
        # Fallback to CMC
        try:
            headers = {'X-CMC_PRO_API_KEY': CMC_API_KEY}
            url = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/quotes/latest"
            params = {'symbol': 'USDC', 'convert': 'USD'}
            resp = requests.get(url, headers=headers, params=params, timeout=15)
            if resp.status_code == 200:
                data = resp.json()
                if 'data' in data and 'USDC' in data['data']:
                    circ = float(data['data']['USDC']['quote']['USD']['circulating_supply'])
                    result.append({
                        "symbol": "USDC",
                        "circulating": circ,
                        "inflow_24h": 0,
                        "outflow_24h": 0,
                        "netflow": 0
                    })
                    log_issue("INFO", f"USDC fallback: circ={circ:.0f}")
        except Exception as e2:
            log_issue("WARNING", f"USDC fallback error: {e2}")
    _stable_cache["data"] = result
    _stable_cache["timestamp"] = now
    return result

# ========== BINANCE API FETCHERS ==========
def fetch_binance_endpoint(symbol, endpoint, params=None):
    url = f"{BINANCE_FUTURES_BASE}{endpoint}"
    if params is None:
        params = {}
    params["symbol"] = symbol.upper()
    try:
        r = requests.get(url, params=params, timeout=8)
        if r.status_code == 200:
            return r.json()
        else:
            log_issue("WARNING", f"Binance {endpoint} HTTP {r.status_code}")
    except Exception as e:
        log_issue("WARNING", f"Binance {endpoint} error: {e}")
    return None

def get_binance_liquidations(symbol):
    data = fetch_binance_endpoint(symbol, "/fapi/v1/allForceOrders", {"limit": 100})
    events = []
    if data:
        for item in data:
            qty = float(item.get('origQty', 0))
            if qty > 5.0:
                events.append({
                    "timestamp": item.get('time', 0),
                    "price": float(item.get('price', 0)),
                    "quantity": qty,
                    "side": item.get('side', '')
                })
    return events[:20]

def get_whale_ratio(symbol):
    data = fetch_binance_endpoint(symbol, "/futures/data/topLongShortPositionRatio", {"period": "5m", "limit": 1})
    if data and len(data):
        return float(data[0].get('longShortRatio', 0))
    return 0.0

def get_taker_ratio(symbol):
    data = fetch_binance_endpoint(symbol, "/futures/data/takerlongshortRatio", {"period": "5m", "limit": 1})
    if data and len(data):
        return float(data[0].get('longShortRatio', 0))
    return 0.0

def get_funding_rate(symbol):
    data = fetch_binance_endpoint(symbol, "/fapi/v1/premiumIndex")
    if data:
        return float(data.get('lastFundingRate', 0))
    return 0.0

def get_open_interest(symbol):
    data = fetch_binance_endpoint(symbol, "/fapi/v1/openInterest")
    if data:
        return float(data.get('openInterest', 0))
    return 0.0

def get_depth_imbalance(symbol, limit=100):
    url = f"{BINANCE_SPOT_BASE}/depth"
    params = {"symbol": symbol.upper(), "limit": limit}
    try:
        r = requests.get(url, params=params, timeout=8)
        if r.status_code == 200:
            data = r.json()
            bid_vol = sum(float(b[1]) for b in data['bids'])
            ask_vol = sum(float(a[1]) for a in data['asks'])
            total = bid_vol + ask_vol
            return (bid_vol - ask_vol) / total if total > 0 else 0
    except Exception as e:
        log_issue("WARNING", f"Depth error: {e}")
    return 0.0

def get_atr(symbol, period=14):
    url = f"{BINANCE_SPOT_BASE}/klines"
    params = {"symbol": symbol.upper(), "interval": "1h", "limit": period+1}
    try:
        r = requests.get(url, params=params, timeout=8)
        if r.status_code == 200:
            data = r.json()
            atr_sum = 0.0
            for i in range(1, len(data)):
                high = float(data[i][2])
                low = float(data[i][3])
                prev_close = float(data[i-1][4])
                tr = max(high-low, abs(high-prev_close), abs(low-prev_close))
                atr_sum += tr
            return atr_sum / period
    except Exception as e:
        log_issue("WARNING", f"ATR error: {e}")
    return 0.0

def get_current_price(symbol):
    try:
        r = requests.get(f"{BINANCE_SPOT_BASE}/ticker/price", params={"symbol": symbol.upper()}, timeout=5)
        if r.status_code == 200:
            return float(r.json().get('price', 0))
    except:
        pass
    return 0.0

# ========== WHALE DETECTION (public APIs) ==========
def get_dexscreener_whales(min_vol_usd=200_000):
    queries = ["USDT", "BTC", "ETH", "BNB", "SOL", "XRP", "DOGE", "MATIC"]
    whales = []
    for q in queries:
        try:
            url = f"https://api.dexscreener.com/latest/dex/search?q={q}"
            resp = requests.get(url, timeout=6)
            if resp.status_code == 200:
                data = resp.json()
                for pair in data.get('pairs', [])[:30]:
                    vol = float(pair.get('volume', {}).get('h24', 0))
                    if vol >= min_vol_usd:
                        base = pair.get('baseToken', {}).get('symbol', q)
                        whales.append({
                            "timestamp": int(time.time() * 1000),
                            "text": f"DEX Whale: {base} ${vol:,.0f} vol"
                        })
        except:
            pass
    return whales[:40]

def get_binance_whale_trades(symbol, min_qty=1.0):
    url = f"{BINANCE_SPOT_BASE}/trades"
    params = {"symbol": symbol.upper(), "limit": 200}
    try:
        r = requests.get(url, params=params, timeout=6)
        if r.status_code == 200:
            data = r.json()
            whales = []
            for t in data:
                qty = float(t['qty'])
                if qty >= min_qty:
                    price = float(t['price'])
                    side = "BUY" if t['isBuyerMaker'] == 'False' else "SELL"
                    whales.append({
                        "timestamp": t['time'],
                        "text": f"Binance Whale: {side} {qty:.2f} @ {price:.4f}"
                    })
            return whales[:30]
    except:
        pass
    return []

def get_blockchair_whales():
    whales = []
    coins = ["bitcoin", "ethereum", "litecoin", "dogecoin"]
    for coin in coins:
        try:
            url = f"https://api.blockchair.com/{coin}/transactions?q=output_value_usd>10000&limit=20"
            resp = requests.get(url, timeout=6)
            if resp.status_code == 200:
                data = resp.json()
                txs = data.get('data', {})
                for tx_id, tx in list(txs.items())[:20]:
                    value_usd = float(tx.get('output_value_usd', 0))
                    if value_usd > 10000:
                        whales.append({
                            "timestamp": int(tx.get('time', time.time())) * 1000,
                            "text": f"Blockchair ({coin.upper()}): ${value_usd:,.0f}"
                        })
        except:
            pass
    return whales[:40]

def get_coinglass_whales():
    whales = []
    try:
        resp = requests.get("https://api.coinglass.com/api/whale", timeout=6)
        if resp.status_code == 200:
            data = resp.json()
            for item in data.get('data', [])[:30]:
                whales.append({
                    "timestamp": item.get('time', int(time.time()*1000)),
                    "text": f"CoinGlass Whale: {item.get('symbol','')} {item.get('amount',0)} {item.get('side','')}"
                })
    except:
        pass
    return whales[:20]

def get_whale_alert():
    if WHALE_ALERT_API_KEY == "YOUR_WHALE_ALERT_API_KEY":
        return []
    whales = []
    try:
        url = f"https://api.whale-alert.io/v1/transactions?api_key={WHALE_ALERT_API_KEY}&limit=50"
        resp = requests.get(url, timeout=6)
        if resp.status_code == 200:
            data = resp.json()
            for tx in data.get('transactions', [])[:50]:
                whales.append({
                    "timestamp": tx.get('timestamp', int(time.time()*1000)),
                    "text": f"WhaleAlert: {tx.get('amount',0):.2f} {tx.get('symbol','')} moved"
                })
    except:
        pass
    return whales[:30]

def get_etherscan_whales():
    if ETHERSCAN_API_KEY == "YOUR_ETHERSCAN_API_KEY":
        return []
    whales = []
    tokens = [
        {"symbol": "USDT", "contract": "0xdAC17F958D2ee523a2206206994597C13D831ec7", "decimals": 6},
        {"symbol": "USDC", "contract": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48", "decimals": 6}
    ]
    for token in tokens:
        try:
            url = "https://api.etherscan.io/api"
            params = {
                "module": "account",
                "action": "tokentx",
                "contractaddress": token['contract'],
                "startblock": 0,
                "endblock": 99999999,
                "sort": "desc",
                "apikey": ETHERSCAN_API_KEY
            }
            resp = requests.get(url, params=params, timeout=6)
            if resp.status_code == 200:
                data = resp.json()
                if data.get('status') == '1':
                    for tx in data.get('result', [])[:30]:
                        value = float(tx['value']) / 10**token['decimals']
                        if value >= 2000:
                            whales.append({
                                "timestamp": int(tx['timeStamp']) * 1000,
                                "text": f"Etherscan Whale: {value:,.0f} {token['symbol']}"
                            })
        except:
            pass
    return whales[:30]

def get_bscscan_whales():
    if BSCSCAN_API_KEY == "YOUR_BSCSCAN_API_KEY":
        return []
    whales = []
    tokens = [
        {"symbol": "BUSD", "contract": "0xe9e7CEA3DedcA5984780Bafc599bD69ADd087D56", "decimals": 18},
        {"symbol": "USDT", "contract": "0x55d398326f99059fF775485246999027B3197955", "decimals": 18}
    ]
    for token in tokens:
        try:
            url = "https://api.bscscan.com/api"
            params = {
                "module": "account",
                "action": "tokentx",
                "contractaddress": token['contract'],
                "startblock": 0,
                "endblock": 99999999,
                "sort": "desc",
                "apikey": BSCSCAN_API_KEY
            }
            resp = requests.get(url, params=params, timeout=6)
            if resp.status_code == 200:
                data = resp.json()
                if data.get('status') == '1':
                    for tx in data.get('result', [])[:30]:
                        value = float(tx['value']) / 10**token['decimals']
                        if value >= 20000:
                            whales.append({
                                "timestamp": int(tx['timeStamp']) * 1000,
                                "text": f"BscScan Whale: {value:,.0f} {token['symbol']}"
                            })
        except:
            pass
    return whales[:30]

def get_mempool_whales():
    whales = []
    try:
        resp = requests.get("https://mempool.space/api/v1/transactions", timeout=6)
        if resp.status_code == 200:
            txs = resp.json()
            for tx in txs[:30]:
                total_out = sum(out['value'] for out in tx.get('vout', [])) / 1e8
                if total_out > 2:
                    whales.append({
                        "timestamp": tx['status']['block_time'] * 1000 if 'block_time' in tx['status'] else int(time.time()*1000),
                        "text": f"Mempool Whale: {total_out:.2f} BTC"
                    })
    except:
        pass
    return whales[:20]

def get_blockchain_com_whales():
    whales = []
    try:
        resp = requests.get("https://blockchain.info/unconfirmed-transactions?format=json", timeout=6)
        if resp.status_code == 200:
            data = resp.json()
            for tx in data.get('txs', [])[:30]:
                total_out = sum(out['value'] for out in tx.get('out', [])) / 1e8
                if total_out > 2:
                    whales.append({
                        "timestamp": tx['time'] * 1000,
                        "text": f"Blockchain.com Whale: {total_out:.2f} BTC"
                    })
    except:
        pass
    return whales[:20]

# ========== EXCHANGE NETFLOW (BTC/ETH only) ==========
def get_exchange_netflow(symbol):
    asset_map = {"BTCUSDT": "btc", "ETHUSDT": "eth"}
    asset = asset_map.get(symbol.upper())
    if not asset:
        return []
    try:
        url = f"https://community-api.coinmetrics.io/v4/timeseries/asset-metrics?assets={asset}&metrics=FlowInExUSD,FlowOutExUSD&frequency=1h&limit=120"
        resp = requests.get(url, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            rows = data.get('data', [])
            netflows = []
            for item in rows:
                ts_str = item.get('time')
                if not ts_str:
                    continue
                dt = datetime.fromisoformat(ts_str.replace('Z', '+00:00'))
                ts_ms = int(dt.timestamp() * 1000)
                inflow = float(item.get('FlowInExUSD', 0))
                outflow = float(item.get('FlowOutExUSD', 0))
                netflows.append({"timestamp": ts_ms, "netflow": inflow - outflow})
            netflows.sort(key=lambda x: x['timestamp'], reverse=True)
            return netflows[:120]
    except:
        pass
    return []

# ========== MAIN DOWNLOADER ==========
def run_download(symbol):
    log_issue("INFO", f"Starting on‑chain raw download for {symbol}")
    start_time = time.time()

    # 1. Stablecoin inflow (cached)
    stable = get_stablecoin_inflow()

    # 2. Parallel fetch of Binance endpoints
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {
            executor.submit(get_binance_liquidations, symbol): "liquidations",
            executor.submit(get_whale_ratio, symbol): "whale_ratio",
            executor.submit(get_taker_ratio, symbol): "taker_ratio",
            executor.submit(get_funding_rate, symbol): "funding_rate",
            executor.submit(get_open_interest, symbol): "oi",
            executor.submit(get_depth_imbalance, symbol): "depth",
            executor.submit(get_atr, symbol): "atr",
            executor.submit(get_current_price, symbol): "price"
        }
        results = {}
        for future in as_completed(futures):
            key = futures[future]
            try:
                results[key] = future.result()
            except Exception as e:
                log_issue("WARNING", f"{key} fetch error: {e}")
                results[key] = None if key != "price" else 0.0

    liquidations = results.get("liquidations", [])
    whale_ratio = results.get("whale_ratio", 0.0)
    taker_ratio = results.get("taker_ratio", 0.0)
    funding_rate = results.get("funding_rate", 0.0)
    oi = results.get("oi", 0.0)
    depth_imbalance = results.get("depth", 0.0)
    atr = results.get("atr", 0.0)
    current_price = results.get("price", 0.0)

    # 3. Exchange netflow
    netflow = get_exchange_netflow(symbol)

    # 4. Whale alerts (all APIs)
    all_whales = []
    all_whales.extend(get_dexscreener_whales(200_000))
    all_whales.extend(get_binance_whale_trades(symbol, 1.0))
    all_whales.extend(get_blockchair_whales())
    all_whales.extend(get_coinglass_whales())
    all_whales.extend(get_whale_alert())
    all_whales.extend(get_etherscan_whales())
    all_whales.extend(get_bscscan_whales())
    all_whales.extend(get_mempool_whales())
    all_whales.extend(get_blockchain_com_whales())
    all_whales = all_whales[:100]

    # Write to .tmp_x TSV (overwrites each call)
    ts = int(time.time() * 1000)
    tmp_x_path = os.path.join(SYMBOLS_DIR, f"{symbol.lower()}_onchain.tmp_x")
    with open(tmp_x_path, "w", encoding="utf-8") as f:
        # Header
        f.write("type\ttimestamp\tfield1\tfield2\tfield3\tfield4\tfield5\n")

        # Stablecoin snapshot (one row per stablecoin)
        for s in stable:
            f.write(f"stablecoin\t{ts}\t{s['symbol']}\t{s['circulating']:.0f}\t{s['inflow_24h']:.0f}\t{s['outflow_24h']:.0f}\t{s['netflow']:.0f}\n")

        # Binance metrics snapshot (single row)
        f.write(f"binance_snapshot\t{ts}\t{current_price:.2f}\t{oi:.2f}\t{funding_rate:.8f}\t{whale_ratio:.4f}\t{taker_ratio:.4f}\t{depth_imbalance:.4f}\t{atr:.2f}\n")

        # Whale events (one row per whale)
        for w in all_whales:
            f.write(f"whale\t{w['timestamp']}\t{w['text']}\t\t\t\t\n")

        # Exchange netflow events (one row per netflow point)
        for nf in netflow:
            f.write(f"exchange_netflow\t{nf['timestamp']}\t{nf['netflow']:.2f}\t\t\t\t\n")

        # Liquidation events
        for liq in liquidations:
            f.write(f"liquidation\t{liq['timestamp']}\t{liq['price']:.2f}\t{liq['quantity']:.2f}\t{liq['side']}\t\t\n")

    log_issue("INFO", f"Raw on‑chain data saved to {tmp_x_path}")
    elapsed = time.time() - start_time
    log_issue("INFO", f"Download complete in {elapsed:.2f}s")
    return True

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python X23_onchain_rest.py SYMBOL")
        sys.exit(1)
    success = run_download(sys.argv[1].upper())
    sys.exit(0 if success else 1)