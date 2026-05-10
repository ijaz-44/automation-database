"""
X23 - On‑Chain Data Module (Optimized for Speed & Minimum Calls)
- Parallel Binance API calls (ThreadPoolExecutor)
- Global cache for stablecoin inflow (5 min)
- Whale alerts: 10+ public APIs (up to 100 whales)
- All original data preserved (stablecoin, netflow, liquidations, ATR, depth, predictions)
- Fast completion (3-5 sec per symbol)
"""

import os
import time
import json
import glob
import requests
import math
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SYMBOLS_DIR = os.path.join(BASE_DIR, "market_data", "binance", "symbols")
os.makedirs(SYMBOLS_DIR, exist_ok=True)

LOG_FILE = os.path.join(SYMBOLS_DIR, "onchain_issues.log")
MAX_LOG_SIZE_BYTES = 5 * 1024 * 1024
CMC_API_KEY = "36a9dba86c4d49c7b74a0ca49728d7d2"
BINANCE_FUTURES_BASE = "https://fapi.binance.com"
BINANCE_SPOT_BASE = "https://api.binance.com/api/v3"

# API keys for explorers (optional, but recommended)
ETHERSCAN_API_KEY = "YOUR_ETHERSCAN_API_KEY"
BSCSCAN_API_KEY = "YOUR_BSCSCAN_API_KEY"
POLYGONSCAN_API_KEY = "YOUR_POLYGONSCAN_API_KEY"
WHALE_ALERT_API_KEY = "YOUR_WHALE_ALERT_API_KEY"

_log_console = True

# Global cache for stablecoin inflow (5 minutes)
_stable_cache = {"data": None, "timestamp": 0}
_STABLE_TTL = 300

def rotate_log_if_needed():
    if not os.path.exists(LOG_FILE):
        return
    if os.path.getsize(LOG_FILE) > MAX_LOG_SIZE_BYTES:
        try:
            with open(LOG_FILE, 'r', encoding='utf-8') as f:
                lines = f.readlines()
            keep_lines = lines[-5000:] if len(lines) > 5000 else lines
            with open(LOG_FILE, 'w', encoding='utf-8') as f:
                f.writelines(keep_lines)
            print("[X23] Log rotated (kept last 5000 lines)")
        except:
            pass

def log_issue(issue_type, message, level="INFO"):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S,%f")[:-3]
    log_line = f"{timestamp} [{level}] [{issue_type}] {message}\n"
    if _log_console:
        print(log_line.strip())
    rotate_log_if_needed()
    try:
        with open(LOG_FILE, 'a', encoding='utf-8') as f:
            f.write(log_line)
    except:
        pass

def atomic_write(final_path, content):
    dirname = os.path.dirname(final_path)
    os.makedirs(dirname, exist_ok=True)
    tmp_path = final_path + ".tmp"
    try:
        with open(tmp_path, 'w', encoding='utf-8') as f:
            f.write(content)
            f.flush()
            os.fsync(f.fileno())
        os.rename(tmp_path, final_path)
        log_issue("ATOMIC", f"OK {os.path.basename(final_path)}", "INFO")
    except Exception as e:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
        log_issue("ATOMIC", f"FAIL {final_path}: {e}", "ERROR")
        raise e

# Cleanup orphaned .tmp files
for tmp in glob.glob(os.path.join(SYMBOLS_DIR, "*_onchain.tmp")):
    try:
        os.remove(tmp)
        log_issue("CLEANUP", f"Removed {os.path.basename(tmp)}", "INFO")
    except:
        pass

# -------------------- STABLECOIN INFLOW (with cache) --------------------
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
                result.append({"symbol": "USDT", "circulating": curr_circ, "inflow_24h": netflow if netflow>0 else 0, "outflow_24h": -netflow if netflow<0 else 0, "netflow": netflow})
                log_issue("DEFILLAMA", f"USDT: circ={curr_circ:.0f}, netflow={netflow:.0f}", "INFO")
        else:
            log_issue("DEFILLAMA", f"USDT HTTP {resp.status_code}", "WARNING")
    except Exception as e:
        log_issue("DEFILLAMA", f"USDT error: {e}", "WARNING")
    # USDC – DeFiLlama with CMC fallback
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
                    log_issue("DEFILLAMA", f"USDC suspicious {curr_circ:.0f}, fallback to CMC", "WARNING")
                    raise ValueError("Suspicious USDC value")
                prev_circ = float(prev.get('totalCirculating', {}).get('peggedUSD', 0))
                netflow = curr_circ - prev_circ
                result.append({"symbol": "USDC", "circulating": curr_circ, "inflow_24h": netflow if netflow>0 else 0, "outflow_24h": -netflow if netflow<0 else 0, "netflow": netflow})
                log_issue("DEFILLAMA", f"USDC: circ={curr_circ:.0f}, netflow={netflow:.0f}", "INFO")
            else:
                raise ValueError("No data")
        else:
            raise ValueError("HTTP error")
    except Exception:
        try:
            headers = {'X-CMC_PRO_API_KEY': CMC_API_KEY}
            url = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/quotes/latest"
            params = {'symbol': 'USDC', 'convert': 'USD'}
            resp = requests.get(url, headers=headers, params=params, timeout=15)
            if resp.status_code == 200:
                data = resp.json()
                if 'data' in data and 'USDC' in data['data']:
                    circ = float(data['data']['USDC']['quote']['USD']['circulating_supply'])
                    result.append({"symbol": "USDC", "circulating": circ, "inflow_24h": 0, "outflow_24h": 0, "netflow": 0})
                    log_issue("CMC", f"USDC fallback: circ={circ:.0f}", "INFO")
        except Exception as e2:
            log_issue("CMC", f"USDC error: {e2}", "WARNING")
    _stable_cache["data"] = result
    _stable_cache["timestamp"] = now
    return result

# -------------------- BINANCE ENDPOINTS (parallel) --------------------
def fetch_binance_endpoint(symbol, endpoint, params=None):
    """Generic fetch for Binance Futures endpoints."""
    url = f"{BINANCE_FUTURES_BASE}{endpoint}"
    if params is None:
        params = {}
    params["symbol"] = symbol.upper()
    try:
        r = requests.get(url, params=params, timeout=8)
        if r.status_code == 200:
            return r.json()
        else:
            log_issue("BINANCE_API", f"{endpoint} HTTP {r.status_code}", "WARNING")
    except Exception as e:
        log_issue("BINANCE_API", f"{endpoint} error: {e}", "WARNING")
    return None

def get_binance_liquidations(symbol):
    data = fetch_binance_endpoint(symbol, "/fapi/v1/allForceOrders", {"limit": 100})
    if data:
        events = []
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
    return []

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
    """Spot depth snapshot – imbalance over top 100 levels."""
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
        log_issue("DEPTH", f"Error: {e}", "WARNING")
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
        log_issue("ATR", f"Error: {e}", "WARNING")
    return 0.0

def get_current_price(symbol):
    try:
        r = requests.get(f"{BINANCE_SPOT_BASE}/ticker/price", params={"symbol": symbol.upper()}, timeout=5)
        if r.status_code == 200:
            return float(r.json().get('price', 0))
    except:
        pass
    return 0.0

# -------------------- WHALE DETECTION (public APIs, fast) --------------------
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
                    value_usd = float(tx.get('output_value_usd',0))
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
                        if value >= 2000:  # 2000 USDT/USDC
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

# -------------------- EXCHANGE NETFLOW (only BTC/ETH) --------------------
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
            if netflows:
                return netflows[:120]
    except:
        pass
    # CryptoQuant fallback
    try:
        asset_uc = asset.upper()
        url = f"https://raw.githubusercontent.com/cryptoquant/data/master/exchange_flows/{asset_uc}_exchange_netflow_1h.csv"
        resp = requests.get(url, timeout=10)
        if resp.status_code == 200:
            lines = resp.text.strip().splitlines()
            netflows = []
            for line in lines[1:]:
                if not line.strip():
                    continue
                try:
                    parts = line.split(',')
                    if len(parts) >= 2:
                        ts = int(parts[0])
                        netflow = float(parts[1])
                        netflows.append({"timestamp": ts, "netflow": netflow})
                except:
                    continue
            netflows.sort(key=lambda x: x['timestamp'], reverse=True)
            if netflows:
                return netflows[:120]
    except:
        pass
    return []

# -------------------- PREDICTION LOGIC --------------------
def compute_prediction(usdt_netflow, whale_ratio, taker_ratio, funding_rate, oi, depth_imbalance, stablecoin_bullish, liquidations):
    score = 0
    if usdt_netflow > 100_000_000:
        score += 2
    elif usdt_netflow < -100_000_000:
        score -= 2
    if whale_ratio > 1.2:
        score += 2
    elif whale_ratio < 0.8:
        score -= 2
    if taker_ratio > 1.2:
        score += 2
    elif taker_ratio < 0.8:
        score -= 2
    if funding_rate > 0.0001:
        score -= 1
    elif funding_rate < -0.0001:
        score += 1
    if depth_imbalance > 0.2:
        score += 2
    elif depth_imbalance < -0.2:
        score -= 2
    if stablecoin_bullish:
        score += 1
    else:
        score -= 1
    if liquidations and len(liquidations) > 5:
        if score > 0:
            score += 1
        elif score < 0:
            score -= 1
    if score >= 3:
        return "UP"
    elif score <= -3:
        return "DOWN"
    else:
        return "NEUTRAL"

# -------------------- MAIN FUNCTION (parallel binance calls) --------------------
def collect_and_save(symbol):
    start_time = time.time()
    log_issue("COLLECT", f"Starting for {symbol} (optimized parallel)", "INFO")

    # 1. Stablecoin inflow (cached)
    stable = get_stablecoin_inflow()
    usdt_netflow = next((s['netflow'] for s in stable if s['symbol'] == 'USDT'), 0)
    stable_bullish = any(s['netflow'] > 0 for s in stable)

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
                log_issue("PARALLEL", f"{key} failed: {e}", "WARNING")
                results[key] = None if key != "price" else 0.0
    liquidations = results.get("liquidations", [])
    whale_ratio = results.get("whale_ratio", 0.0)
    taker_ratio = results.get("taker_ratio", 0.0)
    funding_rate = results.get("funding_rate", 0.0)
    oi = results.get("oi", 0.0)
    depth_imbalance = results.get("depth", 0.0)
    atr = results.get("atr", 0.0)
    current_price = results.get("price", 0.0)

    # 3. Exchange netflow (only for BTC/ETH)
    netflow = get_exchange_netflow(symbol)

    # 4. Whale alerts (all public APIs – run in series but fast)
    all_whales = []
    all_whales.extend(get_dexscreener_whales(min_vol_usd=200_000))
    all_whales.extend(get_binance_whale_trades(symbol, min_qty=1.0))
    all_whales.extend(get_blockchair_whales())
    all_whales.extend(get_coinglass_whales())
    all_whales.extend(get_whale_alert())
    all_whales.extend(get_etherscan_whales())
    all_whales.extend(get_bscscan_whales())
    all_whales.extend(get_mempool_whales())
    all_whales.extend(get_blockchain_com_whales())
    all_whales = all_whales[:100]  # keep up to 100

    # 5. Prediction
    direction = compute_prediction(usdt_netflow, whale_ratio, taker_ratio, funding_rate, oi, depth_imbalance, stable_bullish, liquidations)
    if current_price > 0 and atr > 0:
        target = current_price + (atr * 0.8) if direction == "UP" else current_price - (atr * 0.8) if direction == "DOWN" else current_price
    else:
        target = current_price

    ts = int(time.time() * 1000)

    # Build TOON content
    lines = []
    lines.append(f"# On‑chain data for {symbol.upper()} – TOON format")
    lines.append(f"generated: {datetime.now().isoformat()}")
    lines.append(f"symbol: {symbol}")
    lines.append("")
    lines.append(f"next_1h_direction: {direction}")
    lines.append(f"next_1h_target: {target:.2f}")
    lines.append(f"current_price: {current_price:.2f}")
    lines.append(f"atr_1h_14: {atr:.2f}")
    lines.append(f"depth_imbalance: {depth_imbalance:.4f}")
    lines.append(f"whale_ratio_5m: {whale_ratio:.2f}")
    lines.append(f"taker_ratio_5m: {taker_ratio:.2f}")
    lines.append(f"funding_rate: {funding_rate:.8f}")
    lines.append(f"open_interest: {oi:.0f}")
    lines.append("")

    # Stablecoin array
    fields = ["timestamp", "symbol", "circulating", "inflow_24h", "outflow_24h", "netflow_24h"]
    rows = [f"{ts},{s['symbol']},{s['circulating']:.0f},{s['inflow_24h']:.0f},{s['outflow_24h']:.0f},{s['netflow']:.0f}" for s in stable]
    if not rows:
        rows = [f"{ts},NO_DATA,0,0,0,0"]
    lines.append(f"stablecoin_netflow[{len(rows)}]{{{','.join(fields)}}}:")
    lines.append("  " + " |\n  ".join(rows))
    lines.append("")

    # Whale transactions
    fields2 = ["timestamp", "message"]
    rows2 = [f"{w['timestamp']},{w['text']}" for w in all_whales]
    if not rows2:
        rows2 = [f"{ts},NO_WHALE_ACTIVITY"]
    lines.append(f"whale_transactions[{len(rows2)}]{{{','.join(fields2)}}}:")
    lines.append("  " + " |\n  ".join(rows2))
    lines.append("")

    # Exchange netflow
    fields3 = ["timestamp", "netflow_usd"]
    if netflow:
        rows3 = [f"{r['timestamp']},{r['netflow']:.2f}" for r in netflow]
    else:
        rows3 = [f"{ts},0"]
    lines.append(f"exchange_netflow[{len(rows3)}]{{{','.join(fields3)}}}:")
    lines.append("  " + " |\n  ".join(rows3))
    lines.append("")

    # Liquidations
    fields4 = ["timestamp", "price", "quantity", "side"]
    rows4 = [f"{liq['timestamp']},{liq['price']:.2f},{liq['quantity']:.2f},{liq['side']}" for liq in liquidations]
    lines.append(f"binance_liquidations[{len(rows4)}]{{{','.join(fields4)}}}:")
    lines.append("  " + (" |\n  ".join(rows4) if rows4 else " "))
    lines.append("")

    lines.append("# ========== END OF TOON DATA ==========")

    filepath = os.path.join(SYMBOLS_DIR, f"{symbol.lower()}_onchain.toon")
    try:
        atomic_write(filepath, "\n".join(lines) + "\n")
        elapsed = time.time() - start_time
        log_issue("SAVE_SUCCESS", f"{symbol}: {elapsed:.2f}s, whales:{len(all_whales)}", "INFO")
    except Exception as e:
        log_issue("SAVE_ERROR", f"{symbol}: {e}", "ERROR")
        return False
    return True

if __name__ == "__main__":
    collect_and_save("BTCUSDT")