"""
Z10_checker.py – Background pre‑flight scorer (5 sec interval, TSV cache)
No dummy data – uses "No data" for missing/neutral statuses.
Module availability check: only returns data for factors whose corresponding Z module exists.
Output file: market_data/checker.tsv (with header indicating Z‑group)
"""

import os
import time
import threading
from collections import deque
import logging

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
GROUPS_Z_DIR = os.path.join(BASE_DIR, "Groups", "group_z")
SYMBOLS_DIR = os.path.join(BASE_DIR, "market_data", "binance", "symbols")
NEWS_DIR = os.path.join(BASE_DIR, "market_data", "news")
CACHE_TSV = os.path.join(BASE_DIR, "market_data", "checker.tsv")
DEBUG_LOG = os.path.join(BASE_DIR, "market_data", "checker_debug.log")

# Configure logging with immediate flush
for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)
file_handler = logging.FileHandler(DEBUG_LOG, mode='a', encoding='utf-8')
file_handler.setLevel(logging.DEBUG)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
console_handler.setFormatter(formatter)
logging.root.addHandler(file_handler)
logging.root.addHandler(console_handler)
logging.root.setLevel(logging.DEBUG)
logger = logging.getLogger(__name__)

# Helper: check if a Z module file exists
def _is_z_module_available(module_filename):
    return os.path.exists(os.path.join(GROUPS_Z_DIR, module_filename))

def _read_last_lines(filepath, n=100):
    if not os.path.exists(filepath):
        return []
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            lines = deque(f, maxlen=n)
        return list(lines)
    except:
        return []

def _is_crypto(symbol):
    s = symbol.upper()
    return s.endswith(('USDT','USD')) or s in ['BTC','ETH','BNB','XRP','ADA','DOGE','SOL']

def _dynamic_tolerance(price, symbol):
    if _is_crypto(symbol):
        return price * 0.001
    else:
        return price * 0.0005

# ---------- Factor 1: Support & Resistance (25 pts) ----------
def _get_sr_score_and_status(symbol):
    filepath = os.path.join(SYMBOLS_DIR, f"{symbol.lower()}.tsv")
    lines = _read_last_lines(filepath, n=200)
    if len(lines) < 20:
        return 0, "No data"
    highs, lows, closes = [], [], []
    for line in lines:
        parts = line.strip().split('\t')
        if len(parts) >= 5:
            try:
                highs.append(float(parts[2]))
                lows.append(float(parts[3]))
                closes.append(float(parts[4]))
            except:
                pass
    if not closes:
        return 0, "No data"
    recent_high = max(highs[-50:]) if len(highs) >= 50 else max(highs)
    recent_low = min(lows[-50:]) if len(lows) >= 50 else min(lows)
    price = closes[-1]
    tolerance = _dynamic_tolerance(price, symbol)
    if abs(price - recent_high) <= tolerance:
        return 25, "Resistance"
    if abs(price - recent_low) <= tolerance:
        return 25, "Support"
    return 0, "No data"

# ---------- Factor 2: Orderflow Imbalance (20 pts) ----------
def _get_cvd_score_and_status(symbol):
    if not _is_z_module_available("Z03_momentum.py"):
        return 0, "No data"
    filepath = os.path.join(SYMBOLS_DIR, f"{symbol.lower()}_cvd.tsv")
    lines = _read_last_lines(filepath, n=20)
    if len(lines) < 5:
        return 0, "No data"
    deltas = []
    for line in lines:
        parts = line.strip().split('\t')
        if len(parts) >= 2:
            try:
                deltas.append(float(parts[1]))
            except:
                pass
    if len(deltas) < 5:
        return 0, "No data"
    changes = [deltas[i] - deltas[i-1] for i in range(1, len(deltas))]
    if not changes:
        return 0, "No data"
    recent_sum = sum(changes[-5:])
    avg_abs = sum(abs(c) for c in changes) / len(changes)
    if avg_abs > 0 and abs(recent_sum) > 0.1 * avg_abs * 5:
        return 20, "Strong"
    return 0, "No data"

# ---------- Factor 3: Technical Regime (Trend) (15 pts) ----------
def _get_trend_score_and_status(symbol):
    if not _is_z_module_available("Z0h_trend.py"):
        return 0, "No data"
    filepath = os.path.join(SYMBOLS_DIR, f"{symbol.lower()}.tsv")
    lines = _read_last_lines(filepath, n=100)
    if len(lines) < 30:
        return 0, "No data"
    closes = []
    for line in lines:
        parts = line.strip().split('\t')
        if len(parts) >= 5:
            try:
                closes.append(float(parts[4]))
            except:
                pass
    if len(closes) < 30:
        return 0, "No data"
    ema = sum(closes[-20:]) / 20
    price = closes[-1]
    if price > ema and closes[-1] > closes[-5]:
        return 15, "Bullish"
    elif price < ema and closes[-1] < closes[-5]:
        return 15, "Bearish"
    return 0, "No data"

# ---------- Factor 4: News Sentiment (10 pts) ----------
def _get_news_score_and_status(symbol):
    if not _is_z_module_available("Z01_news.py"):
        return 0, "No news", 0.0
    filepath = os.path.join(NEWS_DIR, f"{symbol.lower()}_news.tsv")
    if not os.path.exists(filepath):
        return 0, "No news", 0.0
    lines = _read_last_lines(filepath, n=20)
    if len(lines) < 3:
        return 0, "No news", 0.0
    error_msg = None
    sentiments = []
    for line in lines:
        if line.startswith('#error:'):
            error_msg = line.strip()
            break
        if line.startswith('#fetch_time') or line.startswith('timestamp'):
            continue
        parts = line.strip().split('\t')
        if len(parts) >= 5:
            try:
                sentiments.append(int(parts[4]))
            except:
                pass
    if error_msg:
        return 0, "Error", 0.0
    if not sentiments:
        return 0, "No news", 0.0
    avg = sum(sentiments) / len(sentiments)
    score = 10 if abs(avg) > 0.2 else 0
    if avg > 0.2:
        return score, "Bullish", avg
    elif avg < -0.2:
        return score, "Bearish", avg
    else:
        return 0, "No news", 0.0

# ---------- Factor 5: Volatility (10 pts) ----------
def _get_volatility_score_and_status(symbol):
    if not _is_z_module_available("Z02_volume.py"):
        return 0, "No data"
    filepath = os.path.join(SYMBOLS_DIR, f"{symbol.lower()}.tsv")
    lines = _read_last_lines(filepath, n=50)
    if len(lines) < 10:
        return 0, "No data"
    ranges = []
    for line in lines:
        parts = line.strip().split('\t')
        if len(parts) >= 4:
            try:
                high = float(parts[2])
                low = float(parts[3])
                ranges.append(high - low)
            except:
                pass
    if not ranges:
        return 0, "No data"
    avg_range = sum(ranges) / len(ranges)
    last_price = float(lines[-1].split('\t')[4]) if lines else 1
    vol_pct = avg_range / last_price * 100
    if vol_pct > 0.3:
        return 10, "High"
    else:
        return 0, "Low"

# ---------- Factor 6: Correlation (10 pts) ----------
def _get_correlation_score_and_status(symbol):
    filepath = os.path.join(SYMBOLS_DIR, f"{symbol.lower()}_correlation.tsv")
    if not os.path.exists(filepath):
        return 0, "No data"
    lines = _read_last_lines(filepath, n=5)
    if not lines:
        return 0, "No data"
    last_line = None
    for line in reversed(lines):
        if line.strip() and not line.startswith('timestamp'):
            last_line = line.strip()
            break
    if not last_line:
        return 0, "No data"
    parts = last_line.split('\t')
    if len(parts) < 3:
        return 0, "No data"
    try:
        corr_val = float(parts[2])
        if abs(corr_val) > 0.7:
            return 10, "High"
        elif abs(corr_val) > 0.3:
            return 5, "Medium"
        else:
            return 0, "Low"
    except (ValueError, IndexError):
        return 0, "No data"

# ---------- Factor 7: Spread & Cost (5 pts) ----------
def _get_spread_score_and_status(symbol):
    filepath = os.path.join(SYMBOLS_DIR, f"{symbol.lower()}_depth.tsv")
    lines = _read_last_lines(filepath, n=10)
    if not lines:
        return 0, "No data"
    best_bid, best_ask = None, None
    for line in lines:
        parts = line.strip().split('\t')
        if len(parts) >= 3:
            typ = parts[0]
            try:
                price = float(parts[1])
            except:
                continue
            if typ == 'bid':
                if best_bid is None or price > best_bid:
                    best_bid = price
            elif typ == 'ask':
                if best_ask is None or price < best_ask:
                    best_ask = price
    if best_bid is None or best_ask is None or best_ask <= best_bid:
        return 0, "No data"
    spread_pct = (best_ask - best_bid) / best_bid * 100
    if spread_pct < 0.05:
        return 5, "No data"
    else:
        return 0, "No data"

# ---------- Factor 8: ADR Exhaustion (5 pts) ----------
def _get_adr_score_and_status(symbol):
    filepath = os.path.join(SYMBOLS_DIR, f"{symbol.lower()}.tsv")
    lines = _read_last_lines(filepath, n=2000)
    if len(lines) < 100:
        return 0, "No data"
    day_high = 0
    day_low = float('inf')
    for line in lines[-1440:]:
        parts = line.strip().split('\t')
        if len(parts) >= 4:
            try:
                high = float(parts[2])
                low = float(parts[3])
                day_high = max(day_high, high)
                day_low = min(day_low, low)
            except:
                pass
    if day_low == float('inf'):
        return 0, "No data"
    current_range = day_high - day_low
    try:
        last_candle = lines[-1].strip().split('\t')
        last_close = float(last_candle[4]) if len(last_candle) > 4 else 1.0
    except:
        last_close = 1.0
    if last_close <= 0:
        last_close = 1.0
    range_pct = current_range / last_close
    threshold = 0.05 if _is_crypto(symbol) else 0.015
    if range_pct > threshold:
        return 0, "No data"
    return 5, "No data"

# ---------- Main scoring ----------
def get_preflight_score(symbol):
    sr_score, sr_status = _get_sr_score_and_status(symbol)
    of_score, of_status = _get_cvd_score_and_status(symbol)
    trend_score, trend_status = _get_trend_score_and_status(symbol)
    news_score, news_status, news_sentiment = _get_news_score_and_status(symbol)
    vol_score, vol_status = _get_volatility_score_and_status(symbol)
    corr_score, corr_status = _get_correlation_score_and_status(symbol)
    spread_score, spread_status = _get_spread_score_and_status(symbol)
    adr_score, adr_status = _get_adr_score_and_status(symbol)
    
    total = sr_score + of_score + trend_score + news_score + vol_score + corr_score + spread_score + adr_score
    total = min(100, total)
    decision = "Trigger Deep Research (Fill)" if total >= 80 else "Discard - Low Confluence"
    
    return {
        "total": total,
        "decision": decision,
        "news_status": news_status,
        "news_sentiment": news_sentiment,
        "statuses": {
            "sr": sr_status,
            "orderflow": of_status,
            "trend": trend_status,
            "news": news_status,
            "volatility": vol_status,
            "correlation": corr_status,
            "spread": spread_status,
            "adr": adr_status
        }
    }

# ---------- Background updater ----------
_background_started = False

def start_background_updater(symbols_list, interval_sec=5):
    global _background_started
    if _background_started:
        return
    _background_started = True

    def wait_for_news():
        while True:
            try:
                if _is_z_module_available("Z01_news.py"):
                    from Groups.group_z.Z01_news import is_news_ready
                    if is_news_ready():
                        logging.info("News module ready, starting background updates")
                        print("[Checker] News module ready, starting background updates")
                        return
                else:
                    logging.info("No news module, starting background updates without news")
                    print("[Checker] No news module, starting background updates without news")
                    return
            except:
                pass
            time.sleep(2)

    def updater():
        wait_for_news()
        logging.info("Background updater started")
        time.sleep(2)
        while True:
            start_time = time.time()
            logging.debug("Starting scoring cycle")
            results = {}
            for sym in symbols_list:
                clean = sym.upper().replace('/', '').replace(' (OTC)', '')
                try:
                    pf = get_preflight_score(clean)
                    news_word = pf['news_status']
                    results[clean] = (pf, news_word)
                except Exception as e:
                    logging.error(f"Error for {clean}: {e}")
                    print(f"[Checker] Error for {clean}: {e}")
            try:
                os.makedirs(os.path.dirname(CACHE_TSV), exist_ok=True)
                with open(CACHE_TSV, 'w', encoding='utf-8') as f:
                    # ========== ADD HEADER COMMENT INDICATING Z‑GROUP ==========
                    f.write("# Z-group pre-flight scores (generated by Z10_checker.py)\n")
                    f.write("timestamp\tsymbol\ttotal_score\tdecision\tsr\tregime\tnews\tvol\tcorr\tspread\torderflow\tadr\tnews_sentiment\tnews_word\n")
                    now = int(time.time())
                    for sym, (data, word) in results.items():
                        statuses = data['statuses']
                        line = f"{now}\t{sym}\t{data['total']}\t{data['decision']}\t{statuses['sr']}\t{statuses['trend']}\t{statuses['news']}\t{statuses['volatility']}\t{statuses['correlation']}\t{statuses['spread']}\t{statuses['orderflow']}\t{statuses['adr']}\t{data['news_sentiment']}\t{word}\n"
                        f.write(line)
                logging.debug(f"Checker.tsv written with {len(results)} entries")
                print(f"[Checker] Successfully wrote {len(results)} entries to checker.tsv")
            except Exception as e:
                logging.error(f"TSV write error: {e}")
                print(f"[Checker] TSV write error: {e}")
            elapsed = time.time() - start_time
            sleep_time = max(0, interval_sec - elapsed)
            logging.debug(f"Cycle took {elapsed:.2f}s, sleeping {sleep_time:.2f}s")
            time.sleep(sleep_time)

    thread = threading.Thread(target=updater, daemon=True)
    thread.start()

def get_gatekeeper(symbol, z_score=None, a_score=None, interval=None, rows=None):
    pf = get_preflight_score(symbol)
    return {
        "score_mod": 0,
        "conclusion": pf['decision'],
        "color": "#ffaa44" if pf['total'] >= 80 else "#888",
        "preflight_score": pf['total'],
        "statuses": pf['statuses'],
        "news_status": pf['news_status'],
        "news_sentiment": pf['news_sentiment']
    }