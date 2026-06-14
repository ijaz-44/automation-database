#!/usr/bin/env python3
"""
X17_sentiment_rest.py – Raw Sentiment Data Downloader (Only .tmp_x)
- Fetches news sentiment (Free Crypto News API + CryptoPanic fallback)
- Fetches social velocity (ApeWisdom)
- Fetches funding rate history (Binance Futures)
- Fetches retail sentiment (Long/Short account ratio)
- Fetches open interest (current and 1h ago) and current price
- Writes raw TSV: {symbol}_sentiment.tmp_x
- Logs to market_data/binance/symbols/X17_sentiment.log (global, not per symbol)
- No processing, no TOON, no derived predictions.
"""

import os
import sys
import time
import requests

# ========== CONFIGURATION ==========
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SYMBOLS_DIR = os.path.join(BASE_DIR, "market_data", "binance", "symbols")
os.makedirs(SYMBOLS_DIR, exist_ok=True)

# **GLOBAL LOG FILE** (not per symbol)
LOG_FILE = os.path.join(SYMBOLS_DIR, "X17_sentiment.log")
LOG_MAX_SIZE = 5_000_000

# API Endpoints
FREE_CRYPTO_NEWS_API = "https://mcpmarket.com/api/news"
CRYPTOPANIC_API = "https://cryptopanic.com/api/v1/posts/"
APEWISDOM_API = "https://apewisdom.io/api/v1.0/all-crypto"
BINANCE_FUTURES_API = "https://fapi.binance.com/fapi/v1"
BINANCE_SPOT_API = "https://api.binance.com/api/v3"

# ========== LOGGING ==========
def rotate_log_if_needed():
    if os.path.exists(LOG_FILE) and os.path.getsize(LOG_FILE) > LOG_MAX_SIZE:
        backup = LOG_FILE + ".old"
        try:
            os.replace(LOG_FILE, backup)
        except:
            pass

def log_issue(level, msg, **kwargs):
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    line = f"{ts} [{level}] {msg}"
    if kwargs:
        line += " " + str(kwargs)
    print(line)
    rotate_log_if_needed()
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(line + "\n")

# ========== RATE LIMITED FETCH ==========
_last_call = 0
RATE_LIMIT_SEC = 1

def rate_limited_fetch(url, params=None, headers=None, timeout=10):
    global _last_call
    now = time.time()
    elapsed = now - _last_call
    if elapsed < RATE_LIMIT_SEC:
        time.sleep(RATE_LIMIT_SEC - elapsed)
    _last_call = time.time()
    try:
        r = requests.get(url, params=params, headers=headers, timeout=timeout)
        if r.status_code == 200:
            return r.json()
        else:
            log_issue("WARNING", f"HTTP {r.status_code} from {url[:50]}")
            return None
    except Exception as e:
        log_issue("WARNING", f"Request error: {e}")
        return None

# ---------- 1. News Sentiment ----------
def fetch_news_sentiment_raw(symbol):
    """Return (source, positive_count, negative_count, total_count)"""
    # Primary API
    params = {"symbol": symbol.upper(), "limit": 50}
    data = rate_limited_fetch(FREE_CRYPTO_NEWS_API, params=params)
    if data and "articles" in data:
        articles = data["articles"]
        pos_keywords = ["surge","bull","high","rise","positive","up","green","gain"]
        neg_keywords = ["drop","bear","low","fall","negative","down","red","loss"]
        pos = 0
        neg = 0
        for art in articles:
            title = art.get("title", "").lower()
            if any(kw in title for kw in pos_keywords):
                pos += 1
            if any(kw in title for kw in neg_keywords):
                neg += 1
        log_issue("INFO", f"News: free_crypto_news, pos={pos}, neg={neg}, total={pos+neg}")
        return "free_crypto_news", pos, neg, pos+neg

    # Fallback to CryptoPanic
    params = {"currencies": symbol.upper(), "limit": 50, "kind": "news"}
    data = rate_limited_fetch(CRYPTOPANIC_API, params=params)
    if data and "results" in data:
        results = data["results"]
        pos = 0
        neg = 0
        for item in results:
            sentiment_tag = item.get("sentiment", {}).get("sentiment", "")
            if sentiment_tag == "positive":
                pos += 1
            elif sentiment_tag == "negative":
                neg += 1
        log_issue("INFO", f"News: cryptopanic, pos={pos}, neg={neg}, total={pos+neg}")
        return "cryptopanic", pos, neg, pos+neg

    log_issue("WARNING", "No news data available")
    return "none", 0, 0, 0

# ---------- 2. Social Velocity ----------
def fetch_social_velocity_raw(symbol):
    """Return (mention_count, upvotes, velocity)"""
    data = rate_limited_fetch(APEWISDOM_API)
    if data and "data" in data:
        for item in data["data"]:
            if item.get("ticker") == symbol.upper():
                mentions = item.get("mentions", 0)
                upvotes = item.get("upvotes", 0)
                velocity = mentions + upvotes
                log_issue("INFO", f"Social: mentions={mentions}, upvotes={upvotes}, velocity={velocity}")
                return mentions, upvotes, velocity
    log_issue("WARNING", "No social data for symbol")
    return 0, 0, 0

# ---------- 3. Funding Rate History ----------
def fetch_funding_rate_history_raw(symbol):
    """Return list of dicts with 'timestamp', 'funding_rate'"""
    url = f"{BINANCE_FUTURES_API}/fundingRate"
    params = {"symbol": symbol.upper(), "limit": 20}
    data = rate_limited_fetch(url, params=params)
    if data:
        history = []
        for item in data:
            history.append({
                "timestamp": item.get("fundingTime", 0),
                "funding_rate": float(item.get("fundingRate", 0))
            })
        log_issue("INFO", f"Funding history: {len(history)} rows")
        return history
    log_issue("WARNING", "No funding history")
    return []

# ---------- 4. Retail Sentiment ----------
def fetch_retail_sentiment_raw(symbol):
    """Return (timestamp, long_account_pct, short_account_pct, long_short_ratio)"""
    url = f"{BINANCE_FUTURES_API}/topLongShortAccountRatio"
    params = {"symbol": symbol.upper(), "period": "1h", "limit": 1}
    data = rate_limited_fetch(url, params=params)
    if data and len(data) > 0:
        item = data[0]
        long_acc = float(item.get("longAccount", 0))
        short_acc = float(item.get("shortAccount", 0))
        ratio = float(item.get("longShortRatio", 0))
        ts = item.get("timestamp", int(time.time()*1000))
        log_issue("INFO", f"Retail: long={long_acc:.2f}%, short={short_acc:.2f}%, ratio={ratio:.2f}")
        return ts, long_acc, short_acc, ratio
    log_issue("WARNING", "No retail sentiment data")
    return 0, 0.0, 0.0, 0.0

# ---------- 5. Open Interest & Price ----------
def fetch_oi_current(symbol):
    url = f"{BINANCE_FUTURES_API}/openInterest"
    params = {"symbol": symbol.upper()}
    data = rate_limited_fetch(url, params=params)
    if data:
        return float(data.get('openInterest', 0))
    return 0.0

def fetch_oi_1h_ago(symbol):
    url = f"{BINANCE_FUTURES_API}/futures/data/openInterestHist"
    params = {"symbol": symbol.upper(), "period": "1h", "limit": 2}
    data = rate_limited_fetch(url, params=params)
    if data and len(data) >= 2:
        prev = float(data[1]['sumOpenInterest'])
        curr = float(data[0]['sumOpenInterest'])
        change_pct = ((curr - prev) / prev * 100) if prev > 0 else 0
        return prev, curr, change_pct
    return 0.0, 0.0, 0.0

def fetch_current_price(symbol):
    url = f"{BINANCE_SPOT_API}/ticker/price"
    params = {"symbol": symbol.upper()}
    data = rate_limited_fetch(url, params=params)
    if data:
        return float(data.get('price', 0))
    return 0.0

# ---------- Main Downloader ----------
def run_download(symbol):
    log_issue("INFO", f"Starting sentiment raw download for {symbol}")
    start = time.time()

    timestamp = int(time.time() * 1000)

    # Fetch all raw data
    news_source, news_pos, news_neg, news_total = fetch_news_sentiment_raw(symbol)
    mentions, upvotes, social_vel = fetch_social_velocity_raw(symbol)
    funding_hist = fetch_funding_rate_history_raw(symbol)
    retail_ts, retail_long_pct, retail_short_pct, retail_ratio = fetch_retail_sentiment_raw(symbol)
    oi_current = fetch_oi_current(symbol)
    oi_prev, oi_curr_hist, oi_change_pct = fetch_oi_1h_ago(symbol)
    price = fetch_current_price(symbol)

    # Write to .tmp_x TSV
    tmp_x_path = os.path.join(SYMBOLS_DIR, f"{symbol.lower()}_sentiment.tmp_x")
    with open(tmp_x_path, "w", encoding="utf-8") as f:
        # Header
        f.write("type\ttimestamp\tvalue1\tvalue2\tvalue3\tvalue4\tvalue5\n")

        # 1. News sentiment
        f.write(f"news\t{timestamp}\t{news_source}\t{news_pos}\t{news_neg}\t{news_total}\t\n")

        # 2. Social velocity
        f.write(f"social\t{timestamp}\t{mentions}\t{upvotes}\t{social_vel}\t\t\n")

        # 3. Funding rate history (one row per entry)
        for fr in funding_hist:
            f.write(f"funding_history\t{fr['timestamp']}\t{fr['funding_rate']:.8f}\t\t\t\t\n")

        # 4. Retail sentiment (latest)
        f.write(f"retail\t{retail_ts}\t{retail_long_pct:.2f}\t{retail_short_pct:.2f}\t{retail_ratio:.4f}\t\t\n")

        # 5. Snapshot (price, OI, OI change)
        f.write(f"snapshot\t{timestamp}\t{price:.4f}\t{oi_current:.2f}\t{oi_prev:.2f}\t{oi_curr_hist:.2f}\t{oi_change_pct:.2f}\n")

    log_issue("INFO", f"Raw sentiment data saved to {tmp_x_path}")
    elapsed = time.time() - start
    log_issue("INFO", f"Download complete in {elapsed:.2f}s")
    return True

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python X17_sentiment_rest.py SYMBOL")
        sys.exit(1)
    success = run_download(sys.argv[1].upper())
    sys.exit(0 if success else 1)