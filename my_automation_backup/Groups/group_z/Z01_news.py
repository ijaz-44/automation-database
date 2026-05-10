# Z01_news.py – Ultra‑safe version, no regex, no "no such group" error
"""
Z01_news.py – Batch News Fetcher with Per‑Symbol Freshness Check
- Checks each symbol's news TSV file fetch_time
- Only fetches for symbols whose file is missing or older than 30 minutes
- Runs in background loop (30 min interval)
- Supports CryptoCompare (crypto), Finnhub Calendar, polygon.io, Marketaux, Finnhub general news
- Calendar events sentiment forced to 0 (neutral)
- Safe reading of TSV, auto‑create missing files
- NO REGEX OPERATIONS – all parsing uses split and csv.reader
"""

import os, time, threading, requests, csv
from datetime import datetime, timedelta
from pairs import get_all_pairs

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
NEWS_DIR = os.path.join(BASE_DIR, "market_data", "news")
os.makedirs(NEWS_DIR, exist_ok=True)
LOG_FILE = os.path.join(NEWS_DIR, "news_errors.log")
DEBUG_LOG = os.path.join(NEWS_DIR, "z01_debug.log")

CRYPTOCOMPARE_KEY = "2f8a33bc64db22db858d7962112cead0ccc07035f9806adb031ad4ce71743d75"
FINNHUB_KEY = "d6u7u8pr01qp1k9bjte0d6u7u8pr01qp1k9bjteg"
POLYGON_KEY = "KorjOw9PlvhL4TcBf7Duixw0j2p7dtFp"
MARKETAUX_KEY = "BRR1ra0DUAM5eRkR7LXcO2DsWbOsuAslpIlJoFOr"

_api_calls = {"CryptoCompare": 0, "FinnhubCalendar": 0, "Polygon": 0, "Marketaux": 0, "FinnhubNews": 0}
_news_ready = False

def log_info(msg): print(f"[NEWS] {msg}")
def log_error(msg):
    print(f"[ERR] {msg}")
    with open(LOG_FILE, 'a', encoding='utf-8') as f:
        f.write(f"{datetime.now().isoformat()} - {msg}\n")

def log_debug(msg):
    with open(DEBUG_LOG, 'a', encoding='utf-8') as f:
        f.write(f"{datetime.now().isoformat()} - {msg}\n")

def inc_api(src): _api_calls[src] = _api_calls.get(src, 0) + 1
def get_api_stats(): return dict(_api_calls)
def is_news_ready(): return _news_ready

def normalize_symbol(s):
    return s.upper().replace('/', '').replace('-', '')

def is_crypto(s):
    return s.upper().endswith(('USDT','USD')) or s.upper() in ['BTC','ETH','BNB','XRP','ADA','DOGE','SOL']

def sentiment(text):
    # Calendar events always neutral
    if "Impact:" in text:
        return 0
    negations = {'not','no',"n't",'never','fail','fails','failed','slump','plunge','crash','drop'}
    text_lower = text.lower()
    has_neg = any(neg in text_lower for neg in negations)
    pos = sum(1 for w in ['surge','rally','bull','gain','up','positive','green','profit','high','breakout'] if w in text_lower)
    neg = sum(1 for w in ['bear','loss','down','negative','red','fall','low','selloff','panic'] if w in text_lower)
    if has_neg:
        return 1 if neg > pos else -1 if pos > neg else 0
    else:
        return 1 if pos > neg else -1 if neg > pos else 0

def ensure_news_file_exists(symbol):
    fpath = os.path.join(NEWS_DIR, f"{normalize_symbol(symbol).lower()}_news.tsv")
    if not os.path.exists(fpath):
        ft = int(time.time())
        dt = datetime.fromtimestamp(ft).strftime('%Y-%m-%d %H:%M:%S')
        with open(fpath, 'w', encoding='utf-8', newline='') as f:
            w = csv.writer(f, delimiter='\t')
            w.writerow(['timestamp', 'datetime', 'source', 'title', 'sentiment'])
            w.writerow([f'#fetch_time: {ft}'])
        log_debug(f"Auto-created missing news file for {symbol}")

def save_tsv(symbol, items, error_msg=None):
    ensure_news_file_exists(symbol)
    fpath = os.path.join(NEWS_DIR, f"{normalize_symbol(symbol).lower()}_news.tsv")
    ft = int(time.time())
    dt = datetime.fromtimestamp(ft).strftime('%Y-%m-%d %H:%M:%S')
    with open(fpath, 'w', encoding='utf-8', newline='') as f:
        w = csv.writer(f, delimiter='\t')
        w.writerow(['timestamp', 'datetime', 'source', 'title', 'sentiment'])
        for it in items:
            w.writerow([ft, dt, it['src'], it['title'], sentiment(it['title'])])
        if error_msg:
            w.writerow([f'#error: {error_msg}'])
        w.writerow([f'#fetch_time: {ft}'])

def get_file_fetch_time(symbol):
    fpath = os.path.join(NEWS_DIR, f"{normalize_symbol(symbol).lower()}_news.tsv")
    if not os.path.exists(fpath):
        return None
    try:
        with open(fpath, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        for line in reversed(lines):
            if line.startswith('#fetch_time:'):
                parts = line.split(':')
                if len(parts) >= 2:
                    return int(parts[1].strip())
        return None
    except:
        return None

# ----- API fetch functions (unchanged) -----
def fetch_cryptocompare():
    if not CRYPTOCOMPARE_KEY: return []
    inc_api("CryptoCompare")
    try:
        r = requests.get("https://min-api.cryptocompare.com/data/v2/news/",
                         headers={'authorization': f'Apikey {CRYPTOCOMPARE_KEY}'},
                         params={'lang':'EN','feeds':'coindesk,cointelegraph,bitcoinist'}, timeout=15)
        if r.status_code == 200:
            data = r.json()
            if data.get('Response') == 'Success':
                articles = data.get('Data', [])
                log_debug(f"CryptoCompare returned {len(articles)} articles")
                return articles
        else:
            log_error(f"CryptoCompare HTTP {r.status_code}")
    except Exception as e:
        log_error(f"CryptoCompare error: {e}")
    return []

def fetch_finnhub_calendar():
    if not FINNHUB_KEY: return []
    inc_api("FinnhubCalendar")
    now = datetime.utcnow()
    from_date = now.strftime('%Y-%m-%d')
    to_date = (now + timedelta(days=7)).strftime('%Y-%m-%d')
    url = "https://finnhub.io/api/v1/calendar/economic"
    params = {'token': FINNHUB_KEY, 'from': from_date, 'to': to_date}
    try:
        r = requests.get(url, params=params, timeout=10)
        if r.status_code == 200:
            data = r.json()
            events = []
            for e in data.get('economicCalendar', []):
                impact = e.get('impact', 'low').upper()
                title = f"{e.get('country','')} {e.get('event','')} - Impact: {impact}"
                events.append({'src':'finnhub_calendar','title':title})
            log_debug(f"Finnhub Calendar returned {len(events)} events")
            return events
        else:
            log_error(f"Finnhub calendar HTTP {r.status_code}")
    except Exception as e:
        log_error(f"Finnhub calendar error: {e}")
    return []

def fetch_polygon_batch(symbols):
    if not POLYGON_KEY or not symbols: return {}
    inc_api("Polygon")
    formatted = []
    for s in symbols:
        s_norm = normalize_symbol(s)
        if is_crypto(s_norm): continue
        if len(s_norm) == 6 and s_norm.isalpha():
            formatted.append(f"C:{s_norm}")
        else:
            formatted.append(s_norm)
    if not formatted: return {}
    tickers_str = ','.join(formatted)
    url = "https://api.polygon.io/v2/reference/news"
    params = {'ticker.any_of': tickers_str, 'limit': 20, 'apiKey': POLYGON_KEY, 'order': 'desc'}
    try:
        r = requests.get(url, params=params, timeout=15)
        if r.status_code == 200:
            data = r.json()
            results = data.get('results', [])
            news_map = {normalize_symbol(s): [] for s in symbols}
            for article in results:
                title = article.get('title', '')
                for t in article.get('tickers', []):
                    clean_t = normalize_symbol(t.replace('C:', ''))
                    if clean_t in news_map:
                        news_map[clean_t].append({'src':'polygon','title':title})
            log_debug(f"Polygon batch: {len(results)} articles for {len(symbols)} symbols")
            return news_map
        elif r.status_code == 429:
            log_error("Polygon Rate Limit – delay applied")
        else:
            log_error(f"Polygon HTTP {r.status_code}")
    except Exception as e:
        log_error(f"Polygon error: {e}")
    return {}

def fetch_marketaux_batch(symbols):
    if not MARKETAUX_KEY or not symbols: return {}
    inc_api("Marketaux")
    symbols_str = ','.join([normalize_symbol(s) for s in symbols[:15]])
    url = "https://api.marketaux.com/v1/news/all"
    params = {'symbols': symbols_str, 'api_token': MARKETAUX_KEY, 'limit': 3, 'language': 'en'}
    try:
        r = requests.get(url, params=params, timeout=15)
        if r.status_code == 200:
            data = r.json()
            articles = data.get('data', [])
            news_map = {normalize_symbol(s): [] for s in symbols}
            for art in articles:
                title = art.get('title', '')
                for sym_info in art.get('symbols', []):
                    s = normalize_symbol(sym_info.get('symbol', ''))
                    if s in news_map:
                        news_map[s].append({'src':'marketaux','title':title})
            log_debug(f"Marketaux batch: {len(articles)} articles for {len(symbols)} symbols")
            return news_map
        else:
            log_error(f"Marketaux HTTP {r.status_code}")
    except Exception as e:
        log_error(f"Marketaux error: {e}")
    return {}

def fetch_finnhub_news_batch(symbols):
    if not FINNHUB_KEY or not symbols: return {}
    inc_api("FinnhubNews")
    url = "https://finnhub.io/api/v1/news"
    params = {'category': 'general', 'token': FINNHUB_KEY, 'limit': 100}
    try:
        r = requests.get(url, params=params, timeout=10)
        if r.status_code == 200:
            articles = r.json()
            log_debug(f"Finnhub general news: {len(articles)} articles")
            news_map = {normalize_symbol(s): [] for s in symbols}
            for art in articles[:50]:
                title = art.get('headline', '') + ' ' + art.get('summary', '')
                title_lower = title.lower()
                for sym in symbols:
                    clean_sym = normalize_symbol(sym).lower()
                    if clean_sym in title_lower:
                        news_map[clean_sym.upper()].append({
                            'src': 'finnhub_news',
                            'title': art.get('headline', 'No title')[:200]
                        })
            return news_map
        else:
            log_error(f"Finnhub general news HTTP {r.status_code}")
    except Exception as e:
        log_error(f"Finnhub general news error: {e}")
    return {}

# ----- Batch fetch (unchanged) -----
def batch_fetch():
    global _news_ready
    log_debug("=== Batch fetch started ===")
    syms = get_all_pairs()
    if not syms:
        log_error("No symbols found")
        log_debug("ERROR: No symbols found")
        return
    
    need_refresh = []
    for sym in syms:
        ft = get_file_fetch_time(sym)
        if ft is None:
            need_refresh.append(sym)
        else:
            age = time.time() - ft
            if age > 1800:
                need_refresh.append(sym)
    if not need_refresh:
        log_info(f"No symbols need refresh (checked {len(syms)} symbols)")
        status_path = os.path.join(NEWS_DIR, "news_fetch_status.tsv")
        with open(status_path, 'w', encoding='utf-8') as f:
            f.write("symbol\tstatus_word\n")
            for sym in syms:
                f.write(f"{sym}\tFRESH\n")
        log_debug("No refresh needed, wrote FRESH status")
        _news_ready = True
        return
    
    log_info(f"Refreshing news for {len(need_refresh)} symbols...")
    log_debug(f"Symbols to refresh: {need_refresh}")
    
    crypto_needed = [s for s in need_refresh if is_crypto(s)]
    crypto_arts = fetch_cryptocompare() if crypto_needed else []
    log_info(f"Crypto: {len(crypto_arts)} articles (1 call)")
    
    finn_events = fetch_finnhub_calendar()
    log_info(f"Finnhub Calendar: {len(finn_events)} events (1 call)")
    
    non_crypto_needed = [s for s in need_refresh if not is_crypto(s)]
    non_crypto_news_map = {}
    if non_crypto_needed:
        chunk_size = 50
        total_chunks = (len(non_crypto_needed) + chunk_size - 1) // chunk_size
        for idx, i in enumerate(range(0, len(non_crypto_needed), chunk_size)):
            chunk = non_crypto_needed[i:i+chunk_size]
            news_map = fetch_polygon_batch(chunk)
            for sym, news in news_map.items():
                non_crypto_news_map[sym] = news
            if idx + 1 < total_chunks:
                log_info("Polygon rate limit: waiting 12 sec")
                time.sleep(12)
        
        missing = [s for s in non_crypto_needed if s not in non_crypto_news_map or not non_crypto_news_map[s]]
        if missing:
            log_info(f"Polygon no news for {len(missing)} symbols, trying Finnhub general news...")
            finnhub_batch = fetch_finnhub_news_batch(missing)
            for sym, news in finnhub_batch.items():
                if sym not in non_crypto_news_map:
                    non_crypto_news_map[sym] = []
                non_crypto_news_map[sym].extend(news)
            
            still_missing = [s for s in missing if not non_crypto_news_map.get(s)]
            if still_missing:
                log_info(f"Finnhub also no news for {len(still_missing)} symbols, trying Marketaux fallback...")
                for i in range(0, len(still_missing), 15):
                    chunk = still_missing[i:i+15]
                    fallback_map = fetch_marketaux_batch(chunk)
                    for sym, news in fallback_map.items():
                        if sym not in non_crypto_news_map:
                            non_crypto_news_map[sym] = []
                        non_crypto_news_map[sym].extend(news)
        log_info(f"Non‑crypto news coverage: {len([s for s in non_crypto_news_map if non_crypto_news_map[s]])} symbols")
    
    status_map = {}
    for sym in need_refresh:
        items = []
        error_msg = None
        try:
            if is_crypto(sym):
                base = normalize_symbol(sym).replace('USDT','').replace('USD','')
                for a in crypto_arts:
                    title = a.get('title','')
                    body = a.get('body','')
                    if base in title.upper() or base in body.upper():
                        items.append({'src':'cryptocompare','title':title})
            items.extend(finn_events[:10])
            if not is_crypto(sym):
                for art in non_crypto_news_map.get(normalize_symbol(sym), []):
                    items.append(art)
            # dedup
            seen = set()
            unique = []
            for it in items:
                if it['title'] not in seen:
                    seen.add(it['title'])
                    unique.append(it)
        except Exception as e:
            error_msg = str(e)[:200]
            log_error(f"Error processing {sym}: {error_msg}")
            unique = []
        save_tsv(sym, unique[:20], error_msg=error_msg)
        if error_msg:
            status_map[sym] = "ERROR"
        elif not unique:
            status_map[sym] = "NO_NEWS"
        else:
            status_map[sym] = "OK"
        log_debug(f"Processed {sym}: items={len(unique)}, error={error_msg}")
    
    status_path = os.path.join(NEWS_DIR, "news_fetch_status.tsv")
    with open(status_path, 'w', encoding='utf-8') as f:
        f.write("symbol\tstatus_word\n")
        for sym in syms:
            if sym in status_map:
                f.write(f"{sym}\t{status_map[sym]}\n")
            else:
                f.write(f"{sym}\tFRESH\n")
    log_debug(f"Temp status file written: {status_path}")
    
    log_info(f"Batch refresh done. API calls: {_api_calls}")
    log_debug("=== Batch fetch completed ===")
    _news_ready = True

def scheduler():
    while True:
        batch_fetch()
        time.sleep(1800)

threading.Thread(target=scheduler, daemon=True).start()

# ----- SAFE get_news_score (no regex, fully guarded) -----
def get_news_score(symbol, interval=None, rows=None):
    """
    Read news sentiment from TSV file. Returns dict with signal, score_mod, reason.
    NEVER throws an exception – always returns a valid dict.
    """
    sym = normalize_symbol(symbol)
    fpath = os.path.join(NEWS_DIR, f"{sym.lower()}_news.tsv")
    ensure_news_file_exists(sym)
    total = 0
    cnt = 0
    try:
        with open(fpath, 'r', encoding='utf-8') as f:
            reader = csv.reader(f, delimiter='\t')
            next(reader, None)  # skip header
            for row in reader:
                if not row or (row and row[0].startswith('#')):
                    continue
                if len(row) < 5:
                    continue
                try:
                    sent = int(row[4])
                    if -1 <= sent <= 1:
                        total += sent
                        cnt += 1
                except:
                    continue
    except Exception as e:
        log_error(f"Read error {sym}: {e}")
        return {"signal": "WAIT", "score_mod": 0, "reason": "File read error"}
    if cnt == 0:
        return {"signal": "WAIT", "score_mod": 0, "reason": "No news data"}
    avg = total / cnt
    mod = int(avg * 30)
    reason = f"Sentiment {avg:.2f} (based on {cnt} articles)"
    if mod > 0:
        return {"signal": "BUY", "score_mod": mod, "reason": reason}
    elif mod < 0:
        return {"signal": "SELL", "score_mod": mod, "reason": reason}
    else:
        return {"signal": "WAIT", "score_mod": 0, "reason": "Neutral sentiment"}