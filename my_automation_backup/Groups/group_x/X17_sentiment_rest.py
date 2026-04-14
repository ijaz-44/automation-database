# Groups/group_x/X17_sentiment_rest.py
"""
X17 - Sentiment Data Module (Multi-Source, No Key Required)
- Real-time news sentiment (Free Crypto News API + CryptoPanic)
- Social velocity (Twitter/Reddit mentions via ApeWisdom API)
- Funding rate history (last 20 readings over 8 hours from Binance Futures)
- Retail trade sentiment (Long/Short ratio from Binance Futures)
All data saved to a single TSV file: {symbol}_sentiment.tsv
Full logging with fallbacks and error handling.
"""

import requests
import time
import os
import json
import math
from collections import defaultdict

# ==================== CONFIGURATION ====================
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SYMBOLS_DIR = os.path.join(BASE_DIR, "market_data", "binance", "symbols")
os.makedirs(SYMBOLS_DIR, exist_ok=True)

# API Endpoints (No API Keys Required)
FREE_CRYPTO_NEWS_API = "https://mcpmarket.com/api/news"
CRYPTOPANIC_API = "https://cryptopanic.com/api/v1/posts/"
APEWISDOM_API = "https://apewisdom.io/api/v1.0/all-crypto"
BINANCE_FUTURES_API = "https://fapi.binance.com/fapi/v1"

class SentimentData:
    def __init__(self):
        self._last_call = 0
        self._rate_limit_sec = 1  # 1 call per second

    def _rate_limited_fetch(self, url, params=None, headers=None):
        now = time.time()
        elapsed = now - self._last_call
        if elapsed < self._rate_limit_sec:
            time.sleep(self._rate_limit_sec - elapsed)
        self._last_call = time.time()
        try:
            r = requests.get(url, params=params, headers=headers, timeout=10)
            if r.status_code == 200:
                return r.json()
            else:
                print(f"[X17] HTTP {r.status_code} from {url[:50]}: {r.text[:100]}")
                return None
        except Exception as e:
            print(f"[X17] Request error: {e}")
            return None

    # ---------- 1. Real-Time News Sentiment (Multiple Sources) ----------
    def fetch_news_sentiment(self, symbol):
        """
        Fetch news sentiment from multiple free sources.
        Returns: {'source': 'free_crypto_news', 'sentiment': float, 'count': int}
        """
        # Try Free Crypto News API (primary)
        try:
            print(f"[X17] Fetching news sentiment from Free Crypto News API for {symbol}")
            params = {"symbol": symbol.upper(), "limit": 50}
            data = self._rate_limited_fetch(FREE_CRYPTO_NEWS_API, params=params)
            if data and "articles" in data:
                articles = data["articles"]
                # Simple sentiment: count positive/negative based on title keywords
                pos_keywords = ["surge", "bull", "high", "rise", "positive", "up", "green", "gain"]
                neg_keywords = ["drop", "bear", "low", "fall", "negative", "down", "red", "loss"]
                pos = 0
                neg = 0
                for art in articles:
                    title = art.get("title", "").lower()
                    if any(kw in title for kw in pos_keywords):
                        pos += 1
                    if any(kw in title for kw in neg_keywords):
                        neg += 1
                total = pos + neg
                sentiment = (pos - neg) / total if total > 0 else 0.0
                print(f"[X17] Free Crypto News API OK: {total} articles, sentiment={sentiment:.2f}")
                return {"source": "free_crypto_news", "sentiment": sentiment, "count": total}
        except Exception as e:
            print(f"[X17] Free Crypto News API error: {e}")

        # Fallback to CryptoPanic API
        try:
            print(f"[X17] Falling back to CryptoPanic API for {symbol}")
            params = {"currencies": symbol.upper(), "limit": 50, "kind": "news"}
            data = self._rate_limited_fetch(CRYPTOPANIC_API, params=params)
            if data and "results" in data:
                results = data["results"]
                # CryptoPanic provides sentiment tags
                pos = 0
                neg = 0
                for item in results:
                    sentiment_tag = item.get("sentiment", {}).get("sentiment", "")
                    if sentiment_tag == "positive":
                        pos += 1
                    elif sentiment_tag == "negative":
                        neg += 1
                total = pos + neg
                sentiment = (pos - neg) / total if total > 0 else 0.0
                print(f"[X17] CryptoPanic API OK: {total} articles, sentiment={sentiment:.2f}")
                return {"source": "cryptopanic", "sentiment": sentiment, "count": total}
        except Exception as e:
            print(f"[X17] CryptoPanic API error: {e}")

        print("[X17] WARNING: No news sentiment data available")
        return {"source": "none", "sentiment": 0.0, "count": 0}

    # ---------- 2. Social Velocity (Twitter/Reddit Mentions) ----------
    def fetch_social_velocity(self, symbol):
        """
        Fetch social media mention velocity from ApeWisdom (free, no key).
        Returns: {'mention_count': int, 'velocity': float}
        """
        try:
            print(f"[X17] Fetching social sentiment from ApeWisdom for {symbol}")
            data = self._rate_limited_fetch(APEWISDOM_API)
            if data and "data" in data:
                for item in data["data"]:
                    if item.get("ticker") == symbol.upper():
                        mentions = item.get("mentions", 0)
                        upvotes = item.get("upvotes", 0)
                        # Calculate velocity (mentions + upvotes) as a simple metric
                        velocity = mentions + upvotes
                        print(f"[X17] ApeWisdom OK: {mentions} mentions, {upvotes} upvotes")
                        return {"mention_count": mentions, "velocity": velocity}
                print(f"[X17] Symbol {symbol} not found in ApeWisdom data")
        except Exception as e:
            print(f"[X17] ApeWisdom error: {e}")

        print("[X17] WARNING: No social velocity data available")
        return {"mention_count": 0, "velocity": 0}

    # ---------- 3. Funding Rate History (Last 20 readings, 8 hours) ----------
    def fetch_funding_rate_history(self, symbol):
        """
        Fetch funding rate history from Binance Futures.
        Returns list of dicts with 'timestamp', 'funding_rate'.
        """
        try:
            print(f"[X17] Fetching funding rate history for {symbol}")
            url = f"{BINANCE_FUTURES_API}/fundingRate"
            params = {"symbol": symbol.upper(), "limit": 20}
            data = self._rate_limited_fetch(url, params=params)
            if data:
                history = []
                for item in data:
                    history.append({
                        "timestamp": item.get("fundingTime", 0),
                        "funding_rate": float(item.get("fundingRate", 0))
                    })
                print(f"[X17] Funding rate history OK: {len(history)} readings")
                return history
        except Exception as e:
            print(f"[X17] Funding rate error: {e}")

        print("[X17] WARNING: No funding rate data available")
        return []

    # ---------- 4. Retail Trade Sentiment (Long/Short Ratio) ----------
    def fetch_retail_sentiment(self, symbol):
        """
        Fetch long/short ratio from Binance Futures.
        Returns: {'long_ratio': float, 'short_ratio': float, 'timestamp': int}
        """
        try:
            print(f"[X17] Fetching retail sentiment (long/short ratio) for {symbol}")
            url = f"{BINANCE_FUTURES_API}/topLongShortAccountRatio"
            params = {"symbol": symbol.upper(), "period": "1h", "limit": 1}
            data = self._rate_limited_fetch(url, params=params)
            if data and len(data) > 0:
                item = data[0]
                long_account = float(item.get("longAccount", 0))
                short_account = float(item.get("shortAccount", 0))
                total = long_account + short_account
                long_ratio = long_account / total if total > 0 else 0.5
                short_ratio = short_account / total if total > 0 else 0.5
                print(f"[X17] Retail sentiment OK: long={long_ratio:.2f}, short={short_ratio:.2f}")
                return {
                    "long_ratio": long_ratio,
                    "short_ratio": short_ratio,
                    "timestamp": item.get("timestamp", int(time.time() * 1000))
                }
        except Exception as e:
            print(f"[X17] Retail sentiment error: {e}")

        print("[X17] WARNING: No retail sentiment data available")
        return {"long_ratio": 0.5, "short_ratio": 0.5, "timestamp": 0}

    # ---------- Main Save Function (Single TSV File) ----------
    def collect_and_save(self, symbol):
        print(f"[X17] Collecting sentiment data for {symbol}")
        filepath = os.path.join(SYMBOLS_DIR, f"{symbol.lower()}_sentiment.tsv")

        # Step 1: News sentiment
        print("[X17] Step 1: Fetching news sentiment...")
        news = self.fetch_news_sentiment(symbol)

        # Step 2: Social velocity
        print("[X17] Step 2: Fetching social velocity...")
        social = self.fetch_social_velocity(symbol)

        # Step 3: Funding rate history (last 20 readings over 8 hours)
        print("[X17] Step 3: Fetching funding rate history...")
        funding_history = self.fetch_funding_rate_history(symbol)

        # Step 4: Retail sentiment (long/short ratio)
        print("[X17] Step 4: Fetching retail sentiment...")
        retail = self.fetch_retail_sentiment(symbol)

        # Write all data to a single TSV file with sections
        with open(filepath, 'w') as f:
            # SECTION 1: NEWS SENTIMENT
            f.write("# ========== NEWS SENTIMENT ==========\n")
            f.write("source\tsentiment_score\tarticle_count\n")
            f.write(f"{news['source']}\t{news['sentiment']:.4f}\t{news['count']}\n")
            f.write("\n")

            # SECTION 2: SOCIAL VELOCITY (Twitter/Reddit Mentions)
            f.write("# ========== SOCIAL VELOCITY (Twitter/Reddit Mentions) ==========\n")
            f.write("mention_count\tvelocity\n")
            f.write(f"{social['mention_count']}\t{social['velocity']}\n")
            f.write("\n")

            # SECTION 3: FUNDING RATE HISTORY (last 20 readings, 8 hours)
            f.write("# ========== FUNDING RATE HISTORY (last 20 readings, 8 hours) ==========\n")
            f.write("timestamp\tfunding_rate\n")
            if funding_history:
                for entry in funding_history:
                    f.write(f"{entry['timestamp']}\t{entry['funding_rate']}\n")
            else:
                f.write("NO_DATA\t0\n")
            f.write("\n")

            # SECTION 4: RETAIL TRADE SENTIMENT (Long/Short Ratio)
            f.write("# ========== RETAIL TRADE SENTIMENT (Long/Short Ratio) ==========\n")
            f.write("timestamp\tlong_ratio\tshort_ratio\n")
            f.write(f"{retail['timestamp']}\t{retail['long_ratio']:.4f}\t{retail['short_ratio']:.4f}\n")

        print(f"[X17] All sentiment data saved to {filepath}")

if __name__ == "__main__":
    sent = SentimentData()
    sent.collect_and_save("BTCUSDT")