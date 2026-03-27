# data_sources/finnhub_rest.py
import requests
import time

class FinnhubREST:
    """
    Passive REST client for Finnhub – used only for historical data when needed.
    Free tier: 60 calls/minute, 50 REST calls/minute? (Finnhub docs: 60/min for free)
    """
    def __init__(self, api_key):
        self.api_key = api_key
        self.base_url = "https://finnhub.io/api/v1"
        print("[FinnhubREST] Initialized (passive)")

    def get_klines(self, symbol, resolution="5", from_ts=None, to_ts=None, limit=100):
        """
        Fetch historical candlestick data.
        resolution: '1', '5', '15', '30', '60', 'D', 'W', 'M' (minutes, day, week, month)
        from_ts: start timestamp (seconds), to_ts: end timestamp (seconds)
        If not provided, will fetch last 'limit' candles.
        """
        endpoint = f"{self.base_url}/stock/candle"
        if not from_ts or not to_ts:
            # Default: fetch last 'limit' candles (approx)
            to_ts = int(time.time())
            # Estimate minutes per resolution
            res_min = {'1':1, '5':5, '15':15, '30':30, '60':60}.get(resolution, 5)
            from_ts = to_ts - (limit * res_min * 60)
        params = {
            "symbol": symbol.upper(),
            "resolution": resolution,
            "from": from_ts,
            "to": to_ts,
            "token": self.api_key
        }
        try:
            response = requests.get(endpoint, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()

            if data.get("s") != "ok":
                print(f"[FinnhubREST] No data for {symbol}: {data.get('s')}")
                return []

            klines = []
            for i in range(len(data.get('t', []))):
                klines.append({
                    "timestamp": data['t'][i] * 1000,  # convert to ms
                    "open": float(data['o'][i]),
                    "high": float(data['h'][i]),
                    "low": float(data['l'][i]),
                    "close": float(data['c'][i]),
                    "volume": float(data['v'][i])
                })
            return klines
        except Exception as e:
            print(f"[FinnhubREST] Error fetching klines: {e}")
            return []

    def get_price(self, symbol):
        """Get current price (quote)."""
        endpoint = f"{self.base_url}/quote"
        params = {
            "symbol": symbol.upper(),
            "token": self.api_key
        }
        try:
            response = requests.get(endpoint, params=params, timeout=5)
            response.raise_for_status()
            data = response.json()
            return float(data.get("c", 0))  # current price
        except Exception as e:
            print(f"[FinnhubREST] Error getting price: {e}")
            return None