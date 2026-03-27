# data_sources/binance_rest.py
import requests
import time

class BinanceREST:
    """
    Passive REST API client for Binance.
    Used only when explicitly called (e.g., for backtesting).
    """
    
    def __init__(self):
        self.base_url = "https://api.binance.com/api/v3"
        print("[BinanceREST] Initialized (passive)")
    
    def get_klines(self, symbol: str, interval: str = "5m", limit: int = 100):
        """
        Fetch historical candlestick data.
        Returns list of dicts with keys: timestamp, open, high, low, close, volume.
        """
        endpoint = f"{self.base_url}/klines"
        params = {
            "symbol": symbol.upper(),
            "interval": interval,
            "limit": limit
        }
        try:
            response = requests.get(endpoint, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            klines = []
            for candle in data:
                klines.append({
                    "timestamp": candle[0],          # open time in ms
                    "open": float(candle[1]),
                    "high": float(candle[2]),
                    "low": float(candle[3]),
                    "close": float(candle[4]),
                    "volume": float(candle[5])
                })
            return klines
        except Exception as e:
            print(f"[BinanceREST] Error fetching klines: {e}")
            return []
    
    def get_price(self, symbol: str):
        """Get current price (optional, but could be used)."""
        endpoint = f"{self.base_url}/ticker/price"
        params = {"symbol": symbol.upper()}
        try:
            response = requests.get(endpoint, params=params, timeout=5)
            response.raise_for_status()
            data = response.json()
            return float(data["price"])
        except Exception as e:
            print(f"[BinanceREST] Error getting price: {e}")
            return None