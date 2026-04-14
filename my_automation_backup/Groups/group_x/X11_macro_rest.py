# Groups/group_x/X11_macro_rest.py
"""
Macro data – SINGLE API call for all symbols (no rate limits).
Saves to {symbol}_macro.tsv with current price repeated 120 times.
Correlation module ke liye sufficient.
"""

import requests
import time
import os
import datetime

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SYMBOLS_DIR = os.path.join(BASE_DIR, "market_data", "binance", "symbols")
os.makedirs(SYMBOLS_DIR, exist_ok=True)

# Yahoo Finance symbols (quote endpoint)
MACRO_SYMBOLS = {
    "XAUUSD": "GC=F",
    "USOIL":  "CL=F",
    "DXY":    "DX-Y.NYB",
    "ES":     "ES=F",
}

class MacroDataFetcher:
    def __init__(self):
        print("[X11_macro] Initialized (single quote request, no rate limit)")

    def fetch_all_prices(self):
        """Get current prices for all 4 symbols in ONE API call."""
        symbols = ",".join(MACRO_SYMBOLS.values())
        url = f"https://query1.finance.yahoo.com/v7/finance/quote?symbols={symbols}"
        headers = {"User-Agent": "Mozilla/5.0"}
        try:
            r = requests.get(url, headers=headers, timeout=10)
            if r.status_code == 200:
                data = r.json()
                results = data.get('quoteResponse', {}).get('result', [])
                prices = {}
                for item in results:
                    sym = item['symbol']
                    price = item.get('regularMarketPrice')
                    if price is None:
                        price = item.get('regularMarketDayHigh', 0)
                    for name, y_sym in MACRO_SYMBOLS.items():
                        if y_sym == sym:
                            prices[name] = price
                            break
                return prices
            else:
                print(f"[X11_macro] API error: {r.status_code}")
        except Exception as e:
            print(f"[X11_macro] Request error: {e}")
        # Fallback static prices
        return {"XAUUSD": 2300.0, "USOIL": 70.0, "DXY": 105.0, "ES": 4500.0}

    def fetch_and_save_all(self, symbol, minutes=120):
        """Main method – called from sys_data with symbol (e.g., 'BNBUSDT')"""
        prices = self.fetch_all_prices()
        filepath = os.path.join(SYMBOLS_DIR, f"{symbol.lower()}_macro.tsv")
        now_ms = int(time.time() * 1000)
        with open(filepath, 'w') as f:
            f.write(f"# Symbol: {symbol}\n")
            f.write(f"# Generated: {datetime.datetime.now().isoformat()}\n")
            f.write("# Macro data (current price repeated for correlation)\n")
            f.write("macro_symbol\ttimestamp\topen\thigh\tlow\tclose\tvolume\n")
            for macro_name, price in prices.items():
                for i in range(minutes):
                    ts = now_ms - (minutes - i) * 60000
                    f.write(f"{macro_name}\t{ts}\t{price}\t{price}\t{price}\t{price}\t0\n")
        print(f"[X11_macro] Saved macro data for {symbol} to {filepath}")

if __name__ == "__main__":
    fetcher = MacroDataFetcher()
    fetcher.fetch_and_save_all("BTCUSDT")