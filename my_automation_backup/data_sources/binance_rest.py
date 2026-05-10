"""
Binance REST API Call Tracker
Central counter for all Binance REST API calls.
"""

import os

_BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(_BASE, "market_data", "binance")

class BinanceREST:
    _total_calls = 0

    def __init__(self):
        os.makedirs(DATA_DIR, exist_ok=True)
        self.symbols_dir = os.path.join(DATA_DIR, "symbols")
        os.makedirs(self.symbols_dir, exist_ok=True)
        print("✅ [BinanceREST] Initialized (API call tracker)")

    @classmethod
    def increment_calls(cls, count=1):
        cls._total_calls += count
        print(f"[BinanceREST] Calls incremented to {cls._total_calls}")  # Debug

    @classmethod
    def get_total_calls(cls):
        return cls._total_calls

    @classmethod
    def reset_calls(cls):
        cls._total_calls = 0

print("✅ [binance_rest] Module loaded (call tracker only)")