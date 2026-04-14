# data_sources/binance_rest.py
"""
Binance REST Manager – coordinates other modules but does NOT fetch klines.
Kline fetching is handled by X01_klines_rest.py (SQLite).
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
        print("✅ [BinanceREST] Initialized (manager mode – no kline fetching)")

    # ========== Dummy methods for compatibility (no kline handling) ==========
    def needs_fill(self, symbol, minutes=120):
        """Dummy – always returns False. Kline filling is done by X01."""
        return False

    def fill_gaps(self, symbol, minutes=120):
        """Dummy – does nothing. Use X01_klines_rest.py for fetching klines."""
        print(f"[BinanceREST] fill_gaps called for {symbol} but disabled (use X01)")

    def get_candles_for_symbol(self, symbol):
        """Dummy – returns empty list. Use X01 SQLite database."""
        return []

    # ========== API call tracking (kept for compatibility) ==========
    @classmethod
    def get_total_calls(cls):
        return cls._total_calls

    @classmethod
    def reset_calls(cls):
        cls._total_calls = 0

print("✅ [binance_rest] Module loaded (manager only)")