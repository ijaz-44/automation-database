# Groups/group_x/X04_cvd_ws.py
"""
X04 – Dummy CVD WebSocket module (no real functionality).
Avoids file missing errors by returning safe defaults.
"""

import os
import time

class CVDWebSocket:
    """Dummy class – does nothing, just prevents import errors."""
    
    def __init__(self, base_dir):
        self.base_dir = base_dir
        self.symbols_dir = os.path.join(base_dir, "symbols")
        os.makedirs(self.symbols_dir, exist_ok=True)
        self.metrics = {}
        print("[X04_cvd_ws] Dummy module loaded (no real CVD WebSocket)")

    def set_symbols(self, symbols):
        print(f"[X04_cvd_ws] Dummy: set_symbols called with {symbols} (no action)")

    def add_symbol_metrics(self, symbol, initial_metrics):
        print(f"[X04_cvd_ws] Dummy: add_symbol_metrics for {symbol} (no action)")

    def stop(self):
        print("[X04_cvd_ws] Dummy: stop called (no action)")

    # For any other methods that might be called, add safe dummies:
    def get_cvd(self, symbol):
        return 0.0

    def get_footprint(self, symbol):
        return {}

    def get_imbalance_events(self, symbol):
        return []

    def get_absorption_events(self, symbol):
        return []

# Also provide safe file read/write helpers if needed elsewhere
def safe_read_cvd_file(filepath):
    """Return empty dict if file missing/corrupt."""
    if not os.path.exists(filepath):
        return {}
    try:
        with open(filepath, 'r') as f:
            # For TSV/TOON, return whatever, but we just avoid crash
            return {}
    except Exception:
        return {}

print("[X04_cvd_ws] Dummy module initialized – no real WebSocket, no file errors")