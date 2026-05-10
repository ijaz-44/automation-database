# Groups/group_x/X05_depth_ws.py
"""
X05 – Dummy Depth WebSocket module (no real functionality).
Avoids import errors and does nothing.
"""

import os
import time

class DepthWebSocket:
    """Dummy class – does nothing, just prevents import errors."""
    
    def __init__(self, base_dir):
        self.base_dir = base_dir
        self.symbols_dir = os.path.join(base_dir, "symbols")
        os.makedirs(self.symbols_dir, exist_ok=True)
        print("[X05_depth_ws] Dummy module loaded (no WebSocket, no data saved)")

    def add_symbol(self, symbol):
        print(f"[X05_depth_ws] Dummy: add_symbol called for {symbol} (ignored)")

    def stop(self):
        print("[X05_depth_ws] Dummy: stop called")

    # Any other methods that might be called from elsewhere (safe stubs)
    def get_depth(self, symbol):
        return None

    def get_icebergs(self, symbol):
        return []

    def get_spoofs(self, symbol):
        return []

print("[X05_depth_ws] Dummy module initialized – all operations are no-ops")