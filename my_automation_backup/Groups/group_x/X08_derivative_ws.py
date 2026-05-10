# Groups/group_x/X08_derivative_ws.py
"""
X08 – Dummy Derivative WebSocket module (no real functionality).
Avoids import errors and does nothing.
"""

import os
import time

class DerivativeWebSocket:
    """Dummy class – does nothing, just prevents import errors."""
    
    def __init__(self, base_dir, derivative_rest, min_quantity=1.0):
        self.base_dir = base_dir
        self.derivative_rest = derivative_rest
        self.min_quantity = min_quantity
        self._running = False
        print("[X08_derivative_ws] Dummy module loaded (no WebSocket, no data saved)")

    def start(self, symbols):
        print(f"[X08_derivative_ws] Dummy: start called for {symbols} (ignored)")

    def stop(self):
        print("[X08_derivative_ws] Dummy: stop called")

    # Any other methods that might be called (safe stubs)
    def get_last_event_count(self, symbol):
        return 0

print("[X08_derivative_ws] Dummy module initialized – all operations are no-ops")