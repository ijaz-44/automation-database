# Groups/group_x/X02_klines_ws.py
"""
X02 – Dummy module to avoid file missing errors.
Does nothing, just safely handles missing files.
"""

import os
import time

def safe_read_last_line(filepath, default=""):
    """Return last non-empty line from file, or default if missing/empty."""
    if not os.path.exists(filepath):
        return default
    try:
        with open(filepath, 'r') as f:
            lines = [line.strip() for line in f if line.strip()]
        return lines[-1] if lines else default
    except Exception:
        return default

def safe_read_content(filepath, default=""):
    """Return full file content or default if missing/error."""
    if not os.path.exists(filepath):
        return default
    try:
        with open(filepath, 'r') as f:
            return f.read()
    except Exception:
        return default

# Agar koi function call ho raha ho jo file read karta hai, to ye use karo:
# data = safe_read_content("somefile.toon", "{}")

# Baqi kuch nahi karta. Is module ka bas yehi maqsad hai.
print(f"[X02] Loaded dummy module (no errors on missing files)")