# Groups/group_c/__init__.py
"""
Group C – Central configuration & scoring defaults.
No separate C01 files – everything is here.
"""

# Default Z‑group weights (trend, volume, momentum)
DEFAULT_WEIGHTS_Z = {
    "Binary OTC": {"trend": 0.35, "volume": 0.15, "momentum": 0.50},
    "CFD":        {"trend": 0.40, "volume": 0.20, "momentum": 0.40},
    "Spot":       {"trend": 0.30, "volume": 0.25, "momentum": 0.45},
    "Future":     {"trend": 0.35, "volume": 0.20, "momentum": 0.45},
}

# Default A‑group weights (structure, indicators, sr, candle)
DEFAULT_WEIGHTS_A = {
    "Binary OTC": {"structure": 0.30, "indicators": 0.35, "sr": 0.20, "candle": 0.15},
    "CFD":        {"structure": 0.35, "indicators": 0.30, "sr": 0.20, "candle": 0.15},
    "Spot":       {"structure": 0.30, "indicators": 0.25, "sr": 0.25, "candle": 0.20},
    "Future":     {"structure": 0.30, "indicators": 0.30, "sr": 0.20, "candle": 0.20},
}

# Z‑group thresholds (buy, sell, strong_buy, strong_sell)
DEFAULT_THRESHOLDS_Z = {
    "Binary OTC": {"buy": 65, "sell": 35, "strong_buy": 85, "strong_sell": 15},
    "CFD":        {"buy": 62, "sell": 38, "strong_buy": 82, "strong_sell": 18},
    "Spot":       {"buy": 68, "sell": 32, "strong_buy": 88, "strong_sell": 12},
    "Future":     {"buy": 66, "sell": 34, "strong_buy": 84, "strong_sell": 16},
}

# A‑group thresholds (go, block, strong_buy, strong_sell)
DEFAULT_THRESHOLDS_A = {
    "Binary OTC": {"go": 60, "block": 45, "strong_buy": 85, "strong_sell": 15},
    "CFD":        {"go": 58, "block": 42, "strong_buy": 80, "strong_sell": 20},
    "Spot":       {"go": 62, "block": 48, "strong_buy": 86, "strong_sell": 14},
    "Future":     {"go": 60, "block": 44, "strong_buy": 82, "strong_sell": 18},
}

# Minimum Z‑score to allow A‑score calculation
DEFAULT_MIN_Z_FOR_A = {
    "Binary OTC": 45,
    "CFD":        42,
    "Spot":       48,
    "Future":     44,
}

# Minimum A‑score to consider a trade
DEFAULT_MIN_A_SCORE = {
    "Binary OTC": 60,
    "CFD":        58,
    "Spot":       62,
    "Future":     60,
}

# Risk parameters (SL/TP multipliers)
DEFAULT_RISK = {
    "Binary OTC": {"sl_atr": 1.5, "tp_atr": 2.0},
    "CFD":        {"sl_atr": 1.2, "tp_atr": 1.8},
    "Spot":       {"sl_atr": 1.5, "tp_atr": 2.0},
    "Future":     {"sl_atr": 1.3, "tp_atr": 1.9},
}

def get_defaults_for_market(market, interval=None):
    """Return a complete config dict for a given market."""
    return {
        "weights_z": DEFAULT_WEIGHTS_Z.get(market, DEFAULT_WEIGHTS_Z["Binary OTC"]),
        "weights_a": DEFAULT_WEIGHTS_A.get(market, DEFAULT_WEIGHTS_A["Binary OTC"]),
        "thresholds_z": DEFAULT_THRESHOLDS_Z.get(market, DEFAULT_THRESHOLDS_Z["Binary OTC"]),
        "thresholds_a": DEFAULT_THRESHOLDS_A.get(market, DEFAULT_THRESHOLDS_A["Binary OTC"]),
        "min_z_for_a": DEFAULT_MIN_Z_FOR_A.get(market, DEFAULT_MIN_Z_FOR_A["Binary OTC"]),
        "min_a_score": DEFAULT_MIN_A_SCORE.get(market, DEFAULT_MIN_A_SCORE["Binary OTC"]),
        "risk": DEFAULT_RISK.get(market, DEFAULT_RISK["Binary OTC"]),
        "interval": interval,
    }