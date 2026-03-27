import os
import json
import hashlib

# ========== CONFIGURATION ==========
OUTPUT_DIR = "Groups/group_c/configs/future"
OVERWRITE_EXISTING = False   # Change to True to force overwrite
# ==================================

# ---- 1. Symbols (same 139 unique set) ----
FOREX_SYMBOLS = {
    "EURUSD", "GBPUSD", "USDJPY", "USDCHF", "AUDUSD",
    "NZDUSD", "USDCAD", "EURGBP", "EURJPY", "GBPJPY",
    "AUDJPY", "CHFJPY", "EURCHF", "EURAUD", "EURCAD",
    "GBPCHF", "GBPAUD", "AUDCAD", "CADCHF", "CADJPY",
    "NZDJPY", "NZDCAD", "AUDNZD", "GBPNZD", "EURNZD",
    "USDTRY", "USDMXN", "USDSGD", "USDNOK", "USDPLN",
    "USDHUF", "USDCNH", "USDTHB", "USDMYR", "USDZAR",
    "USDHKD", "USDSEK", "USDDKK", "EURNOK", "GBPNOK",
    "AUDSEK", "EURPLN", "USDBRL", "EURHUF", "USDCZK",
    "EURSEK", "GBPSEK", "AUDNOK", "USDILS", "USDKRW",
    "USDTWD", "USDIDR", "EURCZK"
}
CRYPTO_SYMBOLS = {
    "BTCUSDT", "ETHUSDT", "BNBUSDT", "XRPUSDT", "ADAUSDT",
    "DOGEUSDT", "LTCUSDT", "SHIBUSDT", "DOTUSDT", "SOLUSDT",
    "AVAXUSDT", "MATICUSDT", "LINKUSDT", "UNIUSDT", "AAVEUSDT",
    "ATOMUSDT", "NEARUSDT", "VETUSDT", "TRXUSDT", "EOSUSDT",
    "XLMUSDT", "ETCUSDT", "XMRUSDT", "BCHUSDT", "ALGOUSDT",
    "PEPEUSDT", "BONKUSDT", "POPCATUSDT", "TURBOUSDT", "BRETTUSDT",
    "GMEUSDT", "AMCUSDT", "TRUMPUSDT", "MELANIAUSDT", "FLOKIUSDT",
    "WIFUSDT", "MOGUSDT", "SPX6900USDT", "APUUSDT", "DAIUSDT",
    "USDCUSDT", "XAUTUSDT", "PAXGUSDT", "FDUSDUSDT", "TUSDUSDT",
    "EURTUSDT"
}
COMMODITY_SYMBOLS = {
    "XAUUSD", "XAGUSD", "XPTUSD", "XPDUSD", "COPPERUSD",
    "UKOIL", "USOIL", "NATGAS", "HEATOIL", "GASOLINE",
    "COFFEEUSD", "COCOAUSD", "WHEATUSD", "CORNUSD", "SOYBEANSUSD",
    "COTTONUSD", "LIVECATTLEUSD", "LEANHOGSUSD", "SUGARUSD", "ORANGEJUICEUSD"
}
STOCK_SYMBOLS = {
    "AAPL", "MSFT", "GOOGL", "AMZN", "META",
    "TSLA", "NVDA", "NFLX", "AMD", "CRM",
    "JNJ", "PFE", "UNH", "MRK", "ABBV",
    "WMT", "KO", "PG", "V", "MA"
}
ALL_SYMBOLS = FOREX_SYMBOLS | CRYPTO_SYMBOLS | COMMODITY_SYMBOLS | STOCK_SYMBOLS
print(f"Total symbols: {len(ALL_SYMBOLS)}")

# ---- 2. Base Future template (5 timeframes) ----
BASE_TEMPLATE = {
    "timeframes": {
        "5m": {
            "weights_z": {"trend": 0.30, "volume": 0.20, "momentum": 0.50},
            "thresholds_z": {"buy": 65, "sell": 35, "strong_buy": 82, "strong_sell": 18},
            "weights_a": {"structure": 0.25, "indicators": 0.25, "sr": 0.20, "candle": 0.30},
            "thresholds_a": {"go": 70, "strong_buy": 86, "strong_sell": 14},
            "min_a_score": 70,
            "risk": {"sl_atr_mult": 1.2, "tp_atr_mult": 1.8},
            "deep": {"min_deep_score": 68, "require_deep_confirm": False},
            "manipulation": {"enabled": True, "penalty": -12, "min_wick_ratio": 0.6, "volume_spike_threshold": 2.2, "false_breakout_tolerance": 0.4}
        },
        "10m": {
            "weights_z": {"trend": 0.30, "volume": 0.20, "momentum": 0.50},
            "thresholds_z": {"buy": 65, "sell": 35, "strong_buy": 82, "strong_sell": 18},
            "weights_a": {"structure": 0.25, "indicators": 0.25, "sr": 0.20, "candle": 0.30},
            "thresholds_a": {"go": 70, "strong_buy": 86, "strong_sell": 14},
            "min_a_score": 70,
            "risk": {"sl_atr_mult": 1.2, "tp_atr_mult": 1.8},
            "deep": {"min_deep_score": 68, "require_deep_confirm": False},
            "manipulation": {"enabled": True, "penalty": -12, "min_wick_ratio": 0.6, "volume_spike_threshold": 2.2, "false_breakout_tolerance": 0.4}
        },
        "15m": {
            "weights_z": {"trend": 0.35, "volume": 0.20, "momentum": 0.45},
            "thresholds_z": {"buy": 64, "sell": 36, "strong_buy": 80, "strong_sell": 20},
            "weights_a": {"structure": 0.30, "indicators": 0.25, "sr": 0.20, "candle": 0.25},
            "thresholds_a": {"go": 68, "strong_buy": 84, "strong_sell": 16},
            "min_a_score": 68,
            "risk": {"sl_atr_mult": 1.3, "tp_atr_mult": 1.9},
            "deep": {"min_deep_score": 66, "require_deep_confirm": False},
            "manipulation": {"enabled": True, "penalty": -10, "min_wick_ratio": 0.6, "volume_spike_threshold": 2.0, "false_breakout_tolerance": 0.45}
        },
        "1h": {
            "weights_z": {"trend": 0.40, "volume": 0.20, "momentum": 0.40},
            "thresholds_z": {"buy": 62, "sell": 38, "strong_buy": 78, "strong_sell": 22},
            "weights_a": {"structure": 0.35, "indicators": 0.25, "sr": 0.20, "candle": 0.20},
            "thresholds_a": {"go": 66, "strong_buy": 82, "strong_sell": 18},
            "min_a_score": 66,
            "risk": {"sl_atr_mult": 1.5, "tp_atr_mult": 2.0},
            "deep": {"min_deep_score": 64, "require_deep_confirm": False},
            "manipulation": {"enabled": True, "penalty": -8, "min_wick_ratio": 0.6, "volume_spike_threshold": 1.8, "false_breakout_tolerance": 0.5}
        },
        "4h": {
            "weights_z": {"trend": 0.45, "volume": 0.20, "momentum": 0.35},
            "thresholds_z": {"buy": 60, "sell": 40, "strong_buy": 75, "strong_sell": 25},
            "weights_a": {"structure": 0.40, "indicators": 0.25, "sr": 0.20, "candle": 0.15},
            "thresholds_a": {"go": 64, "strong_buy": 78, "strong_sell": 22},
            "min_a_score": 64,
            "risk": {"sl_atr_mult": 1.8, "tp_atr_mult": 2.2},
            "deep": {"min_deep_score": 62, "require_deep_confirm": False},
            "manipulation": {"enabled": True, "penalty": -6, "min_wick_ratio": 0.6, "volume_spike_threshold": 1.6, "false_breakout_tolerance": 0.55}
        }
    },
    "default": {
        "weights_z": {"trend": 0.35, "volume": 0.20, "momentum": 0.45},
        "thresholds_z": {"buy": 62, "sell": 38, "strong_buy": 78, "strong_sell": 22},
        "weights_a": {"structure": 0.30, "indicators": 0.25, "sr": 0.20, "candle": 0.25},
        "thresholds_a": {"go": 66, "strong_buy": 82, "strong_sell": 18},
        "min_a_score": 66,
        "risk": {"sl_atr_mult": 1.4, "tp_atr_mult": 2.0},
        "deep": {"min_deep_score": 64, "require_deep_confirm": False},
        "manipulation": {"enabled": True, "penalty": -8, "min_wick_ratio": 0.6, "volume_spike_threshold": 2.0, "false_breakout_tolerance": 0.5}
    }
}

# ---- 3. Category‑specific adjustments for Future ----
CATEGORY_ADJUSTMENTS = {
    "forex": {
        "weights_z": {"trend": 0.35, "volume": 0.20, "momentum": 0.45},
        "thresholds_z": {"buy": 64, "sell": 36},
        "weights_a": {"structure": 0.30, "indicators": 0.25, "sr": 0.20, "candle": 0.25},
        "thresholds_a": {"go": 68},
        "min_a_score": 68,
        "risk": {"sl_atr_mult": 1.4, "tp_atr_mult": 2.0},
        "manipulation": {"penalty": -10}
    },
    "crypto": {
        "weights_z": {"trend": 0.25, "volume": 0.20, "momentum": 0.55},
        "thresholds_z": {"buy": 66, "sell": 34, "strong_buy": 84, "strong_sell": 16},
        "weights_a": {"structure": 0.20, "indicators": 0.25, "sr": 0.20, "candle": 0.35},
        "thresholds_a": {"go": 72},
        "min_a_score": 72,
        "risk": {"sl_atr_mult": 1.6, "tp_atr_mult": 2.2},
        "manipulation": {"penalty": -15, "volume_spike_threshold": 2.5}
    },
    "commodity": {
        "weights_z": {"trend": 0.40, "volume": 0.20, "momentum": 0.40},
        "thresholds_z": {"buy": 62, "sell": 38, "strong_buy": 78, "strong_sell": 22},
        "weights_a": {"structure": 0.35, "indicators": 0.25, "sr": 0.20, "candle": 0.20},
        "thresholds_a": {"go": 66},
        "min_a_score": 66,
        "risk": {"sl_atr_mult": 1.5, "tp_atr_mult": 2.0},
        "manipulation": {"penalty": -8}
    },
    "stock": {
        "weights_z": {"trend": 0.35, "volume": 0.20, "momentum": 0.45},
        "thresholds_z": {"buy": 64, "sell": 36, "strong_buy": 80, "strong_sell": 20},
        "weights_a": {"structure": 0.30, "indicators": 0.25, "sr": 0.20, "candle": 0.25},
        "thresholds_a": {"go": 68},
        "min_a_score": 68,
        "risk": {"sl_atr_mult": 1.4, "tp_atr_mult": 2.0},
        "manipulation": {"penalty": -8}
    }
}

def get_category(symbol):
    if symbol in FOREX_SYMBOLS:
        return "forex"
    if symbol in CRYPTO_SYMBOLS:
        return "crypto"
    if symbol in COMMODITY_SYMBOLS:
        return "commodity"
    if symbol in STOCK_SYMBOLS:
        return "stock"
    return "forex"

def apply_category_adjustments(config, category):
    if category not in CATEGORY_ADJUSTMENTS:
        return config
    adj = CATEGORY_ADJUSTMENTS[category]

    for tf in config["timeframes"]:
        if "weights_z" in adj:
            config["timeframes"][tf]["weights_z"].update(adj["weights_z"])
        if "thresholds_z" in adj:
            config["timeframes"][tf]["thresholds_z"].update(adj["thresholds_z"])
        if "weights_a" in adj:
            config["timeframes"][tf]["weights_a"].update(adj["weights_a"])
        if "thresholds_a" in adj:
            config["timeframes"][tf]["thresholds_a"].update(adj["thresholds_a"])
        if "min_a_score" in adj:
            config["timeframes"][tf]["min_a_score"] = adj["min_a_score"]
        if "risk" in adj:
            config["timeframes"][tf]["risk"].update(adj["risk"])
        if "manipulation" in adj:
            config["timeframes"][tf]["manipulation"].update(adj["manipulation"])

    for key in ["weights_z", "thresholds_z", "weights_a", "thresholds_a", "min_a_score", "risk", "manipulation"]:
        if key in adj and isinstance(adj[key], dict):
            config["default"][key].update(adj[key])
        elif key in adj and key == "min_a_score":
            config["default"][key] = adj[key]

    return config

def add_per_symbol_tweak(config, symbol):
    hash_val = int(hashlib.md5(symbol.encode()).hexdigest()[:4], 16) % 3 - 1
    if hash_val == 0:
        return config
    for tf in config["timeframes"]:
        if "thresholds_z" in config["timeframes"][tf]:
            config["timeframes"][tf]["thresholds_z"]["buy"] += hash_val
        if "thresholds_a" in config["timeframes"][tf]:
            config["timeframes"][tf]["thresholds_a"]["go"] += hash_val
    config["default"]["thresholds_z"]["buy"] += hash_val
    config["default"]["thresholds_a"]["go"] += hash_val
    return config

def write_config(symbol, config):
    file_path = os.path.join(OUTPUT_DIR, f"{symbol}.json")
    if os.path.exists(file_path) and not OVERWRITE_EXISTING:
        print(f"⏭️  Skipping {symbol}.json (already exists)")
        return False
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(config, f, indent=2)
    print(f"✅ Created {symbol}.json")
    return True

def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    print(f"Total symbols to process: {len(ALL_SYMBOLS)}")
    print("Generating Future config files (5m,10m,15m,1h,4h)...")

    created = 0
    for sym in sorted(ALL_SYMBOLS):
        category = get_category(sym)
        config = json.loads(json.dumps(BASE_TEMPLATE))
        config = apply_category_adjustments(config, category)
        config = add_per_symbol_tweak(config, sym)
        if write_config(sym, config):
            created += 1

    print(f"\nDone. Created {created} new file(s).")
    if created < len(ALL_SYMBOLS):
        print(f"Existing files skipped: {len(ALL_SYMBOLS) - created}. To overwrite, set OVERWRITE_EXISTING = True.")

if __name__ == "__main__":
    main()