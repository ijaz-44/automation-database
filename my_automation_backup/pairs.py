# pairs.py
# Kaam: Har platform ke liye sahi pair list dena
#        Crypto vs Forex vs Commodity vs Stock identification
#        OTC suffix handling for IQ Option

print("[pairs] Loading...")

# ── Forex ─────────────────────────────────────────────────────────────────────
REAL_CURRENCY_CORR = [
    "EURUSD","GBPUSD","USDJPY","USDCHF","AUDUSD",
    "NZDUSD","USDCAD","EURGBP","EURJPY","GBPJPY",
    "AUDJPY","CHFJPY","EURCHF","EURAUD","EURCAD",
    "GBPCHF","GBPAUD","AUDCAD","CADCHF","CADJPY",
    "NZDJPY","NZDCAD","AUDNZD","GBPNZD","EURNZD",
]
REAL_CURRENCY_UNCORR = [
    "USDTRY","USDMXN","USDSGD","USDNOK","USDPLN",
    "USDHUF","USDCNH","USDTHB","USDMYR","USDZAR",
    "USDHKD","USDSEK","USDDKK","EURNOK","GBPNOK",
    "AUDSEK","EURPLN","USDBRL","EURHUF","USDCZK",
    "EURSEK","GBPSEK","USDILS","USDKRW","USDTWD","USDIDR",
]

# ── Crypto ────────────────────────────────────────────────────────────────────
CRYPTO_CORR = [
      "BTCUSDT",   # example
    "ETHUSDT",
    "BNBUSDT",
    "LTCUSDT",
    "SOLUSDT",
    "ADAUSDT",
    "DOGEUSDT",
    "DOTUSDT",
    "ZECUSDT",
    "LINKUSDT"
]
CRYPTO_UNCORR = [
    
    ]

# ── Commodity ─────────────────────────────────────────────────────────────────
COMMODITY_CORR = [
    "XAUUSD","XAGUSD","XPTUSD","XPDUSD","COPPERUSD",
    "UKOIL","USOIL","NATGAS","HEATOIL","GASOLINE",
]
COMMODITY_UNCORR = [
    "COFFEEUSD","COCOAUSD","WHEATUSD","CORNUSD","SOYBEANSUSD",
    "COTTONUSD","LIVECATTLEUSD","LEANHOGSUSD","SUGARUSD","ORANGEJUICEUSD",
]

# ── Stock ─────────────────────────────────────────────────────────────────────
STOCK_CORR = [
    "AAPL","MSFT","GOOGL","AMZN","META",
    "TSLA","NVDA","NFLX","AMD","CRM",
]
STOCK_UNCORR = [
    "JNJ","PFE","UNH","MRK","ABBV",
    "WMT","KO","PG","V","MA",
]

# ── Pre-built sets (for fast lookup) ─────────────────────────────────────────
_CRYPTO_SET    = set(CRYPTO_CORR    + CRYPTO_UNCORR)
_FOREX_SET     = set(REAL_CURRENCY_CORR + REAL_CURRENCY_UNCORR)
_COMMODITY_SET = set(COMMODITY_CORR + COMMODITY_UNCORR)
_STOCK_SET     = set(STOCK_CORR     + STOCK_UNCORR)
_ALL_SET       = _CRYPTO_SET | _FOREX_SET | _COMMODITY_SET | _STOCK_SET

# ── Type checks ───────────────────────────────────────────────────────────────
def is_crypto_symbol(symbol: str) -> bool:
    return symbol.upper().replace("/","") in _CRYPTO_SET

def is_real_currency_symbol(symbol: str) -> bool:
    return symbol.upper().replace("/","") in _FOREX_SET

def is_commodity_symbol(symbol: str) -> bool:
    return symbol.upper().replace("/","") in _COMMODITY_SET

def is_stock_symbol(symbol: str) -> bool:
    return symbol.upper().replace("/","") in _STOCK_SET

# ── Getters ───────────────────────────────────────────────────────────────────
def get_all_pairs() -> list:
    return sorted(_ALL_SET)

def get_crypto_pairs() -> list:
    return sorted(_CRYPTO_SET)

def get_real_currency_pairs() -> list:
    return sorted(_FOREX_SET)

def get_commodity_pairs() -> list:
    return sorted(_COMMODITY_SET)

def get_stock_pairs() -> list:
    return sorted(_STOCK_SET)

def get_pairs_by_market(market_name: str) -> list:
    """
    Return pair list for given market.
    Binary OTC / IQ Option → all pairs (OTC available for most)
    CFD / Spot / Future → all pairs
    """
    mapping = {
        "Binary OTC":    get_all_pairs,
        "CFD":           get_all_pairs,
        "Spot":          lambda: sorted(_CRYPTO_SET | _FOREX_SET),
        "Future":        lambda: sorted(_CRYPTO_SET | _COMMODITY_SET),
        "Crypto":        get_crypto_pairs,
        "Forex":         get_real_currency_pairs,
        "Commodities":   get_commodity_pairs,
        "Stocks":        get_stock_pairs,
    }
    fn = mapping.get(market_name, get_all_pairs)
    pairs = fn()
    print(f"[pairs] {market_name} → {len(pairs)} pairs")
    return pairs

def get_ws_pairs() -> list:
    """All pairs for WebSocket subscription."""
    return get_all_pairs()

def get_correlation_group(symbol: str) -> str:
    s = symbol.upper().replace("/","")
    if s in set(REAL_CURRENCY_CORR):   return "forex_high_corr"
    if s in set(REAL_CURRENCY_UNCORR): return "forex_low_corr"
    if s in set(CRYPTO_CORR):          return "crypto_high_corr"
    if s in set(CRYPTO_UNCORR):        return "crypto_low_corr"
    if s in set(COMMODITY_CORR):       return "commodity_high_corr"
    if s in set(COMMODITY_UNCORR):     return "commodity_low_corr"
    if s in set(STOCK_CORR):           return "stock_high_corr"
    if s in set(STOCK_UNCORR):         return "stock_low_corr"
    return "unknown"

print(f"✅ [pairs] Loaded OK: {len(_ALL_SET)} total symbols")
