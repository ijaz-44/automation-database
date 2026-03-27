# pairs_config.py - Updated with 4 markets (Binary OTC, CFD, Spot, Future)
import json
import os

def get_pairs_config():
    """Load pairs configuration from JSON file"""
    try:
        config_path = os.path.join(os.path.dirname(__file__), 'pairs.json')
        with open(config_path, 'r') as f:
            config = json.load(f)
            print(f"[Pairs] Loaded config with correlation strategy")
            return config
    except Exception as e:
        print(f"[Pairs] Error loading config: {e}")
        return {
            "real_currency_corr": [],
            "real_currency_uncorr": [],
            "crypto_corr": [],
            "crypto_uncorr": [],
            "commodity_corr": [],
            "commodity_uncorr": [],
            "stock_corr": [],
            "stock_uncorr": [],
            "ws_pairs": ["btcusdt", "ethusdt"]
        }

def get_all_pairs():
    """Get all pairs from config"""
    config = get_pairs_config()
    all_pairs = []
    
    # All 4 categories with correlated and uncorrelated
    all_categories = [
        "real_currency_corr", "real_currency_uncorr",
        "crypto_corr", "crypto_uncorr", 
        "commodity_corr", "commodity_uncorr",
        "stock_corr", "stock_uncorr"
    ]
    
    for category in all_categories:
        pairs = config.get(category, [])
        if pairs:
            all_pairs.extend(pairs)
    
    return all_pairs

def get_real_currency_pairs():
    """Get all real currency (forex) pairs - unique only"""
    config = get_pairs_config()
    forex_pairs = []
    
    # Get unique pairs from both lists
    corr = set(config.get("real_currency_corr", []))
    uncorr = set(config.get("real_currency_uncorr", []))
    
    forex_pairs = list(corr | uncorr)
    return forex_pairs

def get_crypto_pairs():
    """Get all crypto pairs - unique only"""
    config = get_pairs_config()
    crypto_pairs = []
    
    # Get unique pairs from both lists
    corr = set(config.get("crypto_corr", []))
    uncorr = set(config.get("crypto_uncorr", []))
    
    crypto_pairs = list(corr | uncorr)
    return crypto_pairs

def get_commodity_pairs():
    """Get all commodity pairs - unique only"""
    config = get_pairs_config()
    commodity_pairs = []
    
    # Get unique pairs from both lists
    corr = set(config.get("commodity_corr", []))
    uncorr = set(config.get("commodity_uncorr", []))
    
    commodity_pairs = list(corr | uncorr)
    return commodity_pairs

def get_stock_pairs():
    """Get all stock pairs - unique only"""
    config = get_pairs_config()
    stock_pairs = []
    
    # Get unique pairs from both lists
    corr = set(config.get("stock_corr", []))
    uncorr = set(config.get("stock_uncorr", []))
    
    stock_pairs = list(corr | uncorr)
    return stock_pairs

def get_correlation_group(symbol):
    """Get correlation group for symbol"""
    config = get_pairs_config()
    symbol_upper = symbol.upper().strip()
    
    if symbol_upper in [p.upper().strip() for p in config.get('real_currency_corr', [])]:
        return "forex_high_corr"
    elif symbol_upper in [p.upper().strip() for p in config.get('real_currency_uncorr', [])]:
        return "forex_low_corr"
    elif symbol_upper in [p.upper().strip() for p in config.get('crypto_corr', [])]:
        return "crypto_high_corr"
    elif symbol_upper in [p.upper().strip() for p in config.get('crypto_uncorr', [])]:
        return "crypto_low_corr"
    elif symbol_upper in [p.upper().strip() for p in config.get('commodity_corr', [])]:
        return "commodity_high_corr"
    elif symbol_upper in [p.upper().strip() for p in config.get('commodity_uncorr', [])]:
        return "commodity_low_corr"
    elif symbol_upper in [p.upper().strip() for p in config.get('stock_corr', [])]:
        return "stock_high_corr"
    elif symbol_upper in [p.upper().strip() for p in config.get('stock_uncorr', [])]:
        return "stock_low_corr"
    else:
        return "unknown"

def get_pairs_by_correlation():
    """Get pairs grouped by correlation"""
    config = get_pairs_config()
    
    return {
        "real_currency": {
            "high_corr": config.get('real_currency_corr', []),
            "low_corr": config.get('real_currency_uncorr', [])
        },
        "crypto": {
            "high_corr": config.get('crypto_corr', []),
            "low_corr": config.get('crypto_uncorr', [])
        },
        "commodity": {
            "high_corr": config.get('commodity_corr', []),
            "low_corr": config.get('commodity_uncorr', [])
        },
        "stock": {
            "high_corr": config.get('stock_corr', []),
            "low_corr": config.get('stock_uncorr', [])
        }
    }

def get_pairs_by_market(market_name):
    """Get pairs by specific market name - ALL 139 symbols for each market"""
    # All 139 unique symbols from all categories
    all_pairs = []
    all_pairs.extend(get_real_currency_pairs())
    all_pairs.extend(get_crypto_pairs())
    all_pairs.extend(get_commodity_pairs())
    all_pairs.extend(get_stock_pairs())
    
    # Remove duplicates
    all_pairs = list(set(all_pairs))
    
    # For all markets, return ALL symbols
    # Because each market (Binary OTC, CFD, Spot, Future) can trade all these instruments
    market_mapping = {
        "Real Currency": all_pairs,
        "Forex": all_pairs,
        "Crypto": get_crypto_pairs(),
        "Commodities": get_commodity_pairs(),
        "Stocks": get_stock_pairs(),
        "Binary OTC": all_pairs,
        "Binary Digital": get_crypto_pairs(),
        "CFD": all_pairs,
        "Spot": all_pairs,
        "Future": all_pairs,
    }
    
    pairs = market_mapping.get(market_name, [])
    
    # For backwards compatibility with existing code
    if market_name == "Binary OTC":
        print(f"[Pairs] Binary OTC pairs: {len(pairs)}")
    elif market_name == "CFD":
        print(f"[Pairs] CFD pairs: {len(pairs)}")
    elif market_name == "Spot":
        print(f"[Pairs] Spot pairs: {len(pairs)}")
    elif market_name == "Future":
        print(f"[Pairs] Future pairs: {len(pairs)}")
    
    return pairs

def get_ws_pairs():
    """Get WebSocket pairs - ALL pairs for WebSocket connection"""
    # Get all unique pairs from all 4 categories
    all_pairs = []
    all_pairs.extend(get_real_currency_pairs())
    all_pairs.extend(get_crypto_pairs())
    all_pairs.extend(get_commodity_pairs())
    all_pairs.extend(get_stock_pairs())
    
    # Remove duplicates
    all_pairs = list(set(all_pairs))
    
    # Convert to lowercase for WebSocket format
    ws_pairs = []
    for pair in all_pairs:
        # Remove / if present and convert to lowercase
        clean_pair = pair.replace("/", "").replace("USD", "USDT").lower()
        ws_pairs.append(clean_pair)
    
    print(f"[Pairs] Total WebSocket pairs: {len(ws_pairs)}")
    return ws_pairs

def is_real_currency_symbol(symbol):
    """Check if symbol is real currency (forex)"""
    forex_pairs = get_real_currency_pairs()
    return symbol.upper().strip() in [p.upper().strip() for p in forex_pairs]

def is_crypto_symbol(symbol):
    """Check if symbol is crypto"""
    crypto_pairs = get_crypto_pairs()
    return symbol.upper().strip() in [p.upper().strip() for p in crypto_pairs]

def is_commodity_symbol(symbol):
    """Check if symbol is commodity"""
    commodity_pairs = get_commodity_pairs()
    return symbol.upper().strip() in [p.upper().strip() for p in commodity_pairs]

def is_stock_symbol(symbol):
    """Check if symbol is stock"""
    stock_pairs = get_stock_pairs()
    return symbol.upper().strip() in [p.upper().strip() for p in stock_pairs]