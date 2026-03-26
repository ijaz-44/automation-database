# pairs_config.py - Updated with 4 categories (Real Currency, Crypto, Commodity, Stock)
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
            print(f"[Pairs] Added {len(pairs)} pairs from {category}")
    
    print(f"[Pairs] Total pairs: {len(all_pairs)}")
    return all_pairs

def get_real_currency_pairs():
    """Get all real currency (forex) pairs - 70 total (35 corr + 35 uncorr)"""
    config = get_pairs_config()
    forex_pairs = []
    
    for category in ["real_currency_corr", "real_currency_uncorr"]:
        pairs = config.get(category, [])
        if pairs:
            forex_pairs.extend(pairs)
    
    print(f"[Pairs] Found {len(forex_pairs)} total real currency pairs")
    print(f"[Pairs] Correlated: {len(config.get('real_currency_corr', []))}")
    print(f"[Pairs] Uncorrelated: {len(config.get('real_currency_uncorr', []))}")
    
    return forex_pairs

def get_crypto_pairs():
    """Get all crypto pairs - 50 total (25 corr + 25 uncorr)"""
    config = get_pairs_config()
    crypto_pairs = []
    
    for category in ["crypto_corr", "crypto_uncorr"]:
        pairs = config.get(category, [])
        if pairs:
            crypto_pairs.extend(pairs)
    
    print(f"[Pairs] Found {len(crypto_pairs)} total crypto pairs")
    print(f"[Pairs] Correlated: {len(config.get('crypto_corr', []))}")
    print(f"[Pairs] Uncorrelated: {len(config.get('crypto_uncorr', []))}")
    
    return crypto_pairs

def get_commodity_pairs():
    """Get all commodity pairs - 20 total (10 corr + 10 uncorr)"""
    config = get_pairs_config()
    commodity_pairs = []
    
    for category in ["commodity_corr", "commodity_uncorr"]:
        pairs = config.get(category, [])
        if pairs:
            commodity_pairs.extend(pairs)
    
    print(f"[Pairs] Found {len(commodity_pairs)} total commodity pairs")
    print(f"[Pairs] Correlated: {len(config.get('commodity_corr', []))}")
    print(f"[Pairs] Uncorrelated: {len(config.get('commodity_uncorr', []))}")
    
    return commodity_pairs

def get_stock_pairs():
    """Get all stock pairs - 20 total (10 corr + 10 uncorr)"""
    config = get_pairs_config()
    stock_pairs = []
    
    for category in ["stock_corr", "stock_uncorr"]:
        pairs = config.get(category, [])
        if pairs:
            stock_pairs.extend(pairs)
    
    print(f"[Pairs] Found {len(stock_pairs)} total stock pairs")
    print(f"[Pairs] Correlated: {len(config.get('stock_corr', []))}")
    print(f"[Pairs] Uncorrelated: {len(config.get('stock_uncorr', []))}")
    
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
    """Get pairs by specific market name"""
    config = get_pairs_config()
    
    market_mapping = {
        "Real Currency": ["real_currency_corr", "real_currency_uncorr"],
        "Forex": ["real_currency_corr", "real_currency_uncorr"],
        "Crypto": ["crypto_corr", "crypto_uncorr"],
        "Commodities": ["commodity_corr", "commodity_uncorr"],
        "Stocks": ["stock_corr", "stock_uncorr"],
        "Indices": [],
        "Binary OTC": ["real_currency_corr", "real_currency_uncorr"],
        "Binary Digital": ["crypto_corr", "crypto_uncorr"],
    }
    
    categories = market_mapping.get(market_name, [])
    pairs = []
    
    for category in categories:
        category_pairs = config.get(category, [])
        if category_pairs:
            pairs.extend(category_pairs)
    
    return pairs

def get_ws_pairs():
    """Get WebSocket pairs - ALL pairs for WebSocket connection"""
    # Get all pairs from all 4 categories for WebSocket
    all_pairs = []
    
    # Real Currency (Forex) - 70 pairs
    all_pairs.extend(get_real_currency_pairs())
    
    # Crypto - 50 pairs
    all_pairs.extend(get_crypto_pairs())
    
    # Commodity - 20 pairs
    all_pairs.extend(get_commodity_pairs())
    
    # Stock - 20 pairs
    all_pairs.extend(get_stock_pairs())
    
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