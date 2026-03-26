# data_manager.py - WebSocket as PRIMARY data source
import time
from data_sources.websocket import WebSocketSource
from pairs_config import get_ws_pairs

class DataManager:
    def __init__(self):
        # PRIMARY: WebSocket for live data
        self.websocket = WebSocketSource()
        self.cache = {}
        self.cache_time = {}
        print("[DataManager] Initialized - WebSocket PRIMARY")
    
    def initialize(self):
        """Initialize with WebSocket as primary live data source"""
        print("[DataManager] Initializing with WebSocket...")
        
        try:
            # Get all pairs for WebSocket
            ws_pairs = get_ws_pairs()
            print(f"[DataManager] Found {len(ws_pairs)} pairs for WebSocket")
        except Exception as e:
            print(f"[DataManager] Error loading pairs: {e}")
            return 0
        
        # Start WebSocket connection (PRIMARY)
        try:
            self.websocket.connect(ws_pairs[:100])  # Connect first 100
            print("[DataManager] WebSocket connected - live data flowing")
        except Exception as e:
            print(f"[DataManager] WebSocket connection failed: {e}")
            return 0
        
        # Initialize empty cache for all pairs
        loaded_count = 0
        for pair in ws_pairs[:100]:
            key = f"{pair}_5m"
            self.cache[key] = []
            self.cache_time[key] = time.time()
            loaded_count += 1
        
        print(f"[DataManager] ===== INITIALIZATION COMPLETE =====")
        print(f"[DataManager] Pairs ready: {loaded_count}")
        print(f"[DataManager] Primary source: WebSocket (LIVE)")
        print(f"[DataManager] =================================")
        
        return loaded_count
    
    def get_data(self, symbol, interval="5m", limit=70):
        """Get market data - uses WebSocket primarily"""
        key = f"{symbol}_{interval}"
        
        # Try WebSocket first (PRIMARY)
        ws_price = self.websocket.get_price(symbol)
        if ws_price:
            # Create/update candle with live price
            current_time = int(time.time() * 1000)
            live_candle = {
                'timestamp': current_time,
                'open': ws_price,
                'high': ws_price,
                'low': ws_price,
                'close': ws_price,
                'volume': 0
            }
            
            # Update cache with live data
            if key not in self.cache:
                self.cache[key] = []
            
            self.cache[key].append(live_candle)
            if len(self.cache[key]) > limit:
                self.cache[key] = self.cache[key][-limit:]
            
            return self.cache[key]
        
        # Fallback to cache
        if key in self.cache and len(self.cache[key]) >= limit:
            return self.cache[key][-limit:]
        
        return []
    
    def get_price(self, symbol):
        """Get current price - WebSocket first"""
        # PRIMARY: WebSocket live price
        ws_price = self.websocket.get_price(symbol)
        if ws_price:
            return float(ws_price)
        
        # FALLBACK: Cache
        for interval in ["1m", "5m", "15m"]:
            key = f"{symbol}_{interval}"
            if key in self.cache and self.cache[key]:
                return float(self.cache[key][-1]["close"])
        
        return 0
    
    def get_cache_info(self):
        """Get detailed cache information"""
        total_symbols = len(self.cache)
        valid_cache = 0
        
        for key, timestamp in self.cache_time.items():
            age = time.time() - timestamp
            if age < 300:  # 5 minutes
                valid_cache += 1
        
        return {
            "total_symbols": total_symbols,
            "valid_cache_5min": valid_cache,
            "websocket_connected": self.websocket.is_connected,
            "primary_source": "WebSocket"
        }
    
    def get_supported_symbols(self):
        """Get list of currently cached symbols"""
        return list(self.cache.keys())
    
    def clear_cache(self):
        """Clear all cache"""
        self.cache.clear()
        self.cache_time.clear()
        print("[DataManager] Cache cleared")

# Global instance
_data_manager = None

def get_data_manager():
    global _data_manager
    if _data_manager is None:
        _data_manager = DataManager()
    return _data_manager

# Backward compatibility functions
def get_rows(symbol, interval="5m", limit=50):
    """Get rows from WebSocket primarily"""
    return get_data_manager().get_data(symbol, interval, limit)

def get_price(symbol):
    """Get price from WebSocket primarily"""
    return get_data_manager().get_price(symbol)

def prefetch(pairs, interval="5m"):
    """Initialize data"""
    return get_data_manager().initialize()

def start_ws():
    """Start WebSocket connection"""
    try:
        dm = get_data_manager()
        return dm.websocket.is_connected
    except Exception as e:
        print(f"[DataManager] WebSocket error: {e}")
        return False

def cache_info():
    """Get cache information"""
    dm = get_data_manager()
    return {
        "websocket_connected": dm.websocket.is_connected,
        "total_cached": len(dm.cache)
    }

def detailed_cache_info():
    """Get detailed cache statistics"""
    return get_data_manager().get_cache_info()

def get_supported_symbols():
    """Get list of cached symbols"""
    return get_data_manager().get_supported_symbols()