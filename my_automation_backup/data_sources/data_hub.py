# data_sources/data_hub.py
import time
from abc import ABC, abstractmethod

# ========== Abstract Base Class ==========
class DataSource(ABC):
    """Base class for all data sources"""
    
    @abstractmethod
    def connect(self, symbols):
        pass
    
    @abstractmethod
    def get_price(self, symbol):
        pass
    
    @abstractmethod
    def disconnect(self):
        pass
    
    @property
    @abstractmethod
    def name(self):
        pass
    
    @property
    @abstractmethod
    def is_connected(self):
        pass


# ========== Data Hub ==========
class DataHub:
    """Central hub that manages multiple data sources"""
    
    def __init__(self):
        self.sources = []  # (priority, source)
        self._prices = {}  # Cache for stage functions
        self._price_time = {}
        print("[DataHub] Initialized")
    
    def add_source(self, source, priority=5):
        """Add source (lower priority number = higher priority)"""
        self.sources.append((priority, source))
        self.sources.sort(key=lambda x: x[0])
        print(f"[DataHub] Added {source.name} (priority {priority})")
    
    def get_price(self, symbol, require_confirmation=False):
        """
        Get price from highest priority source
        If confirmation=True, returns price only if 2+ sources agree
        """
        prices = []
        sources_used = []
        
        for priority, source in self.sources:
            if source.is_connected:
                price = source.get_price(symbol)
                if price and price > 0:
                    prices.append(price)
                    sources_used.append(source.name)
        
        if not prices:
            return None
        
        # Confirmation: need 2+ sources within 0.5%
        if require_confirmation and len(prices) >= 2:
            avg = sum(prices) / len(prices)
            if all(abs(p - avg) / avg < 0.005 for p in prices):
                price = round(avg, 6)
                self._prices[symbol] = price
                self._price_time[symbol] = time.time()
                return price
            return None
        
        # Return highest priority
        for priority, source in self.sources:
            if source.is_connected:
                price = source.get_price(symbol)
                if price:
                    self._prices[symbol] = price
                    self._price_time[symbol] = time.time()
                    return price
        return None
    
    def get_all_prices(self, symbol):
        """Get prices from all connected sources"""
        result = {}
        for priority, source in self.sources:
            if source.is_connected:
                price = source.get_price(symbol)
                if price:
                    result[source.name] = price
        return result
    
    def get_cached_price(self, symbol):
        """Get last known price from cache"""
        data = self._prices.get(symbol.lower())
        if data:
            age = time.time() - self._price_time.get(symbol.lower(), 0)
            if age < 300:  # 5 minutes
                return data
        return None
    
    def is_any_connected(self):
        return any(s.is_connected for _, s in self.sources)
    
    def disconnect_all(self):
        for _, source in self.sources:
            source.disconnect()
        print("[DataHub] All sources disconnected")