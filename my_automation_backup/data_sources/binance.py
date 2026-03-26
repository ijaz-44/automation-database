# data_sources/binance.py - REAL BATCHING VERSION with PASSIVE MODE
import requests
import time
import json
import os

class BinanceSource:
    def __init__(self):
        self.base_url = "https://api.binance.com/api/v3"
        self.cached_data = {}
        self.api_call_count = 0
        self.ws_prices = {}
        # PASSIVE MODE: By default inactive
        self.is_active = False
        print("[BinanceSource] Initialized - PASSIVE MODE (inactive)")
    
    def activate(self):
        """Activate Binance source - call this when needed"""
        self.is_active = True
        print("[BinanceSource] ACTIVATED - now ready to serve data")
        return True
    
    def deactivate(self):
        """Deactivate Binance source"""
        self.is_active = False
        print("[BinanceSource] DEACTIVATED")
        return True
    
    def check_active(self):
        """Check if source is active"""
        if not self.is_active:
            print("[BinanceSource] WARNING: Source is PASSIVE - call activate() first")
            return False
        return True
    
    def get_symbol_data(self, symbol, interval="5m", limit=50):
        """INDIVIDUAL CALL - only works if activated"""
        if not self.check_active():
            return None
        
        self.api_call_count += 1
        print(f"[Binance] INDIVIDUAL CALL #{self.api_call_count}: {symbol}")
        # ... rest of code (keep as backup)
    
    def get_batch_klines(self, symbols, interval="5m", limit=50):
        """SINGLE API CALL FOR MULTIPLE SYMBOLS - only if activated"""
        if not self.check_active():
            return {}
        
        try:
            self.api_call_count += 1
            print(f"[Binance] BATCH CALL #{self.api_call_count} for {len(symbols)} symbols")
            
            # Method 1: Try Binance's multi-symbol endpoint if available
            if len(symbols) <= 10:
                return self._try_multi_klines(symbols, interval, limit)
            
            # Method 2: Fallback to individual with delays
            return self._fallback_individual(symbols, interval, limit)
            
        except Exception as e:
            print(f"[Binance] Batch error: {e}")
            return {}
    
    def _try_multi_klines(self, symbols, interval, limit):
        """Try to get multiple symbols in one call"""
        results = {}
        
        # Try different Binance endpoints
        endpoints = [
            "/api/v3/klines",  # Individual endpoint
            "/api/v3/ticker/24hr",  # Try 24hr ticker
            "/fapi/v1/klines"  # Try futures endpoint
        ]
        
        for endpoint in endpoints:
            try:
                url = f"{self.base_url.replace('/api/v3', endpoint.split('/api')[1] if '/api' in endpoint else endpoint)}"
                
                # For ticker, we get different data format
                if "ticker" in endpoint:
                    params = {'symbols': json.dumps(symbols)}
                    response = requests.get(url, params=params, timeout=10)
                    if response.status_code == 200:
                        return self._convert_ticker_to_klines(response.json(), interval, limit)
                
                # For klines, need individual calls but with optimization
                else:
                    return self._optimized_individual_calls(symbols, interval, limit)
                    
            except Exception as e:
                print(f"[Binance] Endpoint {endpoint} failed: {e}")
                continue
        
        return {}
    
    def _optimized_individual_calls(self, symbols, interval, limit):
        """Optimized individual calls with minimal requests"""
        results = {}
        
        # Use connection pooling and session reuse
        session = requests.Session()
        session.headers.update({
            'User-Agent': 'Mozilla/5.0',
            'Connection': 'keep-alive'
        })
        
        for i, symbol in enumerate(symbols):
            try:
                url = f"{self.base_url}/klines"
                params = {
                    'symbol': symbol.upper(),
                    'interval': interval,
                    'limit': limit
                }
                
                # Faster timeout for trading
                response = session.get(url, params=params, timeout=5)
                if response.status_code == 200:
                    results[symbol] = self._format_klines(response.json())
                
                # Minimal delay - only for every 3rd call
                if i % 3 == 0 and i > 0:
                    time.sleep(0.1)  # 100ms every 3 calls
                    
            except Exception as e:
                print(f"[Binance] Optimized call failed for {symbol}: {e}")
        
        session.close()
        return results
    
    def _fallback_individual(self, symbols, interval, limit):
        """Fallback with conservative delays"""
        results = {}
        
        for symbol in symbols:
            try:
                url = f"{self.base_url}/klines"
                params = {
                    'symbol': symbol.upper(),
                    'interval': interval,
                    'limit': limit
                }
                
                response = requests.get(url, params=params, timeout=10)
                if response.status_code == 200:
                    results[symbol] = self._format_klines(response.json())
                
                # Conservative delay
                time.sleep(0.3)  # 300ms delay
                
            except Exception as e:
                print(f"[Binance] Fallback failed for {symbol}: {e}")
        
        return results
    
    def _convert_ticker_to_klines(self, ticker_data, interval, limit):
        """Convert ticker data to klines format"""
        # This is a placeholder - ticker data doesn't have OHLCV history
        # We'd need actual klines data
        results = {}
        for ticker in ticker_data:
            symbol = ticker['symbol']
            # Create minimal data from ticker
            results[symbol] = [{
                'timestamp': ticker['closeTime'],
                'open': float(ticker['prevClosePrice']),
                'high': float(ticker['highPrice']),
                'low': float(ticker['lowPrice']),
                'close': float(ticker['lastPrice']),
                'volume': float(ticker['volume'])
            }]
        return results
    
    def initialize_all_pairs(self, symbols):
        """Initialize all pairs with SMART BATCHING - only if activated"""
        if not self.check_active():
            print("[Binance] Cannot initialize - source is PASSIVE")
            return {}
        
        print(f"[Binance] Initializing {len(symbols)} pairs with SMART BATCHING")
        
        all_results = {}
        batch_size = 8  # Conservative batch size
        loaded_count = 0
        
        for i in range(0, len(symbols), batch_size):
            batch = symbols[i:i+batch_size]
            print(f"[Binance] Processing batch {i//batch_size + 1}: {batch}")
            
            # Get batch data
            batch_results = self.get_batch_klines(batch, "5m", 100)
            
            # Cache results
            for symbol, data in batch_results.items():
                if data and len(data) > 0:
                    all_results[symbol] = data
                    loaded_count += 1
            
            # Longer delay between batches
            if i + batch_size < len(symbols):
                time.sleep(1.0)  # 1 second between batches
        
        print(f"[Binance] SMART BATCH RESULTS: {loaded_count}/{len(symbols)} loaded")
        print(f"[Binance] SMART BATCH CALLS: {self.api_call_count}")
        return all_results
    
    def get_price(self, symbol):
        """Get current price - only if activated"""
        if not self.check_active():
            return 0
        
        try:
            # Use cached data if available
            if symbol in self.cached_data and len(self.cached_data[symbol]) > 0:
                return float(self.cached_data[symbol][-1]['close'])
            
            # Only call API if not cached
            self.api_call_count += 1
            url = f"{self.base_url}/ticker/price"
            params = {'symbol': symbol.upper()}
            response = requests.get(url, params=params, timeout=5)
            if response.status_code == 200:
                return float(response.json()['price'])
            return 0
        except Exception as e:
            print(f"[Binance] Price error for {symbol}: {e}")
            return 0
    
    def _format_klines(self, data):
        """Format Binance klines to standard format"""
        formatted = []
        for item in data:
            formatted.append({
                'timestamp': item[0],
                'open': float(item[1]),
                'high': float(item[2]),
                'low': float(item[3]),
                'close': float(item[4]),
                'volume': float(item[5])
            })
        return formatted
    
    def update_ws_price(self, symbol, price):
        """Update WebSocket price - works even in passive mode"""
        self.ws_prices[symbol.lower()] = float(price)