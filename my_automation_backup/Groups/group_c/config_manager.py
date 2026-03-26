# Groups/group_c/config_manager.py
# Configuration manager for C-Group

import os
import json

class ConfigLoader:
    def __init__(self, base_dir=None):
        if base_dir is None:
            # Try to find the Groups directory
            current_dir = os.path.dirname(os.path.abspath(__file__))
            base_dir = os.path.dirname(os.path.dirname(current_dir))
        
        self.base_dir = base_dir
        self.config_dir = os.path.join(base_dir, "Groups", "group_c", "configs")
        self._cache = {}
        self.defaults = self._load_defaults()
    
    def get(self, symbol, market, interval):
        """Get configuration for specific symbol/market/interval combination"""
        key = f"{symbol}_{market}_{interval}"
        if key in self._cache:
            return self._cache[key]
        
        # Start with market defaults
        config = self.defaults.get(market, self._get_fallback()).copy()
        
        # Override with interval setting
        config["interval"] = interval
        
        # TODO: Add symbol-specific overrides if needed
        # This can be extended later for pair-specific configs
        
        self._cache[key] = config
        return config
    
    def _load_defaults(self):
        """Load default configurations from JSON files"""
        defaults = {}
        
        # Load crypto defaults
        try:
            crypto_path = os.path.join(self.config_dir, "crypto_default.json")
            if os.path.exists(crypto_path):
                with open(crypto_path, 'r') as f:
                    defaults["Crypto"] = json.load(f)
        except Exception as e:
            print(f"[Config] Error loading crypto config: {e}")
        
        # Load forex defaults
        try:
            forex_path = os.path.join(self.config_dir, "forex_default.json")
            if os.path.exists(forex_path):
                with open(forex_path, 'r') as f:
                    defaults["Forex"] = json.load(f)
        except Exception as e:
            print(f"[Config] Error loading forex config: {e}")
        
        # Load binary defaults
        try:
            binary_path = os.path.join(self.config_dir, "binary_default.json")
            if os.path.exists(binary_path):
                with open(binary_path, 'r') as f:
                    defaults["Binary OTC"] = json.load(f)
        except Exception as e:
            print(f"[Config] Error loading binary config: {e}")
        
        return defaults
    
    def _get_fallback(self):
        """Get fallback configuration"""
        return self.defaults.get("Forex", {
            "market": "Default",
            "weights_z": {"trend": 0.35, "volume": 0.10, "momentum": 0.30},
            "weights_a": {"structure": 0.35, "indicators": 0.35, "sr": 0.20, "candle": 0.10},
            "thresholds_z": {"strong_buy": 75, "buy": 60, "wait": 45},
            "thresholds_a": {"go": 60, "block": 45},
            "min_z_for_a": 48,
            "min_a_score": 58,
            "risk": {"sl_atr": 1.2, "tp_atr": 2.0}
        })
    
    def clear_cache(self):
        """Clear configuration cache"""
        self._cache = {}
    
    def reload_configs(self):
        """Reload all configurations from files"""
        self._cache = {}
        self.defaults = self._load_defaults()


# Global config loader instance
_loader = None

def get_config_loader():
    global _loader
    if _loader is None:
        _loader = ConfigLoader()
    return _loader