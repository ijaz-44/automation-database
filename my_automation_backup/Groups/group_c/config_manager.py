# Groups/group_c/config_manager.py
# Configuration manager for C-Group - Loads per-symbol JSON files from market folders

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
        self.defaults = self._load_defaults()  # kept for fallback
        
        # Map market names to folder names
        self.market_folders = {
            "Binary OTC": "binary_otc",
            "CFD": "cfd",
            "Spot": "spot",
            "Future": "future",
        }
        
        print(f"[ConfigLoader] Initialized. Config dir: {self.config_dir}")
    
    def get(self, symbol, market, interval):
        """Get configuration for specific symbol/market/interval combination."""
        key = f"{symbol}_{market}_{interval}"
        if key in self._cache:
            return self._cache[key]
        
        # Try to load per-symbol config
        config = self._load_symbol_config(symbol, market, interval)
        
        if config is None:
            # Fall back to old defaults
            config = self.defaults.get(market, self._get_fallback()).copy()
            config["interval"] = interval
            print(f"[ConfigLoader] Using fallback config for {symbol} ({market})")
        else:
            config["interval"] = interval
            print(f"[ConfigLoader] Loaded config for {symbol} ({market}, {interval})")
        
        self._cache[key] = config
        return config
    
    def _load_symbol_config(self, symbol, market, interval):
        """Load per-symbol config from file, return timeframe-specific dict or None."""
        market_folder = self.market_folders.get(market)
        if not market_folder:
            # Unknown market – no per-symbol configs
            return None
        
        file_path = os.path.join(self.config_dir, market_folder, f"{symbol}.json")
        
        if not os.path.exists(file_path):
            print(f"[ConfigLoader] Config file not found: {file_path}")
            return None
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
        except Exception as e:
            print(f"[ConfigLoader] Error loading {file_path}: {e}")
            return None
        
        # If the file contains a "timeframes" dict, use that
        if "timeframes" in data and isinstance(data["timeframes"], dict):
            # First try the exact interval
            if interval in data["timeframes"]:
                return data["timeframes"][interval]
            # Then try a default section inside the file
            if "default" in data and isinstance(data["default"], dict):
                return data["default"]
            # Fallback: use the first timeframe available
            for tf in data["timeframes"]:
                return data["timeframes"][tf]
        
        # If file is a flat config (no timeframes), assume it's the config itself
        return data
    
    def _load_defaults(self):
        """Load default configurations from JSON files (kept for fallback)."""
        defaults = {}
        
        # Try to load market-wide defaults if they exist
        default_files = {
            "Binary OTC": "binary_default.json",
            "CFD": "cfd_default.json",
            "Spot": "spot_default.json",
            "Future": "future_default.json",
        }
        
        for market, filename in default_files.items():
            try:
                file_path = os.path.join(self.config_dir, filename)
                if os.path.exists(file_path):
                    with open(file_path, 'r') as f:
                        defaults[market] = json.load(f)
                        print(f"[ConfigLoader] Loaded {filename}")
            except Exception as e:
                print(f"[ConfigLoader] Error loading {filename}: {e}")
        
        return defaults
    
    def _get_fallback(self):
        """Get fallback configuration."""
        return {
            "market": "Default",
            "weights_z": {"trend": 0.35, "volume": 0.15, "momentum": 0.30},
            "weights_a": {"structure": 0.30, "indicators": 0.35, "sr": 0.20, "candle": 0.15},
            "thresholds_z": {"strong_buy": 72, "buy": 58, "sell": 38, "strong_sell": 22},
            "thresholds_a": {"go": 62, "block": 48},
            "min_z_for_a": 48,
            "min_a_score": 62,
            "risk": {"sl_atr": 1.5, "tp_atr": 2.0}
        }
    
    def clear_cache(self):
        """Clear configuration cache."""
        self._cache = {}
        print("[ConfigLoader] Cache cleared")
    
    def reload_configs(self):
        """Reload all configurations from files."""
        self._cache = {}
        self.defaults = self._load_defaults()
        print("[ConfigLoader] Configs reloaded")

# Global config loader instance
_loader = None

def get_config_loader():
    global _loader
    if _loader is None:
        _loader = ConfigLoader()
    return _loader