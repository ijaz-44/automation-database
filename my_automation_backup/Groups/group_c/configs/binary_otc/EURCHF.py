{
  "timeframes": {
    "1m": {
      "weights_z": {
        "trend": 0.20,
        "volume": 0.20,
        "momentum": 0.60
      },
      "thresholds_z": {
        "buy": 72,
        "sell": 26,
        "strong_buy": 88,
        "strong_sell": 10
      },
      "weights_a": {
        "structure": 0.20,
        "indicators": 0.20,
        "sr": 0.20,
        "candle": 0.40
      },
      "thresholds_a": {
        "go": 80,
        "strong_buy": 90,
        "strong_sell": 8
      },
      "min_a_score": 80,
      "risk": {
        "sl_atr_mult": 1.0,
        "tp_atr_mult": 1.2
      },
      "binary_otc": {
        "expiry_mode": "next_candle",
        "min_confidence": 82,
        "allowed_signals": ["BUY", "SELL"]
      },
      "deep": {
        "min_deep_score": 78,
        "require_deep_confirm": true
      },
      "manipulation": {
        "enabled": true,
        "penalty": -30,
        "min_wick_ratio": 0.6,
        "volume_spike_threshold": 2.5,
        "false_breakout_tolerance": 0.3
      }
    },
    "2m": {
      "weights_z": {
        "trend": 0.25,
        "volume": 0.20,
        "momentum": 0.55
      },
      "thresholds_z": {
        "buy": 70,
        "sell": 28,
        "strong_buy": 85,
        "strong_sell": 12
      },
      "weights_a": {
        "structure": 0.25,
        "indicators": 0.25,
        "sr": 0.20,
        "candle": 0.30
      },
      "thresholds_a": {
        "go": 78,
        "strong_buy": 87,
        "strong_sell": 10
      },
      "min_a_score": 78,
      "risk": {
        "sl_atr_mult": 1.1,
        "tp_atr_mult": 1.4
      },
      "binary_otc": {
        "expiry_mode": "next_candle",
        "min_confidence": 80,
        "allowed_signals": ["BUY", "SELL"]
      },
      "deep": {
        "min_deep_score": 75,
        "require_deep_confirm": true
      },
      "manipulation": {
        "enabled": true,
        "penalty": -25,
        "min_wick_ratio": 0.6,
        "volume_spike_threshold": 2.2,
        "false_breakout_tolerance": 0.35
      }
    },
    "5m": {
      "weights_z": {
        "trend": 0.30,
        "volume": 0.20,
        "momentum": 0.50
      },
      "thresholds_z": {
        "buy": 68,
        "sell": 30,
        "strong_buy": 82,
        "strong_sell": 15
      },
      "weights_a": {
        "structure": 0.30,
        "indicators": 0.25,
        "sr": 0.25,
        "candle": 0.20
      },
      "thresholds_a": {
        "go": 75,
        "strong_buy": 85,
        "strong_sell": 12
      },
      "min_a_score": 75,
      "risk": {
        "sl_atr_mult": 1.3,
        "tp_atr_mult": 1.8
      },
      "binary_otc": {
        "expiry_mode": "next_candle",
        "min_confidence": 77,
        "allowed_signals": ["BUY", "SELL"]
      },
      "deep": {
        "min_deep_score": 72,
        "require_deep_confirm": true
      },
      "manipulation": {
        "enabled": true,
        "penalty": -20,
        "min_wick_ratio": 0.6,
        "volume_spike_threshold": 2.0,
        "false_breakout_tolerance": 0.4
      }
    },
    "10m": {
      "weights_z": {
        "trend": 0.35,
        "volume": 0.20,
        "momentum": 0.45
      },
      "thresholds_z": {
        "buy": 65,
        "sell": 33,
        "strong_buy": 78,
        "strong_sell": 18
      },
      "weights_a": {
        "structure": 0.35,
        "indicators": 0.25,
        "sr": 0.25,
        "candle": 0.15
      },
      "thresholds_a": {
        "go": 72,
        "strong_buy": 82,
        "strong_sell": 15
      },
      "min_a_score": 72,
      "risk": {
        "sl_atr_mult": 1.5,
        "tp_atr_mult": 2.0
      },
      "binary_otc": {
        "expiry_mode": "next_candle",
        "min_confidence": 74,
        "allowed_signals": ["BUY", "SELL"]
      },
      "deep": {
        "min_deep_score": 70,
        "require_deep_confirm": true
      },
      "manipulation": {
        "enabled": true,
        "penalty": -15,
        "min_wick_ratio": 0.6,
        "volume_spike_threshold": 1.8,
        "false_breakout_tolerance": 0.45
      }
    },
    "15m": {
      "weights_z": {
        "trend": 0.40,
        "volume": 0.20,
        "momentum": 0.40
      },
      "thresholds_z": {
        "buy": 62,
        "sell": 36,
        "strong_buy": 75,
        "strong_sell": 20
      },
      "weights_a": {
        "structure": 0.40,
        "indicators": 0.25,
        "sr": 0.25,
        "candle": 0.10
      },
      "thresholds_a": {
        "go": 68,
        "strong_buy": 78,
        "strong_sell": 18
      },
      "min_a_score": 68,
      "risk": {
        "sl_atr_mult": 1.8,
        "tp_atr_mult": 2.2
      },
      "binary_otc": {
        "expiry_mode": "next_candle",
        "min_confidence": 72,
        "allowed_signals": ["BUY", "SELL"]
      },
      "deep": {
        "min_deep_score": 68,
        "require_deep_confirm": true
      },
      "manipulation": {
        "enabled": true,
        "penalty": -10,
        "min_wick_ratio": 0.6,
        "volume_spike_threshold": 1.6,
        "false_breakout_tolerance": 0.5
      }
    }
  },
  "default": {
    "weights_z": {
      "trend": 0.35,
      "volume": 0.15,
      "momentum": 0.30
    },
    "thresholds_z": {
      "buy": 58,
      "sell": 38,
      "strong_buy": 72,
      "strong_sell": 22
    },
    "weights_a": {
      "structure": 0.25,
      "indicators": 0.25,
      "sr": 0.25,
      "candle": 0.25
    },
    "thresholds_a": {
      "go": 62,
      "strong_buy": 75,
      "strong_sell": 20
    },
    "min_a_score": 62,
    "risk": {
      "sl_atr_mult": 1.5,
      "tp_atr_mult": 2.0
    },
    "binary_otc": {
      "expiry_mode": "next_candle",
      "min_confidence": 65,
      "allowed_signals": ["BUY", "SELL"]
    },
    "deep": {
      "min_deep_score": 60,
      "require_deep_confirm": false
    },
    "manipulation": {
      "enabled": false,
      "penalty": 0
    }
  }
}