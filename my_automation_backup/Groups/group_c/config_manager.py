# Groups/group_c/config_manager.py
"""
Configuration & Scoring Manager – Central scoring logic.
- Computes Z-score (trend, volume, momentum)
- Computes A-score (structure, indicators, sr, candle)
- Combines Z + A for final GO score
- Tracks issues (errors/warnings) for debugging
"""

import logging
from typing import Dict, List, Optional, Tuple
from . import (
    DEFAULT_WEIGHTS_Z, DEFAULT_WEIGHTS_A,
    DEFAULT_THRESHOLDS_Z, DEFAULT_THRESHOLDS_A,
    DEFAULT_MIN_Z_FOR_A, DEFAULT_MIN_A_SCORE,
    DEFAULT_RISK, get_defaults_for_market
)

# Setup logger for issue tracking
logger = logging.getLogger("ConfigLoader")
logger.setLevel(logging.INFO)

class ConfigLoader:
    def __init__(self):
        self._cache = {}
        self._issues = []  # list of (level, message)
        print("[ConfigLoader] Initialized (score calculation + issue tracker)")

    def _log_issue(self, level: str, message: str):
        """Store issue for later retrieval and also log."""
        self._issues.append((level, message))
        if level == "ERROR":
            logger.error(message)
        elif level == "WARNING":
            logger.warning(message)
        else:
            logger.info(message)

    def get_issues(self) -> List[Tuple[str, str]]:
        """Return list of all issues (level, message) since last clear."""
        return self._issues.copy()

    def clear_issues(self):
        self._issues.clear()

    # ---------- Z‑score calculation ----------
    def get_z_score(self, trend: float, volume: float, momentum: float,
                    market: str = "Binary OTC", interval: str = "5m") -> float:
        """
        Calculate weighted Z‑score from trend, volume, momentum (each 0‑100).
        Returns a float (0‑100).
        """
        try:
            w = DEFAULT_WEIGHTS_Z.get(market, DEFAULT_WEIGHTS_Z["Binary OTC"])
            total_weight = w["trend"] + w["volume"] + w["momentum"]
            if total_weight <= 0:
                self._log_issue("ERROR", f"Invalid Z weights: {w}")
                return 0.0
            score = (trend * w["trend"] + volume * w["volume"] + momentum * w["momentum"]) / total_weight
            score = max(0.0, min(100.0, score))  # clamp
            self._log_issue("INFO", f"Z-score computed: {score:.2f} (trend={trend}, vol={volume}, mom={momentum})")
            return round(score, 2)
        except Exception as e:
            self._log_issue("ERROR", f"Z-score calculation error: {e}")
            return 0.0

    # ---------- A‑score calculation ----------
    def get_a_score(self, structure: float, indicators: float, sr: float, candle: float,
                    market: str = "Binary OTC", interval: str = "5m") -> float:
        """
        Calculate weighted A‑score from structure, indicators, sr, candle (each 0‑100).
        Returns a float (0‑100).
        """
        try:
            w = DEFAULT_WEIGHTS_A.get(market, DEFAULT_WEIGHTS_A["Binary OTC"])
            total_weight = w["structure"] + w["indicators"] + w["sr"] + w["candle"]
            if total_weight <= 0:
                self._log_issue("ERROR", f"Invalid A weights: {w}")
                return 0.0
            score = (structure * w["structure"] + indicators * w["indicators"] +
                     sr * w["sr"] + candle * w["candle"]) / total_weight
            score = max(0.0, min(100.0, score))
            self._log_issue("INFO", f"A-score computed: {score:.2f} (struct={structure}, ind={indicators}, sr={sr}, candle={candle})")
            return round(score, 2)
        except Exception as e:
            self._log_issue("ERROR", f"A-score calculation error: {e}")
            return 0.0

    # ---------- Combined score (for GO button) ----------
    def get_combined_score(self, z_score: float, a_score: float,
                           market: str = "Binary OTC", interval: str = "5m") -> float:
        """
        Combine Z‑score and A‑score for the final GO signal.
        By default, returns the average of both (0‑100).
        """
        try:
            # Simple average – you can change to weighted sum if needed
            combined = (z_score + a_score) / 2.0
            combined = max(0.0, min(100.0, combined))
            self._log_issue("INFO", f"Combined score: {combined:.2f} (Z={z_score}, A={a_score})")
            return round(combined, 2)
        except Exception as e:
            self._log_issue("ERROR", f"Combined score error: {e}")
            return 0.0

    # ---------- Convenience methods for direct data dicts ----------
    def get_z_score_from_dict(self, data: Dict[str, float], market: str = "Binary OTC", interval: str = "5m") -> float:
        """Expects dict with keys: trend, volume, momentum."""
        return self.get_z_score(data.get("trend", 0), data.get("volume", 0),
                                data.get("momentum", 0), market, interval)

    def get_a_score_from_dict(self, data: Dict[str, float], market: str = "Binary OTC", interval: str = "5m") -> float:
        """Expects dict with keys: structure, indicators, sr, candle."""
        return self.get_a_score(data.get("structure", 0), data.get("indicators", 0),
                                data.get("sr", 0), data.get("candle", 0), market, interval)

    # ---------- Optional: get full config (weights, thresholds, etc.) ----------
    def get_config(self, symbol: str, market: str, interval: str) -> dict:
        key = f"{symbol}_{market}_{interval}"
        if key in self._cache:
            return self._cache[key]
        config = get_defaults_for_market(market, interval)
        self._cache[key] = config
        return config

    def get_weights_z(self, market: str, interval: str) -> dict:
        return DEFAULT_WEIGHTS_Z.get(market, DEFAULT_WEIGHTS_Z["Binary OTC"])

    def get_weights_a(self, market: str, interval: str) -> dict:
        return DEFAULT_WEIGHTS_A.get(market, DEFAULT_WEIGHTS_A["Binary OTC"])

    def get_thresholds_z(self, market: str, interval: str) -> dict:
        return DEFAULT_THRESHOLDS_Z.get(market, DEFAULT_THRESHOLDS_Z["Binary OTC"])

    def get_thresholds_a(self, market: str, interval: str) -> dict:
        return DEFAULT_THRESHOLDS_A.get(market, DEFAULT_THRESHOLDS_A["Binary OTC"])

    def get_min_z_for_a(self, market: str, interval: str) -> float:
        return DEFAULT_MIN_Z_FOR_A.get(market, DEFAULT_MIN_Z_FOR_A["Binary OTC"])

    def get_min_a_score(self, market: str, interval: str) -> float:
        return DEFAULT_MIN_A_SCORE.get(market, DEFAULT_MIN_A_SCORE["Binary OTC"])

    def get_risk(self, market: str, interval: str) -> dict:
        return DEFAULT_RISK.get(market, DEFAULT_RISK["Binary OTC"])

    def clear_cache(self):
        self._cache.clear()

# Global instance
_loader = None

def get_config_loader():
    global _loader
    if _loader is None:
        _loader = ConfigLoader()
    return _loader