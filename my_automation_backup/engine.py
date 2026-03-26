# engine.py — Score System + Config Manager
# Sirf yeh score calculate kare, baki sab sirf data provide karein

import os
import sys
import time
import json
from typing import Dict, Any, List, Optional

# Add path for imports
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

# Import config loader
try:
    # Try different import paths for group_c
    from Groups.group_c.config_manager import get_config_loader
    config_loader_available = True
except ImportError:
    try:
        from group_c.config_manager import get_config_loader
        config_loader_available = True
    except ImportError:
        config_loader_available = False
        print("[Engine] Warning: C-Group config loader not found")
        get_config_loader = None

# Import Z-group modules
try:
    sys.path.insert(0, os.path.join(BASE_DIR, "Groups", "group_z"))
    from Z01_trend import score as z_trend_score
    from Z02_volume import score as z_volume_score  
    from Z03_momentum import score as z_momentum_score
    z_modules_available = True
except ImportError as e:
    z_modules_available = False
    print(f"[Engine] Warning: Z-modules not found: {e}")

# Import A-group modules  
try:
    sys.path.insert(0, os.path.join(BASE_DIR, "Groups", "group_a"))
    from A01_structure import confirm as a_structure_confirm
    from A02_indicators import confirm as a_indicators_confirm
    from A03_sr import confirm as a_sr_confirm
    from A04_candle import confirm as a_candle_confirm
    a_modules_available = True
except ImportError as e:
    a_modules_available = False
    print(f"[Engine] Warning: A-modules not found: {e}")


class TradingEngine:
    """Main scoring engine - har pair ka score yahan calculate hoga"""
    
    def __init__(self):
        self.config_loader = None
        if config_loader_available and get_config_loader:
            try:
                self.config_loader = get_config_loader()
                print("[Engine] Config loader connected to Group C")
            except Exception as e:
                print(f"[Engine] Config loader init error: {e}")
        else:
            print("[Engine] Using default scoring - Group C not available")
        
        print("[Engine] Initialized - Real Data Only")
    
    def get_z_score(self, symbol: str, market: str, interval: str, rows: List) -> Dict[str, Any]:
        """
        Z layer score - fast scanner ke liye
        Returns: {score, signal, trend, sr_position, reason}
        """
        try:
            # Get pair-specific config from Group C
            config = {}
            if self.config_loader:
                try:
                    config = self.config_loader.get(symbol, market, interval)
                    print(f"[Engine] Using {symbol}_{market}_{interval} config from Group C")
                except Exception as e:
                    print(f"[Engine] Config error for {symbol}: {e}")
                    config = {}
            else:
                print(f"[Engine] Using default config for {symbol}")
            
            # Use Group C weights if available
            weights_z = config.get("weights_z", {"trend": 0.35, "volume": 0.15, "momentum": 0.30})
            thresholds_z = config.get("thresholds_z", {"strong_buy": 72, "buy": 58, "wait": 42, "sell": 38, "strong_sell": 22})
            
            # Run each indicator if available
            z01_result = {"signal": "WAIT", "trend": "FLAT", "score": 40, "reason": "Not available"}
            z02_result = {"score_mod": 0, "label": "N/A", "reason": "Not available"}  
            z03_result = {"signal": "WAIT", "score_mod": 0, "strength": "UNKNOWN", "reason": "Not available"}
            
            if z_modules_available:
                try:
                    z01_result = z_trend_score(symbol, interval, rows) if z_trend_score else z01_result
                except Exception as e:
                    z01_result = {"signal": "WAIT", "trend": "FLAT", "score": 40, "reason": f"Trend error: {str(e)[:20]}"}
                
                try:
                    z02_result = z_volume_score(symbol, interval, rows) if z_volume_score else z02_result
                except Exception as e:
                    z02_result = {"score_mod": 0, "label": "ERROR", "reason": f"Volume error: {str(e)[:20]}"}
                
                try:
                    z03_result = z_momentum_score(symbol, interval, rows) if z_momentum_score else z03_result
                except Exception as e:
                    z03_result = {"signal": "WAIT", "score_mod": 0, "strength": "ERROR", "reason": f"Momentum error: {str(e)[:20]}"}
            
            # Calculate weighted score using Group C config
            buy_points = 0
            sell_points = 0
            
            # Trend weight from Group C
            if z01_result.get("signal") == "BUY":
                buy_points += int(weights_z.get("trend", 0.35) * 100)
            elif z01_result.get("signal") == "SELL":
                sell_points += int(weights_z.get("trend", 0.35) * 100)
            
            # Volume weight from Group C
            vol_mod = z02_result.get("score_mod", 0)
            if vol_mod > 0:
                buy_points += min(vol_mod, int(weights_z.get("volume", 0.15) * 100))
            elif vol_mod < 0:
                sell_points += min(abs(vol_mod), int(weights_z.get("volume", 0.15) * 100))
            
            # Momentum weight from Group C
            mom_mod = z03_result.get("score_mod", 0)
            if mom_mod > 0:
                buy_points += min(mom_mod, int(weights_z.get("momentum", 0.30) * 100))
            elif mom_mod < 0:
                sell_points += min(abs(mom_mod), int(weights_z.get("momentum", 0.30) * 100))
            
            # Calculate final score using Group C thresholds
            total_weight = int(weights_z.get("trend", 0.35) * 100) + int(weights_z.get("volume", 0.15) * 100) + int(weights_z.get("momentum", 0.30) * 100)
            
            if total_weight > 0:
                if buy_points > sell_points:
                    score = int(50 + (buy_points / total_weight) * 45)
                    signal = "BUY" if score >= thresholds_z.get("buy", 58) else "WAIT"
                elif sell_points > buy_points:
                    score = int(50 - (sell_points / total_weight) * 45)
                    signal = "SELL" if score <= thresholds_z.get("sell", 38) else "WAIT"
                else:
                    score = 50
                    signal = "WAIT"
            else:
                score = 45
                signal = "WAIT"
            
            # Check strong signals from Group C
            if signal == "BUY" and score >= thresholds_z.get("strong_buy", 72):
                signal = "STRONG BUY"
            elif signal == "SELL" and score <= thresholds_z.get("strong_sell", 22):
                signal = "STRONG SELL"
            
            # Quick S/R position
            sr_pos = self._quick_sr_position(rows)
            
            # Build reason
            reasons = []
            if z01_result.get("signal") in ["BUY", "SELL"]:
                reasons.append(z01_result.get("reason", "Trend")[:20])
            if z02_result.get("label") not in ["N/A", "ERROR"]:
                reasons.append(z02_result.get("reason", "Volume")[:20])
            if z03_result.get("strength") not in ["UNKNOWN", "ERROR"]:
                reasons.append(z03_result.get("reason", "Momentum")[:20])
            
            reason = " + ".join(reasons[:3]) if reasons else "Mixed signals"
            
            return {
                "score": score,
                "signal": signal,
                "trend": z01_result.get("trend", "FLAT"),
                "sr_position": sr_pos,
                "reason": reason,
                "details": {
                    "trend": z01_result,
                    "volume": z02_result,
                    "momentum": z03_result
                }
            }
            
        except Exception as e:
            return {
                "score": 40,
                "signal": "WAIT",
                "trend": "FLAT",
                "sr_position": "—",
                "reason": f"Z-error: {str(e)[:30]}",
                "details": {}
            }
    
    def get_a_score(self, symbol: str, market: str, interval: str, rows: List, z_result: Dict) -> Dict[str, Any]:
        """
        A layer score - GO button pressed
        Returns: {a_score, a_signal, go, sl, tp, forecast, reason}
        """
        try:
            # Get pair-specific config from Group C
            config = {}
            if self.config_loader:
                try:
                    config = self.config_loader.get(symbol, market, interval)
                except Exception:
                    config = {}
            
            # Check if Z score is enough using Group C config
            min_z = config.get("min_z_for_a", 48) if config else 48
            if z_result.get("score", 0) < min_z:
                return {
                    "a_score": 0,
                    "a_signal": "BLOCK",
                    "go": False,
                    "reason": f"Z score {z_result.get('score',0)}% < min {min_z}%",
                    "sl": 0,
                    "tp": 0,
                    "forecast": {}
                }
            
            # Use Group C weights for A layer
            weights_a = config.get("weights_a", {"structure": 0.30, "indicators": 0.35, "sr": 0.20, "candle": 0.15})
            thresholds_a = config.get("thresholds_a", {"strong": 78, "go": 62, "caution": 52, "block": 48})
            
            # Run A indicators if available
            a01_result = {"signal": "WAIT", "score_mod": 0, "reason": "Not available"}
            a02_result = {"signal": "WAIT", "score_mod": 0, "reason": "Not available"}
            a03_result = {"signal": "WAIT", "score_mod": 0, "reason": "Not available"}
            a04_result = {"signal": "WAIT", "score_mod": 0, "reason": "Not available", "forecast": {}}
            
            if a_modules_available:
                try:
                    a01_result = a_structure_confirm(symbol, interval, rows) if a_structure_confirm else a01_result
                except Exception as e:
                    a01_result = {"signal": "WAIT", "score_mod": 0, "reason": f"Structure error: {str(e)[:20]}"}
                
                try:
                    a02_result = a_indicators_confirm(symbol, interval, rows) if a_indicators_confirm else a02_result
                except Exception as e:
                    a02_result = {"signal": "WAIT", "score_mod": 0, "reason": f"Indicators error: {str(e)[:20]}"}
                
                try:
                    a03_result = a_sr_confirm(symbol, interval, rows) if a_sr_confirm else a03_result
                except Exception as e:
                    a03_result = {"signal": "WAIT", "score_mod": 0, "reason": f"S/R error: {str(e)[:20]}"}
                
                try:
                    a04_result = a_candle_confirm(symbol, interval, rows) if a_candle_confirm else a04_result
                except Exception as e:
                    a04_result = {"signal": "WAIT", "score_mod": 0, "reason": f"Candle error: {str(e)[:20]}", "forecast": {}}
            
            # Calculate weighted score using Group C config
            buy_points = 0
            sell_points = 0
            
            # Structure weight from Group C
            if a01_result.get("signal") == "BUY":
                buy_points += int(weights_a.get("structure", 0.30) * 100)
            elif a01_result.get("signal") == "SELL":
                sell_points += int(weights_a.get("structure", 0.30) * 100)
            
            # Indicators weight from Group C
            if a02_result.get("signal") == "BUY":
                buy_points += int(weights_a.get("indicators", 0.35) * 100)
            elif a02_result.get("signal") == "SELL":
                sell_points += int(weights_a.get("indicators", 0.35) * 100)
            
            # S/R weight from Group C
            if a03_result.get("signal") == "BUY":
                buy_points += int(weights_a.get("sr", 0.20) * 100)
            elif a03_result.get("signal") == "SELL":
                sell_points += int(weights_a.get("sr", 0.20) * 100)
            
            # Candle weight from Group C
            if a04_result.get("signal") == "BUY":
                buy_points += int(weights_a.get("candle", 0.15) * 100)
            elif a04_result.get("signal") == "SELL":
                sell_points += int(weights_a.get("candle", 0.15) * 100)
            
            # Calculate final score
            total_weight = int(weights_a.get("structure", 0.30) * 100) + int(weights_a.get("indicators", 0.35) * 100) + int(weights_a.get("sr", 0.20) * 100) + int(weights_a.get("candle", 0.15) * 100)
            
            if total_weight > 0:
                if buy_points > sell_points:
                    a_score = int(50 + (buy_points / total_weight) * 45)
                    a_signal = "BUY" if a_score >= thresholds_a.get("go", 62) else "WAIT"
                elif sell_points > buy_points:
                    a_score = int(50 - (sell_points / total_weight) * 45)
                    a_signal = "SELL" if a_score <= (100 - thresholds_a.get("go", 62)) else "WAIT"
                else:
                    a_score = 50
                    a_signal = "WAIT"
            else:
                a_score = 45
                a_signal = "WAIT"
            
            # Check direction conflict with Z
            z_direction = "BUY" if "BUY" in z_result.get("signal", "") else "SELL" if "SELL" in z_result.get("signal", "") else None
            a_direction = a_signal
            
            if z_direction and a_direction and z_direction != a_direction and a_direction != "WAIT":
                a_score = int(a_score * 0.7)  # Penalty for conflict
                a_signal = "WAIT"
            
            # Final decision using Group C thresholds
            min_a = config.get("min_a_score", thresholds_a.get("go", 62)) if config else thresholds_a.get("go", 62)
            go_thresh = thresholds_a.get("go", 62)
            block_thresh = thresholds_a.get("block", 48)
            
            if a_score >= min_a and a_signal != "WAIT":
                go = a_score >= go_thresh
            else:
                go = False
                if a_score < block_thresh:
                    a_signal = "BLOCK"
            
            # Calculate SL/TP using Group C risk config
            sl, tp = self._calc_sl_tp(rows, a_signal, config.get("risk", {}) if config else {})
            
            # Get forecast from candle analysis
            forecast = a04_result.get("forecast", {})
            
            # Build reason
            reasons = []
            if a01_result.get("signal") == a_signal:
                reasons.append("Structure")
            if a02_result.get("signal") == a_signal:
                reasons.append("Indicators")
            if a03_result.get("signal") == a_signal:
                reasons.append("S/R")
            if a04_result.get("signal") == a_signal:
                reasons.append("Candle")
            
            reason = "+".join(reasons[:4]) if reasons else "Mixed signals"
            
            return {
                "a_score": a_score,
                "a_signal": a_signal,
                "go": go,
                "sl": sl,
                "tp": tp,
                "forecast": forecast,
                "reason": reason,
                "details": {
                    "structure": a01_result,
                    "indicators": a02_result,
                    "sr": a03_result,
                    "candle": a04_result
                }
            }
            
        except Exception as e:
            return {
                "a_score": 0,
                "a_signal": "BLOCK",
                "go": False,
                "reason": f"A-error: {str(e)[:30]}",
                "sl": 0,
                "tp": 0,
                "forecast": {},
                "details": {}
            }
    
    def _quick_sr_position(self, rows: List) -> str:
        """Fast S/R position from rows"""
        if len(rows) < 20:
            return "—"
        try:
            closes = [r["close"] for r in rows[-50:]]
            highs = [r["high"] for r in rows[-50:]]
            lows = [r["low"] for r in rows[-50:]]
            current = closes[-1]
            res = max(highs)
            sup = min(lows)
            rng = res - sup
            if rng <= 0:
                return "Mid"
            pos = (current - sup) / rng
            if pos < 0.25:
                return "Near Support"
            if pos > 0.75:
                return "Near Resistance"
            return "Mid Range"
        except:
            return "—"
    
    def _calc_sl_tp(self, rows: List, direction: str, risk_config: Dict) -> tuple:
        """Calculate SL/TP using ATR and Group C risk config"""
        if len(rows) < 15 or direction not in ["BUY", "SELL"]:
            return 0, 0
        
        try:
            # Calculate ATR
            trs = []
            for i in range(1, len(rows)):
                h = rows[i]["high"]
                l = rows[i]["low"]
                pc = rows[i-1]["close"]
                trs.append(max(h - l, abs(h - pc), abs(l - pc)))
            
            atr = sum(trs[-14:]) / 14 if len(trs) >= 14 else trs[-1] if trs else 0
            price = rows[-1]["close"]
            
            # Use Group C risk config or defaults
            sl_mult = risk_config.get("sl_atr", 2.0) if risk_config else 2.0
            tp_mult = risk_config.get("tp_atr", 3.5) if risk_config else 3.5
            
            if direction == "BUY":
                sl = round(price - (atr * sl_mult), 6)
                tp = round(price + (atr * tp_mult), 6)
            else:
                sl = round(price + (atr * sl_mult), 6)
                tp = round(price - (atr * tp_mult), 6)
            
            return sl, tp
        except:
            return 0, 0


# Global instance
_engine = None

def get_engine() -> TradingEngine:
    """Get singleton engine instance"""
    global _engine
    if _engine is None:
        _engine = TradingEngine()
    return _engine