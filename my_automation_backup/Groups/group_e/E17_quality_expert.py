"""
E17_quality_expert.py
Meta-expert jo saare E modules ke outputs ka quality analysis karta hai aur sirf high-probability signals (≥70% estimated win rate) leta hai.

Input: List of outputs from E01..E16 (each output is a dict with keys: direction, probability, scenario_type, confidence_metrics)
Output: Filtered signals with quality score, win_rate_estimate, and final_action
"""

import json
import math
from datetime import datetime
from typing import Dict, List, Optional, Tuple

class QualityExpert:
    """Quality filtering expert for all E-module signals"""
    
    def __init__(self):
        self.module_weights = {
            # Higher weight for historically more reliable experts
            'E01_candles': 0.08,
            'E02_cvd': 0.10,
            'E03_depth': 0.09,
            'E04_derivative': 0.12,   # OI+funding very reliable in trends
            'E05_correlation': 0.06,
            'E06_macro': 0.05,
            'E07_liquidation': 0.10,  # Liquidation cascades are strong
            'E08_sessions': 0.05,
            'E09_sentiment': 0.07,
            'E10_volProfile': 0.08,
            'E11_mstructure': 0.09,
            'E12_onchain': 0.07,
            'E13_tick': 0.10,
            'E14_regime': 0.00,   # Regime is context only, not directional
            'E15_indicators': 0.06,
            'E16_manipulation': 0.00, # Used as penalty, not direct signal
        }
        
        # Minimum win rate threshold (70%)
        self.min_win_rate = 0.70
        
        # Consistency threshold: how many modules must agree
        self.min_agreement_ratio = 0.35   # 35% of non-neutral modules
    
    def evaluate_signal(self, all_expert_outputs: List[Dict]) -> Dict:
        """
        Evaluate all E-module outputs and return quality-filtered final signal.
        
        all_expert_outputs: list of dicts, each containing:
            - module_name: str (e.g., "E01_candles")
            - direction: "UP", "DOWN", or "NEUTRAL"
            - probability: float (0-1)
            - scenario_type: str (optional)
            - confidence_metrics: dict (optional, e.g., {'z_score': 2.1})
        """
        # 1. Separate direction signals from context modules
        directional_signals = []
        regime_info = None
        manipulation_score = 1.0  # 1 = no manipulation detected (lower is worse)
        
        for out in all_expert_outputs:
            mod_name = out.get('module_name', 'unknown')
            direction = out.get('direction', 'NEUTRAL')
            prob = out.get('probability', 0.5)
            
            if mod_name == 'E14_regime':
                regime_info = out.get('scenario_type', 'unknown')
                continue
            if mod_name == 'E16_manipulation':
                # manipulation output: probability_score (0-1), where 1 = likely manipulation
                manip_prob = out.get('probability', 0.0)
                manipulation_score = 1.0 - manip_prob  # invert: 1 = clean, 0 = heavy manip
                continue
            
            if direction in ['UP', 'DOWN'] and prob >= 0.5:
                directional_signals.append({
                    'name': mod_name,
                    'direction': direction,
                    'prob': prob,
                    'weight': self.module_weights.get(mod_name, 0.05),
                    'raw': out
                })
        
        if not directional_signals:
            return self._neutral_output("No directional signals from any E-module")
        
        # 2. Weighted direction vote
        up_weight = 0.0
        down_weight = 0.0
        total_weight = 0.0
        for sig in directional_signals:
            w = sig['weight'] * sig['prob']  # weight * confidence
            total_weight += w
            if sig['direction'] == 'UP':
                up_weight += w
            else:
                down_weight += w
        
        if total_weight == 0:
            return self._neutral_output("Zero total weight in signals")
        
        raw_direction = 'UP' if up_weight > down_weight else 'DOWN'
        direction_strength = max(up_weight, down_weight) / total_weight  # 0.5 to 1.0
        
        # 3. Agreement ratio
        up_count = sum(1 for s in directional_signals if s['direction'] == 'UP')
        down_count = len(directional_signals) - up_count
        agreement_ratio = max(up_count, down_count) / len(directional_signals)
        
        # 4. Average probability from majority side
        majority_signals = [s for s in directional_signals if s['direction'] == raw_direction]
        avg_prob_majority = sum(s['prob'] for s in majority_signals) / len(majority_signals) if majority_signals else 0.5
        
        # 5. Manipulation penalty
        manip_penalty = 1.0 if manipulation_score > 0.7 else (manipulation_score / 0.7)  # if <0.7, reduce score
        
        # 6. Regime context bonus/penalty
        regime_bonus = 1.0
        if regime_info:
            # Example: in strong trend, trend-following signals get bonus; mean-reversion gets penalty
            if regime_info in ['bull', 'bear', 'trending']:
                regime_bonus = 1.1
            elif regime_info in ['range', 'choppy']:
                regime_bonus = 0.9
        
        # 7. Final estimated win rate (core formula)
        win_rate_estimate = (
            direction_strength * 0.35 +
            agreement_ratio * 0.25 +
            avg_prob_majority * 0.25 +
            manip_penalty * 0.10 +
            regime_bonus * 0.05
        )
        
        # Clamp between 0 and 1
        win_rate_estimate = max(0.0, min(1.0, win_rate_estimate))
        
        # 8. Also compute a signal quality score (0-100)
        quality_score = win_rate_estimate * 100
        
        # 9. Apply threshold
        if win_rate_estimate >= self.min_win_rate and agreement_ratio >= self.min_agreement_ratio:
            final_action = "TAKE_SIGNAL"
            confidence = win_rate_estimate
        else:
            final_action = "FILTER_OUT"
            confidence = win_rate_estimate
            raw_direction = "NEUTRAL"
        
        return {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "module": "E17_quality_expert",
            "final_action": final_action,
            "direction": raw_direction if final_action == "TAKE_SIGNAL" else "NEUTRAL",
            "estimated_win_rate": round(win_rate_estimate, 4),
            "quality_score": round(quality_score, 2),
            "metrics": {
                "agreement_ratio": round(agreement_ratio, 4),
                "direction_strength": round(direction_strength, 4),
                "avg_majority_probability": round(avg_prob_majority, 4),
                "manipulation_clean_score": round(manipulation_score, 4),
                "regime": regime_info,
                "total_signals_considered": len(directional_signals),
                "up_votes": up_count,
                "down_votes": down_count
            },
            "filter_reason": None if final_action == "TAKE_SIGNAL" else f"Win rate {win_rate_estimate:.1%} < {self.min_win_rate:.0%} or agreement {agreement_ratio:.1%} < {self.min_agreement_ratio:.0%}"
        }
    
    def _neutral_output(self, reason: str) -> Dict:
        return {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "module": "E17_quality_expert",
            "final_action": "NO_SIGNAL",
            "direction": "NEUTRAL",
            "estimated_win_rate": 0.0,
            "quality_score": 0.0,
            "metrics": {},
            "filter_reason": reason
        }

# ------------------- Example usage in Flask / QPython -------------------
if __name__ == "__main__":
    # Simulated outputs from all E-modules
    mock_outputs = [
        {"module_name": "E01_candles", "direction": "UP", "probability": 0.85},
        {"module_name": "E02_cvd", "direction": "UP", "probability": 0.92},
        {"module_name": "E03_depth", "direction": "DOWN", "probability": 0.60},
        {"module_name": "E04_derivative", "direction": "UP", "probability": 0.88},
        {"module_name": "E14_regime", "direction": "NEUTRAL", "scenario_type": "bull", "probability": 1.0},
        {"module_name": "E16_manipulation", "direction": "NEUTRAL", "probability": 0.25},  # 25% chance of manipulation
        # ... other modules assumed neutral or missing
    ]
    
    expert = QualityExpert()
    result = expert.evaluate_signal(mock_outputs)
    print(json.dumps(result, indent=2))