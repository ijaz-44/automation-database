# engine.py
import os
import sys
from typing import Dict, Any, List
from collections import deque

print("[engine] Loading...")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
GROUPS_DIR = os.path.join(BASE_DIR, "Groups")

for p in [BASE_DIR,
          os.path.join(GROUPS_DIR, "group_z"),
          os.path.join(GROUPS_DIR, "group_a"),
          os.path.join(GROUPS_DIR, "group_d")]:
    if p not in sys.path:
        sys.path.insert(0, p)

# Group Z
try:
    from Z01_trend    import score as z01_score
    from Z02_volume   import score as z02_score
    from Z03_momentum import score as z03_score
    _z_ok = True
    print("✅ [engine] Z modules loaded")
except Exception as e:
    _z_ok = False
    print(f"❌ [engine] Z modules: {e}")

# Group A
try:
    from A01_structure  import confirm as a01_confirm
    from A02_indicators import confirm as a02_confirm
    from A03_sr         import confirm as a03_confirm
    from A04_candle     import confirm as a04_confirm
    _a_ok = True
    print("✅ [engine] A modules loaded")
except Exception as e:
    _a_ok = False
    print(f"❌ [engine] A modules: {e}")

# Group D (optional)
try:
    sys.path.insert(0, os.path.join(GROUPS_DIR, "group_d"))
    from D01_deep_volume    import analyze as d01_analyze
    from D02_deep_structure import analyze as d02_analyze
    from D03_deep_indicators import analyze as d03_analyze
    _d_ok = True
    print("✅ [engine] D modules loaded")
except Exception as e:
    _d_ok = False
    print(f"❌ [engine] D modules: {e}")

_signal_history: Dict[str, deque] = {}
FLICKER_WINDOW = 3

# ===================== DEFAULT CONFIGURATIONS (No JSON files) =====================
def _get_default_config(market: str, interval: str) -> Dict[str, Any]:
    """Return default configuration based on market and interval."""
    # Base config (works for most markets)
    base = {
        "weights_z": {"trend": 0.35, "volume": 0.20, "momentum": 0.45},
        "weights_a": {"structure": 0.30, "indicators": 0.30, "sr": 0.20, "candle": 0.20},
        "thresholds_z": {"buy": 65, "sell": 35, "strong_buy": 80, "strong_sell": 20},
        "thresholds_a": {"go": 70, "strong_buy": 85, "strong_sell": 15},
        "min_z_for_a": 55,
        "min_a_score": 65,
        "risk": {"sl_atr_mult": 1.5, "tp_atr_mult": 2.0},
        "deep": {"min_deep_score": 60, "require_deep_confirm": False},
        "manipulation": {"enabled": False, "penalty": -20, "min_wick_ratio": 0.6, "volume_spike_threshold": 2.5}
    }
    
    # Adjust for Binary OTC (more conservative, requires higher confidence)
    if market == "Binary OTC":
        base["thresholds_z"]["buy"] = 72
        base["thresholds_z"]["sell"] = 28
        base["thresholds_z"]["strong_buy"] = 85
        base["thresholds_z"]["strong_sell"] = 15
        base["thresholds_a"]["go"] = 75
        base["thresholds_a"]["strong_buy"] = 88
        base["thresholds_a"]["strong_sell"] = 12
        base["min_z_for_a"] = 60
        base["min_a_score"] = 70
        base["deep"]["min_deep_score"] = 65
        base["deep"]["require_deep_confirm"] = True
        base["manipulation"]["enabled"] = True
    
    # Adjust for longer timeframes (more weight on trend, less on momentum)
    if interval in ["15m", "30m", "1h", "4h"]:
        base["weights_z"]["trend"] = 0.45
        base["weights_z"]["momentum"] = 0.30
        base["weights_a"]["structure"] = 0.40
        base["weights_a"]["candle"] = 0.10
    
    # Adjust for very short timeframes (more weight on momentum)
    if interval == "1m":
        base["weights_z"]["momentum"] = 0.60
        base["weights_z"]["trend"] = 0.20
        base["weights_a"]["candle"] = 0.35
        base["weights_a"]["indicators"] = 0.25
    
    return base


class TradingEngine:
    def __init__(self):
        # No config loader – using defaults
        print("✅ [engine] Ready (using default configs, no JSON files)")

    def get_z_score(self, symbol: str, market: str, interval: str, rows: List) -> Dict[str, Any]:
        try:
            if not rows or len(rows) < 20:
                return self._na("Insufficient data")
            
            cfg = _get_default_config(market, interval)
            weights_z = cfg.get("weights_z", {})
            thresholds_z = cfg.get("thresholds_z", {})
            manip_cfg = cfg.get("manipulation", {})
            
            if not weights_z or not thresholds_z:
                return self._na("Missing weights/thresholds")
            
            regime = self._detect_regime(rows)
            dw = self._dynamic_weights(weights_z, regime)
            
            z01 = self._run_safe(z01_score, _z_ok, symbol, interval, rows,
                                 default={"signal":"WAIT","trend":"FLAT","score":40,"reason":"Z01 N/A"})
            z02 = self._run_safe(z02_score, _z_ok, symbol, interval, rows,
                                 default={"score_mod":0,"label":"N/A","reason":"Z02 N/A"})
            z03 = self._run_safe(z03_score, _z_ok, symbol, interval, rows,
                                 default={"signal":"WAIT","score_mod":0,"strength":"UNKNOWN","reason":"Z03 N/A"})
            
            buy_pts = sell_pts = 0
            tw = sum(int(v*100) for v in dw.values()) or 1
            
            if z01.get("signal") == "BUY":
                buy_pts += int(dw.get("trend",0)*100)
            elif z01.get("signal") == "SELL":
                sell_pts += int(dw.get("trend",0)*100)
            
            vm = z02.get("score_mod", 0)
            if vm > 0:
                buy_pts += min(vm, int(dw.get("volume",0)*100))
            elif vm < 0:
                sell_pts += min(-vm, int(dw.get("volume",0)*100))
            
            mm = z03.get("score_mod", 0)
            if mm > 0:
                buy_pts += min(mm, int(dw.get("momentum",0)*100))
            elif mm < 0:
                sell_pts += min(-mm, int(dw.get("momentum",0)*100))
            
            if buy_pts > sell_pts:
                score = int(50 + (buy_pts / tw) * 45)
                raw_sig = "BUY"
            elif sell_pts > buy_pts:
                score = int(50 - (sell_pts / tw) * 45)
                raw_sig = "SELL"
            else:
                score = 50
                raw_sig = "WAIT"
            
            score = max(0, min(100, score))
            buy_thr   = thresholds_z.get("buy",   65)
            sell_thr  = thresholds_z.get("sell",  35)
            sbuy_thr  = thresholds_z.get("strong_buy",  80)
            ssell_thr = thresholds_z.get("strong_sell", 15)
            
            if raw_sig == "BUY":
                signal = "STRONG BUY" if score >= sbuy_thr else "BUY" if score >= buy_thr else "WAIT"
            elif raw_sig == "SELL":
                signal = "STRONG SELL" if score <= ssell_thr else "SELL" if score <= sell_thr else "WAIT"
            else:
                signal = "WAIT"
            
            manip_penalty = 0
            manip_note = ""
            if manip_cfg.get("enabled") and signal != "WAIT":
                pen, note = self._check_manipulation(rows, manip_cfg)
                manip_penalty = pen
                manip_note = note
                score = max(0, score + pen)
                if pen < 0:
                    signal = "WAIT"
            
            stable_signal = self._stabilise(symbol, signal)
            quality = self._quality(score, stable_signal, regime, z01, z02, z03)
            sr_pos = self._sr_position(rows)
            
            reasons = [r for r in [
                z01.get("reason","")[:20] if z01.get("signal") != "WAIT" else "",
                z02.get("reason","")[:15] if z02.get("label") not in ["N/A","ERROR"] else "",
                z03.get("reason","")[:15] if z03.get("strength") not in ["UNKNOWN","ERROR"] else "",
                manip_note[:20] if manip_note else "",
            ] if r]
            reason = " + ".join(reasons) or "Mixed signals"
            
            return {
                "score": score,
                "signal": stable_signal,
                "trend": z01.get("trend", "FLAT"),
                "sr_position": sr_pos,
                "regime": regime,
                "quality": quality,
                "reason": reason,
                "details": {"trend": z01, "volume": z02, "momentum": z03}
            }
        except Exception as e:
            return self._na(f"Z-error: {str(e)[:40]}")

    def get_a_score(self, symbol: str, market: str, interval: str, rows: List, z_result: Dict) -> Dict[str, Any]:
        try:
            if not rows or len(rows) < 20:
                return self._na_a("Insufficient data")
            
            cfg = _get_default_config(market, interval)
            weights_a = cfg.get("weights_a", {})
            thresholds_a = cfg.get("thresholds_a", {})
            min_z = cfg.get("min_z_for_a", cfg.get("min_a_score", 55))
            deep_cfg = cfg.get("deep", {})
            
            z_score = z_result.get("score", 0)
            if not isinstance(z_score, (int, float)) or z_score < min_z:
                return {**self._na_a(f"Z={z_score} < min {min_z}"), "requires_deep": False}
            
            a01 = self._run_safe(a01_confirm, _a_ok, symbol, interval, rows,
                                 default={"signal":"WAIT","score_mod":0,"reason":"A01 N/A"})
            a02 = self._run_safe(a02_confirm, _a_ok, symbol, interval, rows,
                                 default={"signal":"WAIT","score_mod":0,"reason":"A02 N/A"})
            a03 = self._run_safe(a03_confirm, _a_ok, symbol, interval, rows,
                                 default={"signal":"WAIT","score_mod":0,"reason":"A03 N/A"})
            a04 = self._run_safe(a04_confirm, _a_ok, symbol, interval, rows,
                                 default={"signal":"WAIT","score_mod":0,"reason":"A04 N/A","forecast":{}})
            
            buy_pts = sell_pts = 0
            tw = sum(int(weights_a.get(k,0)*100) for k in ["structure","indicators","sr","candle"]) or 1
            
            for key, mod, w in [
                ("structure", a01, weights_a.get("structure",0)),
                ("indicators", a02, weights_a.get("indicators",0)),
                ("sr", a03, weights_a.get("sr",0)),
                ("candle", a04, weights_a.get("candle",0)),
            ]:
                if mod.get("signal") == "BUY":
                    buy_pts += int(w*100)
                elif mod.get("signal") == "SELL":
                    sell_pts += int(w*100)
            
            if buy_pts > sell_pts:
                a_score = int(50 + (buy_pts / tw) * 45)
                a_signal = "BUY"
            elif sell_pts > buy_pts:
                a_score = int(50 - (sell_pts / tw) * 45)
                a_signal = "SELL"
            else:
                a_score = 50
                a_signal = "WAIT"
            
            a_score = max(0, min(100, a_score))
            z_dir = ("BUY" if "BUY" in z_result.get("signal","") else "SELL" if "SELL" in z_result.get("signal","") else None)
            if z_dir and a_signal not in ["WAIT"] and z_dir != a_signal:
                a_score = int(a_score * 0.65)
                a_signal = "WAIT"
            
            go_thr   = thresholds_a.get("go",   70)
            min_a    = cfg.get("min_a_score", go_thr)
            sbuy_thr = thresholds_a.get("strong_buy",  85)
            ssell    = thresholds_a.get("strong_sell",  15)
            
            if a_signal == "BUY":
                final_sig = "STRONG BUY" if a_score >= sbuy_thr else "BUY" if a_score >= go_thr else "WAIT"
            elif a_signal == "SELL":
                final_sig = "STRONG SELL" if a_score <= ssell else "SELL" if a_score <= (100-go_thr) else "WAIT"
            else:
                final_sig = "WAIT"
            
            go = (a_score >= min_a and final_sig not in ["WAIT"])
            require_deep = deep_cfg.get("require_deep_confirm", False)
            min_deep     = deep_cfg.get("min_deep_score", 60)
            sl, tp = self._calc_sl_tp(rows, a_signal, cfg.get("risk", {}))
            
            reasons = [k for k, m in [
                ("Structure", a01), ("Indicators", a02),
                ("S/R", a03),       ("Candle", a04)
            ] if m.get("signal") == a_signal]
            reason = "+".join(reasons) or "Mixed"
            
            return {
                "a_score": a_score,
                "a_signal": final_sig,
                "go": go,
                "sl": sl,
                "tp": tp,
                "forecast": a04.get("forecast", {}),
                "reason": reason,
                "requires_deep": require_deep,
                "min_deep_score": min_deep,
                "details": {"structure": a01, "indicators": a02, "sr": a03, "candle": a04}
            }
        except Exception as e:
            return self._na_a(f"A-error: {str(e)[:40]}")

    def get_d_score(self, symbol: str, market: str, interval: str, rows: List, a_result: Dict) -> Dict[str, Any]:
        try:
            if not rows or len(rows) < 20:
                return {"d_score":"NA","d_signal":"NA","confirmed":False,"reason":"Insufficient data"}
            if not _d_ok:
                return {"d_score":"NA","d_signal":"NA","confirmed":False,"reason":"D modules not loaded"}
            
            cfg = _get_default_config(market, interval)
            deep_cfg = cfg.get("deep", {})
            min_deep = a_result.get("min_deep_score", deep_cfg.get("min_deep_score", 60))
            
            # Note: D modules expect rows parameter, but they also try to fetch data via fetcher.
            # To avoid that, we pass rows as a keyword argument if they support it.
            # For now, we'll just call with rows argument (they should accept it)
            d01 = self._run_safe(d01_analyze, True, symbol, interval, rows,
                                 default={"signal":"WAIT","score":50,"reason":"D01 N/A"})
            d02 = self._run_safe(d02_analyze, True, symbol, interval, rows,
                                 default={"signal":"WAIT","score":50,"reason":"D02 N/A"})
            d03 = self._run_safe(d03_analyze, True, symbol, interval, rows,
                                 default={"signal":"WAIT","score":50,"reason":"D03 N/A"})
            
            scores = [d01.get("score",50), d02.get("score",50), d03.get("score",50)]
            d_score = int(sum(scores) / len(scores))
            sigs = [d01.get("signal","WAIT"), d02.get("signal","WAIT"), d03.get("signal","WAIT")]
            buys = sigs.count("BUY")
            sells = sigs.count("SELL")
            d_signal = "BUY" if buys >= 2 else "SELL" if sells >= 2 else "WAIT"
            
            a_signal = a_result.get("a_signal", "WAIT")
            a_dir = "BUY" if "BUY" in a_signal else "SELL" if "SELL" in a_signal else None
            confirmed = (d_score >= min_deep and d_signal != "WAIT" and d_signal == a_dir)
            
            return {
                "d_score": d_score,
                "d_signal": d_signal,
                "confirmed": confirmed,
                "reason": f"Vol:{d01.get('reason','')[:12]} Str:{d02.get('reason','')[:12]} Ind:{d03.get('reason','')[:12]}",
                "details": {"d01": d01, "d02": d02, "d03": d03}
            }
        except Exception as e:
            return {"d_score":"NA","d_signal":"NA","confirmed":False,"reason": f"D-error: {str(e)[:40]}"}

    # ===================== Helper methods (unchanged) =====================
    @staticmethod
    def _run_safe(fn, available, *args, default=None):
        if not available:
            return default or {}
        try:
            # If the function expects rows as keyword argument, handle it
            # For D modules, they might have signature analyze(symbol, interval, rows=...)
            # We'll try positional first, then keyword
            try:
                return fn(*args) or default or {}
            except TypeError:
                # Try with rows keyword if the last arg is rows
                if len(args) >= 3 and isinstance(args[-1], list):
                    return fn(args[0], args[1], rows=args[-1]) or default or {}
                raise
        except Exception as e:
            print(f"❌ [engine] Module error: {e}")
            return default or {}

    @staticmethod
    def _candles_valid(rows):
        if not rows:
            return False
        for i in range(1, len(rows)):
            prev = rows[i-1].get("close", 1)
            curr = rows[i].get("close", 1)
            if prev > 0 and abs(curr - prev) / prev > 0.20:
                return False
        return True

    @staticmethod
    def _detect_regime(rows):
        if len(rows) < 20:
            return "ranging"
        closes = [r["close"] for r in rows[-20:]]
        highs = [r["high"] for r in rows[-20:]]
        lows = [r["low"] for r in rows[-20:]]
        rng = max(highs) - min(lows)
        move = abs(closes[-1] - closes[0])
        vol_pct = (rng / closes[0] * 100) if closes[0] else 0
        if vol_pct > 3.0:
            return "volatile"
        if move > rng * 0.6:
            return "trending"
        return "ranging"

    @staticmethod
    def _dynamic_weights(base_weights, regime):
        w = dict(base_weights)
        if regime == "trending":
            w["trend"] = min(1.0, w.get("trend",0.30) * 1.30)
            w["momentum"] = w.get("momentum",0.30) * 0.85
        elif regime == "ranging":
            w["momentum"] = min(1.0, w.get("momentum",0.30) * 1.20)
            w["trend"] = w.get("trend",0.30) * 0.80
        elif regime == "volatile":
            w["momentum"] = min(1.0, w.get("momentum",0.30) * 1.40)
            w["trend"] = w.get("trend",0.30) * 0.70
            w["volume"] = w.get("volume",0.20) * 0.80
        total = sum(w.values()) or 1
        return {k: round(v/total,4) for k,v in w.items()}

    @staticmethod
    def _check_manipulation(rows, manip_cfg):
        if len(rows) < 3:
            return 0, ""
        last = rows[-1]
        body = abs(last["close"] - last["open"])
        wick = (last["high"] - last["low"]) - body
        wr = wick / (last["high"] - last["low"] + 1e-9)
        min_wick = manip_cfg.get("min_wick_ratio", 0.6)
        penalty = manip_cfg.get("penalty", -20)
        if wr >= min_wick:
            return penalty, f"High wick ratio {round(wr,2)}"
        vols = [r.get("volume",0) for r in rows[-10:-1]]
        avg_vol = (sum(vols) / len(vols)) if vols else 1
        last_vol = last.get("volume",0)
        spike_thr = manip_cfg.get("volume_spike_threshold", 2.5)
        if avg_vol > 0 and last_vol > avg_vol * spike_thr:
            return penalty, f"Volume spike x{round(last_vol/avg_vol,1)}"
        return 0, ""

    @staticmethod
    def _stabilise(symbol, signal):
        hist = _signal_history.setdefault(symbol, deque(maxlen=FLICKER_WINDOW))
        hist.append(signal)
        if len(hist) < FLICKER_WINDOW:
            return "WAIT"
        if all(s == signal for s in hist):
            return signal
        buys = list(hist).count("BUY") + list(hist).count("STRONG BUY")
        sells = list(hist).count("SELL") + list(hist).count("STRONG SELL")
        if buys > sells and buys >= 2:
            return "BUY"
        if sells > buys and sells >= 2:
            return "SELL"
        return "WAIT"

    @staticmethod
    def _quality(score, signal, regime, z01, z02, z03):
        if signal == "WAIT":
            return "LOW"
        pts = 0
        if score >= 75:
            pts += 2
        elif score >= 60:
            pts += 1
        if regime == "trending":
            pts += 1
        if z01.get("signal") not in ["WAIT","ERROR"]:
            pts += 1
        if z02.get("label") not in ["N/A","ERROR"]:
            pts += 1
        if z03.get("strength") not in ["UNKNOWN","ERROR"]:
            pts += 1
        if pts >= 5:
            return "HIGH"
        if pts >= 3:
            return "MED"
        return "LOW"

    @staticmethod
    def _sr_position(rows):
        if len(rows) < 20:
            return "—"
        try:
            closes = [r["close"] for r in rows[-50:]]
            highs = [r["high"] for r in rows[-50:]]
            lows = [r["low"] for r in rows[-50:]]
            cur = closes[-1]
            res = max(highs)
            sup = min(lows)
            rng = res - sup
            if rng <= 0:
                return "Mid"
            pos = (cur - sup) / rng
            if pos < 0.25:
                return "Near Support"
            if pos > 0.75:
                return "Near Resistance"
            return "Mid Range"
        except Exception:
            return "—"

    @staticmethod
    def _calc_sl_tp(rows, direction, risk_cfg):
        if len(rows) < 15 or direction not in ["BUY","SELL"]:
            return 0, 0
        try:
            trs = [max(rows[i]["high"] - rows[i]["low"],
                       abs(rows[i]["high"] - rows[i-1]["close"]),
                       abs(rows[i]["low"] - rows[i-1]["close"]))
                   for i in range(1, len(rows))]
            atr = sum(trs[-14:]) / 14 if len(trs) >= 14 else trs[-1]
            price = rows[-1]["close"]
            sl_m = risk_cfg.get("sl_atr_mult", risk_cfg.get("sl_atr", 1.5))
            tp_m = risk_cfg.get("tp_atr_mult", risk_cfg.get("tp_atr", 2.0))
            if direction == "BUY":
                return round(price - atr*sl_m, 6), round(price + atr*tp_m, 6)
            else:
                return round(price + atr*sl_m, 6), round(price - atr*tp_m, 6)
        except Exception:
            return 0, 0

    @staticmethod
    def _na(reason):
        return {"score":"NA","signal":"WAIT","trend":"FLAT","sr_position":"—","regime":"unknown","quality":"LOW","reason":reason,"details":{}}

    @staticmethod
    def _na_a(reason):
        return {"a_score":"NA","a_signal":"WAIT","go":False,"sl":0,"tp":0,"forecast":{},"reason":reason,"requires_deep":False,"details":{}}


_engine = None

def get_engine():
    global _engine
    if _engine is None:
        _engine = TradingEngine()
    return _engine

print("✅ [engine] Loaded OK")