# sys_data.py
# FOLDER: Groups/ (Android pe "Groups", local pe "main")
# NO cache warming — fetcher handles it automatically
# Routes: scan → go → detail

import os, sys, importlib.util, json, time

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Auto-detect folder name: "Groups" ya "main"
for _candidate in ["Groups", "groups", "main"]:
    _candidate_path = os.path.join(BASE_DIR, _candidate)
    if os.path.isdir(_candidate_path):
        GROUPS_DIR = _candidate_path
        break
else:
    GROUPS_DIR = os.path.join(BASE_DIR, "Groups")

if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

# Import engine for scoring
from engine import get_engine

from data_manager import get_rows, get_price, prefetch, start_ws, cache_info

_mods = {}
_current_scores = {}  # Real-time score storage
_last_scan_results = []  # Store last scan results for refresh

class SysData:

    def __init__(self):
        print("[SysData] Init...")
        self._load_all()
        self.engine = get_engine()
        print("[SysData] Ready")

    def warm_up(self, pairs):
        """No cache warming — fetcher auto-loads on first get_rows call"""
        prefetch(pairs, "5m")
        try:
            # WebSocket from data_sources (PRIMARY)
            start_ws()
            print("[SysData] WebSocket started (data_sources)")
        except Exception as e:
            print("[SysData] WS: "+str(e)[:40])

    # ── Layer 1: Scanner ──────────────────────────────
    def scan(self, market, pairs):
        """Get Z-scores for all pairs - REAL SCORES"""
        global _current_scores, _last_scan_results
        
        results = []
        for pair in pairs:
            try:
                sym = pair.upper().strip().replace("/", "").replace(" (OTC)", "")
                # Get data from data_manager (WebSocket primary)
                rows = get_rows(sym, "5m", 100)
                
                if rows and len(rows) >= 10:
                    # Use engine to calculate REAL score
                    result = self.engine.get_z_score(sym, market, "5m", rows)
                    result["pair"] = pair
                    results.append(result)
                else:
                    # No data - return WAIT with 40 score
                    results.append({
                        "pair": pair,
                        "symbol": sym,
                        "score": 40,
                        "signal": "WAIT",
                        "trend": "FLAT",
                        "sr_position": "—",
                        "reason": "No data"
                    })
            except Exception as e:
                results.append({
                    "pair": pair,
                    "symbol": pair.replace(" (OTC)", "").strip(),
                    "score": 40,
                    "signal": "WAIT",
                    "trend": "FLAT",
                    "sr_position": "—",
                    "reason": f"Error: {str(e)[:30]}"
                })
        
        # Sort by score descending
        results.sort(key=lambda x: x["score"], reverse=True)
        
        # Store REAL scores for 5-second refresh
        _current_scores = {r["pair"]: {
            "score": r["score"],
            "signal": r["signal"],
            "trend": r["trend"],
            "reason": r.get("reason", ""),
            "sr_position": r.get("sr_position", "—")
        } for r in results}
        
        # Store full results for re-scan if needed
        _last_scan_results = results
        
        print(f"[SysData] Scan complete: {len(results)} pairs, scores updated")
        return results

    # ── Layer 2: GO pressed ───────────────────────────
    def go(self, symbol, market):
        """Get A-layer analysis when GO pressed - REAL ANALYSIS"""
        try:
            sym = symbol.upper().strip().replace("/", "")
            
            # Get data from data_manager (WebSocket primary)
            rows = get_rows(sym, "5m", 100)
            
            if not rows or len(rows) < 10:
                return "<div class='err-msg'>Not enough data for analysis</div>"
            
            # Get Z result first (needed for A-layer)
            z_result = self.engine.get_z_score(sym, market, "5m", rows)
            
            # Get A result - REAL calculation
            result = self.engine.get_a_score(sym, market, "5m", rows, z_result)
            
            # Format as HTML table
            return self._format_a_result(result)
            
        except Exception as e:
            return f"<div class='err-msg'>GO analysis error: {str(e)[:80]}</div>"

    # ── Layer 3: Continue pressed ─────────────────────
    def detail(self, symbol, market):
        """Get D-layer analysis when Continue pressed"""
        try:
            sym = symbol.upper().strip().replace("/", "")
            
            # Load D10 judge module
            d10 = _mods.get("d10")
            if not d10:
                return "<div class='err-msg'>D-layer not loaded - check Groups</div>"
            
            # Get F01 forecast module
            f01 = _mods.get("f01")
            
            html = ""
            
            # Run D10 analysis
            try:
                html += d10.get_table(sym, market, "5m")
            except Exception as e:
                html += f"<div class='err-msg'>D-analysis error: {str(e)[:80]}</div>"
            
            # Add forecast if available
            if f01 and hasattr(f01, "get_table"):
                try:
                    html += ("<div class='section'>"
                            "<div class='section-title'>Forecast</div>"
                            +f01.get_table(sym, "5m")+"</div>")
                except Exception:
                    pass
            
            return html if html else "<div class='err-msg'>No detail analysis available</div>"
            
        except Exception as e:
            return f"<div class='err-msg'>Detail error: {str(e)[:80]}</div>"

    # ── Module loader ─────────────────────────────────
    def _load_all(self):
        """Load all required modules"""
        paths = {
            "d10": os.path.join(GROUPS_DIR, "group_d", "D10_judge.py"),
            "f01": os.path.join(GROUPS_DIR, "group_f", "F01_next_candle.py"),
        }
        for key, path in paths.items():
            _mods[key] = _import(path, key)
            status = "OK" if _mods[key] else "missing"
            print("[SysData] "+key+": "+status)

    def get_current_scores(self):
        """Get current REAL scores for 5-second refresh"""
        global _current_scores
        
        # If no scores yet, return empty (frontend will handle)
        if not _current_scores:
            print("[SysData] Warning: No scores available - run SCAN first")
            return {}
        
        # Update scores with latest WebSocket prices if possible
        updated_scores = {}
        for pair, data in _current_scores.items():
            try:
                # Get latest price from WebSocket
                sym = pair.upper().strip().replace("/", "").replace(" (OTC)", "")
                live_price = get_price(sym)
                
                if live_price > 0:
                    # Calculate quick price change
                    # Note: Full recalculation would need rows, this is quick update
                    updated_scores[pair] = {
                        "score": data["score"],  # Keep original score
                        "signal": data["signal"],
                        "trend": data["trend"],
                        "live_price": live_price  # Add live price for frontend
                    }
                else:
                    updated_scores[pair] = data
            except Exception as e:
                # Keep original score if update fails
                updated_scores[pair] = data
        
        return updated_scores

    def refresh_scores(self, market, pairs):
        """Force refresh all scores - REAL recalculation"""
        print(f"[SysData] Force refreshing {len(pairs)} scores...")
        return self.scan(market, pairs)

    def _format_a_result(self, result):
        """Format A-result as HTML table"""
        a_score = result.get("a_score", 0)
        a_signal = result.get("a_signal", "WAIT")
        reason = result.get("reason", "")
        sl = result.get("sl", 0)
        tp = result.get("tp", 0)
        forecast = result.get("forecast", {})
        
        colors = {"BUY":"#44cc88","SELL":"#ff8866","WAIT":"#ffcc44",
                  "BLOCK":"#ff6666","STRONG BUY":"#00ff88","STRONG SELL":"#ff4444"}
        color = colors.get(a_signal, "#ffcc44")
        
        html = (
            "<div style='background:rgba(0,0,0,0.22);border:1px solid "
            +color+"44;border-radius:8px;padding:11px 15px;margin-bottom:12px;'>"
            "<span style='color:"+color+";font-size:15px;font-weight:bold;'>"
            +a_signal+" &nbsp;"+str(a_score)+"%</span>"
            "<span style='color:#666;font-size:10px;margin-left:12px;'>"
            "Reason: "+reason+"</span>"
            "<br><span style='color:#555;font-size:10px;'>"
            "SL: "+str(round(sl,6))+" &nbsp;|&nbsp; TP: "+str(round(tp,6))
            +"</span></div>"
        )
        
        # Add forecast if available
        if forecast:
            up = forecast.get("up", 50)
            down = forecast.get("down", 50)
            flat = forecast.get("flat", 0)
            html += (
                "<div style='padding:8px 12px;background:rgba(255,255,255,0.02);"
                "border-radius:6px;border:1px solid rgba(255,255,255,0.06);"
                "margin-bottom:12px;font-size:11px;'>"
                "<span style='color:#888;margin-right:10px;'>Next candle:</span>"
                "<span style='color:#44cc88;'>UP "+str(up)+"%</span>"
                " &nbsp; <span style='color:#ff8866;'>DOWN "+str(down)+"%</span>"
                " &nbsp; <span style='color:#888;'>FLAT "+str(flat)+"%</span>"
                "</div>"
            )
        
        return html


def _import(path, name):
    if not os.path.exists(path):
        return None
    try:
        spec = importlib.util.spec_from_file_location(name, path)
        mod  = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        return mod
    except Exception as e:
        print("[SysData] Import error "+name+": "+str(e)[:50])
        return None