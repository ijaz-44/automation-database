# Z02_volume.py — Layer 1: Volume analysis
# NO API CALLS — sirf rows se kaam karta hai

def score(symbol, interval="15m", rows=None):
    """
    Returns volume signal and score modifier.
    rows: already fetched data (no API call)
    """
    try:
        if not rows or len(rows) < 5:
            return _m(0, "N/A", "Not enough data")
        
        vols = [r["volume"] for r in rows if r.get("volume", 0) > 0]
        
        # Forex — no volume data, neutral
        if not vols or sum(vols) < 1:
            return _m(0, "N/A", "Forex — no volume data (neutral)")
        
        curr = vols[-1]
        avg = sum(vols[:-1]) / len(vols[:-1]) if len(vols) > 1 else curr
        
        if avg == 0:
            return _m(0, "N/A", "Zero avg volume")
        
        ratio = round(curr / avg, 2)
        
        if ratio < 0.15:
            return _m(-18, "DEAD", f"Dead volume ({ratio}x) — no participation")
        if ratio < 0.4:
            return _m(-10, "LOW", f"Low volume ({ratio}x avg)")
        if ratio > 5.0:
            return _m(-12, "SPIKE", f"Volume spike ({ratio}x) — news?")
        if ratio > 2.5:
            return _m(10, "HIGH", f"Strong volume ({ratio}x) — good participation")
        if ratio > 1.5:
            return _m(6, "ABOVE", f"Above avg volume ({ratio}x)")
        
        # Check volume trend (last 5 vs previous 5)
        if len(vols) >= 10:
            recent = sum(vols[-5:]) / 5
            prev = sum(vols[-10:-5]) / 5
            if prev > 0 and recent > prev * 1.3:
                return _m(5, "RISING", f"Volume rising ({round(recent/prev,1)}x)")
            elif prev > 0 and recent < prev * 0.7:
                return _m(-5, "FALLING", "Volume falling — weak")
        
        return _m(0, "NORMAL", f"Normal volume ({ratio}x avg)")
    
    except Exception as e:
        return _m(0, "UNKNOWN", str(e)[:40])


def _m(mod, label, reason):
    return {"score_mod": mod, "label": label, "reason": reason}