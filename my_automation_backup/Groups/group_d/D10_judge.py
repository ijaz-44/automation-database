# Groups/group_d/D10_judge.py - Final trade judge for Layer 3
import os, sys, time
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from data_manager import get_rows, stage_d, stage_z, stage_a

def get_table(symbol, market, interval):
    """Final trade judge - Layer 3 confirmation"""
    try:
        # Get current price data
        rows = get_rows(symbol, interval, 100)
        if len(rows) < 20:
            return "<div class='section'><div class='section-title'>D10 Judge</div><div class='err-msg'>Not enough data for deep analysis</div></div>"
        
        current_price = rows[-1]['close']
        price_change = ((current_price - rows[0]['close']) / rows[0]['close']) * 100
        
        # Calculate basic metrics
        highs = [r['high'] for r in rows]
        lows = [r['low'] for r in rows]
        volumes = [r['volume'] for r in rows]
        
        recent_high = max(highs[-20:])
        recent_low = min(lows[-20:])
        avg_volume = sum(volumes[-20:]) / len(volumes[-20:])
        current_volume = volumes[-1]
        volume_ratio = current_volume / avg_volume if avg_volume > 0 else 1
        
        # Risk assessment
        volatility = (max(highs[-20:]) - min(lows[-20:])) / current_price * 100
        risk_level = "LOW" if volatility < 2 else "MEDIUM" if volatility < 5 else "HIGH"
        
        # Support/Resistance analysis
        support_distance = ((current_price - recent_low) / current_price) * 100
        resistance_distance = ((recent_high - current_price) / current_price) * 100
        
        # Final recommendation
        recommendation = "HOLD"
        confidence = 50
        
        if price_change > 1 and volume_ratio > 1.5 and support_distance > 0.5:
            recommendation = "BUY"
            confidence = min(85, 50 + price_change + (volume_ratio - 1) * 10)
        elif price_change < -1 and volume_ratio > 1.5 and resistance_distance > 0.5:
            recommendation = "SELL"  
            confidence = min(85, 50 + abs(price_change) + (volume_ratio - 1) * 10)
        
        confidence = int(confidence)
        
        # Color coding
        colors = {"BUY": "#44cc88", "SELL": "#ff8866", "HOLD": "#ffcc44", "LOW": "#44cc88", "MEDIUM": "#ffcc44", "HIGH": "#ff8866"}
        rec_color = colors.get(recommendation, "#ffcc44")
        risk_color = colors.get(risk_level, "#ffcc44")
        
        html = (
            "<div class='section'>"
            "<div class='section-title'>D10 Judge - Final Decision</div>"
            "<table>"
            "<tr><td>Current Price</td><td style='color:#00ff88;'>$" + str(round(current_price, 6)) + "</td></tr>"
            "<tr><td>Price Change</td><td style='color:#" + ("44cc88" if price_change > 0 else "ff8866") + ";'>" + ("+" if price_change > 0 else "") + str(round(price_change, 3)) + "%</td></tr>"
            "<tr><td>Volume Ratio</td><td style='color:#4da8ff;'>" + str(round(volume_ratio, 2)) + "x</td></tr>"
            "<tr><td>Risk Level</td><td style='color:" + risk_color + ";'>" + risk_level + "</td></tr>"
            "<tr><td>Volatility</td><td style='color:#888;'>" + str(round(volatility, 2)) + "%</td></tr>"
            "<tr><td>Support Distance</td><td style='color:#888;'>" + str(round(support_distance, 2)) + "%</td></tr>"
            "<tr><td>Resistance Distance</td><td style='color:#888;'>" + str(round(resistance_distance, 2)) + "%</td></tr>"
            "</table>"
            "<div style='margin-top:10px;padding:8px;background:rgba(0,0,0,0.22);border-radius:6px;border:1px solid rgba(0,200,255,0.15);'>"
            "<div style='color:#4da8ff;font-size:10px;margin-bottom:4px;'>FINAL RECOMMENDATION</div>"
            "<div style='color:" + rec_color + ";font-size:14px;font-weight:bold;'>" + recommendation + "</div>"
            "<div style='color:#888;font-size:9px;margin-top:2px;'>Confidence: " + str(confidence) + "%</div>"
            "</div>"
            "</div>"
        )
        
        return html
        
    except Exception as e:
        return "<div class='section'><div class='section-title'>D10 Judge</div><div class='err-msg'>D10 error: " + str(e)[:80] + "</div></div>"

__all__ = ['get_table']