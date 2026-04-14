# Groups/group_d/D10_judge.py
# Final trade judge for Layer 3 (Deep Analysis)
# Uses rows passed from sys_data.py – no extra API calls

def get_table(symbol, market, interval, rows):
    """Return HTML table with deep analysis."""
    if not rows or len(rows) < 20:
        return "<div class='err-msg'>Not enough data for deep analysis (need at least 20 candles)</div>"

    try:
        # ── Basic data ─────────────────────────────────────────────────────
        current_price = rows[-1]['close']
        price_change = ((current_price - rows[0]['close']) / rows[0]['close']) * 100 if rows[0]['close'] else 0
        highs = [r['high'] for r in rows]
        lows  = [r['low'] for r in rows]
        closes = [r['close'] for r in rows]
        volumes = [r.get('volume', 0) for r in rows]

        recent_high = max(highs[-20:])
        recent_low  = min(lows[-20:])
        avg_volume = sum(volumes[-20:]) / 20 if volumes[-20:] else 1
        current_volume = volumes[-1]
        volume_ratio = current_volume / avg_volume if avg_volume > 0 else 1

        # ── Volatility & Risk ─────────────────────────────────────────────
        volatility = (recent_high - recent_low) / current_price * 100 if current_price else 0
        if volatility < 2:
            risk_level = "LOW"
            risk_color = "#44cc88"
        elif volatility < 5:
            risk_level = "MEDIUM"
            risk_color = "#ffcc44"
        else:
            risk_level = "HIGH"
            risk_color = "#ff8866"

        # ── Support/Resistance distance ───────────────────────────────────
        support_distance = ((current_price - recent_low) / current_price) * 100 if current_price else 0
        resistance_distance = ((recent_high - current_price) / current_price) * 100 if current_price else 0

        # ── OBV (On Balance Volume) trend ─────────────────────────────────
        obv = 0
        obv_trend = "FLAT"
        for i in range(1, len(closes)):
            if closes[i] > closes[i-1]:
                obv += volumes[i]
            elif closes[i] < closes[i-1]:
                obv -= volumes[i]
        if len(closes) > 5:
            obv_trend = "UP" if obv > 0 else "DOWN"

        # ── CVD (Cumulative Volume Delta) estimate ────────────────────────
        cvd = 0
        for i in range(len(rows)):
            c, o, v = closes[i], rows[i]['open'], volumes[i]
            if c >= o:
                delta = v * ((c - o) / (rows[i]['high'] - rows[i]['low'] + 0.0001))
            else:
                delta = -v * ((o - c) / (rows[i]['high'] - rows[i]['low'] + 0.0001))
            cvd += delta
        cvd_trend = "UP" if len(rows) > 5 and cvd > 0 else "DOWN"

        # ── Volume Profile (POC – Point of Control) ───────────────────────
        price_min = min(lows[-50:]) if len(lows) >= 50 else min(lows)
        price_max = max(highs[-50:]) if len(highs) >= 50 else max(highs)
        bins = 10
        step = (price_max - price_min) / bins if price_max > price_min else 0.001
        vol_by_level = {}
        for i in range(-50, 0):
            if i >= -len(rows):
                r = rows[i]
                lvl = int((r['close'] - price_min) / step)
                lvl = max(0, min(bins-1, lvl))
                vol_by_level[lvl] = vol_by_level.get(lvl, 0) + r.get('volume', 0)
        poc_lvl = max(vol_by_level, key=vol_by_level.get) if vol_by_level else 5
        poc_price = round(price_min + poc_lvl * step, 6)
        above_poc = current_price > poc_price

        # ── Buy/Sell pressure (last 10 candles) ───────────────────────────
        buy_vol = sum(volumes[i] for i in range(-10, 0) if i >= -len(rows) and closes[i] >= rows[i]['open'])
        sell_vol = sum(volumes[i] for i in range(-10, 0) if i >= -len(rows) and closes[i] < rows[i]['open'])
        total_vol = buy_vol + sell_vol
        buy_pct = int(buy_vol / total_vol * 100) if total_vol > 0 else 50

        # ── Volume spike ──────────────────────────────────────────────────
        recent_vols = [volumes[i] for i in range(-5, 0) if i >= -len(rows) and volumes[i] > 0]
        avg_vol_prev = sum(volumes[:-5]) / len(volumes[:-5]) if len(volumes) > 5 else 1
        spike = recent_vols and recent_vols[-1] > avg_vol_prev * 2.0

        # ── Final recommendation ──────────────────────────────────────────
        recommendation = "HOLD"
        confidence = 50
        # Bullish signals
        if obv_trend == "UP": confidence += 5
        if cvd_trend == "UP": confidence += 5
        if above_poc: confidence += 5
        if buy_pct > 55: confidence += 5
        if volume_ratio > 1.5 and price_change > 1 and support_distance > 0.5:
            recommendation = "BUY"
            confidence = min(85, confidence + 15)
        elif volume_ratio > 1.5 and price_change < -1 and resistance_distance > 0.5:
            recommendation = "SELL"
            confidence = min(85, confidence + 15)
        else:
            # Neutral – adjust confidence down
            confidence = max(30, confidence - 10)

        confidence = int(confidence)

        # Color coding
        rec_color = {"BUY": "#44cc88", "SELL": "#ff8866", "HOLD": "#ffcc44"}.get(recommendation, "#ffcc44")

        # ── Build HTML table ──────────────────────────────────────────────
        html = f"""
        <div class='section'>
            <div class='section-title'>D10 Judge - Deep Analysis</div>
            <table style='width:100%'>
                <tr><th>Current Price</th><td style='color:#00ff88;'>${current_price:.6f}</td></tr>
                <tr><th>Price Change</th><td style='color:{"#44cc88" if price_change>0 else "#ff8866"};'>{price_change:+.2f}%</td></tr>
                <tr><th>Volume Ratio</th><td style='color:#4da8ff;'>{volume_ratio:.2f}x</td></tr>
                <tr><th>Risk Level</th><td style='color:{risk_color};'>{risk_level}</td></tr>
                <tr><th>Volatility</th><td>{volatility:.2f}%</td></tr>
                <tr><th>Support Distance</th><td>{support_distance:.2f}%</td></tr>
                <tr><th>Resistance Distance</th><td>{resistance_distance:.2f}%</td></tr>
                <tr><th>OBV Trend</th><td style='color:{"#44cc88" if obv_trend=="UP" else "#ff8866" if obv_trend=="DOWN" else "#aaaaff"};'>{obv_trend}</td></tr>
                <tr><th>CVD Trend</th><td style='color:{"#44cc88" if cvd_trend=="UP" else "#ff8866" if cvd_trend=="DOWN" else "#aaaaff"};'>{cvd_trend}</td></tr>
                <tr><th>POC Price</th><td>{poc_price:.6f}</td></tr>
                <tr><th>Above POC</th><td style='color:{"#44cc88" if above_poc else "#ff8866"};'>{above_poc}</td></tr>
                <tr><th>Buy Pressure</th><td>{buy_pct}%</td></tr>
                <tr><th>Volume Spike</th><td style='color:{"#44cc88" if spike else "#ff8866"};'>{spike}</td></tr>
            </table>
            <div style='margin-top:12px;padding:10px;background:rgba(0,0,0,0.22);border-radius:6px;border:1px solid rgba(0,200,255,0.15);'>
                <div style='color:#4da8ff;font-size:10px;margin-bottom:4px;'>FINAL RECOMMENDATION</div>
                <div style='color:{rec_color};font-size:16px;font-weight:bold;'>{recommendation}</div>
                <div style='color:#888;font-size:9px;margin-top:2px;'>Confidence: {confidence}%</div>
            </div>
        </div>
        """
        return html

    except Exception as e:
        return f"<div class='err-msg'>D10 error: {str(e)[:100]}</div>"