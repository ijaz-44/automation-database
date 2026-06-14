#!/usr/bin/env python3
# go.py – Dynamic Full Analysis Page (Auto-detects TSV columns & rows)

import os
import time
from flask import render_template_string

FEATURES_BASE_DIR = os.path.join(os.path.dirname(__file__), "market_data", "binance", "symbols")

# Ordered list of modules (by importance)
MODULES = [
    ("E14_regime", "📈 MARKET REGIME"),
    ("E16_manipulation", "🎭 MANIPULATION DETECTION"),
    ("E12_mstructure", "🏗️ MARKET STRUCTURE"),
    ("E11_volProfile", "📊 VOLUME PROFILE"),
    ("E02_derivative", "📈 DERIVATIVES (OI/FUNDING)"),
    ("E08_liquidation", "💀 LIQUIDATION EVENTS"),
    ("E03_tick", "⚡ TICK FLOW"),
    ("E05_depth", "📚 ORDER BOOK DEPTH"),
    ("E04_cvd", "📉 CUMULATIVE VOLUME DELTA"),
    ("E06_correlation", "🔄 CORRELATION WITH INDICES"),
    ("E10_sentiment", "😊 SENTIMENT & OI"),
    ("E09_sessions", "⏰ SESSION INTELLIGENCE"),
    ("E13_onchain", "⛓️ ON-CHAIN METRICS"),
    ("E07_macro", "🌍 MACROECONOMIC DATA"),
    ("E01_candles", "🕯️ CANDLE PATTERNS"),
    ("E15_indicators", "📊 TECHNICAL INDICATORS"),
]

# HTML template – fully dynamic, uses data passed from Python
TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=yes">
    <title>Analysis – {{ symbol }}</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body {
            background: #000000;
            color: #ccc;
            font-family: 'Segoe UI', -apple-system, monospace;
            font-size: 12px;
            padding: 8px;
            line-height: 1.4;
        }
        h1 {
            font-family: 'Orbitron', monospace;
            font-weight: 900;
            letter-spacing: 1px;
            color: #00FFFF;
            font-size: 16px;
            margin-bottom: 2px;
        }
        .timestamp {
            color: #666;
            font-size: 8px;
            margin-bottom: 8px;
            border-bottom: 1px solid #2a2a2a;
            padding-bottom: 4px;
        }
        .section-title {
            font-size: 12px;
            font-weight: bold;
            color: #9AFFAB;
            margin: 8px 0 4px 0;
            padding-left: 2px;
        }
        .data-table {
            width: 100%;
            border-collapse: collapse;
            font-size: 9px;
            margin-bottom: 10px;
            background: #0a0a0a;
            border: 1px solid #2a2a2a;
            border-radius: 6px;
            overflow: hidden;
        }
        .data-table th {
            background: #1a1a2e;
            color: #FCDB66;
            padding: 5px 6px;
            text-align: left;
            font-weight: bold;
            border-bottom: 1px solid #2a2a2a;
        }
        .data-table td {
            padding: 4px 6px;
            border-bottom: 1px solid #1e1e1e;
            color: #bbb;
        }
        .data-table tr:last-child td {
            border-bottom: none;
        }
        .scroll-hint {
            font-size: 7px;
            color: #666;
            text-align: right;
            margin-top: -8px;
            margin-bottom: 6px;
        }
        .footer {
            margin-top: 10px;
            text-align: center;
            font-size: 8px;
            color: #555;
            border-top: 1px solid #2a2a2a;
            padding-top: 6px;
        }
    </style>
</head>
<body>
    <h1>📊 {{ symbol }}</h1>
    <div class="timestamp">Generated: {{ timestamp }}</div>

    {% for module in modules %}
    <div class="section-title">{{ module.title }}</div>
    <div style="overflow-x: auto;">
        <table class="data-table">
            <thead>
                <tr>
                {% for col in module.headers %}
                <th>{{ col }}</th>
                {% endfor %}
                </tr>
            </thead>
            <tbody>
                {% for row in module.rows %}
                <tr>
                    {% for cell in row %}
                    <td>{{ cell }}</td>
                    {% endfor %}
                </tr>
                {% endfor %}
            </tbody>
        </table>
    </div>
    {% endfor %}

    <div class="footer">Kali Port – Full Analysis (scroll horizontally for tables)</div>
</body>
</html>
"""


def load_tsv(filepath):
    """Read a TSV file and return (headers, rows). Return (None, None) if file missing or empty."""
    if not os.path.exists(filepath):
        return None, None
    try:
        with open(filepath, "r") as f:
            lines = [l.strip() for l in f if l.strip()]
        if len(lines) < 2:
            return None, None
        headers = lines[0].split('\t')
        rows = [line.split('\t') for line in lines[1:]]
        # Limit to first 20 rows (to keep page size manageable)
        if len(rows) > 20:
            rows = rows[:20]
        return headers, rows
    except Exception:
        return None, None


def generate_full_analysis(symbol):
    symbol_lower = symbol.lower()
    base_dir = FEATURES_BASE_DIR

    modules_data = []
    for suffix, title in MODULES:
        # Try both underscore and hyphen? The user's files are e.g. "btcusdt_E14_regime.tsv" (underscore)
        filepath = os.path.join(base_dir, f"{symbol_lower}_{suffix.lower()}.tsv")
        headers, rows = load_tsv(filepath)
        if headers is not None:
            modules_data.append({
                "title": title,
                "headers": headers,
                "rows": rows
            })
        # else: silently skip missing modules

    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    return render_template_string(TEMPLATE, symbol=symbol, timestamp=timestamp, modules=modules_data)


def get_full_analysis(symbol):
    return generate_full_analysis(symbol)