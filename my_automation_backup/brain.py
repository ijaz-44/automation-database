# brain.py
# -*- coding: utf-8 -*-
"""
Brain – AI prediction for next 1h candle (Android‑optimized)
- Sends raw file content as‑is (text files) or SQLite dump (DB files)
- Disables SSL verification to avoid Android certificate issues
- Uses gemini-2.0-flash (light, stable)
- Detailed logging of all errors
"""

import os
import json
import sqlite3
import re
import requests
import glob
import time
import urllib3
from datetime import datetime

# Disable SSL warnings (Android often has certificate issues)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ========== CONFIGURATION ==========
GEMINI_API_KEY = "AIzaSyBUHnodAy_-XzOJQSdlUDKOIexal-nR_nc"
GEMINI_URL_BASE = "https://generativelanguage.googleapis.com/v1/models"
SYMBOLS_DIR = "market_data/binance/symbols"
LOG_FILE = "brain.log"
LEARNING_LOG = "brain_learning.log"
MAX_LOG_SIZE = 5 * 1024 * 1024

# Models – start with the lightest for Android
MODELS = [
    "gemini-2.0-flash",      # light, fast
    "gemini-2.5-flash",
    "gemini-2.5-flash-lite",
    "gemini-2.5-pro"
]

def rotate_log_if_needed(filepath):
    if os.path.exists(filepath) and os.path.getsize(filepath) > MAX_LOG_SIZE:
        backup = filepath + ".old"
        try:
            os.replace(filepath, backup)
        except:
            pass

def log_message(level, msg, logfile=LOG_FILE):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"{timestamp} [{level}] {msg}"
    print(line)
    rotate_log_if_needed(logfile)
    try:
        with open(logfile, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except:
        pass

def log_learning(symbol, prompt_content, gemini_response, prediction_result=None):
    timestamp = datetime.now().isoformat()
    record = {
        "timestamp": timestamp,
        "symbol": symbol,
        "prompt_length": len(prompt_content),
        "prompt_snippet": prompt_content[:500] + "..." if len(prompt_content) > 500 else prompt_content,
        "gemini_response": gemini_response,
        "parsed_result": prediction_result
    }
    rotate_log_if_needed(LEARNING_LOG)
    with open(LEARNING_LOG, "a", encoding="utf-8") as f:
        f.write(json.dumps(record) + "\n")

# ---------- Read SQLite DB as text (full dump) ----------
def dump_sqlite_to_text(db_path):
    """Convert SQLite DB to TSV text (all tables)."""
    lines = []
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = cursor.fetchall()
        for (table_name,) in tables:
            lines.append(f"== TABLE: {table_name} ==")
            cursor.execute(f"PRAGMA table_info({table_name})")
            cols = [row[1] for row in cursor.fetchall()]
            lines.append("\t".join(cols))
            cursor.execute(f"SELECT * FROM {table_name}")
            rows = cursor.fetchall()
            for row in rows:
                lines.append("\t".join(str(v) if v is not None else "" for v in row))
            lines.append("")
        conn.close()
        log_message("INFO", f"Converted DB {os.path.basename(db_path)} -> {len(lines)} lines")
        return "\n".join(lines)
    except Exception as e:
        log_message("ERROR", f"Failed to dump {db_path}: {e}")
        return f"ERROR reading {db_path}: {e}"

def read_text_file(filepath):
    """Read raw text file content."""
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            content = f.read()
        log_message("INFO", f"Read text file {os.path.basename(filepath)} -> {len(content)} chars")
        return content
    except Exception as e:
        log_message("ERROR", f"Failed to read {filepath}: {e}")
        return f"ERROR reading {filepath}: {e}"

def get_all_files_for_symbol(symbol):
    clean = symbol.upper().replace("/", "").replace(" (OTC)", "").lower()
    pattern = os.path.join(SYMBOLS_DIR, f"{clean}*")
    all_files = glob.glob(pattern)
    result = []
    for fp in all_files:
        if os.path.isfile(fp):
            basename = os.path.basename(fp)
            # Skip log/tmp files
            if basename.startswith("x50_master.log") or basename.startswith("brain") or basename.endswith(".tmp"):
                continue
            result.append(fp)
    log_message("INFO", f"Found {len(result)} files for {symbol}: {[os.path.basename(f) for f in result]}")
    return result

def read_file_content(filepath):
    """Return file content as string (text or DB dump)."""
    if filepath.endswith(".db"):
        return dump_sqlite_to_text(filepath)
    elif filepath.endswith(".toon") or filepath.endswith(".tsv") or filepath.endswith(".jsonl"):
        return read_text_file(filepath)
    else:
        # Other files – treat as text
        return read_text_file(filepath)

def build_prompt(symbol, files_content):
    separator = "\n" + "="*80 + "\n"
    parts = [f"# FULL DATA FOR SYMBOL: {symbol.upper()}\n"]
    total_chars = 0
    for filepath, content in files_content:
        fname = os.path.basename(filepath)
        parts.append(f"## FILE: {fname}")
        parts.append(content)
        parts.append("")
        total_chars += len(content)
    full_data = separator.join(parts)

    # Gemini free tier limit ~1M tokens ≈ 500k chars. We'll cap at 250k to be safe.
    max_chars = 250000
    if len(full_data) > max_chars:
        log_message("WARNING", f"Data too large ({len(full_data)} chars), truncating to {max_chars}")
        full_data = full_data[:max_chars] + "\n... (truncated due to length)"
    
    prompt = f"""You are a professional crypto trader. 
Below is the complete raw data for {symbol} (all modules).
Based on ALL this information, predict the **next 1‑hour candle** direction, confidence (0-100), price target range (lowest and highest expected), and a short path description.

Respond ONLY with a JSON object in this exact format (no extra text, no markdown):
{{"direction": "UP", "confidence": 85, "target_low": 123.45, "target_high": 125.67, "path": "break resistance then rally"}}

- direction: "UP" or "DOWN"
- confidence: integer 0-100
- target_low and target_high: price range (lowest and highest)
- path: max 80 characters

Raw data:
{full_data}
"""
    return prompt

# ---------- Gemini API call with fallback and SSL disabled ----------
def ask_gemini_with_fallback(prompt):
    for model in MODELS:
        url = f"{GEMINI_URL_BASE}/{model}:generateContent?key={GEMINI_API_KEY}"
        log_message("INFO", f"Trying model: {model} (SSL verification OFF)")
        headers = {"Content-Type": "application/json"}
        payload = {"contents": [{"parts": [{"text": prompt}]}]}
        try:
            # Disable SSL verification, increase timeout
            resp = requests.post(url, headers=headers, json=payload, timeout=120, verify=False)
            if resp.status_code == 200:
                data = resp.json()
                text = data['candidates'][0]['content']['parts'][0]['text'].strip()
                log_message("INFO", f"✅ Gemini response from {model}: {len(text)} chars")
                return text, model
            elif resp.status_code == 429:
                log_message("WARNING", f"Rate limit hit for {model}. Waiting 60 seconds...")
                time.sleep(60)
                continue
            elif resp.status_code == 404:
                log_message("WARNING", f"Model {model} not found (HTTP 404). Trying next.")
                continue
            else:
                log_message("WARNING", f"Model {model} failed: HTTP {resp.status_code} - {resp.text[:200]}")
        except requests.exceptions.SSLError as e:
            log_message("WARNING", f"SSL error for {model}: {e}. Trying next model.")
        except requests.exceptions.ConnectionError as e:
            log_message("WARNING", f"Connection error for {model}: {e}. Trying next.")
        except Exception as e:
            log_message("WARNING", f"Model {model} exception: {e}")
    log_message("ERROR", "All models failed. Check network, API key, and Android SSL settings.")
    return None, None

def parse_gemini_response(text, fallback_price):
    if not text:
        return default_response(fallback_price)
    # Remove markdown code fences
    text = re.sub(r'```json\s*', '', text)
    text = re.sub(r'```\s*$', '', text)
    try:
        data = json.loads(text)
        if 'direction' not in data:
            data['direction'] = "WAIT"
        if 'confidence' not in data:
            data['confidence'] = 0
        if 'target_low' not in data or 'target_high' not in data:
            data['target_low'] = fallback_price * 0.99
            data['target_high'] = fallback_price * 1.01
        if 'path' not in data:
            data['path'] = "No path given"
        data['confidence'] = max(0, min(100, int(data['confidence'])))
        if data['direction'] not in ["UP", "DOWN", "WAIT"]:
            data['direction'] = "WAIT"
        return data
    except Exception as e:
        log_message("ERROR", f"JSON parse error: {e} | Raw: {text[:200]}")
        return default_response(fallback_price)

def default_response(price):
    return {
        "direction": "WAIT",
        "confidence": 0,
        "target_low": price,
        "target_high": price,
        "path": "No data or API error"
    }

def extract_fallback_price(files_content):
    for _, content in files_content:
        match = re.search(r'([0-9]+\.[0-9]{2,})', content)
        if match:
            try:
                return float(match.group(1))
            except:
                pass
    return 0

def predict(symbol):
    log_message("INFO", f"=== START PREDICTION FOR {symbol} ===")
    files = get_all_files_for_symbol(symbol)
    if not files:
        log_message("ERROR", f"No files found for {symbol}")
        return default_response(0)

    files_content = []
    for fp in files:
        content = read_file_content(fp)
        files_content.append((fp, content))

    prompt = build_prompt(symbol, files_content)
    fallback_price = extract_fallback_price(files_content)

    gemini_response, used_model = ask_gemini_with_fallback(prompt)
    if used_model:
        log_message("INFO", f"Successfully used model: {used_model}")
    result = parse_gemini_response(gemini_response, fallback_price)

    log_learning(symbol, prompt, gemini_response, result)
    log_message("INFO", f"Prediction result: {result}")
    log_message("INFO", f"=== END PREDICTION ===")
    return result

if __name__ == "__main__":
    import sys
    sym = sys.argv[1] if len(sys.argv) > 1 else "BTCUSDT"
    print(json.dumps(predict(sym), indent=2))